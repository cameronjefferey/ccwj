"""SnapTrade brokerage aggregator — connect, sync, multi-account.

SnapTrade unlocks ~20 brokers (Schwab, Fidelity, Vanguard, Robinhood,
IBKR, Tradier, etc.) through one integration. **All broker OAuth in v2
goes through SnapTrade** — there is no parallel native Schwab module.

We use SnapTrade's hosted Connection Portal and pull activities +
positions + balances via SnapTrade's REST API, then normalize to the same
seed CSVs (``trade_history.csv`` / ``current_positions.csv`` /
``account_balances.csv``) as manual upload. Convergence happens at
``app.upload.merge_and_push_seeds`` so all the broker-sync-safety
invariants apply automatically.

Module structure mirrors ``app/schwab.py`` deliberately so callers,
templates, and tests can pattern-match between the two:

* ``snaptrade_connect``      — POST /snaptrade/connect (start)
* ``snaptrade_callback``     — GET  /snaptrade/callback (return)
* ``snaptrade_accounts``     — GET  /snaptrade/accounts (multi-account UI)
* ``snaptrade_sync``         — POST /snaptrade/sync (per-account or sync_all)
* ``snaptrade_disconnect``   — POST /snaptrade/accounts/disconnect
* ``snaptrade_nickname``     — POST /snaptrade/accounts/nickname
* ``_sync_one_connection``   — orchestrates one account sync (parallel to Schwab)
* ``_sync_all_for_user``     — bulk sync, mirrors ``_schwab_sync_all_for_user``
* ``_bulk_sync_lookback_days`` — first-sync vs routine lookback picker
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional

from flask import flash, redirect, render_template, request, session, url_for
from flask_login import current_user, login_required

from app import app
from app.models import (
    User,
    add_account_for_user,
    clear_snaptrade_connection_broken,
    get_snaptrade_account,
    get_snaptrade_accounts,
    get_snaptrade_user,
    list_all_snaptrade_accounts,
    mark_snaptrade_connection_broken,
    mark_snaptrade_first_sync_completed,
    record_snaptrade_holdings_sync,
    record_snaptrade_sync_attempt,
    record_snaptrade_sync_observation,
    remove_snaptrade_account,
    remove_snaptrade_user,
    save_snaptrade_user,
    set_snaptrade_brokerage_authorization_id,
    stamp_snaptrade_force_refresh_attempt,
    update_snaptrade_account_nickname,
    upsert_snaptrade_account,
    get_or_create_broker_tenant,
    build_tenant_id,
    SNAPTRADE_BROKER_SLUG,
)
from app.snaptrade_normalize import (
    activities_to_history_df,
    balances_to_balance_df,
    orders_to_history_df,
    positions_to_current_df,
)
from app.utils import demo_block_writes

_log = logging.getLogger(__name__)


def _ensure_snaptrade_tenant_id(
    user_id, snaptrade_account_id, account_name, *,
    snaptrade_connection_id=None,
):
    """Resolve (and persist) the v2 ``tenant_id`` for a SnapTrade account.

    ``snaptrade_account_id`` is SnapTrade's per-account UUID — stable
    for the life of the broker connection and survives renames. The
    resulting ``tenant_id`` is ``snaptrade:<uuid>``.

    Idempotent: calls ``get_or_create_broker_tenant`` which upserts
    ``broker_tenants``. See ``docs/V2_TENANT_KEY_DESIGN.md``.
    """
    ext_id = (snaptrade_account_id or "").strip()
    if not ext_id:
        raise ValueError("SnapTrade account row has no snaptrade_account_id")
    label = (account_name or "SnapTrade Account").strip() or "SnapTrade Account"
    return get_or_create_broker_tenant(
        user_id=user_id,
        broker_slug=SNAPTRADE_BROKER_SLUG,
        broker_uuid=ext_id,
        account_name=label,
        snaptrade_connection_id=snaptrade_connection_id,
    )

# Full-history cap for first sync UX. Per-broker depth varies — we ask
# for the full window and let SnapTrade clamp to what the broker carries.
SYNC_FULL_HISTORY_LOOKBACK_DAYS = 1825
SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS = SYNC_FULL_HISTORY_LOOKBACK_DAYS


def _routine_lookback_days() -> int:
    """Calendar days of transactions to request each routine sync."""
    raw = (
        os.environ.get("SNAPTRADE_SYNC_TRANSACTION_DAYS")
        or os.environ.get("SCHWAB_SYNC_TRANSACTION_DAYS")
        or "60"
    ).strip() or "60"
    try:
        days = int(raw)
    except ValueError:
        days = 60
    return max(1, min(days, SYNC_FULL_HISTORY_LOOKBACK_DAYS))


def _bulk_sync_lookback_days(first_done, *, force_full_history, routine_days, full_days):
    """Pick the per-row lookback for bulk sync loops."""
    if force_full_history or not first_done:
        return full_days
    return routine_days


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _snaptrade_config() -> Optional[tuple[str, str, str]]:
    """Return ``(client_id, consumer_key, redirect_uri)`` or None when
    the SnapTrade integration is not configured. Mirrors the
    ``_schwab_config`` shape so the Connect button / callback gate the
    same way."""
    client_id = os.environ.get("SNAPTRADE_CLIENT_ID", "").strip()
    consumer_key = os.environ.get("SNAPTRADE_CONSUMER_KEY", "").strip()
    redirect_uri = os.environ.get("SNAPTRADE_REDIRECT_URI", "").strip()
    if not client_id or not consumer_key:
        return None
    return client_id, consumer_key, redirect_uri


def snaptrade_enabled() -> bool:
    """Helper used by templates to decide whether to show the Connect
    button at all. Returns True iff env is configured AND the SnapTrade
    SDK is importable. Lazy import keeps the package optional during
    tests / dev shells where the dependency isn't installed yet."""
    if not _snaptrade_config():
        return False
    try:
        import snaptrade_client  # noqa: F401
        return True
    except ImportError:
        return False


def _get_snaptrade_client():
    """Lazily build a configured SnapTrade SDK client.

    Returns the client OR None when the integration is not configured
    or the SDK is missing. Never raises — callers should treat None as
    "feature disabled" the same way ``_get_schwab_client`` returning
    None means "session expired".
    """
    cfg = _snaptrade_config()
    if not cfg:
        return None
    client_id, consumer_key, _redirect = cfg
    try:
        from snaptrade_client import SnapTrade
    except ImportError:
        _log.warning("snaptrade_client not installed; pip install snaptrade-python-sdk")
        return None
    try:
        return SnapTrade(consumer_key=consumer_key, client_id=client_id)
    except Exception as exc:
        _log.warning("SnapTrade client init failed: %s", exc)
        return None


def _unwrap_body(resp):
    """Extract the JSON payload from a SnapTrade SDK response.

    Empirically (snaptrade-python-sdk 11.0.193) the SDK returns
    ``ApiResponseFor200`` with the parsed JSON under ``.body`` —
    NOT ``.data`` as you'd expect from python-openapi-generator
    conventions. Earlier prototype code used ``.data`` and silently
    fell back to the response object itself, which caused a real
    production bug: ``register_snap_trade_user`` returned a fresh
    ``userSecret`` in ``.body``, our code looked at ``.data`` (None),
    fell through to the bare response (no ``userSecret`` attribute),
    and we threw away the secret while SnapTrade had already created
    the user — leaving an orphan we couldn't authenticate against.

    Order of preference:
    1. ``resp.body`` — current SDK shape (dict or list).
    2. ``resp.data`` — older SDK / fallback for future drift.
    3. ``resp`` itself — last-resort for any code path that already
       handed us a plain dict / list (test mocks).

    Always returns the raw payload; callers decide whether they
    expect a dict vs a list.
    """
    body = getattr(resp, "body", None)
    if body is not None:
        return body
    data = getattr(resp, "data", None)
    if data is not None:
        return data
    return resp


# ---------------------------------------------------------------------------
# Account name helpers
# ---------------------------------------------------------------------------

def _stable_account_name(broker_slug: str, account_number_masked: Optional[str]) -> str:
    """Build the warehouse-facing ``account_name`` label. Once written
    into seed.Account, this string is the tenancy key joined to every
    trade and position row — never rename it for an existing account.

    Format: ``{Broker} ••••{last4}`` when we have a masked number, else
    ``{Broker} Account``. Mirrors the Schwab pattern (``Schwab ••••5989``)
    so users see a consistent shape across both connectors.
    """
    broker = (broker_slug or "Broker").strip().title()
    last4 = (account_number_masked or "").strip()[-4:]
    if last4 and last4.isdigit():
        return f"{broker} ••••{last4}"
    return f"{broker} Account"


# ---------------------------------------------------------------------------
# Routes — connect / callback
# ---------------------------------------------------------------------------

@app.route("/snaptrade/connect", methods=["GET", "POST"])
@login_required
def snaptrade_connect():
    """Start a SnapTrade connection. Idempotent — re-registers the
    HappyTrader user with SnapTrade if needed, then redirects them to
    the SnapTrade-hosted Connection Portal where they pick a broker
    and authenticate. SnapTrade redirects back to ``/snaptrade/callback``.

    RECONNECT MODE: when the form carries ``reconnect_authorization_id``
    (a broken connection's SnapTrade brokerage-authorization UUID), we pass
    SnapTrade's ``reconnect`` param so the portal goes straight to repairing
    THAT connection instead of adding a brand-new one. One broker grant backs
    many accounts, so fixing the grant clears every sibling account at once.
    Falls back to the normal "add a brokerage" portal when no id is supplied
    (e.g. the auth id was never cached because the account never refreshed).
    """
    blocked = demo_block_writes("connecting a brokerage account")
    if blocked:
        return blocked
    reconnect_auth_id = (request.form.get("reconnect_authorization_id") or "").strip()
    reconnect_broker_label = (request.form.get("reconnect_broker_label") or "").strip()
    is_reconnect = bool(reconnect_auth_id or reconnect_broker_label)
    client = _get_snaptrade_client()
    if not client:
        flash("Multi-broker connect is not configured. Contact the administrator.", "danger")
        return redirect(url_for("profile", tab="account"))

    user_id = current_user.id
    snap = get_snaptrade_user(user_id)
    try:
        if not snap:
            # Use a stable internal id so SnapTrade can correlate retries
            # to the same user record. SnapTrade userId is opaque to the
            # broker; PII would only land in our own DB.
            #
            # SnapTrade userIds are GLOBAL to the clientId (the SnapTrade
            # app), and dev/staging often share one clientId with prod. A
            # bare ``happytrader-{user_id}`` therefore collides across
            # environments (local user 7 and prod user 7 resolve to the
            # same SnapTrade user) — re-registering one would rotate the
            # secret out from under the other. ``SNAPTRADE_USER_NAMESPACE``
            # (empty in prod, e.g. "local" in dev) keeps each environment's
            # SnapTrade users disjoint so connecting/disconnecting locally
            # can never disturb a real prod connection.
            _snap_ns = os.environ.get("SNAPTRADE_USER_NAMESPACE", "").strip()
            snap_user_label = (
                f"happytrader-{_snap_ns}-{user_id}" if _snap_ns
                else f"happytrader-{user_id}"
            )
            try:
                resp = client.authentication.register_snap_trade_user(
                    user_id=snap_user_label
                )
            except Exception as exc:
                # If a previous attempt left an orphan SnapTrade user
                # (we lost the secret on first try), SnapTrade returns
                # 1010 / "User already exists" on retry. Recover by
                # deleting the orphan and re-registering once. The
                # delete is async (webhook-confirmed) — we sleep
                # briefly then retry.
                if "1010" in str(exc) or "already exists" in str(exc).lower():
                    _log.warning(
                        "SnapTrade register said user '%s' already exists — "
                        "deleting orphan and retrying once.",
                        snap_user_label,
                    )
                    try:
                        client.authentication.delete_snap_trade_user(
                            user_id=snap_user_label
                        )
                    except Exception as del_exc:
                        _log.warning("orphan delete failed (continuing): %s", del_exc)
                    import time as _time
                    _time.sleep(2.0)
                    resp = client.authentication.register_snap_trade_user(
                        user_id=snap_user_label
                    )
                else:
                    raise
            payload = _unwrap_body(resp)
            if not isinstance(payload, dict):
                raise RuntimeError(
                    f"SnapTrade register returned unexpected payload type "
                    f"{type(payload).__name__}"
                )
            snap_user_id = payload.get("userId") or snap_user_label
            snap_secret = payload.get("userSecret") or ""
            if not snap_secret:
                raise RuntimeError("SnapTrade did not return a userSecret on register")
            save_snaptrade_user(user_id, snap_user_id, snap_secret)
            snap = {"snaptrade_user_id": snap_user_id, "snaptrade_secret": snap_secret}

        # SnapTrade doesn't have a per-app "allowed redirect URIs" list
        # like Schwab does — the post-portal redirect is set PER session
        # via custom_redirect. We pass our /snaptrade/callback URL so
        # SnapTrade sends the user back into our app after they finish
        # picking + authenticating their broker. Without this, the user
        # lands on SnapTrade's generic success page and never re-enters
        # our app to trigger the account-list fetch.
        cfg = _snaptrade_config()
        custom_redirect = (cfg[2] if cfg and cfg[2] else "") or url_for(
            "snaptrade_callback", _external=True
        )
        login_kwargs = {
            "user_id": snap["snaptrade_user_id"],
            "user_secret": snap["snaptrade_secret"],
            "custom_redirect": custom_redirect,
        }
        # Reconnect a specific broken grant rather than adding a new one.
        if reconnect_auth_id:
            login_kwargs["reconnect"] = reconnect_auth_id
            _log.info(
                "SnapTrade reconnect requested for user_id=%s auth_id=%s",
                user_id, reconnect_auth_id,
            )
        login_resp = client.authentication.login_snap_trade_user(**login_kwargs)
        login_payload = _unwrap_body(login_resp)
        portal_url = (
            login_payload.get("redirectURI")
            if isinstance(login_payload, dict)
            else None
        )
        if not portal_url:
            raise RuntimeError("SnapTrade did not return a Connection Portal URL")
    except Exception as exc:
        _log.exception("SnapTrade connect failed for user_id=%s: %s", user_id, exc)
        flash(
            "Couldn't open the broker connection portal. Try again, or "
            "contact support if it keeps happening.",
            "danger",
        )
        return redirect(url_for("profile", tab="account"))

    # Stamp the originating user id into the session so /snaptrade/callback
    # can verify the return matches the same login (defense-in-depth: the
    # callback uses login_required already, but this guards against a
    # race where a user logs out / in between connect and callback).
    session["snaptrade_callback_user_id"] = user_id
    # Remember this was a reconnect so the callback can confirm it
    # specifically ("Schwab reconnected") instead of the generic
    # "Connected N accounts" copy. Cleared (popped) in the callback.
    if is_reconnect:
        session["snaptrade_reconnect_label"] = reconnect_broker_label or "broker"
    else:
        session.pop("snaptrade_reconnect_label", None)
    return redirect(portal_url)


@app.route("/snaptrade/callback")
@login_required
def snaptrade_callback():
    """Handle SnapTrade's redirect after the user finishes the
    Connection Portal flow. Lists the user's accounts via SnapTrade
    and persists each one as a row in ``snaptrade_accounts``.
    """
    expected_user_id = session.pop("snaptrade_callback_user_id", None)
    # Pop the reconnect marker unconditionally so it can never leak into a
    # later unrelated connect; only the success path below uses it.
    reconnect_label = session.pop("snaptrade_reconnect_label", None)
    if expected_user_id and expected_user_id != current_user.id:
        flash("Session mismatch. Please try connecting again.", "danger")
        return redirect(url_for("profile", tab="account"))

    user_id = current_user.id
    snap = get_snaptrade_user(user_id)
    if not snap:
        flash("Your SnapTrade session expired. Try Connect again.", "warning")
        return redirect(url_for("snaptrade_connect"))

    client = _get_snaptrade_client()
    if not client:
        flash("Multi-broker connect is not configured.", "danger")
        return redirect(url_for("profile", tab="account"))

    try:
        accounts = _list_snaptrade_accounts(client, snap)
    except Exception as exc:
        _log.exception("SnapTrade list_accounts failed for user_id=%s: %s", user_id, exc)
        flash(
            "Connected, but couldn't fetch your accounts. Try Sync again "
            "from the manage page in a moment.",
            "warning",
        )
        return redirect(url_for("snaptrade_accounts_page"))

    if not accounts:
        flash(
            "No accounts came back from the broker yet. The broker can take "
            "a few minutes to surface new connections — try again shortly.",
            "info",
        )
        return redirect(url_for("snaptrade_accounts_page"))

    saved = 0
    for acc in accounts:
        snaptrade_account_id = str(acc.get("id") or "").strip()
        if not snaptrade_account_id:
            continue
        broker_slug = (
            (acc.get("institution_name") or "").strip()
            or _institution_slug_from(acc)
            or "BROKER"
        )
        masked = (acc.get("number") or acc.get("account_number") or "").strip() or None
        account_name = _stable_account_name(broker_slug, masked)

        upsert_snaptrade_account(
            user_id,
            snaptrade_account_id,
            broker_slug=broker_slug,
            account_number_masked=masked,
            account_name=account_name,
        )
        # Stash the brokerage_authorization UUID on the row so the
        # "Refresh from broker" button doesn't have to spend an extra
        # ``get_user_account_details`` round-trip on first press. The
        # per-auth iteration in _list_snaptrade_accounts always
        # populates this field (see backstop in that function).
        auth_id = (acc.get("brokerage_authorization") or "").strip() if isinstance(acc.get("brokerage_authorization"), str) else ""
        if auth_id:
            set_snaptrade_brokerage_authorization_id(
                user_id, snaptrade_account_id, auth_id,
            )
        # Being returned by the relist means SnapTrade has an AUTHORIZED
        # grant for this account, so clear any stale "Reconnect needed"
        # flag immediately — the page banner shouldn't linger until the
        # next sync. No-op on a fresh connect (flag was already NULL); on a
        # reconnect this is what makes the red banner disappear right away.
        try:
            clear_snaptrade_connection_broken(user_id, snaptrade_account_id)
        except Exception as exc:
            app.logger.warning(
                "clear_snaptrade_connection_broken failed for user_id=%s "
                "snaptrade_account_id=%s: %s",
                user_id, snaptrade_account_id, exc,
            )
        add_account_for_user(user_id, account_name)
        # v2 tenancy: register broker_tenants row so the first sync
        # after callback has a real tenant_id to stamp into seed rows.
        try:
            _ensure_snaptrade_tenant_id(
                user_id=user_id,
                snaptrade_account_id=snaptrade_account_id,
                account_name=account_name,
                snaptrade_connection_id=auth_id or None,
            )
        except Exception as exc:
            app.logger.warning(
                "broker_tenants registration deferred for SnapTrade user_id=%s "
                "snaptrade_account_id=%s: %s",
                user_id, snaptrade_account_id, exc,
            )
        saved += 1

    if reconnect_label:
        flash(
            f"{reconnect_label} reconnected — your connection is healthy "
            f"again. Use Sync now to pull the latest data.",
            "success",
        )
    else:
        flash(
            f"Connected {saved} account{'s' if saved != 1 else ''}. "
            f"Use Sync now to pull your data.",
            "success",
        )
    return redirect(url_for("snaptrade_accounts_page"))


def _list_snaptrade_accounts(client, snap):
    """Return every SnapTrade-linked broker account for one user.

    Modern flow (post-deprecation of ``account_information.list_user_accounts``):
    iterate ``connections.list_brokerage_authorizations`` and for each
    auth call ``connections.list_brokerage_authorization_accounts``.
    SnapTrade SDK 11.0.193 logs a deprecation warning on every
    ``list_user_accounts`` call; the per-auth iteration is the
    documented replacement and returns the SAME record shape (verified
    against Alpaca + Robinhood live, May 2026: same keys, same
    ``brokerage_authorization`` field, same ``id`` UUIDs).

    Cost trade-off: 1 API call → (1 + N_auths) calls. For typical
    users (1–3 brokerages) this is 2–4 calls vs the old 1 — well
    inside SnapTrade's per-user rate limits and actually cheaper than
    the back-and-forth that would happen if SnapTrade fully removes
    the old endpoint and we have to scramble.

    Bonus: every account record from this path carries
    ``brokerage_authorization`` (the auth UUID) directly, which
    callers can stash on the snaptrade_accounts row to back the
    "Refresh from broker" button without an extra
    ``get_user_account_details`` round-trip.

    Defensive: any per-auth exception is logged and skipped — one
    broken connection (e.g. revoked grant) must not blank out the
    other linked brokerages.
    """
    auth_resp = client.connections.list_brokerage_authorizations(
        user_id=snap["snaptrade_user_id"],
        user_secret=snap["snaptrade_secret"],
    )
    auth_body = _unwrap_body(auth_resp)
    if isinstance(auth_body, dict):
        auths = auth_body.get("authorizations", []) or []
    elif isinstance(auth_body, list):
        auths = auth_body
    else:
        auths = []

    out: list[dict] = []
    for auth in auths:
        if not isinstance(auth, dict):
            try:
                auth = auth.to_dict() if hasattr(auth, "to_dict") else {}
            except Exception:
                continue
        auth_id = (auth.get("id") or "").strip()
        if not auth_id:
            continue
        try:
            acc_resp = client.connections.list_brokerage_authorization_accounts(
                user_id=snap["snaptrade_user_id"],
                user_secret=snap["snaptrade_secret"],
                authorization_id=auth_id,
            )
        except Exception as exc:
            app.logger.warning(
                "SnapTrade list_brokerage_authorization_accounts failed for "
                "auth=%s: %s — skipping this brokerage's accounts; the rest "
                "still flow through.",
                auth_id, exc,
            )
            continue
        body = _unwrap_body(acc_resp)
        if isinstance(body, dict):
            items = body.get("accounts", []) or []
        elif isinstance(body, list):
            items = body
        else:
            items = []
        for item in items:
            if isinstance(item, dict):
                # Backstop the brokerage_authorization field in case
                # a future API revision drops it from the per-account
                # payload — without it the auth-id cache backfill in
                # _sync_one_connection wouldn't fire.
                item.setdefault("brokerage_authorization", auth_id)
                out.append(item)
            elif hasattr(item, "to_dict"):
                try:
                    d = item.to_dict()
                    d.setdefault("brokerage_authorization", auth_id)
                    out.append(d)
                except Exception:
                    continue
    return out


def _institution_slug_from(acc) -> str:
    """SnapTrade puts the brokerage either under ``institution_name``
    or under a nested ``brokerage_authorization``. Normalize."""
    auth = acc.get("brokerage_authorization") or {}
    if isinstance(auth, dict):
        b = auth.get("brokerage") or {}
        if isinstance(b, dict):
            return (b.get("name") or b.get("slug") or "").strip()
    return ""


# ---------------------------------------------------------------------------
# Routes — multi-account UI
# ---------------------------------------------------------------------------

@app.route("/snaptrade/accounts", methods=["GET"])
@login_required
def snaptrade_accounts_page():
    """Multi-account manager: list every linked broker, sync each,
    rename, disconnect. Mirrors ``schwab_accounts.html``.

    Accounts are grouped by BROKER CONNECTION (the SnapTrade brokerage
    authorization) because a single broker grant authorizes many accounts
    and reconnection is a connection-level action — showing "Reconnect
    needed" on every individual account is confusing and wrong (the user
    reconnects the broker once, not each account). See
    ``_group_accounts_by_connection``.
    """
    rows = get_snaptrade_accounts(current_user.id) or []
    groups = _group_accounts_by_connection(rows)
    return render_template(
        "snaptrade_accounts.html",
        title="Connected brokerages",
        accounts=rows,
        connection_groups=groups,
        any_reconnect_needed=any(g["needs_reconnect"] for g in groups),
        snaptrade_enabled=snaptrade_enabled(),
    )


def _group_accounts_by_connection(rows):
    """Group SnapTrade account rows by broker connection.

    Grouping key is the SnapTrade ``brokerage_authorization_id`` (the
    per-grant connection UUID) when known, else the ``broker_slug`` (so
    accounts whose auth id hasn't been cached yet still collapse under
    their broker instead of each rendering a standalone "reconnect"
    prompt). Order is preserved by first-seen ``created_at`` so the page
    layout is stable.

    Each group exposes:
      - ``broker_slug`` / ``broker_label`` — for the header + reconnect copy
      - ``authorization_id`` — first known auth id in the group, passed to
        the reconnect flow so SnapTrade fixes THIS connection directly
      - ``needs_reconnect`` — True if any account in the group is broken
      - ``accounts`` — the member rows (unchanged shape)
    """
    groups = []
    by_key = {}
    for r in rows:
        slug = (r.get("broker_slug") or "").strip()
        auth_id = (r.get("brokerage_authorization_id") or "").strip()
        key = auth_id or f"slug:{slug.lower()}"
        g = by_key.get(key)
        if g is None:
            g = {
                "key": key,
                "broker_slug": slug,
                "broker_label": (slug.title() if slug else "Brokerage"),
                "authorization_id": auth_id or None,
                "needs_reconnect": False,
                "accounts": [],
            }
            by_key[key] = g
            groups.append(g)
        # Fill in the first known auth id for the group (used for reconnect).
        if auth_id and not g["authorization_id"]:
            g["authorization_id"] = auth_id
        if r.get("connection_broken_at"):
            g["needs_reconnect"] = True
        g["accounts"].append(r)
    return groups


@app.route("/snaptrade/accounts/nickname", methods=["POST"])
@login_required
def snaptrade_account_nickname():
    """UI-only label. Never touches ``account_name`` (the warehouse
    tenancy key). Mirrors the Schwab nickname endpoint."""
    blocked = demo_block_writes("renaming a brokerage account")
    if blocked:
        return blocked
    snaptrade_account_id = (request.form.get("snaptrade_account_id") or "").strip()
    nickname = (request.form.get("nickname") or "").strip()
    if not snaptrade_account_id:
        flash("Missing account id.", "warning")
        return redirect(url_for("snaptrade_accounts_page"))
    update_snaptrade_account_nickname(current_user.id, snaptrade_account_id, nickname)
    flash("Nickname saved.", "success")
    return redirect(url_for("snaptrade_accounts_page"))


@app.route("/snaptrade/accounts/disconnect", methods=["POST"])
@login_required
def snaptrade_disconnect():
    """Disconnect ONE linked broker account. Leaves the user's
    SnapTrade userId/userSecret in place so they can re-add brokers
    without re-registering."""
    blocked = demo_block_writes("disconnecting a brokerage account")
    if blocked:
        return blocked
    snaptrade_account_id = (request.form.get("snaptrade_account_id") or "").strip()
    if not snaptrade_account_id:
        flash("Missing account id.", "warning")
        return redirect(url_for("snaptrade_accounts_page"))

    snap = get_snaptrade_user(current_user.id)
    client = _get_snaptrade_client()
    if snap and client:
        try:
            client.connections.remove_brokerage_authorization(
                user_id=snap["snaptrade_user_id"],
                user_secret=snap["snaptrade_secret"],
                authorization_id=snaptrade_account_id,
            )
        except Exception as exc:
            # Don't block the local DB cleanup on a SnapTrade error —
            # we still want the user to be able to remove the row from
            # our UI. Surface the error in the flash so they know
            # the broker side may need manual revoke.
            _log.warning(
                "SnapTrade remove_brokerage_authorization failed for user_id=%s account=%s: %s",
                current_user.id, snaptrade_account_id, exc,
            )
            flash(
                "Removed from HappyTrader. The broker side may need a "
                "manual revoke from your broker's app if it shows up again.",
                "warning",
            )

    remove_snaptrade_account(current_user.id, snaptrade_account_id)
    flash("Account disconnected.", "success")
    return redirect(url_for("snaptrade_accounts_page"))


# ---------------------------------------------------------------------------
# Force refresh — Connections.refresh_brokerage_authorization
# ---------------------------------------------------------------------------

# Throttle window for the manual "Refresh from broker" button. SnapTrade
# bills per refresh call (see SDK docstring: "each call to this endpoint
# incurs an additional charge"), so a misbehaving client could otherwise
# rack up cost in seconds. Ten minutes per authorization is generous for
# a user-initiated nudge — refreshes are async and SnapTrade typically
# completes the holding update in 30–60 seconds anyway, so a second
# press inside the throttle adds no real-time information.
SNAPTRADE_FORCE_REFRESH_THROTTLE_SECONDS = 600

# How long an interactive "Sync now" waits after asking SnapTrade to repoll the
# broker before reading holdings. The refresh is async; SnapTrade typically
# pushes the fresh snapshot within ~30-60s, but tying up a Flask worker that
# long is bad UX, so we give the broker a short head start and accept that a
# just-placed trade may need one more sync to appear (the next routine sync
# catches it). Env-overridable. Mirrors the value the old standalone
# "Refresh from broker" route used.
SNAPTRADE_FORCE_REFRESH_SETTLE_SECONDS = int(
    os.environ.get("SNAPTRADE_FORCE_REFRESH_SETTLE_SECONDS", "5") or "5"
)

# Settle window for the DORMANT --force-refresh CLI pass. On our real-time plan
# refresh_brokerage_authorization is a no-op (cached-plan-only; 403s), and trade
# ACTIVITIES are T+1 for every broker regardless (SnapTrade support 2026-07-10 —
# see docs / broker-sync-safety SKILL). So this window is only meaningful if we
# ever downgrade to a cached plan; the market-close cron that used it was removed.
# Kept longer than the interactive "Sync now" settle so a batch repoll would land
# in the SAME run rather than leaning on the follow-up webhook. Env-overridable.
SNAPTRADE_CRON_FORCE_REFRESH_SETTLE_SECONDS = int(
    os.environ.get("SNAPTRADE_CRON_FORCE_REFRESH_SETTLE_SECONDS", "90") or "90"
)


def _force_refresh_brokerage(user_id, snaptrade_account_id, *, throttle_seconds=None):
    """Trigger a SnapTrade ``refresh_brokerage_authorization`` for one
    linked brokerage account, with billing-aware throttle.

    Returns ``(ok: bool, message: str, throttle_remaining_s: int|None)``.

    Side effects:
      - On first call ever for this account, fetches and caches the
        ``brokerage_authorization_id`` via ``get_user_account_details``
        so subsequent presses skip that round-trip.
      - On API success, stamps ``last_force_refresh_at = NOW()``.
      - **Important**: refresh is async on SnapTrade's side. The caller
        sleeps briefly (5s) before chaining into the regular sync so
        the broker has a head start on returning fresh data — but this
        does NOT guarantee the next sync sees the refreshed snapshot.
        The honest UX is "we asked the broker to repoll; this sync may
        already include it, the next definitely will."

    Failure is non-fatal: caller should still proceed to the regular
    sync (the existing flow returns whatever SnapTrade has cached, which
    is the same outcome as before this feature shipped).
    """
    if throttle_seconds is None:
        throttle_seconds = SNAPTRADE_FORCE_REFRESH_THROTTLE_SECONDS

    cfg = _snaptrade_config()
    if not cfg:
        return (False, "Multi-broker connect is not configured.", None)

    snap = get_snaptrade_user(user_id)
    if not snap:
        return (False, "Reconnect SnapTrade — your session is no longer valid.", None)

    acc_row = get_snaptrade_account(user_id, snaptrade_account_id)
    if not acc_row:
        return (False, "Couldn't find that brokerage connection on your account.", None)

    # Throttle check — anchor on last_force_refresh_at, not last_sync_at.
    # The two are independent: a user who called "Sync now" 30 seconds
    # ago should still be allowed to press "Refresh from broker" if they
    # haven't pressed THAT button recently. Different cost profiles.
    last_refresh = acc_row.get("last_force_refresh_at")
    if last_refresh is not None:
        from datetime import datetime, timezone
        try:
            now = datetime.now(timezone.utc)
            elapsed = (now - last_refresh).total_seconds()
        except Exception:
            elapsed = float("inf")
        if elapsed < throttle_seconds:
            remaining = int(throttle_seconds - elapsed)
            mins = max(1, (remaining + 59) // 60)
            return (
                False,
                f"Already refreshed recently — try again in about {mins} "
                f"minute{'s' if mins != 1 else ''}. (SnapTrade bills per "
                f"refresh, so we space them out.)",
                remaining,
            )

    client = _get_snaptrade_client()
    if client is None:
        return (False, "SnapTrade SDK isn't installed on this server.", None)

    # Resolve the brokerage_authorization_id. Prefer the cached value;
    # fetch it via account_details on the first refresh ever for this
    # account, then cache it.
    auth_id = (acc_row.get("brokerage_authorization_id") or "").strip()
    if not auth_id:
        try:
            detail = client.account_information.get_user_account_details(
                user_id=snap["snaptrade_user_id"],
                user_secret=snap["snaptrade_secret"],
                account_id=snaptrade_account_id,
            )
        except Exception as exc:
            # Best-effort pre-pass — log the raw exception so the exact
            # status/body is diagnosable, but do NOT mark the connection
            # broken from here. Broken-detection is owned by the read path's
            # authoritative _brokerage_authorization_disabled check; inferring
            # "broken" from a per-endpoint 401/403 is the first-Fidelity
            # misclassification (see broker-sync-safety SKILL.md).
            app.logger.warning(
                "_force_refresh_brokerage: get_user_account_details failed for "
                "user_id=%s account=%s: %s: %s",
                user_id, snaptrade_account_id, type(exc).__name__, exc,
            )
            if _looks_like_auth_error(exc):
                return (
                    False,
                    "SnapTrade wouldn't look up this connection to refresh it "
                    "(it may not be permitted on the current plan).",
                    None,
                )
            return (
                False,
                "Couldn't reach SnapTrade to look up the brokerage. "
                "Try again in a minute.",
                None,
            )
        body = _unwrap_body(detail)
        if not isinstance(body, dict):
            return (False, "SnapTrade returned an unexpected account shape.", None)
        auth_id = (body.get("brokerage_authorization") or "").strip()
        if not auth_id:
            # No auth id on the account detail payload? Treat as an
            # API contract drift. Don't proceed.
            return (
                False,
                "SnapTrade didn't return a brokerage authorization for this "
                "account. Try a regular sync instead.",
                None,
            )
        # Best-effort cache. If this fails, we'll just re-fetch on the
        # next refresh — not catastrophic.
        set_snaptrade_brokerage_authorization_id(
            user_id, snaptrade_account_id, auth_id,
        )

    # Fire the refresh. SnapTrade returns ``{"detail": "Connection ...
    # scheduled for refresh"}`` on success.
    try:
        client.connections.refresh_brokerage_authorization(
            authorization_id=auth_id,
            user_id=snap["snaptrade_user_id"],
            user_secret=snap["snaptrade_secret"],
        )
    except Exception as exc:
        # Always log the raw exception first — the refresh endpoint is BILLED
        # + entitlement-gated, so its exact status/body (401 vs 403 vs 425) is
        # the only way to tell "plan doesn't allow manual refresh" from "broker
        # rate-limited" from "genuinely revoked". The auth-error branch used to
        # swallow the body entirely, leaving us blind (2026-07-09).
        app.logger.warning(
            "_force_refresh_brokerage: refresh call failed for user_id=%s "
            "auth=%s account=%s: %s: %s",
            user_id, auth_id, snaptrade_account_id, type(exc).__name__, exc,
        )
        msg = str(exc)
        # SnapTrade returns 425 / 429 when refreshes happen too often
        # broker-side (not our throttle — the broker's). Surface that
        # honestly so the user knows it's not on us.
        if any(s in msg for s in ("425", "429", "rate", "Too Many")):
            stamp_snaptrade_force_refresh_attempt(user_id, snaptrade_account_id)
            return (
                False,
                "Your broker is rate-limiting refresh requests. Wait a few "
                "minutes and try again — this is the broker's limit, not ours.",
                None,
            )
        if _looks_like_auth_error(exc):
            # 401/403 on the REFRESH endpoint ONLY. This does NOT mean the
            # connection is dead — reads on the same authorization keep working
            # when manual refresh simply isn't permitted on the plan (real case
            # 2026-07-09: every broker 403'd on refresh while holdings reads
            # returned 200). So we do NOT mark_snaptrade_connection_broken here
            # — that would false-flag every user "reconnect your broker" off an
            # entitlement error and fire reconnect emails. The read that follows
            # this pre-pass (_brokerage_authorization_disabled + the sync error
            # handler) owns authoritative broken-detection. Same lesson as the
            # first-Fidelity 402/403 misclassification (broker-sync-safety).
            return (
                False,
                "SnapTrade wouldn't refresh this connection right now (it may "
                "not be permitted on the current plan). Showing the latest data "
                "SnapTrade already has.",
                None,
            )
        return (
            False,
            "SnapTrade couldn't reach the broker to refresh right now. "
            "Try again in a minute.",
            None,
        )

    stamp_snaptrade_force_refresh_attempt(user_id, snaptrade_account_id)
    return (
        True,
        "Asked your broker to send fresh data. Continuing to sync now…",
        None,
    )


def _flash_and_redirect_after_sync(res, *, first_done, refreshed=False):
    """Shared post-sync UX for the single-account sync routes.

    Centralizes the three terminal outcomes so "Sync now" and "Refresh from
    broker" behave identically:

    * sync failed            → danger flash, back to accounts page.
    * pushed a seed change    → success flash + redirect to the processing
                                page (a dbt build is now running).
    * NO seed change          → info flash, NO redirect to processing. The
                                whole point of the user's request: if nothing
                                changed there's nothing to rebuild, so we don't
                                send them to a "we're processing…" page that
                                waits on a build that will never run.
    """
    if not res["ok"]:
        flash(
            "SnapTrade sync didn't finish. Try again in a minute, or reconnect "
            "the broker if it keeps happening.",
            "danger",
        )
        return redirect(url_for("snaptrade_accounts_page"))

    hr, cr = res["history_rows"], res["current_rows"]
    trade_word = "trade" if hr == 1 else "trades"
    pos_word = "position" if cr == 1 else "positions"
    verb = "Refresh + sync" if refreshed else "Sync"
    summary = (
        f"{verb} complete for {res['label']}. Pulled {hr:,} {trade_word} "
        f"and {cr} open {pos_word}."
    )

    if res["github_pushed"]:
        tail = (
            " We're processing now — refresh in a minute."
            if not refreshed else
            " We're processing now — refresh in a minute. If a just-placed "
            "trade still doesn't appear, that broker takes a few minutes to "
            "push to SnapTrade — try one more sync shortly."
        )
        flash(summary + tail, "success")
        qp = {}
        h = (res["github_head_sha"] or "").strip()
        if h:
            qp["sha"] = h
        if not first_done:
            qp["first"] = 1
        return redirect(url_for("sync_processing", **qp))

    if res.get("github_no_changes"):
        # Nothing changed since the last sync — broker returned the same
        # holdings/trades, so the seed is byte-identical and we deliberately
        # skipped the commit (no dbt build needed). Tell the user honestly
        # instead of pretending a rebuild is in flight.
        flash(
            f"{summary} Nothing has changed since your last sync, so there's "
            f"nothing new to process.",
            "info",
        )
        return redirect(url_for("snaptrade_accounts_page"))

    if res["github_error"]:
        flash(f"{summary} Couldn't push to the cloud: {res['github_error']}", "warning")
    elif res["github_seed_push_skipped"]:
        flash(
            f"{summary} Live dashboard updates are not turned on for this "
            f"environment.",
            "info",
        )
    return redirect(url_for("snaptrade_accounts_page"))


@app.route("/snaptrade/refresh-broker", methods=["POST"])
@login_required
def snaptrade_refresh_broker():
    """User-initiated "Refresh from broker".

    Kept as a backward-compatible alias now that **every** "Sync now" forces
    a broker repoll (see ``_sync_one_connection(force_refresh=True)``). Both
    buttons run the identical path; the per-authorization throttle inside
    ``_force_refresh_brokerage`` means pressing both within the window won't
    double-charge.

    Form fields:
        snaptrade_account_id (required): which brokerage to refresh.
        full_history_again ("1"): forwarded to the sync.
    """
    blocked = demo_block_writes("refreshing brokerage data")
    if blocked:
        return blocked
    if not _snaptrade_config():
        flash("Multi-broker connect is not configured.", "danger")
        return redirect(url_for("profile", tab="account"))

    snaptrade_account_id = (request.form.get("snaptrade_account_id") or "").strip()
    if not snaptrade_account_id:
        flash("Missing account id.", "warning")
        return redirect(url_for("snaptrade_accounts_page"))

    acc_row = get_snaptrade_account(current_user.id, snaptrade_account_id)
    if not acc_row:
        flash("Couldn't find that brokerage connection on your account.", "warning")
        return redirect(url_for("snaptrade_accounts_page"))

    force_full = request.form.get("full_history_again") == "1"
    first_done = bool(acc_row.get("first_sync_completed"))
    lookback_days = _bulk_sync_lookback_days(
        first_done,
        force_full_history=force_full,
        routine_days=_routine_lookback_days(),
        full_days=SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS,
    )
    res = _sync_one_connection(
        current_user.id, acc_row, lookback_days=lookback_days, force_refresh=True,
    )
    return _flash_and_redirect_after_sync(res, first_done=first_done, refreshed=True)


# ---------------------------------------------------------------------------
# Routes — sync
# ---------------------------------------------------------------------------

@app.route("/snaptrade/sync", methods=["POST"])
@login_required
def snaptrade_sync():
    """Sync one or all SnapTrade-managed broker accounts.

    Form fields:
        sync_all: "1" to sync every linked SnapTrade account.
        snaptrade_account_id (optional, ignored when ``sync_all=1``).
        full_history_again: "1" to force the full ~5-year window for
            every row regardless of first-sync state.
    """
    blocked = demo_block_writes("syncing brokerage data")
    if blocked:
        return blocked
    if not _snaptrade_config():
        flash("Multi-broker connect is not configured.", "danger")
        return redirect(url_for("profile", tab="account"))

    sync_all = request.form.get("sync_all") == "1"
    force_full = request.form.get("full_history_again") == "1"

    if sync_all:
        return _sync_all_for_user(current_user.id, force_full_history=force_full)

    snaptrade_account_id = (request.form.get("snaptrade_account_id") or "").strip()
    if not snaptrade_account_id:
        flash("Missing account id.", "warning")
        return redirect(url_for("snaptrade_accounts_page"))

    acc_row = get_snaptrade_account(current_user.id, snaptrade_account_id)
    if not acc_row:
        flash("Couldn't find that brokerage connection on your account.", "warning")
        return redirect(url_for("snaptrade_accounts_page"))

    first_done = bool(acc_row.get("first_sync_completed"))
    lookback_days = _bulk_sync_lookback_days(
        first_done,
        force_full_history=force_full,
        routine_days=_routine_lookback_days(),
        full_days=SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS,
    )
    # "Sync now" forces a broker repoll — a sync that only re-reads SnapTrade's
    # cache is pointless when the cache is stale (the whole point of syncing is
    # to get fresh data). Throttled + non-fatal inside _sync_one_connection.
    res = _sync_one_connection(
        current_user.id, acc_row, lookback_days=lookback_days, force_refresh=True,
    )
    return _flash_and_redirect_after_sync(res, first_done=first_done, refreshed=True)


# ---------------------------------------------------------------------------
# Sync orchestration
# ---------------------------------------------------------------------------

def _market_closed_all_day(now=None):
    """True when the US equity market is closed for the ENTIRE current ET day
    (i.e. a weekend).

    Used to make the webhook auto-sync HISTORY-ONLY on days with no live
    session: broker marks/balances drift on every read, and pushing that drift
    triggers a full dbt build for ZERO trade activity. On a weekend we still
    ingest new fills (Friday's T+1 activities post Saturday) but skip the
    snapshot rewrite. Weekday holidays are intentionally NOT special-cased — a
    holiday just does one full sync that mostly no-ops; not worth a calendar.
    """
    from zoneinfo import ZoneInfo

    et = ZoneInfo("America/New_York")
    now_et = now or datetime.now(et)
    if now_et.tzinfo is None:
        now_et = now_et.replace(tzinfo=et)
    now_et = now_et.astimezone(et)
    return now_et.weekday() >= 5


def _sync_one_connection(user_id, acc_row, *, lookback_days, force_refresh=False, defer_push=False,
                         skip_activities=False, history_only=False):
    """Sync ONE SnapTrade-managed broker account end-to-end.

    Returns a structured dict like ``_sync_one_connection`` in
    app.schwab. Never raises — failures land as
    ``{"ok": False, "error": "...", ...}`` so multi-account loops
    survive one bad row.

    ``force_refresh`` — when True, ask SnapTrade to repoll the broker
    (``refresh_brokerage_authorization``) and wait a short settle window
    BEFORE reading holdings. This is what an interactive "Sync now" wants:
    a sync that only re-reads SnapTrade's cache is pointless if the cache
    is stale (June 2026: user_id=9 frozen on a 7-day-old cache while every
    sync "succeeded"). The refresh is BILLED PER CALL and THROTTLED per
    authorization (see ``SNAPTRADE_FORCE_REFRESH_THROTTLE_SECONDS``), so a
    rapid double-click won't double-charge, and a throttled/failed refresh
    is NON-FATAL — we fall through to the normal read (same outcome as
    before this feature). The daily cron leaves this False on purpose:
    SnapTrade already auto-refreshes connections on its own cadence, and
    forcing a (billed, async) refresh per connection per night would
    multiply cost for data that lands too late for that same run anyway —
    stalled connections are caught instead by the holdings-freshness
    backstop in ``_run_sync``.

    ``skip_activities`` — pass True for the INTRADAY POLL (read the real-time
    ``recent_orders`` feed only, skip the T+1 ``activities`` feed). See
    ``_run_sync`` for the full rationale.

    ``history_only`` — pass True to push only new trade fills and leave the
    positions/balances snapshots untouched (the WEEKEND auto-sync uses this to
    avoid rebuilding the warehouse on snapshot drift while markets are closed,
    yet still ingest Friday's T+1 fills). See ``_run_sync``.
    """
    snaptrade_account_id = acc_row["snaptrade_account_id"]
    label = (
        acc_row.get("display_nickname")
        or acc_row.get("account_name")
        or snaptrade_account_id
    )
    out = {
        "ok": False,
        "label": label,
        "snaptrade_account_id": snaptrade_account_id,
        "first_done_before": bool(acc_row.get("first_sync_completed")),
        "history_rows": 0,
        "current_rows": 0,
        "lookback_days": lookback_days,
        "github_pushed": False,
        "github_head_sha": None,
        "github_error": None,
        "github_seed_push_skipped": False,
        "github_skip_reason": None,
        "github_no_changes": False,
        "error": None,
    }

    snap = get_snaptrade_user(user_id)
    client = _get_snaptrade_client()
    if not snap or not client:
        out["error"] = "session_expired"
        record_snaptrade_sync_attempt(
            user_id, snaptrade_account_id, error="session_expired",
        )
        return out

    # Interactive "Sync now" / "Refresh from broker": ask SnapTrade to repoll
    # the broker first, then give it a short head start before reading. A sync
    # that only re-reads SnapTrade's cache is worthless when the cache is
    # stale. Non-fatal: a throttled/failed refresh falls through to the normal
    # read (same outcome as before this feature). The per-authorization
    # throttle inside _force_refresh_brokerage protects us from double-billing
    # on rapid clicks.
    if force_refresh:
        try:
            ok_r, msg_r, _rem = _force_refresh_brokerage(user_id, snaptrade_account_id)
            app.logger.info(
                "SnapTrade force-refresh before sync user_id=%s account=%s: ok=%s (%s)",
                user_id, snaptrade_account_id, ok_r, msg_r,
            )
            if ok_r:
                import time
                time.sleep(SNAPTRADE_FORCE_REFRESH_SETTLE_SECONDS)
        except Exception as _exc:
            app.logger.warning(
                "SnapTrade force-refresh raised (non-fatal) user_id=%s account=%s: %s",
                user_id, snaptrade_account_id, _exc,
            )

    try:
        result = _run_sync(
            user_id,
            client,
            snap=snap,
            acc_row=acc_row,
            lookback_days=lookback_days,
            defer_push=defer_push,
            skip_activities=skip_activities,
            history_only=history_only,
        )
        mark_snaptrade_first_sync_completed(user_id, snaptrade_account_id)
        clear_snaptrade_connection_broken(user_id, snaptrade_account_id)
        record_snaptrade_sync_attempt(user_id, snaptrade_account_id, error=None)
        # Persist SnapTrade's honest "broker data as of" timestamp (best-effort;
        # None is a no-op so a missing signal never clobbers a good value).
        record_snaptrade_holdings_sync(
            user_id, snaptrade_account_id,
            result.get("holdings_last_successful_sync"),
        )
        # Append a per-run observation row (CLOSE-BASED REPORTING Phase 3):
        # full history of how late after the close SnapTrade's
        # holdings_last_successful_sync actually advances, so we can retime
        # the cron precisely. Best-effort — never breaks a successful sync.
        record_snaptrade_sync_observation(
            user_id, snaptrade_account_id,
            broker_slug=acc_row.get("broker_slug"),
            holdings_last_successful_sync=result.get("holdings_last_successful_sync"),
            ok=True,
        )
        out.update({
            "ok": True,
            "history_rows": int(result.get("history_rows", 0) or 0),
            "current_rows": int(result.get("current_rows", 0) or 0),
            "lookback_days": int(result.get("lookback_days", lookback_days) or lookback_days),
            "github_pushed": bool(result.get("github_pushed")),
            "github_head_sha": (result.get("github_head_sha") or None),
            "github_error": result.get("github_error"),
            "github_seed_push_skipped": bool(result.get("github_seed_push_skipped")),
            "github_skip_reason": result.get("github_skip_reason"),
            "github_no_changes": bool(result.get("github_no_changes")),
        })
        # Deferred-push mode: carry the normalized frames back so the batch
        # caller (nightly cron) can merge every account into ONE commit. The
        # per-account push has NOT happened yet (github_pushed stays False).
        if result.get("deferred"):
            out["deferred"] = True
            out["frames"] = {
                "account_name": result.get("account_name"),
                "tenant_id": result.get("tenant_id"),
                "history_df": result.get("history_df"),
                "current_df": result.get("current_df"),
                "balances_df": result.get("balances_df"),
                "skip_history": bool(result.get("skip_history")),
                "user_id": user_id,
            }
        # First-activation nudge: once data has actually landed for this user,
        # email "your data is ready" exactly once (dedupe per user via
        # email_sends). Best-effort — never let email break a sync.
        try:
            if out["history_rows"] or out["current_rows"]:
                from app.models import record_email_send
                from app.email import send_data_ready_email, app_base_url
                u = User.get_by_id(user_id)
                if u and (u.email or "").strip() and record_email_send(
                    "data_ready", str(user_id), user_id=user_id, to_email=u.email
                ):
                    send_data_ready_email(
                        to=u.email,
                        username=u.username,
                        dashboard_url=f"{app_base_url()}/daily-review",
                    )
        except Exception as _exc:
            from app import app as _app
            _app.logger.warning("data_ready email skipped for user_id=%s: %s", user_id, _exc)
    except _SnapTradeAuthError as auth_exc:
        # Log the SDK exception we classified as auth-revoked BEFORE
        # flagging the row, so admin debugging of "why is this brand-new
        # connection flagged broken" is a one-line grep rather than
        # silent state. See broker-sync-safety SKILL.md (first-Fidelity-
        # connection misclassification, May 2026): the `_looks_like_auth_error`
        # heuristic catches any "401"/"403" substring, which is too broad
        # for Fidelity's first-sync handshake window where SnapTrade
        # sometimes returns 4xx for non-revocation reasons.
        from app import app as _app
        orig = getattr(auth_exc, "original", auth_exc)
        endpoint = getattr(auth_exc, "endpoint", "unknown")
        broker_slug = acc_row.get("broker_slug") or "unknown"
        first_done = bool(acc_row.get("first_sync_completed"))
        _app.logger.warning(
            "SnapTrade sync flagged connection_broken for user_id=%s "
            "account=%s broker=%s first_done=%s endpoint=%s exc_type=%s msg=%s",
            user_id, snaptrade_account_id, broker_slug, first_done,
            endpoint, type(orig).__name__, str(orig)[:500],
        )
        mark_snaptrade_connection_broken(user_id, snaptrade_account_id)
        record_snaptrade_sync_attempt(
            user_id, snaptrade_account_id, error=f"connection_broken:{endpoint}",
        )
        record_snaptrade_sync_observation(
            user_id, snaptrade_account_id,
            broker_slug=acc_row.get("broker_slug"), ok=False,
        )
        out["error"] = "connection_broken"
    except Exception as exc:
        from app import app as _app
        _app.logger.exception(
            "SnapTrade sync failed for user_id=%s account=%s: %s",
            user_id, snaptrade_account_id, exc,
        )
        record_snaptrade_sync_attempt(
            user_id, snaptrade_account_id, error=str(exc)[:500],
        )
        record_snaptrade_sync_observation(
            user_id, snaptrade_account_id,
            broker_slug=acc_row.get("broker_slug"), ok=False,
        )
        out["error"] = "unknown"
    return out


def _sync_all_for_user(user_id, *, force_full_history=False):
    """Iterate every SnapTrade account for ``user_id`` and sync each.

    Returns a Flask redirect. Mirrors ``_schwab_sync_all_for_user``
    exactly — same per-row lookback decision, same redirect to
    ``sync_processing`` when at least one push lands, same flash
    summary phrasing.
    """
    rows = get_snaptrade_accounts(user_id) or []
    if not rows:
        flash("No brokerage accounts connected via SnapTrade yet.", "warning")
        return redirect(url_for("snaptrade_accounts_page"))

    routine_days = _routine_lookback_days()
    full_days = SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS
    successes = []
    failures = []
    last_pushed_sha = None
    any_first_run = False

    # "Sync All" forces a broker repoll like single-account Sync now. Fire all
    # refreshes UP FRONT and sleep ONCE afterward so the bulk request adds a
    # single settle window, not one per account (a 5-account user would
    # otherwise tie up a worker for 5 × the settle window). Each refresh is
    # throttled + non-fatal; the per-account syncs below then read the freshly
    # repolled snapshot with force_refresh=False (no further per-account wait).
    refreshed_any = False
    for acc_row in rows:
        try:
            ok_r, _msg_r, _rem = _force_refresh_brokerage(
                user_id, acc_row["snaptrade_account_id"],
            )
            refreshed_any = refreshed_any or bool(ok_r)
        except Exception as _exc:
            app.logger.warning(
                "SnapTrade bulk force-refresh raised (non-fatal) user_id=%s "
                "account=%s: %s",
                user_id, acc_row.get("snaptrade_account_id"), _exc,
            )
    if refreshed_any:
        import time
        time.sleep(SNAPTRADE_FORCE_REFRESH_SETTLE_SECONDS)

    for acc_row in rows:
        first_done = bool(acc_row.get("first_sync_completed"))
        if not first_done:
            any_first_run = True
        lookback = _bulk_sync_lookback_days(
            first_done,
            force_full_history=force_full_history,
            routine_days=routine_days,
            full_days=full_days,
        )
        res = _sync_one_connection(user_id, acc_row, lookback_days=lookback)
        if res["ok"]:
            successes.append(res)
            if res["github_pushed"] and res["github_head_sha"]:
                last_pushed_sha = res["github_head_sha"]
        else:
            failures.append({"label": res["label"], "reason": res["error"] or "unknown"})

    parts = []
    history_pending_accounts = []  # accounts where positions came through but trades didn't
    if successes:
        per_account = ", ".join(
            f"{s['label']}: {s['history_rows']:,} {'trade' if s['history_rows'] == 1 else 'trades'}, "
            f"{s['current_rows']} open {'position' if s['current_rows'] == 1 else 'positions'}"
            for s in successes
        )
        parts.append(f"Synced {len(successes)} broker account(s) — {per_account}.")
        # Detect the rare case where SnapTrade has positions but no
        # trades from EITHER source (activities or orders). Now that
        # we read both endpoints, ``history_rows == 0`` means SnapTrade
        # hasn't ingested ANY trade signal yet — either a brand-new
        # connection still being backfilled, or an account that
        # genuinely has zero recent trades. Tell the user honestly
        # rather than letting them stare at an empty positions page.
        history_pending_accounts = [
            s["label"] for s in successes
            if int(s.get("history_rows") or 0) == 0
            and int(s.get("current_rows") or 0) > 0
        ]
        # If every successful account was a no-op (broker returned identical
        # data), say so — no commit, no dbt build. Avoids implying a rebuild
        # is in flight when nothing changed.
        if not last_pushed_sha and all(
            s.get("github_no_changes") for s in successes
        ):
            parts.append(
                "Nothing has changed since your last sync, so there's nothing "
                "new to process."
            )
    if failures:
        per_failure = ", ".join(f"{f['label']} ({f['reason']})" for f in failures)
        parts.append(f"Failed: {per_failure}.")
    if history_pending_accounts:
        parts.append(
            f"Heads up: SnapTrade has the current positions for "
            f"{', '.join(history_pending_accounts)} but no trade history "
            f"yet — common on a brand-new connection while SnapTrade "
            f"backfills the broker's transaction archive. Re-sync in 30 "
            f"minutes; if it's still empty after a few hours your account "
            f"may genuinely have no recent trades on file."
        )
    summary = " ".join(parts) or "Nothing to sync."

    if last_pushed_sha:
        flash(
            f"{summary} We're processing your data now — refresh in a minute.",
            "success" if not failures else "warning",
        )
        qp = {"sha": last_pushed_sha}
        if any_first_run:
            qp["first"] = 1
        return redirect(url_for("sync_processing", **qp))

    if failures and not successes:
        flash(summary, "danger")
    elif failures:
        flash(summary, "warning")
    else:
        flash(summary, "info")
    return redirect(url_for("snaptrade_accounts_page"))


# ---------------------------------------------------------------------------
# The sync itself — broker fetches, normalize, push
# ---------------------------------------------------------------------------

class _SnapTradeAuthError(RuntimeError):
    """Raised when SnapTrade reports the broker connection is broken
    (user revoked access at the broker, MFA expired, etc). Caught by
    ``_sync_one_connection`` to flag the account for reconnection.

    Carries ``endpoint`` (which SnapTrade call failed) and the original
    exception so the caller can log a single line that pinpoints which
    surface SnapTrade rejected. Without this, the silent flagging of
    ``connection_broken_at`` was a debugging black box — see
    broker-sync-safety SKILL.md note on first-Fidelity-connection
    misclassification (May 2026)."""

    def __init__(self, endpoint: str, exc: Exception):
        self.endpoint = endpoint
        self.original = exc
        super().__init__(f"{endpoint}: {exc}")


def _brokerage_authorization_disabled(client, snap, acc_row, *, user_id):
    """Authoritative health check: has SnapTrade DISABLED this brokerage
    authorization (broker requires reconnection)?

    Returns:
      ``True``  — SnapTrade explicitly reports the authorization disabled.
      ``False`` — SnapTrade reports it enabled.
      ``None``  — couldn't determine (no auth id, API error, unexpected
                  shape). ``None`` means "change nothing", so a transient
                  metadata blip never false-flags a healthy connection.

    Why this exists: a disabled SnapTrade connection keeps serving the
    LAST-CACHED positions/balances, so the authoritative fetch helpers in
    ``_run_sync`` succeed with stale data and the row silently freezes
    (real case June 2026: ``user_id=9`` Schwab accounts stuck on a June 8
    snapshot while ``connection_status`` still read 'active' and no
    reconnect banner ever fired). The brokerage authorization's own
    ``disabled`` boolean is the ONLY reliable signal that separates "live"
    from "serving stale cache". We deliberately do NOT infer this from the
    recent-orders 402/403 — that endpoint's permissions are broker-specific
    and a prior fix removed it from auth classification (see
    broker-sync-safety SKILL.md, first-Fidelity misclassification).
    """
    snap_user_id = snap.get("snaptrade_user_id") if snap else None
    snap_secret = snap.get("snaptrade_secret") if snap else None
    snaptrade_account_id = acc_row.get("snaptrade_account_id")
    if not (snap_user_id and snap_secret):
        return None

    auth_id = (acc_row.get("brokerage_authorization_id") or "").strip()
    # Resolve + cache the auth id on first use (mirrors
    # _force_refresh_brokerage) so later runs skip this round-trip.
    if not auth_id and snaptrade_account_id:
        try:
            detail = client.account_information.get_user_account_details(
                user_id=snap_user_id,
                user_secret=snap_secret,
                account_id=snaptrade_account_id,
            )
            body = _unwrap_body(detail)
            if isinstance(body, dict):
                auth_id = (body.get("brokerage_authorization") or "").strip()
                if auth_id:
                    try:
                        set_snaptrade_brokerage_authorization_id(
                            user_id, snaptrade_account_id, auth_id,
                        )
                    except Exception:
                        pass
        except Exception:
            return None
    if not auth_id:
        return None

    try:
        auth_resp = client.connections.list_brokerage_authorizations(
            user_id=snap_user_id,
            user_secret=snap_secret,
        )
    except Exception:
        return None
    auth_body = _unwrap_body(auth_resp)
    if isinstance(auth_body, dict):
        auths = auth_body.get("authorizations", []) or []
    elif isinstance(auth_body, list):
        auths = auth_body
    else:
        return None
    for auth in auths:
        if not isinstance(auth, dict):
            try:
                auth = auth.to_dict() if hasattr(auth, "to_dict") else {}
            except Exception:
                continue
        if (auth.get("id") or "").strip() == auth_id:
            return bool(auth.get("disabled"))
    return None


def _run_sync(user_id, client, *, snap, acc_row, lookback_days, defer_push=False,
              skip_activities=False, history_only=False):
    """Pull activities + positions + balances from SnapTrade,
    normalize, push to GitHub seeds.

    Mirrors ``app.schwab._run_sync`` shape so the orchestration layer
    can treat the two connectors identically.

    ``defer_push`` — when True, do everything EXCEPT the GitHub commit:
    return the normalized frames (``history_df`` / ``current_df`` /
    ``balances_df`` + tenant metadata) so a multi-account caller (the nightly
    backstop cron) can merge every account and push ONE commit instead of one
    per account. The single-account webhook / Sync-now paths leave this False
    and push inline as before.

    ``skip_activities`` — when True, DON'T read the ``activities`` feed; use
    only the real-time ``recent_orders`` feed for trade history (positions +
    balances still read as normal). This is the INTRADAY POLL path. Rationale
    (SnapTrade support 2026-07-10): ``activities`` are T+1 for every broker —
    reading them every few minutes is wasted work and never carries today's
    fill. ``recent_orders`` IS real-time on read even for brokers whose
    background holdings poll (and thus the ACCOUNT_HOLDINGS_UPDATED webhook)
    lags — proven live for a Schwab account whose ``holdings_last_successful_
    sync`` was ~19h stale yet ``recent_orders`` returned the just-closed
    contracts. So the intraday poll reads orders to surface same-day trades
    without waiting on the daily Schwab holdings webhook; the 23:00 backstop
    and webhook syncs (which DO read activities) reconcile the authoritative
    copy overnight. The seed merge is monotonic + cross-source-deduped, so a
    later activities row for the same fill collapses onto the order row.

    ``history_only`` — when True, push ONLY new trade fills (trade_history);
    NEVER rewrite the positions/balances snapshots. Same push shape as the
    intraday poll, but DECOUPLED from ``skip_activities`` so a caller can still
    READ activities (e.g. the WEEKEND auto-sync, which must catch Friday's T+1
    fills that post Saturday) while suppressing the snapshot churn that would
    otherwise trigger a full dbt build for nothing but drifting marks. A sync
    with no new fills then becomes a true no-op. (``skip_activities`` implies
    ``history_only`` — the intraday poll wants both.)
    """
    snaptrade_account_id = acc_row["snaptrade_account_id"]
    account_name = acc_row["account_name"]
    snap_user_id = snap["snaptrade_user_id"]
    snap_secret = snap["snaptrade_secret"]

    # v2 tenancy: resolve tenant_id BEFORE any external API calls so a
    # SnapTrade error doesn't leave us without a stable tenant key.
    tenant_id = _ensure_snaptrade_tenant_id(
        user_id=user_id,
        snaptrade_account_id=snaptrade_account_id,
        account_name=account_name,
        snaptrade_connection_id=acc_row.get("brokerage_authorization_id"),
    )

    # AUTHORITATIVE disabled-connection gate — run FIRST, every sync.
    # A disabled SnapTrade connection keeps serving the LAST-CACHED
    # everything: not just positions/balances but also the historical
    # activities/orders inside the lookback window. So row counts can NEVER
    # distinguish "live" from "serving stale cache" (real case June 2026:
    # user_id=9 frozen on a June 12 balance while re-returning 45 cached
    # transactions every sync — the same physical accounts under other
    # logins advanced to June 18). The only reliable signal is the brokerage
    # authorization's own ``disabled`` flag. Checking it up front (one cheap
    # metadata read — NOT the billed refresh endpoint) also skips the wasted
    # fetch/normalize/push of stale rows. We deliberately do NOT infer this
    # from the orders-endpoint 402/403 (broker-specific; see
    # broker-sync-safety SKILL.md first-Fidelity misclassification).
    if _brokerage_authorization_disabled(client, snap, acc_row, user_id=user_id) is True:
        raise _SnapTradeAuthError(
            "connections.list_brokerage_authorizations[disabled=true]",
            RuntimeError(
                "SnapTrade reports this brokerage authorization is disabled; "
                "it is serving stale cached holdings until the user reconnects."
            ),
        )

    end_date = date.today()
    start_date = end_date - timedelta(days=int(lookback_days))

    # Intraday poll skips the T+1 activities feed (never carries today's fill)
    # and leans on the real-time recent_orders read instead. See docstring.
    if skip_activities:
        activities = []
    else:
        activities = _fetch_activities(client, snap_user_id, snap_secret, snaptrade_account_id, start_date, end_date)
    orders = _fetch_recent_orders(client, snap_user_id, snap_secret, snaptrade_account_id)
    positions = _fetch_positions(client, snap_user_id, snap_secret, snaptrade_account_id)
    option_holdings = _fetch_option_holdings(client, snap_user_id, snap_secret, snaptrade_account_id)
    balances = _fetch_balances(client, snap_user_id, snap_secret, snaptrade_account_id)
    account_summary = _fetch_account_summary(client, snap_user_id, snap_secret, snaptrade_account_id)

    # BACKSTOP for the "enabled-but-stalled" failure mode the disabled flag
    # misses. SnapTrade can keep returning HTTP 200 from its last-cached
    # holdings while its OWN ``sync_status.holdings.last_successful_sync``
    # stops advancing (broker auth degraded but not yet flagged disabled).
    # Every fetch above then "succeeds" with byte-identical stale rows, so row
    # counts can't tell live from frozen and the seed merge re-pushes a no-op
    # forever. The authoritative freshness signal is SnapTrade's per-account
    # last_successful_sync timestamp; if it's older than the threshold,
    # escalate to the SAME reconnect path as a disabled connection (real case
    # June 2026: user_id=9 Schwab holdings frozen on a June-12 cache for 7
    # days, no banner). Threshold <= 0 disables the backstop. None-is-safe:
    # a missing/odd timestamp never flags. Runs only AFTER the disabled gate
    # (which raises first when disabled is True), so this catches the
    # complementary case the disabled flag silently passes.
    if SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS > 0:
        stale_days = _holdings_stale_days(account_summary, today=end_date)
        if stale_days is not None and stale_days >= SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS:
            raise _SnapTradeAuthError(
                "sync_status.holdings.last_successful_sync[stale]",
                RuntimeError(
                    f"SnapTrade has not refreshed holdings from the broker in "
                    f"{stale_days} days (threshold "
                    f"{SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS}); it is serving a "
                    f"stale cached snapshot until the user reconnects or "
                    f"forces a Refresh from broker."
                ),
            )

    # SnapTrade has TWO sources for trade history:
    # - ``activities``: authoritative; includes dividends/fees/splits
    #   in addition to fills. Lags hours-to-days behind the broker.
    # - ``recent_orders``: real-time-ish; only carries equity/option
    #   fills, no dividends or fees. Fresh within seconds of execution.
    # Read both and merge. The cross-source dedup in
    # ``app.upload._dedup_history_rows`` collapses overlapping rows by
    # (Date, Action, Symbol, Quantity, Price, Amount) and keeps the
    # richer-description row (activities wins on tie-break). See
    # https://docs.snaptrade.com/demo/get-transactions for the lag
    # acknowledgment. Real bug repro: 2026-05-14 NVDA trade visible in
    # orders 2 min after execution but absent from activities for hours.
    activities_df = activities_to_history_df(
        activities, account_name=account_name, user_id=user_id,
        tenant_id=tenant_id,
    )
    orders_df = orders_to_history_df(
        orders, account_name=account_name, user_id=user_id,
        tenant_id=tenant_id,
    )
    import pandas as pd
    # Concat only the non-empty frames — pandas deprecates concatenating
    # empty/all-NA frames in a future version, and during the lag window
    # one or both of these will routinely be empty.
    non_empty = [df for df in (activities_df, orders_df) if not df.empty]
    if not non_empty:
        history_df = activities_df  # canonical empty frame with columns
    elif len(non_empty) == 1:
        history_df = non_empty[0]
    else:
        history_df = pd.concat(non_empty, ignore_index=True)

    # Surface the lag situation in the log so admin debugging is easy.
    # Helpful taxonomy:
    #   - activities=0, orders=0, positions>0  → SnapTrade hasn't
    #     ingested anything yet for this account. New connection.
    #   - activities=0, orders>N, positions>0  → Orders endpoint is
    #     carrying the truth; activities is still indexing. We've now
    #     captured the trades via orders source so user sees them.
    #   - activities>0, orders>N (overlap)     → Both flowed; dedup
    #     should collapse identical fills. Report the de-duped count
    #     downstream so the user-facing summary is honest.
    if not activities and not orders and positions:
        app.logger.warning(
            "SnapTrade sync: 0 activities AND 0 orders but %d positions "
            "for account=%s (snaptrade_id=%s). SnapTrade hasn't ingested "
            "trade history yet — sync again in 30-60 min.",
            len(positions), account_name, snaptrade_account_id,
        )
    elif not activities and orders:
        app.logger.info(
            "SnapTrade sync: activities=0 but orders=%d for account=%s "
            "(snaptrade_id=%s, lookback=%d days). Using orders endpoint "
            "as fresh-trade fallback — activities will catch up later.",
            len(orders), account_name, snaptrade_account_id, int(lookback_days),
        )
    # Equity/ETF positions + open option holdings share one normalizer
    # (positions_to_current_df classifies each row via _is_option). Option
    # holdings come from a SEPARATE SnapTrade endpoint — see
    # _fetch_option_holdings — so an open option leg lands in the snapshot
    # instead of only the trade-history feed.
    current_df = positions_to_current_df(
        list(positions or []) + list(option_holdings or []),
        account_name=account_name, user_id=user_id,
        tenant_id=tenant_id,
    )
    balances_df = balances_to_balance_df(
        account_summary=account_summary,
        balances=balances,
        positions=positions,
        account_name=account_name,
        user_id=user_id,
        tenant_id=tenant_id,
    )

    skip_history = history_df is None or history_df.empty

    # History-only push: only new trade fills go to trade_history; the
    # positions/balances snapshots are NOT rewritten. skip_activities (intraday
    # poll) always implies this; the weekend auto-sync sets history_only
    # directly while still reading activities for Friday's T+1 fills.
    push_history_only = bool(history_only or skip_activities)

    # Deferred-push mode (nightly batch cron): hand the normalized frames back
    # to the caller instead of committing, so many accounts collapse into ONE
    # push. Everything above (fetch, staleness backstop, normalize) already ran.
    if defer_push:
        # INTRADAY POLL is TRADE-DETECTION ONLY. It pushes just new trade
        # fills (trade_history) and NEVER the positions/balances snapshots,
        # because those snapshots drift on EVERY read (intraday broker marks +
        # balances move constantly). Pushing them on a ~15-min cadence would
        # rewrite current_positions.csv / account_balances.csv every run → a
        # full dbt build every 15 minutes even when nothing traded. Snapshot
        # freshness is owned by the webhook syncs + the 23:00 backstop; daily
        # valuation of held positions is owned by the evening price refresh
        # (prices_refresh.yml). So a poll with no new fills is a true no-op
        # (empty history → skip_history → nothing appended → no commit/build).
        return {
            "deferred": True,
            "account_name": account_name,
            "tenant_id": tenant_id,
            "history_df": None if skip_history else history_df,
            "current_df": None if push_history_only else current_df,
            "balances_df": None if push_history_only else balances_df,
            "push_history_only": push_history_only,
            "skip_history": skip_history,
            "history_rows": 0 if skip_history else len(history_df),
            "current_rows": len(current_df),
            "lookback_days": int(lookback_days),
            "github_pushed": False,
            "github_error": None,
            "github_head_sha": None,
            "github_seed_push_skipped": False,
            "github_skip_reason": None,
            "github_no_changes": False,
            "holdings_last_successful_sync": _holdings_last_successful_sync_dt(account_summary),
        }

    github_pushed = False
    github_error = None
    github_head_sha = None
    github_skip_reason = None
    github_no_changes = False

    from app.upload import _upload_github_config_ok, merge_and_push_seeds

    ok_cfg, cfg_err = _upload_github_config_ok()
    if not ok_cfg:
        github_skip_reason = cfg_err or "GitHub seed push not configured."
        app.logger.warning(
            "SnapTrade sync: skipping GitHub push for user_id=%s account=%s — %s",
            user_id, account_name, github_skip_reason,
        )
    elif push_history_only and skip_history:
        # History-only push (weekend auto-sync / intraday) with NO new fills:
        # nothing to push. Skip the merge entirely so drifting snapshot marks
        # can't trigger a full dbt build for zero trade activity.
        github_skip_reason = "history_only_no_new_trades"
        app.logger.info(
            "SnapTrade sync (history-only, no new trades): no push for "
            "user_id=%s account=%s", user_id, account_name,
        )
    else:
        uname = "user"
        u = User.get_by_id(user_id)
        if u:
            uname = u.username
        if push_history_only:
            # Only new trade fills are pushed; snapshots left untouched.
            commit_msg = (
                f"SnapTrade sync ({uname}): {len(history_df)} tx "
                f"(orders only) ({account_name})"
            )
        elif skip_history:
            commit_msg = (
                f"SnapTrade sync ({uname}): positions only "
                f"({len(current_df)} lines) ({account_name})"
            )
        else:
            commit_msg = (
                f"SnapTrade sync ({uname}): {len(history_df)} tx, "
                f"{len(current_df)} open lines ({account_name})"
            )
        ok, err, _hr, _cr, github_head_sha, github_no_changes = merge_and_push_seeds(
            account_name,
            history_df,
            None if push_history_only else current_df,
            commit_message=commit_msg,
            user_id=user_id,
            tenant_id=tenant_id,
            skip_history=skip_history,
            balances_df=None if push_history_only else balances_df,
        )
        # "Pushed" means a commit (and therefore a dbt build) actually
        # happened. A no-op merge (identical seed) reports ok=True but pushes
        # nothing — callers must NOT send the user to a processing page for a
        # build that will never run.
        github_pushed = ok and not github_no_changes
        github_error = err if not ok else None
        if github_no_changes:
            github_skip_reason = "no_changes"

    return {
        "history_rows": 0 if skip_history else len(history_df),
        "current_rows": len(current_df),
        "lookback_days": int(lookback_days),
        "github_pushed": github_pushed,
        "github_error": github_error,
        "github_head_sha": github_head_sha,
        "github_seed_push_skipped": not ok_cfg,
        "github_skip_reason": github_skip_reason,
        "github_no_changes": bool(github_no_changes),
        # Honest "broker data as of" — SnapTrade's OWN holdings sync timestamp
        # (NOT when our cron read the cache). We already fetched account_summary
        # for the staleness backstop above; surface it here so the caller can
        # persist it for the freshness badge.
        "holdings_last_successful_sync": _holdings_last_successful_sync_dt(account_summary),
    }


def _fetch_activities(client, snap_user_id, snap_secret, account_id, start_date, end_date):
    """Pull SnapTrade activities for one account, paginating through
    SnapTrade's default 1000-row page size. Defensive on response
    shape (list vs paginated-object-with-data)."""
    activities = []
    offset = 0
    page_size = 1000
    while True:
        try:
            resp = client.account_information.get_account_activities(
                user_id=snap_user_id,
                user_secret=snap_secret,
                account_id=account_id,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                offset=offset,
                limit=page_size,
            )
        except Exception as exc:
            if _looks_like_auth_error(exc):
                raise _SnapTradeAuthError("get_account_activities", exc)
            raise
        page = _coerce_paginated_data(resp)
        if not page:
            break
        activities.extend(page)
        if len(page) < page_size:
            break
        offset += len(page)
        # Safety bound — SnapTrade's hard cap is unstated, but if a
        # broker ever returns a runaway feed, refuse to keep paging.
        if offset > 100_000:
            break
    return activities


def _coerce_paginated_data(resp):
    """SnapTrade activities use ``PaginatedUniversalActivity`` which
    wraps the page array under ``data`` inside the response body. Some
    SDK versions surface a top-level list. Handle both shapes."""
    body = _unwrap_body(resp)
    # paginated dict shape: { "data": [...], "pagination": {...} }
    if isinstance(body, dict) and isinstance(body.get("data"), list):
        items = body["data"]
    elif isinstance(body, list):
        items = body
    elif hasattr(body, "data") and isinstance(getattr(body, "data"), list):
        items = body.data
    else:
        items = []
    out = []
    for item in items:
        if isinstance(item, dict):
            out.append(item)
        elif hasattr(item, "to_dict"):
            try:
                out.append(item.to_dict())
            except Exception:
                out.append({})
    return out


def _fetch_positions(client, snap_user_id, snap_secret, account_id):
    try:
        resp = client.account_information.get_user_account_positions(
            user_id=snap_user_id,
            user_secret=snap_secret,
            account_id=account_id,
        )
    except Exception as exc:
        if _looks_like_auth_error(exc):
            raise _SnapTradeAuthError("get_user_account_positions", exc)
        raise
    return _coerce_list(resp)


def _fetch_option_holdings(client, snap_user_id, snap_secret, account_id):
    """Pull OPEN option holdings for one account.

    SnapTrade serves equity/ETF positions and option holdings from TWO
    different endpoints: ``get_user_account_positions`` returns only
    stock/ETF/crypto, while ``options.list_option_holdings`` returns the
    open option contracts. Without this second call an open option leg
    (e.g. a long LEAP call) never lands in the current-positions snapshot
    — it shows in trade history but not as a held position. Best-effort:
    a 403 ("feature not enabled for this connection") or empty response
    is normal for brokers/plans without option data and must not fail the
    sync — the activities feed still carries the option lifecycle.
    """
    try:
        resp = client.options.list_option_holdings(
            user_id=snap_user_id,
            user_secret=snap_secret,
            account_id=account_id,
        )
    except Exception as exc:
        if _looks_like_auth_error(exc):
            raise _SnapTradeAuthError("list_option_holdings", exc)
        app.logger.warning(
            "SnapTrade list_option_holdings failed for account=%s: %s "
            "— continuing without open-option snapshot (best-effort).",
            account_id, exc,
        )
        return []
    return _coerce_list(resp)


def _fetch_recent_orders(client, snap_user_id, snap_secret, account_id):
    """Pull SnapTrade's ``recent_orders`` endpoint for one account.

    This is the real-time-ish trade source we use to backfill the
    activities-feed lag (see ``orders_to_history_df`` and
    ``_run_sync`` for the larger flow). Response shape (verified
    against snaptrade-python-sdk 11.0.193 + Alpaca Paper):
    ``{"orders": [...]}`` where each order has ``status``, ``action``,
    ``universal_symbol``, ``filled_quantity``, ``execution_price``,
    ``time_executed``.
    """
    try:
        resp = client.account_information.get_user_account_recent_orders(
            user_id=snap_user_id,
            user_secret=snap_secret,
            account_id=account_id,
        )
    except Exception as exc:
        # Orders endpoint is the real-time fallback for the activities
        # feed (see broker-sync-safety SKILL.md, 2026-05-14 PM entry).
        # By the time we reach this helper, ``_fetch_activities`` has
        # ALREADY been called once and either succeeded (proving the
        # broker auth is intact) or raised _SnapTradeAuthError itself
        # (in which case _run_sync never gets here). So a 4xx from
        # ``get_user_account_recent_orders`` alone is endpoint-specific
        # — not a revoked grant. Don't escalate it.
        #
        # Real-world cause (2026-05-15, Fidelity first-sync via SnapTrade):
        # SnapTrade returned a bare ``(403)`` on this endpoint for a
        # brand-new Fidelity connection that had just successfully
        # answered activities/positions/balances. Likely Fidelity
        # doesn't expose the order-stream API for this account class,
        # or has no recent orders to surface. Either way, the
        # ``_looks_like_auth_error`` substring heuristic
        # over-classified the bare 403 and marked the row
        # connection_broken, locking the user out of their fresh
        # connection. Log loudly and continue — the canonical
        # activities path is the auth signal that matters.
        app.logger.warning(
            "SnapTrade _fetch_recent_orders failed for account=%s: %s — "
            "falling back to activities-only history (orders endpoint is "
            "best-effort; auth was already proven by _fetch_activities).",
            account_id, exc,
        )
        return []
    body = _unwrap_body(resp)
    if isinstance(body, dict):
        items = body.get("orders") or []
    elif isinstance(body, list):
        items = body
    else:
        items = []
    out = []
    for item in items:
        if isinstance(item, dict):
            out.append(item)
        elif hasattr(item, "to_dict"):
            try:
                out.append(item.to_dict())
            except Exception:
                continue
    return out


def _fetch_balances(client, snap_user_id, snap_secret, account_id):
    try:
        resp = client.account_information.get_user_account_balance(
            user_id=snap_user_id,
            user_secret=snap_secret,
            account_id=account_id,
        )
    except Exception as exc:
        if _looks_like_auth_error(exc):
            raise _SnapTradeAuthError("get_user_account_balance", exc)
        raise
    return _coerce_list(resp)


def _fetch_account_summary(client, snap_user_id, snap_secret, account_id):
    try:
        resp = client.account_information.get_user_account_details(
            user_id=snap_user_id,
            user_secret=snap_secret,
            account_id=account_id,
        )
    except Exception as exc:
        if _looks_like_auth_error(exc):
            raise _SnapTradeAuthError("get_user_account_details", exc)
        raise
    data = _unwrap_body(resp)
    if isinstance(data, dict):
        return data
    if hasattr(data, "to_dict"):
        try:
            return data.to_dict()
        except Exception:
            return {}
    return {}


def _coerce_list(resp):
    """Normalize SnapTrade SDK responses into a plain list of dicts.

    The Python SDK returns model instances for older versions and plain
    lists for newer ones. This helper hides that drift from callers.
    """
    data = _unwrap_body(resp)
    if isinstance(data, list):
        out = []
        for item in data:
            if isinstance(item, dict):
                out.append(item)
            elif hasattr(item, "to_dict"):
                try:
                    out.append(item.to_dict())
                except Exception:
                    out.append({})
        return out
    if isinstance(data, dict):
        return [data]
    return []


def _looks_like_auth_error(exc) -> bool:
    """Heuristic: does this exception look like SnapTrade saying the
    broker connection has been revoked / requires reconnection?

    SnapTrade returns 401 / 403 with messages like "Authorization
    disabled" or "Authorization expired" when the user has revoked
    access at the broker. We don't want to import the SDK exception
    classes here (lazy import keeps the dependency optional), so we
    pattern-match the string."""
    msg = str(exc)
    if "401" in msg or "403" in msg:
        return True
    needles = ("authorization disabled", "authorization expired", "auth revoked", "reconnect required")
    low = msg.lower()
    return any(n in low for n in needles)


# ---------------------------------------------------------------------------
# Banner: rows that need reconnection (parallel to Schwab banner)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Proactive connection-health alert ("expires in X days" / "X days stale")
# ---------------------------------------------------------------------------

# How many days before a heuristic-known token lifetime to start warning.
SNAPTRADE_CONNECTION_WARN_WINDOW_DAYS = 7

# Per-broker connection token lifetimes (calendar days from link/reconnect),
# for the FORWARD "expires in X days" countdown. SnapTrade exposes NO uniform
# expiry field on the brokerage authorization (only created/updated/disabled
# dates — verified against snaptrade_client 11.x), and ``meta`` is broker-
# specific + deprecated, so a true countdown is only honest for brokers whose
# re-auth cadence we have OPERATOR-VERIFIED. This map is intentionally empty
# by default: every connection falls back to the staleness path below until a
# real lifetime is filled in here. DO NOT guess — a wrong number ships a
# "expires in 3 days!" alarm that never comes true and burns user trust.
SNAPTRADE_BROKER_CONNECTION_LIFETIME_DAYS: dict[str, int] = {}

# How many days SnapTrade can go WITHOUT a successful holdings refresh from the
# broker before we treat the connection as stalled — even though SnapTrade
# still returns HTTP 200 from its last-cached snapshot and reports the
# authorization ``disabled=False``. This is the BACKSTOP for the
# "enabled-but-not-syncing" failure mode the ``disabled`` flag misses (real
# case June 2026: user_id=9 Schwab holdings frozen on a June-12 cache for 7
# days; every sync 200-OK; balances + every position price byte-identical
# across syncs; no reconnect banner because the authorization was never marked
# disabled). The authoritative freshness signal is SnapTrade's per-account
# ``sync_status.holdings.last_successful_sync`` from the ``/accounts/{id}``
# payload we already fetch as ``account_summary``.
#
# Conservative default (4 days) tolerates a long weekend plus a market holiday
# of SnapTrade-side refresh skew without false-flagging a healthy connection.
# Env-overridable for ops tuning; set <= 0 to disable the backstop entirely.
SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS = int(
    os.environ.get("SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS", "4") or "4"
)


def _as_date(value):
    """Coerce a date/datetime (or None) to a ``date``; None on failure.

    Order matters: ``datetime`` is a subclass of ``date``, so the datetime
    branch must come first or a timestamp would slip through unconverted and
    break date arithmetic against a plain ``date``.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "date"):
        try:
            return value.date()
        except Exception:
            return None
    return None


def _parse_iso_datetime(value):
    """Parse a SnapTrade ISO-8601 timestamp (possibly ``Z``-suffixed) into a
    naive ``datetime``; ``None`` on failure. Tolerates already-parsed
    ``datetime``/``date`` values and a bare ``YYYY-MM-DD`` prefix."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    s = str(value).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d")
        except ValueError:
            return None
    return dt.replace(tzinfo=None) if dt.tzinfo is not None else dt


def _holdings_last_successful_sync_dt(account_summary):
    """Extract SnapTrade's ``sync_status.holdings.last_successful_sync`` from
    the ``/accounts/{id}`` payload as a full naive ``datetime``; ``None`` if
    absent or unparseable.

    This is the authoritative "when did SnapTrade last pull fresh holdings
    from the broker" timestamp. A value many days old means the connection has
    stalled even when the brokerage authorization is not ``disabled`` — the
    failure mode the disabled flag alone misses. We persist this (see
    ``record_snaptrade_holdings_sync``) to publish an honest "broker data as
    of" badge, and reduce it to a ``date`` for the staleness backstop.
    """
    if not isinstance(account_summary, dict):
        return None
    sync_status = account_summary.get("sync_status")
    if not isinstance(sync_status, dict):
        return None
    holdings = sync_status.get("holdings")
    if not isinstance(holdings, dict):
        return None
    return _parse_iso_datetime(holdings.get("last_successful_sync"))


def _holdings_last_successful_sync(account_summary):
    """Same authoritative freshness signal as
    ``_holdings_last_successful_sync_dt`` reduced to a ``date`` (the grain the
    staleness backstop compares against); ``None`` if absent/unparseable."""
    dt = _holdings_last_successful_sync_dt(account_summary)
    return dt.date() if dt is not None else None


def _holdings_stale_days(account_summary, *, today=None):
    """Days since SnapTrade last refreshed holdings from the broker.

    Returns ``None`` when the freshness signal is unavailable or in the future
    — ``None`` means "change nothing", so a missing/odd timestamp never
    false-flags a healthy connection (mirrors the ``disabled`` helper's
    None-is-safe philosophy)."""
    last = _holdings_last_successful_sync(account_summary)
    if last is None:
        return None
    today = today or date.today()
    days = (today - last).days
    return days if days >= 0 else None


def _connection_attention(acc_row, *, today=None):
    """Classify ONE SnapTrade account row's connection health for the
    proactive alert + reminder email.

    Returns ``None`` when the connection is healthy, otherwise a dict:

      ``kind``            — "stale" (already stopped syncing) or
                            "expiring" (heuristic countdown, not yet broken).
      ``stale_days``      — days since ``connection_broken_at`` ("stale"), else None.
      ``expires_in_days`` — days until the heuristic lifetime expiry
                            ("expiring", may be 0/negative if overdue), else None.
      plus the label fields the banner/email need (``snaptrade_account_id``,
      ``account_name``, ``display_nickname``, ``broker_slug``).

    Precedence: a BROKEN connection ("stale") always wins over a heuristic
    countdown — there's no point counting down to an expiry that already
    happened.
    """
    today = today or date.today()
    broker_slug = (acc_row.get("broker_slug") or "").strip()
    base = {
        "snaptrade_account_id": acc_row.get("snaptrade_account_id"),
        "account_name": acc_row.get("account_name"),
        "display_nickname": acc_row.get("display_nickname"),
        "broker_slug": broker_slug,
    }

    broken_on = _as_date(acc_row.get("connection_broken_at"))
    if broken_on is not None:
        stale_days = max(0, (today - broken_on).days)
        return {**base, "kind": "stale", "stale_days": stale_days, "expires_in_days": None}

    lifetime = SNAPTRADE_BROKER_CONNECTION_LIFETIME_DAYS.get(broker_slug)
    created_on = _as_date(acc_row.get("created_at"))
    if lifetime is not None and created_on is not None:
        expires_in_days = (created_on + timedelta(days=int(lifetime)) - today).days
        if expires_in_days <= SNAPTRADE_CONNECTION_WARN_WINDOW_DAYS:
            return {
                **base, "kind": "expiring",
                "stale_days": None, "expires_in_days": expires_in_days,
            }
    return None


def snaptrade_accounts_needing_attention(user_id, *, today=None):
    """All of ``user_id``'s SnapTrade accounts that warrant a reconnect
    alert, each enriched by :func:`_connection_attention`. Healthy
    connections are dropped. One Postgres read; safe to call per request."""
    from app.models import get_snaptrade_accounts
    out = []
    for row in get_snaptrade_accounts(user_id) or []:
        att = _connection_attention(row, today=today)
        if att is not None:
            out.append(att)
    return out


@app.context_processor
def _inject_snaptrade_reauth_needed():
    """Surface SnapTrade rows that need reconnecting — both already-stale
    (``connection_broken_at`` set) and heuristic "expiring soon" — each
    carrying a day count so the banner can say "stopped syncing X days ago"
    / "expires in X days". Templates render the same banner shape.
    """
    try:
        if not getattr(current_user, "is_authenticated", False):
            return {"snaptrade_reauth_needed": []}
        rows = snaptrade_accounts_needing_attention(current_user.id)
        return {"snaptrade_reauth_needed": rows}
    except Exception:
        return {"snaptrade_reauth_needed": []}


def broker_data_freshness(user_id, *, today=None):
    """Honest "broker data as of" for the always-on freshness strip.

    Returns ``(as_of_date, stale_days)`` where ``as_of_date`` is the OLDEST
    ``holdings_last_successful_sync`` across the user's connected accounts —
    the weakest link, so we NEVER overstate freshness when one connection has
    stalled — and ``stale_days`` is whole days since that date. ``(None, None)``
    when no account reports a usable timestamp (cold start / pre-migration /
    unauthorized creds). This is SnapTrade's own "fresh from the broker" clock,
    NOT our cron's cache-read ``last_sync_at``."""
    stamps = [
        r.get("holdings_last_successful_sync")
        for r in (get_snaptrade_accounts(user_id) or [])
        if r.get("holdings_last_successful_sync") is not None
    ]
    if not stamps:
        return None, None
    oldest = _as_date(min(stamps))
    if oldest is None:
        return None, None
    today = today or date.today()
    days = (today - oldest).days
    return oldest, (days if days >= 0 else 0)


def post_close_broker_tenant_ids(user_id, *, now=None):
    """Tenant_ids whose broker mark is a genuine POST-CLOSE mark — the scope
    for the Daily Review "After-hours movers" section.

    That section compares each holding's broker mark to today's OFFICIAL
    close to surface extended-hours drift. The comparison is only meaningful
    when the broker mark was captured AFTER the 4pm ET bell — otherwise a mark
    from earlier in the session is compared against the close and the intraday
    move is shown BACKWARDS (real case 2026-07-07: BE synced mid-session at
    ~$295, closed $269.57, and the section reported a bogus +$25.88/sh
    "after-hours" gain that was really the day's -8.6% drop inverted).

    The warehouse has no per-row capture time (``stg_current.snapshot_date``
    is just ``current_date()``), so we gate on SnapTrade's authoritative
    per-account ``holdings_last_successful_sync``. Rather than the old
    all-or-nothing weakest-link gate (one stale account — a broken connection,
    or an account that simply hasn't re-synced since the close — hid the WHOLE
    section for everyone else), we return the SET of tenant_ids that ARE
    post-close so the caller can scope the query to exactly those accounts.
    Accounts that aren't post-close are dropped from the aggregate; healthy
    post-close accounts still render. ``tenant_id`` is ``snaptrade:<uuid>``
    where the uuid is the account's ``snaptrade_account_id`` (the after-hours
    query is tenant-keyed, so scoping by tenant_id is exact).

    Returns an EMPTY set off-hours, on weekends, and on cold start / missing
    timestamps — which the caller treats as "hide the section"."""
    from zoneinfo import ZoneInfo

    et = ZoneInfo("America/New_York")
    now_et = now or datetime.now(et)
    if now_et.tzinfo is None:
        now_et = now_et.replace(tzinfo=et)
    now_et = now_et.astimezone(et)

    # U.S. regular-session close = 16:00 ET, Mon–Fri. No holiday calendar:
    # on a holiday no official close publishes, so the query is empty anyway.
    close_et = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    if now_et.weekday() >= 5 or now_et < close_et:
        return set()

    out = set()
    for r in get_snaptrade_accounts(user_id) or []:
        # Prefer SnapTrade's own broker-pull time; fall back to our
        # cache-read time (last_sync_at) for brokers/accounts that don't
        # report holdings_last_successful_sync. Under the real-time plan the
        # two are close, and both prove the mark we hold is post-close.
        stamp = r.get("holdings_last_successful_sync")
        if not isinstance(stamp, datetime):
            stamp = r.get("last_sync_at")
        if not isinstance(stamp, datetime):
            continue
        s = stamp if stamp.tzinfo is not None else stamp.replace(tzinfo=ZoneInfo("UTC"))
        if s.astimezone(et) < close_et:
            continue
        acct_id = (r.get("snaptrade_account_id") or "").strip()
        if not acct_id:
            continue
        try:
            out.add(build_tenant_id(SNAPTRADE_BROKER_SLUG, acct_id))
        except ValueError:
            continue
    return out


@app.context_processor
def _inject_broker_data_freshness():
    """Global "broker data as of" — surfaced on EVERY page via the slim strip
    in base.html so a trader always knows how current their numbers are.
    Best-effort; never breaks a render."""
    try:
        if not getattr(current_user, "is_authenticated", False):
            return {"broker_data_as_of": None, "broker_data_stale_days": None}
        as_of, stale_days = broker_data_freshness(current_user.id)
        return {"broker_data_as_of": as_of, "broker_data_stale_days": stale_days}
    except Exception:
        return {"broker_data_as_of": None, "broker_data_stale_days": None}
