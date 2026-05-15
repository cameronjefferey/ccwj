"""SnapTrade brokerage aggregator — connect, sync, multi-account.

SnapTrade unlocks ~20 brokers (Schwab, Fidelity, Vanguard, Robinhood,
IBKR, Tradier, etc.) through one integration. We use SnapTrade's
hosted Connection Portal (no broker OAuth implemented here) and pull
activities + positions + balances via SnapTrade's REST API, then
normalize to the same seed CSVs (``trade_history.csv`` /
``current_positions.csv`` / ``account_balances.csv``) that native
Schwab sync and manual upload write to. Convergence happens at
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
* ``_bulk_sync_lookback_days`` — re-uses Schwab's helper of the same name
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
    record_snaptrade_sync_attempt,
    remove_snaptrade_account,
    remove_snaptrade_user,
    save_snaptrade_user,
    set_snaptrade_brokerage_authorization_id,
    stamp_snaptrade_force_refresh_attempt,
    update_snaptrade_account_nickname,
    upsert_snaptrade_account,
)
from app.schwab import (
    SCHWAB_FULL_HISTORY_LOOKBACK_DAYS as _FULL_HISTORY_LOOKBACK_DAYS,
    _bulk_sync_lookback_days,
    _schwab_transaction_lookback_days as _routine_lookback_days,
)
from app.snaptrade_normalize import (
    activities_to_history_df,
    balances_to_balance_df,
    orders_to_history_df,
    positions_to_current_df,
)
from app.utils import demo_block_writes

_log = logging.getLogger(__name__)

# Reuse the Schwab full-history cap for symmetric UX. SnapTrade's
# per-broker history depth varies (Schwab via SnapTrade can go years;
# Fidelity / Robinhood return less) — we ask for the full window and let
# SnapTrade clamp.
SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS = _FULL_HISTORY_LOOKBACK_DAYS


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
    """
    blocked = demo_block_writes("connecting a brokerage account")
    if blocked:
        return blocked
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
            snap_user_label = f"happytrader-{user_id}"
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
        login_resp = client.authentication.login_snap_trade_user(
            user_id=snap["snaptrade_user_id"],
            user_secret=snap["snaptrade_secret"],
            custom_redirect=custom_redirect,
        )
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
    return redirect(portal_url)


@app.route("/snaptrade/callback")
@login_required
def snaptrade_callback():
    """Handle SnapTrade's redirect after the user finishes the
    Connection Portal flow. Lists the user's accounts via SnapTrade
    and persists each one as a row in ``snaptrade_accounts``.
    """
    expected_user_id = session.pop("snaptrade_callback_user_id", None)
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
        add_account_for_user(user_id, account_name)
        saved += 1

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
    """
    rows = get_snaptrade_accounts(current_user.id) or []
    return render_template(
        "snaptrade_accounts.html",
        title="Connected brokerages",
        accounts=rows,
        snaptrade_enabled=snaptrade_enabled(),
    )


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
            if _looks_like_auth_error(exc):
                mark_snaptrade_connection_broken(user_id, snaptrade_account_id)
                return (
                    False,
                    "SnapTrade said this connection isn't authorized anymore. "
                    "Reconnect the broker and try again.",
                    None,
                )
            app.logger.warning(
                "_force_refresh_brokerage: get_user_account_details failed for "
                "user_id=%s account=%s: %s",
                user_id, snaptrade_account_id, exc,
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
        if _looks_like_auth_error(exc):
            mark_snaptrade_connection_broken(user_id, snaptrade_account_id)
            return (
                False,
                "SnapTrade said this connection isn't authorized anymore. "
                "Reconnect the broker and try again.",
                None,
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
        app.logger.warning(
            "_force_refresh_brokerage: refresh failed for user_id=%s "
            "auth=%s account=%s: %s",
            user_id, auth_id, snaptrade_account_id, exc,
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


@app.route("/snaptrade/refresh-broker", methods=["POST"])
@login_required
def snaptrade_refresh_broker():
    """User-initiated "Refresh from broker" — call SnapTrade's
    ``refresh_brokerage_authorization``, then chain into the regular
    sync so any data that landed during the refresh window flows
    through to the seed.

    Why this is its own route (not folded into ``snaptrade_sync``):
    refresh is BILLED PER CALL by SnapTrade. Folding it in would
    multiply our cost by every cron run. Surfacing it as an explicit
    user action keeps the spend bounded to "user pressed a button"
    instead of "every scheduled job".

    Form fields:
        snaptrade_account_id (required): which brokerage to refresh.
        full_history_again ("1"): forwarded to the chained sync.
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

    ok, message, _throttle = _force_refresh_brokerage(
        current_user.id, snaptrade_account_id,
    )
    if not ok:
        # Throttle / rate-limit / config / auth — already user-friendly.
        flash(message, "warning")
        return redirect(url_for("snaptrade_accounts_page"))

    # Give the broker a head start. SnapTrade webhooks deliver the
    # holding update within ~30s typically; sleeping the request thread
    # for the full window would be a bad UX (ties up a Flask worker).
    # 5 seconds is a happy middle: short enough that the user doesn't
    # bounce, long enough that the broker has often already pushed the
    # first batch. The chained sync then picks up whatever is there;
    # if more lands later, the next routine sync will catch it.
    import time
    time.sleep(5)

    # Chain into the regular per-account sync so the user gets the same
    # post-sync summary (trade count, position count, push status) they
    # see from the regular Sync now button.
    force_full = request.form.get("full_history_again") == "1"
    first_done = bool(acc_row.get("first_sync_completed"))
    lookback_days = _bulk_sync_lookback_days(
        first_done,
        force_full_history=force_full,
        routine_days=_routine_lookback_days(),
        full_days=SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS,
    )
    res = _sync_one_connection(current_user.id, acc_row, lookback_days=lookback_days)

    if not res["ok"]:
        flash(
            f"Refreshed broker, but the sync didn't finish: "
            f"{res.get('error') or 'unknown error'}. Try the regular Sync now "
            f"in a minute.",
            "danger",
        )
        return redirect(url_for("snaptrade_accounts_page"))

    hr, cr = res["history_rows"], res["current_rows"]
    trade_word = "trade" if hr == 1 else "trades"
    pos_word = "position" if cr == 1 else "positions"
    summary = (
        f"Refresh + sync complete for {res['label']}. Pulled {hr:,} "
        f"{trade_word} and {cr} open {pos_word}."
    )
    if res["github_pushed"]:
        flash(
            f"{summary} We're processing now — refresh in a minute. "
            f"If a just-placed trade still doesn't appear, that broker takes "
            f"a few minutes to push to SnapTrade — try one more sync shortly.",
            "success",
        )
        qp = {}
        h = (res["github_head_sha"] or "").strip()
        if h:
            qp["sha"] = h
        if not first_done:
            qp["first"] = 1
        return redirect(url_for("sync_processing", **qp))
    if res["github_error"]:
        flash(f"{summary} Couldn't push to the cloud: {res['github_error']}", "warning")
    elif res["github_seed_push_skipped"]:
        flash(f"{summary} Live dashboard updates are not turned on for this environment.", "info")
    return redirect(url_for("snaptrade_accounts_page"))


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
    res = _sync_one_connection(current_user.id, acc_row, lookback_days=lookback_days)

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
    summary = (
        f"Sync complete for {res['label']}. Pulled {hr:,} {trade_word} "
        f"and {cr} open {pos_word}."
    )
    if res["github_pushed"]:
        flash(
            f"{summary} We're processing your data now — refresh in a minute.",
            "success",
        )
        qp = {}
        h = (res["github_head_sha"] or "").strip()
        if h:
            qp["sha"] = h
        if not first_done:
            qp["first"] = 1
        return redirect(url_for("sync_processing", **qp))
    if res["github_error"]:
        flash(f"{summary} Couldn't push to the cloud: {res['github_error']}", "warning")
    elif res["github_seed_push_skipped"]:
        flash(f"{summary} Live dashboard updates are not turned on for this environment.", "info")
    return redirect(url_for("snaptrade_accounts_page"))


# ---------------------------------------------------------------------------
# Sync orchestration
# ---------------------------------------------------------------------------

def _sync_one_connection(user_id, acc_row, *, lookback_days):
    """Sync ONE SnapTrade-managed broker account end-to-end.

    Returns a structured dict like ``_sync_one_connection`` in
    app.schwab. Never raises — failures land as
    ``{"ok": False, "error": "...", ...}`` so multi-account loops
    survive one bad row.
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

    try:
        result = _run_sync(
            user_id,
            client,
            snap=snap,
            acc_row=acc_row,
            lookback_days=lookback_days,
        )
        mark_snaptrade_first_sync_completed(user_id, snaptrade_account_id)
        clear_snaptrade_connection_broken(user_id, snaptrade_account_id)
        record_snaptrade_sync_attempt(user_id, snaptrade_account_id, error=None)
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
        })
    except _SnapTradeAuthError:
        mark_snaptrade_connection_broken(user_id, snaptrade_account_id)
        record_snaptrade_sync_attempt(
            user_id, snaptrade_account_id, error="connection_broken",
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
    ``_sync_one_connection`` to flag the account for reconnection."""


def _run_sync(user_id, client, *, snap, acc_row, lookback_days):
    """Pull activities + positions + balances from SnapTrade,
    normalize, push to GitHub seeds.

    Mirrors ``app.schwab._run_sync`` shape so the orchestration layer
    can treat the two connectors identically.
    """
    snaptrade_account_id = acc_row["snaptrade_account_id"]
    account_name = acc_row["account_name"]
    snap_user_id = snap["snaptrade_user_id"]
    snap_secret = snap["snaptrade_secret"]

    end_date = date.today()
    start_date = end_date - timedelta(days=int(lookback_days))

    activities = _fetch_activities(client, snap_user_id, snap_secret, snaptrade_account_id, start_date, end_date)
    orders = _fetch_recent_orders(client, snap_user_id, snap_secret, snaptrade_account_id)
    positions = _fetch_positions(client, snap_user_id, snap_secret, snaptrade_account_id)
    balances = _fetch_balances(client, snap_user_id, snap_secret, snaptrade_account_id)
    account_summary = _fetch_account_summary(client, snap_user_id, snap_secret, snaptrade_account_id)

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
    )
    orders_df = orders_to_history_df(
        orders, account_name=account_name, user_id=user_id,
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
    current_df = positions_to_current_df(
        positions, account_name=account_name, user_id=user_id,
    )
    balances_df = balances_to_balance_df(
        account_summary=account_summary,
        balances=balances,
        positions=positions,
        account_name=account_name,
        user_id=user_id,
    )

    skip_history = history_df is None or history_df.empty

    github_pushed = False
    github_error = None
    github_head_sha = None
    github_skip_reason = None

    from app.upload import _upload_github_config_ok, merge_and_push_seeds

    ok_cfg, cfg_err = _upload_github_config_ok()
    if not ok_cfg:
        github_skip_reason = cfg_err or "GitHub seed push not configured."
        app.logger.warning(
            "SnapTrade sync: skipping GitHub push for user_id=%s account=%s — %s",
            user_id, account_name, github_skip_reason,
        )
    else:
        uname = "user"
        u = User.get_by_id(user_id)
        if u:
            uname = u.username
        if skip_history:
            commit_msg = (
                f"SnapTrade sync ({uname}): positions only "
                f"({len(current_df)} lines) ({account_name})"
            )
        else:
            commit_msg = (
                f"SnapTrade sync ({uname}): {len(history_df)} tx, "
                f"{len(current_df)} open lines ({account_name})"
            )
        ok, err, _hr, _cr, github_head_sha = merge_and_push_seeds(
            account_name,
            history_df,
            current_df,
            commit_message=commit_msg,
            user_id=user_id,
            skip_history=skip_history,
            balances_df=balances_df,
        )
        github_pushed = ok
        github_error = err if not ok else None

    return {
        "history_rows": 0 if skip_history else len(history_df),
        "current_rows": len(current_df),
        "lookback_days": int(lookback_days),
        "github_pushed": github_pushed,
        "github_error": github_error,
        "github_head_sha": github_head_sha,
        "github_seed_push_skipped": not ok_cfg,
        "github_skip_reason": github_skip_reason,
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
                raise _SnapTradeAuthError(str(exc))
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
            raise _SnapTradeAuthError(str(exc))
        raise
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
        if _looks_like_auth_error(exc):
            raise _SnapTradeAuthError(str(exc))
        # Don't fail the whole sync on an orders-fetch error — the
        # activities path is the canonical source. Log and return
        # empty so the rest of _run_sync proceeds.
        app.logger.warning(
            "SnapTrade _fetch_recent_orders failed for account=%s: %s — "
            "falling back to activities-only history.",
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
            raise _SnapTradeAuthError(str(exc))
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
            raise _SnapTradeAuthError(str(exc))
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

@app.context_processor
def _inject_snaptrade_reauth_needed():
    """Surface SnapTrade rows whose ``connection_broken_at`` is set.
    Templates render the same banner shape as ``schwab_reauth_needed``.
    """
    try:
        if not getattr(current_user, "is_authenticated", False):
            return {"snaptrade_reauth_needed": []}
        from app.models import get_expired_snaptrade_accounts
        rows = get_expired_snaptrade_accounts(current_user.id) or []
        return {"snaptrade_reauth_needed": rows}
    except Exception:
        return {"snaptrade_reauth_needed": []}
