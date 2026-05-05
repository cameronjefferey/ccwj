"""Schwab API — OAuth connection and sync of positions/transactions."""
import json
import os
import secrets
import tempfile
import time
from datetime import datetime, date, timedelta
from urllib.parse import urlencode

from flask import redirect, render_template, request, url_for, flash, session
from flask_login import login_required, current_user

from app import app
from app.models import (
    AccountClaimedError,
    User,
    account_is_claimed_by_other,
    get_schwab_connection,
    get_schwab_connections,
    mark_schwab_first_sync_completed,
    save_schwab_connection,
    update_schwab_account_hash,
    update_schwab_connection_nickname,
    update_schwab_token,
    update_schwab_tokens_for_user,
    add_account_for_user,
)
from app.utils import demo_block_writes

# OAuth state stored in Flask session

SCHWAB_AUTH_URL = "https://api.schwabapi.com/v1/oauth/authorize"
SCHWAB_TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
SCHWAB_API_BASE = "https://api.schwabapi.com"

# UI "import full history" uses this many days (cap for API + dbt); routine sync uses env default.
SCHWAB_FULL_HISTORY_LOOKBACK_DAYS = 1825


def _schwab_asset_type_to_security_type(asset_type):
    """
    Map Schwab API assetType → the same security_type vocabulary as the
    brokerage-export current_positions.csv (e.g. 'Equity', 'Option',
    'ETFs & Closed End Funds'). Anything unmapped passes through title-cased.
    """
    at = str(asset_type or "").strip().upper()
    if at == "EQUITY":
        return "Equity"
    if at == "OPTION":
        return "Option"
    if at == "COLLECTIVE_INVESTMENT":
        return "ETFs & Closed End Funds"
    if at == "MUTUAL_FUND":
        return "Mutual Funds"
    if at == "FIXED_INCOME":
        return "Fixed Income"
    if at == "CURRENCY":
        return "Cash and Money Market"
    if at:
        return at.title()
    return ""


def _schwab_position_cost_basis(sec, inst, qty, avg, inst_type):
    """
    Total position cost for seeds / dbt.

    Schwab's averagePrice on OPTION positions is normally premium **per share of
    underlying** (same convention as quotes). Cost = avg * |contracts| * multiplier.
    US equity options default to 100 when the API omits multiplier.

    Prefer API costBasis when present (covers odd instruments / shorts).
    """
    raw_cb = sec.get("costBasis")
    if raw_cb is not None and str(raw_cb).strip() != "":
        try:
            return float(raw_cb)
        except (TypeError, ValueError):
            pass
    q = abs(float(qty or 0))
    a = float(avg or 0)
    if str(inst_type).upper() == "OPTION" and q:
        mult = inst.get("multiplier")
        try:
            m = float(mult) if mult is not None else 100.0
        except (TypeError, ValueError):
            m = 100.0
        if m <= 0:
            m = 100.0
        return a * q * m
    return a * q


def _schwab_transaction_lookback_days():
    """
    Calendar days of transactions to request from Schwab each sync.
    Set SCHWAB_SYNC_TRANSACTION_DAYS (integer); default 60, clamped to 1..1825.
    """
    raw = (os.environ.get("SCHWAB_SYNC_TRANSACTION_DAYS") or "60").strip() or "60"
    try:
        days = int(raw)
    except ValueError:
        days = 60
    return max(1, min(days, 1825))


def _schwab_transaction_chunk_days():
    """
    Max span (inclusive) per get_transactions call. Schwab returns HTTP 400 if
    startDate/endDate cover too long a range (see schwab-py docs, often ~60 days).
    Set SCHWAB_TRANSACTION_CHUNK_DAYS to tune (7–366, default 60). If sync 400s,
    lower this; if smaller chunks are slow but stable, you can try nudging upward.
    """
    raw = (os.environ.get("SCHWAB_TRANSACTION_CHUNK_DAYS") or "60").strip() or "60"
    try:
        d = int(raw)
    except ValueError:
        d = 60
    return max(7, min(d, 366))


def _schwab_fetch_transactions_window(client, account_hash, start_date, end_date):
    """
    Single GET /transactions for [start_date, end_date] (inclusive, date objects).
    """
    return _schwab_resp_with_refresh(
        client,
        lambda: client.get_transactions(
            account_hash, start_date=start_date, end_date=end_date
        ),
    )


def _schwab_fetch_all_transaction_items(client, account_hash, start_date, end_date):
    """
    Merge all transaction records from Schwab, requesting in time windows
    of at most _schwab_transaction_chunk_days() so the API does not 400
    on long lookbacks (e.g. 1825 days).
    """
    if start_date > end_date:
        return []
    chunk_days = _schwab_transaction_chunk_days()
    merged = []
    seen = set()
    cursor = start_date
    is_first = True
    while cursor <= end_date:
        if not is_first:
            time.sleep(0.12)
        is_first = False
        window_end = min(
            end_date, cursor + timedelta(days=chunk_days - 1)
        )
        resp = _schwab_fetch_transactions_window(
            client, account_hash, cursor, window_end
        )
        tx_data = resp.json()
        part = (
            tx_data
            if isinstance(tx_data, list)
            else (
                tx_data.get("transaction")
                or tx_data.get("transactions")
                or []
            )
        )
        if not isinstance(part, list):
            part = []
        for tx in part:
            if not isinstance(tx, dict):
                continue
            key = None
            for k in ("transactionId", "activityId"):
                v = tx.get(k)
                if v is not None and str(v).strip() != "":
                    key = f"id:{k}:{v}"
                    break
            if key is None:
                key = (
                    f"hash:{tx.get('transactionDate')!s}:"
                    f"{tx.get('type')!s}:{str(tx)[:200]}"
                )
            if key in seen:
                continue
            seen.add(key)
            merged.append(tx)
        cursor = window_end + timedelta(days=1)
    return merged


def _schwab_resp_with_refresh(client, request_fn):
    """
    Run a schwab-py call that returns httpx.Response. On 401, refresh OAuth
    tokens and retry once (access token can be rejected before local expiry).
    """
    resp = request_fn()
    if resp.status_code == 401:
        sess = client.session
        token = getattr(sess, "token", None)
        rt = token.get("refresh_token") if token else None
        if rt:
            sess.refresh_token(SCHWAB_TOKEN_URL, refresh_token=rt)
        resp = request_fn()
    resp.raise_for_status()
    return resp


def _schwab_float(bal, *keys, default=0.0):
    """Read first present key from Schwab balances dict as float."""
    for k in keys:
        if not k:
            continue
        v = bal.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return default


def _schwab_build_balance_seed_rows(acct_data, open_position_rows):
    """
    Two rows for schwab_account_balances.csv (cash + account_total), derived
    from Schwab currentBalances. open_position_rows: API positions only
    (list of dicts with market_value/cost_basis). Account column is added
    by merge_and_push_seeds.
    """
    if not isinstance(acct_data, dict):
        return []
    sec = acct_data.get("securitiesAccount")
    if not isinstance(sec, dict):
        sec = {}
    bal = sec.get("currentBalances")
    if not isinstance(bal, dict):
        bal = {}

    cash = _schwab_float(bal, "cashBalance", "cashAvailableForTrading", "availableFunds")
    long_mv = _schwab_float(bal, "longMarketValue")
    total = _schwab_float(
        bal,
        "liquidationValue",
        "equity",
        "netLiquidationValue",
        "longMarginValue",
    )
    pos_mv = sum(float(p.get("market_value") or 0) for p in open_position_rows)
    pos_cb = sum(float(p.get("cost_basis") or 0) for p in open_position_rows)
    if total <= 0 and (cash != 0 or long_mv > 0 or pos_mv > 0):
        total = long_mv + cash if (long_mv or cash) else pos_mv + cash
    if total <= 0 and cash == 0 and not open_position_rows:
        return []

    pct_cash = ""
    if total > 0:
        pct_cash = str(round(100.0 * cash / total, 6))

    unreal = None
    if total and pos_cb:
        unreal = total - pos_cb
    unreal_pct = None
    if unreal is not None and pos_cb:
        unreal_pct = round(100.0 * unreal / abs(pos_cb), 6) if pos_cb != 0 else None

    return [
        {
            "row_type": "cash",
            "market_value": cash,
            "cost_basis": "",
            "unrealized_pnl": "",
            "unrealized_pnl_pct": "",
            "percent_of_account": pct_cash,
        },
        {
            "row_type": "account_total",
            "market_value": total,
            "cost_basis": pos_cb if pos_cb else "",
            "unrealized_pnl": unreal if unreal is not None else "",
            "unrealized_pnl_pct": unreal_pct if unreal_pct is not None else "",
            "percent_of_account": "",
        },
    ]


def _sync_account_hash_from_numbers(client, user_id, account_number, current_hash):
    """
    Fetch hashValue from GET /accounts/accountNumbers and persist if it changed.
    Stale hashes produce 401 on /accounts/{hash}.
    """
    resp = _schwab_resp_with_refresh(client, client.get_account_numbers)
    data = resp.json()
    if not isinstance(data, list) or not data:
        return current_hash
    want = str(account_number or "").strip()
    new_hash = None
    for row in data:
        if str(row.get("accountNumber", "")).strip() == want:
            new_hash = row.get("hashValue") or ""
            break
    if not new_hash and len(data) == 1:
        new_hash = data[0].get("hashValue") or ""
    if new_hash and new_hash != current_hash:
        update_schwab_account_hash(user_id, account_number, new_hash)
        return new_hash
    return current_hash


def _wrap_schwab_token_for_py(raw):
    """
    schwab-py TokenMetadata.from_loaded_token() requires:
        {"creation_timestamp": int, "token": { access_token, refresh_token, ... }}

    Older HappyTrader builds stored a flat dict with created_at instead.
    """
    if not raw or not isinstance(raw, dict):
        return raw
    if "creation_timestamp" in raw and "token" in raw:
        return raw
    if "access_token" in raw and "token" not in raw:
        inner = {}
        for k in ("access_token", "refresh_token", "token_type", "expires_in", "expires_at", "scope"):
            if k in raw and raw[k] is not None:
                inner[k] = raw[k]
        if "token_type" not in inner:
            inner["token_type"] = "Bearer"
        ts = int(raw.get("created_at") or time.time())
        return {"creation_timestamp": ts, "token": inner}
    return raw


def _schwab_config():
    """Return (app_key, app_secret, callback_url) or None if not configured.

    Used by OAuth-grant paths (``/schwab/connect`` and ``/schwab/callback``)
    which need the callback URL to build the redirect. The sync path uses
    :func:`_schwab_sync_config` instead because it only needs the API key
    and secret to refresh tokens.
    """
    key = os.environ.get("SCHWAB_APP_KEY", "").strip()
    secret = os.environ.get("SCHWAB_APP_SECRET", "").strip()
    callback = os.environ.get("SCHWAB_CALLBACK_URL", "").strip()
    if not key or not secret or not callback:
        return None
    return (key, secret, callback)


def _schwab_sync_config():
    """Return ``(app_key, app_secret)`` or ``None`` if not configured.

    The Render cron runs ``app.schwab_sync_cli`` in an environment that
    only carries ``SCHWAB_APP_KEY`` and ``SCHWAB_APP_SECRET`` — the
    callback URL is a web-only env var. Reusing the full
    :func:`_schwab_config` here used to silently turn every cron run
    into a 5/5 "No valid client" no-op even though the tokens were
    fine, because the missing callback URL forced the helper to return
    ``None``.
    """
    key = os.environ.get("SCHWAB_APP_KEY", "").strip()
    secret = os.environ.get("SCHWAB_APP_SECRET", "").strip()
    if not key or not secret:
        return None
    return (key, secret)


def _get_schwab_client(user_id, account_number=None):
    """
    Create a schwab-py client using stored token.
    Uses client_from_access_functions with DB-backed token read/write.

    Returns ``None`` when the connection row, API credentials, or
    schwab-py client init fails. Each branch logs at WARNING so cron
    log readers can tell "missing env var" from "expired refresh
    token" — both used to look identical from the CLI.
    """
    conn_data = get_schwab_connection(user_id, account_number)
    if not conn_data:
        app.logger.warning(
            "Schwab client: no DB row (user_id=%s, account_number=%s)",
            user_id, account_number,
        )
        return None

    cfg = _schwab_sync_config()
    if not cfg:
        app.logger.warning(
            "Schwab client: SCHWAB_APP_KEY/SECRET missing in env "
            "(user_id=%s) — re-check the cron service env vars",
            user_id,
        )
        return None

    app_key, app_secret = cfg

    def token_read():
        c = get_schwab_connection(user_id, conn_data["account_number"])
        if not c:
            return None
        raw = json.loads(c["token_json"])
        wrapped = _wrap_schwab_token_for_py(raw)
        if wrapped is not raw:
            # Wrap-format upgrade should propagate to *all* the user's rows so
            # subsequent reads on a sibling connection don't re-do it.
            update_schwab_tokens_for_user(user_id, json.dumps(wrapped))
        return wrapped

    def token_write(token, *args, **kwargs):
        # schwab-py passes the wrapped {creation_timestamp, token} dict on refresh.
        # Newer authlib/oauth passes through extra kwargs (e.g. refresh_token=...) — ignore them.
        # Update *every* row for this user — the OAuth grant covers all
        # accounts under a single Schwab login, so a refresh here means
        # the sibling rows' refresh tokens are now stale.
        update_schwab_tokens_for_user(user_id, json.dumps(token))

    try:
        from schwab.auth import client_from_access_functions

        return client_from_access_functions(
            app_key,
            app_secret,
            token_read_func=token_read,
            token_write_func=token_write,
        )
    except Exception:
        app.logger.exception(
            "Schwab client init failed (user_id=%s, account_number=%s)",
            user_id,
            conn_data.get("account_number"),
        )
        return None


@app.route("/schwab/connect")
@login_required
def schwab_connect():
    """Redirect user to Schwab OAuth authorization."""
    blocked = demo_block_writes("connecting a Schwab account")
    if blocked:
        return blocked
    cfg = _schwab_config()
    if not cfg:
        flash("Schwab API is not configured. Contact the administrator.", "danger")
        return redirect(url_for("profile", tab="account"))

    app_key, app_secret, callback_url = cfg
    state = secrets.token_urlsafe(32)
    session["schwab_oauth_state"] = state
    session["schwab_oauth_user_id"] = current_user.id

    params = {
        "response_type": "code",
        "client_id": app_key,
        "redirect_uri": callback_url,
        "state": state,
    }
    auth_url = f"{SCHWAB_AUTH_URL}?{urlencode(params)}"
    return redirect(auth_url)


@app.route("/schwab/callback")
@login_required
def schwab_callback():
    """Handle OAuth callback from Schwab. Exchange code for tokens."""
    cfg = _schwab_config()
    if not cfg:
        flash("Schwab API is not configured.", "danger")
        return redirect(url_for("profile", tab="account"))

    code = request.args.get("code")
    state = request.args.get("state")
    error = request.args.get("error")

    if error:
        from app import app as _app
        _app.logger.warning("Schwab OAuth returned error: %s", error)
        flash("Schwab couldn't authorize the connection. Please try again from Settings.", "danger")
        return redirect(url_for("profile", tab="account"))

    if not code or not state:
        flash("Invalid callback from Schwab. Please try again.", "danger")
        return redirect(url_for("profile", tab="account"))

    saved_state = session.pop("schwab_oauth_state", None)
    user_id = session.pop("schwab_oauth_user_id", None)
    if not saved_state or saved_state != state or not user_id or user_id != current_user.id:
        flash("Invalid state. Please try connecting again.", "danger")
        return redirect(url_for("profile", tab="account"))

    app_key, app_secret, callback_url = cfg

    # Exchange code for tokens
    import requests

    resp = requests.post(
        SCHWAB_TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": callback_url,
        },
        auth=(app_key, app_secret),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )

    if resp.status_code != 200:
        from app import app as _app
        _app.logger.error("Schwab token exchange failed (%s): %s", resp.status_code, resp.text[:500])
        flash("Couldn't finish connecting to Schwab. Please try again, or contact support if it keeps happening.", "danger")
        return redirect(url_for("profile", tab="account"))

    token_data = resp.json()
    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")
    if not access_token or not refresh_token:
        flash("Schwab did not return tokens. Please try again.", "danger")
        return redirect(url_for("profile", tab="account"))

    # schwab-py TokenMetadata expects { creation_timestamp, token: { oauth fields } }
    inner = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": token_data.get("token_type", "Bearer"),
        "expires_in": token_data.get("expires_in", 1800),
    }
    if token_data.get("scope"):
        inner["scope"] = token_data["scope"]
    wrapped = {
        "creation_timestamp": int(datetime.utcnow().timestamp()),
        "token": inner,
    }
    token_json = json.dumps(wrapped)

    # Get account info (Schwab API returns accountNumbers with hashValue)
    headers = {"Authorization": f"Bearer {access_token}"}
    acct_resp = requests.get(
        f"{SCHWAB_API_BASE}/trader/v1/accounts/accountNumbers",
        headers=headers,
        timeout=15,
    )

    account_hash = ""
    account_number = ""
    account_name = ""
    remote_accounts = []

    if acct_resp.status_code == 200:
        payload = acct_resp.json()
        if isinstance(payload, list):
            remote_accounts = [a for a in payload if isinstance(a, dict)]
        if remote_accounts:
            first = remote_accounts[0]
            account_hash = first.get("hashValue", "")
            account_number = str(first.get("accountNumber", ""))
    else:
        flash("Connected but could not fetch account list. Sync may be limited.", "warning")

    # Try to enrich with a nickname/description from /accounts (optional).
    if acct_resp.status_code == 200:
        acct_resp2 = requests.get(
            f"{SCHWAB_API_BASE}/trader/v1/accounts",
            headers=headers,
            params={"fields": "positions"},
            timeout=15,
        )
        if acct_resp2.status_code == 200:
            data = acct_resp2.json()
            if isinstance(data, list) and data:
                acc = data[0] if isinstance(data[0], dict) else {}
                sec = acc.get("securitiesAccount") if isinstance(acc, dict) else None
                if isinstance(sec, dict):
                    acc = sec
                if not account_hash:
                    account_hash = acc.get("hashValue", acc.get("accountNumber", ""))
                if not account_number:
                    account_number = str(acc.get("accountNumber", ""))
                nickname = acc.get("nickname") or acc.get("description") or ""
                if nickname and str(nickname).strip():
                    account_name = str(nickname).strip()

    if not account_hash:
        flash("Could not get account hash. Please try again or use CSV upload.", "danger")
        return redirect(url_for("profile", tab="account"))

    # Friendly, stable label: prefer Schwab nickname; else "Schwab ••••<last4>".
    if not account_name:
        last4 = (account_number or "")[-4:]
        if last4 and last4.isdigit():
            account_name = f"Schwab ••••{last4}"
        else:
            account_name = "Schwab Account"

    # Tenant isolation: BigQuery seed data is keyed by account label, so two
    # users with the same Schwab nickname (e.g. 'Brokerage') would silently
    # see each other's rows. The unique index in user_accounts forbids that
    # collision; we resolve it here by suffixing this user's username so the
    # downstream seed/dbt rows stay disjoint. The user can rename later.
    final_account_name = account_name or account_number
    if account_is_claimed_by_other(current_user.id, final_account_name):
        candidate = f"{final_account_name} ({current_user.username})"
        # Cap fallback collisions too — if even the suffixed name is taken,
        # append the user_id which is globally unique.
        if account_is_claimed_by_other(current_user.id, candidate):
            candidate = f"{final_account_name} (u{current_user.id})"
        app.logger.info(
            "Schwab account label collision: %r already linked to a different "
            "user; using %r for user_id=%s instead.",
            final_account_name, candidate, current_user.id,
        )
        flash(
            f"That Schwab nickname ({final_account_name!r}) is already in use "
            f"by another HappyTrader account, so we linked yours as "
            f"{candidate!r}. You can rename it later.",
            "info",
        )
        final_account_name = candidate

    save_schwab_connection(
        current_user.id,
        account_hash=account_hash,
        account_number=account_number,
        account_name=final_account_name,
        token_json=token_json,
    )
    try:
        add_account_for_user(current_user.id, final_account_name)
    except AccountClaimedError:
        # Race: another connect-callback between our check and insert took
        # this exact label. Bail to a guaranteed-unique fallback. We do not
        # silently inherit another user's BQ rows.
        fallback = f"{final_account_name} (u{current_user.id})"
        app.logger.warning(
            "Schwab account claim race for user_id=%s on %r; falling back to %r",
            current_user.id, final_account_name, fallback,
        )
        save_schwab_connection(
            current_user.id,
            account_hash=account_hash,
            account_number=account_number,
            account_name=fallback,
            token_json=token_json,
        )
        add_account_for_user(current_user.id, fallback)
        final_account_name = fallback

    # Re-auth covers every account under this Schwab login. Refresh tokens
    # on any pre-existing rows so a sync that targets a sibling connection
    # doesn't keep using the now-revoked old refresh token.
    update_schwab_tokens_for_user(current_user.id, token_json)

    has_more_remote = len(remote_accounts) > 1
    if has_more_remote:
        flash(
            "Schwab login connected. We saved one account — pick any others "
            "you want to bring over below.",
            "success",
        )
        return redirect(url_for("schwab_accounts"))

    flash("Schwab account connected. Use Sync now to pull your data.", "success")
    return redirect(url_for("profile", tab="account"))


def _sync_one_connection(user_id, conn_row, *, lookback_days):
    """Run ``_run_sync`` for a single Schwab connection and return a
    structured result. Wraps token-fetch, sync, and first-sync marking
    so both the per-account and "sync all" paths share the same
    semantics. Never raises — failures come back as ``{"ok": False,
    "error": "..."}`` so a multi-account loop can keep going past one
    bad connection.
    """
    label = (
        conn_row.get("display_nickname")
        or conn_row.get("account_name")
        or conn_row["account_number"]
    )
    account_number = conn_row["account_number"]
    first_done = bool(conn_row.get("schwab_first_sync_completed"))
    out = {
        "ok": False,
        "label": label,
        "account_number": account_number,
        "first_done_before": first_done,
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

    client = _get_schwab_client(user_id, account_number)
    if not client:
        out["error"] = "session_expired"
        return out

    try:
        result = _run_sync(
            user_id,
            client,
            account_number=account_number,
            transaction_lookback_days=lookback_days,
        )
        mark_schwab_first_sync_completed(user_id, account_number=account_number)
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
    except Exception as e:
        msg = str(e)
        if "401" in msg or "Unauthorized" in msg:
            out["error"] = "unauthorized"
        else:
            from app import app as _app
            _app.logger.exception(
                "Schwab sync failed for user_id=%s account_number=%s: %s",
                user_id, account_number, e,
            )
            out["error"] = "unknown"
    return out


@app.route("/schwab/sync", methods=["POST"])
@login_required
def schwab_sync():
    """Sync positions and transactions from Schwab to our data store.

    Form fields:
        sync_all: "1" to sync every linked Schwab connection in one
            click. Uses the routine (rolling) lookback for each so the
            request stays bounded; per-account "Sync this account" is
            still where users go for a one-off full-history refresh.
        account_number (optional, ignored when ``sync_all=1``): when
            provided, sync only that connection. If omitted and
            ``sync_all`` is not set, the user's first connection is
            synced (legacy single-account behaviour).
        sync_scope: "full" or "rolling" (only used on the first sync
            per connection — see ``schwab_first_sync_completed``).
        full_history_again: "1" to do a one-off full re-import after
            the first sync.
    """
    blocked = demo_block_writes("syncing Schwab data")
    if blocked:
        return blocked
    cfg = _schwab_config()
    if not cfg:
        flash("Schwab API is not configured.", "danger")
        return redirect(url_for("profile", tab="account"))

    sync_all = request.form.get("sync_all") == "1"

    if sync_all:
        return _schwab_sync_all_for_user(current_user.id)

    requested_account = (request.form.get("account_number") or "").strip() or None
    conn_row = get_schwab_connection(current_user.id, requested_account)
    if not conn_row:
        if requested_account:
            flash(
                "We couldn't find that Schwab connection on your account. "
                "Try the manage page and pick from your connected accounts.",
                "warning",
            )
            return redirect(url_for("schwab_accounts"))
        flash("No Schwab connection. Connect your account first.", "warning")
        return redirect(url_for("profile", tab="account"))

    first_done = bool(conn_row.get("schwab_first_sync_completed"))
    if not first_done:
        scope = (request.form.get("sync_scope") or "full").strip().lower()
        use_full_history = scope == "full"
    else:
        use_full_history = request.form.get("full_history_again") == "1"

    if use_full_history:
        lookback_days = SCHWAB_FULL_HISTORY_LOOKBACK_DAYS
    else:
        lookback_days = _schwab_transaction_lookback_days()

    res = _sync_one_connection(current_user.id, conn_row, lookback_days=lookback_days)

    if res["error"] == "session_expired":
        flash(
            "Could not open your Schwab session (invalid token or app credentials). "
            "Click Connect Schwab again to re-authorize. If it keeps failing, check Render logs.",
            "warning",
        )
        return redirect(url_for("profile", tab="account"))
    if res["error"] == "unauthorized":
        flash(
            "Schwab returned 401 (session expired or account key out of date). "
            "Try Sync now again; if it persists, use Connect Schwab in Settings to re-authorize.",
            "danger",
        )
        return redirect(url_for("profile", tab="account"))
    if res["error"]:
        flash(
            "Schwab sync didn't finish. Try again in a minute, or reconnect Schwab in Settings if it keeps happening.",
            "danger",
        )
        return redirect(url_for("profile", tab="account"))

    hr = res["history_rows"]
    cr = res["current_rows"]
    lb = res["lookback_days"]
    scope_phrase = (
        "This run included the longest history we request (up to about five years)."
        if lb >= SCHWAB_FULL_HISTORY_LOOKBACK_DAYS
        else f"This run included trades from the last {lb} days."
    )
    trade_word = "trade" if hr == 1 else "trades"
    pos_word = "position" if cr == 1 else "positions"
    summary = (
        f"Sync complete. We pulled {hr:,} {trade_word} and {cr} open {pos_word}. {scope_phrase}"
    )
    if res["github_pushed"]:
        flash(
            f"{summary} We’re processing your data now—this page will move along when it’s ready.",
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
        flash(
            f"{summary} We could not save this to the cloud: {res['github_error']}",
            "warning",
        )
    elif res["github_seed_push_skipped"]:
        flash(
            f"{summary} Live dashboard updates are not turned on for this environment.",
            "info",
        )

    return redirect(url_for("profile", tab="account"))


def _schwab_sync_all_for_user(user_id):
    """Iterate every Schwab connection for ``user_id`` and run a routine
    sync for each. Each connection commits to GitHub independently
    (one CSV merge + push per account); the GitHub Action workflow
    debounces the rebuilds so trailing pushes catch up. Surfaces a
    combined flash summary and redirects to ``sync_processing`` when
    at least one connection landed a push to GitHub.
    """
    rows = get_schwab_connections(user_id)
    if not rows:
        flash("No Schwab connection. Connect your account first.", "warning")
        return redirect(url_for("profile", tab="account"))

    lookback_days = _schwab_transaction_lookback_days()
    successes = []
    failures = []
    last_pushed_sha = None
    any_push_skipped = False
    any_first_run = False

    for conn_meta in rows:
        # ``get_schwab_connections`` returns lightweight metadata; we need
        # the token + account_hash from ``get_schwab_connection``.
        full = get_schwab_connection(user_id, conn_meta["account_number"])
        if not full:
            failures.append({"label": conn_meta.get("account_name") or conn_meta["account_number"], "reason": "missing"})
            continue
        if not full.get("schwab_first_sync_completed"):
            any_first_run = True
        res = _sync_one_connection(user_id, full, lookback_days=lookback_days)
        if res["ok"]:
            successes.append(res)
            if res["github_pushed"] and res["github_head_sha"]:
                last_pushed_sha = res["github_head_sha"]
            if res["github_seed_push_skipped"]:
                any_push_skipped = True
        else:
            failures.append({"label": res["label"], "reason": res["error"] or "unknown"})

    parts = []
    if successes:
        per_account = ", ".join(
            f"{s['label']}: {s['history_rows']:,} {'trade' if s['history_rows'] == 1 else 'trades'}, "
            f"{s['current_rows']} open {'position' if s['current_rows'] == 1 else 'positions'}"
            for s in successes
        )
        parts.append(f"Sync complete. {per_account}.")
    if failures:
        per_failure = ", ".join(f"{f['label']} ({f['reason']})" for f in failures)
        parts.append(f"These didn't finish: {per_failure}.")
    summary = " ".join(parts) if parts else "Sync ran but nothing landed."

    any_pushed = any(s["github_pushed"] for s in successes)
    if any_pushed:
        flash(
            f"{summary} We’re processing your data now—this page will move along when it’s ready.",
            "success" if not failures else "warning",
        )
        qp = {}
        if last_pushed_sha:
            qp["sha"] = last_pushed_sha
        if any_first_run:
            qp["first"] = 1
        return redirect(url_for("sync_processing", **qp))

    if failures and not successes:
        flash(summary, "danger")
    elif any_push_skipped:
        flash(f"{summary} Live dashboard updates are not turned on for this environment.", "info")
    else:
        flash(summary, "warning" if failures else "info")

    return redirect(url_for("profile", tab="account"))


# ---------------------------------------------------------------------------
# Multi-account management — pull additional accounts under the same Schwab
# OAuth grant after the initial connect.
# ---------------------------------------------------------------------------

def _schwab_default_account_label(account_number):
    """Friendly fallback name when Schwab does not return a nickname."""
    last4 = (account_number or "")[-4:]
    if last4 and last4.isdigit():
        return f"Schwab ••••{last4}"
    return f"Schwab {account_number}"


def _schwab_fetch_remote_accounts(client):
    """
    Discover every Schwab account reachable with the user's current OAuth
    grant. Returns a list of dicts:
        {"account_number": str, "account_hash": str, "account_name": str|None}

    Falls back to the bare /accountNumbers list when the richer /accounts
    endpoint is unavailable so we still get the numbers/hashes needed to
    save a connection. Empty list on auth failure.
    """
    try:
        nums_resp = _schwab_resp_with_refresh(client, client.get_account_numbers)
        nums_payload = nums_resp.json()
    except Exception:
        app.logger.exception("Schwab account discovery (account numbers) failed")
        return []

    if not isinstance(nums_payload, list):
        return []

    accounts = []
    seen_numbers = set()
    for row in nums_payload:
        if not isinstance(row, dict):
            continue
        number = str(row.get("accountNumber", "")).strip()
        if not number or number in seen_numbers:
            continue
        seen_numbers.add(number)
        accounts.append({
            "account_number": number,
            "account_hash": (row.get("hashValue") or "").strip(),
            "account_name": None,
        })

    if not accounts:
        return accounts

    # Best-effort enrichment: pull human nicknames from /accounts. Failures
    # are logged but don't block the page.
    try:
        full_resp = _schwab_resp_with_refresh(
            client, lambda: client.get_accounts()
        )
        full_payload = full_resp.json()
    except Exception:
        app.logger.exception("Schwab account discovery (accounts enrich) failed")
        full_payload = None

    if isinstance(full_payload, list):
        nick_by_number = {}
        for entry in full_payload:
            if not isinstance(entry, dict):
                continue
            sec = entry.get("securitiesAccount")
            sec = sec if isinstance(sec, dict) else entry
            num = str(sec.get("accountNumber", "")).strip()
            if not num:
                continue
            nick = (
                sec.get("nickname")
                or sec.get("displayAcctId")
                or sec.get("description")
                or ""
            )
            nick = str(nick).strip()
            if nick:
                nick_by_number[num] = nick
        for acc in accounts:
            if acc["account_number"] in nick_by_number:
                acc["account_name"] = nick_by_number[acc["account_number"]]

    return accounts


@app.route("/schwab/accounts")
@login_required
def schwab_accounts():
    """
    Manage which Schwab accounts under the user's OAuth grant are
    connected. Lets the trader bring over an account they skipped on
    the initial connect.
    """
    cfg = _schwab_config()
    if not cfg:
        flash("Schwab API is not configured.", "danger")
        return redirect(url_for("profile", tab="account"))

    connected_rows = list(get_schwab_connections(current_user.id) or [])
    if not connected_rows:
        flash(
            "Connect a Schwab login first, then you can pick which accounts to sync.",
            "warning",
        )
        return redirect(url_for("profile", tab="account"))

    client = _get_schwab_client(current_user.id)
    if client is None:
        flash(
            "We couldn't open your Schwab session. Click Connect Schwab to "
            "re-authorize so we can list your accounts.",
            "warning",
        )
        return redirect(url_for("profile", tab="account"))

    remote = _schwab_fetch_remote_accounts(client)
    connected_numbers = {
        str(r.get("account_number") or "").strip() for r in connected_rows
    }
    available = [
        a for a in remote
        if a["account_number"] and a["account_number"] not in connected_numbers
    ]
    # Decorate the connected list with default-display names so the template
    # doesn't have to do that work. ``account_name`` stays as the BigQuery
    # tenancy key; ``display_nickname`` is the user-editable label that the
    # template shows as the primary heading.
    connected = []
    for row in connected_rows:
        number = str(row.get("account_number") or "")
        label = (row.get("account_name") or "").strip() or _schwab_default_account_label(number)
        nickname = (row.get("display_nickname") or "").strip()
        connected.append({
            "account_number": number,
            "account_name": label,
            "display_nickname": nickname,
            "first_sync_completed": bool(row.get("schwab_first_sync_completed")),
        })

    discovery_failed = (not remote) and bool(connected_rows)

    return render_template(
        "schwab_accounts.html",
        title="Schwab accounts",
        connected_accounts=connected,
        available_accounts=available,
        discovery_failed=discovery_failed,
    )


@app.route("/schwab/accounts/add", methods=["POST"])
@login_required
def schwab_accounts_add():
    """
    Save a previously-skipped Schwab account using the existing OAuth
    grant. The new connection reuses the token from any current
    connection (one Schwab login authorizes every account under it).
    """
    blocked = demo_block_writes("adding a Schwab account")
    if blocked:
        return blocked
    cfg = _schwab_config()
    if not cfg:
        flash("Schwab API is not configured.", "danger")
        return redirect(url_for("profile", tab="account"))

    requested_number = (request.form.get("account_number") or "").strip()
    nickname = (request.form.get("account_name") or "").strip()
    if not requested_number:
        flash("Pick an account to add.", "warning")
        return redirect(url_for("schwab_accounts"))

    existing = get_schwab_connection(current_user.id, requested_number)
    if existing:
        flash("That Schwab account is already connected.", "info")
        return redirect(url_for("schwab_accounts"))

    primary = get_schwab_connection(current_user.id)
    if not primary:
        flash(
            "Connect a Schwab login first, then you can add additional accounts.",
            "warning",
        )
        return redirect(url_for("profile", tab="account"))

    client = _get_schwab_client(current_user.id)
    if client is None:
        flash(
            "We couldn't open your Schwab session. Click Connect Schwab to "
            "re-authorize, then try again.",
            "warning",
        )
        return redirect(url_for("profile", tab="account"))

    remote = _schwab_fetch_remote_accounts(client)
    match = next(
        (a for a in remote if a["account_number"] == requested_number), None
    )
    if not match:
        # Don't trust the form alone — the account number must come from the
        # OAuth grant. Anything else would let a user attach a number they
        # don't actually own.
        flash(
            "That account isn't visible under your Schwab login. "
            "Refresh this page or re-connect Schwab.",
            "danger",
        )
        return redirect(url_for("schwab_accounts"))

    account_hash = (match.get("account_hash") or "").strip()
    if not account_hash:
        flash(
            "Schwab didn't return a routing key for that account. "
            "Try again in a moment.",
            "danger",
        )
        return redirect(url_for("schwab_accounts"))

    label = nickname or (match.get("account_name") or "").strip()
    if not label:
        label = _schwab_default_account_label(requested_number)

    save_schwab_connection(
        current_user.id,
        account_hash=account_hash,
        account_number=requested_number,
        account_name=label,
        token_json=primary["token_json"],
    )
    add_account_for_user(current_user.id, label)

    flash(
        f"Added {label}. Use Sync now on this page to pull its history.",
        "success",
    )
    return redirect(url_for("schwab_accounts"))


@app.route("/schwab/accounts/nickname", methods=["POST"])
@login_required
def schwab_accounts_nickname():
    """
    Save (or clear) a display-only nickname for one of the user's Schwab
    connections. The underlying ``account_name`` — which is the BigQuery
    tenancy key — is intentionally untouched so existing warehouse rows
    keep matching this user. The nickname only changes how the account
    is labeled in the front end (manage page, sync card, etc.).
    """
    blocked = demo_block_writes("renaming Schwab connections")
    if blocked:
        return blocked
    requested_number = (request.form.get("account_number") or "").strip()
    nickname_raw = request.form.get("display_nickname", "")
    if not requested_number:
        flash("We couldn't tell which account you wanted to rename.", "warning")
        return redirect(url_for("schwab_accounts"))

    existing = get_schwab_connection(current_user.id, requested_number)
    if not existing:
        # Defense: only let a user rename a connection they actually own.
        flash(
            "That Schwab account isn't connected to your login. "
            "Refresh this page and try again.",
            "warning",
        )
        return redirect(url_for("schwab_accounts"))

    update_schwab_connection_nickname(
        current_user.id, requested_number, nickname_raw
    )
    cleaned = (nickname_raw or "").strip()
    if cleaned:
        flash(f"Saved nickname “{cleaned[:80]}”.", "success")
    else:
        flash("Cleared nickname.", "info")
    return redirect(url_for("schwab_accounts"))


def _run_sync(user_id, client, *, account_number=None, transaction_lookback_days=None):
    """
    Fetch positions and transactions from Schwab, map to our schema,
    and write to the configured output (GitHub seeds or BigQuery).
    Returns dict with history_rows, current_rows, lookback_days.

    account_number: which Schwab connection to sync. If omitted, the
        first connection for the user is used (legacy single-account
        behaviour). Required when the user has multiple connections so
        each row is synced exactly once.
    transaction_lookback_days: if set (e.g. from UI), use it; else env SCHWAB_SYNC_TRANSACTION_DAYS
    (used by cron CLI). Clamped to 1..1825.
    """
    conn_data = get_schwab_connection(user_id, account_number)
    if not conn_data:
        raise ValueError("No Schwab connection")

    account_hash = conn_data["account_hash"]
    account_name = conn_data.get("account_name") or conn_data["account_number"]
    account_number = conn_data["account_number"]

    # Stale hashValue in DB → 401 on /accounts/{hash}. Refresh from accountNumbers first.
    account_hash = _sync_account_hash_from_numbers(
        client, user_id, account_number, account_hash
    )

    # Fetch account with positions (Account.Fields.POSITIONS = 'positions')
    try:
        from schwab.client import Client
        fields = [Client.Account.Fields.POSITIONS]
    except Exception:
        fields = ["positions"]
    acct_resp = _schwab_resp_with_refresh(
        client,
        lambda: client.get_account(account_hash, fields=fields),
    )
    acct_data = acct_resp.json()

    # Open positions — shaped to match the current_positions.csv column family
    # so the manual upload path and Schwab sync both feed dbt's stg_current.
    # Raw-API fields are kept in parallel (`_api_*`) for balance calculations.
    open_positions_current = []  # current_positions.csv shape (seed rows)
    open_positions_api = []       # raw API view used by balance calc
    for sec in acct_data.get("securitiesAccount", {}).get("positions", []):
        inst = sec.get("instrument", {}) or {}
        sym = inst.get("symbol", "")
        desc = inst.get("description", sym)
        qty = sec.get("longQuantity", 0) or -(sec.get("shortQuantity", 0) or 0)
        avg = sec.get("averagePrice", 0) or 0
        mv = sec.get("marketValue", 0) or 0
        inst_type = str(inst.get("assetType", "EQUITY")).upper()
        cb = _schwab_position_cost_basis(sec, inst, qty, avg, inst_type)

        open_positions_api.append({
            "symbol": sym,
            "market_value": mv,
            "cost_basis": cb,
        })

        security_type = _schwab_asset_type_to_security_type(inst_type)
        gl_dollar = ""
        gl_percent = ""
        try:
            mv_f = float(mv or 0)
            cb_f = float(cb or 0)
            if cb_f:
                gl_dollar = round(mv_f - cb_f, 4)
                gl_percent = round(100.0 * (mv_f - cb_f) / abs(cb_f), 4)
        except (TypeError, ValueError):
            pass

        open_positions_current.append({
            "Symbol": sym,
            "Description": desc,
            "Quantity": qty,
            "Price": avg,
            "market_value": mv,
            "cost_bases": cb,
            "gain_or_loss_dollat": gl_dollar,
            "gain_or_loss_percent": gl_percent,
            "security_type": security_type,
        })

    if transaction_lookback_days is not None:
        lookback = max(1, min(int(transaction_lookback_days), 1825))
    else:
        lookback = _schwab_transaction_lookback_days()
    end_date = date.today()
    start_date = end_date - timedelta(days=lookback)
    # Schwab 400s if startDate/endDate span a single long range; fetch in chunks.
    tx_list = _schwab_fetch_all_transaction_items(
        client, account_hash, start_date, end_date
    )

    transactions = []
    for tx in tx_list:
        transactions.extend(_schwab_trade_rows(tx))

    # Cross-transaction wash-pair cleanup.
    #
    # Schwab's assignment flow frequently ships *two* separate TRADE
    # transactions for the same underlying on the same day: one with a
    # negative-amount equity leg (the real sell at strike) and one with the
    # equal-and-opposite positive-amount equity leg (a bookkeeping journal).
    # Left in place, the pair (Buy, Sell) at identical price nets to zero and
    # the strategy engine misses the covered-call exit entirely — cost basis
    # on the equity session never closes and short calls get mis-classified
    # as Naked Call.
    #
    # Real day-trades almost never buy and sell the same symbol at exactly
    # the same price, so we drop the Buy side of any (symbol, qty, price,
    # date) pair where amounts cancel. Put-assignment cases would need the
    # opposite rule (drop Sell, keep Buy); we haven't hit one in this
    # dataset yet, so leave that as an explicit TODO.
    transactions = _schwab_collapse_wash_pairs(transactions)

    import pandas as pd

    tx_df = pd.DataFrame(transactions) if transactions else None
    skip_tx = tx_df is None or tx_df.empty

    current_df = (
        pd.DataFrame(open_positions_current) if open_positions_current else pd.DataFrame()
    )
    bal_seed = _schwab_build_balance_seed_rows(acct_data, open_positions_api)
    balances_df = (
        pd.DataFrame(bal_seed)
        if bal_seed
        else pd.DataFrame(
            columns=[
                "row_type",
                "market_value",
                "cost_basis",
                "unrealized_pnl",
                "unrealized_pnl_pct",
                "percent_of_account",
            ]
        )
    )

    github_pushed = False
    github_error = None
    github_head_sha = None
    ok_cfg = False
    github_skip_reason = None
    from app.upload import merge_and_push_seeds, _upload_github_config_ok

    ok_cfg, _cfg_err = _upload_github_config_ok()
    if not ok_cfg:
        github_skip_reason = _cfg_err or "GitHub seed push not configured."
        # WARNING (not INFO) so cron/web logs surface a misconfig that
        # would otherwise look identical to a successful no-op. We hit
        # this on the Render cron when GITHUB_PAT is set but
        # GITHUB_REPO is malformed (or vice versa) — the difference
        # used to be invisible in the CLI summary.
        app.logger.warning(
            "Schwab sync: skipping GitHub push for user_id=%s account=%s — %s",
            user_id, account_name, github_skip_reason,
        )
    if ok_cfg:
        uname = "user"
        u = User.get_by_id(user_id)
        if u:
            uname = u.username
        if skip_tx:
            commit_msg = (
                f"Schwab sync ({uname}): positions only "
                f"({len(open_positions_current)} lines) ({account_name})"
            )
        else:
            commit_msg = (
                f"Schwab sync ({uname}): {len(transactions)} tx, "
                f"{len(open_positions_current)} open lines ({account_name})"
            )
        ok, err, _hr, _cr, github_head_sha = merge_and_push_seeds(
            account_name,
            tx_df,
            current_df,
            commit_message=commit_msg,
            user_id=user_id,
            skip_history=skip_tx,
            balances_df=balances_df,
        )
        github_pushed = ok
        github_error = err if not ok else None
    else:
        github_error = None

    # Local CSV fallback / debugging (ephemeral on Render unless storage mounted)
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "schwab_sync")
    os.makedirs(out_dir, exist_ok=True)
    safe_name = "".join(c if c.isalnum() else "_" for c in account_name)[:50]

    if tx_df is not None and not tx_df.empty:
        tx_df.to_csv(
            os.path.join(out_dir, f"{safe_name}_trade_history_delta.csv"),
            index=False,
        )
    if current_df is not None and not current_df.empty:
        current_df.to_csv(
            os.path.join(out_dir, f"{safe_name}_current_positions_delta.csv"),
            index=False,
        )
    if balances_df is not None and not balances_df.empty:
        balances_df.to_csv(
            os.path.join(out_dir, f"{safe_name}_schwab_account_balances.csv"),
            index=False,
        )

    return {
        "history_rows": len(transactions),
        "current_rows": len(open_positions_current),
        "lookback_days": lookback,
        "output_dir": out_dir,
        "github_pushed": github_pushed,
        "github_error": github_error,
        "github_head_sha": github_head_sha,
        "github_seed_push_skipped": not ok_cfg,
        "github_skip_reason": github_skip_reason,
    }


def _schwab_action_from_effect(asset_type, signed_amount, position_effect, instruction=""):
    """
    Determine brokerage-export action from Schwab v1 trader transferItem fields.

    Schwab trader-v1 returns transferItems with 'positionEffect' ("OPENING" |
    "CLOSING") and a signed 'amount' (positive = received, negative = delivered).
    instruction is a loose hint ("BUY", "SELL", "BUY_TO_OPEN", ...) that some
    responses also include; used as a tiebreaker.
    """
    at = str(asset_type or "").upper()
    pe = str(position_effect or "").upper()
    ins = str(instruction or "").upper()
    try:
        amt = float(signed_amount or 0)
    except (TypeError, ValueError):
        amt = 0.0
    bought = amt > 0 if amt != 0 else ("BUY" in ins and "SELL" not in ins)

    is_option = at == "OPTION"
    if is_option:
        if pe == "OPENING" or "OPEN" in ins:
            return "Buy to Open" if bought else "Sell to Open"
        if pe == "CLOSING" or "CLOSE" in ins:
            return "Buy to Close" if bought else "Sell to Close"
        return "Buy to Open" if bought else "Sell to Open"
    return "Buy" if bought else "Sell"


def _schwab_trade_rows(tx):
    """
    Turn one Schwab transaction (trader v1 shape) into zero or more brokerage-
    export-shaped rows. Trader v1 puts one or more security legs in
    'transferItems'; fee legs have no 'instrument' and are skipped.

    Covered-call assignments were previously double-counted: Schwab emits
    multiple equity transferItems per assignment (a delivery leg plus a
    zero-net bookkeeping pair), which became three rows (Sell + Buy + Sell)
    for a single real sale. We now group transferItems by
    (symbol, price, asset type) and sum signed amount/cost so wash pairs
    cancel. Option legs tied to assignment / exercise / expiration get the
    matching manual-export action label ("Assigned" / "Exchange or Exercise"
    / "Expired") instead of being forced into Buy to Close / Sell to Close.

    Falls back to the legacy 'transactionItem' shape if 'transferItems' is
    absent, so partial responses and older sandboxes still work.
    """
    if not isinstance(tx, dict):
        return []
    if tx.get("type") != "TRADE":
        return []

    dt = (
        tx.get("tradeDate")
        or tx.get("time")
        or tx.get("transactionDate")
        or tx.get("settlementDate", "")
    )
    if isinstance(dt, (int, float)):
        dt = str(int(dt))
    date_str = _format_date(dt)

    items = tx.get("transferItems")
    if not isinstance(items, list) or not items:
        ti = tx.get("transactionItem")
        items = [ti] if isinstance(ti, dict) else []

    subtype_raw = str(tx.get("transactionSubType") or "").upper()
    is_assignment = (
        "ASSIGN" in subtype_raw or subtype_raw in {"AS", "OA"}
    )
    is_exercise = (
        "EXERC" in subtype_raw or subtype_raw in {"EX", "OE"}
    )
    is_expiration = (
        "EXPIR" in subtype_raw or subtype_raw in {"OX", "EXP"}
    )

    from collections import OrderedDict

    buckets = OrderedDict()
    for it in items:
        if not isinstance(it, dict):
            continue
        inst = it.get("instrument")
        if not isinstance(inst, dict):
            continue
        sym = inst.get("symbol") or ""
        if not str(sym).strip():
            continue

        try:
            amt = float(it.get("amount") or 0)
        except (TypeError, ValueError):
            amt = 0.0
        try:
            price = float(it.get("price") or 0)
        except (TypeError, ValueError):
            price = 0.0
        cost_raw = it.get("cost")
        if cost_raw is None:
            cost = 0.0
            has_cost = False
        else:
            try:
                cost = float(cost_raw)
                has_cost = True
            except (TypeError, ValueError):
                cost = 0.0
                has_cost = False

        asset_type = inst.get("assetType", "") or ""
        key = (str(sym).strip(), round(price, 6), str(asset_type).upper())
        b = buckets.setdefault(
            key,
            {
                "symbol": str(sym).strip(),
                "description": inst.get("description", sym),
                "asset_type": asset_type,
                "price": price,
                "amount": 0.0,
                "cost": 0.0,
                "has_cost": False,
                "position_effect": "",
                "instruction": (
                    it.get("instruction") or tx.get("transactionSubType") or ""
                ),
            },
        )
        b["amount"] += amt
        if has_cost:
            b["cost"] += cost
            b["has_cost"] = True
        # prefer a non-empty positionEffect on any leg in the bucket
        if not b["position_effect"]:
            b["position_effect"] = it.get("positionEffect", "") or ""

    rows = []
    for b in buckets.values():
        # wash pair: legs cancel both in quantity and cash
        if abs(b["amount"]) < 1e-9 and (not b["has_cost"] or abs(b["cost"]) < 1e-9):
            continue

        asset_type_upper = str(b["asset_type"] or "").upper()
        is_option = asset_type_upper == "OPTION"

        if is_option and is_assignment:
            action = "Assigned"
        elif is_option and is_exercise:
            action = "Exchange or Exercise"
        elif is_option and is_expiration:
            action = "Expired"
        else:
            action = _schwab_action_from_effect(
                b["asset_type"],
                b["amount"],
                b["position_effect"],
                b["instruction"],
            )

        if b["has_cost"]:
            seed_amount = b["cost"]
        else:
            seed_amount = tx.get("netAmount") or 0

        rows.append(
            {
                "transaction_date": date_str,
                "action": action,
                "symbol": b["symbol"],
                "description": b["description"],
                "quantity": abs(b["amount"]),
                "price": abs(b["price"]) if b["price"] else "",
                "fees": "",
                "amount": seed_amount,
            }
        )
    return rows


def _schwab_collapse_wash_pairs(rows):
    """
    Drop the bookkeeping leg of same-day covered-call assignment wash pairs.

    A wash pair is (Buy, Sell) rows on the same date, same symbol, same
    quantity, same price, with amounts that are exact negatives of each
    other. Real day-trades effectively never buy and sell the same symbol
    at exactly the same price, so this signature is specific to the
    journal entry Schwab emits alongside a real assignment sale.

    We keep the Sell (economically real: shares went out, cash came in at
    strike) and drop the Buy (journal noise). This is the correct rule for
    call assignments; for put assignments the real leg is the Buy — we
    haven't observed that case yet in this codebase but flag it here.
    """
    if not rows:
        return rows
    from collections import defaultdict

    groups = defaultdict(list)
    for i, r in enumerate(rows):
        sym = str(r.get("symbol") or "").strip()
        try:
            qty = float(r.get("quantity") or 0)
        except (TypeError, ValueError):
            qty = 0.0
        try:
            price = float(r.get("price") or 0)
        except (TypeError, ValueError):
            price = 0.0
        date = str(r.get("transaction_date") or "").strip()
        if not sym or not date or qty == 0 or price == 0:
            continue
        key = (sym, round(qty, 6), round(price, 6), date)
        groups[key].append(i)

    drop = set()
    for idxs in groups.values():
        if len(idxs) < 2:
            continue
        buys = [i for i in idxs if str(rows[i].get("action") or "") == "Buy"]
        sells = [i for i in idxs if str(rows[i].get("action") or "") == "Sell"]
        while buys and sells:
            bi = buys.pop(0)
            si = sells.pop(0)
            try:
                b_amt = float(rows[bi].get("amount") or 0)
                s_amt = float(rows[si].get("amount") or 0)
            except (TypeError, ValueError):
                continue
            if abs(b_amt + s_amt) < 0.01:
                drop.add(bi)  # keep Sell (economic), drop Buy (journal)

    if not drop:
        return rows
    return [r for i, r in enumerate(rows) if i not in drop]


def _format_date(dt):
    """Convert Schwab date to MM/DD/YYYY."""
    if not dt:
        return ""
    if isinstance(dt, (int, float)):
        # Sometimes milliseconds
        from datetime import datetime
        try:
            d = datetime.fromtimestamp(int(dt) / 1000 if dt > 1e10 else int(dt))
            return d.strftime("%m/%d/%Y")
        except Exception:
            return str(dt)
    s = str(dt)[:10]
    if "-" in s:
        try:
            y, m, d = s.split("-")
            return f"{int(m):02d}/{int(d):02d}/{y}"
        except Exception:
            pass
    return s
