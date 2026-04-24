"""Schwab API — OAuth connection and sync of positions/transactions."""
import json
import os
import secrets
import tempfile
import time
from datetime import datetime, date, timedelta
from urllib.parse import urlencode

from flask import redirect, request, url_for, flash, session
from flask_login import login_required, current_user

from app import app
from app.models import (
    User,
    get_schwab_connection,
    get_schwab_connections,
    mark_schwab_first_sync_completed,
    save_schwab_connection,
    update_schwab_account_hash,
    update_schwab_token,
    add_account_for_user,
)

# OAuth state stored in Flask session

SCHWAB_AUTH_URL = "https://api.schwabapi.com/v1/oauth/authorize"
SCHWAB_TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
SCHWAB_API_BASE = "https://api.schwabapi.com"

# UI "import full history" uses this many days (cap for API + dbt); routine sync uses env default.
SCHWAB_FULL_HISTORY_LOOKBACK_DAYS = 1825


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
    from Schwab currentBalances. open_position_rows: API positions only (no
    summary rows). Account column is added by merge_and_push_schwab_seeds.
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
    """Return (app_key, app_secret, callback_url) or None if not configured."""
    key = os.environ.get("SCHWAB_APP_KEY", "").strip()
    secret = os.environ.get("SCHWAB_APP_SECRET", "").strip()
    callback = os.environ.get("SCHWAB_CALLBACK_URL", "").strip()
    if not key or not secret or not callback:
        return None
    return (key, secret, callback)


def _get_schwab_client(user_id, account_number=None):
    """
    Create a schwab-py client using stored token.
    Uses client_from_access_functions with DB-backed token read/write.
    """
    conn_data = get_schwab_connection(user_id, account_number)
    if not conn_data:
        return None

    cfg = _schwab_config()
    if not cfg:
        return None

    app_key, app_secret, _ = cfg

    def token_read():
        c = get_schwab_connection(user_id, conn_data["account_number"])
        if not c:
            return None
        raw = json.loads(c["token_json"])
        wrapped = _wrap_schwab_token_for_py(raw)
        if wrapped is not raw:
            update_schwab_token(
                user_id, conn_data["account_number"], json.dumps(wrapped)
            )
        return wrapped

    def token_write(token, *args, **kwargs):
        # schwab-py passes the wrapped {creation_timestamp, token} dict on refresh.
        # Newer authlib/oauth passes through extra kwargs (e.g. refresh_token=...) — ignore them.
        update_schwab_token(user_id, conn_data["account_number"], json.dumps(token))

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
        flash(f"Schwab authorization failed: {error}", "danger")
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
        flash(f"Failed to get token from Schwab: {resp.text[:200]}", "danger")
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
    account_name = "Schwab Account"

    if acct_resp.status_code == 200:
        accounts = acct_resp.json()
        if isinstance(accounts, list) and accounts:
            first = accounts[0]
            account_hash = first.get("hashValue", "")
            account_number = str(first.get("accountNumber", ""))
            if not account_number:
                account_number = account_hash[:8] if account_hash else "schwab"
    else:
        flash("Connected but could not fetch account list. Sync may be limited.", "warning")

    if not account_hash and acct_resp.status_code == 200:
        # Fallback: accounts endpoint may return different structure
        acct_resp2 = requests.get(
            f"{SCHWAB_API_BASE}/trader/v1/accounts",
            headers=headers,
            params={"fields": "positions"},
            timeout=15,
        )
        if acct_resp2.status_code == 200:
            data = acct_resp2.json()
            if isinstance(data, list) and data:
                acc = data[0]
                account_hash = acc.get("hashValue", acc.get("accountNumber", ""))
                account_number = str(acc.get("accountNumber", account_hash))
                account_name = acc.get("nickname", acc.get("description", account_number))

    if not account_hash:
        flash("Could not get account hash. Please try again or use CSV upload.", "danger")
        return redirect(url_for("profile", tab="account"))

    save_schwab_connection(
        current_user.id,
        account_hash=account_hash,
        account_number=account_number,
        account_name=account_name,
        token_json=token_json,
    )
    add_account_for_user(current_user.id, account_name or account_number)

    flash("Schwab account connected. Use Sync now to pull your data.", "success")
    return redirect(url_for("profile", tab="account"))


@app.route("/schwab/sync", methods=["POST"])
@login_required
def schwab_sync():
    """Sync positions and transactions from Schwab to our data store."""
    cfg = _schwab_config()
    if not cfg:
        flash("Schwab API is not configured.", "danger")
        return redirect(url_for("profile", tab="account"))

    conn_row = get_schwab_connection(current_user.id)
    if not conn_row:
        flash("No Schwab connection. Connect your account first.", "warning")
        return redirect(url_for("profile", tab="account"))

    client = _get_schwab_client(current_user.id)
    if not client:
        flash(
            "Could not open your Schwab session (invalid token or app credentials). "
            "Click Connect Schwab again to re-authorize. If it keeps failing, check Render logs.",
            "warning",
        )
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

    try:
        result = _run_sync(current_user.id, client, transaction_lookback_days=lookback_days)
        hr = result.get("history_rows", 0)
        cr = result.get("current_rows", 0)
        lb = result.get("lookback_days", lookback_days)
        mark_schwab_first_sync_completed(current_user.id)
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
        if result.get("github_pushed"):
            flash(
                f"{summary} We’re processing your data now—this page will move along when it’s ready.",
                "success",
            )
            h = (result.get("github_head_sha") or "").strip()
            if h:
                return redirect(url_for("sync_processing", sha=h))
            return redirect(url_for("sync_processing"))
        if result.get("github_error"):
            flash(
                f"{summary} We could not save this to the cloud: {result['github_error']}",
                "warning",
            )
        elif result.get("github_seed_push_skipped"):
            flash(
                f"{summary} Live dashboard updates are not turned on for this environment.",
                "info",
            )
    except Exception as e:
        msg = str(e)
        if "401" in msg or "Unauthorized" in msg:
            flash(
                "Schwab returned 401 (session expired or account key out of date). "
                "Try Sync now again; if it persists, use Connect Schwab in Settings to re-authorize.",
                "danger",
            )
        else:
            flash(f"Sync failed: {e}", "danger")

    return redirect(url_for("profile", tab="account"))


def _run_sync(user_id, client, transaction_lookback_days=None):
    """
    Fetch positions and transactions from Schwab, map to our schema,
    and write to the configured output (GitHub seeds or BigQuery).
    Returns dict with history_rows, current_rows, lookback_days.

    transaction_lookback_days: if set (e.g. from UI), use it; else env SCHWAB_SYNC_TRANSACTION_DAYS
    (used by cron CLI). Clamped to 1..1825.
    """
    # Get account hash
    conn_data = get_schwab_connection(user_id)
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

    # Open positions — native Schwab columns (see schwab_open_positions.csv)
    open_positions = []
    for sec in acct_data.get("securitiesAccount", {}).get("positions", []):
        inst = sec.get("instrument", {}) or {}
        sym = inst.get("symbol", "")
        desc = inst.get("description", sym)
        qty = sec.get("longQuantity", 0) or -(sec.get("shortQuantity", 0) or 0)
        avg = sec.get("averagePrice", 0) or 0
        mv = sec.get("marketValue", 0) or 0
        inst_type = inst.get("assetType", "EQUITY")
        cb = _schwab_position_cost_basis(sec, inst, qty, avg, inst_type)
        open_positions.append({
            "symbol": sym,
            "description": desc,
            "quantity": qty,
            "average_price": avg,
            "market_value": mv,
            "cost_basis": cb,
            "asset_type": str(inst_type),
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
        if tx.get("type") != "TRADE":
            continue
        dt = tx.get("transactionDate") or tx.get("settlementDate", "")
        if isinstance(dt, (int, float)):
            dt = str(int(dt))
        # Map Schwab transaction to our history format
        item = tx.get("transactionItem", {}) or {}
        inst = item.get("instrument", {}) or {}
        sym = inst.get("symbol", "")
        desc = inst.get("description", sym)
        qty = item.get("amount", 0) or 0
        price = item.get("price", 0) or 0
        # Schwab uses different action names
        instruction = tx.get("transactionSubType", tx.get("type", ""))
        action = _map_schwab_action(instruction, item)
        amount = tx.get("netAmount", 0) or (qty * price)
        transactions.append({
            "transaction_date": _format_date(dt),
            "action": action,
            "symbol": sym,
            "description": desc,
            "quantity": abs(qty),
            "price": abs(price) if price else "",
            "fees": "",
            "amount": amount,
        })

    import pandas as pd

    tx_df = pd.DataFrame(transactions) if transactions else None
    skip_tx = tx_df is None or tx_df.empty

    open_df = pd.DataFrame(open_positions) if open_positions else None
    bal_seed = _schwab_build_balance_seed_rows(acct_data, open_positions)
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
    from app.upload import merge_and_push_schwab_seeds, _upload_github_config_ok

    ok_cfg, _cfg_err = _upload_github_config_ok()
    if ok_cfg:
        uname = "user"
        u = User.get_by_id(user_id)
        if u:
            uname = u.username
        if skip_tx:
            commit_msg = (
                f"Schwab sync ({uname}): open positions + balances only "
                f"({len(open_positions)} lines) ({account_name})"
            )
        else:
            commit_msg = (
                f"Schwab sync ({uname}): {len(transactions)} tx, "
                f"{len(open_positions)} open lines ({account_name})"
            )
        (
            ok,
            err,
            _open_n,
            _tx_n,
            _bal_n,
            github_head_sha,
        ) = merge_and_push_schwab_seeds(
            account_name,
            open_df,
            balances_df,
            tx_df,
            commit_message=commit_msg,
            user_id=user_id,
            skip_transactions=skip_tx,
        )
        github_pushed = ok
        github_error = err if not ok else None
    else:
        github_error = None

    # Local CSV fallback / debugging (ephemeral on Render unless you mount storage)
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "schwab_sync")
    os.makedirs(out_dir, exist_ok=True)
    safe_name = "".join(c if c.isalnum() else "_" for c in account_name)[:50]

    if tx_df is not None and not tx_df.empty:
        tx_path = os.path.join(out_dir, f"{safe_name}_schwab_transactions.csv")
        tx_df.to_csv(tx_path, index=False)

    if open_df is not None and not open_df.empty:
        op_path = os.path.join(out_dir, f"{safe_name}_schwab_open_positions.csv")
        open_df.to_csv(op_path, index=False)

    if balances_df is not None and not balances_df.empty:
        bal_path = os.path.join(out_dir, f"{safe_name}_schwab_account_balances.csv")
        balances_df.to_csv(bal_path, index=False)

    return {
        "history_rows": len(transactions),
        "current_rows": len(open_positions),
        "lookback_days": lookback,
        "output_dir": out_dir,
        "github_pushed": github_pushed,
        "github_error": github_error,
        "github_head_sha": github_head_sha,
        "github_seed_push_skipped": not ok_cfg,
    }


def _map_schwab_action(instruction, item):
    """Map Schwab transaction type to our action taxonomy."""
    i = str(instruction).upper()
    if "BUY" in i and "CLOSE" not in i:
        return "Buy" if "EQUITY" in str(item.get("instrument", {}).get("assetType", "")).upper() else "Buy to Open"
    if "SELL" in i and "CLOSE" not in i:
        return "Sell" if "EQUITY" in str(item.get("instrument", {}).get("assetType", "")).upper() else "Sell to Open"
    if "BUY" in i and "CLOSE" in i:
        return "Buy to Close"
    if "SELL" in i and "CLOSE" in i:
        return "Sell to Close"
    if "EXPIR" in i or "EXPIRED" in i:
        return "Expired"
    if "ASSIGN" in i:
        return "Assigned"
    if "EXERCISE" in i:
        return "Exchange or Exercise"
    if "DIVIDEND" in i:
        return "Qualified Dividend"
    return "Other"


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
