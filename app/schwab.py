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
    save_schwab_connection,
    update_schwab_account_hash,
    update_schwab_token,
    add_account_for_user,
)

# OAuth state stored in Flask session

SCHWAB_AUTH_URL = "https://api.schwabapi.com/v1/oauth/authorize"
SCHWAB_TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
SCHWAB_API_BASE = "https://api.schwabapi.com"


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

    def token_write(token):
        # schwab-py passes the wrapped {creation_timestamp, token} dict on refresh
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
        return redirect(url_for("settings"))

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
        return redirect(url_for("settings"))

    code = request.args.get("code")
    state = request.args.get("state")
    error = request.args.get("error")

    if error:
        flash(f"Schwab authorization failed: {error}", "danger")
        return redirect(url_for("settings"))

    if not code or not state:
        flash("Invalid callback from Schwab. Please try again.", "danger")
        return redirect(url_for("settings"))

    saved_state = session.pop("schwab_oauth_state", None)
    user_id = session.pop("schwab_oauth_user_id", None)
    if not saved_state or saved_state != state or not user_id or user_id != current_user.id:
        flash("Invalid state. Please try connecting again.", "danger")
        return redirect(url_for("settings"))

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
        return redirect(url_for("settings"))

    token_data = resp.json()
    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")
    if not access_token or not refresh_token:
        flash("Schwab did not return tokens. Please try again.", "danger")
        return redirect(url_for("settings"))

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
        return redirect(url_for("settings"))

    save_schwab_connection(
        current_user.id,
        account_hash=account_hash,
        account_number=account_number,
        account_name=account_name,
        token_json=token_json,
    )
    add_account_for_user(current_user.id, account_name or account_number)

    flash("Schwab account connected. Use Sync now to pull your data.", "success")
    return redirect(url_for("settings"))


@app.route("/schwab/sync", methods=["POST"])
@login_required
def schwab_sync():
    """Sync positions and transactions from Schwab to our data store."""
    cfg = _schwab_config()
    if not cfg:
        flash("Schwab API is not configured.", "danger")
        return redirect(url_for("settings"))

    conn_row = get_schwab_connection(current_user.id)
    if not conn_row:
        flash("No Schwab connection. Connect your account first.", "warning")
        return redirect(url_for("settings"))

    client = _get_schwab_client(current_user.id)
    if not client:
        flash(
            "Could not open your Schwab session (invalid token or app credentials). "
            "Click Connect Schwab again to re-authorize. If it keeps failing, check Render logs.",
            "warning",
        )
        return redirect(url_for("settings"))

    try:
        result = _run_sync(current_user.id, client)
        hr = result.get("history_rows", 0)
        cr = result.get("current_rows", 0)
        if result.get("github_pushed"):
            flash(
                f"Synced {hr} history rows, {cr} positions. "
                "GitHub seeds were updated; your Actions / dbt pipeline should refresh the dashboard.",
                "success",
            )
        else:
            msg = (
                f"Synced {hr} history rows, {cr} positions. "
                "Run 'dbt seed && dbt build' locally if you rely on CSV files only."
            )
            if result.get("github_error"):
                flash(
                    f"{msg} Could not update GitHub seeds: {result['github_error']}",
                    "warning",
                )
            elif result.get("github_seed_push_skipped"):
                flash(
                    f"{msg} Set GITHUB_PAT (+ GITHUB_REPO) to push the same data path as manual upload.",
                    "info",
                )
            else:
                flash(msg, "success")
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

    return redirect(url_for("settings"))


def _run_sync(user_id, client):
    """
    Fetch positions and transactions from Schwab, map to our schema,
    and write to the configured output (GitHub seeds or BigQuery).
    Returns dict with history_rows, current_rows.
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

    # Parse positions
    positions = []
    for sec in acct_data.get("securitiesAccount", {}).get("positions", []):
        inst = sec.get("instrument", {}) or {}
        sym = inst.get("symbol", "")
        desc = inst.get("description", sym)
        qty = sec.get("longQuantity", 0) or -(sec.get("shortQuantity", 0) or 0)
        avg = sec.get("averagePrice", 0) or 0
        mv = sec.get("marketValue", 0) or 0
        inst_type = inst.get("assetType", "EQUITY")
        positions.append({
            "account": account_name,
            "symbol": sym,
            "description": desc,
            "quantity": qty,
            "price": mv / qty if qty else avg,
            "market_value": mv,
            "cost_basis": avg * qty if qty else 0,
            "security_type": "Equity" if inst_type == "EQUITY" else "Option",
        })

    # Fetch transactions (last 60 days)
    end_date = date.today()
    start_date = end_date - timedelta(days=60)
    tx_resp = _schwab_resp_with_refresh(
        client,
        lambda: client.get_transactions(
            account_hash,
            start_date=start_date,
            end_date=end_date,
        ),
    )
    tx_data = tx_resp.json()

    tx_list = tx_data if isinstance(tx_data, list) else (tx_data.get("transaction") or tx_data.get("transactions") or [])
    if not isinstance(tx_list, list):
        tx_list = []

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
            "account": account_name,
            "date": _format_date(dt),
            "action": action,
            "symbol": sym,
            "description": desc,
            "quantity": abs(qty),
            "price": abs(price) if price else "",
            "fees_and_comm": "",
            "amount": amount,
        })

    import pandas as pd

    if transactions:
        hist_df = pd.DataFrame(transactions)
        hist_df = hist_df[HISTORY_COLUMNS] if all(c in hist_df.columns for c in HISTORY_COLUMNS) else hist_df
    else:
        hist_df = None

    curr_df = pd.DataFrame(positions) if positions else pd.DataFrame()
    skip_hist = hist_df is None or hist_df.empty

    github_pushed = False
    github_error = None
    ok_cfg = False
    from app.upload import merge_and_push_seeds, _upload_github_config_ok

    ok_cfg, _cfg_err = _upload_github_config_ok()
    if ok_cfg:
        uname = "user"
        u = User.get_by_id(user_id)
        if u:
            uname = u.username
        if skip_hist:
            commit_msg = (
                f"Schwab sync ({uname}): positions only, {len(curr_df)} rows ({account_name})"
            )
        else:
            commit_msg = (
                f"Schwab sync ({uname}): {len(transactions)} history, "
                f"{len(curr_df)} positions ({account_name})"
            )
        ok, err, _hr, _cr = merge_and_push_seeds(
            account_name,
            hist_df,
            curr_df,
            commit_message=commit_msg,
            user_id=user_id,
            skip_history=skip_hist,
        )
        github_pushed = ok
        github_error = err if not ok else None
    else:
        github_error = None

    # Local CSV fallback / debugging (ephemeral on Render unless you mount storage)
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "schwab_sync")
    os.makedirs(out_dir, exist_ok=True)
    safe_name = "".join(c if c.isalnum() else "_" for c in account_name)[:50]

    if hist_df is not None and not hist_df.empty:
        hist_path = os.path.join(out_dir, f"{safe_name}_history.csv")
        hist_df.to_csv(hist_path, index=False)

    if not curr_df.empty:
        curr_path = os.path.join(out_dir, f"{safe_name}_current.csv")
        curr_df.to_csv(curr_path, index=False)

    return {
        "history_rows": len(transactions),
        "current_rows": len(positions),
        "output_dir": out_dir,
        "github_pushed": github_pushed,
        "github_error": github_error,
        "github_seed_push_skipped": not ok_cfg,
    }


HISTORY_COLUMNS = ["account", "date", "action", "symbol", "description", "quantity", "price", "fees_and_comm", "amount"]


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
