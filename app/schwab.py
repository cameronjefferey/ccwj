"""Schwab API — OAuth connection and sync of positions/transactions."""
import json
import os
import secrets
import tempfile
from datetime import datetime, date, timedelta
from urllib.parse import urlencode

from flask import redirect, request, url_for, flash, session
from flask_login import login_required, current_user

from app import app
from app.models import (
    get_schwab_connection,
    get_schwab_connections,
    save_schwab_connection,
    update_schwab_token,
    add_account_for_user,
)

# OAuth state stored in Flask session

SCHWAB_AUTH_URL = "https://api.schwabapi.com/v1/oauth/authorize"
SCHWAB_TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
SCHWAB_API_BASE = "https://api.schwabapi.com"


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
        return json.loads(c["token_json"]) if c else None

    def token_write(token):
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

    # Build token in schwab-py format (they expect a specific structure)
    token_json = json.dumps({
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": token_data.get("token_type", "Bearer"),
        "expires_in": token_data.get("expires_in", 1800),
        "created_at": int(datetime.utcnow().timestamp()),
    })

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

    client = _get_schwab_client(current_user.id)
    if not client:
        flash("No Schwab connection. Connect your account first.", "warning")
        return redirect(url_for("settings"))

    try:
        result = _run_sync(current_user.id, client)
        flash(
            f"Synced {result.get('history_rows', 0)} history rows, "
            f"{result.get('current_rows', 0)} positions. "
            "Run 'dbt seed && dbt build' to refresh the dashboard, or use automated sync.",
            "success",
        )
    except Exception as e:
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

    # Fetch account with positions (Account.Fields.POSITIONS = 'positions')
    try:
        from schwab.client import Client
        fields = [Client.Account.Fields.POSITIONS]
    except Exception:
        fields = ["positions"]
    resp = client.get_account(account_hash, fields=fields)
    resp.raise_for_status()
    acct_data = resp.json()

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
    tx_resp = client.get_transactions(
        account_hash,
        start_date=start_date,
        end_date=end_date,
    )
    tx_resp.raise_for_status()
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

    # For now: write to a file the user can use. Full BigQuery integration next.
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "schwab_sync")
    os.makedirs(out_dir, exist_ok=True)
    safe_name = "".join(c if c.isalnum() else "_" for c in account_name)[:50]

    import pandas as pd

    if transactions:
        hist_df = pd.DataFrame(transactions)
        hist_df = hist_df[HISTORY_COLUMNS] if all(c in hist_df.columns for c in HISTORY_COLUMNS) else hist_df
        hist_path = os.path.join(out_dir, f"{safe_name}_history.csv")
        hist_df.to_csv(hist_path, index=False)

    if positions:
        curr_df = pd.DataFrame(positions)
        curr_path = os.path.join(out_dir, f"{safe_name}_current.csv")
        curr_df.to_csv(curr_path, index=False)

    return {
        "history_rows": len(transactions),
        "current_rows": len(positions),
        "output_dir": out_dir,
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
