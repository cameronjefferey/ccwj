import os
import re
import base64
import requests
import pandas as pd
from io import StringIO
from flask import render_template, request, flash, redirect, url_for
from flask_login import login_required, current_user
from app import app
from app.extensions import limiter
from app.models import (
    get_accounts_for_user, add_account_for_user,
    remove_account_for_user, is_admin,
    record_upload, get_uploads_for_user,
)


# ------------------------------------------------------------------
# Expected CSV column headers (lowercase for comparison)
# The brokerage export uses "Fees & Comm" -- we normalize it to "fees_and_comm"
# Account is always set from the form (mandatory); any Account column in the
# CSV is dropped and replaced with the user's selection.
# ------------------------------------------------------------------
HISTORY_REQUIRED_COLS = {"date", "action", "symbol", "description",
                         "quantity", "price", "amount"}

CURRENT_REQUIRED_COLS = {"symbol", "description", "quantity", "price",
                         "security_type"}

# Column renames: brokerage export name (lowercase) → seed file column name
HISTORY_COL_RENAMES = {
    "fees & comm": "fees_and_comm",
    "fees and comm": "fees_and_comm",
    "fees_&_comm": "fees_and_comm",
}

CURRENT_COL_RENAMES = {
    "qty": "Quantity",
    "cost basis": "cost_bases",
    "margin req": "margin_requirement",
    "price chng %": "price_change_percent",
    "price chng $": "price_change_dollar",
    "mkt val": "market_value",
    "day chng %": "day_change_percent",
    "day chng $": "day_change_dollar",
    "gain %": "gain_or_loss_percent",
    "gain $": "gain_or_loss_dollat",            # preserving typo from original seed
    "ratings": "rating",
    "reinvest?": "divident_reinvestment",       # preserving typo from original seed
    "reinvest capital gains?": "is_capital_gain",
    "% of acct": "percent_of_account",
    "exp/mat": "expiration_date",
    "cost/share": "cost_per_share",
    "last earnings": "last_earnings_date",
    "div yld": "dividend_yield",
    "last div": "last_dividend",
    "ex-div": "ex_dividend_date",
    "p/e ratio": "pe_ratio",
    "52 wk low": "annual_week_low",
    "52 wk high": "annual_week_high",
    "intr val": "intrinsic_value",
    "itm": "in_the_money",
    "security type": "security_type",
    "asset type": "security_type",   # Schwab export uses "Asset Type"
}

# Exact column order for each seed file
HISTORY_SEED_COLUMNS = [
    "Account", "Date", "Action", "Symbol", "Description",
    "Quantity", "Price", "fees_and_comm", "Amount",
]
CURRENT_SEED_COLUMNS = [
    "Account", "Symbol", "Description", "Quantity", "Price",
    "price_change_dollar", "price_change_percent", "market_value",
    "day_change_dollar", "day_change_percent", "cost_bases",
    "gain_or_loss_dollat", "gain_or_loss_percent", "rating",
    "divident_reinvestment", "is_capital_gain", "percent_of_account",
    "expiration_date", "cost_per_share", "last_earnings_date",
    "dividend_yield", "last_dividend", "ex_dividend_date", "pe_ratio",
    "annual_week_low", "annual_week_high", "volume", "intrinsic_value",
    "in_the_money", "security_type", "margin_requirement",
]

# Seed paths inside the repo (same layout as dbt/)
HISTORY_PATH = "dbt/seeds/trade_history.csv"
CURRENT_PATH = "dbt/seeds/current_positions.csv"


def _github_repo() -> str:
    """owner/repo for the GitHub API (override with GITHUB_REPO)."""
    return os.environ.get("GITHUB_REPO", "cameronjefferey/ccwj").strip()


def _github_branch() -> str:
    """Branch to read/write seed files (override with GITHUB_BRANCH)."""
    return os.environ.get("GITHUB_BRANCH", "master").strip()


def _github_headers():
    pat = os.environ.get("GITHUB_PAT", "")
    return {
        "Authorization": f"Bearer {pat}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _find_header_line(content, markers):
    """
    Scan raw file lines to find the row that contains column headers.
    markers: set of lowercase strings that must ALL appear somewhere in the
             lowercased line.  This is a simple substring check so it is
             immune to quoting, delimiters, and BOM characters.
    Returns the 0-based line index, or 0 as fallback.
    """
    for i, line in enumerate(content.splitlines()):
        low = line.lower()
        if all(m in low for m in markers):
            return i
    return 0


def _validate_csv(file_storage, required_cols, label, col_renames=None,
                   header_markers=None):
    """
    Read an uploaded CSV file, validate that required columns are present.
    Applies column renames (e.g. "Fees & Comm" → "fees_and_comm").
    Auto-detects tab vs. comma separator and finds the header row by
    scanning for `header_markers` (set of lowercase column names).
    Returns (dataframe, error_message). error_message is None on success.
    """
    if not file_storage or file_storage.filename == "":
        return None, f"No {label} file selected."

    if not file_storage.filename.lower().endswith(".csv"):
        return None, f"{label} file must be a .csv file."

    try:
        raw_bytes = file_storage.read()

        # Handle BOM and common encodings from brokerage exports
        for encoding in ("utf-8-sig", "utf-8", "utf-16", "latin-1"):
            try:
                content = raw_bytes.decode(encoding)
                break
            except (UnicodeDecodeError, UnicodeError):
                continue
        else:
            content = raw_bytes.decode("latin-1")   # fallback

        # ---- locate header row in the raw text ----
        if header_markers:
            header_idx = _find_header_line(content, header_markers)
            # Trim everything above the header so pandas always sees
            # the header as the very first line (avoids skip_blank_lines
            # counting issues).
            lines = content.splitlines()
            content = "\n".join(lines[header_idx:])

        # ---- detect delimiter from the (now-first) header line ----
        first_line = content.splitlines()[0] if content.splitlines() else ""
        sep = "\t" if first_line.count("\t") > first_line.count(",") else ","

        df = pd.read_csv(StringIO(content), sep=sep)
    except Exception as exc:
        return None, f"Could not parse {label} CSV: {exc}"

    # Drop completely empty rows (trailing blank lines in brokerage exports)
    df = df.dropna(how="all")

    # Drop columns whose header is blank or starts with "Unnamed"
    # (caused by trailing delimiters in brokerage exports)
    df = df[[c for c in df.columns
             if str(c).strip() and not str(c).startswith("Unnamed")]]

    # Strip whitespace from column names
    df.columns = [c.strip() for c in df.columns]

    # Strip the parenthetical long-name suffixes that Fidelity adds
    # e.g. "Qty (Quantity)" → "Qty", "Security Type" stays "Security Type"
    df.columns = [re.sub(r"\s*\(.*\)\s*$", "", c).strip() for c in df.columns]

    # Apply column renames (case-insensitive)
    if col_renames:
        rename_map = {}
        for col in df.columns:
            lower = col.lower()
            if lower in col_renames:
                rename_map[col] = col_renames[lower]
        if rename_map:
            df = df.rename(columns=rename_map)

    # Normalize column names for comparison
    actual_cols = {c.lower() for c in df.columns}
    missing = required_cols - actual_cols
    if missing:
        return None, (
            f"{label} CSV is missing required columns: {', '.join(sorted(missing))}. "
            f"Got: {', '.join(sorted(actual_cols))}"
        )

    if df.empty:
        return None, f"{label} CSV has no data rows."

    return df, None


def _get_file_sha(path):
    """Get the current SHA of a file in the repo (needed to update it)."""
    repo = _github_repo()
    branch = _github_branch()
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    resp = requests.get(
        url, headers=_github_headers(), params={"ref": branch}, timeout=15
    )
    if resp.status_code == 200:
        return resp.json().get("sha")
    return None  # File doesn't exist yet


def _get_file_content(path):
    """
    Fetch the raw content of a file from the repo.
    Returns decoded string, or None if file doesn't exist or fetch fails.
    """
    repo = _github_repo()
    branch = _github_branch()
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    resp = requests.get(
        url, headers=_github_headers(), params={"ref": branch}, timeout=15
    )
    if resp.status_code != 200:
        return None
    data = resp.json()
    if data.get("encoding") == "base64":
        return base64.b64decode(data["content"]).decode("utf-8")
    return data.get("content", "")


def _merge_seed_with_existing(path, account_name, new_df, seed_columns):
    """
    Merge new account data with existing seed data.
    - Fetches current file from GitHub
    - For current positions: replace that account's rows (snapshot semantics)
    - For history: append new rows for that account and de-duplicate
    - Returns CSV string ready to commit
    """
    existing_content = _get_file_content(path)
    if not existing_content or not existing_content.strip():
        # No existing file or empty: use only new data
        for col in seed_columns:
            if col not in new_df.columns:
                new_df[col] = ""
        merged = new_df[seed_columns]
        return merged.to_csv(index=False)

    # Parse existing CSV
    try:
        existing_df = pd.read_csv(StringIO(existing_content))
    except Exception:
        # If parse fails, fall back to overwrite with new data only
        for col in seed_columns:
            if col not in new_df.columns:
                new_df[col] = ""
        return new_df[seed_columns].to_csv(index=False)

    if existing_df.empty:
        for col in seed_columns:
            if col not in new_df.columns:
                new_df[col] = ""
        return new_df[seed_columns].to_csv(index=False)

    # Normalize Account column name (may be "Account" or "account" from CSV)
    acct_col = None
    for c in existing_df.columns:
        if c.strip().lower() == "account":
            acct_col = c
            break
    if acct_col is None:
        # No account column in existing: overwrite with new only
        for col in seed_columns:
            if col not in new_df.columns:
                new_df[col] = ""
        return new_df[seed_columns].to_csv(index=False)

    # Normalize Account field on both sides for comparison
    existing_df[acct_col] = existing_df[acct_col].astype(str).str.strip()
    account_mask = existing_df[acct_col] == account_name

    # Ensure new_df has same columns as seed; align columns
    for col in seed_columns:
        if col not in new_df.columns:
            new_df[col] = ""
    new_df = new_df[seed_columns]

    # Align existing columns (may have different order or extras)
    for col in seed_columns:
        if col not in existing_df.columns:
            existing_df[col] = ""
    existing_df = existing_df[seed_columns]

    # Split existing into this account vs other accounts
    other_df = existing_df.loc[~account_mask]
    existing_account_df = existing_df.loc[account_mask]

    if path == HISTORY_PATH:
        # History: append and de-duplicate within this account.
        # New rows take precedence when keys collide.
        if existing_account_df.empty:
            merged_account = new_df.copy()
        else:
            # Mark source to prefer new rows on duplicates
            existing_account_df = existing_account_df.copy()
            new_tagged = new_df.copy()
            existing_account_df["__src"] = "old"
            new_tagged["__src"] = "new"
            combined = pd.concat([existing_account_df, new_tagged], ignore_index=True)

            # Normalize key columns so duplicates match: NaN != NaN in pandas,
            # so fill nulls with a sentinel before dedupe.
            key_cols = [c for c in seed_columns if c != "Account"]
            for c in key_cols:
                if c in combined.columns:
                    combined[c] = combined[c].astype(str).replace("nan", "").replace("None", "").str.strip()

            combined = combined.sort_values("__src")  # old first, new second
            combined = combined.drop_duplicates(subset=key_cols, keep="last")
            merged_account = combined.drop(columns=["__src"])
    else:
        # Current positions (snapshot): replace that account entirely
        merged_account = new_df

    merged = pd.concat([other_df, merged_account], ignore_index=True)
    return merged[seed_columns].to_csv(index=False)


def _commit_file(path, content, message):
    """
    Create or update a file in the GitHub repo via the Contents API.
    Returns (success, error_message).
    """
    repo = _github_repo()
    branch = _github_branch()
    url = f"https://api.github.com/repos/{repo}/contents/{path}"

    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        "branch": branch,
    }

    # If file exists, include its SHA (required for updates)
    sha = _get_file_sha(path)
    if sha:
        payload["sha"] = sha

    resp = requests.put(url, json=payload, headers=_github_headers(), timeout=30)
    if resp.status_code in (200, 201):
        return True, None
    else:
        return False, f"GitHub API error (HTTP {resp.status_code}): {resp.text[:300]}"


def _commit_two_files(path1, content1, path2, content2, message):
    """
    Create a single commit that updates both files (Git Data API).
    One commit = one push event = one workflow run.
    Returns (success, error_message).
    """
    repo_full = _github_repo()
    branch = _github_branch()
    parts = repo_full.split("/", 1)
    if len(parts) != 2:
        return False, "Invalid GITHUB_REPO (expected owner/repo)"
    owner, repo = parts
    base = f"https://api.github.com/repos/{owner}/{repo}/git"

    headers = _github_headers()

    # 1. Get current commit SHA
    ref_resp = requests.get(
        f"{base}/refs/heads/{branch}", headers=headers, timeout=15
    )
    if ref_resp.status_code != 200:
        return False, f"Failed to get ref (HTTP {ref_resp.status_code})"
    commit_sha = ref_resp.json()["object"]["sha"]

    # 2. Get commit and tree SHA
    commit_resp = requests.get(f"{base}/commits/{commit_sha}", headers=headers, timeout=15)
    if commit_resp.status_code != 200:
        return False, f"Failed to get commit (HTTP {commit_resp.status_code})"
    tree_sha = commit_resp.json()["tree"]["sha"]

    # 3. Create blobs for both file contents
    def create_blob(content):
        blob_resp = requests.post(
            f"{base}/blobs",
            headers=headers,
            json={"content": base64.b64encode(content.encode("utf-8")).decode("ascii"), "encoding": "base64"},
            timeout=30,
        )
        if blob_resp.status_code != 201:
            raise RuntimeError(f"Blob create failed: {blob_resp.status_code}")
        return blob_resp.json()["sha"]

    try:
        blob1_sha = create_blob(content1)
        blob2_sha = create_blob(content2)
    except RuntimeError as e:
        return False, str(e)

    # 4. Create tree with both files (base_tree keeps rest of repo)
    tree_payload = {
        "base_tree": tree_sha,
        "tree": [
            {"path": path1, "mode": "100644", "type": "blob", "sha": blob1_sha},
            {"path": path2, "mode": "100644", "type": "blob", "sha": blob2_sha},
        ],
    }
    tree_resp = requests.post(f"{base}/trees", headers=headers, json=tree_payload, timeout=30)
    if tree_resp.status_code != 201:
        return False, f"Failed to create tree (HTTP {tree_resp.status_code}): {tree_resp.text[:200]}"
    new_tree_sha = tree_resp.json()["sha"]

    # 5. Create commit
    commit_payload = {"tree": new_tree_sha, "parents": [commit_sha], "message": message}
    commit_resp = requests.post(f"{base}/commits", headers=headers, json=commit_payload, timeout=30)
    if commit_resp.status_code != 201:
        return False, f"Failed to create commit (HTTP {commit_resp.status_code}): {commit_resp.text[:200]}"
    new_commit_sha = commit_resp.json()["sha"]

    # 6. Update ref
    patch_resp = requests.patch(
        f"{base}/refs/heads/{branch}",
        headers=headers,
        json={"sha": new_commit_sha},
        timeout=15,
    )
    if patch_resp.status_code != 200:
        return False, f"Failed to update ref (HTTP {patch_resp.status_code})"
    return True, None


EXISTING_ACCOUNTS_QUERY = """
    SELECT DISTINCT account
    FROM `ccwj-dbt.analytics.positions_summary`
    ORDER BY account
"""


def _get_existing_accounts():
    """Fetch existing account names from BigQuery for the dropdown."""
    try:
        from app.bigquery_client import get_bigquery_client
        client = get_bigquery_client()
        df = client.query(EXISTING_ACCOUNTS_QUERY).to_dataframe()
        return sorted(df["account"].dropna().unique().tolist())
    except Exception:
        return []


def _upload_github_config_ok():
    """Return (ok, error_message). PAT + owner/repo shape."""
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat:
        return False, "GITHUB_PAT is not set. Manual upload is disabled."
    repo = _github_repo()
    if not repo or repo.count("/") != 1 or ".." in repo or repo.startswith("/"):
        return False, "GITHUB_REPO must be set to owner/repo (e.g. myorg/ccwj)."
    return True, None


@app.route("/upload", methods=["GET", "POST"])
@login_required
@limiter.limit("30 per minute", exempt_when=lambda: request.method != "POST")
def upload():
    pat_configured = bool(os.environ.get("GITHUB_PAT", "").strip())

    if request.method == "GET":
        user_accounts = get_accounts_for_user(current_user.id)
        # Admins see every account in BigQuery; regular users see only
        # their linked accounts plus any BigQuery accounts they own.
        if is_admin(current_user.username):
            all_bq = _get_existing_accounts()
            accounts = sorted(set(all_bq))
        else:
            accounts = sorted(set(user_accounts))
        recent_uploads = get_uploads_for_user(current_user.id)
        return render_template(
            "upload.html", title="Upload Data",
            accounts=accounts,
            recent_uploads=recent_uploads,
            github_upload_enabled=pat_configured,
        )

    # ------------------------------------------------------------------
    # GitHub PAT + repo config
    # ------------------------------------------------------------------
    ok_cfg, cfg_err = _upload_github_config_ok()
    if not ok_cfg:
        flash(cfg_err, "danger")
        return redirect(url_for("upload"))

    # ------------------------------------------------------------------
    # Parse and validate CSVs
    # ------------------------------------------------------------------
    skip_history = request.form.get("no_trades_today") == "1"

    current_file = request.files.get("current_csv")
    current_df, current_err = _validate_csv(
        current_file, CURRENT_REQUIRED_COLS, "Current",
        col_renames=CURRENT_COL_RENAMES,
        header_markers={"symbol", "description", "price"},
    )
    if current_err:
        flash(current_err, "danger")
        return redirect(url_for("upload"))

    history_df = None
    if not skip_history:
        history_file = request.files.get("history_csv")
        history_df, history_err = _validate_csv(
            history_file, HISTORY_REQUIRED_COLS, "History",
            col_renames=HISTORY_COL_RENAMES,
            header_markers={"date", "action", "symbol", "quantity"},
        )
        if history_err:
            flash(history_err, "danger")
            return redirect(url_for("upload"))

    # ------------------------------------------------------------------
    # Account name is mandatory (selected or typed on the form)
    # ------------------------------------------------------------------
    account_select = request.form.get("account_name", "").strip()
    account_custom = request.form.get("account_name_custom", "").strip()

    # Use the custom name only when the user chose "+ Create new account..."
    if account_select == "__new__":
        account_name = account_custom
    else:
        account_name = account_select

    if not account_name:
        flash("Please select or enter an account name.", "danger")
        return redirect(url_for("upload"))

    # ------------------------------------------------------------------
    # Set the Account column on DataFrames (overwrite if present)
    # ------------------------------------------------------------------
    for df in [history_df, current_df]:
        if df is None:
            continue
        acct_cols = [c for c in df.columns if c.lower() == "account"]
        if acct_cols:
            df.drop(columns=acct_cols, inplace=True)
        df.insert(0, "Account", account_name)

    # ------------------------------------------------------------------
    # Normalize column names to match seed file format
    # ------------------------------------------------------------------
    if history_df is not None:
        history_standard = {
            "account": "Account", "date": "Date", "action": "Action",
            "symbol": "Symbol", "description": "Description",
            "quantity": "Quantity", "price": "Price",
            "fees_and_comm": "fees_and_comm", "amount": "Amount",
        }
        history_col_map = {c: history_standard[c.lower()]
                           for c in history_df.columns if c.lower() in history_standard}
        history_df = history_df.rename(columns=history_col_map)
        for col in HISTORY_SEED_COLUMNS:
            if col not in history_df.columns:
                history_df[col] = ""
        history_df = history_df[HISTORY_SEED_COLUMNS]

    current_norm = {c.lower(): c for c in CURRENT_SEED_COLUMNS}
    current_col_map = {}
    for col in current_df.columns:
        lower = col.lower()
        if lower in current_norm:
            current_col_map[col] = current_norm[lower]
    current_df = current_df.rename(columns=current_col_map)
    for seed_col in CURRENT_SEED_COLUMNS:
        if seed_col not in current_df.columns:
            current_df[seed_col] = ""
    current_df = current_df[CURRENT_SEED_COLUMNS]

    # Merge with existing seed data (preserve other accounts, replace this account)
    if history_df is not None:
        history_content = _merge_seed_with_existing(
            HISTORY_PATH, account_name, history_df, HISTORY_SEED_COLUMNS
        )
    current_content = _merge_seed_with_existing(
        CURRENT_PATH, account_name, current_df, CURRENT_SEED_COLUMNS
    )

    history_rows = len(history_df) if history_df is not None else 0
    current_rows = len(current_df)

    # ------------------------------------------------------------------
    # Commit to GitHub (this triggers the Actions pipeline)
    # ------------------------------------------------------------------
    if skip_history:
        commit_msg = (
            f"Upload by {current_user.username}: "
            f"positions only, {current_rows} current rows ({account_name})"
        )
    else:
        commit_msg = (
            f"Upload by {current_user.username}: "
            f"{history_rows} history rows, {current_rows} current rows "
            f"({account_name})"
        )

    try:
        if skip_history:
            ok, err = _commit_file(CURRENT_PATH, current_content, commit_msg)
        else:
            ok, err = _commit_two_files(
                HISTORY_PATH, history_content,
                CURRENT_PATH, current_content,
                commit_msg,
            )
        if not ok:
            flash(f"Failed to update seeds: {err}", "danger")
            return redirect(url_for("upload"))

    except Exception as exc:
        flash(f"GitHub commit failed: {exc}", "danger")
        return redirect(url_for("upload"))

    # Auto-link this account and record the upload
    add_account_for_user(current_user.id, account_name)
    record_upload(current_user.id, account_name, history_rows, current_rows)

    if skip_history:
        flash(
            f"Upload saved for {account_name} ({current_rows:,} positions). "
            "Your data is updating in the background.",
            "success",
        )
    else:
        flash(
            f"Upload saved for {account_name} ({history_rows:,} trades, {current_rows:,} positions). "
            "Your data is updating in the background.",
            "success",
        )

    return redirect(url_for("upload_processing"))


@app.route("/upload/processing")
@login_required
def upload_processing():
    """Intermediary page shown after upload while data refreshes."""
    expected_minutes = 3

    return render_template(
        "upload_processing.html",
        title="Processing Upload",
        expected_minutes=expected_minutes,
    )


@app.route("/unclaim-account", methods=["POST"])
@login_required
def unclaim_account():
    """Unlink an account from the current user."""
    account_name = request.form.get("unclaim_account_name", "").strip()
    if not account_name:
        flash("No account selected to remove.", "danger")
        return redirect(url_for("upload"))
    remove_account_for_user(current_user.id, account_name)
    flash(f"Account \"{account_name}\" removed from your profile.", "info")
    return redirect(url_for("upload"))
