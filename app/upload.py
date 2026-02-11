import os
import base64
import requests
import pandas as pd
from io import StringIO
from flask import render_template, request, flash, redirect, url_for
from flask_login import login_required, current_user
from app import app


# ------------------------------------------------------------------
# Expected CSV column headers (lowercase for comparison)
# ------------------------------------------------------------------
HISTORY_REQUIRED_COLS = {"account", "date", "action", "symbol", "description",
                         "quantity", "price", "fees_and_comm", "amount"}

CURRENT_REQUIRED_COLS = {"account", "symbol", "description", "quantity", "price",
                         "security_type"}

# GitHub config
GITHUB_REPO = "cameronjefferey/ccwj"
HISTORY_PATH = "dbt/seeds/0417_history.csv"
CURRENT_PATH = "dbt/seeds/0417_current.csv"


def _github_headers():
    pat = os.environ.get("GITHUB_PAT", "")
    return {
        "Authorization": f"Bearer {pat}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _validate_csv(file_storage, required_cols, label):
    """
    Read an uploaded CSV file, validate that required columns are present.
    Returns (csv_content_string, dataframe, error_message).
    error_message is None on success.
    """
    if not file_storage or file_storage.filename == "":
        return None, None, f"No {label} file selected."

    if not file_storage.filename.lower().endswith(".csv"):
        return None, None, f"{label} file must be a .csv file."

    try:
        raw_bytes = file_storage.read()
        content = raw_bytes.decode("utf-8")
        df = pd.read_csv(StringIO(content))
    except Exception as exc:
        return None, None, f"Could not parse {label} CSV: {exc}"

    # Normalize column names for comparison
    actual_cols = {c.strip().lower() for c in df.columns}
    missing = required_cols - actual_cols
    if missing:
        return None, None, (
            f"{label} CSV is missing required columns: {', '.join(sorted(missing))}. "
            f"Got: {', '.join(sorted(actual_cols))}"
        )

    if df.empty:
        return None, None, f"{label} CSV has no data rows."

    return content, df, None


def _get_file_sha(path):
    """Get the current SHA of a file in the repo (needed to update it)."""
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    resp = requests.get(url, headers=_github_headers(), timeout=15)
    if resp.status_code == 200:
        return resp.json().get("sha")
    return None  # File doesn't exist yet


def _commit_file(path, content, message):
    """
    Create or update a file in the GitHub repo via the Contents API.
    Returns (success, error_message).
    """
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"

    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        "branch": "master",
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


@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    if request.method == "GET":
        return render_template("upload.html", title="Upload Data")

    # ------------------------------------------------------------------
    # Check GITHUB_PAT is set
    # ------------------------------------------------------------------
    if not os.environ.get("GITHUB_PAT"):
        flash("GITHUB_PAT environment variable not set. Upload is disabled.", "danger")
        return redirect(url_for("upload"))

    # ------------------------------------------------------------------
    # Parse and validate both CSVs
    # ------------------------------------------------------------------
    history_file = request.files.get("history_csv")
    current_file = request.files.get("current_csv")

    history_content, history_df, history_err = _validate_csv(
        history_file, HISTORY_REQUIRED_COLS, "History"
    )
    current_content, current_df, current_err = _validate_csv(
        current_file, CURRENT_REQUIRED_COLS, "Current"
    )

    if history_err:
        flash(history_err, "danger")
        return redirect(url_for("upload"))
    if current_err:
        flash(current_err, "danger")
        return redirect(url_for("upload"))

    # ------------------------------------------------------------------
    # Summarize what's being uploaded
    # ------------------------------------------------------------------
    history_account_col = next(c for c in history_df.columns if c.strip().lower() == "account")
    current_account_col = next(c for c in current_df.columns if c.strip().lower() == "account")
    all_accounts = sorted(
        set(history_df[history_account_col].str.strip().unique())
        | set(current_df[current_account_col].str.strip().unique())
    )
    history_rows = len(history_df)
    current_rows = len(current_df)

    # ------------------------------------------------------------------
    # Commit both files to GitHub (this triggers the Actions pipeline)
    # ------------------------------------------------------------------
    commit_msg = (
        f"Upload by {current_user.username}: "
        f"{history_rows} history rows, {current_rows} current rows "
        f"({', '.join(all_accounts)})"
    )

    try:
        # Commit history CSV first
        ok, err = _commit_file(HISTORY_PATH, history_content, commit_msg)
        if not ok:
            flash(f"Failed to update history seed: {err}", "danger")
            return redirect(url_for("upload"))

        # Commit current CSV (separate commit so both get saved even if one is unchanged)
        ok, err = _commit_file(CURRENT_PATH, current_content, commit_msg)
        if not ok:
            flash(f"Failed to update current seed: {err}", "danger")
            return redirect(url_for("upload"))

    except Exception as exc:
        flash(f"GitHub commit failed: {exc}", "danger")
        return redirect(url_for("upload"))

    flash(
        f"Seed files updated: {history_rows:,} history rows and {current_rows:,} current rows "
        f"for {', '.join(all_accounts)}.",
        "success",
    )
    flash(
        "Pipeline triggered -- dbt seed + build is running. "
        "Data will refresh in a few minutes.",
        "info",
    )

    return redirect(url_for("upload"))
