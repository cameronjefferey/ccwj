import os
import re
import base64
import requests
import pandas as pd
from io import StringIO
from flask import render_template, request, flash, redirect, url_for, jsonify
from flask_login import login_required, current_user
from app import app
from app.extensions import limiter
from app.models import (
    get_accounts_for_user, add_account_for_user,
    remove_account_for_user, is_admin,
    record_upload, get_uploads_for_user, count_uploads_for_user,
)
from app.utils import demo_block_writes


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

# Exact column order for each seed file.
#
# `user_id` follows `Account` so the per-row tenant key is co-located with
# the (legacy, free-form) account label. Two users picking the same
# `Account` string are now disambiguated by `user_id` — see
# ``docs/USER_ID_TENANCY.md``. Stage 0 keeps `user_id` nullable so legacy
# rows in already-pushed seeds load fine; Stage 1's
# ``scripts/backfill_seed_user_ids.py`` fills them in; Stage 3 flips the
# Flask BQ filter to require ``WHERE user_id = current_user.id``.
HISTORY_SEED_COLUMNS = [
    "Account", "user_id", "Date", "Action", "Symbol", "Description",
    "Quantity", "Price", "fees_and_comm", "Amount",
]
CURRENT_SEED_COLUMNS = [
    "Account", "user_id", "Symbol", "Description", "Quantity", "Price",
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

# Schwab-only extra seed (cash + account-total rows for equity snapshots).
# Schwab sync writes trade history into trade_history.csv and open positions
# into current_positions.csv — the same seeds the manual upload path uses —
# so there's a single pipeline into dbt.
SCHWAB_ACCOUNT_BALANCES_PATH = "dbt/seeds/schwab_account_balances.csv"

SCHWAB_BALANCE_COLUMNS = [
    "account",
    "user_id",
    "row_type",
    "market_value",
    "cost_basis",
    "unrealized_pnl",
    "unrealized_pnl_pct",
    "percent_of_account",
]


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


class SeedFetchError(RuntimeError):
    """Raised when reading the existing seed from GitHub fails for any reason
    other than the file genuinely not existing (HTTP 404).

    A merge that proceeds on a transient fetch failure would silently
    treat the seed as empty and overwrite every other tenant's history
    with just the syncing user's new rows. That actually happened in
    production once (see commit ``3f4aecb`` — Sara Investment sync wiped
    10,446 rows belonging to four other accounts and three other users).
    The merge MUST refuse to run unless we can distinguish "file does
    not yet exist" from "GitHub call blipped".
    """


def _get_file_content(path):
    """
    Fetch the raw content of a file from the repo.
    Returns decoded string on 200, or ``None`` only when the file truly
    does not exist (HTTP 404). Raises ``SeedFetchError`` on any other
    failure (timeout, 5xx, 403 rate-limit, auth) so callers cannot
    accidentally treat a transient blip as "no existing data". See
    ``_merge_seed_with_existing`` and the Bug A note in the
    SeedFetchError docstring.
    """
    repo = _github_repo()
    branch = _github_branch()
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    try:
        resp = requests.get(
            url, headers=_github_headers(), params={"ref": branch}, timeout=15
        )
    except requests.RequestException as exc:
        raise SeedFetchError(
            f"GitHub fetch for {path} failed: {exc}"
        ) from exc
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        raise SeedFetchError(
            f"GitHub fetch for {path} returned HTTP {resp.status_code}: "
            f"{resp.text[:200]}"
        )
    data = resp.json()
    if data.get("encoding") == "base64":
        return base64.b64decode(data["content"]).decode("utf-8")
    return data.get("content", "")


def _normalize_uid(value) -> str:
    """Canonicalize a ``user_id`` value to its int-string form.

    Pandas reads any column that contains a NaN as ``float64``, so a CSV
    user_id of ``9`` becomes ``9.0`` after ``.astype(str)``. The original
    merge compared that to ``str(int(user_id)) = "9"`` and never
    matched, which silently moved every existing row of the syncing
    user into ``other_df`` and then APPENDED the fresh sync on top —
    doubling the row count on every re-sync (see commit ``05c5ae5``:
    Cameron Investment went 2,703 → 4,059 user_9 rows after a fresh
    1,356-tx sync that should have replaced the existing rows). Both
    sides of the dedup MUST canonicalize the same way.
    """
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return ""
    try:
        return str(int(float(s)))
    except (ValueError, TypeError):
        return s


# Numeric fields in trade rows (Quantity, Price, fees_and_comm, Amount) round-
# trip through Schwab's API → pandas → JSON → CSV with float-precision drift:
# the same trade can land as ``26.99`` on one sync and ``26.990000000000002``
# on the next. ``astype(str)`` keeps both literals intact so the dedup treats
# them as different rows and BOTH survive — observed in production for
# ``user_id=7, 'Schwab ••••5989'`` (213 rows / 158 unique = 55 byte-different
# but value-identical dupes; CURRENCY_USD rows show the drift directly:
# ``-16.189999999999998``, ``-27.000000000000004``, ``-26.990000000000002``).
# Canonicalizing numeric-looking cells to a fixed precision before the dedup
# collapses these. Non-numeric cells (Date, Action, Symbol, Description) are
# returned unchanged so we don't accidentally normalize away semantic content.
def _canonicalize_seed_cell(value):
    """Normalize a seed cell for the merge dedup key.

    - ``None`` / NaN / ``"nan"`` / ``"None"`` / ``"<NA>"`` → empty string.
    - Numeric-looking cells → ``"%.6f"`` (trailing-zero stripped) so float
      precision drift across syncs does not break dedup.
    - Everything else → ``str(value).strip()``.
    """
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return ""
    try:
        f = float(s)
    except (TypeError, ValueError):
        return s
    if pd.isna(f):
        return ""
    # Round to 6 decimal places — finer than any broker reports, coarser than
    # the noise floor introduced by JSON/float round-trips. ``rstrip('0')`` +
    # ``rstrip('.')`` normalize ``"4600.000000"`` and ``"4600"`` to the same
    # canonical form so int-vs-float seed cells dedup against each other too.
    out = f"{f:.6f}".rstrip("0").rstrip(".")
    if out in ("", "-"):
        return "0"
    if out == "-0":
        return "0"
    return out


def _merge_seed_with_existing(path, account_name, new_df, seed_columns, *, user_id=None):
    """
    Merge new account data with existing seed data.
    - Fetches current file from GitHub
    - For current positions: replace that account's rows (snapshot semantics)
    - For history: append new rows for that account and de-duplicate
    - Returns CSV string ready to commit

    ``user_id`` (the syncing user's tenant id) MUST be passed for any
    history merge that lands user-facing data. The dedup window is
    scoped to the syncing user's own rows plus legacy unowned rows
    (``user_id=""`` from before tenancy was tracked). Rows owned by
    OTHER users under the same ``account_name`` are kept verbatim in
    ``other_df`` and never touched — required by the tenant-isolation
    rule: account labels can collide across users (parent + child both
    saying "Schwab Account"), and a sync must never silently overwrite
    another user's rows even on a key collision. See
    ``.cursor/rules/bigquery-tenant-isolation.mdc`` and
    ``docs/USER_ID_TENANCY.md``.

    Raises ``SeedFetchError`` if the existing seed cannot be fetched
    for any reason other than HTTP 404 — see commit ``3f4aecb`` for
    why a silent "treat as empty" path is unacceptable.
    """
    existing_content = _get_file_content(path)
    if existing_content is None:
        # File truly does not exist yet (HTTP 404). Safe to use only new data.
        for col in seed_columns:
            if col not in new_df.columns:
                new_df[col] = ""
        merged = new_df[seed_columns]
        return merged.to_csv(index=False)
    if not existing_content.strip():
        # File exists but is empty (e.g. someone manually truncated it).
        # Still safe to use only new data — there is nothing to preserve.
        for col in seed_columns:
            if col not in new_df.columns:
                new_df[col] = ""
        merged = new_df[seed_columns]
        return merged.to_csv(index=False)

    # Parse existing CSV. Refuse to proceed on parse failure rather than
    # silently overwriting — a corrupted file in the repo is something a
    # human needs to look at, not something a sync should paper over by
    # destroying every other tenant's data.
    try:
        existing_df = pd.read_csv(StringIO(existing_content))
    except Exception as exc:
        raise SeedFetchError(
            f"Existing seed at {path} failed to parse: {exc}. "
            "Refusing to overwrite to protect other tenants' data."
        ) from exc

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
        # No account column in existing: same reasoning as the parse-fail
        # branch — refuse rather than silently nuking other tenants.
        raise SeedFetchError(
            f"Existing seed at {path} has no Account column. "
            "Refusing to overwrite to protect other tenants' data."
        )

    # Normalize Account field on both sides for comparison
    existing_df[acct_col] = existing_df[acct_col].astype(str).str.strip()
    acct_match = existing_df[acct_col] == account_name

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

    # Tenancy scope: only the syncing user's own rows (and legacy unowned
    # rows from before user_id was tracked) are eligible to be rewritten
    # by this merge. Rows owned by OTHER users under the same
    # account_name stay in ``other_df`` and are never touched. Without
    # this scope, two users sharing an account label (parent + child
    # both calling theirs "Schwab Account", or a re-tested test user)
    # would dedup against each other on identical trade keys and
    # silently steal each other's history. See tenancy rule.
    if user_id is not None and "user_id" in existing_df.columns:
        target_uid = _normalize_uid(user_id)
        existing_uid_norm = existing_df["user_id"].map(_normalize_uid)
        legacy_or_self = existing_uid_norm.isin(["", target_uid])
        account_mask = acct_match & legacy_or_self
    else:
        account_mask = acct_match

    other_df = existing_df.loc[~account_mask]
    existing_account_df = existing_df.loc[account_mask]

    if path == HISTORY_PATH:
        # History: append and de-duplicate within this account.
        # New rows take precedence when keys collide.
        if existing_account_df.empty:
            merged_account = new_df.copy()
        else:
            # Tag rows by source so the dedup keeps the NEW row on key
            # collisions. Use integer sentinels (0=old, 1=new) — the
            # previous "old"/"new" string sentinels sorted alphabetically
            # ("new" < "old") and silently put NEW rows BEFORE OLD ones,
            # which made keep="last" retain the legacy row instead of
            # the freshly-tagged sync row. Multi-account users with
            # pre-tenancy seed data ended up losing every re-synced
            # trade to that misordering. Regression-tested in
            # tests/test_upload_merge.py.
            existing_account_df = existing_account_df.copy()
            new_tagged = new_df.copy()
            existing_account_df["__src"] = 0
            new_tagged["__src"] = 1
            combined = pd.concat([existing_account_df, new_tagged], ignore_index=True)

            # Normalize key columns so duplicates match: NaN != NaN in pandas,
            # so fill nulls with a sentinel before dedupe. ``user_id`` is
            # excluded from the dedupe key because it's tenant metadata,
            # not part of the trade's identity — without this exclusion a
            # legacy row (``user_id=""``) and the same-trade re-sync row
            # (``user_id=5``) would both be kept, double-counting the
            # trade. The combined frame is already scoped to the syncing
            # user + legacy rows, so excluding user_id is safe (other
            # users' rows can't reach this dedup at all).
            key_cols = [
                c for c in seed_columns
                if str(c).lower() not in ("account", "user_id")
            ]
            for c in key_cols:
                if c in combined.columns:
                    combined[c] = combined[c].map(_canonicalize_seed_cell)

            combined = combined.sort_values("__src", kind="stable")  # 0 first, 1 last
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
    Returns (success, error_message, head_commit_sha or None).
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
    fsha = _get_file_sha(path)
    if fsha:
        payload["sha"] = fsha

    resp = requests.put(url, json=payload, headers=_github_headers(), timeout=30)
    if resp.status_code in (200, 201):
        try:
            csha = (resp.json().get("commit") or {}).get("sha")
        except Exception:
            csha = None
        return True, None, csha
    else:
        return False, f"GitHub API error (HTTP {resp.status_code}): {resp.text[:300]}", None


def _commit_git_paths(path_contents, message):
    """
    Create a single commit updating one or more files (Git Data API).
    path_contents: list of (repo_path, utf-8 content).
    One commit = one push event = one workflow run.
    Returns (success, error_message, head_commit_sha or None).
    """
    if not path_contents:
        return True, None, None
    if len(path_contents) == 1:
        p, c = path_contents[0]
        return _commit_file(p, c, message)

    repo_full = _github_repo()
    branch = _github_branch()
    parts = repo_full.split("/", 1)
    if len(parts) != 2:
        return False, "Invalid GITHUB_REPO (expected owner/repo)", None
    owner, repo = parts
    base = f"https://api.github.com/repos/{owner}/{repo}/git"

    headers = _github_headers()

    ref_resp = requests.get(
        f"{base}/refs/heads/{branch}", headers=headers, timeout=15
    )
    if ref_resp.status_code != 200:
        return False, f"Failed to get ref (HTTP {ref_resp.status_code})", None
    commit_sha = ref_resp.json()["object"]["sha"]

    commit_resp = requests.get(f"{base}/commits/{commit_sha}", headers=headers, timeout=15)
    if commit_resp.status_code != 200:
        return False, f"Failed to get commit (HTTP {commit_resp.status_code})", None
    tree_sha = commit_resp.json()["tree"]["sha"]

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
        tree_entries = []
        for path, content in path_contents:
            sha = create_blob(content)
            tree_entries.append({"path": path, "mode": "100644", "type": "blob", "sha": sha})
    except RuntimeError as e:
        return False, str(e), None

    tree_payload = {"base_tree": tree_sha, "tree": tree_entries}
    tree_resp = requests.post(f"{base}/trees", headers=headers, json=tree_payload, timeout=30)
    if tree_resp.status_code != 201:
        return (
            False,
            f"Failed to create tree (HTTP {tree_resp.status_code}): {tree_resp.text[:200]}",
            None,
        )
    new_tree_sha = tree_resp.json()["sha"]

    commit_payload = {"tree": new_tree_sha, "parents": [commit_sha], "message": message}
    commit_resp = requests.post(f"{base}/commits", headers=headers, json=commit_payload, timeout=30)
    if commit_resp.status_code != 201:
        return (
            False,
            f"Failed to create commit (HTTP {commit_resp.status_code}): {commit_resp.text[:200]}",
            None,
        )
    new_commit_sha = commit_resp.json()["sha"]

    patch_resp = requests.patch(
        f"{base}/refs/heads/{branch}",
        headers=headers,
        json={"sha": new_commit_sha},
        timeout=15,
    )
    if patch_resp.status_code != 200:
        return False, f"Failed to update ref (HTTP {patch_resp.status_code})", None
    return True, None, new_commit_sha


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


def _prepare_seed_df(df, account_name, columns, account_col="Account", user_id=None):
    """Align a DataFrame to the seed's column set and set the tenant columns.

    Tenant columns (``Account``/``account`` and ``user_id``) are forcibly
    set on every row so a writer can never accidentally ship rows under
    the wrong tenant. ``user_id`` may be ``None`` (legacy callers); the
    column is still emitted so the seed shape matches
    ``schema.yml``.
    """
    if df is None:
        return pd.DataFrame(columns=columns)
    out = df.copy()
    acct_cols = [c for c in out.columns if str(c).lower() == "account"]
    for c in acct_cols:
        out.drop(columns=[c], inplace=True)
    uid_cols = [c for c in out.columns if str(c).lower() == "user_id"]
    for c in uid_cols:
        out.drop(columns=[c], inplace=True)
    out.insert(0, account_col, account_name)
    out.insert(1, "user_id", "" if user_id is None else int(user_id))
    for col in columns:
        if col not in out.columns:
            out[col] = ""
    return out[columns]


def merge_and_push_seeds(
    account_name,
    history_df,
    current_df,
    *,
    commit_message,
    user_id,
    skip_history=False,
    balances_df=None,
):
    """
    Normalize DataFrames, merge into the GitHub seed files, and commit.
    Both manual uploads and Schwab sync call this so trade_history.csv +
    current_positions.csv stay the single pair of seeds that feed dbt.

    Args:
        history_df: trade rows shaped for HISTORY_SEED_COLUMNS (or None).
        current_df: open-position rows shaped for CURRENT_SEED_COLUMNS.
        user_id: required Postgres ``users.id`` of the row owner. Stamped
            into every emitted row's ``user_id`` column so BigQuery rows
            are tenant-keyed by ``(user_id, account_name)`` rather than
            by ``account_name`` alone. See ``docs/USER_ID_TENANCY.md``.
        balances_df: optional Schwab cash + account_total rows shaped for
            SCHWAB_BALANCE_COLUMNS. Committed atomically with the others.
        skip_history: when True, commit positions only (and balances if given).

    Returns:
        (ok, err_message, history_rows, current_rows, head_commit_sha or None).

    Caller must verify _upload_github_config_ok() first.
    """
    if current_df is None:
        return False, "current_df is required.", 0, 0, None
    if user_id is None:
        # Stage 0+ never wants an unowned write — the cross-tenant guard
        # only works if every row carries the right user_id from day one.
        return False, "user_id is required.", 0, 0, None

    user_id_int = int(user_id)

    if history_df is not None:
        history_df = history_df.copy()
    current_df = current_df.copy()

    for df in [history_df, current_df]:
        if df is None:
            continue
        acct_cols = [c for c in df.columns if c.lower() == "account"]
        if acct_cols:
            df.drop(columns=acct_cols, inplace=True)
        uid_cols = [c for c in df.columns if c.lower() == "user_id"]
        if uid_cols:
            df.drop(columns=uid_cols, inplace=True)
        df.insert(0, "Account", account_name)
        df.insert(1, "user_id", user_id_int)

    if history_df is not None:
        history_standard = {
            "account": "Account", "date": "Date", "action": "Action",
            "symbol": "Symbol", "description": "Description",
            "quantity": "Quantity", "price": "Price",
            "fees_and_comm": "fees_and_comm", "amount": "Amount",
            # Schwab sync emits lowercase analogues
            "transaction_date": "Date", "fees": "fees_and_comm",
        }
        history_col_map = {c: history_standard[c.lower()]
                           for c in history_df.columns if c.lower() in history_standard}
        history_df = history_df.rename(columns=history_col_map)
        for col in HISTORY_SEED_COLUMNS:
            if col not in history_df.columns:
                history_df[col] = ""
        history_df = history_df[HISTORY_SEED_COLUMNS]

    # Schwab API uses cost_basis; seed column is cost_bases
    if "cost_basis" in current_df.columns and "cost_bases" not in current_df.columns:
        current_df = current_df.rename(columns={"cost_basis": "cost_bases"})

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

    path_contents = []
    if not skip_history and history_df is not None:
        history_content = _merge_seed_with_existing(
            HISTORY_PATH, account_name, history_df, HISTORY_SEED_COLUMNS,
            user_id=user_id_int,
        )
        path_contents.append((HISTORY_PATH, history_content))

    current_content = _merge_seed_with_existing(
        CURRENT_PATH, account_name, current_df, CURRENT_SEED_COLUMNS,
        user_id=user_id_int,
    )
    path_contents.append((CURRENT_PATH, current_content))

    if balances_df is not None and len(balances_df) > 0:
        balances_prepared = _prepare_seed_df(
            balances_df,
            account_name,
            SCHWAB_BALANCE_COLUMNS,
            account_col="account",
            user_id=user_id_int,
        )
        bal_content = _merge_seed_with_existing(
            SCHWAB_ACCOUNT_BALANCES_PATH,
            account_name,
            balances_prepared,
            SCHWAB_BALANCE_COLUMNS,
            user_id=user_id_int,
        )
        path_contents.append((SCHWAB_ACCOUNT_BALANCES_PATH, bal_content))

    history_rows = len(history_df) if history_df is not None else 0
    current_rows = len(current_df)

    try:
        ok, err, head_sha = _commit_git_paths(path_contents, commit_message)
        if not ok:
            return False, err or "GitHub commit failed.", history_rows, current_rows, None
    except Exception as exc:
        return False, str(exc), history_rows, current_rows, None

    add_account_for_user(user_id_int, account_name)
    record_upload(user_id_int, account_name, history_rows, current_rows)

    return True, None, history_rows, current_rows, head_sha


def purge_user_id_from_seeds(user_id, *, commit_message):
    """Strip every seed-CSV row whose ``user_id`` matches and commit a
    cleaned version to GitHub in a single atomic commit.

    Why this exists: BigQuery is rebuilt from ``dbt/seeds/*.csv`` on
    every CI run (``.github/workflows/bigquery_update.yml``). Issuing a
    BQ ``DELETE FROM analytics.stg_history WHERE user_id = N`` is reversed
    the next time ``dbt build`` runs because the seed CSVs in GitHub
    still hold those rows. Permanently purging a user from the warehouse
    therefore requires editing the seed CSVs themselves.

    Filter is strict per the tenancy rule: only rows whose ``user_id``
    column equals the target are removed. Rows with empty/NULL
    ``user_id`` (legacy / un-migrated, see
    ``scripts/backfill_seed_user_ids.py``) are left alone — we cannot
    prove they belong to this tenant, and a per-user delete must never
    accidentally take another tenant's history with it.

    Returns ``(ok, error_message, rows_removed_dict, head_commit_sha or None)``
    where ``rows_removed_dict`` maps each seed path to how many rows were
    dropped. ``ok=True`` with empty ``rows_removed`` and ``head_commit_sha=None``
    means no matching rows existed (no commit was created).
    """
    ok, err = _upload_github_config_ok()
    if not ok:
        return False, err, {}, None

    try:
        target = str(int(user_id))
    except (TypeError, ValueError):
        return False, f"Invalid user_id: {user_id!r}", {}, None

    seed_specs = [
        (HISTORY_PATH, HISTORY_SEED_COLUMNS),
        (CURRENT_PATH, CURRENT_SEED_COLUMNS),
        (SCHWAB_ACCOUNT_BALANCES_PATH, SCHWAB_BALANCE_COLUMNS),
    ]

    path_contents = []
    rows_removed = {}

    for path, columns in seed_specs:
        existing_content = _get_file_content(path)
        if not existing_content or not existing_content.strip():
            rows_removed[path] = 0
            continue
        try:
            # dtype=str + keep_default_na=False so we never coerce ""→NaN→"nan"
            # on round-trip, which would dirty every other tenant's row.
            df = pd.read_csv(StringIO(existing_content), dtype=str, keep_default_na=False)
        except Exception as exc:
            return False, f"Could not parse {path}: {exc}", rows_removed, None
        if df.empty:
            rows_removed[path] = 0
            continue

        uid_col = None
        for c in df.columns:
            if str(c).strip().lower() == "user_id":
                uid_col = c
                break
        if uid_col is None:
            # Older seed shapes pre-date the user_id column. Nothing to
            # filter on, so skip rather than guessing by Account.
            rows_removed[path] = 0
            continue

        before = len(df)
        keep_mask = df[uid_col].astype(str).str.strip() != target
        cleaned = df.loc[keep_mask].copy()
        removed = before - len(cleaned)
        if removed == 0:
            rows_removed[path] = 0
            continue

        # Re-align to the canonical seed column shape so the commit
        # doesn't drift column order or drop unrelated columns the
        # CSV happens to be missing.
        for col in columns:
            if col not in cleaned.columns:
                cleaned[col] = ""
        cleaned = cleaned[columns]
        path_contents.append((path, cleaned.to_csv(index=False)))
        rows_removed[path] = removed

    if not path_contents:
        return True, None, rows_removed, None

    try:
        ok, err, head_sha = _commit_git_paths(path_contents, commit_message)
    except Exception as exc:
        return False, str(exc), rows_removed, None
    if not ok:
        return False, err or "GitHub commit failed.", rows_removed, None
    return True, None, rows_removed, head_sha


@app.route("/upload", methods=["GET", "POST"])
@login_required
@limiter.limit("30 per minute", exempt_when=lambda: request.method != "POST")
def upload():
    pat_configured = bool(os.environ.get("GITHUB_PAT", "").strip())

    if request.method == "POST":
        # Demo is read-only. Without this, any visitor could replace the seed
        # CSVs that every other demo viewer is reading from.
        blocked = demo_block_writes("uploading new trade data")
        if blocked:
            return blocked

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
    # Sharing labels across users is allowed — tenant isolation is
    # enforced at the BQ row level by ``user_id`` everywhere downstream
    # (``_account_sql_and`` adds the user_id predicate to every query;
    # ``_filter_df_by_accounts`` re-filters every DataFrame). See
    # docs/USER_ID_TENANCY.md.
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Merge into GitHub seeds (same path as Schwab sync)
    # ------------------------------------------------------------------
    if skip_history:
        commit_msg = (
            f"Upload by {current_user.username}: "
            f"positions only, {len(current_df)} current rows ({account_name})"
        )
    else:
        commit_msg = (
            f"Upload by {current_user.username}: "
            f"{len(history_df)} history rows, {len(current_df)} current rows "
            f"({account_name})"
        )

    is_first_upload = False
    try:
        is_first_upload = count_uploads_for_user(current_user.id) == 0
    except Exception:
        is_first_upload = False

    ok, err, history_rows, current_rows, head_sha = merge_and_push_seeds(
        account_name,
        history_df,
        current_df,
        commit_message=commit_msg,
        user_id=current_user.id,
        skip_history=skip_history,
    )
    if not ok:
        from app import app as _app
        _app.logger.error("Upload seeds update failed: %s", err)
        flash("Couldn't save that upload right now. Try again in a moment, or contact support if it keeps happening.", "danger")
        return redirect(url_for("upload"))

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

    qp = {}
    if head_sha:
        qp["sha"] = head_sha
    if is_first_upload:
        qp["first"] = 1
    return redirect(url_for("upload_processing", **qp))


def _github_workflow_state_for_head(head_sha: str) -> dict:
    """
    Return dict with keys: state, github_status, conclusion, html_url, error.
    state is pending | running | success | failure | error
    """
    if not head_sha or len(head_sha) < 7:
        return {"state": "error", "error": "invalid_sha"}
    ok, _e = _upload_github_config_ok()
    if not ok:
        return {"state": "error", "error": "github_not_configured"}
    parts = _github_repo().split("/", 1)
    if len(parts) != 2:
        return {"state": "error", "error": "bad_repo"}
    owner, repo = parts
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs"
    try:
        resp = requests.get(
            url,
            headers=_github_headers(),
            params={"head_sha": head_sha.strip(), "per_page": 5},
            timeout=20,
        )
    except OSError as e:
        return {"state": "error", "error": str(e)}

    if resp.status_code == 403:
        return {
            "state": "error",
            "error": "github_actions_forbidden",
        }
    if resp.status_code != 200:
        return {
            "state": "error",
            "error": f"HTTP {resp.status_code}",
        }
    data = resp.json() or {}
    runs = data.get("workflow_runs") or []
    if not runs:
        return {
            "state": "pending",
            "github_status": None,
            "conclusion": None,
            "html_url": None,
        }
    w = runs[0]
    ghs = w.get("status")
    concl = w.get("conclusion")
    wurl = w.get("html_url")
    if ghs is None or ghs in ("queued", "waiting", "requested", "pending"):
        return {
            "state": "pending",
            "github_status": ghs,
            "conclusion": concl,
            "html_url": wurl,
        }
    if ghs == "in_progress":
        return {
            "state": "running",
            "github_status": ghs,
            "conclusion": concl,
            "html_url": wurl,
        }
    if ghs == "completed":
        if concl == "success":
            return {
                "state": "success",
                "github_status": ghs,
                "conclusion": concl,
                "html_url": wurl,
            }
        return {
            "state": "failure",
            "github_status": ghs,
            "conclusion": concl or "unknown",
            "html_url": wurl,
        }
    return {
        "state": "pending",
        "github_status": ghs,
        "conclusion": concl,
        "html_url": wurl,
    }


@app.route("/api/github/workflow-status")
@login_required
@limiter.limit("60 per minute")
def api_github_workflow_status():
    """Poll GitHub Actions for the workflow run associated with a commit (head) SHA."""
    head_sha = (request.args.get("sha") or "").strip()
    st = _github_workflow_state_for_head(head_sha)
    if st.get("state") == "error" and st.get("error") == "invalid_sha":
        return jsonify({"ok": False, "error": "invalid sha", "state": "error"}), 400
    st["ok"] = True
    return jsonify(st)


@app.route("/upload/processing")
@login_required
def upload_processing():
    """Intermediary page shown after upload while data refreshes."""
    expected_minutes = 3
    head_sha = (request.args.get("sha") or "").strip() or None
    is_first = (request.args.get("first") or "").strip() == "1"

    if is_first:
        done_url = url_for("first_look", from_upload=1)
    else:
        done_url = url_for("weekly_review", from_upload=1)

    return render_template(
        "upload_processing.html",
        title="Processing Upload",
        expected_minutes=expected_minutes,
        head_sha=head_sha,
        done_url=done_url,
    )


@app.route("/sync/processing")
@login_required
def sync_processing():
    """After Schwab seed push, wait for GitHub Actions dbt to finish (optional poll by commit SHA)."""
    expected_minutes = 5
    head_sha = (request.args.get("sha") or "").strip() or None
    is_first = (request.args.get("first") or "").strip() == "1"

    if is_first:
        done_url = url_for("first_look", from_sync=1)
    else:
        done_url = url_for("weekly_review", from_sync=1)

    return render_template(
        "sync_processing.html",
        title="Processing Schwab sync",
        expected_minutes=expected_minutes,
        head_sha=head_sha,
        done_url=done_url,
    )


@app.route("/unclaim-account", methods=["POST"])
@login_required
def unclaim_account():
    """Unlink an account from the current user."""
    blocked = demo_block_writes("removing accounts from your profile")
    if blocked:
        return blocked
    account_name = request.form.get("unclaim_account_name", "").strip()
    if not account_name:
        flash("No account selected to remove.", "danger")
        return redirect(url_for("upload"))
    remove_account_for_user(current_user.id, account_name)
    flash(f"Account \"{account_name}\" removed from your profile.", "info")
    return redirect(url_for("upload"))
