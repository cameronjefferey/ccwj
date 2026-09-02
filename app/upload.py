import os
import re
import hmac
import threading
from contextlib import contextmanager
from datetime import date, datetime
from functools import wraps
import requests
import pandas as pd
from io import StringIO
from flask import render_template, request, flash, redirect, url_for, jsonify, abort
from flask_login import login_required, current_user
from app import app
from app.extensions import csrf, limiter
from app.models import (
    get_accounts_for_user, add_account_for_user,
    remove_account_for_user,
    record_upload, get_uploads_for_user, count_uploads_for_user,
    get_or_create_broker_tenant, get_broker_tenants_for_user,
    delete_broker_tenant, MANUAL_BROKER_SLUG,
)
from app.utils import demo_block_writes
from app.seed_store import (
    SeedStoreError,
    is_production_store,
    read_seed_csv as _seed_store_read,
    write_seed_csvs as _seed_store_write,
)


# Every seed writer performs a read/merge/write against the same three raw
# tables (see app/seed_store.py). Hold one cluster-wide lock across that
# entire operation so a webhook, cron, manual sync, or CSV upload cannot
# build content from a stale read and overwrite another writer's rows.
SEED_WRITE_LOCK_KEY = 8274013
_seed_write_lock_state = threading.local()

# CSV export guides on /upload. Only Schwab has steps today; every other
# row is a collapsed "request this brokerage" that posts to /feedback.
# Add a ready=True entry (and the matching template branch) when a new
# parser/export guide ships. Keep this list aligned with the brokers we
# already name in product copy (SnapTrade connect supports more).
CSV_EXPORT_BROKERS = (
    {"slug": "schwab", "name": "Charles Schwab", "ready": True},
    {"slug": "fidelity", "name": "Fidelity", "ready": False},
    {"slug": "vanguard", "name": "Vanguard", "ready": False},
    {"slug": "robinhood", "name": "Robinhood", "ready": False},
    {"slug": "interactive", "name": "Interactive Brokers", "ready": False},
    {"slug": "alpaca", "name": "Alpaca", "ready": False},
    {"slug": "wealthsimple", "name": "Wealthsimple", "ready": False},
)


@contextmanager
def seed_write_lock():
    """Cluster-wide seed lock, re-entrant within the current worker thread."""
    depth = getattr(_seed_write_lock_state, "depth", 0)
    if depth:
        _seed_write_lock_state.depth = depth + 1
        try:
            yield
        finally:
            _seed_write_lock_state.depth -= 1
        return

    # Runtime import avoids a module cycle and lets unit tests replace the
    # Postgres lock with an in-memory context manager.
    from app.db import advisory_lock
    with advisory_lock(SEED_WRITE_LOCK_KEY):
        _seed_write_lock_state.depth = 1
        try:
            yield
        finally:
            _seed_write_lock_state.depth = 0


def _serialized_seed_write(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        with seed_write_lock():
            return func(*args, **kwargs)
    return wrapped


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
# Tenancy columns are at the front of every seed so the first three
# cells of any row identify the tenant unambiguously:
#   1. Account     — user-facing display label (informational)
#   2. user_id     — Postgres users.id (informational metadata)
#   3. tenant_id   — v2 warehouse tenant key (``<broker_slug>:<broker_uuid>``;
#                    see docs/V2_TENANT_KEY_DESIGN.md). Required on every
#                    writer-emitted row.
HISTORY_SEED_COLUMNS = [
    "Account", "user_id", "tenant_id",
    "Date", "Action", "Symbol", "Description",
    "Quantity", "Price", "fees_and_comm", "Amount",
]
CURRENT_SEED_COLUMNS = [
    "Account", "user_id", "tenant_id",
    "Symbol", "Description", "Quantity", "Price",
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

# Broker-agnostic balance seed (cash + account-total rows for equity
# snapshots). Native Schwab sync, SnapTrade sync, and any future
# broker connector write here. Trade history goes to trade_history.csv
# and open positions to current_positions.csv — the same seeds the
# manual upload path uses — so there's a single pipeline into dbt.
BALANCE_SEED_PATH = "dbt/seeds/account_balances.csv"

BALANCE_SEED_COLUMNS = [
    "account",
    "user_id",
    "tenant_id",
    "row_type",
    "market_value",
    "cost_basis",
    "unrealized_pnl",
    "unrealized_pnl_pct",
    "percent_of_account",
]

# Backwards-compat aliases for callers that still import the Schwab-named
# constants (third-party scripts, older imports). Safe to remove once a
# repo-wide grep returns no hits.
SCHWAB_ACCOUNT_BALANCES_PATH = BALANCE_SEED_PATH
SCHWAB_BALANCE_COLUMNS = BALANCE_SEED_COLUMNS


# ---------------------------------------------------------------------------
# Crypto symbol whitelist
# ---------------------------------------------------------------------------
# Mirror of ``dbt/seeds/crypto_symbols.csv`` for fast in-process checks
# without a BigQuery round-trip. Used by:
#   - ``app.snaptrade_normalize._is_crypto`` to emit
#     ``security_type='Cryptocurrency'`` on positions.
#   - ``app.routes._is_crypto_symbol`` (and downstream
#     ``_compute_breakdown_by_type``) to relabel the per-symbol
#     Position Detail card as Crypto instead of Equity.
#
# Source of truth is the dbt seed (auditable, queryable from BQ); this
# constant is a runtime mirror. A pytest in
# ``tests/test_snaptrade_normalize.py`` reads both and asserts they are
# the same set so the two never silently drift.
CRYPTO_SYMBOLS: frozenset[str] = frozenset({
    "BTC", "ETH", "SOL", "USDC", "USDT", "DAI", "ADA", "AVAX", "DOGE",
    "DOT", "LINK", "LTC", "MATIC", "XRP", "ATOM", "BCH", "ALGO", "XLM",
    "NEAR", "APT", "ARB", "OP", "SHIB", "UNI", "ETC", "FIL", "SUI",
    "TON", "TRX", "PEPE", "INJ", "SEI", "HBAR", "ICP", "GRT", "AAVE",
    "MKR", "SNX", "COMP", "CRV", "LDO", "RNDR", "FET", "TAO", "JUP",
    "WIF", "BONK", "PYUSD",
    "AURORA", "BOBA", "EOS", "MORPHO", "OMG",
})


def is_crypto_symbol(symbol: str) -> bool:
    """Whether ``symbol`` (case-insensitive) is on the curated crypto
    whitelist. See ``CRYPTO_SYMBOLS`` for the full set and the dbt
    seed for the source of truth."""
    if not symbol:
        return False
    return str(symbol).strip().upper() in CRYPTO_SYMBOLS


def _github_repo() -> str:
    """owner/repo for the GitHub API (override with GITHUB_REPO)."""
    return os.environ.get("GITHUB_REPO", "cameronjefferey/ccwj").strip()


def _github_branch() -> str:
    """Git ref the dispatched rebuild workflow runs on (override with
    GITHUB_BRANCH; seed DATA no longer flows through git)."""
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


class SeedFetchError(RuntimeError):
    """Raised when reading the existing seed from the store fails for any
    reason other than the table genuinely not existing yet (first write).

    A merge that proceeds on a transient fetch failure would silently
    treat the seed as empty and overwrite every other tenant's history
    with just the syncing user's new rows. That actually happened in
    production once (see commit ``3f4aecb`` — Sara Investment sync wiped
    10,446 rows belonging to four other accounts and three other users).
    The merge MUST refuse to run unless we can distinguish "seed does
    not yet exist" from "the read blipped".
    """


def _get_file_content(path):
    """
    Fetch the current seed content (CSV text) from the BigQuery seed
    store. Returns the CSV string, or ``None`` only when the raw table
    truly does not exist yet (first-ever write — the analogue of the old
    GitHub 404). Raises ``SeedFetchError`` on any other failure (network,
    auth, quota) so callers cannot accidentally treat a transient blip as
    "no existing data". See ``_merge_seed_with_existing`` and the Bug A
    note in the SeedFetchError docstring.

    Name kept from the GitHub-storage era — it is the read half of the
    storage seam that every merge test stubs.
    """
    try:
        return _seed_store_read(path)
    except SeedStoreError as exc:
        raise SeedFetchError(str(exc)) from exc


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


def _normalize_tid(value) -> str:
    """Canonicalize a ``tenant_id`` cell for merge-scope comparisons."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return ""
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
# collapses these. Non-numeric cells (Action, Symbol, Description) are
# returned unchanged so we don't accidentally normalize away semantic content.
# Date is handled separately by ``_canonicalize_date_mdy`` (CSV vs SnapTrade
# zero-padding). Currency-formatted Amount/Price ("$1,150.00", "($26.99)")
# from a Schwab web export is treated as numeric so it keys with SnapTrade's
# bare float. Broker blank sentinels (``--``, ``N/A``) are empty: Schwab's
# CSV writes them in Quantity/Price on dividends and expiries, SnapTrade
# writes a true blank, and ``stg_history`` ``safe_cast`` turns both into
# NULL so CHECK 1 groups them. Run 33140422151: action-alias repair
# dropped 1 of 11,274 raw rows and the warehouse test still saw 153
# groups — the leftover pairs differ by these sentinels / Amount ``""``
# vs ``0`` (``coalesce(amount, 0)``).
_BLANK_NUMERIC_SENTINELS = frozenset({
    "--", "---", "—", "–", "n/a", "#n/a", "na", "#na", "null",
})


def _canonicalize_seed_cell(value):
    """Normalize a seed cell for the merge dedup key.

    - ``None`` / NaN / ``"nan"`` / ``"None"`` / ``"<NA>"`` / broker blank
      sentinels (``--``, ``N/A``) → empty string.
    - Numeric-looking cells → ``"%.6f"`` (trailing-zero stripped) so float
      precision drift across syncs does not break dedup. ``$``, thousands
      commas, and accounting ``(123.45)`` negatives are stripped first.
    - Everything else → ``str(value).strip()``.
    """
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return ""
    if s.lower() in _BLANK_NUMERIC_SENTINELS:
        return ""
    # Strip every ``$`` (not just a leading one). Schwab debits write
    # ``-$4,600.00``; ``float("-$4600.00")`` fails and the cell used to
    # stay non-numeric. Staging ``parse_seed_number`` has the same rule
    # (warehouse run 33141412571: 893 option-multiplier failures).
    s_num = s.replace(",", "").replace("$", "").strip()
    if len(s_num) >= 2 and s_num[0] == "(" and s_num[-1] == ")":
        s_num = "-" + s_num[1:-1].strip()
    try:
        f = float(s_num)
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


# SnapTrade writes Date via ``strftime('%m/%d/%Y')`` (zero-padded). Schwab's
# web CSV export omits the leading zero (``5/14/2024``). stg_history parses
# both with ``parse_date('%m/%d/%Y', …)`` so they become the SAME trade_date
# and trip ``stg_history_no_duplicate_fills_per_tenant`` unless the merge
# key treats them as one fill. ISO ``YYYY-MM-DD`` (pandas datetime
# round-trip) is accepted too. Production: run 33132317666, 2026-08-28 —
# a CSV upload onto a SnapTrade tenant grew stg_history 6.9k → 10.0k and
# failed the test with 153 groups.
_DATE_MDY_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
_DATE_ISO_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
_DATE_ISO_SLASH_RE = re.compile(r"^(\d{4})/(\d{2})/(\d{2})")
_DATE_MDY_YY_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{2})(?:\s|$)")


def _canonicalize_date_mdy(value):
    """Normalize a Date cell to zero-padded ``MM/DD/YYYY``.

    Unrecognized values fall through to ``str.strip()`` so a weird broker
    date still keys as itself instead of collapsing into ``""``.
    """
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if isinstance(value, datetime):
        return value.strftime("%m/%d/%Y")
    if isinstance(value, date):
        return value.strftime("%m/%d/%Y")
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return ""
    m = _DATE_MDY_RE.search(s)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(year, month, day).strftime("%m/%d/%Y")
        except ValueError:
            return s
    m = _DATE_ISO_RE.search(s)
    if m:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(year, month, day).strftime("%m/%d/%Y")
        except ValueError:
            return s
    m = _DATE_ISO_SLASH_RE.search(s)
    if m:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(year, month, day).strftime("%m/%d/%Y")
        except ValueError:
            return s
    m = _DATE_MDY_YY_RE.search(s)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year += 2000 if year < 100 else 0
        try:
            return date(year, month, day).strftime("%m/%d/%Y")
        except ValueError:
            return s
    return s


def _canonicalize_key_cell(col, value):
    """Dedup-key canonicalizer: Date → MM/DD/YYYY, everything else as usual."""
    if str(col).lower() == "date":
        return _canonicalize_date_mdy(value)
    return _canonicalize_seed_cell(value)


# Mirrors ``stg_history.sql`` ``cleaned.action`` (lower(trim(action)) CASE).
# SnapTrade writes ``Cash Dividend`` for type=DIVIDEND and ``Qualified
# Dividend`` for type=QUALIFIEDDIVIDEND; Schwab's CSV export uses
# ``Qualified Dividend`` for the same coupon. The warehouse test groups
# on this normalized action, so the merge key must too — otherwise a CSV
# upload onto a SnapTrade tenant rematerializes every overlapping
# dividend (run 33139304912: 153 groups; date-padding repair dropped 0).
# Unmapped non-empty labels become ``other`` (same as dbt); blank stays
# blank so we never fuse empty-action rows into that bucket.
_STG_HISTORY_ACTION = {
    "buy": "equity_buy",
    "sell": "equity_sell",
    "sell short": "equity_sell_short",
    "sell to open": "option_sell_to_open",
    "buy to close": "option_buy_to_close",
    "buy to open": "option_buy_to_open",
    "sell to close": "option_sell_to_close",
    "expired": "option_expired",
    "assigned": "option_assigned",
    "exchange or exercise": "option_exercised",
    "qualified dividend": "dividend",
    "cash dividend": "dividend",
    "special dividend": "dividend",
    "special qual div": "dividend",
    "pr yr cash div": "dividend",
    "margin interest": "margin_interest",
    "credit interest": "credit_interest",
    "adr mgmt fee": "adr_fee",
    "deposit": "cash_transfer",
    "withdrawal": "cash_transfer",
    "cash transfer": "cash_transfer",
    "funds received": "cash_transfer",
    "moneylink transfer": "cash_transfer",
    "journal": "cash_transfer",
}
_STG_CASH_OUT_ACTIONS = {
    "equity_buy", "option_buy_to_open", "option_buy_to_close",
    "margin_interest", "adr_fee",
}
_STG_CASH_IN_ACTIONS = {
    "equity_sell", "equity_sell_short", "option_sell_to_open",
    "option_sell_to_close", "dividend", "credit_interest",
}


def _normalize_history_action(value):
    """Map a seed Action cell to ``stg_history.action``."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return ""
    return _STG_HISTORY_ACTION.get(s.lower(), "other")


def _canonicalize_stg_amount(action, amount):
    """Amount as ``stg_history.amount_signed`` would emit it.

    Empty / unparseable Amount becomes ``0`` to match
    ``coalesce(safe_cast(amount), 0)`` in ``stg_history`` — CSV expiries
    and some coupons land with a blank Amount while SnapTrade writes
    ``0`` / ``0.0``.
    """
    base = _canonicalize_seed_cell(amount)
    if base == "":
        return "0"
    try:
        f = float(base)
    except (TypeError, ValueError):
        return "0"
    norm = _normalize_history_action(action)
    if norm in _STG_CASH_OUT_ACTIONS:
        f = -abs(f)
    elif norm in _STG_CASH_IN_ACTIONS:
        f = abs(f)
    return _canonicalize_seed_cell(f)


# Cross-source Price precision. SnapTrade's two feeds report a fill's Price at
# DIFFERENT precision: the ``recent_orders`` feed derives it at full float
# precision (e.g. 131.960622, 0.486667, 351.43513) while ``activities`` carries
# the broker's already-rounded 4-decimal Price (131.9606, 0.4867, 351.4351).
# The 6-decimal canonical form (``_canonicalize_seed_cell``) preserves that
# 5th/6th-digit drift, so the same fill keys DIFFERENTLY across sources and the
# cross-source dedup keeps both → doubled shares / phantom cost basis (AAOI,
# Aug 2026: 225 shares reported twice → +$29.7k phantom equity, -$29.8k phantom
# unrealized). Rounding the cross-source key's Price to 4dp absorbs the orders
# feed's extra precision (it always rounds to the activities 4dp value) while
# staying fine enough to keep genuinely distinct fills — including sub-penny
# option prices like 0.4867 — apart. This is the exact parallel of why Amount
# is omitted from the cross-source key. (Known boundary: a broker whose
# activities feed reported Price at <4dp would need this coarsened further.)
_CROSS_SOURCE_PRICE_DP = 4
# Schwab's web CSV prints equity Price at cents (``$58.97``) while SnapTrade
# keeps the fill Price (``58.965``). Those miss the 4dp Price key and both
# survive → doubled shares / phantom unrealized (jefflsmith JEPQ IRA, Aug
# 2026). Amount is the same cent proceeds; rounding it to 2dp is the
# CSV-vs-SnapTrade collapse. Distinct option fills a few tenths of a cent
# apart have different Amounts and stay separate.
_CROSS_SOURCE_AMOUNT_DP = 2


def _canonicalize_cross_source_price(value):
    """Canonicalize a Price cell for the cross-source dedup key: same as
    :func:`_canonicalize_seed_cell` but rounded to 4dp so orders-vs-activities
    float-precision drift on Price collapses to one key. Non-numeric cells fall
    through to the standard canonicalizer."""
    base = _canonicalize_seed_cell(value)
    if base == "":
        return ""
    try:
        f = round(float(base), _CROSS_SOURCE_PRICE_DP)
    except (TypeError, ValueError):
        return base
    out = f"{f:.{_CROSS_SOURCE_PRICE_DP}f}".rstrip("0").rstrip(".")
    if out in ("", "-", "-0"):
        return "0"
    return out


def _canonicalize_cross_source_amount(action, amount):
    """Amount for the CSV-vs-SnapTrade fill key: stg-signed, then 2dp.

    Absorbs Schwab CSV cent-rounded Price vs SnapTrade's extra decimals
    when Quantity × Price proceeds already agree. Non-numeric cells follow
    :func:`_canonicalize_stg_amount` (blank → ``0``).
    """
    base = _canonicalize_stg_amount(action, amount)
    try:
        f = round(float(base), _CROSS_SOURCE_AMOUNT_DP)
    except (TypeError, ValueError):
        return base
    return _canonicalize_seed_cell(f)


def _dedup_history_rows(df, seed_columns):
    """Collapse byte-different but value-identical history rows.

    Used for ``HISTORY_PATH`` writes only. Schwab's transactions API
    has been observed returning the SAME fill twice in one response
    with different float-text formatting (``100`` vs ``100.0`` /
    ``-7660`` vs ``-7660.0``); without an explicit canonicalize+dedup
    step those byte-different forms both survive and produce phantom
    doubled trades downstream (positions_summary, mart_daily_pnl,
    every UI surface).

    Match grain: the trade key columns from ``seed_columns``,
    excluding ``account`` and ``user_id`` (tenant metadata, not part
    of the trade's identity). Last-write-wins on collision so a fresh
    sync always overrides a stale row when both are pinned to the
    syncing user.

    Bug shipped May 2026 (commit ``cafc0713``: Sara Investment ASTS
    x2 — both float-drift forms in one Schwab sync response, the
    pre-existing dedup branch was bypassed because
    ``existing_account_df.empty == True`` for a freshly-linked
    account). Regression test:
    ``tests/test_upload_merge.py::test_dedup_collapses_drift_within_new_df_even_when_existing_empty``.
    """
    if df is None or df.empty:
        return df
    # ``account`` and ``user_id`` are informational metadata, not part of
    # the trade's identity. ``tenant_id`` IS part of the dedup key under
    # v2 (see docs/V2_TENANT_KEY_DESIGN.md).
    key_cols = [
        c for c in seed_columns
        if str(c).lower() not in ("account", "user_id")
    ]
    if not key_cols:
        return df
    canon = df[key_cols].copy()
    for c in key_cols:
        if c in canon.columns:
            canon[c] = canon[c].map(lambda v, _c=c: _canonicalize_key_cell(_c, v))
    keep_mask = ~canon.duplicated(subset=key_cols, keep="last")
    df = df.loc[keep_mask].reset_index(drop=True)

    # ---- Second pass: cross-source dedup -----------------------------------
    # SnapTrade has TWO sources of truth for the same trade. The
    # ``recent_orders`` endpoint reflects executed orders within seconds
    # (real-time); the ``activities`` endpoint takes hours-to-days to
    # ingest the same fills. Our pipeline writes both, so the same
    # economic trade can land twice in one merge — once with a thin
    # Description ("NVIDIA Corporation", from orders) and once with a
    # richer Description ("Bought 98 NVDA at market", from activities).
    # The strict-key dedup above does NOT catch this because Description
    # differs (and so does ``fees_and_comm`` and often ``Amount`` —
    # orders derives Amount = qty * exec_price at full precision while
    # activities carries the broker's cent-rounded Amount).
    #
    # Cross-source key: (Date, Action, Symbol, Quantity, Price). These
    # five cells uniquely identify a trade fill. ``Amount`` is omitted
    # because it's derived from Quantity * Price ± rounding direction
    # — keying on it lets sub-cent FP drift between the two sources
    # defeat the dedup. ``Description`` and ``fees_and_comm`` are
    # omitted because they're the cells the two sources legitimately
    # disagree on.
    #
    # Risk analysis for omitting Amount: two trades with identical
    # Date+Action+Symbol+Quantity+Price MUST have identical Amount
    # modulo rounding (Amount = ±qty × price). Any case where
    # Amount differs but the other five agree is a rounding artifact,
    # not a different trade. Keeping both rows would double-count the
    # same money.
    #
    # On collision, prefer the row with the LONGER non-empty
    # Description (heuristic: activities-source has the broker's
    # original wording, which is more useful to users than the
    # symbol-name fallback orders-source emits).
    #
    # ELIGIBILITY GUARD (critical). This pass drops BOTH Description and
    # Amount from the key, so it must ONLY see rows whose remaining cells
    # (Symbol, Price, Quantity) actually pin the identity — i.e. real trade
    # fills. Non-fill events (Expired, dividends, ADR/regulatory fees, bank
    # interest) land with a BLANK Symbol and/or Price and are distinguished
    # ONLY by Description/Amount: e.g. four different expired contracts on
    # one day (CRWV/PLTR/QBTS/RKLB, Symbol="", Price="") or four different
    # fee lines (Symbol="", Price="", distinct Amount). The orders feed NEVER
    # emits those (it only reports BUY/SELL/option fills with a symbol and a
    # price), so there is no orders-vs-activities collision to resolve for
    # them — and collapsing them here would silently DELETE genuinely
    # distinct rows. So require a non-empty Symbol AND Price; every other row
    # is passed through untouched (the strict first pass already handled its
    # exact/float-drift dupes).
    cross_key_lower = {"date", "action", "symbol", "quantity", "price"}
    cross_key_cols = [
        c for c in seed_columns
        if str(c).lower() in cross_key_lower
    ]
    if len(cross_key_cols) < len(cross_key_lower):
        # Caller's seed_columns doesn't have the full identity set —
        # bail out rather than dedup with a partial key.
        return df
    if "Description" not in df.columns:
        return df

    df = df.reset_index(drop=True)
    sym_col = next((c for c in df.columns if str(c).lower() == "symbol"), None)
    price_col = next((c for c in cross_key_cols if str(c).lower() == "price"), None)
    sym_blank = df[sym_col].map(lambda v: _canonicalize_seed_cell(v) == "")
    price_blank = df[price_col].map(lambda v: _canonicalize_seed_cell(v) == "")
    eligible = ~(sym_blank | price_blank)

    canon2 = df[cross_key_cols].copy()
    for c in cross_key_cols:
        if c == price_col:
            # Price drifts across sources by trailing precision (orders 6dp vs
            # activities 4dp) — round it in the key so the same fill collides.
            canon2[c] = canon2[c].map(_canonicalize_cross_source_price)
        elif str(c).lower() == "action":
            # Buy/BUY and Cash Dividend/Qualified Dividend are one action
            # after stg_history parse — key them that way here too.
            canon2[c] = df[c].map(_normalize_history_action)
        else:
            canon2[c] = canon2[c].map(lambda v, _c=c: _canonicalize_key_cell(_c, v))
    desc_lens = df["Description"].fillna("").astype(str).str.len()
    # Visit longer-description rows first so the richer one wins its group.
    order = (-desc_lens.to_numpy()).argsort(kind="stable")

    seen: set = set()
    drop_positions: set = set()
    for pos in order:
        if not bool(eligible.iloc[pos]):
            continue  # non-fill event — never cross-source deduped
        key = tuple(canon2.iloc[pos][c] for c in cross_key_cols)
        if key in seen:
            drop_positions.add(pos)
        else:
            seen.add(key)
    if drop_positions:
        keep_mask2 = [i not in drop_positions for i in range(len(df))]
        df = df.loc[keep_mask2].reset_index(drop=True)

    # ---- Third pass: staging CHECK 1 grain --------------------------------
    # Dividend / interest rows usually have a ticker and a BLANK Price, so
    # the fill pass above skips them. CSV ``Qualified Dividend`` vs
    # SnapTrade ``Cash Dividend`` then survive until stg_history maps both
    # to ``dividend`` and the warehouse test fails. Eligibility requires a
    # Symbol so blank-symbol expiries / fee lines (distinguished only by
    # Description) are never fused — same guard as the fill pass, Price
    # allowed blank. Amount is re-signed the way stg_history does.
    # cash_transfer is the exception: Schwab CSV ``Funds Received`` /
    # ``MoneyLink Transfer`` vs SnapTrade ``Deposit`` have a NULL Symbol.
    amount_col = next((c for c in seed_columns if str(c).lower() == "amount"), None)
    date_col = next((c for c in seed_columns if str(c).lower() == "date"), None)
    action_col = next((c for c in seed_columns if str(c).lower() == "action"), None)
    qty_col = next((c for c in seed_columns if str(c).lower() == "quantity"), None)
    if not all([amount_col, date_col, action_col, qty_col, sym_col, price_col]):
        return df
    if "Description" not in df.columns:
        return df

    df = df.reset_index(drop=True)
    eligible3 = (
        df[sym_col].map(lambda v: _canonicalize_seed_cell(v) != "")
        | df[action_col].map(
            lambda v: _normalize_history_action(v) == "cash_transfer"
        )
    )
    desc_lens3 = df["Description"].fillna("").astype(str).str.len()
    order3 = (-desc_lens3.to_numpy()).argsort(kind="stable")
    seen3: set = set()
    drop3: set = set()
    for pos in order3:
        if not bool(eligible3.iloc[pos]):
            continue
        key3 = (
            _canonicalize_date_mdy(df.iloc[pos][date_col]),
            _normalize_history_action(df.iloc[pos][action_col]),
            _canonicalize_seed_cell(df.iloc[pos][sym_col]),
            _canonicalize_seed_cell(df.iloc[pos][qty_col]),
            _canonicalize_seed_cell(df.iloc[pos][price_col]),
            _canonicalize_stg_amount(
                df.iloc[pos][action_col], df.iloc[pos][amount_col],
            ),
        )
        if key3 in seen3:
            drop3.add(pos)
        else:
            seen3.add(key3)
    if drop3:
        keep_mask3 = [i not in drop3 for i in range(len(df))]
        df = df.loc[keep_mask3].reset_index(drop=True)

    # ---- Fourth pass: CSV cent Price vs SnapTrade fill Price -------------
    # Pass 2 keys Price at 4dp (orders vs activities). Schwab's CSV prints
    # equity Price at 2dp (``$58.97``) against SnapTrade ``58.965`` — those
    # miss pass 2 and pass 3 (Price is in that key too). Amount is the same
    # cent proceeds. Key (Date, Action, Symbol, Quantity, Amount@2dp) on
    # fills with a Quantity so blank-qty coupons stay on pass 3. Two real
    # option fills a few tenths of a cent apart have different Amounts.
    df = df.reset_index(drop=True)
    qty_blank = df[qty_col].map(lambda v: _canonicalize_seed_cell(v) == "")
    eligible4 = (
        df[sym_col].map(lambda v: _canonicalize_seed_cell(v) != "")
        & ~qty_blank
    )
    desc_lens4 = df["Description"].fillna("").astype(str).str.len()
    order4 = (-desc_lens4.to_numpy()).argsort(kind="stable")
    seen4: set = set()
    drop4: set = set()
    for pos in order4:
        if not bool(eligible4.iloc[pos]):
            continue
        key4 = (
            _canonicalize_date_mdy(df.iloc[pos][date_col]),
            _normalize_history_action(df.iloc[pos][action_col]),
            _canonicalize_seed_cell(df.iloc[pos][sym_col]),
            _canonicalize_seed_cell(df.iloc[pos][qty_col]),
            _canonicalize_cross_source_amount(
                df.iloc[pos][action_col], df.iloc[pos][amount_col],
            ),
        )
        if key4 in seen4:
            drop4.add(pos)
        else:
            seen4.add(key4)
    if not drop4:
        return df
    keep_mask4 = [i not in drop4 for i in range(len(df))]
    return df.loc[keep_mask4].reset_index(drop=True)


# Sentinel so ``_merge_seed_with_existing`` can tell "fetch the file from
# GitHub" (default) apart from an explicit ``existing_content=None`` (caller
# asserting the file does not exist yet — same as a 404). Needed for batched
# multi-account commits that fold accounts in-memory without re-fetching.
_FETCH_FROM_GITHUB = object()


def _merge_seed_with_existing(
    path, account_name, new_df, seed_columns,
    *, tenant_id=None, existing_content=_FETCH_FROM_GITHUB,
):
    """
    Merge new account data with existing seed data.
    - Fetches current file from GitHub (unless ``existing_content`` is given)
    - For current positions: replace that account's rows (snapshot semantics)
    - For history: append new rows for that account and de-duplicate
    - Returns CSV string ready to commit

    ``existing_content`` — by default the current file is fetched from GitHub.
    A caller batching several accounts into ONE commit passes the running
    merged CSV of the PREVIOUS account here so each account folds onto the
    last instead of re-fetching (and clobbering) the branch. Pass the raw CSV
    string, or ``None`` to mean "file does not exist yet" (same as a 404).

    ``tenant_id`` (the syncing broker tenant key) MUST be passed for any
    merge that lands user-facing data. The dedup window is scoped to
    rows whose ``tenant_id`` matches the syncing tenant plus legacy
    unowned rows (``tenant_id=""`` under the same account label from
    before v2). Rows owned by OTHER tenants are kept verbatim in
    ``other_df`` and never touched — required by the tenant-isolation
    rule. See ``.cursor/rules/bigquery-tenant-isolation.mdc`` and
    ``docs/V2_TENANT_KEY_DESIGN.md``.

    Raises ``SeedFetchError`` if the existing seed cannot be fetched
    for any reason other than HTTP 404 — see commit ``3f4aecb`` for
    why a silent "treat as empty" path is unacceptable.
    """
    if existing_content is _FETCH_FROM_GITHUB:
        existing_content = _get_file_content(path)
    if existing_content is None:
        # File truly does not exist yet (HTTP 404). Safe to use only new data.
        for col in seed_columns:
            if col not in new_df.columns:
                new_df[col] = ""
        merged = new_df[seed_columns]
        if path == HISTORY_PATH:
            merged = _dedup_history_rows(merged, seed_columns)
        return merged.to_csv(index=False)
    if not existing_content.strip():
        # File exists but is empty (e.g. someone manually truncated it).
        # Still safe to use only new data — there is nothing to preserve.
        for col in seed_columns:
            if col not in new_df.columns:
                new_df[col] = ""
        merged = new_df[seed_columns]
        if path == HISTORY_PATH:
            merged = _dedup_history_rows(merged, seed_columns)
        return merged.to_csv(index=False)

    # Parse existing CSV. Refuse to proceed on parse failure rather than
    # silently overwriting — a corrupted file in the repo is something a
    # human needs to look at, not something a sync should paper over by
    # destroying every other tenant's data.
    try:
        existing_df = pd.read_csv(
            StringIO(existing_content), dtype=str, keep_default_na=False,
        )
    except Exception as exc:
        raise SeedFetchError(
            f"Existing seed at {path} failed to parse: {exc}. "
            "Refusing to overwrite to protect other tenants' data."
        ) from exc

    if existing_df.empty:
        for col in seed_columns:
            if col not in new_df.columns:
                new_df[col] = ""
        merged = new_df[seed_columns]
        if path == HISTORY_PATH:
            merged = _dedup_history_rows(merged, seed_columns)
        return merged.to_csv(index=False)

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

    # Tenancy scope: only the syncing tenant's own rows (and legacy
    # unowned rows from before tenant_id was tracked under this account
    # label) are eligible to be rewritten by this merge. Rows owned by
    # OTHER tenants stay in ``other_df`` and are never touched.
    if tenant_id is not None and "tenant_id" in existing_df.columns:
        target_tid = _normalize_tid(tenant_id)
        existing_tid_norm = existing_df["tenant_id"].map(_normalize_tid)
        legacy_or_self = existing_tid_norm.isin(["", target_tid])
        account_mask = acct_match & legacy_or_self
    else:
        account_mask = acct_match

    other_df = existing_df.loc[~account_mask]
    existing_account_df = existing_df.loc[account_mask]

    if path == HISTORY_PATH:
        # History: append and de-duplicate within this account.
        # New rows take precedence when keys collide.
        #
        # Tag rows by source so the dedup keeps the NEW row on key
        # collisions. Use integer sentinels (0=old, 1=new) — the
        # previous "old"/"new" string sentinels sorted alphabetically
        # ("new" < "old") and silently put NEW rows BEFORE OLD ones,
        # which made keep="last" retain the legacy row instead of
        # the freshly-tagged sync row. Multi-account users with
        # pre-tenancy seed data ended up losing every re-synced
        # trade to that misordering. Regression-tested in
        # tests/test_upload_merge.py.
        #
        # IMPORTANT: dedup runs on EVERY history merge — including
        # when ``existing_account_df`` is empty (first sync for this
        # tenant after account linking). Schwab's transactions API has
        # been observed returning the SAME fill twice in one response
        # with float-drift formatting (``100`` vs ``100.0`` /
        # ``-7660`` vs ``-7660.0``); the previous "if empty: just
        # copy new_df" shortcut let both forms survive into the seed.
        # Bug shipped May 2026 (commit cafc0713: Sara Investment ASTS
        # x2). Regression test:
        # tests/test_upload_merge.py::test_dedup_collapses_drift_within_new_df_even_when_existing_empty
        existing_account_df = existing_account_df.copy()
        new_tagged = new_df.copy()
        existing_account_df["__src"] = 0
        new_tagged["__src"] = 1
        combined = pd.concat([existing_account_df, new_tagged], ignore_index=True)

        # Normalize key columns so duplicates match: NaN != NaN in pandas,
        # so fill nulls with a sentinel before dedupe. ``account`` and
        # ``user_id`` are informational metadata; ``tenant_id`` is part
        # of the trade identity under v2. The combined frame is already
        # scoped to the syncing tenant + legacy rows, so other tenants'
        # rows can't reach this dedup at all.
        key_cols = [
            c for c in seed_columns
            if str(c).lower() not in ("account", "user_id")
        ]
        canon = combined[key_cols].copy()
        for c in key_cols:
            if c in combined.columns:
                if str(c).lower() == "tenant_id" and tenant_id is not None:
                    target_tid = _normalize_tid(tenant_id)
                    canon[c] = combined[c].map(
                        lambda v, _t=target_tid: _t
                        if _normalize_tid(v) == "" else _normalize_tid(v)
                    )
                else:
                    canon[c] = combined[c].map(
                        lambda v, _c=c: _canonicalize_key_cell(_c, v)
                    )

        combined = combined.sort_values("__src", kind="stable")  # 0 first, 1 last
        keep_mask = ~canon.duplicated(subset=key_cols, keep="last")
        combined = combined.loc[keep_mask].reset_index(drop=True)
        merged_account = combined.drop(columns=["__src"])

        # CROSS-SOURCE pass over the combined (existing + new) tenant frame.
        # The strict-key dedup above keys on EVERY column incl Description
        # and fees, so the SAME economic fill reported by two SnapTrade
        # sources — the real-time ``recent_orders`` feed and the slower
        # ``activities`` feed — survives as two rows whenever they disagree
        # on description text (orders emits the symbol name "FB Financial
        # Corp"; activities emits the broker wording "FB FINL CORP" — and
        # for options, orders "…BUY FILL at 0.96" vs "…BUY PARTIAL_FILL…").
        # Those land in DIFFERENT sync cycles, so the strict pass never
        # collapses them and the warehouse test
        # ``stg_history_no_duplicate_fills_per_tenant`` (grain excludes
        # description/fees) trips. ``_dedup_history_rows`` re-keys on the
        # source-agnostic identity (Date, Action, Symbol, Quantity, Price),
        # keeping the richer description. Previously this pass only ran in
        # the empty/first-sync branches, so cross-cycle cross-source dupes
        # slipped through here. Regression: tests/test_upload_merge.py
        # ::test_cross_source_dupe_collapses_across_sync_cycles.
        merged_account = _dedup_history_rows(merged_account, seed_columns)
    else:
        # Current positions (snapshot): replace that account entirely
        merged_account = new_df

    merged = pd.concat([other_df, merged_account], ignore_index=True)
    return merged[seed_columns].to_csv(index=False)


def _seed_contents_unchanged(path_contents):
    """True iff every ``(path, content)`` already equals the seed currently
    in the store.

    Used to skip no-op writes: one changed write = one workflow dispatch =
    one dbt build, and rebuilding the entire warehouse for zero data change
    is wasted CI + BigQuery cost ("I don't need to run dbt if no new data is
    going in"). A missing table (``None``) counts as a change so first-ever
    creation still writes. Byte-exact comparison — if a future pandas version
    reformats output, this errs toward writing (safe), never toward
    silently dropping a real change.
    """
    for path, content in path_contents:
        current = _get_file_content(path)
        if current is None or current != content:
            return False
    return True


_WORKFLOW_FILE = "bigquery_update.yml"

# Build markers replace commit SHAs in the post-sync "processing" UI: a
# changed seed write returns ``dispatch:<unix_ts>`` and the workflow-status
# poller resolves it to the live or successful bigquery_update.yml run
# created at/after that timestamp. (workflow_dispatch runs aren't tied to
# a commit the app knows about, so SHA-based lookup no longer works.)
_DISPATCH_MARKER_PREFIX = "dispatch:"


def _dispatch_warehouse_rebuild(reason):
    """POST a ``workflow_dispatch`` for the warehouse rebuild workflow.

    Returns a ``dispatch:<unix_ts>`` marker string on success, ``None`` on
    failure. Log-don't-crash: the seed write already landed, so a dispatch
    failure must never fail the sync — the next changed sync (or a manual
    workflow run) rebuilds. Skipped entirely outside the production store
    (local dev builds via scripts/dev-refresh.sh, never via CI).
    """
    import time as _time

    if not is_production_store():
        app.logger.info(
            "Seed write landed in non-production raw dataset; skipping "
            "CI rebuild dispatch (%s).", reason,
        )
        return None
    if not os.environ.get("GITHUB_PAT", "").strip():
        app.logger.warning(
            "Seed write landed but GITHUB_PAT is not set — cannot dispatch "
            "the warehouse rebuild (%s). Trigger %s manually.",
            reason, _WORKFLOW_FILE,
        )
        return None
    repo = _github_repo()
    url = (
        f"https://api.github.com/repos/{repo}/actions/workflows/"
        f"{_WORKFLOW_FILE}/dispatches"
    )
    dispatched_at = int(_time.time())
    try:
        resp = requests.post(
            url,
            headers=_github_headers(),
            json={"ref": _github_branch()},
            timeout=20,
        )
    except requests.RequestException as exc:
        app.logger.error(
            "Warehouse rebuild dispatch failed (%s): %s", reason, exc
        )
        return None
    if resp.status_code != 204:
        app.logger.error(
            "Warehouse rebuild dispatch failed (%s): HTTP %s %s",
            reason, resp.status_code, resp.text[:200],
        )
        return None
    app.logger.info("Dispatched warehouse rebuild (%s).", reason)
    return f"{_DISPATCH_MARKER_PREFIX}{dispatched_at}"


def _commit_git_paths(path_contents, message):
    """
    Atomically replace the raw seed tables in the BigQuery store, then
    dispatch a warehouse rebuild.
    path_contents: list of (seed_path, CSV text).
    Returns (success, error_message, build_marker or None, no_changes).
    ``no_changes=True`` means every seed already matched the store, so NO
    write happened (and therefore no dbt build will run). ``build_marker``
    is a ``dispatch:<unix_ts>`` string the processing UI can poll (the
    slot that used to carry the GitHub commit SHA).

    Name kept from the GitHub-storage era — it is the write half of the
    storage seam that the merge tests stub.
    """
    if not path_contents:
        return True, None, None, False

    # No-op guard — skip the write (and the dbt build it would trigger) when
    # nothing actually changed. Never let this optimization block a real
    # write: any error in the check falls through to the normal write path.
    try:
        if _seed_contents_unchanged(path_contents):
            return True, None, None, True
    except Exception as exc:
        app.logger.warning(
            "seed no-op check failed (%s) — falling through to a normal "
            "write, which may trigger an avoidable rebuild.", exc,
        )

    try:
        _seed_store_write(path_contents)
    except SeedStoreError as exc:
        return False, str(exc), None, False

    marker = _dispatch_warehouse_rebuild(message)
    return True, None, marker, False


def _csv_upload_account_choices(tenants):
    """Build the CSV upload picker: ``{tenant_id, label}`` in display order.

    Labels match Positions (nickname, disambiguated when several physical
    accounts share a broker label). Values are tenant_ids so picking an
    existing row can never mint a second ``manual:`` tenant from a nickname.
    """
    from app.routes import _disambiguated_tenant_labels

    labels = _disambiguated_tenant_labels(tenants)
    choices = []
    for row in tenants or []:
        tid = (row.get("tenant_id") or "").strip()
        if not tid:
            continue
        label = (labels.get(tid) or row.get("account_name") or tid).strip()
        choices.append({"tenant_id": tid, "label": label})
    choices.sort(key=lambda c: (c["label"] or "").lower())
    return choices


def _norm_upload_label(val) -> str:
    return " ".join(str(val or "").strip().split()).lower()


def _tenants_matching_upload_label(tenants, label):
    """Owned tenants whose name, nickname, or picker label equals ``label``."""
    from app.routes import _disambiguated_tenant_labels, _tenant_display_label

    want = _norm_upload_label(label)
    if not want:
        return []
    labels = _disambiguated_tenant_labels(tenants)
    hits = {}
    for row in tenants or []:
        tid = (row.get("tenant_id") or "").strip()
        if not tid:
            continue
        candidates = (
            row.get("account_name"),
            row.get("display_nickname"),
            labels.get(tid),
            _tenant_display_label(row),
        )
        if any(_norm_upload_label(c) == want for c in candidates if c):
            hits[tid] = row
    return list(hits.values())


def _unique_tenant_for_upload_label(tenants, label):
    """Return the one owned tenant whose name, nickname, or picker label matches.

    Ambiguous matches (two tenants nicknamed similarly, or several Schwab
    accounts that all ship as ``Schwab Account``) return None — the caller
    must pick by tenant_id rather than invent a new account.
    """
    hits = _tenants_matching_upload_label(tenants, label)
    if len(hits) == 1:
        return hits[0]
    return None


def _create_manual_csv_tenant(user_id, account_name):
    # Manual account labels are user-chosen and routinely collide ("IRA",
    # "Schwab Account"). Include the owner in the deterministic broker UUID;
    # the old ``manual:<label>`` value produced one globally shared tenant_id,
    # so a second user's same-named upload could be written into the first
    # user's warehouse partition.
    manual_uuid = f"manual:{int(user_id)}:{account_name}"
    return get_or_create_broker_tenant(
        user_id=int(user_id),
        broker_slug=MANUAL_BROKER_SLUG,
        broker_uuid=manual_uuid,
        account_name=account_name,
        broker_label="CSV Upload",
    )


def _resolve_csv_upload_target(
    user_id, account_select, account_custom, *,
    tenants=None, create_manual=None,
):
    """Map the upload form to ``(account_name, tenant_id, error)``.

    Existing picker values are ``tenant_id``. ``__new__`` mints a manual
    tenant only when the typed name does not uniquely match an owned
    tenant's account_name / nickname / picker label. A bare legacy label
    (pre-tenant picker) is resolved the same way and otherwise rejected —
    never used as a new manual tenant key, which is how
    picking "Emmory Investment" created a second account next to the
    SnapTrade tenant nicknamed Emmory.
    """
    select = (account_select or "").strip()
    custom = (account_custom or "").strip()
    if tenants is None:
        tenants = get_broker_tenants_for_user(user_id) or []
    if create_manual is None:
        create_manual = _create_manual_csv_tenant

    by_tid = {
        (row.get("tenant_id") or "").strip(): row
        for row in tenants
        if (row.get("tenant_id") or "").strip()
    }

    def _use(row):
        name = (row.get("account_name") or "").strip()
        tid = (row.get("tenant_id") or "").strip()
        if not (name and tid):
            return None, None, "That account is missing a tenant id. Pick another, or create a new one."
        return name, tid, None

    if not select:
        return None, None, "Please select or enter an account name."

    if select == "__new__":
        if not custom:
            return None, None, "Please enter a new account name."
        matches = _tenants_matching_upload_label(tenants, custom)
        if len(matches) == 1:
            return _use(matches[0])
        if len(matches) > 1:
            # Jeff's IRA/General/Coco/… are all account_name "Schwab Account".
            # Unique-match returned None, and the old mint path created
            # a manual Schwab Account beside them.
            return None, None, (
                "That name matches more than one linked account. "
                "Pick the specific account from the list instead of creating a new one."
            )
        tenant_id = create_manual(user_id, custom)
        return custom, tenant_id, None

    if select in by_tid:
        return _use(by_tid[select])

    match = _unique_tenant_for_upload_label(tenants, select)
    if match:
        return _use(match)

    return None, None, (
        "That account is not in your linked accounts. "
        "Pick one from the list, or use “Create new account”."
    )


def _restamp_seed_identity(df, account_name, user_id, tenant_id, account_col="Account"):
    """Force tenant columns after any CSV rename/slice.

    Brokerage exports often carry an Account / nickname column (Schwab
    Positions: "Emmory"). Drop+insert is not enough if a later rename
    produces a duplicate Account column — the CSV nickname would win and
    mint a second warehouse account. Collapse duplicates then overwrite.
    """
    if df is None:
        return df
    out = df.copy()
    if out.columns.duplicated().any():
        out = out.loc[:, ~out.columns.duplicated()].copy()
    if account_col in out.columns:
        out[account_col] = account_name
    if "user_id" in out.columns:
        out["user_id"] = "" if user_id is None else int(user_id)
    if "tenant_id" in out.columns:
        out["tenant_id"] = "" if tenant_id is None else str(tenant_id).strip()
    return out


def _upload_github_config_ok():
    """Return (ok, error_message) — whether seed writes are enabled.

    Seed data now lands directly in the BigQuery seed store
    (``app/seed_store.py``), so no GitHub credentials are required to
    WRITE. ``GITHUB_PAT`` is still used opportunistically to dispatch the
    CI rebuild after a changed write (log-don't-crash if absent).
    ``SEED_WRITES_DISABLED=1`` is an explicit operator kill switch.

    Name kept for import compatibility across the sync callers/tests.
    """
    if os.environ.get("SEED_WRITES_DISABLED", "").strip() == "1":
        return False, "Seed writes are disabled in this environment (SEED_WRITES_DISABLED=1)."
    return True, None


def _prepare_seed_df(
    df, account_name, columns, account_col="Account",
    user_id=None, tenant_id=None,
):
    """Align a DataFrame to the seed's column set and set the tenant columns.

    Tenant columns are forcibly set on every row so a writer can never
    accidentally ship rows under the wrong tenant:

    1. ``Account`` / ``account`` — display label (caller-provided).
    2. ``user_id``               — Postgres ``users.id`` (informational).
    3. ``tenant_id``             — v2 warehouse tenant key
       (``<broker_slug>:<broker_uuid>``; required at the writer boundary).

    See ``docs/V2_TENANT_KEY_DESIGN.md``.
    """
    if df is None:
        return pd.DataFrame(columns=columns)
    out = df.copy()
    for sentinel in ("account", "user_id", "tenant_id"):
        for c in [col for col in out.columns if str(col).lower() == sentinel]:
            out.drop(columns=[c], inplace=True)
    out.insert(0, account_col, account_name)
    out.insert(1, "user_id", "" if user_id is None else int(user_id))
    out.insert(
        2,
        "tenant_id",
        "" if tenant_id is None else str(tenant_id).strip(),
    )
    for col in columns:
        if col not in out.columns:
            out[col] = ""
    return _restamp_seed_identity(
        out[columns], account_name, user_id, tenant_id, account_col=account_col,
    )


def _normalize_account_seed_frames(
    account_name, history_df, current_df, *,
    user_id_int, tenant_id_str, skip_history, balances_df,
):
    """Shape one account's frames into ``(path, prepared_df, seed_columns)``
    tuples ready for ``_merge_seed_with_existing`` — the normalization half of
    ``merge_and_push_seeds`` factored out so both the single-account push and
    the batched multi-account commit run byte-identical logic.

    Returns ``(specs, history_rows, current_rows)`` where ``specs`` is an
    ordered list (history first when present, then current, then balances).
    """
    if history_df is not None:
        history_df = history_df.copy()
    # current_df is None for a history-only push (the intraday trade poll):
    # only trade_history is rewritten, never the positions/balances snapshot.
    if current_df is not None:
        current_df = current_df.copy()

    for df in [history_df, current_df]:
        if df is None:
            continue
        for sentinel in ("account", "user_id", "tenant_id"):
            stale = [c for c in df.columns if c.lower() == sentinel]
            if stale:
                df.drop(columns=stale, inplace=True)
        df.insert(0, "Account", account_name)
        df.insert(1, "user_id", user_id_int)
        df.insert(2, "tenant_id", tenant_id_str)

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
        history_df = _restamp_seed_identity(
            history_df[HISTORY_SEED_COLUMNS],
            account_name, user_id_int, tenant_id_str,
        )

    if current_df is not None:
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
        current_df = _restamp_seed_identity(
            current_df[CURRENT_SEED_COLUMNS],
            account_name, user_id_int, tenant_id_str,
        )

    specs = []
    if not skip_history and history_df is not None:
        specs.append((HISTORY_PATH, history_df, HISTORY_SEED_COLUMNS))
    if current_df is not None:
        specs.append((CURRENT_PATH, current_df, CURRENT_SEED_COLUMNS))
    if balances_df is not None and len(balances_df) > 0:
        balances_prepared = _prepare_seed_df(
            balances_df, account_name, BALANCE_SEED_COLUMNS,
            account_col="account", user_id=user_id_int, tenant_id=tenant_id_str,
        )
        specs.append((BALANCE_SEED_PATH, balances_prepared, BALANCE_SEED_COLUMNS))

    history_rows = len(history_df) if history_df is not None else 0
    current_rows = len(current_df) if current_df is not None else 0
    return specs, history_rows, current_rows


@_serialized_seed_write
def merge_and_push_seeds(
    account_name,
    history_df,
    current_df,
    *,
    commit_message,
    user_id,
    tenant_id,
    skip_history=False,
    balances_df=None,
):
    """
    Normalize DataFrames, merge into the BigQuery seed store, and write.
    Both manual uploads and SnapTrade sync call this so trade_history +
    current_positions stay the single pair of raw tables that feed dbt.

    Args:
        history_df: trade rows shaped for HISTORY_SEED_COLUMNS (or None).
        current_df: open-position rows shaped for CURRENT_SEED_COLUMNS. Pass
            None for a HISTORY-ONLY push (intraday poll / weekend auto-sync) —
            only trade_history is written, the snapshots are left untouched.
        user_id: required Postgres ``users.id`` of the row owner. Stamped
            into every emitted row's ``user_id`` column (informational).
        tenant_id: required v2 warehouse tenant key
            (``<broker_slug>:<broker_uuid>``). Stamped into every emitted
            row's ``tenant_id`` column. Every writer (SnapTrade, manual
            upload) derives this via ``get_or_create_broker_tenant`` or
            accepts it from the caller. See ``docs/V2_TENANT_KEY_DESIGN.md``.
        balances_df: optional cash + account_total rows shaped for
            BALANCE_SEED_COLUMNS. Written in the same batch as the others.
            Any broker connector writes here.
        skip_history: when True, write positions only (and balances if given).

    Returns:
        (ok, err_message, history_rows, current_rows, build_marker or None,
         no_changes). ``build_marker`` is a ``dispatch:<ts>`` string for the
        processing UI (the slot that used to carry the commit SHA).
        ``no_changes=True`` means the merged seed was identical to what's
        already in the store, so NO write/dispatch/dbt-build happened.

    Caller must verify _upload_github_config_ok() first.
    """
    # current_df=None is a HISTORY-ONLY push (intraday poll / weekend auto-sync):
    # only trade_history is rewritten. It's valid as long as there's history to
    # push — otherwise there's genuinely nothing to commit.
    if current_df is None and (skip_history or history_df is None):
        return False, "nothing to push (no positions and no history).", 0, 0, None, False
    if user_id is None:
        return False, "user_id is required.", 0, 0, None, False
    if tenant_id is None or not str(tenant_id).strip():
        return False, "tenant_id is required.", 0, 0, None, False

    user_id_int = int(user_id)
    tenant_id_str = str(tenant_id).strip()

    specs, history_rows, current_rows = _normalize_account_seed_frames(
        account_name, history_df, current_df,
        user_id_int=user_id_int, tenant_id_str=tenant_id_str,
        skip_history=skip_history, balances_df=balances_df,
    )

    path_contents = []
    for path, prepared_df, seed_columns in specs:
        content = _merge_seed_with_existing(
            path, account_name, prepared_df, seed_columns, tenant_id=tenant_id_str,
        )
        path_contents.append((path, content))

    try:
        ok, err, head_sha, no_changes = _commit_git_paths(path_contents, commit_message)
        if not ok:
            return False, err or "Seed store write failed.", history_rows, current_rows, None, False
    except Exception as exc:
        return False, str(exc), history_rows, current_rows, None, False

    add_account_for_user(user_id_int, account_name)
    record_upload(user_id_int, account_name, history_rows, current_rows, tenant_id=tenant_id_str)

    return True, None, history_rows, current_rows, head_sha, no_changes


@_serialized_seed_write
def merge_and_push_seeds_batch(entries, *, commit_message):
    """Merge SEVERAL accounts' frames into the seed store and write them in a
    SINGLE batch (one changed write = one dispatch = one dbt build).

    The nightly backstop cron syncs every account; doing a per-account
    ``merge_and_push_seeds`` fanned out into one write — and therefore
    one full ``Update Daily Position Performance`` workflow run — PER ACCOUNT
    (~14 near-simultaneous runs a night, most immediately cancelled by
    ``concurrency: cancel-in-progress``). This folds every account onto the
    prior account's merged CSV in-memory (via ``_merge_seed_with_existing``'s
    ``existing_content`` hand-off) and writes once, so the same monotonic
    merge semantics collapse to a single build.

    ``entries`` — list of dicts, each:
        ``account_name`` (str), ``history_df`` (DataFrame|None),
        ``current_df`` (DataFrame|None), ``user_id`` (int), ``tenant_id`` (str),
        ``skip_history`` (bool), ``balances_df`` (DataFrame|None).
    ``current_df=None`` is a HISTORY-ONLY push (the intraday trade poll): only
    trade_history is rewritten — the positions/balances snapshots are left
    untouched so an intraday cadence doesn't rebuild the warehouse on snapshot
    drift. Such an entry is kept only when it carries new trade fills.
    Order matters and must match the per-account push order it replaces:
    each entry folds onto the previous, exactly as sequential pushes did.

    Returns ``(ok, err_message, build_marker or None, no_changes,
    pushed_entry_count)``. Caller must verify ``_upload_github_config_ok()``
    first (same contract as ``merge_and_push_seeds``).
    """
    valid = []
    for e in entries or []:
        has_current = e.get("current_df") is not None
        # Intraday poll entries are history-only (current_df=None): keep them
        # as long as there are new trade fills to push.
        has_history = e.get("history_df") is not None and not e.get("skip_history")
        if not has_current and not has_history:
            continue
        if e.get("user_id") is None:
            continue
        if e.get("tenant_id") is None or not str(e.get("tenant_id")).strip():
            continue
        valid.append(e)

    if not valid:
        return True, None, None, True, 0

    # Normalize every entry up front → {path: [(account_name, tenant_id, df, cols), ...]}
    # preserving entry order, so each path folds accounts in the same sequence
    # the per-account pushes used.
    from collections import OrderedDict
    per_path = OrderedDict()  # path -> list of (account_name, tenant_id_str, df, cols)
    prepared_counts = []      # (entry, history_rows, current_rows)
    for e in valid:
        user_id_int = int(e["user_id"])
        tenant_id_str = str(e["tenant_id"]).strip()
        specs, hr, cr = _normalize_account_seed_frames(
            e["account_name"], e.get("history_df"), e["current_df"],
            user_id_int=user_id_int, tenant_id_str=tenant_id_str,
            skip_history=bool(e.get("skip_history")), balances_df=e.get("balances_df"),
        )
        prepared_counts.append((e, hr, cr))
        for path, prepared_df, seed_columns in specs:
            per_path.setdefault(path, []).append(
                (e["account_name"], tenant_id_str, prepared_df, seed_columns)
            )

    # Fold each path once, in the canonical seed order (history, current,
    # balances), fetching the stored seed a single time and threading the
    # running merged CSV through each account.
    path_contents = []
    for path in (HISTORY_PATH, CURRENT_PATH, BALANCE_SEED_PATH):
        contributions = per_path.get(path)
        if not contributions:
            continue
        content = _get_file_content(path)  # single fetch per file for the whole batch
        for account_name, tenant_id_str, prepared_df, seed_columns in contributions:
            content = _merge_seed_with_existing(
                path, account_name, prepared_df, seed_columns,
                tenant_id=tenant_id_str, existing_content=content,
            )
        path_contents.append((path, content))

    try:
        ok, err, head_sha, no_changes = _commit_git_paths(path_contents, commit_message)
        if not ok:
            return False, err or "Seed store write failed.", None, False, 0
    except Exception as exc:
        return False, str(exc), None, False, 0

    # Per-account bookkeeping (idempotent), matching the single-push path.
    for e, hr, cr in prepared_counts:
        add_account_for_user(int(e["user_id"]), e["account_name"])
        record_upload(
            int(e["user_id"]), e["account_name"], hr, cr,
            tenant_id=e.get("tenant_id"),
        )

    return True, None, head_sha, no_changes, len(valid)


@_serialized_seed_write
def purge_user_id_from_seeds(user_id, *, commit_message):
    """Strip every raw-seed row owned by ``user_id`` and write the
    cleaned tables back to the seed store (a tenant-scoped delete
    implemented as read → filter → atomic ``WRITE_TRUNCATE`` rewrite, so
    it shares the exact storage path and locking every other writer uses).

    Why this exists: the marts are rebuilt from the raw seed tables on
    every dbt build (``.github/workflows/bigquery_update.yml``). Issuing a
    BQ ``DELETE FROM analytics.stg_history WHERE user_id = N`` is reversed
    the next time ``dbt build`` runs because the RAW tables still hold
    those rows. Permanently purging a user from the warehouse therefore
    requires rewriting the raw seed tables themselves (the rebuild
    dispatched after the write then flushes the derived marts).

    v2 ownership is ``broker_tenants.tenant_id``, not the informational
    ``user_id`` carried on raw rows. Resolve every active and inactive
    tenant before reading the seed store, then remove rows matching either
    an owned tenant_id or the legacy user_id fallback. The tenant match is
    load-bearing for pre-link rows whose user_id is NULL/stale: leaving
    those behind after self-serve deletion retains the user's financial
    history indefinitely even though the UI says their data was deleted.

    Returns ``(ok, error_message, rows_removed_dict, build_marker or None)``
    where ``rows_removed_dict`` maps each seed path to how many rows were
    dropped. ``ok=True`` with empty ``rows_removed`` and marker ``None``
    means no matching rows existed (no write happened).
    """
    ok, err = _upload_github_config_ok()
    if not ok:
        return False, err, {}, None

    try:
        target = str(int(user_id))
    except (TypeError, ValueError):
        return False, f"Invalid user_id: {user_id!r}", {}, None

    try:
        # Include disabled/disconnected tenants: deleting an account must
        # remove its retained warehouse history regardless of connection
        # health. Runtime import avoids widening upload.py's model imports.
        from app.models import get_broker_tenants_for_user

        tenant_ids = {
            str(row.get("tenant_id") or "").strip()
            for row in (
                get_broker_tenants_for_user(int(user_id), include_inactive=True)
                or []
            )
            if str(row.get("tenant_id") or "").strip()
        }
    except Exception as exc:
        # Fail closed. Falling back silently to user_id-only would recreate
        # the privacy bug for NULL/stale user_id rows.
        return False, f"Could not resolve tenant ownership: {exc}", {}, None

    seed_specs = [
        (HISTORY_PATH, HISTORY_SEED_COLUMNS),
        (CURRENT_PATH, CURRENT_SEED_COLUMNS),
        (BALANCE_SEED_PATH, BALANCE_SEED_COLUMNS),
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

        tenant_col = None
        for c in df.columns:
            if str(c).strip().lower() == "tenant_id":
                tenant_col = c
                break

        if uid_col is None and tenant_col is None:
            # Older seed shapes pre-date both ownership columns. Nothing
            # can be proved from the free-form Account label.
            rows_removed[path] = 0
            continue

        before = len(df)
        remove_mask = pd.Series(False, index=df.index)
        if tenant_col is not None:
            row_tenants = df[tenant_col].astype(str).str.strip()
            if tenant_ids:
                remove_mask |= row_tenants.isin(tenant_ids)
            # user_id is only a fallback for rows that genuinely predate
            # tenant_id. A non-empty tenant_id is canonical even if its
            # informational user_id is stale, so never delete another
            # tenant on the strength of user_id alone.
            if uid_col is not None:
                remove_mask |= (
                    row_tenants.eq("")
                    & df[uid_col].map(_normalize_uid).eq(target)
                )
        elif uid_col is not None:
            remove_mask |= df[uid_col].map(_normalize_uid).eq(target)
        keep_mask = ~remove_mask
        cleaned = df.loc[keep_mask].copy()
        removed = before - len(cleaned)
        if removed == 0:
            rows_removed[path] = 0
            continue

        # Re-align to the canonical seed column shape so the write
        # doesn't drift column order or drop unrelated columns the
        # stored seed happens to be missing.
        for col in columns:
            if col not in cleaned.columns:
                cleaned[col] = ""
        cleaned = cleaned[columns]
        path_contents.append((path, cleaned.to_csv(index=False)))
        rows_removed[path] = removed

    if not path_contents:
        return True, None, rows_removed, None

    try:
        ok, err, head_sha, _no_changes = _commit_git_paths(path_contents, commit_message)
    except Exception as exc:
        return False, str(exc), rows_removed, None
    if not ok:
        return False, err or "Seed store write failed.", rows_removed, None
    return True, None, rows_removed, head_sha


@_serialized_seed_write
def purge_tenant_ids_from_seeds(tenant_ids, *, commit_message):
    """Strip raw-seed rows for specific ``tenant_id``s (not a whole user).

    Same read → filter → WRITE_TRUNCATE path as ``purge_user_id_from_seeds``,
    but the predicate is only ``tenant_id IN (...)``. Does not fall back to
    informational ``user_id`` — that would yank unrelated tenants that
    happen to share a numeric id. Use this to undo a stray CSV upload
    tenant without touching the SnapTrade account it was meant to merge
    into.
    """
    ok, err = _upload_github_config_ok()
    if not ok:
        return False, err, {}, None

    wanted = {
        str(tid).strip()
        for tid in (tenant_ids or [])
        if str(tid or "").strip()
    }
    if not wanted:
        return False, "tenant_id is required.", {}, None

    seed_specs = [
        (HISTORY_PATH, HISTORY_SEED_COLUMNS),
        (CURRENT_PATH, CURRENT_SEED_COLUMNS),
        (BALANCE_SEED_PATH, BALANCE_SEED_COLUMNS),
    ]

    path_contents = []
    rows_removed = {}

    for path, columns in seed_specs:
        existing_content = _get_file_content(path)
        if not existing_content or not existing_content.strip():
            rows_removed[path] = 0
            continue
        try:
            df = pd.read_csv(StringIO(existing_content), dtype=str, keep_default_na=False)
        except Exception as exc:
            return False, f"Could not parse {path}: {exc}", rows_removed, None
        if df.empty:
            rows_removed[path] = 0
            continue

        tenant_col = None
        for c in df.columns:
            if str(c).strip().lower() == "tenant_id":
                tenant_col = c
                break
        if tenant_col is None:
            rows_removed[path] = 0
            continue

        before = len(df)
        keep_mask = ~df[tenant_col].astype(str).str.strip().isin(wanted)
        cleaned = df.loc[keep_mask].copy()
        removed = before - len(cleaned)
        if removed == 0:
            rows_removed[path] = 0
            continue

        for col in columns:
            if col not in cleaned.columns:
                cleaned[col] = ""
        cleaned = cleaned[columns]
        path_contents.append((path, cleaned.to_csv(index=False)))
        rows_removed[path] = removed

    if not path_contents:
        return True, None, rows_removed, None

    try:
        ok, err, head_sha, _no_changes = _commit_git_paths(path_contents, commit_message)
    except Exception as exc:
        return False, str(exc), rows_removed, None
    if not ok:
        return False, err or "Seed store write failed.", rows_removed, None
    return True, None, rows_removed, head_sha


@app.route("/internal/purge-tenant", methods=["POST"])
@csrf.exempt
@limiter.limit("10 per hour")
def internal_purge_tenant():
    """Operator purge of one manual CSV tenant (seeds + broker_tenants).

    Auth: same ``X-Cache-Flush-Token`` / ``CACHE_FLUSH_TOKEN`` as cache
    flush. Only ``manual:`` tenant_ids are accepted so a token leak cannot
    drop a SnapTrade connection.
    """
    expected = (os.environ.get("CACHE_FLUSH_TOKEN") or "").strip()
    provided = (request.headers.get("X-Cache-Flush-Token") or "").strip()
    if not expected or not hmac.compare_digest(provided, expected):
        abort(403)

    data = request.get_json(silent=True) or {}
    tenant_id = (data.get("tenant_id") or request.form.get("tenant_id") or "").strip()
    if not tenant_id.startswith("manual:"):
        return jsonify({"ok": False, "error": "only manual CSV tenants can be purged this way"}), 400

    pg_row = delete_broker_tenant(tenant_id)
    ok, err, removed, marker = purge_tenant_ids_from_seeds(
        [tenant_id],
        commit_message=f"ops: purge stray CSV tenant {tenant_id}",
    )
    if not ok:
        return jsonify({
            "ok": False,
            "error": err,
            "postgres_deleted": bool(pg_row),
            "rows_removed": removed,
        }), 500
    return jsonify({
        "ok": True,
        "postgres_deleted": bool(pg_row),
        "account_name": (pg_row or {}).get("account_name"),
        "rows_removed": removed,
        "build_marker": marker,
    })


@app.route("/upload", methods=["GET", "POST"])
@login_required
@limiter.limit("30 per minute", exempt_when=lambda: request.method != "POST")
def upload():
    seed_writes_enabled, _cfg_err = _upload_github_config_ok()

    if request.method == "POST":
        # Demo is read-only. Without this, any visitor could replace the seed
        # CSVs that every other demo viewer is reading from.
        blocked = demo_block_writes("uploading new trade data")
        if blocked:
            return blocked
        # Frozen trial: uploads are data writes (each changed write dispatches
        # a warehouse rebuild), so they pause with the rest of the mirror.
        from app.plan import plan_block_writes
        blocked = plan_block_writes("uploading new trade data")
        if blocked:
            return blocked
        from app.auth import email_block_writes
        blocked = email_block_writes("uploading new trade data")
        if blocked:
            return blocked

    if request.method == "GET":
        user_accounts = get_accounts_for_user(current_user.id)
        # Picker is tenant-addressed (same nicknames as Positions). Do not
        # list unscoped BigQuery account labels — picking a nickname/label
        # used to mint a new manual tenant instead of attaching to the
        # SnapTrade account the user thought they selected.
        account_choices = _csv_upload_account_choices(
            get_broker_tenants_for_user(current_user.id) or [],
        )
        accounts = sorted(set(user_accounts))
        recent_uploads = get_uploads_for_user(current_user.id)
        return render_template(
            "upload.html", title="Upload Data",
            accounts=accounts,
            account_choices=account_choices,
            recent_uploads=recent_uploads,
            github_upload_enabled=seed_writes_enabled,
            csv_export_brokers=CSV_EXPORT_BROKERS,
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
    skip_current = request.form.get("skip_current_positions") == "1"

    def _csv_uploaded(file_storage):
        return bool(file_storage and (file_storage.filename or "").strip())

    current_file = request.files.get("current_csv")
    current_df = None
    if not skip_current and _csv_uploaded(current_file):
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
        if _csv_uploaded(history_file):
            history_df, history_err = _validate_csv(
                history_file, HISTORY_REQUIRED_COLS, "History",
                col_renames=HISTORY_COL_RENAMES,
                header_markers={"date", "action", "symbol", "quantity"},
            )
            if history_err:
                flash(history_err, "danger")
                return redirect(url_for("upload"))

    if history_df is None and current_df is None:
        flash("Upload a trade-history CSV, a current-positions CSV, or both.", "danger")
        return redirect(url_for("upload"))

    # ------------------------------------------------------------------
    # Account name is mandatory (selected or typed on the form)
    # ------------------------------------------------------------------
    account_select = request.form.get("account_name", "").strip()
    account_custom = request.form.get("account_name_custom", "").strip()
    account_name, tenant_id, acct_err = _resolve_csv_upload_target(
        current_user.id, account_select, account_custom,
    )
    if acct_err:
        flash(acct_err, "danger")
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
    n_hist = 0 if history_df is None else len(history_df)
    n_cur = 0 if current_df is None else len(current_df)
    if history_df is None:
        commit_msg = (
            f"Upload by {current_user.username}: "
            f"positions only, {n_cur} current rows ({account_name})"
        )
    elif current_df is None:
        commit_msg = (
            f"Upload by {current_user.username}: "
            f"history only, {n_hist} history rows ({account_name})"
        )
    else:
        commit_msg = (
            f"Upload by {current_user.username}: "
            f"{n_hist} history rows, {n_cur} current rows "
            f"({account_name})"
        )

    is_first_upload = False
    try:
        is_first_upload = count_uploads_for_user(current_user.id) == 0
    except Exception:
        is_first_upload = False

    # tenant_id comes from _resolve_csv_upload_target: an existing
    # SnapTrade/manual tenant the user picked, or a newly minted
    # owner-scoped ``manual:manual:<user_id>:<name>`` only when they chose
    # Create new and the name does not already match an owned account. See
    # docs/V2_TENANT_KEY_DESIGN.md.

    ok, err, history_rows, current_rows, head_sha, no_changes = merge_and_push_seeds(
        account_name,
        history_df,
        current_df,
        commit_message=commit_msg,
        user_id=current_user.id,
        tenant_id=tenant_id,
        skip_history=skip_history,
    )
    if not ok:
        from app import app as _app
        _app.logger.error("Upload seeds update failed: %s", err)
        flash("Couldn't save that upload right now. Try again in a moment, or contact support if it keeps happening.", "danger")
        return redirect(url_for("upload"))

    # Reverse trial: first data starts the 30-day clock (once-only,
    # trial-plan-only inside the helper; best-effort).
    try:
        from app.plan import start_trial_clock
        start_trial_clock(current_user.id)
    except Exception:
        pass

    display_name = account_name
    try:
        from app.routes import _tenant_label_map_for_user
        display_name = (
            _tenant_label_map_for_user(current_user.id).get(tenant_id)
            or account_name
        )
    except Exception:
        display_name = account_name

    if no_changes:
        # Identical upload — nothing changed on the branch, so no rebuild ran.
        # Don't send the user to the processing page to watch a build that
        # will never start.
        flash(
            f"That upload for {display_name} matches what's already on file — "
            "nothing changed, so there's nothing new to process.",
            "info",
        )
        return redirect(url_for("upload"))

    if history_df is None:
        flash(
            f"Upload saved for {display_name} ({current_rows:,} positions). "
            "Your data is updating in the background.",
            "success",
        )
    elif current_df is None:
        flash(
            f"Upload saved for {display_name} ({history_rows:,} trades). "
            "Your data is updating in the background.",
            "success",
        )
    else:
        flash(
            f"Upload saved for {display_name} ({history_rows:,} trades, {current_rows:,} positions). "
            "Your data is updating in the background.",
            "success",
        )

    qp = {}
    if head_sha:
        qp["sha"] = head_sha
    if is_first_upload:
        qp["first"] = 1
    return redirect(url_for("upload_processing", **qp))


_INFLIGHT_GITHUB_STATUS = frozenset({
    "queued", "waiting", "requested", "pending", "in_progress",
})


def _pick_dispatch_run(matches):
    """Choose which workflow_dispatch run the processing poll should follow.

    GitHub returns newest-first. ``cancel-in-progress`` means a later dispatch
    cancels an earlier one: the oldest match is often ``cancelled`` while the
    live rebuild is still running. Prefer an in-flight run, then a success,
    else the newest terminal result.
    """
    if not matches:
        return None
    for run in matches:
        if run.get("status") in _INFLIGHT_GITHUB_STATUS:
            return run
    for run in matches:
        if run.get("status") == "completed" and run.get("conclusion") == "success":
            return run
    return matches[0]


def _github_workflow_state_for_head(head_sha: str) -> dict:
    """
    Return dict with keys: state, github_status, conclusion, html_url, error.
    state is pending | running | success | failure | error

    Accepts either a legacy commit SHA or a ``dispatch:<unix_ts>`` build
    marker (what seed writes return since the BQ seed store migration —
    the rebuild is a ``workflow_dispatch`` run, not a push-triggered one,
    so it's resolved as the live/successful bigquery_update run created
    at/after the dispatch timestamp).
    """
    if not head_sha or len(head_sha) < 7:
        return {"state": "error", "error": "invalid_sha"}
    if not os.environ.get("GITHUB_PAT", "").strip():
        return {"state": "error", "error": "github_not_configured"}
    parts = _github_repo().split("/", 1)
    if len(parts) != 2:
        return {"state": "error", "error": "bad_repo"}
    owner, repo = parts

    dispatched_at = None
    if head_sha.startswith(_DISPATCH_MARKER_PREFIX):
        try:
            dispatched_at = int(head_sha[len(_DISPATCH_MARKER_PREFIX):])
        except ValueError:
            return {"state": "error", "error": "invalid_sha"}
        url = (
            f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/"
            f"{_WORKFLOW_FILE}/runs"
        )
        params = {"event": "workflow_dispatch", "per_page": 20}
    else:
        url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs"
        params = {"head_sha": head_sha.strip(), "per_page": 5}

    try:
        resp = requests.get(
            url,
            headers=_github_headers(),
            params=params,
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
    if dispatched_at is not None:
        # Only runs created at/after the dispatch (small clock-skew grace)
        # can be "our" build. Newest-first list: prefer in-flight over a
        # cancelled predecessor (concurrency cancel-in-progress).
        from datetime import datetime, timezone as _tz

        def _created_ts(r):
            try:
                return datetime.strptime(
                    r.get("created_at") or "", "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=_tz.utc).timestamp()
            except ValueError:
                return 0
        matches = [r for r in runs if _created_ts(r) >= dispatched_at - 120]
        picked = _pick_dispatch_run(matches)
        runs = [picked] if picked is not None else []
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
    expected_minutes = 25
    head_sha = (request.args.get("sha") or "").strip() or None
    is_first = (request.args.get("first") or "").strip() == "1"

    if is_first:
        done_url = url_for("get_started", from_upload=1)
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
    """After a SnapTrade (or CSV) seed push, wait for the warehouse rebuild.

    ``?connecting=1`` is the post-portal path: the pull is already running
    in the background; copy says so instead of asking them to Sync now.
    """
    from app.models import get_onboarding_response

    expected_minutes = 25
    head_sha = (request.args.get("sha") or "").strip() or None
    is_first = (request.args.get("first") or "").strip() == "1"

    if is_first:
        done_url = url_for("get_started", from_sync=1)
    else:
        done_url = url_for("weekly_review", from_sync=1)

    # Only prompt the onboarding survey on the first sync, and only if
    # the user hasn't already answered. Refreshing the page after submit
    # therefore won't re-show the form. The poll script will pause its
    # auto-redirect while the form is being interacted with so a slow
    # typer doesn't lose their answer to the dbt build finishing.
    show_onboarding = bool(
        is_first and get_onboarding_response(current_user.id) is None
    )

    from app.early_broker import early_broker_notice_for_user
    connecting = (request.args.get("connecting") or "").strip() == "1"
    return render_template(
        "sync_processing.html",
        title="Pulling your brokerage data" if connecting else "Processing sync",
        expected_minutes=expected_minutes,
        head_sha=head_sha,
        done_url=done_url,
        show_onboarding=show_onboarding,
        connecting=connecting,
        early_broker=early_broker_notice_for_user(current_user.id),
    )


@app.route("/api/sync/overview-ready")
@login_required
def api_sync_overview_ready():
    """Poll for whether this user's Overview can actually render.

    Used by the post-connect processing page. Must match the data-ready
    email gate (``positions_summary`` rows for the user's tenants) so we
    never send someone to an empty Overview.
    """
    from app.cache_ops import warehouse_has_rows_for_tenants
    from app.models import get_tenant_ids_for_user

    tids = get_tenant_ids_for_user(current_user.id) or []
    return jsonify({"ready": warehouse_has_rows_for_tenants(tids)})


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
