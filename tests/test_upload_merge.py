"""
Regression tests for ``_merge_seed_with_existing`` — the helper that turns
an existing seed CSV in GitHub plus a freshly-synced DataFrame into the
next CSV state to commit.

Two production-impacting bugs sit here. Both are exercised below:

1. **Dedup direction.** The original code marked rows ``"old"``/``"new"``
   and called ``sort_values("__src")`` ascending. ``"new"`` sorts before
   ``"old"`` alphabetically, so ``keep="last"`` retained the LEGACY row
   on every key collision. Multi-account users who had pre-tenancy seed
   rows (``user_id=""``) saw every freshly-synced trade silently
   discarded — only accounts with no legacy rows landed at all.

2. **Tenant scope.** ``account_mask`` was just ``Account == account_name``,
   so a sync touched every row under that account name regardless of
   owner. When two users legitimately share an account label (parent +
   child both saying ``"Schwab Account"`` per docs/USER_ID_TENANCY.md),
   user B's sync would dedup against user A's rows and either lose or
   steal them depending on the sort direction. The fix scopes the merge
   to (syncing user's rows | legacy unowned rows).

Tests use small in-memory CSV strings and monkeypatch ``_get_file_content``
so they stay unit-fast and don't touch GitHub or pandas IO machinery.
"""
import io

import pandas as pd
import pytest

from app import upload as _upload


HISTORY_PATH = _upload.HISTORY_PATH
HISTORY_SEED_COLUMNS = _upload.HISTORY_SEED_COLUMNS


def _stub_existing(monkeypatch, csv_text):
    """Make ``_get_file_content`` return a deterministic existing CSV."""
    monkeypatch.setattr(_upload, "_get_file_content", lambda path: csv_text)


def _row(account, user_id, date, action, symbol, qty, price, amount, *, desc="", fees=""):
    """Build a dict shaped like a HISTORY_SEED_COLUMNS row.

    Numeric fields are coerced to ``float`` (or kept blank for non-trade
    actions like Bank Interest) so the CSV round-trip type inference is
    stable. Real seeds always end up float-typed because Bank Interest /
    dividend rows leave Quantity/Price blank — pandas coerces the whole
    column on read. Tests must match that to exercise dedup correctly.
    """
    def _f(v):
        return "" if v == "" else float(v)
    return {
        "Account": account,
        "user_id": user_id,
        "Date": date,
        "Action": action,
        "Symbol": symbol,
        "Description": desc,
        "Quantity": _f(qty),
        "Price": _f(price),
        "fees_and_comm": fees,
        "Amount": _f(amount),
    }


def _csv_from_rows(rows):
    df = pd.DataFrame(rows, columns=HISTORY_SEED_COLUMNS)
    return df.to_csv(index=False)


def _parse(csv_text):
    """Round-trip the merged CSV the way dbt reads it.

    ``user_id`` round-trips as the string ``"9.0"`` (not ``"9"``) because
    pandas reads the column as float — empty/legacy rows force float
    inference. Normalize on read so assertions can compare against the
    canonical integer-string form (``"9"``) regardless of how pandas
    chose to stringify the cell.
    """
    df = pd.read_csv(io.StringIO(csv_text), dtype=str, keep_default_na=False)
    if "user_id" in df.columns:
        df["user_id"] = df["user_id"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    return df


# ---------------------------------------------------------------------------
# Bug 1: legacy user_id="" rows must NOT win the dedup over fresh user_id=N
# rows. The new row carries the syncing user's tenant id; without this, the
# user's BigQuery filter (WHERE user_id = N) excludes their own data.
# ---------------------------------------------------------------------------


def test_merge_keeps_new_row_when_legacy_empty_user_id_collides(monkeypatch):
    existing = _csv_from_rows([
        _row("Schwab ••••9437", "", "06/11/2025", "Buy to Close",
             "CRWV  250613C00170000", 1, 0.74, -74.0, desc="COREWEAVE 06/13/25 $170 Call"),
        _row("Schwab ••••9437", "", "06/16/2025", "Bank Interest",
             "", "", "", 0.77, desc="BANK INT 051625-061525"),
    ])
    _stub_existing(monkeypatch, existing)

    new_df = pd.DataFrame([
        _row("Schwab ••••9437", 9, "06/11/2025", "Buy to Close",
             "CRWV  250613C00170000", 1, 0.74, -74.0, desc="COREWEAVE 06/13/25 $170 Call"),
        _row("Schwab ••••9437", 9, "06/16/2025", "Bank Interest",
             "", "", "", 0.77, desc="BANK INT 051625-061525"),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Schwab ••••9437", new_df, HISTORY_SEED_COLUMNS,
        user_id=9,
    )
    out = _parse(out_csv)

    assert len(out) == 2, "dedup should collapse two pairs to two unique trades"
    # The single most important assertion: the legacy "" rows must be GONE,
    # replaced by the freshly-tagged user_id=9 rows. This is the bug that
    # made multi-account syncs look like they "didn't land" anywhere.
    assert set(out["user_id"].tolist()) == {"9"}
    assert set(out["Account"].tolist()) == {"Schwab ••••9437"}


def test_merge_adds_brand_new_trades_when_no_collision(monkeypatch):
    """Sanity: rows that aren't already present land cleanly."""
    existing = _csv_from_rows([
        _row("Schwab ••••9437", "", "01/01/2024", "Buy", "AAPL", 10, 100.0, -1000.0),
    ])
    _stub_existing(monkeypatch, existing)

    new_df = pd.DataFrame([
        _row("Schwab ••••9437", 9, "01/02/2024", "Sell", "AAPL", 10, 110.0, 1100.0),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Schwab ••••9437", new_df, HISTORY_SEED_COLUMNS,
        user_id=9,
    )
    out = _parse(out_csv)
    assert len(out) == 2
    # Existing legacy row stays attributed to user_id="" (no key match to
    # rewrite it); new row lands as user_id=9.
    assert sorted(out["Date"].tolist()) == ["01/01/2024", "01/02/2024"]


# ---------------------------------------------------------------------------
# Bug 2: tenancy scope — when two users share an account label, a sync MUST
# leave the other user's rows alone, even on identical trade keys. Account
# labels are allowed to collide per docs/USER_ID_TENANCY.md (parent+child
# scenario); row-level user_id is what the BQ filter trusts.
# ---------------------------------------------------------------------------


def test_merge_does_not_touch_other_users_rows_under_same_account_name(monkeypatch):
    # User 2 has a 'Sara Investment' account with two trades in the seed.
    # User 9 also has a 'Sara Investment' (label collision is legal) and
    # is now syncing two of their own trades.
    existing = _csv_from_rows([
        _row("Sara Investment", 2, "04/17/2026", "Buy", "ASML", 32, 1521.0, -48672.0),
        _row("Sara Investment", 2, "04/17/2026", "Sell", "UFO", 2100, 50.0, 105000.0),
        _row("Sara Investment", "", "04/14/2026", "Buy", "CURRENCY_USD", 2.57, "", -2.57),
    ])
    _stub_existing(monkeypatch, existing)

    new_df = pd.DataFrame([
        # user_id=9 just happened to do the SAME ASML trade on the same
        # day at the same price. Pre-fix this would have either been
        # silently dropped (legacy/buggy sort) or, worse, would have
        # rewritten user 2's row to carry user_id=9 (sort-only fix).
        _row("Sara Investment", 9, "04/17/2026", "Buy", "ASML", 32, 1521.0, -48672.0),
        _row("Sara Investment", 9, "05/01/2026", "Buy", "MSFT", 5, 400.0, -2000.0),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Sara Investment", new_df, HISTORY_SEED_COLUMNS,
        user_id=9,
    )
    out = _parse(out_csv)

    user2_rows = out[out["user_id"] == "2"]
    user9_rows = out[out["user_id"] == "9"]
    legacy_rows = out[out["user_id"] == ""]

    # User 2's rows must be PRESERVED verbatim — neither dropped nor
    # rewritten — because they were never in the dedup window.
    assert len(user2_rows) == 2
    assert set(zip(user2_rows["Date"], user2_rows["Symbol"])) == {
        ("04/17/2026", "ASML"),
        ("04/17/2026", "UFO"),
    }

    # User 9 gets BOTH new rows: the ASML row that key-matches user 2's
    # ASML row is allowed to land separately because the dedup window
    # excludes user 2 entirely.
    assert len(user9_rows) == 2
    assert set(zip(user9_rows["Date"], user9_rows["Symbol"])) == {
        ("04/17/2026", "ASML"),
        ("05/01/2026", "MSFT"),
    }

    # Legacy unowned currency_usd row stays put — not user 2's, not key-
    # matched by user 9's sync, so it lands in other_df untouched.
    assert len(legacy_rows) == 1


def test_merge_legacy_row_is_reclaimed_only_by_matching_user(monkeypatch):
    """Two users syncing the same legacy row don't both get to claim it.

    Only the syncing user's rows + truly unowned legacy rows are in the
    dedup window, so user 9's sync can re-tag a user_id="" row, but a
    later user 2 sync into a different account cannot reach across.
    """
    existing = _csv_from_rows([
        _row("Schwab ••••9437", "", "06/11/2025", "Buy", "CRWV", 1, 170.0, -170.0),
    ])
    _stub_existing(monkeypatch, existing)

    # User 9 syncs a different account ('Schwab ••••5167') with a
    # coincidentally identical trade key. The legacy row above is for a
    # DIFFERENT account label — account_mask excludes it from the dedup
    # window, so it survives untouched.
    new_df = pd.DataFrame([
        _row("Schwab ••••5167", 9, "06/11/2025", "Buy", "CRWV", 1, 170.0, -170.0),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Schwab ••••5167", new_df, HISTORY_SEED_COLUMNS,
        user_id=9,
    )
    out = _parse(out_csv)
    assert len(out) == 2
    legacy = out[(out["user_id"] == "") & (out["Account"] == "Schwab ••••9437")]
    fresh = out[(out["user_id"] == "9") & (out["Account"] == "Schwab ••••5167")]
    assert len(legacy) == 1, "legacy row in a different account is not in scope"
    assert len(fresh) == 1


# ---------------------------------------------------------------------------
# Re-syncing the same data must be idempotent — clicking "Sync now" twice
# in a row should not double-count trades or leave the file thrashing.
# ---------------------------------------------------------------------------


def test_merge_is_idempotent_for_repeated_sync(monkeypatch):
    existing = _csv_from_rows([
        _row("Schwab ••••9437", 9, "06/11/2025", "Buy", "CRWV", 1, 170.0, -170.0),
        _row("Schwab ••••9437", 9, "06/12/2025", "Sell", "CRWV", 1, 175.0, 175.0),
    ])
    _stub_existing(monkeypatch, existing)

    new_df = pd.DataFrame([
        _row("Schwab ••••9437", 9, "06/11/2025", "Buy", "CRWV", 1, 170.0, -170.0),
        _row("Schwab ••••9437", 9, "06/12/2025", "Sell", "CRWV", 1, 175.0, 175.0),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Schwab ••••9437", new_df, HISTORY_SEED_COLUMNS,
        user_id=9,
    )
    out = _parse(out_csv)
    assert len(out) == 2
    assert set(out["user_id"].tolist()) == {"9"}


# ---------------------------------------------------------------------------
# Defensive: when no user_id is plumbed through (legacy callers, tests),
# the merge falls back to the unscoped behavior. Pinning so the fallback
# stays explicit and future refactors don't drop the kwarg silently.
# ---------------------------------------------------------------------------


def test_merge_without_user_id_falls_back_to_account_only_scope(monkeypatch):
    existing = _csv_from_rows([
        _row("Schwab ••••9437", 2, "06/11/2025", "Buy", "CRWV", 1, 170.0, -170.0),
    ])
    _stub_existing(monkeypatch, existing)

    new_df = pd.DataFrame([
        _row("Schwab ••••9437", 9, "06/12/2025", "Sell", "CRWV", 1, 175.0, 175.0),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Schwab ••••9437", new_df, HISTORY_SEED_COLUMNS,
    )
    out = _parse(out_csv)
    # Without user_id scoping, both rows survive (no key collision); this
    # asserts the fallback path doesn't crash and still de-dups correctly.
    assert len(out) == 2
