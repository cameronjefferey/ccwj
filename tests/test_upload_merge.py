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


def _row(account, user_id, date, action, symbol, qty, price, amount, *, tenant_id="", desc="", fees=""):
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
        "tenant_id": tenant_id,
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
# Bug 1: legacy tenant_id="" rows must NOT win the dedup over fresh tenant_id
# rows. The new row carries the syncing tenant's id; without this, the
# user's BigQuery filter excludes their own data.
# ---------------------------------------------------------------------------

TENANT_SCHWAB_9437 = "snaptrade:bed78305-a764-4c4d-b4c7-fe59e391f661"
TENANT_SARA_2 = "snaptrade:tenant-sara-2"
TENANT_SARA_9 = "snaptrade:tenant-sara-9"
TENANT_CAMERON = "manual:Cameron Investment"
TENANT_SCHWAB_5989 = "snaptrade:tenant-schwab-5989"
TENANT_SCHWAB_5167 = "snaptrade:tenant-schwab-5167"
TENANT_ALPACA = "snaptrade:tenant-alpaca-paper"


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
             "CRWV  250613C00170000", 1, 0.74, -74.0,
             tenant_id=TENANT_SCHWAB_9437,
             desc="COREWEAVE 06/13/25 $170 Call"),
        _row("Schwab ••••9437", 9, "06/16/2025", "Bank Interest",
             "", "", "", 0.77, tenant_id=TENANT_SCHWAB_9437,
             desc="BANK INT 051625-061525"),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Schwab ••••9437", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_SCHWAB_9437,
    )
    out = _parse(out_csv)

    assert len(out) == 2, "dedup should collapse two pairs to two unique trades"
    assert set(out["tenant_id"].tolist()) == {TENANT_SCHWAB_9437}
    assert set(out["Account"].tolist()) == {"Schwab ••••9437"}


def test_merge_adds_brand_new_trades_when_no_collision(monkeypatch):
    """Sanity: rows that aren't already present land cleanly."""
    existing = _csv_from_rows([
        _row("Schwab ••••9437", "", "01/01/2024", "Buy", "AAPL", 10, 100.0, -1000.0),
    ])
    _stub_existing(monkeypatch, existing)

    new_df = pd.DataFrame([
        _row("Schwab ••••9437", 9, "01/02/2024", "Sell", "AAPL", 10, 110.0, 1100.0,
             tenant_id=TENANT_SCHWAB_9437),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Schwab ••••9437", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_SCHWAB_9437,
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
        _row("Sara Investment", 2, "04/17/2026", "Buy", "ASML", 32, 1521.0, -48672.0,
             tenant_id=TENANT_SARA_2),
        _row("Sara Investment", 2, "04/17/2026", "Sell", "UFO", 2100, 50.0, 105000.0,
             tenant_id=TENANT_SARA_2),
        _row("Sara Investment", "", "04/14/2026", "Buy", "CURRENCY_USD", 2.57, "", -2.57),
    ])
    _stub_existing(monkeypatch, existing)

    new_df = pd.DataFrame([
        _row("Sara Investment", 9, "04/17/2026", "Buy", "ASML", 32, 1521.0, -48672.0,
             tenant_id=TENANT_SARA_9),
        _row("Sara Investment", 9, "05/01/2026", "Buy", "MSFT", 5, 400.0, -2000.0,
             tenant_id=TENANT_SARA_9),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Sara Investment", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_SARA_9,
    )
    out = _parse(out_csv)

    tenant2_rows = out[out["tenant_id"] == TENANT_SARA_2]
    tenant9_rows = out[out["tenant_id"] == TENANT_SARA_9]
    legacy_rows = out[out["tenant_id"] == ""]

    assert len(tenant2_rows) == 2
    assert set(zip(tenant2_rows["Date"], tenant2_rows["Symbol"])) == {
        ("04/17/2026", "ASML"),
        ("04/17/2026", "UFO"),
    }

    assert len(tenant9_rows) == 2
    assert set(zip(tenant9_rows["Date"], tenant9_rows["Symbol"])) == {
        ("04/17/2026", "ASML"),
        ("05/01/2026", "MSFT"),
    }

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
        _row("Schwab ••••5167", 9, "06/11/2025", "Buy", "CRWV", 1, 170.0, -170.0,
             tenant_id=TENANT_SCHWAB_5167),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Schwab ••••5167", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_SCHWAB_5167,
    )
    out = _parse(out_csv)
    assert len(out) == 2
    legacy = out[(out["tenant_id"] == "") & (out["Account"] == "Schwab ••••9437")]
    fresh = out[(out["tenant_id"] == TENANT_SCHWAB_5167) & (out["Account"] == "Schwab ••••5167")]
    assert len(legacy) == 1, "legacy row in a different account is not in scope"
    assert len(fresh) == 1


# ---------------------------------------------------------------------------
# Re-syncing the same data must be idempotent — clicking "Sync now" twice
# in a row should not double-count trades or leave the file thrashing.
# ---------------------------------------------------------------------------


def test_merge_is_idempotent_for_repeated_sync(monkeypatch):
    existing = _csv_from_rows([
        _row("Schwab ••••9437", 9, "06/11/2025", "Buy", "CRWV", 1, 170.0, -170.0,
             tenant_id=TENANT_SCHWAB_9437),
        _row("Schwab ••••9437", 9, "06/12/2025", "Sell", "CRWV", 1, 175.0, 175.0,
             tenant_id=TENANT_SCHWAB_9437),
    ])
    _stub_existing(monkeypatch, existing)

    new_df = pd.DataFrame([
        _row("Schwab ••••9437", 9, "06/11/2025", "Buy", "CRWV", 1, 170.0, -170.0,
             tenant_id=TENANT_SCHWAB_9437),
        _row("Schwab ••••9437", 9, "06/12/2025", "Sell", "CRWV", 1, 175.0, 175.0,
             tenant_id=TENANT_SCHWAB_9437),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Schwab ••••9437", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_SCHWAB_9437,
    )
    out = _parse(out_csv)
    assert len(out) == 2
    assert set(out["tenant_id"].tolist()) == {TENANT_SCHWAB_9437}


# ---------------------------------------------------------------------------
# Defensive: when no user_id is plumbed through (legacy callers, tests),
# the merge falls back to the unscoped behavior. Pinning so the fallback
# stays explicit and future refactors don't drop the kwarg silently.
# ---------------------------------------------------------------------------


def test_merge_without_tenant_id_falls_back_to_account_only_scope(monkeypatch):
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
    # Without tenant_id scoping, both rows survive (no key collision); this
    # asserts the fallback path doesn't crash and still de-dups correctly.
    assert len(out) == 2


# ---------------------------------------------------------------------------
# Bug A (commit 3f4aecb): a transient GitHub fetch failure used to fall
# through to "treat existing seed as empty and write only the new account's
# rows", silently destroying every other tenant's history. Sara Investment's
# 408-row sync wiped 10,446 rows belonging to four other accounts and three
# other users. The merge MUST refuse to run unless the existing seed was
# definitively absent (HTTP 404).
# ---------------------------------------------------------------------------


def test_merge_raises_when_existing_fetch_blips_5xx(monkeypatch):
    """A 503 from GitHub must NOT be interpreted as 'no existing data'."""
    def _boom(path):
        raise _upload.SeedFetchError("HTTP 503 from GitHub")
    monkeypatch.setattr(_upload, "_get_file_content", _boom)

    new_df = pd.DataFrame([
        _row("Sara Investment", 9, "05/01/2026", "Buy", "AAPL", 1, 100.0, -100.0,
             tenant_id=TENANT_SARA_9),
    ])
    with pytest.raises(_upload.SeedFetchError):
        _upload._merge_seed_with_existing(
            HISTORY_PATH, "Sara Investment", new_df, HISTORY_SEED_COLUMNS,
            tenant_id=TENANT_SARA_9,
        )


def test_merge_treats_true_404_as_empty_seed(monkeypatch):
    """A real 404 (file does not exist yet) is the only safe overwrite signal."""
    monkeypatch.setattr(_upload, "_get_file_content", lambda path: None)

    new_df = pd.DataFrame([
        _row("Sara Investment", 9, "05/01/2026", "Buy", "AAPL", 1, 100.0, -100.0,
             tenant_id=TENANT_SARA_9),
    ])
    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Sara Investment", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_SARA_9,
    )
    out = _parse(out_csv)
    assert len(out) == 1
    assert out["Account"].tolist() == ["Sara Investment"]
    assert out["tenant_id"].tolist() == [TENANT_SARA_9]


def test_merge_refuses_to_overwrite_when_existing_unparseable(monkeypatch):
    """Garbage in the seed file must abort, not blank out other tenants."""
    monkeypatch.setattr(
        _upload, "_get_file_content",
        lambda path: "this,is,not,a,valid\nheader\x00row\nwith,bad,bytes",
    )
    new_df = pd.DataFrame([
        _row("Sara Investment", 9, "05/01/2026", "Buy", "AAPL", 1, 100.0, -100.0,
             tenant_id=TENANT_SARA_9),
    ])
    # Either parse raises or the no-Account-column branch raises. Both
    # land in SeedFetchError; either way the merge refuses to overwrite.
    with pytest.raises(_upload.SeedFetchError):
        _upload._merge_seed_with_existing(
            HISTORY_PATH, "Sara Investment", new_df, HISTORY_SEED_COLUMNS,
            tenant_id=TENANT_SARA_9,
        )


# ---------------------------------------------------------------------------
# Bug B (commit 05c5ae5): re-sync must replace existing tenant rows even when
# legacy rows in the same file force pandas float coercion on other columns.
# ---------------------------------------------------------------------------


def test_merge_dedupes_against_existing_user_rows_with_float_string_uid(monkeypatch):
    """Re-syncing must REPLACE existing user_9 rows even when pandas
    stringified them as '9.0' due to NaN-induced float coercion in the CSV."""
    # Force pandas to read user_id as float by mixing a NaN row in.
    existing = _csv_from_rows([
        _row("Cameron Investment", 9, "01/01/2025", "Buy", "AAPL", 10, 100.0, -1000.0,
             tenant_id=TENANT_CAMERON),
        _row("Cameron Investment", 9, "01/02/2025", "Sell", "AAPL", 10, 110.0, 1100.0,
             tenant_id=TENANT_CAMERON),
        _row("Schwab Account", "", "01/01/2024", "Buy", "MSFT", 5, 300.0, -1500.0),
    ])
    _stub_existing(monkeypatch, existing)

    new_df = pd.DataFrame([
        _row("Cameron Investment", 9, "01/01/2025", "Buy", "AAPL", 10, 100.0, -1000.0,
             tenant_id=TENANT_CAMERON),
        _row("Cameron Investment", 9, "01/02/2025", "Sell", "AAPL", 10, 110.0, 1100.0,
             tenant_id=TENANT_CAMERON),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Cameron Investment", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_CAMERON,
    )
    out = _parse(out_csv)

    cam = out[out["Account"] == "Cameron Investment"]
    # The two existing user_9 rows MUST dedup against the two new user_9
    # rows. Without the user_id normalization they end up in other_df and
    # we'd see 4 Cameron Investment rows (the production doubling bug).
    assert len(cam) == 2, (
        f"expected 2 Cameron rows after re-sync (dedup), got {len(cam)}: "
        f"{cam.to_dict('records')}"
    )
    assert set(cam["tenant_id"].tolist()) == {TENANT_CAMERON}
    # Other-account legacy row stays untouched.
    other = out[out["Account"] == "Schwab Account"]
    assert len(other) == 1


def test_merge_full_resync_does_not_double_count_after_three_runs(monkeypatch):
    """Run the same sync three times; row count must stay constant."""
    sync_rows = [
        _row("Cameron Investment", 9, "01/01/2025", "Buy", "AAPL", 10, 100.0, -1000.0,
             tenant_id=TENANT_CAMERON),
        _row("Cameron Investment", 9, "01/02/2025", "Sell", "AAPL", 10, 110.0, 1100.0,
             tenant_id=TENANT_CAMERON),
        _row("Cameron Investment", 9, "01/03/2025", "Buy", "MSFT", 5, 400.0, -2000.0,
             tenant_id=TENANT_CAMERON),
    ]
    # Force float-typed user_id column.
    base_with_legacy = _csv_from_rows(sync_rows + [
        _row("Schwab Account", "", "12/31/2023", "Buy", "TSLA", 1, 200.0, -200.0),
    ])
    state = {"csv": base_with_legacy}
    monkeypatch.setattr(_upload, "_get_file_content", lambda path: state["csv"])

    for _ in range(3):
        out_csv = _upload._merge_seed_with_existing(
            HISTORY_PATH, "Cameron Investment",
            pd.DataFrame(sync_rows), HISTORY_SEED_COLUMNS,
            tenant_id=TENANT_CAMERON,
        )
        state["csv"] = out_csv

    final = _parse(state["csv"])
    cam = final[final["Account"] == "Cameron Investment"]
    assert len(cam) == 3, (
        f"three identical re-syncs must remain 3 rows, got {len(cam)}: "
        f"{cam.to_dict('records')}"
    )
    other = final[final["Account"] == "Schwab Account"]
    assert len(other) == 1


def test_normalize_uid_collapses_known_pandas_string_forms():
    """Direct unit test for the helper that fixes Bug B."""
    n = _upload._normalize_uid
    assert n(9) == "9"
    assert n("9") == "9"
    assert n("9.0") == "9"
    assert n(" 9 ") == "9"
    assert n("9.0 ") == "9"
    assert n("") == ""
    assert n("nan") == ""
    assert n("None") == ""
    assert n(None) == ""
    assert n(float("nan")) == ""


# ---------------------------------------------------------------------------
# Bug C (May 2026, /position/BE Sara Investment): production showed
# ``user_id=7, account='Schwab ••••5989'`` with 213 rows in stg_history but
# only 158 unique trades — 55 dupes that the existing dedup did not catch.
# Sample seed rows:
#   ...11/14/2024,Buy,CURRENCY_USD,USD currency,26.44,,,-26.44   (5 copies)
#   ...12/04/2024,Buy,CURRENCY_USD,USD currency,26.990000000000002,,,
#                                              -26.990000000000002  (3 copies)
#   ...12/04/2024,Buy,CURRENCY_USD,USD currency,27.000000000000004,,,
#                                              -27.000000000000004  (1 copy)
# Same trade, different float-precision serializations across sync runs. The
# old dedup ran ``astype(str)`` on the Amount column, so ``"26.99"`` and
# ``"26.990000000000002"`` were treated as different rows and both survived.
# The fix canonicalizes numeric cells via ``_canonicalize_seed_cell`` before
# the dedup. Without the fix these tests fail; with it they pass.
# ---------------------------------------------------------------------------


def test_merge_dedupes_against_float_precision_drift_in_amount(monkeypatch):
    """Same trade re-landing with a JSON-round-trip float precision artifact
    (``26.99`` → ``26.990000000000002``) must collapse to a single row."""
    existing = _csv_from_rows([
        _row("Schwab ••••5989", 7, "12/04/2024", "Buy", "CURRENCY_USD",
             26.99, "", -26.99, tenant_id=TENANT_SCHWAB_5989, desc="USD currency"),
    ])
    _stub_existing(monkeypatch, existing)

    new_df = pd.DataFrame([
        _row("Schwab ••••5989", 7, "12/04/2024", "Buy", "CURRENCY_USD",
             26.990000000000002, "", -26.990000000000002,
             tenant_id=TENANT_SCHWAB_5989, desc="USD currency"),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Schwab ••••5989", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_SCHWAB_5989,
    )
    out = _parse(out_csv)
    assert len(out) == 1, (
        f"float precision drift must dedup; got {len(out)} rows: "
        f"{out.to_dict('records')}"
    )


def test_merge_dedupes_against_int_vs_float_seed_cells(monkeypatch):
    """Existing seed has ``Quantity=40`` (int), new sync has ``40.0`` (float).
    Same trade — must dedup to one row regardless of which side is which."""
    existing = _csv_from_rows([
        # Force int-shaped values into the seed so pandas reads them as ints
        # in the int column it can. We pass plain ints by side-stepping _row's
        # float coercion: mimic a hand-edited seed with bare int strings.
    ])
    int_seed_csv = (
        "Account,user_id,tenant_id,Date,Action,Symbol,Description,Quantity,Price,fees_and_comm,Amount\n"
        "Schwab ••••5989,7,,11/14/2024,Sell to Open,CFLT  241220C00030000,"
        "CONFLUENT INC 12/20/2024 $30 Call,40,1.15,,4600\n"
    )
    _stub_existing(monkeypatch, int_seed_csv)

    new_df = pd.DataFrame([
        _row("Schwab ••••5989", 7, "11/14/2024", "Sell to Open",
             "CFLT  241220C00030000", 40.0, 1.15, 4600.0,
             tenant_id=TENANT_SCHWAB_5989,
             desc="CONFLUENT INC 12/20/2024 $30 Call"),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Schwab ••••5989", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_SCHWAB_5989,
    )
    out = _parse(out_csv)
    assert len(out) == 1, (
        f"int-vs-float same-value cells must dedup; got {len(out)} rows: "
        f"{out.to_dict('records')}"
    )


def test_merge_collapses_byte_identical_quintuple_landing(monkeypatch):
    """The production seed has 5 byte-identical copies of the same option
    fill (CFLT 11/14/2024 Sell to Open). A single new sync of that trade
    must collapse all 5 + the new row down to one row, regardless of how
    they got there. Stronger than the existing 3-cycle resync test because
    it starts from a *seed already poisoned by historical dupes* — exactly
    the recovery state we'll be in after Phase 2 cleans the file."""
    existing_csv = (
        "Account,user_id,tenant_id,Date,Action,Symbol,Description,Quantity,Price,fees_and_comm,Amount\n"
        + ("Schwab ••••5989,7.0,,11/14/2024,Sell to Open,CFLT  241220C00030000,"
           "CONFLUENT INC 12/20/2024 $30 Call,40.0,1.15,,4600.0\n" * 5)
    )
    _stub_existing(monkeypatch, existing_csv)

    new_df = pd.DataFrame([
        _row("Schwab ••••5989", 7, "11/14/2024", "Sell to Open",
             "CFLT  241220C00030000", 40.0, 1.15, 4600.0,
             tenant_id=TENANT_SCHWAB_5989,
             desc="CONFLUENT INC 12/20/2024 $30 Call"),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Schwab ••••5989", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_SCHWAB_5989,
    )
    out = _parse(out_csv)
    assert len(out) == 1, (
        f"5 byte-identical legacy rows + 1 new sync row must collapse to 1, "
        f"got {len(out)}"
    )


def test_dedup_collapses_drift_within_new_df_even_when_existing_empty(monkeypatch):
    """Schwab's transactions API can return the SAME fill twice in a single
    sync response with float-text drift (``100`` vs ``100.0``,
    ``-7660`` vs ``-7660.0``). The old merge had three early-return paths
    that bypassed dedup entirely when the existing seed scope was empty:

      1. ``existing_content is None`` (HTTP 404 from GitHub Contents API)
      2. ``existing_content`` is whitespace-only (manually truncated)
      3. ``existing_df.empty`` (file exists but has only a header)

    AND a fourth subtle path inside the HISTORY_PATH branch:

      4. ``existing_account_df.empty`` — when the syncing user has no
         prior rows for this account (freshly-linked account, no legacy
         user_id="" rows for this account label, all existing rows owned
         by other tenants)

    All four paths shipped the broker's drift dupes verbatim. Bug
    landed May 2026 in commit ``cafc0713`` (Sara Investment ASTS x2 —
    both ``100`` and ``100.0`` forms ended up in
    ``dbt/seeds/trade_history.csv`` and tripped the warehouse-side
    ``stg_history_no_duplicate_fills_per_tenant`` test). The fix
    factors dedup into ``_dedup_history_rows`` and runs it on every
    HISTORY merge regardless of whether the existing scope is empty.

    Each parametrized scenario must collapse the broker's two fills
    into one canonical row. Last-write-wins picks the second form.
    """
    new_with_drift = pd.DataFrame([
        _row("Sara Investment", 9, "05/11/2026", "Buy", "ASTS", 100.0, 76.6, -7660.0,
             tenant_id=TENANT_SARA_9, desc="ASTS"),
        _row("Sara Investment", 9, "05/11/2026", "Buy", "ASTS", 100, 76.6, -7660,
             tenant_id=TENANT_SARA_9, desc="ASTS"),
    ])

    # Path 1: file does not exist (404).
    monkeypatch.setattr(_upload, "_get_file_content", lambda path: None)
    out = _parse(_upload._merge_seed_with_existing(
        HISTORY_PATH, "Sara Investment", new_with_drift.copy(),
        HISTORY_SEED_COLUMNS, tenant_id=TENANT_SARA_9,
    ))
    assert len(out) == 1, f"Path 1 (404) failed to dedup, got {len(out)} rows"

    # Path 2: file exists but content is whitespace-only.
    monkeypatch.setattr(_upload, "_get_file_content", lambda path: "   ")
    out = _parse(_upload._merge_seed_with_existing(
        HISTORY_PATH, "Sara Investment", new_with_drift.copy(),
        HISTORY_SEED_COLUMNS, tenant_id=TENANT_SARA_9,
    ))
    assert len(out) == 1, f"Path 2 (empty file) failed to dedup, got {len(out)} rows"

    # Path 3: parsed CSV is empty (header only, no rows).
    _stub_existing(monkeypatch, ",".join(HISTORY_SEED_COLUMNS) + "\n")
    out = _parse(_upload._merge_seed_with_existing(
        HISTORY_PATH, "Sara Investment", new_with_drift.copy(),
        HISTORY_SEED_COLUMNS, tenant_id=TENANT_SARA_9,
    ))
    assert len(out) == 1, f"Path 3 (header-only) failed to dedup, got {len(out)} rows"

    # Path 4: existing has rows BUT none for the syncing tenant under this
    # account label (all existing rows belong to a different tenant).
    _stub_existing(monkeypatch, _csv_from_rows([
        _row("Sara Investment", 2, "01/01/2025", "Buy", "SPY", 1, 500, -500,
             tenant_id=TENANT_SARA_2, desc="SPY ETF"),
    ]))
    out = _parse(_upload._merge_seed_with_existing(
        HISTORY_PATH, "Sara Investment", new_with_drift.copy(),
        HISTORY_SEED_COLUMNS, tenant_id=TENANT_SARA_9,
    ))
    asts = out.loc[out["Symbol"] == "ASTS"]
    assert len(asts) == 1, (
        f"Path 4 (other-tenant only, syncing user empty scope) failed to "
        f"dedup, got {len(asts)} ASTS rows: {asts.to_dict('records')}"
    )
    # Other tenant's row preserved.
    assert (out["Symbol"] == "SPY").sum() == 1, "other tenant's SPY row was dropped"


def test_dedup_helper_is_idempotent_and_preserves_distinct_trades():
    """``_dedup_history_rows`` must be a true idempotent dedup — running
    it twice gives the same result, and it never collapses rows that
    represent genuinely distinct trades (different symbols, different
    dates, or the same shape on different days).
    """
    df = pd.DataFrame([
        _row("A", 1, "01/01/2025", "Buy", "SPY", 1, 500, -500),
        _row("A", 1, "01/02/2025", "Buy", "SPY", 1, 500, -500),  # different date — distinct
        _row("A", 1, "01/01/2025", "Buy", "QQQ", 1, 500, -500),  # different symbol — distinct
        _row("A", 1, "01/01/2025", "Buy", "SPY", 1, 500, -500),  # exact dupe of row 0
    ])
    once = _upload._dedup_history_rows(df, HISTORY_SEED_COLUMNS)
    twice = _upload._dedup_history_rows(once, HISTORY_SEED_COLUMNS)
    assert len(once) == 3, f"expected 3 distinct trades, got {len(once)}"
    assert len(twice) == len(once), "dedup must be idempotent"
    syms = sorted(once["Symbol"].tolist())
    assert syms == ["QQQ", "SPY", "SPY"], f"unexpected symbol set: {syms}"


def test_canonicalize_seed_cell_collapses_known_drift_forms():
    """Direct unit test for the helper that fixes Bug C."""
    c = _upload._canonicalize_seed_cell
    # Float precision drift collapses to the same canonical form.
    assert c(26.99) == c(26.990000000000002) == "26.99"
    assert c(-16.189999999999998) == c(-16.19) == "-16.19"
    assert c(27.000000000000004) == c(27.0) == c(27) == "27"
    # Int-vs-float collapses.
    assert c(40) == c(40.0) == c("40") == c("40.0") == c("40.000000") == "40"
    # Empty-ish stays empty.
    assert c("") == ""
    assert c(None) == ""
    assert c("nan") == ""
    assert c(float("nan")) == ""
    # Negative zero collapses to zero (CSV serializers sometimes emit -0.0).
    assert c(-0.0) == "0"
    assert c("-0") == "0"
    # Non-numeric strings pass through unchanged (Date / Description / Action).
    assert c("Buy to Close") == "Buy to Close"
    assert c("CFLT  241220C00030000") == "CFLT  241220C00030000"
    assert c(" 11/14/2024 ") == "11/14/2024"


# ---------------------------------------------------------------------------
# Cross-source dedup — orders endpoint vs activities endpoint for the
# same trade. SnapTrade exposes both with hours-to-days lag between the
# two; our pipeline reads both. Without this dedup, every fresh trade
# would land twice (orders-row + later activities-row) once the
# activity feed catches up.
# ---------------------------------------------------------------------------


def test_dedup_collapses_orders_row_with_richer_activities_row():
    """orders_to_history_df emits Description = ``"NVIDIA Corporation"``
    (just the company name). When activities catches up later with the
    same fill but a richer Description (broker text — ``"Bought 98 NVDA
    at market"``), the dedup must collapse the two to ONE row and keep
    the activities row (richer Description).

    Same fill identity = (Date, Action, Symbol, Quantity, Price,
    Amount). Different cells: Description (thin vs rich) and
    fees_and_comm (orders-source has no fees; activities does).
    """
    df = pd.DataFrame([
        # orders-source row (sync 1, when activities lagged)
        _row(
            "Alpaca Paper Account", 6, "5/14/2026", "Buy", "NVDA",
            98, 234.026429, -22934.59,
            desc="NVIDIA Corporation",
            fees="",
        ),
        # activities-source row (sync 2, after SnapTrade indexed it)
        _row(
            "Alpaca Paper Account", 6, "5/14/2026", "Buy", "NVDA",
            98, 234.026429, -22934.59,
            desc="Bought 98 NVDA at market",  # richer broker text
            fees=0.0,
        ),
    ])
    out = _upload._dedup_history_rows(df, HISTORY_SEED_COLUMNS)
    assert len(out) == 1, f"expected 1 row after cross-source dedup, got {len(out)}"
    # Activities row wins — its Description is longer.
    kept_desc = str(out.iloc[0]["Description"])
    assert kept_desc == "Bought 98 NVDA at market", \
        f"expected activities Description to win, got {kept_desc!r}"


def test_dedup_keeps_orders_row_when_activities_not_yet_caught_up():
    """Sync 1: only the orders-source row exists (activities lag).
    The orders row must survive — it's the only signal we have."""
    df = pd.DataFrame([
        _row(
            "Alpaca Paper Account", 6, "5/14/2026", "Buy", "NVDA",
            98, 234.026429, -22934.59,
            desc="NVIDIA Corporation",
        ),
    ])
    out = _upload._dedup_history_rows(df, HISTORY_SEED_COLUMNS)
    assert len(out) == 1
    assert str(out.iloc[0]["Description"]) == "NVIDIA Corporation"


def test_dedup_does_not_collapse_distinct_trades_with_same_symbol():
    """Two BUYs of NVDA at different prices / quantities / dates are
    distinct trades — the cross-source dedup must NOT touch them.
    Identity is (Date, Action, Symbol, Quantity, Price); any of those
    five cells differing means different trades."""
    df = pd.DataFrame([
        # Same date, same action, same symbol — but different price → distinct
        _row("X", 6, "5/14/2026", "Buy", "NVDA", 98, 234.02, -22934.0, desc="Bought NVDA"),
        _row("X", 6, "5/14/2026", "Buy", "NVDA", 98, 235.50, -23079.0, desc="Bought NVDA later"),
        # Same shape but different date → distinct
        _row("X", 6, "5/15/2026", "Buy", "NVDA", 98, 234.02, -22934.0, desc="Bought NVDA Friday"),
        # Same shape but different action → distinct
        _row("X", 6, "5/14/2026", "Sell", "NVDA", 98, 234.02, 22934.0, desc="Sold NVDA"),
        # Same shape but different quantity → distinct (partial fill report)
        _row("X", 6, "5/14/2026", "Buy", "NVDA", 60, 234.02, -14041.2, desc="Partial fill 1"),
    ])
    out = _upload._dedup_history_rows(df, HISTORY_SEED_COLUMNS)
    assert len(out) == 5, f"distinct trades collapsed; expected 5 rows, got {len(out)}"


def test_dedup_collapses_orders_vs_activities_under_float_precision_drift():
    """Real risk: orders-source derives Amount = qty * exec_price at
    full precision (-22934.589242), activities-source carries the
    broker's cent-rounded Amount (-22934.59). The two genuinely differ
    by sub-cent for the same trade. The cross-source dedup must
    collapse them anyway — Amount is intentionally omitted from the
    cross-source key BECAUSE of this rounding drift. Identity is
    (Date, Action, Symbol, Quantity, Price); any two rows agreeing on
    those five cells refer to the same fill."""
    df = pd.DataFrame([
        _row("X", 6, "5/14/2026", "Buy", "NVDA",
             98, 234.026429, -22934.589242,  # broker would never report this Amount
             desc="NVIDIA Corporation"),
        _row("X", 6, "5/14/2026", "Buy", "NVDA",
             98, 234.026429, -22934.59,
             desc="Bought 98 NVDA at market"),
    ])
    out = _upload._dedup_history_rows(df, HISTORY_SEED_COLUMNS)
    assert len(out) == 1, "Amount FP drift must not defeat cross-source dedup"
    # Activities row wins — its Description is longer.
    assert str(out.iloc[0]["Description"]) == "Bought 98 NVDA at market"


def test_dedup_collapses_when_price_has_trailing_zero_drift():
    """Same trade, but orders-source string-parses Price as
    ``234.0264290000`` (10 chars from the broker) while activities
    delivers ``234.026429`` as a float. ``_canonicalize_seed_cell``
    normalizes both to ``"234.026429"`` so the cross-source key
    matches."""
    df = pd.DataFrame([
        _row("X", 6, "5/14/2026", "Buy", "NVDA",
             98, 234.0264290000, -22934.59,
             desc="NVIDIA Corporation"),
        _row("X", 6, "5/14/2026", "Buy", "NVDA",
             98, 234.026429, -22934.59,
             desc="Bought 98 NVDA at market"),
    ])
    out = _upload._dedup_history_rows(df, HISTORY_SEED_COLUMNS)
    assert len(out) == 1
    assert str(out.iloc[0]["Description"]) == "Bought 98 NVDA at market"


def test_dedup_orders_then_activities_then_resync_yields_one_row():
    """End-to-end-ish: simulate the real flow over three syncs.
    Sync 1: orders carries the trade, activities lags → 1 row.
    Sync 2: activities catches up; we re-sync; combined input has
            BOTH the existing orders-row AND the new activities-row.
    Sync 3: activities still there; orders also still there for ~30
            days; combined input has both again. Must still be 1 row.
    """
    orders_row = _row(
        "X", 6, "5/14/2026", "Buy", "NVDA",
        98, 234.026429, -22934.59,
        desc="NVIDIA Corporation",
    )
    activities_row = _row(
        "X", 6, "5/14/2026", "Buy", "NVDA",
        98, 234.026429, -22934.59,
        desc="Bought 98 NVDA at market",
        fees=0.0,
    )

    sync1 = _upload._dedup_history_rows(
        pd.DataFrame([orders_row]), HISTORY_SEED_COLUMNS,
    )
    assert len(sync1) == 1

    sync2 = _upload._dedup_history_rows(
        pd.DataFrame([orders_row, activities_row]), HISTORY_SEED_COLUMNS,
    )
    assert len(sync2) == 1
    assert str(sync2.iloc[0]["Description"]) == "Bought 98 NVDA at market"

    sync3 = _upload._dedup_history_rows(
        pd.DataFrame([orders_row, activities_row, orders_row]), HISTORY_SEED_COLUMNS,
    )
    assert len(sync3) == 1
    assert str(sync3.iloc[0]["Description"]) == "Bought 98 NVDA at market"


# ---------------------------------------------------------------------------
# v2: tenant_id tenancy (see docs/V2_TENANT_KEY_DESIGN.md)
# ---------------------------------------------------------------------------

TENANT_MYACCT = "manual:MyAcct"


def test_merge_and_push_seeds_requires_tenant_id():
    """Writer-boundary contract: refuse the push when tenant_id is missing."""
    current_df = pd.DataFrame([{
        "Symbol": "AAPL", "Description": "APPLE INC",
        "Quantity": 10.0, "Price": 200.0, "security_type": "Equity",
    }])
    ok, err, hr, cr, sha = _upload.merge_and_push_seeds(
        "Acct",
        history_df=None,
        current_df=current_df,
        commit_message="test",
        user_id=9,
        tenant_id=None,
    )
    assert ok is False
    assert "tenant_id" in (err or "").lower()
    assert hr == 0 and cr == 0 and sha is None


def test_merge_and_push_seeds_still_requires_user_id_alongside_tenant_id():
    """Sanity: tenant_id alone doesn't bypass the user_id guard."""
    current_df = pd.DataFrame([{
        "Symbol": "AAPL", "Description": "APPLE INC",
        "Quantity": 10.0, "Price": 200.0, "security_type": "Equity",
    }])
    ok, err, _hr, _cr, _sha = _upload.merge_and_push_seeds(
        "Acct",
        history_df=None,
        current_df=current_df,
        commit_message="test",
        user_id=None,
        tenant_id=TENANT_MYACCT,
    )
    assert ok is False
    assert "user_id" in (err or "").lower()


def test_prepare_seed_df_stamps_tenant_id_on_every_row():
    """The third tenancy cell (after Account, user_id) must be tenant_id."""
    df = pd.DataFrame([
        {"Symbol": "AAPL", "Quantity": 10.0, "Price": 200.0},
        {"Symbol": "MSFT", "Quantity": 5.0, "Price": 400.0},
    ])
    out = _upload._prepare_seed_df(
        df, "MyAcct", _upload.CURRENT_SEED_COLUMNS,
        user_id=9, tenant_id=TENANT_MYACCT,
    )
    assert list(out.columns[:3]) == ["Account", "user_id", "tenant_id"]
    assert out["tenant_id"].tolist() == [TENANT_MYACCT, TENANT_MYACCT]
    assert out["user_id"].astype(int).tolist() == [9, 9]
    assert out["Account"].tolist() == ["MyAcct", "MyAcct"]


def test_prepare_seed_df_empty_tenant_id_when_none():
    """Defensive: passing None for tenant_id emits an empty cell."""
    df = pd.DataFrame([{"Symbol": "AAPL", "Quantity": 1.0, "Price": 200.0}])
    out = _upload._prepare_seed_df(
        df, "MyAcct", _upload.CURRENT_SEED_COLUMNS,
        user_id=9, tenant_id=None,
    )
    assert out["tenant_id"].tolist() == [""]


def test_merge_seed_with_existing_stamps_tenant_id_via_writer(monkeypatch):
    """End-to-end shape: a new sync against an empty existing seed
    produces a CSV where every NEW row has tenant_id populated."""
    monkeypatch.setattr(_upload, "_get_file_content", lambda path: None)
    new_df = pd.DataFrame([
        _row(
            "MyAcct", 9, "06/11/2025", "Buy", "AAPL", 10, 200.0, -2000.0,
            tenant_id=TENANT_MYACCT,
            desc="APPLE",
        ),
    ])
    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "MyAcct", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_MYACCT,
    )
    out = _parse(out_csv)
    assert len(out) == 1
    assert out["tenant_id"].tolist() == [TENANT_MYACCT]


def test_dedup_collapses_legacy_null_tenant_id_against_fresh_stamp(monkeypatch):
    """Same trade re-synced after tenant registration: legacy tenant_id=""
    row and fresh tenant_id-stamped row must collapse."""
    existing = _csv_from_rows([
        _row("MyAcct", 9, "06/11/2025", "Buy", "AAPL", 10, 200.0, -2000.0,
             desc="APPLE INC"),
    ])
    _stub_existing(monkeypatch, existing)

    new_df = pd.DataFrame([
        _row(
            "MyAcct", 9, "06/11/2025", "Buy", "AAPL", 10, 200.0, -2000.0,
            tenant_id=TENANT_MYACCT,
            desc="APPLE INC",
        ),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "MyAcct", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_MYACCT,
    )
    out = _parse(out_csv)
    assert len(out) == 1, "legacy NULL vs fresh stamp must collapse to 1 row"
    assert out["tenant_id"].tolist() == [TENANT_MYACCT]


def test_other_tenants_rows_preserve_their_tenant_id_on_resync(monkeypatch):
    """Tenant isolation: a sync by tenant B must NOT rewrite tenant A's rows."""
    existing = _csv_from_rows([
        _row("Sara Investment", 2, "01/01/2025", "Buy", "AAPL", 5, 150.0, -750.0,
             tenant_id=TENANT_SARA_2, desc="APPLE INC"),
        _row("Sara Investment", 9, "02/01/2025", "Buy", "SPY", 10, 500.0, -5000.0,
             tenant_id=TENANT_SARA_9, desc="SPY ETF"),
    ])
    _stub_existing(monkeypatch, existing)

    new_df = pd.DataFrame([
        _row("Sara Investment", 9, "02/01/2025", "Buy", "SPY", 10, 500.0, -5000.0,
             tenant_id=TENANT_SARA_9, desc="SPY ETF"),
        _row("Sara Investment", 9, "03/01/2025", "Buy", "QQQ", 7, 400.0, -2800.0,
             tenant_id=TENANT_SARA_9, desc="QQQ ETF"),
    ])

    out_csv = _upload._merge_seed_with_existing(
        HISTORY_PATH, "Sara Investment", new_df, HISTORY_SEED_COLUMNS,
        tenant_id=TENANT_SARA_9,
    )
    out = _parse(out_csv)

    tenant2 = out[out["tenant_id"] == TENANT_SARA_2]
    assert len(tenant2) == 1, "tenant 2's row must survive verbatim"
    assert tenant2["tenant_id"].tolist() == [TENANT_SARA_2]

    tenant9 = out[out["tenant_id"] == TENANT_SARA_9]
    assert len(tenant9) == 2, "tenant 9 keeps SPY (deduped) + adds QQQ"
    assert set(tenant9["Symbol"].tolist()) == {"SPY", "QQQ"}


def test_normalize_tid_collapses_known_empty_forms():
    """Direct unit test for tenant_id merge-scope normalization."""
    n = _upload._normalize_tid
    assert n("snaptrade:abc-123") == "snaptrade:abc-123"
    assert n("  manual:MyAcct  ") == "manual:MyAcct"
    assert n("") == ""
    assert n("nan") == ""
    assert n(None) == ""
    assert n(float("nan")) == ""
