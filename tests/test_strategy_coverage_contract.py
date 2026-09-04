"""Pin open-vs-closed covered-call coverage (Sep 2026 / CCJ).

An open 100-share + 1 short call must classify as Covered Call even when
write-date fills are missing (transfer, snapshot-only lot, or stock bought
more than 3 days after the write). Closed contracts stay write-date-only
so Aug 2026 audit F2 still holds.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLASSIFICATION_SQL = (
    ROOT / "dbt/models/intermediate/int_strategy_classification.sql"
).read_text()


def _effective_coverage(*, status, write_qty, ledger_qty, snapshot_qty):
    """Mirrors option_coverage.coverage_qty in int_strategy_classification."""
    if status == "Open":
        return max(ledger_qty or 0, snapshot_qty or 0)
    return write_qty or 0


def _label_sold_call(coverage_qty, required_shares=100.0, shares_per_contract=100.0):
    if coverage_qty + 1e-6 >= required_shares:
        return "Covered Call"
    if coverage_qty + 1e-6 >= shares_per_contract:
        return "Partially Covered Call"
    return "Naked Call"


def test_open_call_uses_current_shares_not_write_date():
    # CCJ: 100 shares in the snapshot, no write-date fills.
    qty = _effective_coverage(
        status="Open", write_qty=0, ledger_qty=0, snapshot_qty=100
    )
    assert _label_sold_call(qty) == "Covered Call"


def test_open_call_uses_ledger_when_snapshot_lags():
    qty = _effective_coverage(
        status="Open", write_qty=0, ledger_qty=100, snapshot_qty=0
    )
    assert _label_sold_call(qty) == "Covered Call"


def test_open_call_becomes_naked_after_shares_sold():
    # Do NOT keep write-date coverage on an open call (reopens F2).
    qty = _effective_coverage(
        status="Open", write_qty=100, ledger_qty=0, snapshot_qty=0
    )
    assert _label_sold_call(qty) == "Naked Call"


def test_closed_call_stays_write_date_only():
    qty = _effective_coverage(
        status="Closed", write_qty=0, ledger_qty=100, snapshot_qty=100
    )
    assert _label_sold_call(qty) == "Naked Call"

    qty = _effective_coverage(
        status="Closed", write_qty=100, ledger_qty=0, snapshot_qty=0
    )
    assert _label_sold_call(qty) == "Covered Call"


def test_partial_coverage_still_distinct():
    qty = _effective_coverage(
        status="Open", write_qty=0, ledger_qty=100, snapshot_qty=100
    )
    assert _label_sold_call(qty, required_shares=200.0) == "Partially Covered Call"


def test_classification_sql_has_open_current_coverage_path():
    assert "option_coverage as (" in CLASSIFICATION_SQL
    assert "ledger_equity_qty as (" in CLASSIFICATION_SQL
    assert "snapshot_equity_qty as (" in CLASSIFICATION_SQL
    assert "left join option_coverage cov" in CLASSIFICATION_SQL
    # Write-date CTE must not INNER JOIN fills (drops snapshot-only lots).
    write_block = CLASSIFICATION_SQL.split("coverage_at_write as (")[1].split(
        "ledger_equity_qty as ("
    )[0]
    assert "left join {{ ref('int_equity_fills') }}" in write_block
    assert "\n    join {{ ref('int_equity_fills') }}" not in write_block
    # Open path uses current holdings, not greatest(write, current).
    open_block = CLASSIFICATION_SQL.split("option_coverage as (")[1].split(
        "diagonal_cover as ("
    )[0]
    assert "when oc.status = 'Open' then greatest(" in open_block
    assert "coalesce(led.qty, 0)" in open_block
    assert "coalesce(snap.qty, 0)" in open_block


def test_dbt_invariant_files_exist():
    assert (ROOT / "dbt/tests/covered_call_has_coverage_at_write.sql").is_file()
    assert (ROOT / "dbt/tests/no_open_naked_call_when_shares_cover.sql").is_file()
    naked_sql = (ROOT / "dbt/tests/no_open_naked_call_when_shares_cover.sql").read_text()
    assert "strategy = 'Naked Call'" in naked_sql
    assert "status = 'Open'" in naked_sql
