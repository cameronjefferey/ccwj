"""Pin CHECK 2 / classification contracts that kept reconcile red.

Run 33301622998 (and every scheduled reconcile since 2026-08-19) failed
CHECK 2 because closed-equity realized used session.total_pnl while the
detail page summed int_closed_equity_legs, and CHECK 12 because Closed
option contracts could have a NULL close_date.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_closed_equity_realized_reads_legs_not_session_total():
    sql = (ROOT / "dbt/models/intermediate/int_strategy_classification.sql").read_text()
    # The Closed branch must not treat e.total_pnl as realized.
    assert "when e.status = 'Closed' then e.total_pnl" not in sql
    assert "coalesce(sr.realized_pnl, 0) as realized_pnl" in sql
    assert "when e.status = 'Closed' then coalesce(sr.realized_pnl, 0)" in sql


def test_check2_options_use_realized_pnl_without_join():
    src = (ROOT / "scripts/audit/reconcile.py").read_text()
    # The old JOIN + status='Closed' + SUM(total_pnl) under-counted
    # partial-close realized and dropped rows on a COALESCE tenant join.
    opt_block = src.split("sql2_detail_opt")[1].split("sql2_detail_eq")[0]
    assert "int_option_contracts" not in opt_block
    assert "status = 'Closed'" not in opt_block
    assert "SUM(realized_pnl)" in opt_block


def test_snapshot_drop_assigns_close_date():
    sql = (ROOT / "dbt/models/intermediate/int_option_contracts.sql").read_text()
    assert "open_date < current_date('America/New_York')" in sql
    assert "then current_date('America/New_York')" in sql
