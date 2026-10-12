"""Positions and position-detail numbers that must match grouped trades."""

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def test_closed_trade_average_uses_one_unit_per_spread():
    sql = (ROOT / "dbt/models/marts/positions_summary.sql").read_text()
    runtime = (ROOT / "app/positions_page.py").read_text()
    assert "num_closed_units" in sql
    assert "closed_unit_pnl" in sql
    assert "countif(c.status = 'Closed')" not in sql
    assert "num_closed_units" in runtime
    assert "closed_unit_pnl" in runtime
    assert "COUNTIF(c.status = 'Closed')" not in runtime


def test_strategy_table_weights_average_pnl_by_closed_trades():
    from app.positions_page import strategy_rollup_frame

    frame = pd.DataFrame([
        _row(avg=200, units=1, days=10, groups=2, pnl=200),
        _row(avg=0, units=9, days=30, groups=1, pnl=0),
    ])
    rolled = strategy_rollup_frame(frame)
    assert float(rolled.iloc[0]["avg_pnl_per_trade"]) == 20
    assert abs(float(rolled.iloc[0]["avg_days_in_trade"]) - (10 * 2 + 30) / 3) < 1e-9

    bare = frame.drop(columns=["num_closed_units"])
    fallback = strategy_rollup_frame(bare)
    assert float(fallback.iloc[0]["avg_pnl_per_trade"]) == 100


def test_tag_rollup_counts_a_spread_as_one_outcome():
    from app.positions_page import _rollup_classification_rows

    rows = pd.DataFrame([
        _leg(pnl=300, expiry="2026-01-02", symbol_id="short"),
        _leg(pnl=-100, expiry="2026-01-02", symbol_id="long"),
        _leg(pnl=0, expiry="2026-02-20", symbol_id="other-short"),
        _leg(pnl=0, expiry="2026-02-20", symbol_id="other-long"),
    ])
    rolled = _rollup_classification_rows(rows)
    assert int(rolled.iloc[0]["num_winners"]) == 1
    assert int(rolled.iloc[0]["num_losers"]) == 0
    assert int(rolled.iloc[0]["num_closed_units"]) == 2
    assert float(rolled.iloc[0]["avg_pnl_per_trade"]) == 100


def _row(*, avg, units, days, groups, pnl):
    return {
        "tenant_id": "snaptrade:one",
        "account": "Schwab Account",
        "strategy": "Call Spread",
        "status": "Closed",
        "total_pnl": pnl,
        "realized_pnl": pnl,
        "unrealized_pnl": 0,
        "total_premium_received": 0,
        "total_dividend_income": 0,
        "total_return": pnl,
        "num_individual_trades": 1,
        "num_winners": 1 if pnl > 0 else 0,
        "num_losers": 1 if pnl < 0 else 0,
        "avg_pnl_per_trade": avg,
        "num_closed_units": units,
        "avg_days_in_trade": days,
        "num_trade_groups": groups,
    }


def _leg(*, pnl, expiry, symbol_id):
    return {
        "tenant_id": "snaptrade:one",
        "account": "Schwab Account",
        "user_id": 1,
        "symbol": "SPX",
        "strategy": "Call Spread",
        "status": "Closed",
        "open_date": "2026-01-02",
        "close_date": expiry,
        "option_expiry": expiry,
        "trade_symbol": symbol_id,
        "days_in_trade": 1,
        "total_pnl": pnl,
        "realized_pnl": pnl,
        "unrealized_pnl": 0,
        "num_trades": 1,
        "premium_received": max(pnl, 0),
        "premium_paid": abs(min(pnl, 0)),
        "is_winner": pnl > 0,
    }
