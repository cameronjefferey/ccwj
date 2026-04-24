"""Unit tests for position detail merge logic (no BigQuery)."""

import pandas as pd
import pytest

from app.routes import (
    _merge_position_strategy_breakdown,
    _supplement_summary_with_rolled,
)


def _summary_row(account, strategy, status, total_pnl):
    return {
        "account": account,
        "symbol": "RDDT",
        "strategy": strategy,
        "status": status,
        "total_pnl": total_pnl,
        "realized_pnl": total_pnl if status == "Closed" else 0.0,
        "unrealized_pnl": total_pnl if status == "Open" else 0.0,
        "total_premium_received": 0.0,
        "total_premium_paid": 0.0,
        "num_trade_groups": 1,
        "num_individual_trades": 1,
        "num_winners": 1 if (status == "Closed" and total_pnl > 0) else 0,
        "num_losers": 1 if (status == "Closed" and total_pnl <= 0) else 0,
        "win_rate": 0.0,
        "avg_pnl_per_trade": total_pnl,
        "avg_days_in_trade": 0.0,
        "first_trade_date": None,
        "last_trade_date": None,
        "total_dividend_income": 0.0,
        "dividend_count": 0,
        "total_return": total_pnl,
    }


def test_merge_adds_all_closed_strategies_when_summary_empty():
    """Same shape as classification fallback: one row per closed option group."""
    closed = pd.DataFrame(
        {
            "account": ["A", "A"],
            "strategy": ["Covered Call", "Long Call"],
            "total_pnl": [100.0, -25.0],
            "premium_received": [5.0, 0.0],
            "premium_paid": [0.0, 0.0],
            "open_date": pd.to_datetime(["2023-01-01", "2024-01-01"]),
            "close_date": pd.to_datetime(["2023-02-01", "2024-02-01"]),
            "days_in_trade": [10, 20],
        }
    )
    out = _merge_position_strategy_breakdown("RDDT", pd.DataFrame(), closed, pd.DataFrame())
    assert len(out) == 2
    strats = set(out["strategy"].astype(str))
    assert strats == {"Covered Call", "Long Call"}
    assert (out["status"] == "Closed").all()
    assert abs(float(out["total_pnl"].sum()) - 75.0) < 0.01


def test_merge_skips_closed_pair_already_in_summary():
    """Extra rows are only (account, strategy) not already in positions_summary."""
    summary = pd.DataFrame(
        [
            {
                "account": "A",
                "symbol": "RDDT",
                "strategy": "Long Call",
                "status": "Open",
                "total_pnl": 50.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 50.0,
                "total_premium_received": 0.0,
                "total_premium_paid": 0.0,
                "num_trade_groups": 1,
                "num_individual_trades": 1,
                "num_winners": 0,
                "num_losers": 0,
                "win_rate": 0.0,
                "avg_pnl_per_trade": 0.0,
                "avg_days_in_trade": 0.0,
                "first_trade_date": None,
                "last_trade_date": None,
                "total_dividend_income": 0.0,
                "dividend_count": 0,
                "total_return": 50.0,
            }
        ]
    )
    closed = pd.DataFrame(
        {
            "account": ["A"],
            "strategy": ["Long Call"],
            "total_pnl": [10.0],
            "premium_received": [0.0],
            "premium_paid": [0.0],
            "open_date": pd.to_datetime(["2020-01-01"]),
            "close_date": pd.to_datetime(["2020-02-01"]),
            "days_in_trade": [5],
        }
    )
    out = _merge_position_strategy_breakdown("RDDT", summary, closed, pd.DataFrame())
    # Long Call already in summary — no duplicate row from closed_legs
    assert len(out) == 1


def test_merge_adds_different_closed_strategy_from_legs():
    summary = pd.DataFrame(
        [
            {
                "account": "A",
                "symbol": "RDDT",
                "strategy": "Long Call",
                "status": "Open",
                "total_pnl": 50.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 50.0,
                "total_premium_received": 0.0,
                "total_premium_paid": 0.0,
                "num_trade_groups": 1,
                "num_individual_trades": 1,
                "num_winners": 0,
                "num_losers": 0,
                "win_rate": 0.0,
                "avg_pnl_per_trade": 0.0,
                "avg_days_in_trade": 0.0,
                "first_trade_date": None,
                "last_trade_date": None,
                "total_dividend_income": 0.0,
                "dividend_count": 0,
                "total_return": 50.0,
            }
        ]
    )
    closed = pd.DataFrame(
        {
            "account": ["A"],
            "strategy": ["Covered Call"],
            "total_pnl": [200.0],
            "premium_received": [0.0],
            "premium_paid": [0.0],
            "open_date": pd.to_datetime(["2019-01-01"]),
            "close_date": pd.to_datetime(["2019-06-01"]),
            "days_in_trade": [100],
        }
    )
    out = _merge_position_strategy_breakdown("RDDT", summary, closed, pd.DataFrame())
    assert len(out) == 2
    assert "Covered Call" in set(out["strategy"].astype(str))


def test_supplement_adds_missing_closed_strategies_from_rollup():
    """Mart has open strategy only; classification rollup fills in a closed one."""
    summary = pd.DataFrame([_summary_row("A", "Long Call", "Open", 50.0)])
    rolled = pd.DataFrame([
        _summary_row("A", "Long Call", "Open", 50.0),
        _summary_row("A", "Wheel", "Closed", 420.0),
    ])
    out = _supplement_summary_with_rolled(summary, rolled)
    strats = set(out["strategy"].astype(str))
    assert strats == {"Long Call", "Wheel"}
    assert len(out) == 2
    wheel = out[out["strategy"] == "Wheel"].iloc[0]
    assert wheel["status"] == "Closed"
    assert float(wheel["total_pnl"]) == 420.0


def test_supplement_keeps_mart_row_when_pair_already_exists():
    """Mart rows take precedence; rollup does not override total_pnl."""
    summary = pd.DataFrame([_summary_row("A", "Long Call", "Open", 50.0)])
    rolled = pd.DataFrame([_summary_row("A", "Long Call", "Closed", 999.0)])
    out = _supplement_summary_with_rolled(summary, rolled)
    assert len(out) == 1
    r = out.iloc[0]
    assert r["status"] == "Open"
    assert float(r["total_pnl"]) == 50.0


def test_supplement_with_empty_summary_returns_rolled():
    rolled = pd.DataFrame([_summary_row("A", "Wheel", "Closed", 100.0)])
    out = _supplement_summary_with_rolled(pd.DataFrame(), rolled)
    assert len(out) == 1
    assert out.iloc[0]["strategy"] == "Wheel"


def test_supplement_with_empty_rolled_returns_summary():
    summary = pd.DataFrame([_summary_row("A", "Long Call", "Open", 10.0)])
    out = _supplement_summary_with_rolled(summary, pd.DataFrame())
    assert len(out) == 1
    assert out.iloc[0]["strategy"] == "Long Call"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
