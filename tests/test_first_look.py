"""Regression tests for the first-data onboarding profile."""

import pandas as pd

from app import first_look
from app.first_look import (
    PROFILE_QUERY,
    STRATEGY_QUERY,
    _build_profile,
    _grouped_result_metrics,
)
from app.grouped_trades import group_book_trades


def test_strategy_query_orders_by_aggregate_alias():
    """Do not re-aggregate the SELECT alias in BigQuery's ORDER BY."""
    normalized = " ".join(STRATEGY_QUERY.split())
    assert "ORDER BY total_return DESC" in normalized
    assert "ORDER BY SUM(total_return)" not in normalized


def test_holding_days_are_weighted_by_fills():
    """A one-fill name must not weigh the same as a fifty-fill name."""
    sql = PROFILE_QUERY + STRATEGY_QUERY
    assert "avg_days_in_trade * GREATEST(num_individual_trades, 0)" in sql
    assert "AVG(avg_days_in_trade)" not in sql


def test_closed_record_keeps_independent_same_day_spreads():
    def leg(strike, direction, pnl):
        return {
            "tenant_id": "snaptrade:one",
            "account": "Broker Account",
            "symbol": "AMD",
            "strategy": "Call Spread",
            "status": "Closed",
            "trade_group_type": "option_contract",
            "trade_symbol": f"AMD   250117C{strike * 1000:08d}",
            "direction": direction,
            "open_date": "2025-01-02",
            "close_date": "2025-01-10",
            "option_expiry": "2025-01-17",
            "option_type": "Call",
            "total_pnl": pnl,
            "realized_pnl": pnl,
            "unrealized_pnl": 0,
            "days_in_trade": 8,
        }

    units = group_book_trades([
        leg(100, "Sold", 12_000),
        leg(105, "Bought", -2_000),
        leg(110, "Sold", -10_000),
        leg(115, "Bought", 1_000),
    ])
    metrics = _grouped_result_metrics(units)

    assert sorted(unit["pnl"] for unit in units) == [-9_000, 10_000]
    assert metrics["total_winners"] == 1
    assert metrics["total_losers"] == 1
    assert metrics["avg_win"] == 10_000
    assert metrics["avg_loss"] == -9_000
    assert metrics["gross_wins"] == 10_000
    assert metrics["gross_losses"] == -9_000
    assert metrics["busiest_month"] == "January 2025"
    assert metrics["busiest_month_trades"] == 2
    assert metrics["by_strategy"]["Call Spread"] == {
        "num_trades": 2,
        "num_winners": 1,
        "num_losers": 1,
    }


def test_profile_uses_fill_lots_and_tenant_filtered_grouped_results(monkeypatch):
    tenant = "snaptrade:one"
    contract = "MU    250117C00120000"
    frames = {
        "fl_profile": pd.DataFrame([{
            "num_symbols": 1, "num_strategies": 1, "total_trades": 4,
            "total_return": 150, "realized_pnl": 150, "unrealized_pnl": 0,
            "premium_collected": 500, "dividend_income": 0,
            # The old contract-grain row incorrectly looked like one win.
            "total_winners": 1, "total_losers": 0,
            "first_trade": "2025-01-02", "last_trade": "2025-01-09",
            "avg_holding_days": 7,
        }]),
        "fl_strategies": pd.DataFrame([{
            "strategy": "Naked Call", "total_return": 150,
            "winners": 1, "losers": 0, "num_symbols": 1,
            "total_trades": 4, "avg_days": 7,
        }]),
        "fl_symbols": pd.DataFrame([{
            "symbol": "MU", "total_return": 150, "strategies": "Naked Call",
        }]),
        "fl_trade_groups": pd.DataFrame([{
            "tenant_id": tenant, "account": "Broker Account", "symbol": "MU",
            "strategy": "Naked Call", "status": "Closed",
            "trade_group_type": "option_contract", "trade_symbol": contract,
            "direction": "Sold", "open_date": "2025-01-02",
            "close_date": "2025-01-09", "option_expiry": "2025-01-17",
            "option_type": "Call", "total_pnl": 150, "realized_pnl": 150,
            "unrealized_pnl": 0, "days_in_trade": 7,
        }]),
        "fl_trade_fills": pd.DataFrame([
            {"tenant_id": tenant, "account": "Broker Account",
             "trade_symbol": contract, "action": "option_sell_to_open",
             "quantity": 1, "price": 3.0, "amount": 300,
             "trade_date": "2025-01-02"},
            {"tenant_id": tenant, "account": "Broker Account",
             "trade_symbol": contract, "action": "option_sell_to_open",
             "quantity": 1, "price": 2.0, "amount": 200,
             "trade_date": "2025-01-03"},
            {"tenant_id": tenant, "account": "Broker Account",
             "trade_symbol": contract, "action": "option_buy_to_close",
             "quantity": 1, "price": 2.0, "amount": -200,
             "trade_date": "2025-01-08"},
            {"tenant_id": tenant, "account": "Broker Account",
             "trade_symbol": contract, "action": "option_buy_to_close",
             "quantity": 1, "price": 1.5, "amount": -150,
             "trade_date": "2025-01-09"},
        ]),
    }
    monkeypatch.setattr("app.routes._bq_parallel", lambda _client, _queries: frames)

    profile = _build_profile(None, "WHERE tenant_id = 'x'", "AND tenant_id = 'x'", [tenant])

    assert profile["total_winners"] == 2
    assert profile["total_losers"] == 0
    assert profile["avg_win"] == 75
    assert profile["gross_wins"] == 150
    assert profile["strategies"][0]["winners"] == 2
    assert profile["busiest_month"] == "January 2025"
    assert profile["busiest_month_trades"] == 2


def test_key_insight_uses_the_sum_of_wins_not_the_average_ratio():
    """Two $100 wins and one $300 loss is 0.67, not 0.33 from the averages."""
    out = first_look._pick_key_insight({
        "win_rate": 2 / 3,
        "profit_factor": 200 / 300,
        "avg_win": 100.0,
        "avg_loss": -300.0,
        "gross_wins": 200.0,
        "gross_losses": -300.0,
        "total_return": -100,
        "total_trades": 4,
        "num_symbols": 1,
        "num_strategies": 1,
        "avg_holding_days": 10,
        "total_winners": 2,
        "total_losers": 1,
        "strategies": [],
        "top3_concentration": 0,
    })
    assert out["title"] == "Losses add up to more than wins"
    assert "$200" in out["body"]
    assert "$300" in out["body"]
    assert "Mirror Score" not in out["body"]


def test_profile_template_hides_a_zero_win_rate_with_no_closed_results():
    from pathlib import Path
    html = Path("app/templates/first_look.html").read_text()
    assert "of closed trades" in html
    assert "term('Collected')" in html
    assert "Premium Collected" not in html
    assert "Mirror Score" not in html
