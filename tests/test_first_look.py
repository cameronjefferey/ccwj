"""Regression tests for the first-data onboarding profile."""

from app import first_look
from app.first_look import (
    BUSIEST_MONTH_QUERY,
    PROFILE_QUERY,
    STRATEGY_QUERY,
    WIN_LOSS_QUERY,
)


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


def test_closed_record_groups_spreads_and_drops_a_zero():
    sql = WIN_LOSS_QUERY
    assert "int_strategy_classification" in sql
    assert "ABS(unit_pnl)" not in sql or "ROUND(unit_pnl, 2)" in sql
    assert "ROUND(unit_pnl, 2) > 0" in sql
    assert "ROUND(unit_pnl, 2) < 0" in sql
    assert "positions_summary" not in sql
    busiest = BUSIEST_MONTH_QUERY
    assert "int_strategy_classification" in busiest
    assert "positions_summary" not in busiest
    assert "FORMAT_DATE('%B %Y'" in busiest


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
