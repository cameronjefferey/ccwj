"""Regression tests for the first-data onboarding profile."""

from app.first_look import STRATEGY_QUERY


def test_strategy_query_orders_by_aggregate_alias():
    """Do not re-aggregate the SELECT alias in BigQuery's ORDER BY."""
    normalized = " ".join(STRATEGY_QUERY.split())
    assert "ORDER BY total_return DESC" in normalized
    assert "ORDER BY SUM(total_return)" not in normalized
