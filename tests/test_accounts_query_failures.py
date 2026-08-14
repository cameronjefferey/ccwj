"""Regression tests for partial BigQuery failures on /accounts."""

from datetime import date

import pandas as pd
import pytest

from app.accounts_page import (
    _accounts_attribution_query,
    _accounts_range_start,
)
from app.routes import _validate_accounts_financial_frames


BALANCE_COLUMNS = ["row_type", "market_value", "cost_basis"]
SUMMARY_COLUMNS = [
    "realized_pnl",
    "unrealized_pnl",
    "dividend_income",
    "total_return",
    "num_winners",
    "num_losers",
]


def _valid_frames():
    return {
        "balances": pd.DataFrame(columns=BALANCE_COLUMNS),
        "strat_summary": pd.DataFrame(columns=SUMMARY_COLUMNS),
        "trades": pd.DataFrame(columns=["account"]),
    }


@pytest.mark.parametrize("failed_query", ["balances", "strat_summary"])
def test_financial_query_failure_is_not_presented_as_zero(failed_query):
    frames = _valid_frames()
    # _bq_parallel's failure sentinel is an empty frame with no columns.
    frames[failed_query] = pd.DataFrame()

    with pytest.raises(RuntimeError, match=failed_query):
        _validate_accounts_financial_frames(frames)


def test_legitimate_zero_row_results_keep_their_schema_and_are_allowed():
    _validate_accounts_financial_frames(_valid_frames())


def test_admin_picker_rejects_failed_trades_query():
    frames = _valid_frames()
    frames["trades"] = pd.DataFrame()

    with pytest.raises(RuntimeError, match="trades"):
        _validate_accounts_financial_frames(
            frames, needs_trade_accounts=True
        )


def test_non_admin_picker_does_not_require_trade_accounts():
    frames = _valid_frames()
    frames["trades"] = pd.DataFrame()

    _validate_accounts_financial_frames(
        frames, needs_trade_accounts=False
    )


def test_breakdown_query_uses_one_lifetime_grain_for_all_financial_fields():
    """A range must not mix full open-group P&L with lifetime dividends."""
    query = _accounts_attribution_query(
        "AND tenant_id IN ('snaptrade:test-tenant')"
    )

    assert "DATE '1900-01-01'" in query or "1900-01-01" in query
    assert "snaptrade:test-tenant" in query


def test_breakdown_window_can_anchor_to_last_chart_date():
    """Active rows and chart/KPI windows must use the same terminal date."""
    assert _accounts_range_start("1M", date(2026, 8, 1)) == date(2026, 7, 2)
    assert _accounts_range_start("YTD", date(2026, 8, 1)) == date(2026, 1, 1)
