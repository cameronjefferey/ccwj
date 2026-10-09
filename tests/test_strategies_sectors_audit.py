"""Strategies and Sectors correctness: pooled months, pending expiry, fit cells."""

from pathlib import Path

import pandas as pd
import pytest

from app.outcome_units import annotate_contract_outcomes
from app.sectors_page import _sector_rollups
from app.strategies import (
    DTE_MONEYNESS_QUERY,
    STRATEGY_TYPE_BREAKDOWN_QUERY,
    _focus_breakdown_rows,
    dte_breakdown,
    roll_strategy_months,
)
from app.strategy_fit import STRATEGY_FIT_OPTIONS_QUERY, _build_strategy_fit_matrix


def test_monthly_win_rate_is_pooled_across_accounts():
    """A 1-trade 100% account must not count the same as a 10-trade 40% book."""
    df = pd.DataFrame([
        {
            "strategy": "Covered Call",
            "month_start": "2026-01-01",
            "trades_closed": 1,
            "num_winners": 1,
            "num_losers": 0,
            "total_pnl": 100,
            "win_rate_pct": 100,
            "avg_pnl_per_trade": 100,
        },
        {
            "strategy": "Covered Call",
            "month_start": "2026-01-01",
            "trades_closed": 10,
            "num_winners": 4,
            "num_losers": 6,
            "total_pnl": -50,
            "win_rate_pct": 40,
            "avg_pnl_per_trade": -5,
        },
    ])
    rolled = roll_strategy_months(df)
    assert len(rolled) == 1
    assert float(rolled.iloc[0]["win_rate_pct"]) == pytest.approx(5 / 11 * 100)
    assert float(rolled.iloc[0]["avg_pnl"]) == pytest.approx(50 / 11)
    assert float(rolled.iloc[0]["win_rate_pct"]) != pytest.approx(70)


def test_dte_ignores_zero_closed_open_and_pending():
    df = pd.DataFrame([
        {"dte_bucket": "0-7 DTE", "status": "Closed", "total_pnl": 100, "num_trades": 2},
        {"dte_bucket": "0-7 DTE", "status": "Closed", "total_pnl": 0, "num_trades": 1},
        {"dte_bucket": "0-7 DTE", "status": "Open", "total_pnl": -40, "num_trades": 1},
        {"dte_bucket": "0-7 DTE", "status": "Settlement pending", "total_pnl": 0, "num_trades": 1},
        {"dte_bucket": "8-30 DTE", "status": "Closed", "total_pnl": -10, "num_trades": 1},
    ])
    rows = {r["dte_bucket"]: r for r in dte_breakdown(df)}
    short = rows["0-7 DTE"]
    assert short["win_rate_pct"] == 100.0
    assert short["num_trades"] == 3
    assert short["total_pnl"] == 60
    assert rows["8-30 DTE"]["win_rate_pct"] == 0.0


def test_annotate_drops_pending_and_zero_is_neither():
    df = pd.DataFrame([
        {"status": "Closed", "total_pnl": 10},
        {"status": "Closed", "total_pnl": 0},
        {"status": "Closed", "total_pnl": -5},
        {"status": "Open", "total_pnl": 3},
        {"status": "Settlement pending", "total_pnl": 0},
    ])
    out = annotate_contract_outcomes(df)
    assert len(out) == 4
    assert "Settlement pending" not in set(out["status"])
    assert int(out["num_winners"].sum()) == 1
    assert int(out["num_losers"].sum()) == 1
    opened = out[out["status"] == "Open"]
    assert float(opened["unrealized_pnl"].iloc[0]) == 3
    assert float(opened["realized_pnl"].iloc[0]) == 0
    closed = out[out["status"] == "Closed"]
    assert float(closed["realized_pnl"].sum()) == 5


def test_breakdown_skips_a_pending_only_zero_row():
    df = pd.DataFrame([{
        "trade_group_type": "option_contract",
        "realized_sum": 0,
        "unrealized_sum": 0,
        "num_groups": 0,
        "num_open_groups": 0,
    }])
    assert _focus_breakdown_rows(df, 0, 0) == []


def test_queries_exclude_settlement_pending_and_project_tenant():
    for query in (DTE_MONEYNESS_QUERY, STRATEGY_FIT_OPTIONS_QUERY):
        assert "Settlement pending" in query
        assert "tenant_id" in query
    assert "total_pnl <= 0" not in STRATEGY_FIT_OPTIONS_QUERY
    assert "COUNTIF(status != 'Settlement pending')" in STRATEGY_TYPE_BREAKDOWN_QUERY


def _position(sector, symbol, strategy, account, pnl, winners, losers, trades=1):
    return {
        "account": account,
        "tenant_id": f"snaptrade:{account}",
        "symbol": symbol,
        "strategy": strategy,
        "status": "Closed",
        "total_pnl": pnl,
        "realized_pnl": pnl,
        "unrealized_pnl": 0.0,
        "total_return": pnl,
        "total_premium_received": 0.0,
        "total_dividend_income": 0.0,
        "num_individual_trades": trades,
        "num_winners": winners,
        "num_losers": losers,
        "sector": sector,
        "subsector": "Software",
    }


def test_fit_sector_column_totals_match_sector_rollups():
    df = pd.DataFrame([
        _position("Technology", "AAPL", "Covered Call", "Ira", 100, 1, 0),
        _position("Technology", "AAPL", "Covered Call", "Sara", -40, 0, 1),
        _position("Energy", "XOM", "Buy and Hold", "Ira", 25, 1, 0, trades=2),
    ])
    matrix = _build_strategy_fit_matrix(df, col_field="sector")
    roll = _sector_rollups(df)
    by_sector = {row["sector"]: row for row in roll["sector_rows"]}
    for sector, row in by_sector.items():
        col = matrix["col_totals"][sector]
        assert float(col["total_pnl"]) == pytest.approx(float(row["total_pnl"]))
        assert int(col["num_winners"]) == int(row["num_winners"])
        assert int(col["num_losers"]) == int(row["num_losers"])
        assert int(col["num_trades"]) == int(row["num_trades"])
    assert roll["kpis"]["num_symbols"] == 2


def test_sector_hero_counts_unique_tickers():
    df = pd.DataFrame([
        _position("Technology", "AAPL", "Covered Call", "Ira", 10, 1, 0),
        _position("Technology", "AAPL", "Covered Call", "Sara", 5, 1, 0),
    ])
    roll = _sector_rollups(df)
    assert roll["kpis"]["num_symbols"] == 1
    assert int(roll["sector_rows"][0]["num_symbols"]) == 1


def test_fit_options_frame_does_not_count_pending_as_a_loss():
    raw = pd.DataFrame([
        {
            "account": "Ira",
            "tenant_id": "snaptrade:ira",
            "symbol": "SPX",
            "strategy": "Naked Put",
            "status": "Closed",
            "dte_bucket": "0-7 DTE",
            "moneyness_at_open": "OTM",
            "total_pnl": 200,
            "num_individual_trades": 1,
        },
        {
            "account": "Ira",
            "tenant_id": "snaptrade:ira",
            "symbol": "SPX",
            "strategy": "Naked Put",
            "status": "Settlement pending",
            "dte_bucket": "0-7 DTE",
            "moneyness_at_open": "ITM",
            "total_pnl": 0,
            "num_individual_trades": 1,
        },
        {
            "account": "Ira",
            "tenant_id": "snaptrade:ira",
            "symbol": "SPX",
            "strategy": "Naked Put",
            "status": "Closed",
            "dte_bucket": "0-7 DTE",
            "moneyness_at_open": "OTM",
            "total_pnl": 0,
            "num_individual_trades": 1,
        },
    ])
    annotated = annotate_contract_outcomes(raw)
    matrix = _build_strategy_fit_matrix(annotated, col_field="dte_bucket")
    cell = matrix["cells"]["Naked Put"]["0-7 DTE"]
    assert int(cell["num_winners"]) == 1
    assert int(cell["num_losers"]) == 0
    assert int(cell["num_trades"]) == 2
    assert float(cell["total_pnl"]) == pytest.approx(200)


def test_phone_templates_keep_column_tables():
    strategies = Path("app/templates/strategies.html").read_text()
    sectors = Path("app/templates/sectors.html").read_text()
    fit = Path("app/templates/strategy_fit.html").read_text()
    for page in (strategies, sectors):
        assert "display: table" in page
        assert "content: attr(data-label)" not in page
        assert "thead { display: none" not in page
    assert 'class="ov-bar ov-bar-keep"' in strategies
    assert 'class="ov-bar ov-bar-keep"' in fit
    assert "_strategies_view_switch.html" in strategies
    assert "_strategies_view_switch.html" in fit
