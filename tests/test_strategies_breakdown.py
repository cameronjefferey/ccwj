"""Unit tests for /strategies breakdown-by-type aggregation (offline)."""

import math

import pandas as pd
import pytest

from app.strategies import (
    _apply_focus_symbol_count,
    _focus_breakdown_rows,
    _population_label,
    _strategy_concentration,
    _unique_symbols_by_strategy,
)


def test_focus_breakdown_empty_when_no_signals():
    df = pd.DataFrame()
    assert _focus_breakdown_rows(df, 0.0, 0) == []


def test_focus_breakdown_equity_option_and_div():
    breakdown = pd.DataFrame(
        [
            {
                "trade_group_type": "equity_session",
                "realized_sum": -126.27,
                "unrealized_sum": 50.0,
                "num_groups": 3,
                "num_open_groups": 1,
            },
            {
                "trade_group_type": "option_contract",
                "realized_sum": 400.0,
                "unrealized_sum": -300.5,
                "num_groups": 10,
                "num_open_groups": 4,
            },
        ]
    )
    rows = _focus_breakdown_rows(breakdown, dividend_total=12.34, dividend_events=8)
    by_type = {r["type"]: r for r in rows}
    eq = by_type["Equity"]
    assert eq["total"] == pytest.approx(-76.27, abs=0.001)
    assert eq["suffix"] != ""
    opts = by_type["Options"]
    assert opts["total"] == pytest.approx(99.5, abs=0.001)
    div = by_type["Dividends"]
    assert div["unrealized"] is None
    assert div["realized"] == pytest.approx(12.34, abs=0.001)
    assert "8 events" in div["suffix"]

def test_focus_breakdown_merges_other_groups():
    b = pd.DataFrame(
        [
            {
                "trade_group_type": "unknown_blob",
                "realized_sum": 1,
                "unrealized_sum": 2,
                "num_groups": 4,
                "num_open_groups": 0,
            },
            {
                "trade_group_type": "junk",
                "realized_sum": 3,
                "unrealized_sum": -1,
                "num_groups": 6,
                "num_open_groups": 6,
            },
        ]
    )
    rows = _focus_breakdown_rows(b, 0.0, 0)
    assert len(rows) == 1
    o = rows[0]
    assert o["type"] == "Other"
    assert o["total"] == 5


def test_focus_breakdown_na_dividends_treated_as_zero():
    df = pd.DataFrame(
        [
            {
                "trade_group_type": "equity_session",
                "realized_sum": math.nan,
                "unrealized_sum": math.nan,
                "num_groups": 0,
                "num_open_groups": 0,
            }
        ]
    )
    out = _focus_breakdown_rows(df, float("nan"), None)
    assert any(r["type"] == "Equity" for r in out)
    eq = next(r for r in out if r["type"] == "Equity")
    assert eq["realized"] == 0


def test_concentration_collapses_duplicate_symbols():
    df = pd.DataFrame(
        [
            {"symbol": "QTUM", "status": "Open", "total_return": 20000,
             "num_individual_trades": 2},
            {"symbol": "QTUM", "status": "Closed", "total_return": 19363.88,
             "num_individual_trades": 1},
            {"symbol": "IYW", "status": "Open", "total_return": 36158,
             "num_individual_trades": 0},
            {"symbol": "UFO", "status": "Open", "total_return": -5000,
             "num_individual_trades": 3},
        ]
    )
    out = _strategy_concentration(df, top_n=5)
    assert out["symbol_count"] == 3
    qt = next(r for r in out["contributors"] if r["symbol"] == "QTUM")
    assert qt["total_return"] == pytest.approx(39363.88)
    assert qt["status"] == "Open"
    assert qt["num_trades"] == 3
    assert out["drags"][0]["symbol"] == "UFO"
    # QTUM is more than the net book — share can exceed 100%.
    book = 20000 + 19363.88 + 36158 - 5000
    assert qt["share_pct"] == pytest.approx(round(39363.88 / book * 100, 1))


def test_population_label_splits_symbols_from_positions():
    """Audit: Covered Call said 52 symbols on the card and 39 on the detail.

    52 is account × symbol position groups (what /positions counts).
    39 is unique tickers. They must not share the word "symbols".
    """
    assert _population_label(39, 52) == "39 symbols · 52 positions"
    assert _population_label(53, 65) == "53 symbols · 65 positions"
    assert _population_label(39, 39) == "39 symbols"
    assert _population_label(1, 1) == "1 symbol"
    assert _population_label(1, 2) == "1 symbol · 2 positions"
    assert _population_label(None, 52) == "52 positions"
    assert _population_label(None, 1) == "1 position"


def test_unique_symbols_collapse_across_accounts():
    df = pd.DataFrame(
        [
            {"strategy": "Covered Call", "symbol": "AAPL", "account": "Ira"},
            {"strategy": "Covered Call", "symbol": "AAPL", "account": "Sara"},
            {"strategy": "Covered Call", "symbol": "MSFT", "account": "Ira"},
            {"strategy": "Long Call", "symbol": "NVDA", "account": "Ira"},
            {"strategy": "Covered Call", "symbol": "  ", "account": "Ira"},
        ]
    )
    counts = _unique_symbols_by_strategy(df)
    assert counts["Covered Call"] == 2
    assert counts["Long Call"] == 1
    assert _unique_symbols_by_strategy(pd.DataFrame()) is None
    assert _unique_symbols_by_strategy(pd.DataFrame({"strategy": ["Covered Call"]})) is None


def test_focus_symbol_count_rewrites_the_hero_label():
    focus = {
        "strategy": "Covered Call",
        "num_positions": 52,
        "num_symbols": None,
        "population_label": "52 positions",
    }
    _apply_focus_symbol_count(focus, 39)
    assert focus["num_symbols"] == 39
    assert focus["population_label"] == "39 symbols · 52 positions"
    _apply_focus_symbol_count({}, 3)
    _apply_focus_symbol_count(focus, None)
    assert focus["num_symbols"] == 39


def test_concentration_empty_and_nan():
    assert _strategy_concentration(pd.DataFrame())["symbol_count"] == 0
    df = pd.DataFrame(
        [{"symbol": "AAPL", "status": "Open", "total_return": float("nan"),
          "num_individual_trades": float("nan")}]
    )
    out = _strategy_concentration(df)
    assert out["contributors"] == []
    assert out["drags"] == []
    assert out["symbol_count"] == 1
    assert out["book_total"] == 0.0

