"""Unit tests for /strategies breakdown-by-type aggregation (offline)."""

import math

import pandas as pd
import pytest

from app.strategies import _focus_breakdown_rows


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

