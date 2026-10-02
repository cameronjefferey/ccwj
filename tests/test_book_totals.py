"""Positions and Strategies share one all-time book total.

The walkthrough showed /strategies at +$169,064 while its own realized
and unrealized summed to $164,786, and /positions at +$171,541. The
strategy mart drops blank-strategy rows and the old hero summed
trade-only realized + unrealized against a dividend-inclusive total.
Both heroes now call ``hero_book`` on the positions frame.
"""
import inspect

import pandas as pd
import pytest

from app.book_totals import book_totals_from_frame, hero_book, reconcile_book
from app.money import MINUS, signed_points
from app.strategies import HOLDING_STRATEGIES, trend_signal_for_strategy


def _frame():
    """Named strategies plus a blank row the strategy mart would drop."""
    return pd.DataFrame([
        {
            "strategy": "Covered Call",
            "realized_pnl": 3000.4,
            "unrealized_pnl": 100000.4,
            "total_dividend_income": 2000.2,
            "total_return": 105001.0,
        },
        {
            "strategy": "Buy and Hold",
            "realized_pnl": 2414.6,
            "unrealized_pnl": 58372.2,
            "total_dividend_income": 2103.4,
            "total_return": 62890.2,
        },
        {
            "strategy": "",
            "realized_pnl": 3650.0,
            "unrealized_pnl": -998.0,
            "total_dividend_income": 0.4,
            "total_return": 2652.4,
        },
    ])


def _page_hero(frame):
    """The four figures both heroes render."""
    book = hero_book(frame)
    return {
        "total": book["total"],
        "realized": book["realized"],
        "unrealized": book["unrealized"],
        "dividends": book["dividends"],
    }


def test_positions_and_strategies_all_time_heroes_agree():
    frame = _frame()
    positions = _page_hero(frame)
    strategies = _page_hero(frame)
    assert positions == strategies
    assert (
        positions["realized"]
        + positions["unrealized"]
        + positions["dividends"]
        == positions["total"]
    )


def test_blank_strategy_rows_stay_in_the_shared_total():
    frame = _frame()
    full = book_totals_from_frame(frame)
    named = frame[frame["strategy"].astype(str).str.strip() != ""]
    cards = book_totals_from_frame(named)
    assert full["total"] != cards["total"]
    assert full["raw_total"] - cards["raw_total"] == pytest.approx(2652.4)


def test_rounding_remainder_keeps_components_equal_to_total():
    # 9064.6 + 158372.6 + 4103.6 = 171540.8 → $171,541.
    # Rounding each part first is $1 high; the remainder lands on
    # the largest component so the page still adds up.
    book = reconcile_book(9064.6, 158372.6, 4103.6)
    assert book["total"] == 171541
    assert book["realized"] + book["unrealized"] + book["dividends"] == 171541
    assert book["dividends"] == 4104


def test_focused_strategy_includes_its_dividends():
    row = {
        "realized_pnl": 5415.4,
        "unrealized_pnl": 159371.2,
        "dividend_income": 4278.3,
    }
    book = hero_book(strategy_row=row)
    assert book["dividends"] == 4278
    assert book["realized"] + book["unrealized"] + book["dividends"] == book["total"]
    # The old hero showed realized + unrealized against total_return.
    assert book["realized"] + book["unrealized"] != book["total"]


def test_both_pages_call_the_shared_helper():
    import app.positions_page as positions_page
    import app.strategies as strategies

    positions_src = inspect.getsource(positions_page.positions)
    strategies_src = inspect.getsource(strategies.strategies)
    assert "hero_book(" in positions_src
    assert "hero_book(" in strategies_src
    assert "positions_summary" in strategies.BOOK_TOTALS_QUERY
    assert "total_dividend_income" in strategies.BOOK_TOTALS_QUERY


def test_buy_and_hold_with_a_long_record_is_not_new():
    rows = pd.DataFrame([
        {"month_start": "2024-01-01", "trend_signal": "stable", "trades_closed": 40},
        {"month_start": "2026-09-01", "trend_signal": "new", "trades_closed": 8},
        {"month_start": "2026-09-01", "trend_signal": "stable", "trades_closed": 12},
    ])
    assert trend_signal_for_strategy(rows) != "new"
    assert "Buy and Hold" in HOLDING_STRATEGIES
    assert "Dividend" in HOLDING_STRATEGIES
    assert "Crypto" in HOLDING_STRATEGIES


def test_a_short_all_new_strategy_stays_new():
    rows = pd.DataFrame([
        {"month_start": "2026-09-01", "trend_signal": "new", "trades_closed": 2},
    ])
    assert trend_signal_for_strategy(rows) == "new"


def test_insights_points_use_a_real_minus():
    rendered = signed_points(-10.3, 1)
    assert rendered == MINUS + "10.3"
    assert "-" not in rendered
    assert signed_points(10.3, 1) == "+10.3"
    assert signed_points(0, 1) == "0.0"


def test_chart_read_limit_fits_a_page_of_symbols():
    import app.position_detail as position_detail

    src = inspect.getsource(position_detail.position_chart_read)
    assert "3 per minute" not in src
    assert "30 per minute" in src
    assert "200 per hour" in src
