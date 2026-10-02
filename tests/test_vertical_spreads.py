"""SPXW verticals are one row each: credit, protection, and one win."""

import re
from pathlib import Path

from app.outcome_units import (
    annotate_strategy_structures,
    group_vertical_spreads,
    legs_activity_summary,
    vertical_display_name,
)


def _leg(**overrides):
    row = {
        "type": "option",
        "strategy": "Call Spread",
        "direction": "Sold",
        "close_type": "Closed",
        "open_date": "2026-10-01",
        "close_date": "2026-10-01",
        "days_held": 0,
        "quantity": 10,
        "quantity_display": "10",
        "cost": 0,
        "proceeds": 0,
        "pnl": 0,
        "return_pct": None,
        "is_winner": None,
        "tenant_id": "t-sara",
        "account": "Sara Investment",
        "account_display": "Sara Investment",
        "leg_num": 1,
        "raw_trades": [],
    }
    row.update(overrides)
    return row


def _spxw_six():
    """The six legs from the SPXW position page, Sara Investment.

    Spread 1: 7650C short +7770 / 7655C long -6320 = +1450.
    Spread 2: 7725C short +1428.89 / 7730C long -991.11 = +437.78.
    Spread 3: 7640P short +1803.89 / 7635P long -1366.11 = +437.78.
    Eight fills: the 7650C and the 7640P each filled in two orders.
    The calls are stamped Closed even though they expired the same day.
    """
    def fills(symbol, action, amounts):
        return [
            {
                "trade_date": "2026-10-01",
                "action": action,
                "trade_symbol": symbol,
                "quantity": 1,
                "price": 1,
                "fees": 0,
                "amount": amount,
            }
            for amount in amounts
        ]

    short_oct = "SPXW  261001C07650000"
    long_oct = "SPXW  261001C07655000"
    long_sep = "SPXW  260930C07730000"
    short_sep = "SPXW  260930C07725000"
    short_put = "SPXW  260929P07640000"
    long_put = "SPXW  260929P07635000"
    return [
        _leg(
            trade_symbol=short_oct,
            strategy="Call Spread",
            direction="Sold",
            close_type="Closed",
            open_date="2026-10-01",
            close_date="2026-10-01",
            quantity=10,
            quantity_display="10",
            cost=0,
            proceeds=7770,
            premium_received=7770,
            premium_paid=0,
            pnl=7770,
            is_winner=True,
            leg_num=3,
            raw_trades=fills(short_oct, "option_sell_to_open", [4000, 3770]),
        ),
        _leg(
            trade_symbol=long_oct,
            strategy="Call Spread",
            direction="Bought",
            close_type="Closed",
            open_date="2026-10-01",
            close_date="2026-10-01",
            quantity=10,
            quantity_display="10",
            cost=6320,
            proceeds=0,
            premium_received=0,
            premium_paid=6320,
            pnl=-6320,
            is_winner=False,
            leg_num=3,
            raw_trades=fills(long_oct, "option_buy_to_open", [-6320]),
        ),
        _leg(
            trade_symbol=long_sep,
            strategy="Call Spread",
            direction="Bought",
            close_type="Closed",
            open_date="2026-09-30",
            close_date="2026-09-30",
            quantity=5,
            quantity_display="5",
            cost=991.11,
            proceeds=0,
            premium_received=0,
            premium_paid=991.11,
            pnl=-991.11,
            is_winner=False,
            leg_num=2,
            raw_trades=fills(long_sep, "option_buy_to_open", [-991.11]),
        ),
        _leg(
            trade_symbol=short_sep,
            strategy="Call Spread",
            direction="Sold",
            close_type="Closed",
            open_date="2026-09-30",
            close_date="2026-09-30",
            quantity=5,
            quantity_display="5",
            cost=0,
            proceeds=1428.89,
            premium_received=1428.89,
            premium_paid=0,
            pnl=1428.89,
            is_winner=True,
            leg_num=2,
            raw_trades=fills(short_sep, "option_sell_to_open", [1428.89]),
        ),
        _leg(
            trade_symbol=short_put,
            strategy="Put Spread",
            direction="Sold",
            close_type="Expired",
            open_date="2026-09-29",
            close_date="2026-09-29",
            quantity=5,
            quantity_display="5",
            cost=0,
            proceeds=1803.89,
            premium_received=1803.89,
            premium_paid=0,
            pnl=1803.89,
            is_winner=True,
            leg_num=1,
            raw_trades=fills(short_put, "option_sell_to_open", [1000, 803.89]),
        ),
        _leg(
            trade_symbol=long_put,
            strategy="Put Spread",
            direction="Bought",
            close_type="Expired",
            open_date="2026-09-29",
            close_date="2026-09-29",
            quantity=5,
            quantity_display="5",
            cost=1366.11,
            proceeds=0,
            premium_received=0,
            premium_paid=1366.11,
            pnl=-1366.11,
            is_winner=False,
            leg_num=1,
            raw_trades=fills(long_put, "option_buy_to_open", [-1366.11]),
        ),
    ]


def test_six_spxw_legs_group_into_three_winning_spreads():
    grouped = group_vertical_spreads(_spxw_six())
    assert len(grouped) == 3
    assert [row["strategy"] for row in grouped] == [
        "Bear Call Credit Spread",
        "Bear Call Credit Spread",
        "Bull Put Credit Spread",
    ]
    assert [row["strategy_family"] for row in grouped] == [
        "Call Spread",
        "Call Spread",
        "Put Spread",
    ]
    assert [row["pnl"] for row in grouped] == [1450.0, 437.78, 437.78]
    assert [row["net_credit"] for row in grouped] == [1450.0, 437.78, 437.78]
    assert [row["collected"] for row in grouped] == [7770.0, 1428.89, 1803.89]
    assert [row["protection"] for row in grouped] == [6320.0, 991.11, 1366.11]
    assert [row["width_label"] for row in grouped] == ["$5 wide", "$5 wide", "$5 wide"]
    assert [row["quantity_display"] for row in grouped] == ["10", "5", "5"]
    assert [row["outcome"] for row in grouped] == ["Expired", "Expired", "Expired"]
    assert [row["is_winner"] for row in grouped] == [True, True, True]
    assert [row["leg_num"] for row in grouped] == [3, 2, 1]
    assert sum(row["pnl"] for row in grouped) == 2325.56
    fills = sum(len(row["raw_trades"]) for row in grouped)
    assert fills == 8
    assert [len(row["legs"]) for row in grouped] == [2, 2, 2]
    assert grouped[0]["legs"][0]["role_label"] == "Premium collected"
    assert grouped[0]["legs"][1]["role_label"] == "Protection"
    assert "premium" not in grouped[0]["legs"][1]["role_label"].lower()
    note = legs_activity_summary(grouped, 8)
    assert note == {
        "fills": 8,
        "contracts": 6,
        "spreads": 3,
        "partial": True,
    }
    rows = [
        {"strategy": "Call Spread", "total_pnl": 1450},
        {"strategy": "Put Spread", "total_pnl": 437.78},
    ]
    annotate_strategy_structures(rows, grouped)
    assert rows[0]["structure_name"] == "Bear Call Credit Spread"
    assert rows[1]["structure_name"] == "Bull Put Credit Spread"


def test_vertical_names_and_outcomes_that_are_not_this_book():
    assert vertical_display_name("C", 100, 105, 40) == "Bear Call Credit Spread"
    assert vertical_display_name("C", 110, 105, -40) == "Bull Call Debit Spread"
    assert vertical_display_name("P", 100, 95, 40) == "Bull Put Credit Spread"
    assert vertical_display_name("P", 95, 100, -40) == "Bear Put Debit Spread"

    closed_early = group_vertical_spreads([
        _leg(
            trade_symbol="SPXW  261001C07650000",
            direction="Sold",
            close_type="Closed",
            cost=200,
            proceeds=7770,
            premium_received=7770,
            pnl=7570,
        ),
        _leg(
            trade_symbol="SPXW  261001C07655000",
            direction="Bought",
            close_type="Closed",
            cost=6320,
            proceeds=80,
            premium_paid=6320,
            pnl=-6240,
        ),
    ])
    assert len(closed_early) == 1
    assert closed_early[0]["outcome"] == "Closed"
    assert closed_early[0]["net_credit"] == 1450

    naked = _leg(strategy="Covered Call", trade_symbol="BE", direction="Sold", pnl=10)
    assert group_vertical_spreads([naked]) == [naked]
    condor = _leg(strategy="Iron Condor", trade_symbol="SPXW  261001C07650000")
    assert group_vertical_spreads([condor, condor]) == [condor, condor]


def _legs_block():
    html = Path("app/templates/position_detail.html").read_text()
    start = html.index('<div class="pd-fold" id="pd-legs">')
    table_at = html.index(
        '<table class="table table-sm table-hover align-middle mb-0 pd-legs">',
        start,
    )
    depth = 0
    i = table_at
    while i < len(html):
        next_open = html.find("<table", i)
        next_close = html.find("</table>", i)
        if next_open != -1 and (next_close == -1 or next_open <= next_close):
            depth += 1
            i = next_open + len("<table")
            continue
        depth -= 1
        i = next_close + len("</table>")
        if depth == 0:
            return html[start:i]
    raise AssertionError("Position Legs block is not closed")


def test_spxw_legs_table_counts_spreads_and_hides_the_long_loss():
    from app import app

    grouped = group_vertical_spreads(_spxw_six())
    for row in grouped:
        row["raw_trades"] = []
    page = Path("app/templates/position_detail.html").read_text()
    assert "overflow: visible" in page
    assert "text-overflow: clip" in page
    assert "max-width: 420px" in page
    assert "min-width: max-content" in page
    source = (
        "{% set current_positions = current_positions %}"
        "{% set trade_outcomes = trade_outcomes %}"
        + _legs_block()
    )
    with app.app_context(), app.test_request_context("/"):
        html = app.jinja_env.from_string(source).render(
            current_positions=[],
            trade_outcomes=grouped,
            symbol="SPXW",
        )
    assert "3 closed" in html
    assert "3/3 winners (100%)" in html
    assert "3/6" not in html
    assert "6 closed" not in html
    assert "50%" not in html
    assert html.count('class="leg-toggle"') == 3
    assert "Bear Call Credit Spread" in html
    assert "Bull Put Credit Spread" in html
    assert "On Strategies this is Call Spread" in html
    assert "On Strategies this is Put Spread" in html
    assert html.count("Premium collected") == 3
    assert html.count("Protection") == 3
    assert html.lower().count("premium") == 3
    assert "Credit $1,450.00" in html
    assert html.count("$5 wide") >= 3
    assert "$2,325.56" in html
    assert "-$6,320.00" in html
    assert "$8,677" not in html
    assert "$11,002" not in html
    assert html.count(">Expired<") >= 3
    assert "SPXW Oct 1 &#39;26 $7650 / $7655C" in html or "SPXW Oct 1 '26 $7650 / $7655C" in html


def test_position_page_does_not_repeat_the_fill_sentence():
    """The hero already names the trade count, dates, and open status."""
    html_src = Path("app/templates/position_detail.html").read_text()
    assert "{# ── Position Narrative" not in html_src
    assert "fills across" not in html_src
    assert "individual fill" not in html_src
    assert "on closed legs" not in html_src
    assert 'class="pos-hero-stats"' in html_src
