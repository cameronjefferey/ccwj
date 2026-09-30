"""Compact cumulative-P&L chart tooltip labels.

The review sentences stay long. The chart card is a short action, the
cash that changed hands, and a running total. Net results are not called
premium, and the label does not say "collecting".
"""

from datetime import date

import pandas as pd

from app.chart_tooltip import (
    clip_tips,
    format_running_number,
    format_running_total,
    format_tip_amount,
    format_tip_date,
)
from app.position_story import build_position_story


def _trades(rows):
    return pd.DataFrame([
        {
            "trade_date": r[0], "action": r[1], "instrument_type": r[2],
            "trade_symbol": r[3], "quantity": r[4], "price": r[5],
            "amount": r[6], "account": r[7] if len(r) > 7 else "Schwab",
            "tenant_id": r[8] if len(r) > 8 else None,
        }
        for r in rows
    ])


def _tips_on(df, day, div=None):
    _items, markers, _stats = build_position_story(df, div)
    marker = next(m for m in markers if m["d"] == day)
    return marker


def _labels(marker):
    return [t["label"] for t in marker["tips"]]


def test_tip_date_drops_the_current_year():
    today = date(2026, 9, 30)
    assert format_tip_date(date(2026, 5, 12), today) == "May 12"
    assert format_tip_date(date(2025, 5, 12), today) == "May 12, 2025"
    assert format_tip_date(date(2026, 1, 2), today) == "January 2"


def test_tip_amount_is_signed_whole_dollars():
    assert format_tip_amount(3909) == "+$3,909"
    assert format_tip_amount(3909.4) == "+$3,909"
    assert format_tip_amount(-730) == "−$730"
    assert format_tip_amount(-730.5) == "−$731"
    assert format_tip_amount(0.4) is None
    assert format_tip_amount(0) is None
    assert format_tip_amount(None) is None


def test_running_total_rounds_and_keeps_the_sign():
    assert format_running_total(19370.21) == "Running total $19,370"
    assert format_running_number(19370.21) == "$19,370"
    assert format_running_total(-1957.4) == "Running total −$1,957"
    assert format_running_total(0) == "Running total $0"


def test_clip_tips_keeps_three_then_counts_the_rest():
    tips = [{"label": str(i)} for i in range(5)]
    shown, more = clip_tips(tips)
    assert [t["label"] for t in shown] == ["0", "1", "2"]
    assert more == 2
    shown, more = clip_tips(tips[:2])
    assert more == 0
    assert len(shown) == 2


def test_covered_call_sale_is_a_short_label_not_the_sentence():
    df = _trades([
        (date(2026, 4, 1), "equity_buy", "Equity", "BE", 200, 280.0, -56000.0),
        (date(2026, 5, 12), "option_sell_to_open", "Call",
         "BE 260626C00320000", 2, 19.545, 3909.0),
    ])
    marker = _tips_on(df, "2026-05-12")
    assert marker["when"] == format_tip_date(date(2026, 5, 12))
    assert _labels(marker) == ["Sold 2 × $320 call · Jun 26"]
    tip = marker["tips"][0]
    assert tip["amt"] == "+$3,909"
    assert tip["sign"] == 1
    assert tip["kind"] == "sell"
    # The review sentence is still the long form. The card is not.
    joined = " ".join(marker["t"])
    assert "collecting" in joined
    assert "covered call" in joined
    low = tip["label"].lower()
    assert "collecting" not in low
    assert "premium" not in low
    assert "covered" not in low


def test_buyback_amount_is_the_debit_not_the_locked_in_premium():
    df = _trades([
        (date(2026, 5, 1), "option_sell_to_open", "Call",
         "BE 260626C00285000", 2, 19.0, 3800.0),
        (date(2026, 5, 20), "option_buy_to_close", "Call",
         "BE 260626C00285000", 2, 3.65, -730.0),
    ])
    marker = _tips_on(df, "2026-05-20")
    assert _labels(marker) == ["Bought back 2 × $285 call"]
    tip = marker["tips"][0]
    assert tip["amt"] == "−$730"
    assert tip["sign"] == -1
    joined = " ".join(marker["t"]).lower()
    assert "premium" in joined
    assert "premium" not in tip["label"].lower()
    assert "collecting" not in tip["label"].lower()


def test_share_buy_and_same_day_call_stack():
    df = _trades([
        (date(2026, 5, 12), "equity_buy", "Equity", "BE", 100, 320.0, -32000.0),
        (date(2026, 5, 12), "option_sell_to_open", "Call",
         "BE 260626C00320000", 2, 19.545, 3909.0),
    ])
    marker = _tips_on(df, "2026-05-12")
    assert _labels(marker) == [
        "Bought 100 sh",
        "Sold 2 × $320 call · Jun 26",
    ]
    assert marker["tips"][0]["amt"] == "−$32,000"
    assert marker["tips"][1]["amt"] == "+$3,909"
    assert len(marker["tips"]) == 2


def test_expiry_and_assignment_do_not_repeat_premium():
    df = _trades([
        (date(2024, 6, 3), "option_sell_to_open", "Put",
         "F 240621P00012000", 1, 0.60, 60.0),
        (date(2024, 6, 21), "option_assigned", "Put",
         "F 240621P00012000", 1, None, 0.0),
        (date(2024, 6, 21), "equity_buy", "Equity", "F", 100, 12.0, -1200.0),
        (date(2026, 5, 1), "option_sell_to_open", "Call",
         "BE 260626C00320000", 2, 19.545, 3909.0),
        (date(2026, 6, 26), "option_expired", "Call",
         "BE 260626C00320000", 2, 0.0, 0.0),
    ])
    assigned = _tips_on(df, "2024-06-21")
    assert _labels(assigned) == ["Assigned · $12 put"]
    assert "amt" not in assigned["tips"][0]
    assert "premium" not in assigned["tips"][0]["label"].lower()
    # The share fill at the strike is the assignment, not a second buy.
    assert all("sh" not in lab for lab in _labels(assigned))

    expired = _tips_on(df, "2026-06-26")
    assert _labels(expired) == ["Expired · $320 call"]
    assert "amt" not in expired["tips"][0]
    assert "premium" not in expired["tips"][0]["label"].lower()
    assert "premium" in " ".join(expired["t"]).lower()


def test_roll_label_names_the_move_and_nets_the_cash():
    df = _trades([
        (date(2024, 11, 1), "option_sell_to_open", "Call",
         "RKLB 241115C00011000", 1, 0.52, 52.0),
        (date(2024, 11, 12), "option_buy_to_close", "Call",
         "RKLB 241115C00011000", 1, 2.10, -210.0),
        (date(2024, 11, 12), "option_sell_to_open", "Call",
         "RKLB 241220C00013000", 1, 2.80, 280.0),
    ])
    marker = _tips_on(df, "2024-11-12")
    assert len(marker["tips"]) == 1
    tip = marker["tips"][0]
    assert tip["label"] == "Rolled 1 × $11 Nov 15 → $13 Dec 20 call"
    assert tip["amt"] == "+$70"
    low = tip["label"].lower()
    assert "collecting" not in low
    assert "premium" not in low
    assert "credit" not in low


def test_iron_condor_is_one_line_with_net_cash():
    df = _trades([
        (date(2026, 7, 8), "option_buy_to_open", "Call",
         "DAL 260717C00095000", 10, 0.82, -817.0),
        (date(2026, 7, 8), "option_buy_to_open", "Put",
         "DAL 260717P00083000", 10, 1.75, -1747.0),
        (date(2026, 7, 8), "option_sell_to_open", "Call",
         "DAL 260717C00093000", 10, 1.14, 1143.0),
        (date(2026, 7, 8), "option_sell_to_open", "Put",
         "DAL 260717P00086000", 10, 2.90, 2903.0),
    ])
    marker = _tips_on(df, "2026-07-08")
    assert _labels(marker) == ["Opened iron condor · Jul 17"]
    assert marker["tips"][0]["amt"] == "+$1,482"
    low = marker["tips"][0]["label"].lower()
    assert "collecting" not in low
    assert "premium" not in low


def test_dividend_tip_is_the_cash_received():
    df = _trades([
        (date(2024, 5, 1), "equity_buy", "Equity", "JEPI", 200, 55.0, -11000.0),
    ])
    div = pd.DataFrame([{"trade_date": "2024-06-03", "amount": 54.12}])
    marker = _tips_on(df, "2024-06-03", div)
    assert _labels(marker) == ["Dividend"]
    assert marker["tips"][0]["amt"] == "+$54"
    assert marker["tips"][0]["kind"] == "income"


def test_multi_account_tip_carries_the_nickname():
    df = _trades([
        (date(2024, 1, 2), "equity_buy", "Equity", "X", 10, 10.0, -100.0, "Cameron 401k"),
        (date(2024, 1, 2), "equity_buy", "Equity", "X", 10, 10.0, -100.0, "Sara IRA"),
    ])
    marker = _tips_on(df, "2024-01-02")
    accts = {t["acct"] for t in marker["tips"]}
    assert accts == {"Cameron 401k", "Sara IRA"}
    assert all(t["label"] == "Bought 10 sh" for t in marker["tips"])
