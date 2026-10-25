"""Compact cumulative-P&L chart tooltip labels.

The review sentences stay long. The chart card is a short action, the
cash that changed hands, and a running total. Net results are not called
premium, and the label does not say "collecting".
"""

import subprocess
from datetime import date
from pathlib import Path

import pandas as pd

from app.chart_tooltip import (
    clip_tips,
    format_running_number,
    format_running_total,
    format_tip_amount,
    format_tip_date,
)
from app.position_story import build_position_story

ROOT = Path(__file__).resolve().parents[1]


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


def test_clip_tips_shows_five_then_counts_the_rest():
    tips = [{"label": str(i)} for i in range(6)]
    shown, more = clip_tips(tips)
    assert [t["label"] for t in shown] == ["0", "1", "2", "3", "4"]
    assert more == 1
    shown, more = clip_tips(tips[:5])
    assert more == 0
    assert len(shown) == 5


def test_covered_call_sale_is_a_short_label_not_the_sentence():
    df = _trades([
        (date(2026, 4, 1), "equity_buy", "Equity", "BE", 200, 280.0, -56000.0),
        (date(2026, 5, 12), "option_sell_to_open", "Call",
         "BE 260626C00320000", 2, 19.545, 3909.0),
    ])
    marker = _tips_on(df, "2026-05-12")
    assert marker["when"] == format_tip_date(date(2026, 5, 12))
    assert _labels(marker) == ["Sold 2 × $320C · Jun 26"]
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
    assert _labels(marker) == ["Bought back 2 × $285C"]
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
        "Sold 2 × $320C · Jun 26",
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
    assert _labels(assigned) == ["Assigned 1 × $12P"]
    assert "amt" not in assigned["tips"][0]
    assert "premium" not in assigned["tips"][0]["label"].lower()
    # The share fill at the strike is the assignment, not a second buy.
    assert all("sh" not in lab for lab in _labels(assigned))

    expired = _tips_on(df, "2026-06-26")
    assert _labels(expired) == ["Expired 2 × $320C"]
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
    assert tip["label"] == "Rolled 1 × $11C Nov 15 → $13C Dec 20"
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


def test_compact_spxw_buyback_names_the_strike():
    """Cameron's SPXW tape is compact OCC. The card must not say '× options'."""
    df = _trades([
        (date(2025, 10, 22), "option_buy_to_close", "Call",
         "SPXW251022C06700000", 20, 3.392, -6784.0),
    ])
    marker = _tips_on(df, "2025-10-22")
    tip = marker["tips"][0]
    assert tip["label"] == "Bought back 20 × $6,700C"
    assert tip["amt"] == "−$6,784"
    assert "options" not in tip["label"].lower()
    assert "acct" not in tip


def test_compact_equity_option_open_names_strike_and_expiry():
    df = _trades([
        (date(2026, 9, 14), "option_buy_to_open", "Call",
         "NVDA260918C00220000", 1, 0.57, -57.0),
    ])
    tip = _tips_on(df, "2026-09-14")["tips"][0]
    assert tip["label"] == "Bought 1 × $220C · Sep 18"
    assert tip["amt"] == "−$57"
    assert "options" not in tip["label"].lower()


def test_long_form_same_day_spreads_collapse_before_the_clip():
    """Demo NVDA Sep 14: four long-form legs, two spreads. All of them show.

    Before the parse fix the card was 'Bought 3 × options', 'Bought 1 ×
    options', 'Sold 1 × options', '+1 more' — the fourth leg was hidden
    and none of the strikes were readable.
    """
    df = _trades([
        (date(2026, 9, 14), "option_buy_to_open", "Put",
         "NVDA 10/02/2026 200 P", 3, 2.37, -710.0),
        (date(2026, 9, 14), "option_buy_to_open", "Call",
         "NVDA 09/18/2026 220 C", 1, 0.57, -57.0),
        (date(2026, 9, 14), "option_sell_to_open", "Call",
         "NVDA 09/18/2026 215 C", 1, 1.60, 160.0),
        (date(2026, 9, 14), "option_sell_to_open", "Put",
         "NVDA 10/02/2026 205 P", 3, 3.88, 1164.0),
    ])
    marker = _tips_on(df, "2026-09-14")
    assert _labels(marker) == [
        "Opened 3 × $200/$205P · Oct 2",
        "Opened 1 × $215/$220C · Sep 18",
    ]
    assert [t["amt"] for t in marker["tips"]] == ["+$454", "+$103"]
    assert all("options" not in t["label"].lower() for t in marker["tips"])
    shown, more = clip_tips(marker["tips"])
    assert more == 0
    assert len(shown) == 2


def test_compact_roll_collapses_to_one_line_before_clip():
    # Trade dates stay on or before each contract's expiry. A close posted
    # after expiry is a settlement, not a roll.
    df = _trades([
        (date(2025, 10, 10), "option_sell_to_open", "Call",
         "SPXW251015C06700000", 20, 3.0, 6000.0),
        (date(2025, 10, 15), "option_buy_to_close", "Call",
         "SPXW251015C06700000", 20, 3.392, -6784.0),
        (date(2025, 10, 15), "option_sell_to_open", "Call",
         "SPXW251022C06800000", 20, 2.5, 5000.0),
    ])
    marker = _tips_on(df, "2025-10-15")
    assert len(marker["tips"]) == 1
    assert marker["tips"][0]["label"] == (
        "Rolled 20 × $6,700C Oct 15 → $6,800C Oct 22"
    )
    assert marker["tips"][0]["amt"] == "−$1,784"
    _shown, more = clip_tips(marker["tips"])
    assert more == 0


def test_mismatched_roll_quantities_keep_the_close_and_open_separate():
    """A 20-lot close plus a 10-lot open is not one 10-lot roll.

    Compact OCC started reaching the roll detector when tooltip parsing
    was broadened. Combining these fills labels all 20 closed contracts
    as a 10-lot roll and removes the completed 20-lot loss from the
    Trader Profile contract record.
    """
    df = _trades([
        (date(2025, 10, 10), "option_sell_to_open", "Call",
         "SPXW251015C06700000", 20, 3.0, 6000.0),
        (date(2025, 10, 15), "option_buy_to_close", "Call",
         "SPXW251015C06700000", 20, 3.392, -6784.0),
        (date(2025, 10, 15), "option_sell_to_open", "Call",
         "SPXW251022C06800000", 10, 2.5, 2500.0),
    ])

    _items, markers, stats = build_position_story(df, None)
    marker = next(m for m in markers if m["d"] == "2025-10-15")

    assert _labels(marker) == [
        "Bought back 20 × $6,700C",
        "Sold 10 × $6,800C · Oct 22",
    ]
    assert [tip["amt"] for tip in marker["tips"]] == ["−$6,784", "+$2,500"]
    assert stats["rolls"] == 0
    assert stats["contract_losses"] == 1
    assert stats["contract_loss_total"] == 784.0


def test_clip_keeps_collapsed_spreads_and_still_sends_the_rest():
    """Six raw legs collapse to two spreads plus four opens: clip after that.

    The marker keeps every tip so '+N more' can expand. The two spread
    lines are inside the five the card shows first.
    """
    rows = [
        (date(2026, 9, 14), "option_buy_to_open", "Put",
         "NVDA261002P00200000", 3, 2.0, -600.0),
        (date(2026, 9, 14), "option_sell_to_open", "Put",
         "NVDA261002P00205000", 3, 3.0, 900.0),
        (date(2026, 9, 14), "option_buy_to_open", "Call",
         "NVDA260918C00220000", 1, 0.5, -50.0),
        (date(2026, 9, 14), "option_sell_to_open", "Call",
         "NVDA260918C00215000", 1, 1.5, 150.0),
    ]
    for i, strike in enumerate((230, 240, 250, 260)):
        rows.append((
            date(2026, 9, 14), "option_buy_to_open", "Call",
            f"NVDA261016C00{strike}000", 1, 1.0, -100.0,
        ))
    marker = _tips_on(_trades(rows), "2026-09-14")
    labels = _labels(marker)
    assert len(labels) == 6  # 2 spreads + 4 leftover opens, not 8 raw fills
    assert "Opened 3 × $200/$205P · Oct 2" in labels
    assert "Opened 1 × $215/$220C · Sep 18" in labels
    assert all("options" not in lab.lower() for lab in labels)
    shown, more = clip_tips(marker["tips"])
    assert more == 1
    shown_labels = [t["label"] for t in shown]
    assert "Opened 3 × $200/$205P · Oct 2" in shown_labels
    assert "Opened 1 × $215/$220C · Sep 18" in shown_labels


def test_same_day_vertical_close_is_one_net_line():
    df = _trades([
        (date(2026, 9, 14), "option_buy_to_open", "Call",
         "NVDA260918C00220000", 1, 0.57, -57.0),
        (date(2026, 9, 14), "option_sell_to_open", "Call",
         "NVDA260918C00215000", 1, 1.60, 160.0),
        (date(2026, 9, 18), "option_sell_to_close", "Call",
         "NVDA260918C00220000", 1, 0.10, 10.0),
        (date(2026, 9, 18), "option_buy_to_close", "Call",
         "NVDA260918C00215000", 1, 0.40, -40.0),
    ])
    marker = _tips_on(df, "2026-09-18")
    assert _labels(marker) == ["Closed 1 × $215/$220C · Sep 18"]
    assert marker["tips"][0]["amt"] == "−$30"


def test_mismatched_close_size_stays_two_strike_lines():
    df = _trades([
        (date(2026, 9, 18), "option_buy_to_close", "Call",
         "NVDA260918C00215000", 20, 0.40, -800.0),
        (date(2026, 9, 18), "option_sell_to_close", "Call",
         "NVDA260918C00220000", 10, 0.10, 100.0),
    ])
    labels = _labels(_tips_on(df, "2026-09-18"))
    assert labels == [
        "Bought back 20 × $215C",
        "Sold 10 × $220C",
    ]
    assert all("options" not in lab.lower() for lab in labels)


def test_iron_condor_close_is_one_net_line():
    df = _trades([
        (date(2026, 7, 8), "option_buy_to_open", "Call",
         "DAL 260717C00095000", 10, 0.82, -817.0),
        (date(2026, 7, 8), "option_buy_to_open", "Put",
         "DAL 260717P00083000", 10, 1.75, -1747.0),
        (date(2026, 7, 8), "option_sell_to_open", "Call",
         "DAL 260717C00093000", 10, 1.14, 1143.0),
        (date(2026, 7, 8), "option_sell_to_open", "Put",
         "DAL 260717P00086000", 10, 2.90, 2903.0),
        (date(2026, 7, 10), "option_sell_to_close", "Call",
         "DAL 260717C00095000", 10, 0.40, 400.0),
        (date(2026, 7, 10), "option_sell_to_close", "Put",
         "DAL 260717P00083000", 10, 0.80, 800.0),
        (date(2026, 7, 10), "option_buy_to_close", "Call",
         "DAL 260717C00093000", 10, 0.50, -500.0),
        (date(2026, 7, 10), "option_buy_to_close", "Put",
         "DAL 260717P00086000", 10, 1.20, -1200.0),
    ])
    items, markers, _stats = build_position_story(df, None)
    close = next(i for i in items if i["date_iso"] == "2026-07-10")
    assert len(close["headlines"]) == 4
    marker = next(m for m in markers if m["d"] == "2026-07-10")
    assert _labels(marker) == ["Closed iron condor · Jul 17"]
    assert marker["tips"][0]["amt"] == "−$500"
    opened = next(m for m in markers if m["d"] == "2026-07-08")
    assert _labels(opened) == ["Opened iron condor · Jul 17"]


def test_expired_lines_name_each_contract():
    """NVDA Sep 18 was 'Expired' / 'Expired'. Each put keeps its strike."""
    df = _trades([
        (date(2026, 9, 1), "option_sell_to_open", "Put",
         "NVDA260918P00200000", 5, 1.0, 500.0),
        (date(2026, 9, 1), "option_sell_to_open", "Put",
         "NVDA260918P00205000", 5, 1.2, 600.0),
        (date(2026, 9, 18), "option_expired", "Put",
         "NVDA260918P00200000", 5, 0.0, 0.0),
        (date(2026, 9, 18), "option_expired", "Put",
         "NVDA260918P00205000", 5, 0.0, 0.0),
    ])
    long_form = _trades([
        (date(2026, 9, 1), "option_sell_to_open", "Put",
         "NVDA 09/18/2026 200 P", 5, 1.0, 500.0),
        (date(2026, 9, 1), "option_sell_to_open", "Put",
         "NVDA 09/18/2026 205 P", 5, 1.2, 600.0),
        (date(2026, 9, 18), "option_expired", "Put",
         "NVDA 09/18/2026 200 P", 5, 0.0, 0.0),
        (date(2026, 9, 18), "option_expired", "Put",
         "NVDA 09/18/2026 205 P", 5, 0.0, 0.0),
    ])
    for frame in (df, long_form):
        labels = _labels(_tips_on(frame, "2026-09-18"))
        assert labels == ["Expired 5 × $200P", "Expired 5 × $205P"]
        assert "Expired" not in labels


def test_bare_expired_lines_are_not_repeated():
    df = _trades([
        (date(2026, 9, 18), "option_expired", "Put", "", 1, 0, 0),
        (date(2026, 9, 18), "option_expired", "Put", "", 1, 0, 0),
    ])
    assert _labels(_tips_on(df, "2026-09-18")) == ["Expired"]


def test_unparsed_option_keeps_the_symbol():
    df = _trades([
        (date(2026, 5, 1), "option_buy_to_open", "Call",
         "NOT-A-CONTRACT", 2, 1.0, -50.0),
    ])
    label = _labels(_tips_on(df, "2026-05-01"))[0]
    assert label == "Bought 2 × NOT-A-CONTRACT"
    assert "options" not in label.lower()


def test_account_name_only_when_that_day_spans_two_accounts():
    df = _trades([
        (date(2026, 5, 1), "option_buy_to_open", "Call",
         "NVDA260618C00120000", 1, 1.0, -100.0, "Cameron", "t1"),
        (date(2026, 5, 2), "option_buy_to_open", "Call",
         "NVDA260618C00120000", 1, 1.2, -120.0, "Cameron", "t1"),
        (date(2026, 5, 2), "option_buy_to_open", "Put",
         "NVDA260618P00100000", 1, 0.8, -80.0, "Sara", "t2"),
    ])
    may1 = _tips_on(df, "2026-05-01")
    assert "acct" not in may1["tips"][0]
    assert "Cameron" not in may1["tips"][0]["label"]
    may2 = _tips_on(df, "2026-05-02")
    assert {t["acct"] for t in may2["tips"]} == {"Cameron", "Sara"}
    assert all(t["label"].startswith("Bought 1 × $") for t in may2["tips"])


def test_browser_tooltip_fill_resolves_marker_colors():
    """One row holds the label and the cash, and +N more expands on click."""
    script = ROOT / "app/static/js/pnl-tooltip.js"
    subprocess.run(
        [
            "node",
            "-e",
            r"""
const fs = require("fs");
const vm = require("vm");

function element() {
  return {
    children: [],
    style: {},
    className: "",
    _ls: {},
    appendChild(child) { this.children.push(child); },
    replaceChildren() { this.children = []; },
    setAttribute() {},
    removeAttribute() {},
    addEventListener(type, fn) {
      (this._ls[type] = this._ls[type] || []).push(fn);
    }
  };
}

function textOf(node) {
  if (!node) return "";
  if (node.children && node.children.length) {
    return node.children.map(textOf).join("");
  }
  return node.textContent == null ? "" : String(node.textContent);
}

global.window = {};
global.document = {
  documentElement: {},
  createElement: element,
  createTextNode(text) { return { textContent: text }; }
};
global.getComputedStyle = function () {
  return {
    getPropertyValue(name) {
      return name === "--loss" ? "#f0556d" : "#28c08a";
    }
  };
};

vm.runInThisContext(fs.readFileSync(process.argv[1], "utf8"));
const root = element();
window.htPnlTooltip.fill(root, {
  when: "May 12",
  kind: "buy",
  tips: [{ kind: "sell", label: "Sold call", amt: "\u2212$730", sign: -1 }],
  total: -730
});
if (root.children[1].children[0].style.background !== "#f0556d") {
  throw new Error("sell marker did not use the loss token");
}
const one = root.children[1];
if (one.className !== "ht-pnl-tip-row") {
  throw new Error("fill did not put the trade on one row");
}
if (root.children.some(function (c) { return c.className === "ht-pnl-tip-amt"; })) {
  throw new Error("amount is still its own card line");
}
const oneText = textOf(one);
if (oneText.indexOf("Sold call") < 0 || oneText.indexOf("\u00b7") < 0 || oneText.indexOf("\u2212$730") < 0) {
  throw new Error("one-line row missing label, middot, or amount: " + oneText);
}

const busy = element();
const tips = [];
for (var i = 1; i <= 6; i++) {
  tips.push({ kind: "buy", label: "Bought " + i + " \u00d7 $200C", amt: "\u2212$" + i, sign: -1 });
}
window.htPnlTooltip.fill(busy, { when: "September 14", kind: "buy", tips: tips, total: 3974 });
const rows = busy.children.filter(function (c) { return c.className === "ht-pnl-tip-row"; });
if (rows.length !== 5) throw new Error("expected 5 visible rows, got " + rows.length);
const more = busy.children.filter(function (c) { return c.className === "ht-pnl-tip-more"; })[0];
if (!more || textOf(more) !== "+1 more") throw new Error("missing +1 more");
if (!more._ls.click || !more._ls.mouseenter) throw new Error("+N more has no click/hover handler");
more._ls.click[0]({ preventDefault: function () {}, stopPropagation: function () {} });
const expanded = busy.children.filter(function (c) { return c.className === "ht-pnl-tip-row"; });
if (expanded.length !== 6) throw new Error("click did not expand, got " + expanded.length);
if (busy.children.some(function (c) { return c.className === "ht-pnl-tip-more"; })) {
  throw new Error("+N more stayed after expand");
}
if (!busy.className || busy.className.indexOf("is-expanded") < 0) {
  throw new Error("expanded card did not take is-expanded");
}

const hover = element();
window.htPnlTooltip.fill(hover, { when: "September 14", kind: "buy", tips: tips, total: 3974 });
const hoverMore = hover.children.filter(function (c) { return c.className === "ht-pnl-tip-more"; })[0];
hoverMore._ls.mouseenter[0]({ preventDefault: function () {}, stopPropagation: function () {} });
const hovered = hover.children.filter(function (c) { return c.className === "ht-pnl-tip-row"; });
if (hovered.length !== 6) throw new Error("hover did not expand, got " + hovered.length);
""",
            str(script),
        ],
        check=True,
    )
