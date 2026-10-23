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
    tighten_tip_accounts,
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
    assert _labels(marker) == ["Sold 2 · $320C · Jun 26"]
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
    assert _labels(marker) == ["Bought back 2 · $285C"]
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
        "Sold 2 · $320C · Jun 26",
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
    assert _labels(assigned) == ["Assigned · $12P"]
    assert "amt" not in assigned["tips"][0]
    assert "premium" not in assigned["tips"][0]["label"].lower()
    # The share fill at the strike is the assignment, not a second buy.
    assert all("sh" not in lab for lab in _labels(assigned))

    expired = _tips_on(df, "2026-06-26")
    assert _labels(expired) == ["Expired · $320C"]
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
    assert tip["label"] == "Rolled 1 · $11C Nov 15 → $13C Dec 20"
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
    """A nickname that already fits stays. It is not rewritten."""
    df = _trades([
        (date(2024, 1, 2), "equity_buy", "Equity", "X", 10, 10.0, -100.0, "Cameron 401k"),
        (date(2024, 1, 2), "equity_buy", "Equity", "X", 10, 10.0, -100.0, "Sara IRA"),
    ])
    marker = _tips_on(df, "2024-01-02")
    accts = {t["acct"] for t in marker["tips"]}
    assert accts == {"Cameron 401k", "Sara IRA"}
    assert all(t["label"] == "Bought 10 sh" for t in marker["tips"])


def test_same_account_day_drops_the_repeated_name():
    """Every line on the card is one account, so the name is not repeated."""
    df = _trades([
        (date(2026, 10, 22), "option_buy_to_close", "Call", "SPXW",
         20, 1.0, -1200.0, "North Brokerage Account", "t-north"),
        (date(2026, 10, 22), "option_buy_to_open", "Call", "SPXW",
         20, 1.0, -4400.0, "North Brokerage Account", "t-north"),
        (date(2026, 10, 22), "option_sell_to_open", "Call", "SPXW",
         20, 1.0, 900.0, "North Brokerage Account", "t-north"),
        (date(2026, 9, 2), "equity_buy", "Equity", "SPXW",
         10, 1.0, -100.0, "South IRA", "t-south"),
    ])
    marker = _tips_on(df, "2026-10-22")
    assert _labels(marker) == ["Bought back 20", "Bought 20", "Sold 20"]
    assert all("acct" not in t for t in marker["tips"])
    assert all("options" not in t["label"].lower() for t in marker["tips"])
    assert [t["amt"] for t in marker["tips"]] == ["−$1,200", "−$4,400", "+$900"]
    joined = " ".join(marker["t"]).lower()
    assert "north" in joined  # the review sentence still names the account


def test_unparsed_option_overflow_still_counts_the_hidden_line():
    df = _trades([
        (date(2026, 10, 22), "option_buy_to_close", "Call", "SPXW",
         20, 1.0, -1200.0, "North Account"),
        (date(2026, 10, 22), "option_buy_to_open", "Call", "SPXW",
         20, 1.0, -4400.0, "North Account"),
        (date(2026, 10, 22), "option_sell_to_open", "Call", "SPXW",
         20, 1.0, 900.0, "North Account"),
        (date(2026, 10, 22), "option_sell_to_close", "Call", "SPXW",
         5, 1.0, 250.0, "North Account"),
    ])
    marker = _tips_on(df, "2026-10-22")
    assert len(marker["tips"]) == 4
    shown, more = clip_tips(marker["tips"])
    assert len(shown) == 3
    assert more == 1
    assert all("options" not in t["label"] for t in marker["tips"])


def test_mixed_long_names_shorten_and_a_bare_line_stays_bare():
    tips = [
        {"label": "Bought back 20", "acct": "North Brokerage Account"},
        {"label": "Dividend"},
        {"label": "Sold 20", "acct": "South Retirement Account"},
    ]
    tighten_tip_accounts(tips)
    assert tips[0]["acct"] == "North"
    assert "acct" not in tips[1]
    assert tips[2]["acct"] == "South"
    assert "Brokerage" not in tips[0]["acct"]


def test_colliding_long_names_keep_a_second_word():
    tips = [
        {"label": "Bought 1", "acct": "North Brokerage Account"},
        {"label": "Sold 1", "acct": "North Retirement Account"},
    ]
    tighten_tip_accounts(tips)
    assert tips[0]["acct"] == "North Bro."
    assert tips[1]["acct"] == "North Ret."


def test_privacy_masks_are_not_abbreviated():
    tips = [
        {"label": "Bought 1", "acct": "Account 1"},
        {"label": "Sold 1", "acct": "Account 2 · Paper"},
    ]
    tighten_tip_accounts(tips)
    assert tips[0]["acct"] == "Account 1"
    assert tips[1]["acct"] == "Account 2 · Paper"


def test_tooltip_year_and_card_width_are_unchanged_rules():
    """Prior-year dates keep the year. The card cannot be wider than the chart."""
    assert format_tip_date(date(2024, 3, 3), date(2026, 10, 23)) == "March 3, 2024"
    css = (ROOT / "app/templates/position_detail.html").read_text()
    assert "min(240px, calc(100% - 12px))" in css
    phone = css.split(".pd-page .pnl-chart-box", 1)[1][:600]
    assert "ht-pnl-tip" in phone
    assert "calc(100% - 12px)" in phone


def test_privacy_mask_runs_before_the_short_name(monkeypatch):
    """A mixed day shows Account N, never the nickname the mask replaced."""
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: True)
    monkeypatch.setattr(
        "app.privacy.shown_account",
        lambda name, tenant_id=None: {
            "t-north": "Account 1",
            "t-south": "Account 2",
        }.get(tenant_id, "Account"),
    )
    df = _trades([
        (date(2026, 10, 22), "option_buy_to_close", "Call", "SPXW",
         20, 1.0, -1200.0, "North Brokerage Account", "t-north"),
        (date(2026, 10, 22), "option_sell_to_open", "Put", "SPXW",
         10, 1.0, 500.0, "South IRA", "t-south"),
    ])
    marker = _tips_on(df, "2026-10-22")
    assert [t.get("acct") for t in marker["tips"]] == ["Account 1", "Account 2"]
    blob = " ".join(t["label"] for t in marker["tips"])
    assert "North" not in blob and "South" not in blob
    assert _labels(marker) == ["Bought back 20", "Sold 10"]


def test_browser_tooltip_fill_resolves_marker_colors():
    """A populated marker must not throw when the browser builds its card."""
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
    appendChild(child) { this.children.push(child); },
    replaceChildren() { this.children = []; },
    setAttribute() {},
    removeAttribute() {}
  };
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
if (window.htPnlTooltip.formatDate("2024-03-03", 2026) !== "March 3, 2024") {
  throw new Error("prior year dropped");
}
if (window.htPnlTooltip.formatDate("2026-10-22", 2026) !== "October 22") {
  throw new Error("current year should stay off the header");
}
const root = element();
window.htPnlTooltip.fill(root, {
  when: "March 3, 2024",
  kind: "buy",
  tips: [
    { kind: "buy", label: "Bought back 20", amt: "\u2212$1,200", sign: -1 },
    { kind: "buy", label: "Bought 20", amt: "\u2212$4,400", sign: -1 },
    { kind: "sell", label: "Sold 20", amt: "+$900", sign: 1, acct: "North" },
    { kind: "sell", label: "Sold 5", amt: "+$250", sign: 1 }
  ],
  total: -2500
});
if (root.children[0].textContent !== "March 3, 2024") {
  throw new Error("date header missing the year: " + root.children[0].textContent);
}
const row = root.children[1];
if (row.children[0].style.background !== "#1d4ed8") {
  throw new Error("buy marker color");
}
if (row.children.length !== 3) {
  throw new Error("amount should sit on the trade line, not under it");
}
const tail = row.children[2];
if (tail.children[0].textContent !== " \u00b7 ") {
  throw new Error("missing separator before the amount");
}
if (tail.children[1].className.indexOf("neg") < 0 || tail.children[1].textContent !== "\u2212$1,200") {
  throw new Error("signed amount was not kept: " + tail.children[1].textContent);
}
const mixed = root.children[3];
const labelText = mixed.children[1].children.map(function (n) { return n.textContent; }).join("");
if (labelText.indexOf("Sold 20") < 0 || labelText.indexOf("North") < 0) {
  throw new Error("short account missing on a mixed line: " + labelText);
}
const more = root.children[4];
if (more.textContent !== "+1 more") {
  throw new Error("overflow count: " + more.textContent);
}
const total = root.children[5];
const totalText = total.children.map(function (n) { return n.textContent; }).join("");
if (totalText.indexOf("Running total") < 0 || totalText.indexOf("\u2212$2,500") < 0) {
  throw new Error("running total: " + totalText);
}
const sell = element();
window.htPnlTooltip.fill(sell, {
  when: "May 12",
  kind: "buy",
  tips: [{ kind: "sell", label: "Sold 2 \u00b7 $320C", amt: "\u2212$730", sign: -1 }],
  total: -730
});
if (sell.children[1].children[0].style.background !== "#f0556d") {
  throw new Error("sell marker did not use the loss token");
}
const wide = { offsetWidth: 300, offsetHeight: 40 };
const pos = window.htPnlTooltip.place(wide, 180, 280, 160, 40);
if (pos.left < 0) throw new Error("tooltip left the chart: " + pos.left);
""",
            str(script),
        ],
        check=True,
    )
