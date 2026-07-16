"""Account cumulative-P&L chart trims the leading pre-first-trade flat-$0
prefix.

mart_daily_pnl ships a dense date spine; a freshly-connected account (e.g. an
Alpaca account created in late June) would otherwise render weeks of flat-$0
days back to the spine's start before the first trade. The account chart should
begin when the account actually started trading — mirrors the per-position
chart's ``position_started`` trim. See _build_account_chart_from_daily_pnl.
"""

from datetime import date

import pandas as pd

from app.routes import _build_account_chart_from_daily_pnl


def _row(d, *, buy_qty=0.0, buy_cost=0.0, close=0.0):
    return {
        "account": "Alpaca Paper Account",
        "user_id": 20,
        "tenant_id": "snaptrade:test",
        "symbol": "KALU",
        "date": d,
        "options_amount": 0.0,
        "dividends_amount": 0.0,
        "equity_buy_qty": buy_qty,
        "equity_buy_cost": buy_cost,
        "equity_sell_qty": 0.0,
        "equity_sell_proceeds": 0.0,
        "other_amount": 0.0,
        "close_price": close,
        "has_trade": buy_qty > 0,
        "cumulative_options_pnl": 0.0,
        "open_options_unrealized_pnl": 0.0,
        "cumulative_dividends_pnl": 0.0,
        "cumulative_other_pnl": 0.0,
    }


def test_leading_zero_days_before_first_trade_are_trimmed():
    rows = [
        _row(date(2026, 5, 14)),           # flat-$0, no activity
        _row(date(2026, 5, 15)),           # flat-$0, no activity
        _row(date(2026, 6, 1)),            # flat-$0, no activity
        _row(date(2026, 7, 15), buy_qty=26.0, buy_cost=4215.32, close=162.13),  # first trade
        _row(date(2026, 7, 16), close=159.69),  # held, marks to close
    ]
    out = _build_account_chart_from_daily_pnl(pd.DataFrame(rows), pd.DataFrame())

    # The three leading zero days are dropped; series starts at the first trade.
    assert out["dates"], "chart should not be empty"
    assert out["dates"][0] == "2026-07-15", (
        f"leading pre-trade zero days not trimmed: {out['dates']}"
    )
    assert "2026-05-14" not in out["dates"]
    assert "2026-06-01" not in out["dates"]
    # First rendered equity point is the first trade day's mark (26 * 162.13 - 4215.32).
    assert out["equity"][0] == round(26.0 * 162.13 - 4215.32, 2)


def test_chart_empty_when_no_activity_at_all():
    rows = [_row(date(2026, 5, 14)), _row(date(2026, 5, 15))]
    out = _build_account_chart_from_daily_pnl(pd.DataFrame(rows), pd.DataFrame())
    assert out["dates"] == []
    assert out["equity"] == []


def _short_row(d, *, sell_qty=0.0, sell_proceeds=0.0, close=0.0):
    r = _row(d, close=close)
    r["symbol"] = "SHORTY"
    r["equity_sell_qty"] = sell_qty
    r["equity_sell_proceeds"] = sell_proceeds
    r["has_trade"] = sell_qty > 0
    return r


def test_short_sale_marked_to_market_not_booked_as_full_proceeds():
    """A sale with no long inventory opens a SHORT; its P&L is
    proceeds - shares*close, NOT the full proceeds as realized gain. The
    account chart previously booked the entire proceeds (the +$46,937
    phantom-equity bug on a short-heavy day-trading account)."""
    rows = [
        # Short 100 @ $50; same-day close $50 → short P&L = 5000 - 100*50 = 0.
        _short_row(date(2026, 7, 14), sell_qty=100.0, sell_proceeds=5000.0, close=50.0),
        # Held short, price falls to $45 → short P&L = 5000 - 100*45 = +500.
        _short_row(date(2026, 7, 15), close=45.0),
        # Price rises to $60 → short P&L = 5000 - 100*60 = -1000.
        _short_row(date(2026, 7, 16), close=60.0),
    ]
    out = _build_account_chart_from_daily_pnl(pd.DataFrame(rows), pd.DataFrame())

    assert out["equity"] == [0.0, 500.0, -1000.0], (
        f"short position not marked to market: {out['equity']} "
        f"(full-proceeds bug would show ~5000)"
    )
