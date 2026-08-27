"""Strategy time chart must follow positions_summary's Dividend reclass."""
from datetime import date

import pandas as pd
import pytest

import app.accounts_page as accounts


def _summary_row(symbol, strategy, *, divs, tenant="t1"):
    return {
        "tenant_id": tenant,
        "account": "Schwab Account",
        "symbol": symbol,
        "strategy": strategy,
        "dividend_income": divs,
    }


def _class_row(symbol, strategy, *, pnl, status="Closed", close=None, tenant="t1"):
    return {
        "tenant_id": tenant,
        "account": "Schwab Account",
        "symbol": symbol,
        "strategy": strategy,
        "status": status,
        "close_date": close,
        "total_pnl": pnl,
        "num_trades": 1,
    }


def test_apply_dividend_labels_renames_buy_and_hold_only():
    summary = pd.DataFrame([
        _summary_row("SCHD", "Dividend", divs=118.38),
        _summary_row("UFO", "Buy and Hold", divs=11.11),
    ])
    strat = pd.DataFrame([
        _class_row("SCHD", "Buy and Hold", pnl=78.43, close=date(2025, 12, 29)),
        _class_row("UFO", "Buy and Hold", pnl=1082.85, close=date(2026, 1, 2)),
        _class_row("AAPL", "Covered Call", pnl=50.0, close=date(2026, 1, 3)),
    ])
    out = accounts._apply_dividend_strategy_labels(strat, summary)
    by_sym = dict(zip(out["symbol"], out["strategy"]))
    assert by_sym["SCHD"] == "Dividend"
    assert by_sym["UFO"] == "Buy and Hold"
    assert by_sym["AAPL"] == "Covered Call"


def test_strategy_chart_splits_dividend_and_lands_cash_on_pay_dates():
    """SCHD is Buy and Hold in classification, Dividend in the summary.

    Pre-fix the $78 close fused into Buy and Hold and the $118 of coupons
    never appeared, so the chart had one line and the table had two.
    """
    summary = pd.DataFrame([
        _summary_row("SCHD", "Dividend", divs=118.38),
        _summary_row("UFO", "Buy and Hold", divs=11.11),
    ])
    strat = pd.DataFrame([
        _class_row("SCHD", "Buy and Hold", pnl=78.43, close=date(2025, 12, 29)),
        _class_row("UFO", "Buy and Hold", pnl=1082.85, close=date(2026, 1, 2)),
    ])
    strat = accounts._apply_dividend_strategy_labels(strat, summary)
    events = pd.DataFrame([
        {"tenant_id": "t1", "symbol": "SCHD", "trade_date": date(2025, 3, 31), "amount": 6.72},
        {"tenant_id": "t1", "symbol": "SCHD", "trade_date": date(2025, 6, 30), "amount": 16.46},
        {"tenant_id": "t1", "symbol": "SCHD", "trade_date": date(2025, 9, 29), "amount": 45.80},
        {"tenant_id": "t1", "symbol": "SCHD", "trade_date": date(2025, 12, 15), "amount": 49.40},
        {"tenant_id": "t1", "symbol": "UFO", "trade_date": date(2025, 6, 1), "amount": 11.11},
    ])
    chart = accounts._build_strategy_time_chart(
        strat,
        dividend_events_df=events,
        dividend_strategy_map=accounts._dividend_strategy_map(summary),
    )
    assert set(chart["series"]) == {"Buy and Hold", "Dividend"}
    assert chart["series"]["Dividend"][-1] == 196.81
    assert chart["series"]["Buy and Hold"][-1] == 1093.96
    # First non-anchor Dividend step is the March coupon, not the December close.
    first_div_idx = next(i for i, v in enumerate(chart["series"]["Dividend"]) if v)
    assert chart["dates"][first_div_idx] == "2025-03-31"
    assert chart["series"]["Dividend"][first_div_idx] == 6.72


def test_strategy_chart_without_relabel_keeps_a_single_buy_and_hold_line():
    strat = pd.DataFrame([
        _class_row("SCHD", "Buy and Hold", pnl=78.43, close=date(2025, 12, 29)),
        _class_row("UFO", "Buy and Hold", pnl=1082.85, close=date(2026, 1, 2)),
    ])
    chart = accounts._build_strategy_time_chart(strat)
    assert list(chart["series"]) == ["Buy and Hold"]
    assert chart["series"]["Buy and Hold"][-1] == 1161.28


def test_primary_strategy_map_prefers_summary_dividend_reclass():
    summary = pd.DataFrame([
        _summary_row("SCHD", "Dividend", divs=118.38) | {"total_pnl": 196.81},
        _summary_row("UFO", "Buy and Hold", divs=11.11) | {"total_pnl": 1093.96},
    ])
    strat = pd.DataFrame([
        _class_row("SCHD", "Buy and Hold", pnl=78.43, close=date(2025, 12, 29)),
        _class_row("UFO", "Buy and Hold", pnl=1082.85, close=date(2026, 1, 2)),
        _class_row("AAPL", "Covered Call", pnl=50.0, close=date(2026, 1, 3)),
    ])
    mapped = accounts._primary_strategy_map(summary, strat)
    assert mapped[("t1", "SCHD")] == "Dividend"
    assert mapped[("t1", "UFO")] == "Buy and Hold"
    assert mapped[("t1", "AAPL")] == "Covered Call"


def test_open_buy_and_hold_follows_daily_marks_not_today_dump():
    """Open lots used to dump lifetime unrealized onto today, so a
    buy-and-hold account's strategy line was flat then a vertical cliff.
    The daily-MTM series must move with the close, matching the
    cumulative Total line."""
    from app.pnl_charts import _build_account_chart_from_daily_pnl

    def _row(d, *, buy_qty=0.0, buy_cost=0.0, close=0.0, symbol="KALU"):
        return {
            "account": "Schwab Account",
            "user_id": 9,
            "tenant_id": "t1",
            "symbol": symbol,
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

    # Wed–Fri so weekend skip does not drop the marks.
    rows = [
        _row(date(2026, 7, 15), buy_qty=10.0, buy_cost=100.0, close=11.0),
        _row(date(2026, 7, 16), close=20.0),
        _row(date(2026, 7, 17), close=20.0),
    ]
    strategy_of = {("t1", "KALU"): "Buy and Hold"}
    out = _build_account_chart_from_daily_pnl(
        pd.DataFrame(rows), pd.DataFrame(), strategy_of=strategy_of,
    )
    series = out["series"]["Buy and Hold"]
    assert series[0] == 0.0
    assert series[1] == round(10.0 * 11.0 - 100.0, 2) == 10.0
    assert series[2] == round(10.0 * 20.0 - 100.0, 2) == 100.0
    assert series == out["total"]
    # The dump-on-today bug would leave the first mark at 0 and cliff at the end.
    assert series[1] != 0.0


def test_daily_strategy_series_splits_dividend_symbol_from_buy_and_hold():
    from app.pnl_charts import _build_account_chart_from_daily_pnl

    def _row(d, symbol, *, buy_qty=0.0, buy_cost=0.0, close=0.0, divs=0.0):
        return {
            "account": "Schwab Account",
            "user_id": 9,
            "tenant_id": "t1",
            "symbol": symbol,
            "date": d,
            "options_amount": 0.0,
            "dividends_amount": divs,
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

    rows = [
        _row(date(2026, 7, 15), "UFO", buy_qty=10.0, buy_cost=100.0, close=11.0),
        _row(date(2026, 7, 15), "SCHD", buy_qty=10.0, buy_cost=100.0, close=10.5, divs=5.0),
        _row(date(2026, 7, 16), "UFO", close=12.0),
        _row(date(2026, 7, 16), "SCHD", close=10.5),
    ]
    strategy_of = {("t1", "UFO"): "Buy and Hold", ("t1", "SCHD"): "Dividend"}
    out = _build_account_chart_from_daily_pnl(
        pd.DataFrame(rows), pd.DataFrame(), strategy_of=strategy_of,
    )
    assert set(out["series"]) == {"Buy and Hold", "Dividend"}
    # UFO: 10 shares, cost 100 → +10 then +20.
    assert out["series"]["Buy and Hold"][1:] == [10.0, 20.0]
    # SCHD: +5 equity + $5 coupon on day 1, then held.
    assert out["series"]["Dividend"][1] == 10.0
    assert out["series"]["Dividend"][2] == 10.0
    for i, total in enumerate(out["total"]):
        parts = sum(out["series"][s][i] for s in out["series"])
        assert parts == pytest.approx(total, abs=0.02)
