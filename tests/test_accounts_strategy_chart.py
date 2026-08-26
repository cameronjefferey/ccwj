"""Strategy time chart must follow positions_summary's Dividend reclass."""
from datetime import date

import pandas as pd

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
