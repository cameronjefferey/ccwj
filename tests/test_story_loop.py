"""This-week / last-week loop on the Trader Profile (app/story_loop.py)."""

from datetime import date, timedelta

import pandas as pd

from app.story_loop import (
    MIN_BASELINE_WEEKS,
    build_last_week,
    build_this_week,
    compose_story_loop,
    week_bounds,
)


_TODAY = date(2026, 8, 25)  # Tuesday
_THIS_MON, _LAST_MON, _LAST_SUN = week_bounds(_TODAY)
assert _THIS_MON == date(2026, 8, 24)
assert _LAST_MON == date(2026, 8, 17)
assert _LAST_SUN == date(2026, 8, 23)


def _open(**kw):
    base = {
        "tenant_id": "snaptrade:t1",
        "account": "Schwab Account",
        "symbol": "VICR",
        "trade_symbol": "VICR  260828P00210000",
        "option_type": "P",
        "option_strike": 210.0,
        "option_expiry": _TODAY + timedelta(days=5),
        "direction": "Sold",
        "open_date": _TODAY - timedelta(days=20),
        "premium_received": 800.0,
        "premium_paid": 0.0,
        "current_market_value": -200.0,
        "current_unrealized_pnl": 600.0,
    }
    base.update(kw)
    return base


def _exec(**kw):
    base = {
        "tenant_id": "snaptrade:t1",
        "symbol": "AAA",
        "direction": "Sold",
        "was_rolled": True,
        "close_type": "Closed",
        "gradeable_early_close": True,
        "dte_at_close": 12,
        "early_close_vs_expiry_delta": -40.0,
    }
    base.update(kw)
    return base


def _trade(d, symbol="JPM", action="option_sell_to_open", amount=100.0,
           tsym=None, tenant="snaptrade:t1"):
    return {
        "tenant_id": tenant,
        "account": "Schwab Account",
        "symbol": symbol,
        "trade_date": d,
        "action": action,
        "instrument_type": "Call",
        "trade_symbol": tsym or f"{symbol} X",
        "quantity": 1,
        "price": 1.0,
        "amount": amount,
    }


def _habit_rolls(n=8):
    """Lifetime shorts that almost always roll, typical DTE 12."""
    return pd.DataFrame([
        _exec(symbol=f"S{i}", was_rolled=True, dte_at_close=12)
        for i in range(n)
    ])


def test_week_bounds_monday_start():
    mon, last_mon, last_sun = week_bounds(date(2026, 8, 24))
    assert mon == date(2026, 8, 24)
    assert last_mon == date(2026, 8, 17)
    assert last_sun == date(2026, 8, 23)


def test_this_week_groups_put_spread_and_asks_roll_question():
    open_df = pd.DataFrame([
        _open(trade_symbol="VICR  260828P00210000", option_strike=210.0,
              direction="Sold", option_expiry=_TODAY + timedelta(days=5)),
        _open(trade_symbol="VICR  260828P00190000", option_strike=190.0,
              direction="Bought", option_expiry=_TODAY + timedelta(days=5),
              premium_received=0.0, premium_paid=-300.0),
        _open(symbol="LITE", trade_symbol="LITE  260918C01100000",
              option_type="C", option_strike=1100.0, direction="Sold",
              option_expiry=_TODAY + timedelta(days=20)),  # outside 14d
    ])
    out = build_this_week(open_df, _habit_rolls(), today=_TODAY)
    assert len(out["items"]) == 1
    item = out["items"][0]
    assert item["symbol"] == "VICR"
    assert item["structure"] == "Put Spread"
    assert item["days_left"] == 5
    assert "Roll it, or let this one expire?" in item["prompt"]
    assert "usual roll window" in out["headline"]


def test_this_week_standalone_stays_one_row():
    open_df = pd.DataFrame([
        _open(symbol="FN", trade_symbol="FN    260828C00600000",
              option_type="C", option_strike=600.0, direction="Sold",
              option_expiry=_TODAY + timedelta(days=3)),
    ])
    out = build_this_week(open_df, None, today=_TODAY)
    assert len(out["items"]) == 1
    assert out["items"][0]["structure"] is None
    assert "expires in 3 days" in out["items"][0]["prompt"]


def test_this_week_empty_when_nothing_in_horizon():
    open_df = pd.DataFrame([
        _open(option_expiry=_TODAY + timedelta(days=40)),
    ])
    out = build_this_week(open_df, None, today=_TODAY)
    assert out["items"] == []
    assert "Nothing on the clock" in out["headline"]


def test_this_week_expire_habit_does_not_nudge_a_roll():
    open_df = pd.DataFrame([_open(option_expiry=_TODAY + timedelta(days=4))])
    exec_df = pd.DataFrame([
        _exec(was_rolled=False, close_type="ExpiredOTM",
              gradeable_early_close=False, dte_at_close=0)
        for _ in range(8)
    ])
    out = build_this_week(open_df, exec_df, today=_TODAY)
    prompt = out["items"][0]["prompt"]
    assert "usually hold to expiry" in prompt
    assert "Roll it" not in prompt


def _history_with_last_week(*, last_fills, typical_fills=6, weeks=6):
    """`weeks` prior completed weeks of `typical_fills`, plus last week."""
    rows = []
    for w in range(weeks):
        start = _LAST_MON - timedelta(days=7 * (w + 1))
        for i in range(typical_fills):
            rows.append(_trade(start + timedelta(days=i % 5),
                               symbol=f"W{w}", tsym=f"W{w} {i}"))
    for i in range(last_fills):
        rows.append(_trade(_LAST_MON + timedelta(days=min(i, 4)),
                           symbol="NOW", tsym=f"NOW {i}"))
    return pd.DataFrame(rows)


def test_last_week_quiet_is_unlike_when_they_usually_trade():
    assert MIN_BASELINE_WEEKS <= 6
    trades = _history_with_last_week(last_fills=0, typical_fills=6, weeks=6)
    out = build_last_week(trades, today=_TODAY)
    assert out["tone"] == "unlike"
    assert "sat last week" in out["headline"]
    fills = next(f for f in out["facts"] if f["label"] == "Fills")
    assert fills["value"] == "0"


def test_last_week_typical_activity_looks_like_them():
    trades = _history_with_last_week(last_fills=6, typical_fills=6, weeks=6)
    out = build_last_week(trades, today=_TODAY)
    assert out["tone"] == "like"
    assert "looked like you" in out["headline"]
    fills = next(f for f in out["facts"] if f["label"] == "Fills")
    assert fills["value"] == "6"


def test_last_week_counts_a_same_day_roll():
    rows = []
    for w in range(6):
        start = _LAST_MON - timedelta(days=7 * (w + 1))
        rows.append(_trade(start, symbol="AAA", tsym="AAA A"))
    # One roll last Tuesday: BTC + STO different strikes.
    rows.append(_trade(_LAST_MON + timedelta(days=1), symbol="JPM",
                       action="option_buy_to_close", amount=-80.0,
                       tsym="JPM   260821C00300000"))
    rows.append(_trade(_LAST_MON + timedelta(days=1), symbol="JPM",
                       action="option_sell_to_open", amount=120.0,
                       tsym="JPM   260828C00305000"))
    out = build_last_week(pd.DataFrame(rows), today=_TODAY)
    assert out["activity"]["rolls"] == 1
    assert any(f["label"] == "Rolls" and f["value"] == "1" for f in out["facts"])


def test_compose_story_loop_none_when_empty():
    assert compose_story_loop(pd.DataFrame(), pd.DataFrame(),
                              today=_TODAY) is None


def test_compose_story_loop_returns_both_cards():
    open_df = pd.DataFrame([_open()])
    trades = _history_with_last_week(last_fills=4, typical_fills=4, weeks=5)
    out = compose_story_loop(trades, open_df, _habit_rolls(), today=_TODAY)
    assert out["this_week"]["items"]
    assert out["last_week"]["facts"]


def test_story_query_batch_includes_open_options():
    from app.trader_story import story_query_batch
    batch = story_query_batch(["snaptrade:t1"])
    assert "story_open_options" in batch
    assert "int_option_contracts" in batch["story_open_options"]
    assert "tenant_id" in batch["story_open_options"]
