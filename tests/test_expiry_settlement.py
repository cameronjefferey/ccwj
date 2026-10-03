"""Expiry estimates from the official close, before the broker line."""

from datetime import date, datetime

import pandas as pd

from app.execution_quality import held_to_expiry_kept
from app.expiry_settlement import (
    ESTIMATE_LABEL,
    PENDING_LABEL,
    settle_expired_option,
)

# Oct 1 SPXW 7650/7655 bear call, 10 contracts. Opens net +1,425.56
# after fees (STO 7,757.78, BTO −6,332.22). SPX above 7655 settles the
# width: −$5 × 100 × 10 = −$5,000. Spread net −$3,574.44.
_SHORT_OPEN = 7757.78
_LONG_OPEN = -6332.22
_EXPIRY = date(2026, 10, 1)
_AFTER = datetime(2026, 10, 2, 9, 30)


def _leg(**kwargs):
    base = dict(
        root="SPXW",
        option_type="C",
        direction="Sold",
        strike=7650,
        close=7700,
        quantity=10,
        net_cash_flow=_SHORT_OPEN,
        expiry=_EXPIRY,
        now_et=_AFTER,
    )
    base.update(kwargs)
    return settle_expired_option(**base)


def test_spxw_bear_call_above_the_long_strike_settles_the_width():
    short = _leg()
    long = _leg(
        direction="Bought",
        strike=7655,
        net_cash_flow=_LONG_OPEN,
    )
    assert short.settled and long.settled
    assert short.close_type == ESTIMATE_LABEL
    assert short.close_date == _EXPIRY
    assert round(short.settlement_cash + long.settlement_cash, 2) == -5000.00
    assert round(short.realized_pnl + long.realized_pnl, 2) == -3574.44
    # Any close above 7655 is the same width. 7660 is not 7700.
    other = _leg(close=7660)
    cover = _leg(direction="Bought", strike=7655, close=7660, net_cash_flow=_LONG_OPEN)
    assert round(other.settlement_cash + cover.settlement_cash, 2) == -5000.00


def test_be_covered_call_otm_today_keeps_the_premium_after_the_bell():
    expiry = date(2026, 10, 2)
    kept = settle_expired_option(
        root="BE",
        option_type="C",
        direction="Sold",
        strike=30,
        close=28.4,
        quantity=2,
        net_cash_flow=430.0,
        expiry=expiry,
        now_et=datetime(2026, 10, 2, 16, 0),
    )
    assert kept.settled
    assert kept.settlement_cash == 0.0
    assert kept.realized_pnl == 430.0
    assert kept.close_date == expiry
    assert kept.close_type == ESTIMATE_LABEL

    early = settle_expired_option(
        root="BE",
        option_type="C",
        direction="Sold",
        strike=30,
        close=28.4,
        quantity=2,
        net_cash_flow=430.0,
        expiry=expiry,
        now_et=datetime(2026, 10, 2, 15, 59),
    )
    assert not early.settled
    assert early.realized_pnl == 0.0
    assert early.close_type is None

    # Index options wait until 16:15 even when the daily bar is already in.
    index_early = _leg(
        expiry=date(2026, 10, 2),
        now_et=datetime(2026, 10, 2, 16, 14),
        close=7000,
        strike=7650,
    )
    assert not index_early.settled
    index_bell = _leg(
        expiry=date(2026, 10, 2),
        now_et=datetime(2026, 10, 2, 16, 15),
        close=7000,
        strike=7650,
    )
    assert index_bell.settled
    assert index_bell.settlement_cash == 0.0
    assert index_bell.realized_pnl == _SHORT_OPEN


def test_broker_as_of_replaces_the_estimate_without_a_second_intrinsic():
    # Statement closes: BTC 7650C 10 @16.45 = −16,450, STC 7655C 10 @11.45
    # = +11,450. Net settlement cash −5,000, already inside net_cash_flow.
    short = _leg(broker_closed=True, net_cash_flow=_SHORT_OPEN - 16450)
    long = _leg(
        direction="Bought",
        strike=7655,
        broker_closed=True,
        net_cash_flow=_LONG_OPEN + 11450,
    )
    assert short.settlement_cash == 0.0 and long.settlement_cash == 0.0
    assert not short.settled and not long.settled
    assert round(short.realized_pnl + long.realized_pnl, 2) == -3574.44
    # Adding the estimate on top of the broker cash would be −8,574.44.
    assert round(short.realized_pnl + long.realized_pnl, 2) != -8574.44


def test_missing_close_does_not_book_the_opening_credit():
    # Thursday expiry, still Thursday after the index bell. The next
    # trading day has not started, so a missing price stays open.
    missing = _leg(close=None, now_et=datetime(2026, 10, 1, 16, 30))
    assert not missing.settled
    assert missing.realized_pnl == 0.0
    assert missing.close_date is None


def test_oct_1_spx_close_above_7655_settles_the_width():
    # yfinance ^GSPC and ^SPX both closed at 7666.45 on 2026-10-01,
    # through the 7655 long strike. Width is still −$5,000.
    close = 7666.45
    assert close > 7655
    short = _leg(close=close)
    long = _leg(
        direction="Bought",
        strike=7655,
        close=close,
        net_cash_flow=_LONG_OPEN,
    )
    assert short.settled and long.settled
    assert round(short.settlement_cash + long.settlement_cash, 2) == -5000.00
    assert round(short.realized_pnl + long.realized_pnl, 2) == -3574.44


def test_standard_spx_waits_for_set_instead_of_using_daily_close():
    """Monthly SPX settles from the opening SET print, not ^GSPC close."""
    pending = _leg(root="SPX", close=7666.45)
    assert pending.settled is False
    assert pending.close_type == PENDING_LABEL
    assert pending.close_date is None
    assert pending.settlement_cash == 0.0
    assert pending.realized_pnl == 0.0


def test_missing_spxw_close_stays_pending_not_a_worthless_win():
    # Thursday Oct 1. Friday morning, still no official close. The $0
    # fallback would book the opening credit (+1,425.56) as a win.
    pending = _leg(close=None, now_et=datetime(2026, 10, 2, 9, 30))
    cover = _leg(
        direction="Bought",
        strike=7655,
        close=None,
        net_cash_flow=_LONG_OPEN,
        now_et=datetime(2026, 10, 2, 9, 30),
    )
    assert pending.close_type == PENDING_LABEL
    assert cover.close_type == PENDING_LABEL
    assert not pending.settled and not cover.settled
    assert pending.realized_pnl == 0.0 and cover.realized_pnl == 0.0
    assert pending.settlement_cash == 0.0
    assert round(pending.realized_pnl + cover.realized_pnl, 2) != 1425.56
    assert round(pending.realized_pnl + cover.realized_pnl, 2) != -3574.44


def test_missing_close_expires_at_zero_on_the_next_trading_day():
    # Friday equity expiry waits through the weekend. Monday is the
    # deadline, and equity still expires at $0. A cash index does not.
    friday = date(2026, 10, 2)
    weekend = _leg(
        close=None,
        expiry=friday,
        now_et=datetime(2026, 10, 3, 12, 0),
        strike=30,
        root="BE",
        quantity=1,
        net_cash_flow=180.0,
    )
    assert not weekend.settled
    monday = _leg(
        close=None,
        expiry=friday,
        now_et=datetime(2026, 10, 5, 9, 30),
        strike=30,
        root="BE",
        quantity=1,
        net_cash_flow=180.0,
    )
    assert monday.settled
    assert monday.settlement_cash == 0.0
    assert monday.realized_pnl == 180.0
    assert monday.close_date == friday
    assert monday.close_type == ESTIMATE_LABEL


def test_pending_spread_is_not_called_a_winner():
    from app.outcome_units import group_vertical_spreads

    short = {
        "type": "option",
        "strategy": "Call Spread",
        "trade_symbol": "SPXW  261001C07650000",
        "direction": "Sold",
        "close_type": PENDING_LABEL,
        "open_date": "2026-09-30",
        "close_date": "",
        "quantity": 10,
        "pnl": _SHORT_OPEN,
        "premium_received": _SHORT_OPEN,
        "tenant_id": "t",
        "account": "a",
        "raw_trades": [],
    }
    long = {
        **short,
        "trade_symbol": "SPXW  261001C07655000",
        "direction": "Bought",
        "pnl": _LONG_OPEN,
        "premium_received": 0,
        "premium_paid": abs(_LONG_OPEN),
    }
    grouped = group_vertical_spreads([short, long])
    assert len(grouped) == 1
    assert grouped[0]["outcome"] == PENDING_LABEL
    assert grouped[0]["is_winner"] is None


def test_itm_equity_stays_on_the_option_cash_line():
    assigned = settle_expired_option(
        root="BE",
        option_type="C",
        direction="Sold",
        strike=30,
        close=32,
        quantity=1,
        net_cash_flow=180.0,
        expiry=_EXPIRY,
        now_et=_AFTER,
    )
    assert assigned.settled
    assert assigned.settlement_cash == 0.0
    assert assigned.realized_pnl == 180.0
    assert assigned.close_type == ESTIMATE_LABEL


def test_otm_estimate_counts_as_premium_kept():
    df = pd.DataFrame([
        {
            "close_type": ESTIMATE_LABEL,
            "direction": "Sold",
            "realized_pnl": 430.0,
            "expired_worthless": True,
        },
        {
            "close_type": ESTIMATE_LABEL,
            "direction": "Sold",
            "realized_pnl": 180.0,
            "expired_worthless": False,
        },
    ])
    n, kept = held_to_expiry_kept(df)
    assert (n, kept) == (1, 430.0)
