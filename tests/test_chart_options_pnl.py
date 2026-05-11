"""
Pin the options P&L sign convention used by ``_build_chart_from_daily_pnl``
on the position detail page.

The chart computes daily ``opt_pnl = cumulative_options_pnl + option_market_value``
where ``cumulative_options_pnl`` is a signed sum from ``stg_history.amount``
(positive on STO/STC, negative on BTO/BTC) and ``option_market_value`` carries
the broker's sign convention (negative for short positions = cost-to-close,
positive for long positions = current asset value). For the formula to land at
the correct net P&L on the chart's terminal point, both inputs must follow the
same convention end-to-end through ``mart_daily_pnl`` and ``stg_current``.

Production regression (May 2026, BE/Sara screenshot): the chart was rendering
options at -$10k while ``Breakdown by Type`` (which sums
``int_option_contracts.total_pnl``) said +$5,128. Root cause was upstream — a
duplicated row in ``mart_daily_pnl`` (cross-tenant price fan-out, see
``dbt/models/marts/mart_daily_pnl.sql``) doubled ``cumulative_options_pnl``.
The chart formula itself was correct; this test pins both the per-day arithmetic
and the today-row reconciliation so a future formula edit doesn't silently
re-introduce a sign or doubling bug.
"""

import pandas as pd
import pytest

from app.routes import _build_chart_from_daily_pnl


def _daily_row(date, *, options_amount=0.0, cum_options=0.0, opt_mv=None,
               equity_buy_qty=0.0, equity_buy_cost=0.0,
               equity_sell_qty=0.0, equity_sell_proceeds=0.0,
               close_price=0.0, has_trade=False):
    """Shape mirrors a single ``mart_daily_pnl`` row."""
    return {
        "account": "Sara Investment",
        "user_id": 9,
        "symbol": "BE",
        "date": date,
        "options_amount": options_amount,
        "cumulative_options_pnl": cum_options,
        "option_market_value": opt_mv,
        "option_cost_basis": None,
        "cumulative_dividends_pnl": 0.0,
        "cumulative_other_pnl": 0.0,
        "equity_buy_qty": equity_buy_qty,
        "equity_buy_cost": equity_buy_cost,
        "equity_sell_qty": equity_sell_qty,
        "equity_sell_proceeds": equity_sell_proceeds,
        "close_price": close_price if close_price > 0 else None,
        "has_trade": has_trade,
    }


def _open_short_option_current_df(market_value):
    """Open short call snapshot row — keeps the chart's
    ``position_is_closed`` flag false so quiet (no-trade) days still render."""
    return pd.DataFrame([{
        "trade_symbol": "BE    260508C00290000",
        "instrument_type": "Call",
        "quantity": -2.0,
        "market_value": market_value,
        "current_price": 20.0,
    }])


def test_short_call_open_position_chart_endpoint_reconciles():
    """STO at $30 × 2 contracts ($3000 in), price climbs to $40 ($4000 cost
    to buy back). Net P&L should be -$1000. Schwab reports short option
    market_value as NEGATIVE (cost-to-close), so option_market_value = -4000.
    Chart formula: cumulative_options_pnl=+3000 + option_market_value=-4000
    = -1000. Mirrors what ``int_option_contracts.total_pnl`` produces."""
    df = pd.DataFrame([
        _daily_row("2026-04-30", options_amount=3000.0, cum_options=3000.0,
                   has_trade=True),
        _daily_row("2026-05-08", cum_options=3000.0, opt_mv=-4000.0),
    ])

    out = _build_chart_from_daily_pnl(df, _open_short_option_current_df(-4000.0))

    # Last *mart* row before the today-row prepend is 2026-05-08 with
    # opt_pnl = 3000 + (-4000) = -1000. The function may then append a
    # today-row reconciling to current_df MV; both points must use the
    # same formula. The second-to-last point is the mart row, the last
    # is today.
    assert "2026-05-08" in out["dates"]
    idx = out["dates"].index("2026-05-08")
    assert out["options"][idx] == -1000.0, (
        f"open short call after price climb should net to -1000 "
        f"(STO 3000 + cost-to-close -4000); got {out['options'][idx]}"
    )


def test_short_call_closed_position_chart_endpoint_matches_realized():
    """Full lifecycle: STO at $30 × 2 ($3000 in), then BTC at $40 × 2
    ($4000 out). cumulative_options_pnl after BTC = -1000. After close,
    no open contract → option_market_value should be 0 / NULL → opt_pnl
    = cum_opt = -1000."""
    df = pd.DataFrame([
        _daily_row("2026-04-30", options_amount=3000.0, cum_options=3000.0,
                   has_trade=True),
        _daily_row("2026-05-01", options_amount=-4000.0, cum_options=-1000.0,
                   has_trade=True),
    ])
    out = _build_chart_from_daily_pnl(df, pd.DataFrame())
    assert out["options"] == [3000.0, -1000.0]


def test_long_call_open_position_chart_endpoint_reconciles():
    """BTO at $5 × 1 ($500 out), price climbs to $7 ($700 current MV).
    Net P&L should be +$200. Schwab reports long option market_value as
    POSITIVE (current asset value). Chart formula:
    cumulative_options_pnl=-500 + option_market_value=+700 = +200."""
    long_call_today = pd.DataFrame([{
        "trade_symbol": "BE    260508C00290000",
        "instrument_type": "Call",
        "quantity": 1.0,
        "market_value": 700.0,
        "current_price": 7.0,
    }])
    df = pd.DataFrame([
        _daily_row("2026-04-30", options_amount=-500.0, cum_options=-500.0,
                   has_trade=True),
        _daily_row("2026-05-08", cum_options=-500.0, opt_mv=700.0),
    ])
    out = _build_chart_from_daily_pnl(df, long_call_today)
    idx = out["dates"].index("2026-05-08")
    assert out["options"][idx] == 200.0


def test_chart_today_row_uses_current_df_market_value_with_correct_sign():
    """When today's date is past the last mart row, the function appends a
    today-row using ``current_df`` MV. For a short position the MV in
    ``current_df`` is also negative (Schwab convention). Test that the
    same formula (cum_opt + opt_mv) is used at the today-row, not (cum_opt -
    abs(mv)) which would double-count the loss."""
    from datetime import date

    df = pd.DataFrame([
        _daily_row("2026-04-30", options_amount=3000.0, cum_options=3000.0,
                   has_trade=True),
    ])
    today = pd.DataFrame([{
        "trade_symbol": "BE    260508C00290000",
        "instrument_type": "Call",
        "quantity": -2.0,
        "market_value": -4000.0,  # Schwab short = negative
        "current_price": 20.0,
    }])

    out = _build_chart_from_daily_pnl(df, today)

    today_str = str(date.today())
    if out["dates"][-1] == today_str:
        assert out["options"][-1] == -1000.0, (
            f"today-row option pnl must be cum_opt + mv = 3000 + (-4000) = -1000; "
            f"got {out['options'][-1]}"
        )
    else:
        assert out["options"][-1] == 3000.0


def test_chart_pre_snapshot_days_show_cash_flows_only():
    """option_market_value is NULL until the first snapshot day. Pre-snapshot
    rows must show cumulative cash flows only (premium received less premium
    paid), not zero. This was a regression vector when an earlier version of
    the function defaulted opt_mv to 0.0 and added it unconditionally."""
    df = pd.DataFrame([
        _daily_row("2026-04-30", options_amount=3000.0, cum_options=3000.0,
                   has_trade=True),
        _daily_row("2026-05-01", cum_options=3000.0, opt_mv=None),
        _daily_row("2026-05-02", cum_options=3000.0, opt_mv=None),
    ])
    out = _build_chart_from_daily_pnl(df, _open_short_option_current_df(-3000.0))
    # Pick out the three mart rows from `out` (today-row may be appended).
    for d in ("2026-04-30", "2026-05-01", "2026-05-02"):
        assert d in out["dates"], f"missing {d} in chart output"
        assert out["options"][out["dates"].index(d)] == 3000.0, (
            f"pre-snapshot day {d} must show cumulative cash flows only "
            f"(no MV component); got {out['options'][out['dates'].index(d)]}"
        )


def test_chart_options_endpoint_matches_int_option_contracts_total_pnl():
    """End-to-end alignment: the chart's terminal options value MUST equal
    the sum of ``int_option_contracts.total_pnl`` for the same scope. The
    ``Breakdown by Type`` card on the same page reads from contracts; if
    they disagree, the user sees two different "options P&L" numbers on
    one page — the original BE/Sara symptom.

    Scenario: Sara/BE user 9 — STO 285C $466, BTC 285C -$904, STO 290C
    $3004. Open 290C with current MV -$2 (per stg_current).
    Sum of contract total_pnl = (-438) + 3002 = 2564.
    Chart endpoint must equal 2564."""
    cum_options_running = 466.0           # 04/30 STO 285C
    cum_options_running += -904.0          # 05/01 BTC 285C
    cum_options_running += 3004.0          # 05/01 STO 290C
    assert cum_options_running == 2566.0   # net cash flow before mark
    df = pd.DataFrame([
        _daily_row("2026-04-30", options_amount=466.0,  cum_options=466.0,  has_trade=True),
        _daily_row("2026-05-01", options_amount=2100.0, cum_options=2566.0, has_trade=True),
        _daily_row("2026-05-08", cum_options=2566.0, opt_mv=-2.0),
    ])
    out = _build_chart_from_daily_pnl(df, _open_short_option_current_df(-2.0))

    # Sum of int_option_contracts.total_pnl = -438 + 3002 = 2564.
    expected_from_contracts = (-438) + 3002
    idx = out["dates"].index("2026-05-08")
    assert out["options"][idx] == expected_from_contracts, (
        f"chart endpoint {out['options'][idx]} must equal Σ int_option_contracts.total_pnl "
        f"({expected_from_contracts}). If this fails, either the chart formula or "
        f"int_option_contracts is wrong; the user sees two different numbers on the same page."
    )
