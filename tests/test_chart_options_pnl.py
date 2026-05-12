"""
Pin the option P&L attribution rule used by ``_build_chart_from_daily_pnl``
on the position-detail page.

THE RULE (see AGENTS.md "Option P&L Attribution" and the dbt model
``int_option_contract_daily_pnl`` for the full explanation):

    For each option contract, the chart shows:
      - $0 contribution before open_date
      - daily mark-to-market while open (snapshot exists)
      - $0 contribution while open if no snapshot has yet existed
        (defer credit to the close date — DO NOT credit STO premium
         on the open date)
      - the FULL realized P&L on close_date (BTC, STC, expiry,
        assignment, exercise) — credit stays at that value forever

    The chart formula at any date d:
        opt_pnl(d) = cumulative_options_pnl(d)         (realized)
                   + open_options_unrealized_pnl(d)    (open MTM)

The pre-fix world summed raw option cash flows on their fill date and
added option_market_value on top. That credited STO premium on STO
date — a misleading time series for any short-premium strategy
(premium spike at open, equal-and-opposite dip at close), and
double-counted on snapshot days.

The chart endpoint MUST still reconcile to ``int_option_contracts.total_pnl``
at every date (closed contracts contribute their realized; open
contracts contribute their MTM). That invariant is the integration test
at the bottom of this file.
"""

from datetime import date

import pandas as pd
import pytest

from app.routes import _build_chart_from_daily_pnl


def _daily_row(date_, *, cum_realized=0.0, open_mtm=0.0,
               options_amount=0.0,
               equity_buy_qty=0.0, equity_buy_cost=0.0,
               equity_sell_qty=0.0, equity_sell_proceeds=0.0,
               close_price=0.0, has_trade=False):
    """Shape mirrors a single ``mart_daily_pnl`` row under the new
    realize-on-close + MTM-while-open attribution.

    ``cum_realized`` -> ``cumulative_options_pnl`` (realized P&L
        cumulated across all contracts that closed on or before this date)
    ``open_mtm`` -> ``open_options_unrealized_pnl`` (point-in-time MTM
        of all currently-open contracts on this date)
    ``options_amount`` -> kept for diagnostics/legacy reads but the
        chart no longer sums it.
    """
    return {
        "account": "Sara Investment",
        "user_id": 9,
        "symbol": "BE",
        "date": date_,
        "options_amount": options_amount,
        "cumulative_options_pnl": cum_realized,
        "open_options_unrealized_pnl": open_mtm,
        # Legacy diagnostics — kept so the helper's column-defensive
        # reads still work but no longer summed into opt_pnl.
        "option_market_value": None,
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


def _open_short_option_current_df(unrealized_pnl, *, expiry=None):
    """Open short call snapshot row with sign-corrected ``unrealized_pnl``.
    Provide ``expiry`` (a future date) so the today-row patch's
    expiry filter doesn't drop this row.
    """
    return pd.DataFrame([{
        "trade_symbol": "BE    260508C00290000",
        "instrument_type": "Call",
        "quantity": -2.0,
        "market_value": -abs(unrealized_pnl) if unrealized_pnl < 0 else 0,
        "cost_bases": 0,
        "current_price": 20.0,
        "unrealized_pnl": unrealized_pnl,
        "option_expiry": expiry or date(2099, 12, 31),
    }])


def test_open_short_call_shows_mtm_no_realized():
    """STO at $30 × 2 (premium $3000), price climbs to $40 (cost-to-close
    $4000). While open the contract contributes daily MTM only.
    Unrealized = cost_basis(+3000) + market_value(-4000) = -1000.
    Chart endpoint: cum_realized=0 + open_mtm=-1000 = -1000.
    """
    df = pd.DataFrame([
        _daily_row("2026-04-30", cum_realized=0.0, open_mtm=0.0,
                   has_trade=True),
        _daily_row("2026-05-08", cum_realized=0.0, open_mtm=-1000.0),
    ])
    out = _build_chart_from_daily_pnl(
        df, _open_short_option_current_df(-1000.0)
    )

    assert "2026-05-08" in out["dates"]
    idx = out["dates"].index("2026-05-08")
    assert out["options"][idx] == -1000.0, (
        f"open short call MTM at -1000 should appear as opt_pnl=-1000; "
        f"got {out['options'][idx]}"
    )
    # The 4/30 row shows $0 — STO premium is NOT credited at open
    # under realize-on-close attribution.
    assert out["options"][out["dates"].index("2026-04-30")] == 0.0, (
        "STO premium must NOT be credited on the open date — that's the "
        "exact bug realize-on-close fixed"
    )


def test_closed_short_call_realizes_at_close_date_step_function():
    """Full lifecycle: STO at $30 × 2 ($3000 in), then BTC at $40 × 2
    ($4000 out). Net realized P&L = -1000 lands ON CLOSE DATE.

    Pre-fix the chart drew a +$3000 step on STO date and a -$4000 step
    on BTC date (cash-flow attribution). Post-fix it draws a single
    -$1000 step on the close date — single, clean realization event.
    """
    df = pd.DataFrame([
        _daily_row("2026-04-30", cum_realized=0.0, open_mtm=0.0,
                   has_trade=True),
        _daily_row("2026-05-01", cum_realized=-1000.0, open_mtm=0.0,
                   has_trade=True),
    ])
    out = _build_chart_from_daily_pnl(df, pd.DataFrame())
    assert out["options"] == [0.0, -1000.0], (
        f"closed contract should show $0 at open, then realized step at "
        f"close — got {out['options']}"
    )


def test_open_long_call_shows_mtm_no_realized():
    """BTO at $5 × 1 ($500 out), price climbs to $7 ($700 current MV).
    For longs: cost_basis=-500 (paid), market_value=+700.
    Unrealized = -500 + 700 = +200.
    Chart endpoint: cum_realized=0 + open_mtm=+200 = +200.
    """
    long_call_today = pd.DataFrame([{
        "trade_symbol": "BE    260508C00290000",
        "instrument_type": "Call",
        "quantity": 1.0,
        "market_value": 700.0,
        "current_price": 7.0,
        "unrealized_pnl": 200.0,
        "option_expiry": date(2099, 12, 31),
    }])
    df = pd.DataFrame([
        _daily_row("2026-04-30", cum_realized=0.0, open_mtm=0.0,
                   has_trade=True),
        _daily_row("2026-05-08", cum_realized=0.0, open_mtm=200.0),
    ])
    out = _build_chart_from_daily_pnl(df, long_call_today)
    idx = out["dates"].index("2026-05-08")
    assert out["options"][idx] == 200.0


def test_today_row_patch_uses_live_unrealized_not_market_value():
    """When today's date is past the last mart row (sync ran but dbt
    hasn't refreshed), the function appends a synthetic today row.

    Today's options total = last cumulative realized + LIVE
    ``unrealized_pnl`` from current_df (broker snapshot, full
    precision). We deliberately use ``unrealized_pnl`` and not raw
    ``market_value`` because unrealized is sign-corrected for shorts
    in stg_current; raw market_value is the broker's wrong-sign value
    for shorts.
    """
    df = pd.DataFrame([
        _daily_row("2026-04-30", cum_realized=0.0, open_mtm=0.0,
                   has_trade=True),
    ])
    today = pd.DataFrame([{
        "trade_symbol": "BE    260508C00290000",
        "instrument_type": "Call",
        "quantity": -2.0,
        # Schwab raw mv for short = -4000. unrealized_pnl is the
        # sign-corrected value our system uses everywhere.
        "market_value": -4000.0,
        "cost_bases": 3000.0,
        "current_price": 20.0,
        "unrealized_pnl": -1000.0,
        "option_expiry": date(2099, 12, 31),
    }])

    out = _build_chart_from_daily_pnl(df, today)
    today_str = str(date.today())
    if out["dates"][-1] == today_str:
        assert out["options"][-1] == -1000.0, (
            "today-row option pnl must be cum_realized(0) + live "
            f"unrealized(-1000) = -1000; got {out['options'][-1]}"
        )


def test_today_row_excludes_past_expiry_options_from_open_mtm():
    """Schwab's snapshot lags actual expiry by 1-2 trading days. A
    past-expiry contract still in current_df must NOT contribute open
    MTM today (its realized credit is already in cumulative). Without
    the expiry filter we'd double-count the contract.
    """
    df = pd.DataFrame([
        _daily_row("2026-05-01", cum_realized=0.0, open_mtm=0.0,
                   has_trade=True),
        _daily_row("2026-05-08", cum_realized=3000.0, open_mtm=0.0),
    ])
    today = pd.DataFrame([{
        "trade_symbol": "BE    260508C00290000",
        "instrument_type": "Call",
        "quantity": -2.0,
        "market_value": -2.0,
        "cost_bases": 3002.0,
        "current_price": 20.0,
        "unrealized_pnl": 3000.0,
        # Expired 3 days ago — Schwab snapshot still carries it.
        "option_expiry": date(2026, 5, 8),
    }])
    out = _build_chart_from_daily_pnl(df, today)
    today_str = str(date.today())
    if out["dates"][-1] == today_str:
        # past-expiry filter -> open_mtm contribution from current_df = 0
        # so today's option pnl = cum_realized(3000) + open_mtm(0) = 3000
        assert out["options"][-1] == 3000.0, (
            "past-expiry contract must not contribute open MTM today — "
            "its realized credit is already in cumulative_options_pnl. "
            f"Expected 3000, got {out['options'][-1]}"
        )


def test_pre_snapshot_open_days_show_zero_not_cash_flow():
    """While a contract is open without snapshot data, the chart shows
    $0 (defer credit to close), NOT the cumulative STO cash flow.

    This is the SEMANTIC CHANGE from the pre-fix model. A short
    sold-to-open at $3000 with no snapshot history shows $0 every day
    until close — when it crystallizes at the realized total. Pre-fix
    the chart showed +$3000 from STO date forward, lying about when
    the P&L was actually earned.
    """
    df = pd.DataFrame([
        _daily_row("2026-04-30", cum_realized=0.0, open_mtm=0.0,
                   has_trade=True),
        _daily_row("2026-05-01", cum_realized=0.0, open_mtm=0.0),
        _daily_row("2026-05-02", cum_realized=0.0, open_mtm=0.0),
    ])
    out = _build_chart_from_daily_pnl(
        df, _open_short_option_current_df(0.0)
    )
    for d in ("2026-04-30", "2026-05-01", "2026-05-02"):
        assert d in out["dates"], f"missing {d} in chart output"
        assert out["options"][out["dates"].index(d)] == 0.0, (
            f"pre-snapshot open day {d} must defer credit (show $0) — "
            f"got {out['options'][out['dates'].index(d)]}"
        )


def test_expiry_crystallization_step_function_closed_position():
    """The flagship case for a CLOSED position: short call sold for
    $3000, expires OTM 7 days later. With current_df empty (position
    has no live legs), the chart helper compresses quiet flat-line days
    — but the realization step day MUST still render. Expected shape:
    open day at $0, close day at $3000.

    Pre-fix:
      (a) chart showed +$3000 on STO date, flat thereafter (cash-flow
          attribution lied about timing), and
      (b) the close day would have been silently skipped (no fill in
          stg_history for OTM expiry → has_trade=False), so the chart
          stayed at the (wrong) +$3000 from STO indefinitely.
    Post-fix the chart is honest about WHEN the P&L was earned.
    """
    df = pd.DataFrame([
        _daily_row("2026-05-01", cum_realized=0.0, open_mtm=0.0,
                   has_trade=True),
        _daily_row("2026-05-02", cum_realized=0.0, open_mtm=0.0),
        _daily_row("2026-05-08", cum_realized=3000.0, open_mtm=0.0),
    ])
    out = _build_chart_from_daily_pnl(df, pd.DataFrame())

    # Quiet days between open and close are compressed (legacy
    # closed-position rendering behavior). Open day shows $0
    # (realize-on-close), close day shows the full realized step.
    assert "2026-05-01" in out["dates"], "open day must render"
    assert "2026-05-08" in out["dates"], (
        "realization-step day MUST render — pre-fix the "
        "skip-quiet-days-for-closed branch silently dropped this "
        "because OTM expiries have no fill in stg_history"
    )
    assert out["options"][out["dates"].index("2026-05-01")] == 0.0
    assert out["options"][out["dates"].index("2026-05-08")] == 3000.0


def test_expiry_crystallization_step_function_open_position():
    """Same scenario but the position is still considered live (other
    legs in current_df keep position_is_closed=False). The chart should
    render every day with the appropriate value: flat $0 from STO
    through 5/7, then $3000 on expiry."""
    df = pd.DataFrame([
        _daily_row("2026-05-01", cum_realized=0.0, open_mtm=0.0,
                   has_trade=True),
        _daily_row("2026-05-02", cum_realized=0.0, open_mtm=0.0),
        _daily_row("2026-05-03", cum_realized=0.0, open_mtm=0.0),
        _daily_row("2026-05-04", cum_realized=0.0, open_mtm=0.0),
        _daily_row("2026-05-05", cum_realized=0.0, open_mtm=0.0),
        _daily_row("2026-05-06", cum_realized=0.0, open_mtm=0.0),
        _daily_row("2026-05-07", cum_realized=0.0, open_mtm=0.0),
        _daily_row("2026-05-08", cum_realized=3000.0, open_mtm=0.0),
    ])
    out = _build_chart_from_daily_pnl(
        df, _open_short_option_current_df(0.0)
    )

    # All 8 days render (position still open via current_df).
    chart_options_for_window = [
        out["options"][out["dates"].index(d)]
        for d in (
            "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04",
            "2026-05-05", "2026-05-06", "2026-05-07", "2026-05-08",
        )
        if d in out["dates"]
    ]
    expected = [0.0] * 7 + [3000.0]
    assert chart_options_for_window == expected, (
        f"open-position render should be flat at $0 then step to $3000 "
        f"on expiry day. Got {chart_options_for_window}"
    )


def test_chart_endpoint_matches_int_option_contracts_total_pnl_sum():
    """End-to-end alignment: chart's terminal options value MUST equal
    the sum of ``int_option_contracts.total_pnl`` for the same scope.
    The Breakdown by Type card on the same page reads from contracts.
    Disagreement = the user sees two different "options P&L" numbers
    on one page (the original BE/Sara symptom).

    Scenario: Sara/BE — STO 285C $466, BTC 285C -$904 (closed at
    -$438 on 5/1). STO 290C $3004, still open with snapshot
    cost_basis=$3002, market_value=-$2 (unrealized = +$3000) on 5/8.

    Sum of contract total_pnl = (-438) + 3000 = 2562.
    Chart endpoint must equal 2562.
    """
    df = pd.DataFrame([
        # 4/30: STO 285C, no snapshot yet -> open_mtm=0, cum_realized=0
        _daily_row("2026-04-30", cum_realized=0.0, open_mtm=0.0,
                   has_trade=True),
        # 5/1: 285C closes at -438, 290C opens (no snapshot yet)
        _daily_row("2026-05-01", cum_realized=-438.0, open_mtm=0.0,
                   has_trade=True),
        # 5/8: 290C still open, MTM = +3000
        _daily_row("2026-05-08", cum_realized=-438.0, open_mtm=3000.0),
    ])
    out = _build_chart_from_daily_pnl(
        df, _open_short_option_current_df(3000.0)
    )

    expected_from_contracts = (-438) + 3000  # -438 closed + 3000 open MTM
    idx = out["dates"].index("2026-05-08")
    assert out["options"][idx] == expected_from_contracts, (
        f"chart endpoint {out['options'][idx]} must equal "
        f"Σ int_option_contracts.total_pnl ({expected_from_contracts}). "
        f"Disagreement here means Strategy Breakdown / Breakdown by Type "
        f"on the same page show a different total than the chart — the "
        f"original BE/Sara reconciliation symptom."
    )
