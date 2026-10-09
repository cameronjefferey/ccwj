"""Unit tests for app/weekly_review.py — new Daily Review helpers.

The page was rebuilt May 2026 as a single-mode "Daily Review" (the
Friday / Monday / Mid-Week mode toggle was removed). These tests pin
the new per-symbol attribution math + rollups so a future regression
doesn't quietly flip a column to zero or extrapolate a $0.50 capital
position to 10,000%/yr.

The endpoint name stayed `weekly_review` for url_for() compat, so the
module path is unchanged.
"""
import logging
from datetime import date

import pandas as pd

from app.weekly_review import (
    ANNUALIZED_DENOMINATOR_FLOOR,
    DAY_OPTIONS_MOVES_QUERY,
    ANNUALIZED_MIN_DAYS,
    DAY_TRADES_QUERY,
    CLOSE_SLEEVE_QUERY,
    TODAY_OPTIONS_MOVES_QUERY,
    _aggregate_breakdown_by,
    _annualized_pct,
    _build_account_breakdown,
    _build_benchmark_rows,
    _build_benchmark_snapshot,
    _build_after_hours_movers,
    _build_breakdown_totals,
    _build_position_breakdown,
    _apply_same_day_option_lots,
    _build_today_movers,
    _invested_from_close_sleeve,
    _build_trades_this_week,
    _build_upcoming_dividends,
    _coerce_date,
    _covered_calls_without_short,
    _group_day_rolls,
    _next_ex_div_on_or_after,
    _drop_stale_option_rows,
    _format_trade_contract,
    _format_session_date,
    _frame_as_of_date,
    _option_row_key,
    _overview_pending_session,
    _overview_pending_banner,
    _pick_trades_week_frame,
    _review_session_cutoff_and_trade_query,
    _snapshot_as_of_date,
    _split_day_fills,
    _today_headline,
    _today_pulse,
    _trades_as_of_date,
    build_daily_review_batch,
)


class TestAnnualizedPct:
    """Annualized return = (net / capital) × (365 / max(days, 30)) × 100.

    The 30-day floor + capital floor prevent the "RKLB +$10 on $0.50 cost
    in 1 day = 730,000,000%/yr" failure mode.
    """

    def test_one_year_at_cost(self):
        # $1,000 net on $10,000 capital over 365 days → 10%/yr.
        assert _annualized_pct(1000, 10000, 365) == 10.0

    def test_short_window_anchored_at_30_days(self):
        # $100 net on $1,000 capital in 1 day must not extrapolate to
        # 100 × 365 = 36,500%/yr. We anchor to 30 days minimum.
        v = _annualized_pct(100, 1000, 1)
        # Expected: 10% × (365 / 30) ≈ 121.7%/yr.
        assert v is not None
        assert 120 <= v <= 125

    def test_capital_below_floor_returns_none(self):
        # $5 cost basis would otherwise give 4-digit annualized.
        assert _annualized_pct(1000, ANNUALIZED_DENOMINATOR_FLOOR - 1, 365) is None

    def test_zero_capital_returns_none(self):
        assert _annualized_pct(100, 0, 30) is None
        assert _annualized_pct(100, None, 30) is None

    def test_negative_pnl(self):
        # -$500 on $10,000 cap, 1 year → -5%/yr.
        assert _annualized_pct(-500, 10000, 365) == -5.0

    def test_min_days_constant_is_sensible(self):
        # If we ever drop ANNUALIZED_MIN_DAYS below ~7 we've made the
        # math gameable by a single-day position. Pin it.
        assert ANNUALIZED_MIN_DAYS >= 14


def test_overview_option_capital_uses_opening_cash_flows_only():
    """stg_history never emits the retired ``option_buy``/``option_sell``
    labels. Capital is deployed when a contract opens; including closing
    cash flows double-counts completed round trips and understates returns.
    """
    from app.weekly_review import (
        OVERVIEW_STYLE_QUERY,
        POSITION_ATTRIBUTION_QUERY,
    )

    assert "action='option_buy'" not in POSITION_ATTRIBUTION_QUERY
    assert "action='option_sell'" not in POSITION_ATTRIBUTION_QUERY
    assert "action = 'option_buy_to_open'" in POSITION_ATTRIBUTION_QUERY
    assert "action = 'option_sell_to_open'" in POSITION_ATTRIBUTION_QUERY
    assert "option_buy_to_close" not in POSITION_ATTRIBUTION_QUERY
    assert "option_sell_to_close" not in POSITION_ATTRIBUTION_QUERY
    assert "action = 'option_buy_to_open'" in OVERVIEW_STYLE_QUERY
    assert "action = 'option_sell_to_open'" in OVERVIEW_STYLE_QUERY


class TestBuildPositionBreakdown:
    """Per-symbol breakdown row builder. Mirrors the trader's external
    Excel: Stock | G/L Stock | G/L Option | Dividend | Net | …"""

    def _row(self, **kw):
        # Default row shape matches POSITION_ATTRIBUTION_QUERY output.
        base = {
            "account": "main", "user_id": 1, "symbol": "JEPI",
            "equity_pnl": 1000.0, "option_pnl": 0.0, "dividend_income": 250.0,
            "net_pnl": 1250.0,
            "equity_capital": 10000.0, "option_capital_paid": 0.0,
            "option_premium_collected": 0.0,
            "current_equity_cost": 10000.0, "current_equity_value": 11000.0,
            "current_option_value": 0.0, "current_equity_unrealized": 1000.0,
            "current_option_unrealized": 0.0,
            "current_equity_shares": 100, "num_equity_legs": 1, "num_option_legs": 0,
            "num_open_groups": 1, "num_closed_groups": 0,
            "current_price": 110.0,
            "first_open_date": date(2025, 5, 1),
            "last_activity_date": date(2026, 5, 1),
            "days_held": 365,
            "status": "Open",
            "sector": "Financial Services", "subsector": "Asset Management",
            "company_name": "JPMorgan Equity Premium Income",
            "last_dividend_date": date(2026, 4, 15),
            "dividend_count": 12,
        }
        base.update(kw)
        return base

    def test_empty_input_returns_empty_list(self):
        assert _build_position_breakdown(None, {}) == []
        assert _build_position_breakdown(pd.DataFrame(), {}) == []

    def test_single_symbol_basic_attribution(self):
        df = pd.DataFrame([self._row()])
        rows = _build_position_breakdown(df, {"JEPI": "Dividend"})
        assert len(rows) == 1
        r = rows[0]
        assert r["symbol"] == "JEPI"
        assert r["equity_pnl"] == 1000.0
        assert r["option_pnl"] == 0.0
        assert r["dividend_income"] == 250.0
        assert r["net_pnl"] == 1250.0
        # Capital deployed should be max(buy_cash, current_cost).
        assert r["capital_at_risk"] == 10000.0
        # Annualized: 1250/10000 = 12.5% over 365d = 12.5%/yr.
        assert r["annualized_pct"] == 12.5
        # %Return = 12.5%.
        assert r["pct_return"] == 12.5
        assert r["status"] == "Open"
        assert r["strategy"] == "Dividend"
        assert r["sector"] == "Financial Services"

    def test_closed_position(self):
        # Closed position: no current legs, no open groups, last_activity = close date.
        df = pd.DataFrame([self._row(
            num_open_groups=0, num_equity_legs=0, num_option_legs=0,
            current_equity_cost=0, current_equity_value=0,
            current_equity_shares=0,
            last_activity_date=date(2026, 1, 15),
            first_open_date=date(2025, 11, 1),
        )])
        rows = _build_position_breakdown(df, {})
        assert rows[0]["status"] == "Closed"

    def test_aggregates_across_accounts_to_single_symbol_row(self):
        # Same symbol in two accounts → one row, summed P&L.
        # current_equity_cost set low so capital_at_risk falls through
        # to the buy-cash branch (otherwise max() picks the snapshot
        # cost basis, which is fine but isn't what this test checks).
        df = pd.DataFrame([
            self._row(account="A1", equity_pnl=500, dividend_income=100,
                     net_pnl=600, equity_capital=5000,
                     current_equity_cost=0, current_equity_value=0),
            self._row(account="A2", equity_pnl=500, dividend_income=150,
                     net_pnl=650, equity_capital=5000,
                     current_equity_cost=0, current_equity_value=0),
        ])
        rows = _build_position_breakdown(df, {})
        assert len(rows) == 1
        assert rows[0]["equity_pnl"] == 1000.0
        assert rows[0]["dividend_income"] == 250.0
        assert rows[0]["net_pnl"] == 1250.0
        assert rows[0]["capital_at_risk"] == 10000.0

    def test_dust_position_annualized_returns_none(self):
        # $5 capital → annualized denominator floor kicks in.
        df = pd.DataFrame([self._row(
            equity_capital=5.0, current_equity_cost=5.0,
            equity_pnl=2, dividend_income=0, net_pnl=2,
        )])
        rows = _build_position_breakdown(df, {})
        assert rows[0]["annualized_pct"] is None
        assert rows[0]["pct_return"] is None

    def test_sorted_by_net_pnl_descending(self):
        df = pd.DataFrame([
            self._row(symbol="AAA", net_pnl=100),
            self._row(symbol="BBB", net_pnl=500),
            self._row(symbol="CCC", net_pnl=-200),
        ])
        rows = _build_position_breakdown(df, {})
        assert [r["symbol"] for r in rows] == ["BBB", "AAA", "CCC"]

    def test_week_start_filter_keeps_open_drops_old_closed(self):
        """Daily Review scope: open positions + closed-this-week only.

        - Open position with old last_activity_date → KEPT (still open).
        - Closed position closed this week → KEPT.
        - Closed position closed before this week → DROPPED.
        """
        week_start = date(2026, 5, 18)  # Monday
        df = pd.DataFrame([
            # Long-held open position; last_activity = today (mart convention).
            self._row(symbol="OPEN_OLD", num_open_groups=1,
                     num_equity_legs=1, current_equity_shares=100,
                     last_activity_date=date(2026, 5, 19)),
            # Closed earlier this week.
            self._row(symbol="CLOSED_THIS_WEEK",
                     num_open_groups=0, num_equity_legs=0, num_option_legs=0,
                     current_equity_cost=0, current_equity_value=0,
                     current_equity_shares=0,
                     last_activity_date=date(2026, 5, 19)),
            # Closed last week — should be filtered out.
            self._row(symbol="CLOSED_LAST_WEEK",
                     num_open_groups=0, num_equity_legs=0, num_option_legs=0,
                     current_equity_cost=0, current_equity_value=0,
                     current_equity_shares=0,
                     last_activity_date=date(2026, 5, 12)),
            # Closed months ago — definitely out.
            self._row(symbol="CLOSED_LONG_AGO",
                     num_open_groups=0, num_equity_legs=0, num_option_legs=0,
                     current_equity_cost=0, current_equity_value=0,
                     current_equity_shares=0,
                     last_activity_date=date(2026, 1, 15)),
        ])
        rows = _build_position_breakdown(df, {}, week_start=week_start)
        symbols = {r["symbol"] for r in rows}
        assert symbols == {"OPEN_OLD", "CLOSED_THIS_WEEK"}

    def test_week_start_none_keeps_all_rows(self):
        """Backward compat: omitting week_start preserves prior behavior
        (returns every symbol, no scope filter)."""
        df = pd.DataFrame([
            self._row(symbol="OPEN", num_open_groups=1, num_equity_legs=1,
                     current_equity_shares=100,
                     last_activity_date=date(2026, 5, 19)),
            self._row(symbol="CLOSED_OLD",
                     num_open_groups=0, num_equity_legs=0, num_option_legs=0,
                     current_equity_cost=0, current_equity_value=0,
                     current_equity_shares=0,
                     last_activity_date=date(2025, 11, 1)),
        ])
        rows = _build_position_breakdown(df, {})
        assert len(rows) == 2


class TestAggregateBreakdownBy:
    """Strategy / sector / subsector rollups. Same shape as positions but
    grouped — totals must reconcile to the position-level totals."""

    def _rows(self):
        return [
            {"symbol": "JEPI", "strategy": "Dividend", "sector": "Financial Services",
             "equity_pnl": 1000, "option_pnl": 0, "dividend_income": 500,
             "net_pnl": 1500, "capital_at_risk": 10000, "days_held": 365,
             "current_equity_value": 11000, "current_option_value": 0,
             "status": "Open"},
            {"symbol": "DELL", "strategy": "Covered Call", "sector": "Technology",
             "equity_pnl": 21012, "option_pnl": -36, "dividend_income": 158,
             "net_pnl": 21134, "capital_at_risk": 80000, "days_held": 365,
             "current_equity_value": 100000, "current_option_value": 500,
             "status": "Open"},
            {"symbol": "NVDA", "strategy": "Covered Call", "sector": "Technology",
             "equity_pnl": 26471, "option_pnl": -550, "dividend_income": 6,
             "net_pnl": 25927, "capital_at_risk": 50000, "days_held": 200,
             "current_equity_value": 75000, "current_option_value": 200,
             "status": "Open"},
        ]

    def test_strategy_rollup_groups_correctly(self):
        out = _aggregate_breakdown_by(self._rows(), "strategy", label_name="strategy")
        labels = [r["strategy"] for r in out]
        assert "Covered Call" in labels
        assert "Dividend" in labels
        # Covered Call: 2 symbols, summed
        cc = next(r for r in out if r["strategy"] == "Covered Call")
        assert cc["num_symbols"] == 2
        assert cc["equity_pnl"] == 47483.0
        assert cc["option_pnl"] == -586.0
        assert cc["dividend_income"] == 164.0
        assert cc["net_pnl"] == 47061.0
        assert cc["max_days_held"] == 365

    def test_sector_rollup_lists_symbols(self):
        out = _aggregate_breakdown_by(self._rows(), "sector", label_name="sector")
        tech = next(r for r in out if r["sector"] == "Technology")
        assert sorted(tech["symbols"]) == ["DELL", "NVDA"]
        assert tech["num_symbols"] == 2

    def test_rollup_sorted_by_net_descending(self):
        out = _aggregate_breakdown_by(self._rows(), "strategy", label_name="strategy")
        # Covered Call ($47k) should come before Dividend ($1.5k).
        assert out[0]["strategy"] == "Covered Call"
        assert out[1]["strategy"] == "Dividend"

    def test_empty_rows_returns_empty(self):
        assert _aggregate_breakdown_by([], "strategy", label_name="strategy") == []


class TestBuildBreakdownTotals:
    """Footer-row totals power both the table footer and the
    Excel-style "Profitability scorecard" card."""

    def test_excel_scorecard_math_matches(self):
        # User's spreadsheet: 12 symbols, 10 stock profitable, 4 option
        # profitable, 10 net profitable. Replicate the shape here.
        rows = [{"symbol": f"S{i}",
                 "equity_pnl": 100 if i < 10 else -100,
                 "option_pnl": 50 if i < 4 else -50,
                 "dividend_income": 10,
                 "net_pnl": 100 if i < 10 else -50,
                 "capital_at_risk": 1000}
                for i in range(12)]
        t = _build_breakdown_totals(rows)
        assert t["num_symbols"] == 12
        assert t["equity_profitable"] == 10
        assert t["equity_with_exposure"] == 12
        assert t["option_profitable"] == 4
        assert t["option_with_exposure"] == 12
        assert t["net_profitable"] == 10
        # 10/12 = 83.3% (matches the screenshot scorecard's "Stk Profitable").
        assert t["equity_win_pct"] == 83.3
        # 4/12 = 33.3%.
        assert t["option_win_pct"] == 33.3
        # 10/12 = 83.3% net profitable.
        assert t["net_win_pct"] == 83.3

    def test_empty_returns_none(self):
        assert _build_breakdown_totals([]) is None

    def test_excludes_zero_exposure_from_win_pct_denominator(self):
        # A symbol with no option P&L shouldn't penalize option win-rate.
        rows = [
            {"symbol": "A", "equity_pnl": 100, "option_pnl": 50, "dividend_income": 0,
             "net_pnl": 150, "capital_at_risk": 1000},
            {"symbol": "B", "equity_pnl": -200, "option_pnl": 0, "dividend_income": 0,
             "net_pnl": -200, "capital_at_risk": 1000},
        ]
        t = _build_breakdown_totals(rows)
        # Option exposure: only A. Option profitable: 1. → 100%.
        assert t["option_with_exposure"] == 1
        assert t["option_win_pct"] == 100.0


class TestBuildTodayMovers:
    def test_option_query_caps_mart_rows_at_latest_official_close(self):
        """Cap option day-moves at @as_of (the session date), not a
        full-symbol stg_daily_prices scan. UTC-tomorrow mart rows are
        excluded by ``m.date <= @as_of``."""
        normalized = " ".join(TODAY_OPTIONS_MOVES_QUERY.lower().split())
        assert "d.date <= @as_of" in normalized
        assert "date_sub(@as_of, interval 10 day)" in normalized
        assert "int_option_contract_daily_pnl" in normalized
        assert "is_realized_close" in normalized
        assert "mart_daily_pnl" not in normalized
        assert "stg_daily_prices" not in normalized

    def test_empty_input(self):
        result = _build_today_movers(None)
        assert result["winners"] == []
        assert result["losers"] == []
        assert result["total_impact"] == 0.0
        assert result["as_of"] is None
        assert result["options"] == []
        assert result["dividends"] == []
        assert result["combined_impact"] == 0.0
        result = _build_today_movers(pd.DataFrame())
        assert result["winners"] == []

    def test_options_and_dividends_fold_into_combined_impact(self):
        eq = pd.DataFrame([
            {"symbol": "AAPL", "shares": 100, "current_value": 17000,
             "today_close": 170, "prev_close": 167,
             "price_change": 3.0, "price_change_pct": 1.8,
             "dollar_impact": 300.0, "today_date": date(2026, 5, 18)},
        ])
        opt = pd.DataFrame([
            {"symbol": "AAPL", "today_date": date(2026, 5, 18), "dollar_impact": -120.0},
            {"symbol": "SPY", "today_date": date(2026, 5, 18), "dollar_impact": 45.0},
        ])
        # One dividend on the as-of day, one older (must be excluded).
        div = pd.DataFrame([
            {"symbol": "JEPI", "trade_date": date(2026, 5, 18), "amount": 88.10},
            {"symbol": "JEPI", "trade_date": date(2026, 5, 15), "amount": 999.0},
        ])
        result = _build_today_movers(eq, options_moves_df=opt, dividends_df=div)
        assert result["total_impact"] == 300.0
        assert result["options_impact"] == -75.0
        assert [o["symbol"] for o in result["options"]] == ["AAPL", "SPY"]
        assert result["dividends"] == [{"symbol": "JEPI", "amount": 88.10}]
        assert result["dividends_impact"] == 88.10
        assert result["combined_impact"] == round(300.0 - 75.0 + 88.10, 2)
        assert [(w["symbol"], w["kind"]) for w in result["winners"]] == [
            ("AAPL", "stock"), ("SPY", "option")]
        assert [(l["symbol"], l["kind"]) for l in result["losers"]] == [
            ("AAPL", "option")]

    def test_options_only_scope_without_equity_rows(self):
        # An options-only account has no equity price rows but still has a
        # today story; the builder must not blank the section.
        opt = pd.DataFrame([
            {"symbol": "SPY", "today_date": date(2026, 5, 18), "dollar_impact": 210.0},
        ])
        result = _build_today_movers(None, options_moves_df=opt)
        assert result["winners"][0]["symbol"] == "SPY"
        assert result["winners"][0]["kind"] == "option"
        assert result["winners"][0]["dollar_impact"] == 210.0
        assert result["losers"] == []
        assert result["options_impact"] == 210.0
        assert result["combined_impact"] == 210.0
        # With no equity rows the card's as-of anchors on the option-mart
        # date so the view can still date-label a stale (weekend) card.
        assert result["as_of"] == "2026-05-18"

    def test_date_honesty_defaults(self):
        # The builder defaults to the "today" voice; the VIEW flips
        # is_today off when as_of != the user's local today (the Monday-
        # morning "Friday realizations labeled today" complaint).
        df = pd.DataFrame([
            {"symbol": "AAPL", "shares": 100, "current_value": 17000,
             "today_close": 170, "prev_close": 167,
             "price_change": 3.0, "price_change_pct": 1.8,
             "dollar_impact": 300.0, "today_date": date(2026, 5, 18)},
        ])
        result = _build_today_movers(df)
        assert result["is_today"] is True
        assert result["as_of_label"] is None
        assert "options_as_of" not in result

    def test_stale_option_rows_stay_off_a_newer_equity_close(self):
        eq = pd.DataFrame([
            {"symbol": "AAPL", "shares": 100, "current_value": 17000,
             "today_close": 170, "prev_close": 169,
             "price_change": 1.0, "price_change_pct": 0.6,
             "dollar_impact": 100.0, "today_date": date(2026, 5, 19)},
        ])
        opt = pd.DataFrame([
            {"symbol": "AAPL", "today_date": date(2026, 5, 18), "dollar_impact": -500.0},
        ])
        result = _build_today_movers(eq, options_moves_df=opt)
        assert result["options"] == []
        assert result["options_impact"] == 0.0
        assert result["combined_impact"] == 100.0
        assert result["as_of"] == "2026-05-19"
        assert all(w["kind"] != "option" for w in result["winners"] + result["losers"])

    def test_options_only_keeps_the_latest_mart_date(self):
        opt = pd.DataFrame([
            {"symbol": "SPY", "today_date": date(2026, 5, 18), "dollar_impact": 210.0},
            {"symbol": "QQQ", "today_date": date(2026, 5, 15), "dollar_impact": -999.0},
        ])
        div = pd.DataFrame([
            {"symbol": "JEPI", "trade_date": date(2026, 5, 18), "amount": 50.0},
            {"symbol": "JEPI", "trade_date": date(2026, 5, 15), "amount": 42.0},
        ])
        result = _build_today_movers(None, options_moves_df=opt, dividends_df=div)
        assert [o["symbol"] for o in result["options"]] == ["SPY"]
        assert result["options_impact"] == 210.0
        assert result["as_of"] == "2026-05-18"
        assert result["dividends_impact"] == 50.0

    def test_dividend_anchor_falls_back_to_option_date(self):
        opt = pd.DataFrame([
            {"symbol": "SPY", "today_date": date(2026, 5, 18), "dollar_impact": 10.0},
        ])
        div = pd.DataFrame([
            {"symbol": "JEPI", "trade_date": date(2026, 5, 18), "amount": 50.0},
            {"symbol": "JEPI", "trade_date": date(2026, 5, 17), "amount": 42.0},
        ])
        result = _build_today_movers(None, options_moves_df=opt, dividends_df=div)
        assert result["dividends_impact"] == 50.0

    def test_splits_winners_and_losers(self):
        df = pd.DataFrame([
            {"symbol": "AAPL", "shares": 100, "current_value": 17000,
             "today_close": 170, "prev_close": 167,
             "price_change": 3.0, "price_change_pct": 1.8,
             "dollar_impact": 300.0, "today_date": date(2026, 5, 18)},
            {"symbol": "TSLA", "shares": 50, "current_value": 8000,
             "today_close": 160, "prev_close": 165,
             "price_change": -5.0, "price_change_pct": -3.0,
             "dollar_impact": -250.0, "today_date": date(2026, 5, 18)},
        ])
        result = _build_today_movers(df)
        assert len(result["winners"]) == 1
        assert len(result["losers"]) == 1
        assert result["winners"][0]["symbol"] == "AAPL"
        assert result["winners"][0]["kind"] == "stock"
        assert result["losers"][0]["symbol"] == "TSLA"
        assert result["losers"][0]["kind"] == "stock"
        assert result["total_impact"] == 50.0
        assert result["as_of"] == "2026-05-18"

    def test_equity_as_of_is_latest_bar_not_first_row(self):
        """A crypto/weekend symbol whose last close is Friday must not
        label the whole movers card Friday when equities have Monday."""
        df = pd.DataFrame([
            {"symbol": "BTC", "shares": 1, "current_value": 100,
             "today_close": 100, "prev_close": 99,
             "price_change": 1.0, "price_change_pct": 1.0,
             "dollar_impact": 1.0, "today_date": date(2026, 8, 28)},
            {"symbol": "AAPL", "shares": 10, "current_value": 1700,
             "today_close": 170, "prev_close": 167,
             "price_change": 3.0, "price_change_pct": 1.8,
             "dollar_impact": 30.0, "today_date": date(2026, 8, 31)},
        ])
        result = _build_today_movers(df)
        assert result["as_of"] == "2026-08-31"

    def test_stocks_and_options_share_five_slots_per_side(self):
        # Six small stock winners plus a larger option winner: the option
        # tile must take a slot, and the list caps at 5.
        stocks = pd.DataFrame([
            {"symbol": f"S{i}", "shares": 1, "current_value": 10,
             "today_close": 10, "prev_close": 9,
             "price_change": 1.0, "price_change_pct": 11.0,
             "dollar_impact": 10.0 + i, "today_date": date(2026, 5, 18)}
            for i in range(6)
        ])
        opt = pd.DataFrame([
            {"symbol": "MRVL", "today_date": date(2026, 5, 18), "dollar_impact": 1000.0},
            {"symbol": "SMTC", "today_date": date(2026, 5, 18), "dollar_impact": -250.0},
        ])
        result = _build_today_movers(stocks, options_moves_df=opt)
        assert len(result["winners"]) == 5
        assert result["winners"][0]["symbol"] == "MRVL"
        assert result["winners"][0]["kind"] == "option"
        assert all(w["kind"] == "stock" for w in result["winners"][1:])
        assert result["losers"] == [{
            "symbol": "SMTC", "kind": "option", "dollar_impact": -250.0,
            "shares": None, "current_value": None, "price_change": None,
            "price_change_pct": None, "today_close": None,
            "contract_detail": "", "option_caption": "",
            "lot_split_reason": "no_fills",
            "lot_split_detail": "anchor=2026-05-18 fills=0",
            "lot_split_line": (
                "no_fills lot_sum=— gap=— budget=— "
                "fills=0 dates=— roots=— accts=—"
            ),
        }]
        # Header still totals every row, not just the displayed 5.
        assert result["options_impact"] == 750.0

    def test_sub_dollar_moves_do_not_take_a_tile(self):
        df = pd.DataFrame([
            {"symbol": "USDC", "shares": 1814, "current_value": 1814,
             "today_close": 1.0, "prev_close": 1.0,
             "price_change": 0.0, "price_change_pct": 0.0,
             "dollar_impact": -0.4, "today_date": date(2026, 5, 18)},
            {"symbol": "FN", "shares": 1, "current_value": 430,
             "today_close": 430, "prev_close": 435,
             "price_change": -5.0, "price_change_pct": -1.16,
             "dollar_impact": -5.0, "today_date": date(2026, 5, 18)},
        ])
        result = _build_today_movers(df)
        assert [l["symbol"] for l in result["losers"]] == ["FN"]
        # Full-book header still includes the dust.
        assert result["total_impact"] == -5.4

    def test_expired_itm_spread_is_the_settlement_not_the_credit(self):
        # SPXW Oct 1 7650/7655 bear call. A zero mark on still-open
        # contracts would show the gross credit (+$1,450). The close
        # books the estimate instead: net about −$3,574, no open mark left.
        opt = pd.DataFrame([
            {
                "symbol": "SPXW", "trade_symbol": "SPXW  261001C07650000",
                "today_date": date(2026, 10, 1), "tenant_id": "t1",
                "option_strike": 7650, "option_type": "C",
                "option_expiry": date(2026, 10, 1), "direction": "Sold",
                "quantity": 10, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": -2242.22,
            },
            {
                "symbol": "SPXW", "trade_symbol": "SPXW  261001C07655000",
                "today_date": date(2026, 10, 1), "tenant_id": "t1",
                "option_strike": 7655, "option_type": "C",
                "option_expiry": date(2026, 10, 1), "direction": "Bought",
                "quantity": 10, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": -1332.22,
            },
        ])
        result = _build_today_movers(None, options_moves_df=opt)
        row = result["losers"][0]
        assert result["winners"] == []
        assert row["symbol"] == "SPXW"
        assert row["dollar_impact"] == -3574.44
        assert row["contract_detail"] == "10× 7650/7655C spread"
        assert row["option_caption"] == "−$3,574 closed"
        assert result["options_impact"] == -3574.44

    def test_two_oct2_spxw_lots_split_and_include_fees(self):
        """20× closed and 10× expired, not one 30× line at the gross +$2,000.

        Order amounts are the gross premium. Fees sit beside them. The
        card uses the position-page lot split and nets each fee once.
        """
        day = date(2026, 10, 2)
        short = "SPXW  261002C07730000"
        long = "SPXW  261002C07735000"
        opt = pd.DataFrame([
            {
                "symbol": "SPXW", "trade_symbol": short,
                "today_date": day, "tenant_id": "snaptrade:sara",
                "account": "Sara Investment",
                "option_strike": 7730, "option_type": "C",
                "option_expiry": day, "direction": "Sold",
                "quantity": 30, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": 10842.00,
            },
            {
                "symbol": "SPXW", "trade_symbol": long,
                "today_date": day, "tenant_id": "snaptrade:sara",
                "account": "Sara Investment",
                "option_strike": 7735, "option_type": "C",
                "option_expiry": day, "direction": "Bought",
                "quantity": 30, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": -8842.00,
            },
        ])

        def fills(net_already):
            # (action, occ, qty, price, gross, fee)
            legs = [
                ("option_sell_to_open", short, 20, 6.85, 13700.00, 24.44),
                ("option_buy_to_open", long, 20, 5.35, -10700.00, 24.44),
                ("option_sell_to_close", long, 20, 1.824, 3648.00, 24.44),
                ("option_buy_to_close", short, 20, 2.824, -5648.00, 24.44),
                ("option_sell_to_open", short, 10, 2.79, 2790.00, 12.22),
                ("option_buy_to_open", long, 10, 1.79, -1790.00, 12.22),
            ]
            rows = []
            for action, occ, qty, price, gross, fee in legs:
                amount = gross
                if net_already:
                    amount = round(gross - fee, 2) if gross > 0 else round(gross - fee, 2)
                rows.append({
                    "tenant_id": "snaptrade:sara",
                    "account": "Sara Investment",
                    "trade_date": day,
                    "action": action,
                    "trade_symbol": occ,
                    "underlying_symbol": "SPXW",
                    "quantity": qty,
                    "price": price,
                    "amount": amount,
                    "fees": fee,
                    "instrument_type": "Call",
                })
            rows.append({
                "tenant_id": "snaptrade:sara",
                "account": "Sara Investment",
                "trade_date": day,
                "action": "equity_buy",
                "trade_symbol": "SPXW",
                "underlying_symbol": "SPXW",
                "quantity": 100, "price": 10, "amount": -1000, "fees": 0,
            })
            rows.append({
                "tenant_id": "snaptrade:sara",
                "account": "Sara Investment",
                "trade_date": day,
                "action": "option_expired",
                "trade_symbol": short,
                "quantity": 30, "price": 0, "amount": 0, "fees": 0,
            })
            return pd.DataFrame(rows)

        for net_already in (False, True):
            result = _build_today_movers(
                None, options_moves_df=opt, option_fills_df=fills(net_already),
            )
            spxw = [r for r in result["winners"] if r["symbol"] == "SPXW"]
            assert result["losers"] == []
            assert len(spxw) == 2
            by_detail = {r["contract_detail"]: r for r in spxw}
            assert set(by_detail) == {
                "20× 7730/7735C spread",
                "10× 7730/7735C spread",
            }
            assert by_detail["20× 7730/7735C spread"]["dollar_impact"] == 902.24
            assert by_detail["20× 7730/7735C spread"]["option_caption"] == "+$902 closed"
            assert by_detail["10× 7730/7735C spread"]["dollar_impact"] == 975.56
            assert by_detail["10× 7730/7735C spread"]["option_caption"] == "+$976 expired"
            assert result["options_impact"] == 1877.80
            assert all("30×" not in (r.get("contract_detail") or "") for r in spxw)
            assert result["combined_impact"] == 1877.80

    def test_date_anchor_still_splits_the_oct2_lots(self):
        """A datetime.date anchor must match ISO fill dates.

        `session = anchor or …` kept the date object, so no lot matched
        and Today stayed on the fused 30× line.
        """
        day = date(2026, 10, 2)
        short = "SPXW  261002C07730000"
        long = "SPXW  261002C07735000"
        legs = [
            ("option_sell_to_open", short, 20, 6.85, 13700.00, 24.44),
            ("option_buy_to_open", long, 20, 5.35, -10700.00, 24.44),
            ("option_sell_to_close", long, 20, 1.824, 3648.00, 24.44),
            ("option_buy_to_close", short, 20, 2.824, -5648.00, 24.44),
            ("option_sell_to_open", short, 10, 2.79, 2790.00, 12.22),
            ("option_buy_to_open", long, 10, 1.79, -1790.00, 12.22),
        ]
        fills = pd.DataFrame([
            {
                "tenant_id": "snaptrade:sara",
                "account": "Sara Investment",
                "trade_date": day,
                "action": action,
                "trade_symbol": occ,
                "quantity": qty,
                "price": price,
                "amount": gross,
                "fees": fee,
            }
            for action, occ, qty, price, gross, fee in legs
        ])
        fused = [{
            "symbol": "SPXW",
            "dollar_impact": 2000.0,
            "open_impact": 0.0,
            "closed_impact": 2000.0,
            "contract_detail": "30× 7730/7735C spread",
            "option_caption": "Closed today",
        }]
        kept, impact, _as_of = _apply_same_day_option_lots(fused, fills, day)
        by_detail = {row["contract_detail"]: row for row in kept}
        assert set(by_detail) == {
            "20× 7730/7735C spread",
            "10× 7730/7735C spread",
        }
        assert by_detail["20× 7730/7735C spread"]["dollar_impact"] == 902.24
        assert by_detail["20× 7730/7735C spread"]["option_caption"] == "+$902 closed"
        assert by_detail["10× 7730/7735C spread"]["dollar_impact"] == 975.56
        assert by_detail["10× 7730/7735C spread"]["option_caption"] == "+$976 expired"
        assert impact == 1877.80
        assert "30×" not in " ".join(by_detail)

    def test_itm_settlement_stays_one_line_when_fills_omit_the_width(self):
        """Oct 1's estimate is the width, not the opening credit.

        Same-day opens without the settlement cash must not replace the
        mart dollar.
        """
        day = date(2026, 10, 1)
        opt = pd.DataFrame([
            {
                "symbol": "SPXW", "trade_symbol": "SPXW  261001C07650000",
                "today_date": day, "tenant_id": "t1",
                "option_strike": 7650, "option_type": "C",
                "option_expiry": day, "direction": "Sold",
                "quantity": 10, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": -2242.22,
            },
            {
                "symbol": "SPXW", "trade_symbol": "SPXW  261001C07655000",
                "today_date": day, "tenant_id": "t1",
                "option_strike": 7655, "option_type": "C",
                "option_expiry": day, "direction": "Bought",
                "quantity": 10, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": -1332.22,
            },
        ])
        fills = pd.DataFrame([
            {
                "tenant_id": "t1", "account": "Sara", "trade_date": day,
                "action": "option_sell_to_open",
                "trade_symbol": "SPXW  261001C07650000",
                "quantity": 10, "price": 7.77, "amount": 7757.78, "fees": 12.22,
            },
            {
                "tenant_id": "t1", "account": "Sara", "trade_date": day,
                "action": "option_buy_to_open",
                "trade_symbol": "SPXW  261001C07655000",
                "quantity": 10, "price": 6.32, "amount": -6332.22, "fees": 12.22,
            },
        ])
        result = _build_today_movers(
            None, options_moves_df=opt, option_fills_df=fills,
        )
        row = result["losers"][0]
        assert len(result["losers"]) == 1
        assert result["winners"] == []
        assert row["dollar_impact"] == -3574.44
        assert row["contract_detail"] == "10× 7650/7655C spread"
        assert row["option_caption"] == "−$3,574 closed"
        # Opening credit versus the width is outside the fee budget.
        assert row["lot_split_reason"] == "total_mismatch"
        assert "gap=" in row["lot_split_detail"]

    def test_single_spread_mover_nets_a_gross_order_fee(self):
        day = date(2026, 10, 2)
        opt = pd.DataFrame([
            {
                "symbol": "SPXW", "trade_symbol": "SPXW  261002C07730000",
                "today_date": day, "tenant_id": "t1",
                "option_strike": 7730, "option_type": "C",
                "option_expiry": day, "direction": "Sold",
                "quantity": 10, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": 2790.00,
            },
            {
                "symbol": "SPXW", "trade_symbol": "SPXW  261002C07735000",
                "today_date": day, "tenant_id": "t1",
                "option_strike": 7735, "option_type": "C",
                "option_expiry": day, "direction": "Bought",
                "quantity": 10, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": -1790.00,
            },
        ])
        fills = pd.DataFrame([
            {
                "tenant_id": "t1", "account": "Sara", "trade_date": day,
                "action": "option_sell_to_open",
                "trade_symbol": "SPXW  261002C07730000",
                "quantity": 10, "price": 2.79, "amount": 2790.00, "fees": 12.22,
            },
            {
                "tenant_id": "t1", "account": "Sara", "trade_date": day,
                "action": "option_buy_to_open",
                "trade_symbol": "SPXW  261002C07735000",
                "quantity": 10, "price": 1.79, "amount": -1790.00, "fees": 12.22,
            },
        ])
        result = _build_today_movers(
            None, options_moves_df=opt, option_fills_df=fills,
        )
        row = result["winners"][0]
        assert len(result["winners"]) == 1
        assert row["contract_detail"] == "10× 7730/7735C spread"
        assert row["option_caption"] == "+$976 expired"
        assert row["dollar_impact"] == 975.56
        assert result["options_impact"] == 975.56

    def test_day_trades_query_carries_fees_for_mover_lots(self):
        assert "f.fees" in DAY_TRADES_QUERY
        assert "c.fees" in DAY_TRADES_QUERY
        # Contract realized P&L joins on the fill's account and user.
        # Tenant + OCC alone repeats each fill once per contract row.
        assert "f.account = o.account" in DAY_TRADES_QUERY
        assert "f.user_id IS NOT DISTINCT FROM o.user_id" in DAY_TRADES_QUERY
        # One row per OCC and direction. A 0 placeholder must not win,
        # and a long plus a short on the same OCC both stay.
        assert "MAX(realized_pnl)" not in DAY_TRADES_QUERY
        assert "opened_before_history" in DAY_TRADES_QUERY
        assert "PARTITION BY tenant_id, account, user_id, trade_symbol, direction" in DAY_TRADES_QUERY
        assert "o.direction = 'Sold'" in DAY_TRADES_QUERY
        assert "o.direction = 'Bought'" in DAY_TRADES_QUERY
        # Opens posted the day after a same-day expiry still belong here.
        assert "option_expiry = @day" in DAY_TRADES_QUERY
        assert "DATE_ADD(@day, INTERVAL 2 DAY)" in DAY_TRADES_QUERY

    def test_zero_placeholder_does_not_erase_a_loss(self):
        from app.weekly_review import _select_contract_realized

        occ = "SPXW  261002C07735000"
        picked = _select_contract_realized([
            {
                "trade_symbol": occ, "direction": "Bought",
                "realized_pnl": -8903.10, "opened_before_history": False,
            },
            {
                "trade_symbol": occ, "direction": "Bought",
                "realized_pnl": 0.0, "opened_before_history": True,
            },
        ])
        assert len(picked) == 1
        assert picked[0]["realized_pnl"] == -8903.10

    def test_long_and_short_on_one_occ_both_stay(self):
        from app.weekly_review import _select_contract_realized

        occ = "SPXW  261002C07730000"
        picked = _select_contract_realized([
            {
                "trade_symbol": occ, "direction": "Sold",
                "realized_pnl": 10780.90, "opened_before_history": False,
            },
            {
                "trade_symbol": occ, "direction": "Bought",
                "realized_pnl": -8903.10, "opened_before_history": False,
            },
            {
                "trade_symbol": occ, "direction": "Sold",
                "realized_pnl": 0.0, "opened_before_history": True,
            },
        ])
        by_dir = {row["direction"]: row["realized_pnl"] for row in picked}
        assert by_dir == {"Sold": 10780.90, "Bought": -8903.10}

    def _oct2_statement_fills(self, *, copies=1, trade_date=None):
        """Oct 2 statement amounts, already net of the broker fee."""
        day = date(2026, 10, 2) if trade_date is None else trade_date
        short = "SPXW  261002C07730000"
        long = "SPXW  261002C07735000"
        legs = [
            ("option_sell_to_open", short, 20, 6.85, 13675.56, 24.44),
            ("option_buy_to_open", long, 20, 5.35, -10724.44, 24.44),
            ("option_sell_to_close", long, 20, 1.824, 3623.56, 24.44),
            ("option_buy_to_close", short, 20, 2.824, -5672.44, 24.44),
            ("option_sell_to_open", short, 10, 2.79, 2777.78, 12.22),
            ("option_buy_to_open", long, 10, 1.79, -1802.22, 12.22),
        ]
        rows = []
        for _copy in range(copies):
            for action, occ, qty, price, amount, fee in legs:
                rows.append({
                    "tenant_id": "snaptrade:sara",
                    "account": "Sara Investment",
                    "user_id": 9,
                    "trade_date": day,
                    "action": action,
                    "trade_symbol": occ,
                    "underlying_symbol": "SPXW",
                    "quantity": qty,
                    "price": price,
                    "amount": amount,
                    "fees": fee,
                    "instrument_type": "Call",
                })
        rows.append({
            "tenant_id": "snaptrade:sara",
            "account": "Sara Investment",
            "user_id": 9,
            "trade_date": day,
            "action": "option_expired",
            "trade_symbol": short,
            "quantity": 30, "price": 0, "amount": 0, "fees": 0,
        })
        return pd.DataFrame(rows)

    def _oct2_production_options(self, symbol="SPXW"):
        """Friday ``today_options_moves`` after fees land in the amount.

        Quantity is the fused open (GREATEST of the two lots). The
        dollars are the statement nets: short +10,780.90, long −8,903.10.
        """
        day = date(2026, 10, 2)
        short = "SPXW  261002C07730000"
        long = "SPXW  261002C07735000"
        return pd.DataFrame([
            {
                "symbol": symbol, "trade_symbol": short,
                "today_date": day, "tenant_id": "snaptrade:sara",
                "account": "Sara Investment", "user_id": 9,
                "option_strike": 7730, "option_type": "C",
                "option_expiry": day, "direction": "Sold",
                "quantity": 30, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": 10780.90,
            },
            {
                "symbol": symbol, "trade_symbol": long,
                "today_date": day, "tenant_id": "snaptrade:sara",
                "account": "Sara Investment", "user_id": 9,
                "option_strike": 7735, "option_type": "C",
                "option_expiry": day, "direction": "Bought",
                "quantity": 30, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": -8903.10,
            },
        ])

    def _friday_equity(self):
        """Overview's anchor is the equity close, not the option mart date."""
        return pd.DataFrame([{
            "symbol": "GEV",
            "today_date": date(2026, 10, 2),
            "shares": 10,
            "today_close": 500.0,
            "prev_close": 499.0,
            "price_change": 1.0,
            "price_change_pct": 0.2,
            "dollar_impact": 10.0,
            "current_value": 5000.0,
        }])

    def _assert_oct2_lots(self, result):
        spxw = [r for r in result["winners"] if r["symbol"] == "SPXW"]
        by_detail = {r["contract_detail"]: r for r in spxw}
        assert set(by_detail) == {
            "20× 7730/7735C spread",
            "10× 7730/7735C spread",
        }
        assert by_detail["20× 7730/7735C spread"]["dollar_impact"] == 902.24
        assert by_detail["20× 7730/7735C spread"]["option_caption"] == "+$902 closed"
        assert by_detail["10× 7730/7735C spread"]["dollar_impact"] == 975.56
        assert by_detail["10× 7730/7735C spread"]["option_caption"] == "+$976 expired"
        assert result["options_impact"] == 1877.80
        assert result["as_of"] == "2026-10-02"
        assert all(r["lot_split_reason"] == "split" for r in spxw)
        for row in spxw:
            line = row["lot_split_line"]
            assert line.startswith("split ")
            assert "lot_sum=1877.80" in line
            assert "gap=" in line
            assert "budget=" in line
            assert "fills=" in line
            assert "dates=" in line
            assert "roots=SPXW" in line
            assert "accts=" in line
        details = " ".join(r.get("contract_detail") or "" for r in result["winners"] + result["losers"])
        assert "30×" not in details
        assert all(r["symbol"] != "SPX" for r in result["winners"] + result["losers"])

    def test_production_friday_row_splits_on_the_weekly_review_path(self):
        """The live Friday tile is one 30× line at +$1,878.

        Overview passes the equity close as the anchor and the net
        ``today_options_moves`` row (fees already in the amount). A
        second copy of each fill, the shape ``option_gl`` produced
        when it joined only on tenant + OCC, must still split.
        """
        from app import app

        equity = self._friday_equity()
        options = self._oct2_production_options()
        for copies, when in ((1, date(2026, 10, 2)), (2, date(2026, 10, 2))):
            result = _build_today_movers(
                equity,
                options_moves_df=options,
                option_fills_df=self._oct2_statement_fills(copies=copies, trade_date=when),
            )
            self._assert_oct2_lots(result)

        # Schwab MDY strings must match the ISO session anchor.
        for raw in ("10/02/2026", "10/02/26"):
            result = _build_today_movers(
                equity,
                options_moves_df=options,
                option_fills_df=self._oct2_statement_fills(trade_date=raw),
            )
            self._assert_oct2_lots(result)

        # Parent root SPX on the mart row is the same position as SPXW.
        spx = _build_today_movers(
            equity,
            options_moves_df=self._oct2_production_options(symbol="SPX"),
            option_fills_df=self._oct2_statement_fills(copies=2),
        )
        self._assert_oct2_lots(spx)
        assert spx["options_impact"] != 3755.60

        movers = _build_today_movers(
            equity,
            options_moves_df=options,
            option_fills_df=self._oct2_statement_fills(copies=2),
        )
        overview = {
            "current_user": __import__("types").SimpleNamespace(is_authenticated=False),
            "title": "Overview",
            "mode": "daily",
            "week_start": date(2026, 9, 28),
            "week_end": date(2026, 10, 2),
            "user_timezone": "UTC",
            "today": date(2026, 10, 3),
            "review_date": date(2026, 10, 2),
            "review_is_today": False,
            "accounts": [],
            "selected_account": "",
            "selected_tenant": None,
            "selected_tenants": None,
            "error": None,
            "equity_snapshot": None,
            "today_snapshots_by_account": [],
            "today_strip": [],
            "expiring_options": [],
            "upcoming_earnings_this_week": [],
            "upcoming_earnings_next_week": [],
            "upcoming_ex_dividends": [],
            "today_movers": movers,
            "after_hours_movers": None,
            "today_pulse": None,
            "today_snapshots_total": None,
            "today_headline": None,
            "from_upload": False,
            "market": None,
            "market_session": {"state": "weekend", "label": "Weekend"},
            "market_open_today": False,
            "market_neutral_line": None,
            "since_last_looked": None,
            "calendar_grid": [],
            "calendar_weeks_back": 1,
            "calendar_default_weeks": 1,
            "calendar_extra_weeks": 0,
            "daily_calendar_no_query_rows": True,
            "trades_this_week": {
                "trades": [], "count": 0, "opened_count": 0,
                "closed_count": 0, "realized_pnl": 0.0,
                "unrealized_pnl": 0.0, "has_any": False,
            },
            "trades_today": {
                "trades": [], "cash": [], "count": 0, "net_cash": 0.0,
                "net_gl": 0.0, "symbols": [], "has_any": False,
            },
            "all_user_tags": [],
            "account_breakdown": {"rows": [], "totals": None, "benchmarks": []},
            "benchmark_snapshot": [],
            "overview_below_deferred": False,
            "overview_below_url": "/overview/below",
            "building_history": None,
        }
        with app.test_request_context("/weekly-review"):
            html = app.jinja_env.get_template("weekly_review.html").render(**overview)
        assert "20× 7730/7735C spread" in html
        assert "10× 7730/7735C spread" in html
        assert "+$902 closed" in html
        assert "+$976 expired" in html
        assert "Closed today" not in html
        assert "Open contracts, change in value" not in html
        assert "+$902" in html
        assert "+$976" in html
        assert "30×" not in html
        assert 'data-peek-symbol="SPXW"' in html
        assert 'data-peek-symbol="SPX"' not in html
        # The skip reason is admin-only. A signed-out render stays clean.
        assert "data-lot-split" not in html
        assert "Lot split" not in html
        with app.test_request_context("/weekly-review"):
            admin_html = app.jinja_env.get_template("weekly_review.html").render(
                **overview, is_admin_user=True,
            )
        assert 'data-lot-split="split"' in admin_html
        assert "Lot split split:" in admin_html
        assert "30×" not in admin_html
        assert 'class="mover-meta lot-split-debug"' not in admin_html
        with app.test_request_context("/weekly-review?debug=lots"):
            owner_html = app.jinja_env.get_template("weekly_review.html").render(
                **overview, debug_lots=True,
            )
        assert 'class="mover-meta lot-split-debug"' in owner_html
        assert "lot_sum=" in owner_html
        assert "gap=" in owner_html
        assert "budget=" in owner_html
        assert "fills=" in owner_html
        assert "dates=" in owner_html
        assert "roots=SPXW" in owner_html
        assert "accts=" in owner_html
        assert "data-lot-split" not in owner_html
        assert "30×" not in owner_html
        today_ctx = {
            **overview,
            "session_is_live": True,
            "delay": {"shared": "These numbers can lag your broker.", "extra": ""},
            "last_close_date": date(2026, 10, 2),
            "trades_today": {
                "trades": [], "cash": [], "count": 0, "fill_count": 0,
                "net_cash": 0.0, "net_gl": 0.0, "symbols": [], "has_any": False,
            },
        }
        with app.test_request_context("/today"):
            today_html = app.jinja_env.get_template("today.html").render(**today_ctx)
        assert "20× 7730/7735C spread" in today_html
        assert "10× 7730/7735C spread" in today_html
        assert "30×" not in today_html
        assert "+$902" in today_html
        assert "+$976" in today_html
        assert "data-lot-split" not in today_html
        with app.test_request_context("/today"):
            admin_today = app.jinja_env.get_template("today.html").render(
                **today_ctx, is_admin_user=True,
            )
        assert 'data-lot-split="split"' in admin_today
        assert "Lot split split:" in admin_today
        assert 'class="mover-meta lot-split-debug"' not in admin_today
        with app.test_request_context("/today?debug=lots"):
            owner_today = app.jinja_env.get_template("today.html").render(
                **today_ctx, debug_lots=True,
            )
        assert 'class="mover-meta lot-split-debug"' in owner_today
        assert "roots=SPXW" in owner_today
        assert "data-lot-split" not in owner_today

    def test_lot_split_does_not_add_open_mark_when_lots_match_total(self):
        """A split must still equal the mart's day move.

        When fill cash already matches the mart total, the residual
        open mark is inside that total. Adding it again turned Oct 2
        SPXW from $1,877.80 into $1,902.80.
        """
        options = self._oct2_production_options()
        short = options["direction"] == "Sold"
        long = options["direction"] == "Bought"
        options.loc[short, "open_mtm"] = 25.0
        options.loc[short, "realized_today"] = 10755.90
        options.loc[long, "open_mtm"] = 0.0
        options.loc[long, "realized_today"] = -8903.10
        assert round(
            options["open_mtm"].sum() + options["realized_today"].sum(), 2
        ) == 1877.80

        result = _build_today_movers(
            self._friday_equity(),
            options_moves_df=options,
            option_fills_df=self._oct2_statement_fills(),
        )
        spxw = [
            row for row in result["winners"] + result["losers"]
            if row["symbol"] == "SPXW"
        ]
        assert result["options_impact"] == 1877.80
        assert sum(row["dollar_impact"] for row in spxw) == 1877.80
        assert len(spxw) == 2
        by_detail = {row["contract_detail"]: row for row in spxw}
        assert by_detail["20× 7730/7735C spread"]["dollar_impact"] == 902.24
        assert by_detail["10× 7730/7735C spread"]["dollar_impact"] == 975.56

        # The open mark is extra when the lot cash matches only the
        # closed component. Keep it, and still reconcile to the mart.
        options.loc[short, "realized_today"] = 10780.90
        result = _build_today_movers(
            self._friday_equity(),
            options_moves_df=options,
            option_fills_df=self._oct2_statement_fills(),
        )
        spxw = [
            row for row in result["winners"] + result["losers"]
            if row["symbol"] == "SPXW"
        ]
        assert result["options_impact"] == 1902.80
        assert sum(row["dollar_impact"] for row in spxw) == 1902.80
        assert len(spxw) == 3
        assert any(row["dollar_impact"] == 25.0 for row in spxw)

    def test_future_expiry_open_is_not_treated_as_expired_pnl(self):
        """Opening premium is not a result while the contract is open.

        A just-opened later expiry can have no mart row yet. The fill
        splitter must not append that cash as an Expired mover.
        """
        day = date(2026, 10, 2)
        fills = pd.DataFrame([
            {
                "tenant_id": "t1", "account": "Main", "trade_date": day,
                "action": "option_buy_to_open",
                "trade_symbol": "SPY   261120C00600000",
                "underlying_symbol": "SPY",
                "option_expiry": date(2026, 11, 20),
                "quantity": 10, "price": 5.0, "amount": -5000.0, "fees": 6.50,
            },
        ])
        result = _build_today_movers(
            None, options_moves_df=pd.DataFrame(), option_fills_df=fills,
        )
        assert result["winners"] == []
        assert result["losers"] == []
        assert result["options"] == []
        assert result["options_impact"] == 0.0

        # A later expiry that actually closed today is still a close.
        fills = pd.DataFrame([
            {
                "tenant_id": "t1", "account": "Main", "trade_date": day,
                "action": "option_sell_to_open",
                "trade_symbol": "SPY   261120C00600000",
                "quantity": 10, "price": 5.0, "amount": 5000.0, "fees": 6.50,
            },
            {
                "tenant_id": "t1", "account": "Main", "trade_date": day,
                "action": "option_buy_to_close",
                "trade_symbol": "SPY   261120C00600000",
                "quantity": 10, "price": 4.0, "amount": -4000.0, "fees": 6.50,
            },
        ])
        result = _build_today_movers(
            None, options_moves_df=pd.DataFrame(), option_fills_df=fills,
        )
        assert len(result["winners"]) == 1
        assert result["winners"][0]["option_caption"] == "+$987 closed"
        assert result["winners"][0]["symbol"] == "SPY"
        assert result["options_impact"] != 0.0

    def test_later_expiry_open_does_not_block_the_oct2_split(self):
        """A new SPXW open expiring later must not join Friday's lots.

        Folding its premium into the Oct 2 cash would miss $1,877.80
        and leave the fused tile. The 10× and 20× stay, and the new
        open is not an Expired mover.
        """
        day = date(2026, 10, 2)
        fills = self._oct2_statement_fills()
        extra = pd.DataFrame([{
            "tenant_id": "snaptrade:sara",
            "account": "Sara Investment",
            "user_id": 9,
            "trade_date": day,
            "action": "option_buy_to_open",
            "trade_symbol": "SPXW  261016C07730000",
            "underlying_symbol": "SPXW",
            "quantity": 10,
            "price": 8.0,
            "amount": -8000.0,
            "fees": 12.22,
            "instrument_type": "Call",
        }])
        fills = pd.concat([fills, extra], ignore_index=True)
        result = _build_today_movers(
            self._friday_equity(),
            options_moves_df=self._oct2_production_options(),
            option_fills_df=fills,
        )
        self._assert_oct2_lots(result)
        captions = [
            row.get("option_caption")
            for row in result["winners"] + result["losers"]
        ]
        assert captions.count("+$976 expired") == 1

    def test_posting_day_opens_of_a_friday_expiry_split(self, caplog):
        """The 10× expired with no close, so nothing caps its open back to Friday.

        A next-day trade_date (UTC, or the broker post) still belongs on
        the expiry session. The mart dollar is already the net credit.
        """
        result = _build_today_movers(
            self._friday_equity(),
            options_moves_df=self._oct2_production_options(),
            option_fills_df=self._oct2_statement_fills(trade_date=date(2026, 10, 3)),
        )
        self._assert_oct2_lots(result)
        with caplog.at_level(logging.INFO):
            _build_today_movers(
                self._friday_equity(),
                options_moves_df=self._oct2_production_options(),
                option_fills_df=self._oct2_statement_fills(trade_date=date(2026, 10, 3)),
            )
        assert "reason=split" not in caplog.text
        assert "lot_split" not in caplog.text

    def test_gross_premium_with_blank_fees_nets_to_the_two_lots(self):
        """Fees column 0 and gross amounts miss the mart by exactly $122.20.

        The budget was $0, so the $122.20 gap kept the fused 30× tile.
        The implied $1.222 per contract is the missing commission.
        """
        day = date(2026, 10, 2)
        short = "SPXW  261002C07730000"
        long = "SPXW  261002C07735000"
        legs = [
            ("option_sell_to_open", short, 20, 6.85, 13700.00),
            ("option_buy_to_open", long, 20, 5.35, -10700.00),
            ("option_sell_to_close", long, 20, 1.824, 3648.00),
            ("option_buy_to_close", short, 20, 2.824, -5648.00),
            ("option_sell_to_open", short, 10, 2.79, 2790.00),
            ("option_buy_to_open", long, 10, 1.79, -1790.00),
        ]
        fills = pd.DataFrame([
            {
                "tenant_id": "snaptrade:sara",
                "account": "Sara Investment",
                "trade_date": day,
                "action": action,
                "trade_symbol": occ,
                "quantity": qty,
                "price": price,
                "amount": gross,
                "fees": 0,
            }
            for action, occ, qty, price, gross in legs
        ])
        result = _build_today_movers(
            self._friday_equity(),
            options_moves_df=self._oct2_production_options(),
            option_fills_df=fills,
        )
        self._assert_oct2_lots(result)

    def test_fills_on_another_day_keep_the_fused_tile(self, caplog):
        """A later expiry dated the next day is not Friday's SPXW lot."""
        fills = self._oct2_statement_fills(trade_date=date(2026, 10, 3))
        fills["trade_symbol"] = fills["trade_symbol"].replace({
            "SPXW  261002C07730000": "SPXW  261016C07730000",
            "SPXW  261002C07735000": "SPXW  261016C07735000",
        })
        result = _build_today_movers(
            self._friday_equity(),
            options_moves_df=self._oct2_production_options(),
            option_fills_df=fills,
        )
        spxw = [r for r in result["winners"] if r["symbol"] == "SPXW"]
        assert len(spxw) == 1
        assert "30×" in spxw[0]["contract_detail"]
        assert spxw[0]["dollar_impact"] == 1877.80
        assert spxw[0]["lot_split_reason"] == "no_session_fills"
        assert "anchor=2026-10-02" in spxw[0]["lot_split_detail"]
        assert "2026-10-03" in spxw[0]["lot_split_detail"]
        line = spxw[0]["lot_split_line"]
        assert line.startswith("no_session_fills ")
        assert "lot_sum=—" in line
        assert "gap=—" in line
        assert "budget=—" in line
        assert "fills=0/" in line
        assert "dates=2026-10-03" in line
        assert "roots=SPXW" in line
        assert "accts=" in line

        with caplog.at_level(logging.INFO):
            _build_today_movers(
                self._friday_equity(),
                options_moves_df=self._oct2_production_options(),
                option_fills_df=fills,
            )
        assert "reason=no_session_fills" in caplog.text

    def test_missing_fills_name_no_fills(self):
        result = _build_today_movers(
            self._friday_equity(),
            options_moves_df=self._oct2_production_options(),
            option_fills_df=None,
        )
        spxw = [r for r in result["winners"] if r["symbol"] == "SPXW"]
        assert len(spxw) == 1
        assert "30×" in spxw[0]["contract_detail"]
        assert spxw[0]["lot_split_reason"] == "no_fills"
        assert "fills=0" in spxw[0]["lot_split_detail"]

    def test_unparsed_session_fill_names_the_skip(self):
        day = date(2026, 10, 2)
        fills = pd.DataFrame([{
            "tenant_id": "t1", "account": "Sara", "trade_date": day,
            "action": "option_sell_to_open",
            "trade_symbol": "not-an-occ",
            "quantity": 1, "price": 1.0, "amount": 100.0, "fees": 0,
        }])
        result = _build_today_movers(
            self._friday_equity(),
            options_moves_df=self._oct2_production_options(),
            option_fills_df=fills,
        )
        spxw = [r for r in result["winners"] if r["symbol"] == "SPXW"]
        assert spxw[0]["lot_split_reason"] == "unparsed_symbol"

    def test_unrelated_root_is_no_lots(self):
        result = _build_today_movers(
            self._friday_equity(),
            options_moves_df=self._oct2_production_options(symbol="QQQ"),
            option_fills_df=self._oct2_statement_fills(),
        )
        qqq = [
            r for r in result["winners"] + result["losers"]
            if r["symbol"] == "QQQ"
        ]
        assert len(qqq) == 1
        assert qqq[0]["lot_split_reason"] == "no_lots"
        assert "lot_roots=SPXW" in qqq[0]["lot_split_detail"]
        assert qqq[0]["dollar_impact"] == 1877.80

    def test_other_roots_cannot_take_the_spxw_longs(self):
        """ASTS and BE are earlier in the day frame and match the lot qty.

        Pairing used to give them the SPXW longs. The SPXW tile then
        summed only the short legs (10,780.90) and the gap was the
        missing 7735C long (−8,903.10). The fused realized_pnl on the
        close rows must not be the lot cash either.
        """
        day = date(2026, 10, 2)
        short = "SPXW  261002C07730000"
        long = "SPXW  261002C07735000"

        def fill(action, occ, qty, price, amount, fee, realized=None):
            return {
                "tenant_id": "snaptrade:sara",
                "account": "Sara Investment",
                "trade_date": day,
                "action": action,
                "trade_symbol": occ,
                "underlying_symbol": occ.split()[0],
                "quantity": qty,
                "price": price,
                "amount": amount,
                "fees": fee,
                "realized_pnl": realized,
            }

        rows = [
            fill("option_sell_to_open", "ASTS  261002C00025000", 20, 2.0, 4000.0, 0),
            fill("option_buy_to_open", "ASTS  261002C00026000", 4, 1.0, -400.0, 0),
            fill("option_sell_to_open", "BE    261002C00297500", 10, 1.0, 1000.0, 0),
            fill("option_buy_to_open", "BE    261002C00300000", 3, 0.5, -150.0, 0),
            fill("option_sell_to_open", "GEV   261002C00500000", 1, 0.5, 50.0, 0),
            fill("option_sell_to_open", "IRD   261002C00010000", 1, 0.5, 50.0, 0),
            fill("option_sell_to_open", "MSOS  261002C00005000", 1, 0.5, 50.0, 0),
            fill("option_sell_to_open", short, 20, 6.85, 13675.56, 24.44),
            fill("option_buy_to_open", long, 20, 5.35, -10724.44, 24.44),
            fill("option_sell_to_close", long, 20, 1.824, 3623.56, 24.44, -8903.10),
            fill("option_buy_to_close", short, 20, 2.824, -5672.44, 24.44, 10780.90),
            fill("option_sell_to_open", short, 10, 2.79, 2777.78, 12.22),
            fill("option_buy_to_open", long, 10, 1.79, -1802.22, 12.22),
        ]
        options = pd.DataFrame([
            {
                "symbol": "SPXW", "trade_symbol": short,
                "today_date": day, "tenant_id": "snaptrade:sara",
                "option_strike": 7730, "option_type": "C",
                "option_expiry": day, "direction": "Sold",
                "quantity": 30, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": 10780.90,
            },
            {
                "symbol": "SPXW", "trade_symbol": long,
                "today_date": day, "tenant_id": "snaptrade:sara",
                "option_strike": 7735, "option_type": "C",
                "option_expiry": day, "direction": "Bought",
                "quantity": 30, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": -8903.10,
            },
            {
                "symbol": "BE", "trade_symbol": "BE    261002C00297500",
                "today_date": day, "tenant_id": "snaptrade:sara",
                "option_strike": 297.5, "option_type": "C",
                "option_expiry": day, "direction": "Sold",
                "quantity": 10, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": 850.0,
            },
            {
                "symbol": "ASTS", "trade_symbol": "ASTS  261002C00025000",
                "today_date": day, "tenant_id": "snaptrade:sara",
                "option_strike": 25, "option_type": "C",
                "option_expiry": day, "direction": "Sold",
                "quantity": 20, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": 3600.0,
            },
        ])
        result = _build_today_movers(
            self._friday_equity(),
            options_moves_df=options,
            option_fills_df=pd.DataFrame(rows),
        )
        spxw = [r for r in result["winners"] if r["symbol"] == "SPXW"]
        by_detail = {r["contract_detail"]: r for r in spxw}
        assert by_detail["20× 7730/7735C spread"]["dollar_impact"] == 902.24
        assert by_detail["10× 7730/7735C spread"]["dollar_impact"] == 975.56
        assert all(r["lot_split_reason"] == "split" for r in spxw)
        for row in spxw:
            assert "roots=SPXW" in row["lot_split_line"]
            assert "ASTS" not in row["lot_split_line"]
            assert "10780.90" not in row["lot_split_line"]
            assert "8903.10" not in row["lot_split_line"]
        be = [r for r in result["winners"] if r["symbol"] == "BE"]
        asts = [r for r in result["winners"] if r["symbol"] == "ASTS"]
        assert len(be) == 1 and be[0]["dollar_impact"] == 850.0
        assert len(asts) == 1 and asts[0]["dollar_impact"] == 3600.0
        assert "roots=BE" in be[0]["lot_split_line"]
        assert "roots=ASTS" in asts[0]["lot_split_line"]

    def test_expiry_credit_is_settlement_cash_not_realized_pnl(self):
        day = date(2026, 10, 2)
        short = "SPXW  261002C07730000"
        long = "SPXW  261002C07735000"
        fills = self._oct2_statement_fills()
        extra = {
            "tenant_id": "snaptrade:sara",
            "account": "Sara Investment",
            "user_id": 9,
            "trade_date": day,
            "action": "option_expired",
            "trade_symbol": long,
            "underlying_symbol": "SPXW",
            "quantity": 10,
            "price": 0,
            "amount": 50.0,
            "fees": 0,
            "realized_pnl": -8903.10,
        }
        fills = pd.concat([fills, pd.DataFrame([extra])], ignore_index=True)
        options = self._oct2_production_options()
        options.loc[options["direction"] == "Sold", "realized_today"] = 1927.80
        options.loc[options["direction"] == "Bought", "realized_today"] = 0.0
        # The fused mart dollar is the two lots after the $50 credit.
        both = options["realized_today"].sum()
        assert both == 1927.80
        result = _build_today_movers(
            self._friday_equity(),
            options_moves_df=options,
            option_fills_df=fills,
        )
        spxw = [r for r in result["winners"] if r["symbol"] == "SPXW"]
        by_detail = {r["contract_detail"]: r for r in spxw}
        assert by_detail["20× 7730/7735C spread"]["dollar_impact"] == 902.24
        assert by_detail["10× 7730/7735C spread"]["dollar_impact"] == 1025.56

    def test_debug_line_masks_names_and_amounts(self, monkeypatch):
        monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: True)
        monkeypatch.setattr(
            "app.privacy.shown_account",
            lambda name, tenant_id=None: "Account 2",
        )
        from app.weekly_review import _lot_fill_census, _lot_split_line

        day = date(2026, 10, 2)
        fills = self._oct2_statement_fills()
        census = _lot_fill_census(fills, day)
        line = _lot_split_line(
            "total_mismatch", census,
            lot_sum=10780.90, gap=8903.10, budget=122.20,
        )
        assert "Account 2" in line
        assert "Sara" not in line
        assert "10780.90" not in line
        assert "8903.10" not in line
        assert "122.20" not in line
        assert "lot_sum=••••" in line
        assert "gap=••••" in line
        assert "budget=••••" in line

    def test_option_caption_matches_what_the_row_contains(self):
        opt = pd.DataFrame([
            {
                "symbol": "MU", "trade_symbol": "MU    261016C00120000",
                "today_date": date(2026, 10, 1), "tenant_id": "t1",
                "option_strike": 120, "option_type": "Call",
                "option_expiry": date(2026, 10, 16), "direction": "Sold",
                "quantity": 2, "open_mtm": -40, "prev_open_mtm": 0,
                "realized_today": 0,
            },
            {
                "symbol": "AAPL", "trade_symbol": "AAPL  261016C00200000",
                "today_date": date(2026, 10, 1), "tenant_id": "t1",
                "option_strike": 200, "option_type": "C",
                "option_expiry": date(2026, 10, 16), "direction": "Bought",
                "quantity": 1, "open_mtm": 450, "prev_open_mtm": 0,
                "realized_today": -120,
            },
        ])
        result = _build_today_movers(None, options_moves_df=opt)
        by_sym = {r["symbol"]: r for r in result["winners"] + result["losers"]}
        assert by_sym["MU"]["contract_detail"] == "2× MU 120C"
        assert by_sym["MU"]["option_caption"] == "−$40 open"
        assert by_sym["MU"]["dollar_impact"] == -40
        assert by_sym["AAPL"]["option_caption"] == "+$450 open · −$120 closed"
        assert by_sym["AAPL"]["dollar_impact"] == 330
        assert by_sym["AAPL"]["contract_detail"] == "1× AAPL 200C"

    def test_day_option_query_uses_the_same_close(self):
        normalized = " ".join(DAY_OPTIONS_MOVES_QUERY.lower().split())
        assert "int_option_contract_daily_pnl" in normalized
        assert "is_realized_close" in normalized
        assert "cur.date = @day" in normalized
        assert "mart_daily_pnl" not in normalized

    def test_mover_templates_use_the_plain_option_line(self):
        from app import app

        movers = _build_today_movers(None, options_moves_df=pd.DataFrame([
            {
                "symbol": "SPXW", "trade_symbol": "SPXW  261001C07650000",
                "today_date": date(2026, 10, 1), "tenant_id": "t1",
                "option_strike": 7650, "option_type": "C",
                "option_expiry": date(2026, 10, 1), "direction": "Sold",
                "quantity": 10, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": -2242.22,
            },
            {
                "symbol": "SPXW", "trade_symbol": "SPXW  261001C07655000",
                "today_date": date(2026, 10, 1), "tenant_id": "t1",
                "option_strike": 7655, "option_type": "C",
                "option_expiry": date(2026, 10, 1), "direction": "Bought",
                "quantity": 10, "open_mtm": 0, "prev_open_mtm": 0,
                "realized_today": -1332.22,
            },
            {
                "symbol": "MU", "trade_symbol": "MU    261016C00120000",
                "today_date": date(2026, 10, 1), "tenant_id": "t1",
                "option_strike": 120, "option_type": "C",
                "option_expiry": date(2026, 10, 16), "direction": "Sold",
                "quantity": 2, "open_mtm": 80, "prev_open_mtm": 0,
                "realized_today": 0,
            },
        ]))
        overview = {
            "current_user": __import__("types").SimpleNamespace(is_authenticated=False),
            "title": "Overview",
            "mode": "daily",
            "week_start": date(2026, 9, 28),
            "week_end": date(2026, 10, 2),
            "user_timezone": "UTC",
            "today": date(2026, 10, 2),
            "review_date": date(2026, 10, 1),
            "review_is_today": False,
            "accounts": [],
            "selected_account": "",
            "selected_tenant": None,
            "selected_tenants": None,
            "error": None,
            "equity_snapshot": None,
            "today_snapshots_by_account": [],
            "today_strip": [],
            "expiring_options": [],
            "upcoming_earnings_this_week": [],
            "upcoming_earnings_next_week": [],
            "upcoming_ex_dividends": [],
            "today_movers": movers,
            "after_hours_movers": None,
            "today_pulse": None,
            "today_snapshots_total": None,
            "today_headline": None,
            "from_upload": False,
            "market": None,
            "market_session": {"state": "closed", "label": "Closed"},
            "market_open_today": False,
            "market_neutral_line": None,
            "since_last_looked": None,
            "calendar_grid": [],
            "calendar_weeks_back": 1,
            "calendar_default_weeks": 1,
            "calendar_extra_weeks": 0,
            "daily_calendar_no_query_rows": True,
            "trades_this_week": {
                "trades": [], "count": 0, "opened_count": 0,
                "closed_count": 0, "realized_pnl": 0.0,
                "unrealized_pnl": 0.0, "has_any": False,
            },
            "trades_today": {
                "trades": [], "cash": [], "count": 0, "net_cash": 0.0,
                "net_gl": 0.0, "symbols": [], "has_any": False,
            },
            "all_user_tags": [],
            "account_breakdown": {"rows": [], "totals": None, "benchmarks": []},
            "benchmark_snapshot": [],
            "overview_below_deferred": False,
            "overview_below_url": "/overview/below",
            "building_history": None,
        }
        with app.test_request_context("/overview"):
            html = app.jinja_env.get_template("weekly_review.html").render(**overview)
        assert "10× 7650/7655C spread" in html
        assert "−$3,574 closed" in html
        assert "2× MU 120C" in html
        assert "+$80 open" in html
        assert "Closed today" not in html
        assert "Open contracts, change in value" not in html
        assert "P&amp;L on contracts" not in html
        assert "Marks + closes" not in html
        assert "$3,574" in html
        today_ctx = {
            **overview,
            "session_is_live": True,
            "delay": {"shared": "These numbers can lag your broker.", "extra": ""},
            "last_close_date": date(2026, 10, 1),
            "trades_today": {
                "trades": [], "cash": [], "count": 0, "fill_count": 0,
                "net_cash": 0.0, "net_gl": 0.0, "symbols": [], "has_any": False,
            },
        }
        with app.test_request_context("/today"):
            today_html = app.jinja_env.get_template("today.html").render(**today_ctx)
        assert "10× 7650/7655C spread" in today_html
        assert "−$3,574 closed" in today_html
        assert "+$80 open" in today_html
        assert "Closed today" not in today_html
        assert "Open contracts, change in value" not in today_html
        assert "Marks + closes" not in today_html


class TestBuildAfterHoursMovers:
    """After-hours movers: broker mark (last sync) vs today's official close.

    Close-based reporting surfaces this drift separately so it informs
    without polluting the core numbers.
    """

    def test_empty_input(self):
        result = _build_after_hours_movers(None)
        assert result == {"winners": [], "losers": [], "total_impact": 0.0, "as_of": None}
        assert _build_after_hours_movers(pd.DataFrame())["winners"] == []

    def test_splits_winners_and_losers_with_as_of(self):
        df = pd.DataFrame([
            {"symbol": "NVDA", "shares": 100, "broker_mark": 211.0,
             "today_close": 208.0, "price_change": 3.0,
             "price_change_pct": 1.44, "dollar_impact": 300.0,
             "snapshot_date": date(2026, 6, 23)},
            {"symbol": "AAPL", "shares": 50, "broker_mark": 296.0,
             "today_close": 297.0, "price_change": -1.0,
             "price_change_pct": -0.34, "dollar_impact": -50.0,
             "snapshot_date": date(2026, 6, 23)},
        ])
        result = _build_after_hours_movers(df)
        assert [w["symbol"] for w in result["winners"]] == ["NVDA"]
        assert [l["symbol"] for l in result["losers"]] == ["AAPL"]
        assert result["winners"][0]["broker_mark"] == 211.0
        assert result["winners"][0]["today_close"] == 208.0
        assert result["total_impact"] == 250.0
        assert result["as_of"] == "2026-06-23"

    def test_zero_drift_filtered_out(self):
        # Broker mark == close (no after-hours move) → not surfaced.
        df = pd.DataFrame([
            {"symbol": "MSFT", "shares": 10, "broker_mark": 400.0,
             "today_close": 400.0, "price_change": 0.0,
             "price_change_pct": 0.0, "dollar_impact": 0.0,
             "snapshot_date": date(2026, 6, 23)},
        ])
        result = _build_after_hours_movers(df)
        assert result["winners"] == []
        assert result["losers"] == []
        # as_of still threads through so the label can show last sync date.
        assert result["as_of"] == "2026-06-23"


class TestBuildUpcomingDividends:
    def test_empty_input(self):
        assert _build_upcoming_dividends(None) == []
        assert _build_upcoming_dividends(pd.DataFrame()) == []

    def test_sorted_by_days_until(self):
        # days_until is computed in Python vs the caller's (user-tz) today,
        # NOT read from SQL — UTC CURRENT_DATE() day counts go stale/off-by-one.
        df = pd.DataFrame([
            {"symbol": "JEPI", "last_ex_div_date": date(2026, 4, 15),
             "last_amount_per_share": 0.45, "median_spacing_days": 30,
             "projected_next_ex_div_date": date(2026, 5, 25),
             "sector": "Financial Services", "subsector": "Asset Management",
             "long_name": "JPMorgan EPI"},
            {"symbol": "SCHD", "last_ex_div_date": date(2026, 3, 20),
             "last_amount_per_share": 0.78, "median_spacing_days": 91,
             "projected_next_ex_div_date": date(2026, 6, 18),
             "sector": "Financial Services", "subsector": "Asset Management",
             "long_name": "Schwab US Dividend ETF"},
            {"symbol": "BKH", "last_ex_div_date": date(2026, 3, 1),
             "last_amount_per_share": 0.665, "median_spacing_days": 91,
             "projected_next_ex_div_date": date(2026, 5, 31),
             "sector": "Utilities", "subsector": "Diversified Utilities",
             "long_name": "Black Hills"},
        ])
        rows = _build_upcoming_dividends(df, today=date(2026, 5, 18))
        # Sorted by days_until ascending: 7, 13, 31 (32+ is outside the
        # watch-list window and is dropped — same bound as the SQL).
        assert [r["symbol"] for r in rows] == ["JEPI", "BKH", "SCHD"]
        assert [r["days_until"] for r in rows] == [7, 13, 31]
        assert all(r["source"] == "heuristic" for r in rows)

    def test_past_projection_rolls_forward_for_monthly_payer(self):
        # JEPI's last+median step can already be in the past when the
        # price feed missed the latest ex-div (JEPQ still projects). Roll
        # last+n*spacing forward instead of dropping the row.
        df = pd.DataFrame([
            {"symbol": "JEPI", "last_ex_div_date": date(2026, 8, 1),
             "last_amount_per_share": 0.45, "median_spacing_days": 30,
             "projected_next_ex_div_date": date(2026, 8, 9),
             "sector": "", "subsector": "", "long_name": "JPMorgan EPI"},
            {"symbol": "SCHD", "last_ex_div_date": date(2026, 7, 1),
             "last_amount_per_share": 0.78, "median_spacing_days": 91,
             "projected_next_ex_div_date": date(2026, 8, 10),
             "sector": "", "subsector": "", "long_name": "Schwab Dividend"},
        ])
        rows = _build_upcoming_dividends(df, today=date(2026, 8, 10))
        assert [r["symbol"] for r in rows] == ["SCHD", "JEPI"]
        assert rows[0]["days_until"] == 0
        assert rows[1]["symbol"] == "JEPI"
        assert rows[1]["projected_date"] == "2026-08-31"
        assert rows[1]["days_until"] == 21
        assert all(r["source"] == "heuristic" for r in rows)

    def test_calendar_date_beats_stale_heuristic(self):
        """JEPI-shaped: last+median is in the past, but yfinance calendar
        already has the declared next ex-div. Calendar wins."""
        heuristic = pd.DataFrame([
            {"symbol": "JEPI", "last_ex_div_date": date(2026, 7, 1),
             "last_amount_per_share": 0.45, "median_spacing_days": 30,
             "projected_next_ex_div_date": date(2026, 7, 31),
             "sector": "", "subsector": "", "long_name": "JPMorgan EPI"},
        ])
        calendar = pd.DataFrame([{
            "symbol": "JEPI",
            "next_ex_div_date": date(2026, 9, 2),
            "next_dividend_pay_date": date(2026, 9, 5),
        }])
        rows = _build_upcoming_dividends(
            heuristic, today=date(2026, 8, 25), calendar_df=calendar,
        )
        assert len(rows) == 1
        assert rows[0]["projected_date"] == "2026-09-02"
        assert rows[0]["source"] == "calendar"
        assert rows[0]["last_amount_per_share"] == 0.45

    def test_past_calendar_date_falls_back_to_heuristic_roll(self):
        heuristic = pd.DataFrame([
            {"symbol": "JEPI", "last_ex_div_date": date(2026, 7, 1),
             "last_amount_per_share": 0.45, "median_spacing_days": 30,
             "projected_next_ex_div_date": date(2026, 7, 31),
             "sector": "", "subsector": "", "long_name": "JPMorgan EPI"},
        ])
        calendar = pd.DataFrame([{
            "symbol": "JEPI",
            "next_ex_div_date": date(2026, 8, 1),
            "next_dividend_pay_date": None,
        }])
        rows = _build_upcoming_dividends(
            heuristic, today=date(2026, 8, 25), calendar_df=calendar,
        )
        assert rows[0]["projected_date"] == "2026-08-30"
        assert rows[0]["source"] == "heuristic"

    def test_calendar_only_symbol_still_renders(self):
        """Held symbol with no heuristic row (no recent prints) but a
        future calendar date still appears on the watch list."""
        calendar = pd.DataFrame([{
            "symbol": "SCHD",
            "next_ex_div_date": date(2026, 9, 15),
            "next_dividend_pay_date": date(2026, 9, 22),
        }])
        rows = _build_upcoming_dividends(
            pd.DataFrame(), today=date(2026, 8, 25), calendar_df=calendar,
        )
        assert len(rows) == 1
        assert rows[0]["symbol"] == "SCHD"
        assert rows[0]["projected_date"] == "2026-09-15"
        assert rows[0]["source"] == "calendar"
        assert rows[0]["last_amount_per_share"] == 0.0

    def test_past_calendar_only_is_dropped(self):
        calendar = pd.DataFrame([{
            "symbol": "SCHD",
            "next_ex_div_date": date(2026, 8, 1),
            "next_dividend_pay_date": None,
        }])
        rows = _build_upcoming_dividends(
            None, today=date(2026, 8, 25), calendar_df=calendar,
        )
        assert rows == []

    def test_next_ex_div_rolls_stale_last_event(self):
        # Last event July 1, 30d cadence, today Aug 22 → Aug 30.
        nxt = _next_ex_div_on_or_after(date(2026, 7, 1), 30, date(2026, 8, 22))
        assert nxt == date(2026, 8, 30)

    def test_far_next_cycle_dropped(self):
        df = pd.DataFrame([
            {"symbol": "OLD", "last_ex_div_date": date(2026, 1, 1),
             "last_amount_per_share": 0.5, "median_spacing_days": 91,
             "projected_next_ex_div_date": date(2026, 4, 2),
             "sector": "", "subsector": "", "long_name": ""},
        ])
        rows = _build_upcoming_dividends(df, today=date(2026, 8, 10))
        assert rows == []

    def test_est_income_is_last_amount_times_shares_held(self):
        # Sep 2026: estimated income projection off the most recent declared
        # per-share amount × shares currently held.
        df = pd.DataFrame([
            {"symbol": "JEPI", "last_ex_div_date": date(2026, 8, 1),
             "last_amount_per_share": 0.45, "shares_held": 200,
             "median_spacing_days": 30,
             "projected_next_ex_div_date": date(2026, 8, 9),
             "sector": "", "subsector": "", "long_name": "JPMorgan EPI"},
        ])
        rows = _build_upcoming_dividends(df, today=date(2026, 8, 5))
        assert rows[0]["shares_held"] == 200.0
        assert rows[0]["est_income"] == 90.0

    def test_est_income_zero_when_shares_held_missing(self):
        # Calendar-only symbols have no shares_held column at all — must not
        # crash, and est_income should come back 0 rather than NaN/None.
        calendar = pd.DataFrame([{
            "symbol": "SCHD",
            "next_ex_div_date": date(2026, 9, 15),
            "next_dividend_pay_date": date(2026, 9, 22),
        }])
        rows = _build_upcoming_dividends(
            pd.DataFrame(), today=date(2026, 8, 25), calendar_df=calendar,
        )
        assert rows[0]["shares_held"] == 0.0
        assert rows[0]["est_income"] == 0.0

    def test_est_income_never_treats_short_shares_as_income(self):
        df = pd.DataFrame([
            {"symbol": "JEPI", "last_ex_div_date": date(2026, 8, 1),
             "last_amount_per_share": 0.45, "shares_held": -200,
             "median_spacing_days": 30,
             "projected_next_ex_div_date": date(2026, 8, 9),
             "sector": "", "subsector": "", "long_name": "JPMorgan EPI"},
        ])

        rows = _build_upcoming_dividends(df, today=date(2026, 8, 5))

        assert rows[0]["shares_held"] == 0.0
        assert rows[0]["est_income"] == 0.0


def test_upcoming_dividend_income_queries_dedupe_and_only_sum_long_shares():
    from app.weekly_review import (
        EX_DIV_CALENDAR_QUERY,
        UPCOMING_DIVIDENDS_QUERY,
    )

    for query in (UPCOMING_DIVIDENDS_QUERY, EX_DIV_CALENDAR_QUERY):
        assert (
            "SUM(CASE WHEN quantity > 0 THEN quantity ELSE 0 END) AS shares_held"
            in query
        )
        # Short-only positions still belong in the ex-div watch list because
        # they owe the distribution; they simply must not become income.
        assert "quantity != 0" in query
        # int_enriched_current has emitted duplicate position rows during
        # source/staging regressions. Collapse its canonical tenant/position
        # grain before quantity is aggregated into the estimate.
        assert "ROW_NUMBER() OVER (" in query
        assert "WHERE position_rn = 1" in query
        assert "COALESCE(\n                    tenant_id," in query


class TestTodayHeadline:
    def test_no_pulse_returns_none(self):
        assert _today_headline(None, None, None) is None

    def test_with_pct(self):
        pulse = {"delta": 1500.0, "positive": True, "date": "2026-05-18"}
        snap = {"account_value": 100000.0}
        s = _today_headline(pulse, None, snap)
        assert s is not None
        assert "+$1,500" in s
        # 1500 / (100000 - 1500) = 1.52%
        assert "1.52%" in s

    def test_negative_delta(self):
        pulse = {"delta": -2100.0, "positive": False, "date": "2026-05-18"}
        snap = {"account_value": 100000.0}
        s = _today_headline(pulse, None, snap)
        assert "-$2,100" in s


class TestFormatTradeContract:
    def test_parses_osi_call(self):
        # Real shape from stg_history: "ASTS  260605C00102000".
        assert _format_trade_contract("ASTS  260605C00102000", "ASTS") == "ASTS Jun 5 $102 Call"

    def test_parses_osi_put(self):
        assert _format_trade_contract("BE    260605P00285000", "BE") == "BE Jun 5 $285 Put"

    def test_fractional_strike(self):
        assert _format_trade_contract("GOOG  260529C00382500", "GOOG") == "GOOG May 29 $382.5 Call"

    def test_equity_session_falls_back_to_symbol(self):
        assert _format_trade_contract("COHR_session_1", "COHR") == "COHR"

    def test_unparseable_returns_compacted_raw(self):
        assert _format_trade_contract("WEIRD VALUE", "X") == "WEIRD VALUE"

    def test_empty_returns_symbol(self):
        assert _format_trade_contract("", "AAPL") == "AAPL"
        assert _format_trade_contract(None, "AAPL") == "AAPL"


class TestPickTradesWeekFrame:
    """Monday of a new ISO week should still show last week's taggable rows."""

    def test_prefers_this_week(self):
        this = date(2026, 8, 31)
        df = pd.DataFrame([
            {"week_start": this, "symbol": "A"},
            {"week_start": date(2026, 8, 24), "symbol": "B"},
        ])
        out, start, is_prior = _pick_trades_week_frame(df, this)
        assert is_prior is False
        assert start == this
        assert list(out["symbol"]) == ["A"]

    def test_falls_back_to_last_week(self):
        this = date(2026, 8, 31)
        prior = date(2026, 8, 24)
        df = pd.DataFrame([{"week_start": prior, "symbol": "NVDA"}])
        out, start, is_prior = _pick_trades_week_frame(df, this)
        assert is_prior is True
        assert start == prior
        assert list(out["symbol"]) == ["NVDA"]

    def test_empty_stays_this_week(self):
        this = date(2026, 8, 31)
        out, start, is_prior = _pick_trades_week_frame(pd.DataFrame(), this)
        assert is_prior is False
        assert start == this


class TestBuildTradesThisWeek:
    WEEK_START = date(2026, 6, 8)
    WEEK_END = date(2026, 6, 14)

    def _row(self, **kw):
        base = {
            "tenant_id": "snaptrade:abc", "account": "Schwab Account",
            "symbol": "ASTS", "trade_symbol": "ASTS  260605C00102000",
            "strategy": "Covered Call", "status": "Closed",
            "open_date": date(2026, 6, 5), "close_date": date(2026, 6, 8),
            "total_pnl": 226.0, "trade_cost": 226.0, "num_trades": 2,
            "current_unrealized_pnl": 0.0, "current_market_value": 0.0,
        }
        base.update(kw)
        return base

    def test_empty(self):
        out = _build_trades_this_week(None, self.WEEK_START, self.WEEK_END)
        assert out["has_any"] is False
        assert out["trades"] == []
        assert out["count"] == 0

    def test_single_symbol_one_contract(self):
        df = pd.DataFrame([self._row()])
        out = _build_trades_this_week(
            df, self.WEEK_START, self.WEEK_END, label_map={"snaptrade:abc": "Sara Investment"}
        )
        assert out["count"] == 1
        assert out["closed_count"] == 1
        assert out["opened_count"] == 0
        assert out["realized_pnl"] == 226.0
        r = out["trades"][0]
        assert r["is_closed"] is True
        assert r["status"] == "Closed"
        assert r["result_kind"] == "realized"
        assert r["result_pnl"] == 226.0
        assert r["account_display"] == "Sara Investment"
        # Single leg → show the actual contract name, not a count.
        assert r["contract"] == "ASTS Jun 5 $102 Call"
        assert r["num_legs"] == 1

    def test_two_contracts_same_symbol_net_to_one_row(self):
        # The core fix: a trader writes a fresh weekly call on ASTS each week,
        # so two different ASTS contracts must NET into ONE symbol row.
        df = pd.DataFrame([
            self._row(trade_symbol="ASTS  260605C00102000", status="Closed",
                      open_date=date(2026, 6, 5), close_date=date(2026, 6, 8),
                      total_pnl=226.0),
            self._row(trade_symbol="ASTS  260612C00098000", status="Closed",
                      open_date=date(2026, 6, 10), close_date=date(2026, 6, 12),
                      total_pnl=-58.0),
        ])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        assert out["count"] == 1
        assert out["closed_count"] == 1
        r = out["trades"][0]
        assert r["symbol"] == "ASTS"
        assert r["num_legs"] == 2
        assert r["contract"] == "2 contracts"
        assert r["is_closed"] is True
        assert r["result_kind"] == "realized"
        assert r["realized_pnl"] == 168.0  # 226 - 58
        assert r["result_pnl"] == 168.0
        assert out["realized_pnl"] == 168.0

    def test_open_contract_shows_unrealized(self):
        # All-open symbol → unrealized G/L at the latest snapshot.
        df = pd.DataFrame([self._row(
            symbol="OPEN", trade_symbol="OPEN  260619C00050000", status="Open",
            open_date=date(2026, 6, 9), close_date=None, num_trades=1,
            total_pnl=0.0, current_unrealized_pnl=140.0, current_market_value=300.0,
        )])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        assert out["opened_count"] == 1
        assert out["closed_count"] == 0
        assert out["unrealized_pnl"] == 140.0
        r = out["trades"][0]
        assert r["is_closed"] is False
        assert r["status"] == "Open"
        assert r["result_kind"] == "unrealized"
        assert r["result_pnl"] == 140.0

    def test_mixed_open_and_closed_same_symbol_is_open_net(self):
        # One ASTS contract closed this week (+200 realized) and another
        # still open (+50 unrealized) → ONE row, status Open, result is the
        # NET of both, tagged "net".
        df = pd.DataFrame([
            self._row(trade_symbol="ASTS  260605C00100000", status="Closed",
                      open_date=date(2026, 6, 5), close_date=date(2026, 6, 10),
                      total_pnl=200.0),
            self._row(trade_symbol="ASTS  260619C00110000", status="Open",
                      open_date=date(2026, 6, 9), close_date=None, num_trades=1,
                      total_pnl=0.0, current_unrealized_pnl=50.0,
                      current_market_value=120.0),
        ])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        assert out["count"] == 1
        assert out["closed_count"] == 0
        assert out["opened_count"] == 1
        r = out["trades"][0]
        assert r["status"] == "Open"
        assert r["is_closed"] is False
        assert r["realized_pnl"] == 200.0
        assert r["unrealized_pnl"] == 50.0
        assert r["result_pnl"] == 250.0
        assert r["result_kind"] == "net"
        assert out["realized_pnl"] == 200.0
        assert out["unrealized_pnl"] == 50.0

    def test_opened_this_week_hides_synthetic_zero_trade_rows(self):
        df = pd.DataFrame([
            self._row(symbol="NEW", trade_symbol="NEW_session_1", status="Open",
                      open_date=date(2026, 6, 9), close_date=None, num_trades=1),
            self._row(symbol="SYN", trade_symbol="SYN_session_1", status="Open",
                      open_date=date(2026, 6, 9), close_date=None, num_trades=0),
        ])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        syms = {r["symbol"] for r in out["trades"]}
        assert "NEW" in syms
        assert "SYN" not in syms  # num_trades==0 synthetic snapshot open

    def test_different_symbols_stay_separate(self):
        df = pd.DataFrame([
            self._row(symbol="ASTS", trade_symbol="ASTS  260605C00102000",
                      close_date=date(2026, 6, 8), total_pnl=226.0),
            self._row(symbol="BE", trade_symbol="BE    260605C00285000",
                      close_date=date(2026, 6, 8), total_pnl=638.0),
        ])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        assert out["count"] == 2
        assert {r["symbol"] for r in out["trades"]} == {"ASTS", "BE"}

    def test_rows_sorted_alphabetically_by_account(self):
        df = pd.DataFrame([
            self._row(tenant_id="snaptrade:z", account="Zoe Investment",
                      symbol="ZZZ", trade_symbol="ZZZ   260605C00100000",
                      close_date=date(2026, 6, 12), total_pnl=10.0),
            self._row(tenant_id="snaptrade:a", account="Aaron Investment",
                      symbol="AAA", trade_symbol="AAA   260605C00100000",
                      close_date=date(2026, 6, 8), total_pnl=20.0),
            self._row(tenant_id="snaptrade:m", account="Mike Investment",
                      symbol="MMM", trade_symbol="MMM   260605C00100000",
                      close_date=date(2026, 6, 14), total_pnl=30.0),
        ])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        accounts = [r["account_display"] for r in out["trades"]]
        assert accounts == [
            "Aaron Investment", "Mike Investment", "Zoe Investment",
        ]

    def test_mixed_strategy_labels_as_mixed(self):
        df = pd.DataFrame([
            self._row(trade_symbol="ASTS  260605C00102000", strategy="Covered Call",
                      close_date=date(2026, 6, 8), total_pnl=226.0),
            self._row(trade_symbol="ASTS_session_1", strategy="Buy and Hold",
                      close_date=date(2026, 6, 9), total_pnl=100.0),
        ])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        assert out["count"] == 1
        assert out["trades"][0]["strategy"] == "Mixed"

    def test_closed_outside_week_excluded(self):
        df = pd.DataFrame([
            self._row(close_date=date(2026, 6, 1), open_date=date(2026, 5, 28)),
        ])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        assert out["has_any"] is False


class TestBuildAccountBreakdown:
    """One summarized row per ACCOUNT (tenant), split by asset type with
    G/L % and annualized G/L %. Drives the Daily Review scorecard."""

    def _row(self, **kw):
        base = {
            "tenant_id": "snaptrade:acct-A", "account": "Schwab Account",
            "user_id": 1, "symbol": "JEPI",
            "equity_pnl": 1000.0, "option_pnl": 0.0, "dividend_income": 250.0,
            "net_pnl": 1250.0,
            "equity_capital": 10000.0, "option_capital_paid": 0.0,
            "option_premium_collected": 0.0,
            "current_equity_cost": 10000.0,
            "num_open_groups": 1, "num_equity_legs": 1, "num_option_legs": 0,
            "first_open_date": date(2025, 5, 1),
            "last_activity_date": date(2026, 5, 1),
        }
        base.update(kw)
        return base

    def test_empty_input(self):
        assert _build_account_breakdown(None) == {"rows": [], "totals": None}
        assert _build_account_breakdown(pd.DataFrame()) == {"rows": [], "totals": None}

    def test_single_account_single_symbol(self):
        df = pd.DataFrame([self._row()])
        out = _build_account_breakdown(df, label_map={"snaptrade:acct-A": "Brokerage"})
        assert len(out["rows"]) == 1
        r = out["rows"][0]
        assert r["account_display"] == "Brokerage"
        assert r["equity_pnl"] == 1000.0
        assert r["dividend_income"] == 250.0
        assert r["net_pnl"] == 1250.0
        assert r["pct_return"] == 12.5
        assert r["annualized_pct"] == 12.5
        # Single account → no all-accounts totals row.
        assert out["totals"] is None

    def test_collapses_symbols_within_account(self):
        df = pd.DataFrame([
            self._row(symbol="JEPI", equity_pnl=1000.0, option_pnl=0.0,
                      dividend_income=250.0, net_pnl=1250.0),
            self._row(symbol="ASTS", equity_pnl=0.0, option_pnl=500.0,
                      dividend_income=0.0, net_pnl=500.0,
                      equity_capital=0.0, option_capital_paid=2000.0,
                      current_equity_cost=0.0),
        ])
        out = _build_account_breakdown(df)
        assert len(out["rows"]) == 1
        r = out["rows"][0]
        assert r["equity_pnl"] == 1000.0
        assert r["option_pnl"] == 500.0
        assert r["net_pnl"] == 1750.0
        # Capital is summed across the account's symbols.
        assert r["capital_at_risk"] == 12000.0

    def test_multiple_accounts_get_totals_row(self):
        df = pd.DataFrame([
            self._row(tenant_id="snaptrade:acct-A", net_pnl=1250.0),
            self._row(tenant_id="snaptrade:acct-B", symbol="MSFT",
                      equity_pnl=300.0, option_pnl=0.0, dividend_income=0.0,
                      net_pnl=300.0),
        ])
        out = _build_account_breakdown(df)
        assert len(out["rows"]) == 2
        # Sorted by net descending → acct-A first.
        assert out["rows"][0]["net_pnl"] == 1250.0
        t = out["totals"]
        assert t is not None
        assert t["num_accounts"] == 2
        assert t["net_pnl"] == 1550.0

    def test_dust_account_annualized_none(self):
        df = pd.DataFrame([self._row(
            equity_capital=10.0, current_equity_cost=10.0,
            net_pnl=5.0, equity_pnl=5.0, dividend_income=0.0,
        )])
        out = _build_account_breakdown(df)
        r = out["rows"][0]
        assert r["pct_return"] is None
        assert r["annualized_pct"] is None

    def test_week_scope_keeps_open_drops_old_closed(self):
        week_start = date(2026, 6, 15)
        df = pd.DataFrame([
            # Open position (no week filter needed) — kept.
            self._row(tenant_id="snaptrade:acct-A", symbol="JEPI",
                      num_open_groups=1, num_equity_legs=1, net_pnl=1250.0,
                      equity_pnl=1250.0, dividend_income=0.0),
            # Closed before the week — dropped from the account total.
            self._row(tenant_id="snaptrade:acct-A", symbol="OLDX",
                      num_open_groups=0, num_equity_legs=0, num_option_legs=0,
                      current_equity_cost=0.0,
                      last_activity_date=date(2026, 5, 1),
                      net_pnl=9999.0, equity_pnl=9999.0, dividend_income=0.0),
        ])
        out = _build_account_breakdown(df, week_start=week_start)
        assert len(out["rows"]) == 1
        # Only the open JEPI position contributes; the stale closed lot is gone.
        assert out["rows"][0]["net_pnl"] == 1250.0

    def test_week_scope_keeps_closed_this_week(self):
        week_start = date(2026, 6, 15)
        df = pd.DataFrame([
            self._row(tenant_id="snaptrade:acct-A", symbol="RCNT",
                      num_open_groups=0, num_equity_legs=0, num_option_legs=0,
                      current_equity_cost=0.0,
                      last_activity_date=date(2026, 6, 18),
                      net_pnl=400.0, equity_pnl=400.0, dividend_income=0.0),
        ])
        out = _build_account_breakdown(df, week_start=week_start)
        assert len(out["rows"]) == 1
        assert out["rows"][0]["net_pnl"] == 400.0

    def test_week_scope_none_keeps_everything(self):
        week_start = None
        df = pd.DataFrame([
            self._row(symbol="JEPI", num_open_groups=1),
            self._row(symbol="OLDX", num_open_groups=0, num_equity_legs=0,
                      num_option_legs=0, current_equity_cost=0.0,
                      last_activity_date=date(2024, 1, 1),
                      net_pnl=50.0, equity_pnl=50.0, dividend_income=0.0),
        ])
        out = _build_account_breakdown(df, week_start=week_start)
        # Lifetime view → both symbols roll into the one account row.
        assert len(out["rows"]) == 1
        assert out["rows"][0]["net_pnl"] == 1300.0

    def test_basis_single_account_uses_row(self):
        df = pd.DataFrame([self._row(
            equity_capital=10000.0, current_equity_cost=10000.0,
        )])
        out = _build_account_breakdown(df)
        # Single account → basis mirrors the one row (capital + window).
        assert out["basis"]["capital_at_risk"] == 10000.0
        assert out["basis"]["days"] == out["rows"][0]["max_days_held"]

    def test_basis_multi_account_sums_capital(self):
        df = pd.DataFrame([
            self._row(tenant_id="snaptrade:acct-A"),
            self._row(tenant_id="snaptrade:acct-B", symbol="MSFT"),
        ])
        out = _build_account_breakdown(df)
        assert out["basis"]["capital_at_risk"] == out["totals"]["capital_at_risk"]


class TestBuildBenchmarkRows:
    """"If your capital had been in the index instead" comparison rows."""

    BASIS = {"capital_at_risk": 10000.0, "days": 365}

    def test_no_basis_or_returns_returns_empty(self):
        assert _build_benchmark_rows(None, {"SPY": 8.0}) == []
        assert _build_benchmark_rows(self.BASIS, {}) == []

    def test_dollar_and_pct_and_annualized(self):
        rows = _build_benchmark_rows(self.BASIS, {"SPY": 8.0, "QQQ": 12.0})
        assert len(rows) == 2
        spy = next(r for r in rows if r["symbol"] == "SPY")
        # 8% of $10,000 = $800 over a 365-day window.
        assert spy["total_pnl"] == 800.0
        assert spy["pct_return"] == 8.0
        # Annualized over exactly a year = the same 8%.
        assert spy["annualized_pct"] == 8.0
        assert spy["label"] == "S&P 500"

    def test_annualized_scales_short_window(self):
        # 90-day window: a 3% raw move annualizes up (× 365/90).
        rows = _build_benchmark_rows({"capital_at_risk": 5000.0, "days": 90}, {"SPY": 3.0})
        spy = rows[0]
        assert spy["annualized_pct"] == round(3.0 * 365.0 / 90, 1)

    def test_skips_index_with_no_data(self):
        rows = _build_benchmark_rows(self.BASIS, {"SPY": 8.0, "QQQ": None})
        assert [r["symbol"] for r in rows] == ["SPY"]


class TestBuildBenchmarkSnapshot:
    """Index 1d / 1w / 1m % for the row under the account-snapshot Total."""

    def test_empty_returns_empty(self):
        assert _build_benchmark_snapshot(None) == []
        assert _build_benchmark_snapshot(pd.DataFrame()) == []

    def test_computes_period_pcts_and_orders_spy_first(self):
        df = pd.DataFrame([
            {"symbol": "QQQ", "latest_close": 110.0,
             "day_close": 108.0, "week_close": 100.0, "month_close": 90.0},
            {"symbol": "SPY", "latest_close": 101.0,
             "day_close": 100.0, "week_close": 100.0, "month_close": 98.0},
        ])
        out = _build_benchmark_snapshot(df)
        assert [r["symbol"] for r in out] == ["SPY", "QQQ"]
        spy = out[0]
        assert spy["label"] == "S&P 500"
        assert spy["day_pct"] == 1.0   # (101-100)/100
        assert spy["month_pct"] == round((101.0 - 98.0) / 98.0 * 100, 2)
        qqq = out[1]
        assert qqq["week_pct"] == 10.0  # (110-100)/100

    def test_missing_base_yields_none(self):
        df = pd.DataFrame([
            {"symbol": "SPY", "latest_close": 101.0,
             "day_close": None, "week_close": 0.0, "month_close": 98.0},
        ])
        out = _build_benchmark_snapshot(df)
        assert out[0]["day_pct"] is None    # base missing
        assert out[0]["week_pct"] is None   # base <= 0 guarded
        assert out[0]["month_pct"] is not None


class TestSplitDayFills:
    """Fill-level rows for Daily Review 'Trades today' and the day page.

    The weekly table only lists groups that opened or closed this ISO week.
    An add/trim on a long-held position is a fill today and MUST show here.
    """

    def _row(self, **kw):
        base = {
            "tenant_id": "snaptrade:abc", "account": "Schwab Account",
            "user_id": 9, "trade_date": date(2026, 8, 13),
            "action": "equity_buy", "trade_symbol": "AAPL",
            "underlying_symbol": "AAPL", "description": "Bought AAPL",
            "quantity": 100.0, "price": 185.20, "amount": -18520.0,
            "instrument_type": "Equity",
        }
        base.update(kw)
        return base

    def test_empty(self):
        out = _split_day_fills(None)
        assert out["has_any"] is False
        assert out["trades"] == []
        assert out["count"] == 0
        assert _split_day_fills(pd.DataFrame())["has_any"] is False

    def test_drip_reinvestment_is_not_a_session_trade(self):
        df = pd.DataFrame([self._row(
            quantity=0.1334, price=56.58, amount=-7.55,
            is_dividend_reinvestment=True,
        )])
        out = _split_day_fills(df)
        assert out["count"] == 0
        assert out["trades"] == []
        assert out["has_any"] is False
        assert out["net_cash"] == 0.0

    def test_drip_action_alias_is_not_a_session_trade(self):
        df = pd.DataFrame([self._row(
            action="dividend_reinvest", quantity=0.12, amount=-7.06,
        )])
        out = _split_day_fills(df)
        assert out["count"] == 0
        assert out["trades"] == []

    def test_monday_tplus1_expiry_is_not_a_session_trade(self):
        # ASTS 260828 expired Friday; Schwab posts option_expired Monday.
        df = pd.DataFrame([self._row(
            trade_date=date(2026, 8, 31),
            action="option_expired",
            trade_symbol="ASTS  260828C00061000",
            underlying_symbol="ASTS",
            quantity=8.0, price=None, amount=0.0,
            instrument_type="Call",
            option_expiry=date(2026, 8, 28),
        )])
        out = _split_day_fills(df)
        assert out["count"] == 0
        assert out["trades"] == []
        assert out["has_any"] is False

    def test_friday_expiry_still_counts_on_expiry_session(self):
        df = pd.DataFrame([self._row(
            trade_date=date(2026, 8, 28),
            action="option_expired",
            trade_symbol="ASTS  260828C00061000",
            underlying_symbol="ASTS",
            quantity=8.0, price=None, amount=0.0,
            realized_pnl=2100.0,
            instrument_type="Call",
            option_expiry=date(2026, 8, 28),
        )])
        out = _split_day_fills(df)
        assert out["count"] == 1
        assert out["trades"][0]["verb"] == "Expired"
        assert out["trades"][0]["symbol"] == "ASTS"
        assert out["net_gl"] == 2100.0

    def test_monday_assignment_is_not_a_session_trade(self):
        df = pd.DataFrame([self._row(
            trade_date=date(2026, 8, 31),
            action="option_assigned",
            trade_symbol="NVDA  260828C00230000",
            underlying_symbol="NVDA",
            quantity=1.0, amount=0.0,
            instrument_type="Call",
            option_expiry=date(2026, 8, 28),
        )])
        out = _split_day_fills(df)
        assert out["count"] == 0
        assert out["trades"] == []

    def test_monday_btc_still_counts(self):
        df = pd.DataFrame([self._row(
            trade_date=date(2026, 8, 31),
            action="option_buy_to_close",
            trade_symbol="MRVL  260904C00242500",
            underlying_symbol="MRVL",
            quantity=10.0, price=17.75, amount=-17742.97,
            realized_pnl=2100.0, instrument_type="Call",
            option_expiry=date(2026, 9, 4),
        )])
        out = _split_day_fills(df)
        assert out["count"] == 1
        assert out["trades"][0]["verb"] == "Bought to close"

    def test_real_buy_on_payable_day_still_counts(self):
        df = pd.DataFrame([
            self._row(quantity=1.0, price=60.17, amount=-60.17,
                      is_dividend_reinvestment=False),
            self._row(quantity=0.1146, price=60.12, amount=-6.89,
                      is_dividend_reinvestment=True),
        ])
        out = _split_day_fills(df)
        assert out["count"] == 1
        assert out["trades"][0]["quantity"] == 1.0
        assert out["net_cash"] == -60.17

    def test_add_to_existing_position_is_a_trade(self):
        # The product gap: this fill would never appear in Trades this week
        # because the AAPL group opened months ago and is still open.
        df = pd.DataFrame([self._row()])
        out = _split_day_fills(df, label_map={"snaptrade:abc": "Sara Investment"})
        assert out["count"] == 1
        assert out["has_any"] is True
        r = out["trades"][0]
        assert r["verb"] == "Bought"
        assert r["symbol"] == "AAPL"
        assert r["is_option"] is False
        assert r["account"] == "Sara Investment"
        assert r["cash_amount"] == -18520.0
        assert r["amount"] is None  # opens have no realized G/L
        assert out["net_cash"] == -18520.0
        assert out["net_gl"] == 0.0
        assert out["symbols"] == ["AAPL"]
        assert out["cash"] == []

    def test_option_sto_and_deposit_split(self):
        df = pd.DataFrame([
            self._row(action="option_sell_to_open", trade_symbol="ASTS  260821C00050000",
                      underlying_symbol="ASTS", quantity=1, price=1.20, amount=120.0,
                      instrument_type="Call"),
            self._row(action="cash_transfer", trade_symbol="", underlying_symbol="",
                      quantity=None, price=None, amount=5000.0, description="Deposit"),
        ])
        out = _split_day_fills(df)
        assert out["count"] == 1
        assert out["trades"][0]["verb"] == "Sold to open"
        assert out["trades"][0]["is_option"] is True
        assert out["trades"][0]["amount"] is None
        assert out["net_cash"] == 120.0  # deposit is not a trade
        assert out["net_gl"] == 0.0
        assert len(out["cash"]) == 1
        assert out["cash"][0]["verb"] == "Cash transfer"
        assert out["cash"][0]["amount"] == 5000.0
        assert out["symbols"] == ["ASTS"]

    def test_unknown_action_is_cash_not_a_trade(self):
        df = pd.DataFrame([self._row(action="margin_interest", amount=-12.5,
                                    quantity=None, price=None, underlying_symbol="")])
        out = _split_day_fills(df)
        assert out["count"] == 0
        assert out["trades"] == []
        assert out["cash"][0]["verb"] == "Margin interest"
        assert out["has_any"] is True

    def _jpm(self, action, trade_symbol, amount, tenant_id="snaptrade:abc",
             account="Schwab Account"):
        return self._row(
            action=action, trade_symbol=trade_symbol,
            underlying_symbol="JPM", quantity=1, price=abs(amount) / 100.0,
            amount=amount, instrument_type="Call" if "C00" in trade_symbol else "Put",
            tenant_id=tenant_id, account=account,
        )

    def test_posted_settlement_is_not_a_roll_into_the_next_spread(self):
        """Oct 1 SPXW cash settlement posted Oct 2 is not a 7650→7730 roll."""
        close = self._row(
            trade_date=date(2026, 10, 2),
            action="option_buy_to_close",
            trade_symbol="SPXW  261001C07650000",
            underlying_symbol="SPXW",
            description="CALL S & P 500 INDEX $7650 EXP 10/01/26 as of 10/01/2026",
            quantity=10, price=16.45, amount=-16450.0,
            realized_pnl=-3574.44,
            instrument_type="Call",
            option_expiry=date(2026, 10, 1),
        )
        opened = self._row(
            trade_date=date(2026, 10, 2),
            action="option_sell_to_open",
            trade_symbol="SPXW  261002C07730000",
            underlying_symbol="SPXW",
            description="SPXW  261002C07730000",
            quantity=20, price=6.85, amount=13675.56,
            instrument_type="Call",
        )
        out = _split_day_fills(pd.DataFrame([close, opened]))
        assert out["roll_count"] == 0
        assert out["fill_count"] == 2
        assert all(not t.get("is_roll") for t in out["trades"])
        assert all("Rolled" not in str(t.get("verb")) for t in out["trades"])

    def test_btc_then_sto_call_credit_and_strike_up_is_successful(self):
        df = pd.DataFrame([
            self._jpm("option_buy_to_close", "JPM   260821C00300000", -120),
            self._jpm("option_sell_to_open", "JPM   260828C00305000", 180),
        ])
        out = _split_day_fills(df)
        assert out["fill_count"] == 2
        assert out["count"] == 1
        assert out["roll_count"] == 1
        assert len(out["trades"]) == 1
        g = out["trades"][0]
        assert g["is_roll"] is True
        assert g["verb"] == "Rolled"
        assert g["cash_amount"] == 60
        assert g["amount"] is None  # no warehouse realized row in this fixture
        assert g["successful"] is True
        assert g["success_bits"] == ["credit", "strike"]
        assert "300" in g["close_label"]
        assert "305" in g["open_label"]

    def test_sto_then_btc_still_groups(self):
        df = pd.DataFrame([
            self._jpm("option_sell_to_open", "JPM   260828C00305000", 180),
            self._jpm("option_buy_to_close", "JPM   260821C00300000", -120),
        ])
        out = _split_day_fills(df)
        assert out["roll_count"] == 1
        assert out["trades"][0]["cash_amount"] == 60
        assert out["trades"][0]["successful"] is True

    def test_sto_and_same_day_expiry_is_one_row(self):
        sto = self._row(
            action="option_sell_to_open",
            trade_symbol="ASTS  260828C00061000",
            underlying_symbol="ASTS", quantity=8, price=0.34,
            amount=266.65, instrument_type="Call",
            trade_date=date(2026, 8, 28),
        )
        expired = self._row(
            action="option_expired",
            trade_symbol="ASTS  260828C00061000",
            underlying_symbol="ASTS", quantity=8, price=None,
            amount=0.0, realized_pnl=266.65, instrument_type="Call",
            trade_date=date(2026, 8, 28),
            option_expiry=date(2026, 8, 28),
        )
        out = _split_day_fills(pd.DataFrame([sto, expired]))
        assert out["fill_count"] == 2
        assert out["count"] == 1
        assert len(out["trades"]) == 1
        t = out["trades"][0]
        assert t["verb"] == "Expired"
        assert t["action"] == "option_expired"
        assert t["price"] is None
        assert t["quantity"] == 8.0
        assert t["amount"] == 266.65
        assert out["net_gl"] == 266.65

    def test_expiry_then_sto_still_groups(self):
        sto = self._row(
            action="option_sell_to_open",
            trade_symbol="BE    260828C00222500",
            underlying_symbol="BE", quantity=2, price=2.26,
            amount=450.65, instrument_type="Call",
        )
        expired = self._row(
            action="option_expired",
            trade_symbol="BE    260828C00222500",
            underlying_symbol="BE", quantity=2, amount=0.0,
            realized_pnl=450.65, instrument_type="Call",
        )
        out = _split_day_fills(pd.DataFrame([expired, sto]))
        assert len(out["trades"]) == 1
        assert out["trades"][0]["verb"] == "Expired"
        assert out["trades"][0]["price"] is None

    def test_sto_and_same_day_assignment_is_one_row(self):
        sto = self._row(
            action="option_sell_to_open",
            trade_symbol="NVDA  260828C00230000",
            underlying_symbol="NVDA", quantity=1, price=6.16,
            amount=616.33, instrument_type="Call",
        )
        assigned = self._row(
            action="option_assigned",
            trade_symbol="NVDA  260828C00230000",
            underlying_symbol="NVDA", quantity=1, amount=0.0,
            realized_pnl=616.33, instrument_type="Call",
        )
        out = _split_day_fills(pd.DataFrame([sto, assigned]))
        assert len(out["trades"]) == 1
        assert out["trades"][0]["verb"] == "Assigned"
        assert out["trades"][0]["price"] is None
        assert out["trades"][0]["amount"] == 616.33

    def test_different_contracts_stay_two_rows(self):
        sto = self._row(
            action="option_sell_to_open",
            trade_symbol="ASTS  260828C00061000",
            underlying_symbol="ASTS", quantity=8, price=0.34,
            amount=266.65, instrument_type="Call",
        )
        expired = self._row(
            action="option_expired",
            trade_symbol="ASTS  260828C00065000",
            underlying_symbol="ASTS", quantity=2, amount=0.0,
            realized_pnl=80.0, instrument_type="Call",
        )
        out = _split_day_fills(pd.DataFrame([sto, expired]))
        assert len(out["trades"]) == 2

    def test_sto_and_estimated_expiry_is_one_row(self):
        sto = self._row(
            action="option_sell_to_open",
            trade_symbol="SPXW  260828C07650000",
            underlying_symbol="SPXW", quantity=10, price=7.77,
            amount=7757.78, instrument_type="Call",
            trade_date=date(2026, 8, 28),
        )
        settled = self._row(
            action="option_settled_est",
            trade_symbol="SPXW  260828C07650000",
            underlying_symbol="SPXW", quantity=10, price=None,
            amount=0.0, realized_pnl=1400.0, instrument_type="Call",
            trade_date=date(2026, 8, 28),
            option_expiry=date(2026, 8, 28),
        )
        out = _split_day_fills(pd.DataFrame([sto, settled]))
        assert out["fill_count"] == 2
        assert out["count"] == 1
        t = out["trades"][0]
        assert t["verb"] == "Settled at expiry (est.)"
        assert t["action"] == "option_settled_est"
        assert t["price"] is None
        assert t["amount"] == 1400.0
        assert t["direction"] == "Sold"
        assert out["net_gl"] == 1400.0

    def test_monday_estimated_expiry_is_not_a_session_trade(self):
        df = pd.DataFrame([self._row(
            trade_date=date(2026, 8, 31),
            action="option_settled_est",
            trade_symbol="SPXW  260828C07650000",
            underlying_symbol="SPXW",
            quantity=10.0, price=None, amount=0.0,
            realized_pnl=1400.0,
            instrument_type="Call",
            option_expiry=date(2026, 8, 28),
        )])
        out = _split_day_fills(df)
        assert out["count"] == 0
        assert out["trades"] == []
        assert out["has_any"] is False

    def test_credit_vertical_is_one_sold_spread(self):
        df = pd.DataFrame([
            self._row(
                action="option_sell_to_open",
                trade_symbol="SPY   260918C00500000",
                underlying_symbol="SPY", quantity=2, price=3.0,
                amount=600.0, instrument_type="Call",
            ),
            self._row(
                action="option_buy_to_open",
                trade_symbol="SPY   260918C00505000",
                underlying_symbol="SPY", quantity=2, price=1.0,
                amount=-200.0, instrument_type="Call",
            ),
        ])
        out = _split_day_fills(df)
        assert out["fill_count"] == 2
        assert out["count"] == 1
        t = out["trades"][0]
        assert t["is_spread"] is True
        assert t["verb"] == "Sold spread"
        assert t["amount"] is None
        assert t["quantity"] == 2.0
        assert t["trade_symbol"] == ""
        assert "500/505C spread" in t["spread_label"]
        assert out["net_gl"] == 0.0

    def test_debit_vertical_is_one_bought_spread(self):
        df = pd.DataFrame([
            self._row(
                action="option_sell_to_open",
                trade_symbol="SPY   260918P00400000",
                underlying_symbol="SPY", quantity=1, price=1.0,
                amount=100.0, instrument_type="Put",
            ),
            self._row(
                action="option_buy_to_open",
                trade_symbol="SPY   260918P00395000",
                underlying_symbol="SPY", quantity=1, price=4.0,
                amount=-400.0, instrument_type="Put",
            ),
        ])
        out = _split_day_fills(df)
        assert out["count"] == 1
        t = out["trades"][0]
        assert t["verb"] == "Bought spread"
        assert t["amount"] is None
        assert "400/395P spread" in t["spread_label"] or "395/400P spread" in t["spread_label"]

    def test_mismatched_spread_size_stays_two_rows(self):
        df = pd.DataFrame([
            self._row(
                action="option_sell_to_open",
                trade_symbol="SPY   260918C00500000",
                underlying_symbol="SPY", quantity=20, price=3.0,
                amount=6000.0, instrument_type="Call",
            ),
            self._row(
                action="option_buy_to_open",
                trade_symbol="SPY   260918C00505000",
                underlying_symbol="SPY", quantity=10, price=1.0,
                amount=-1000.0, instrument_type="Call",
            ),
        ])
        out = _split_day_fills(df)
        assert out["count"] == 2
        assert all(not t.get("is_spread") for t in out["trades"])

    def test_vertical_in_two_accounts_stays_two_rows(self):
        df = pd.DataFrame([
            self._row(
                tenant_id="snaptrade:aaa",
                action="option_sell_to_open",
                trade_symbol="SPY   260918C00500000",
                underlying_symbol="SPY", quantity=2, price=3.0,
                amount=600.0, instrument_type="Call",
            ),
            self._row(
                tenant_id="snaptrade:bbb",
                action="option_buy_to_open",
                trade_symbol="SPY   260918C00505000",
                underlying_symbol="SPY", quantity=2, price=1.0,
                amount=-200.0, instrument_type="Call",
            ),
        ])
        out = _split_day_fills(df)
        assert out["count"] == 2

    def test_closed_vertical_sums_realized(self):
        df = pd.DataFrame([
            self._row(
                action="option_buy_to_close",
                trade_symbol="SPY   260918C00500000",
                underlying_symbol="SPY", quantity=2, price=1.0,
                amount=-200.0, realized_pnl=400.0, instrument_type="Call",
            ),
            self._row(
                action="option_sell_to_close",
                trade_symbol="SPY   260918C00505000",
                underlying_symbol="SPY", quantity=2, price=0.4,
                amount=80.0, realized_pnl=-50.0, instrument_type="Call",
            ),
        ])
        out = _split_day_fills(df)
        assert out["count"] == 1
        t = out["trades"][0]
        assert t["verb"] == "Closed spread"
        assert t["quantity"] == 2.0
        assert t["amount"] == 350.0
        assert out["net_gl"] == 350.0
        assert "500/505C spread" in t["spread_label"]

    def test_same_day_vertical_expiry_is_one_spread(self):
        day = date(2026, 8, 28)
        rows = []
        for action, occ, amount, realized in (
            ("option_sell_to_open", "SPXW  260828C07650000", 7757.78, None),
            ("option_buy_to_open", "SPXW  260828C07655000", -6332.22, None),
            ("option_expired", "SPXW  260828C07650000", 0.0, 500.0),
            ("option_expired", "SPXW  260828C07655000", 0.0, -200.0),
        ):
            rows.append(self._row(
                action=action, trade_symbol=occ, underlying_symbol="SPXW",
                quantity=10, price=None if action == "option_expired" else 1.0,
                amount=amount, realized_pnl=realized, instrument_type="Call",
                trade_date=day, option_expiry=day,
            ))
        out = _split_day_fills(pd.DataFrame(rows))
        assert out["fill_count"] == 4
        assert out["count"] == 1
        t = out["trades"][0]
        assert t["is_spread"] is True
        assert t["verb"] == "Expired"
        assert t["quantity"] == 10.0
        assert t["amount"] == 300.0
        assert "7650/7655C spread" in t["spread_label"]
        assert out["net_gl"] == 300.0

    def test_iron_condor_stays_two_spreads(self):
        df = pd.DataFrame([
            self._row(
                action="option_sell_to_open",
                trade_symbol="SPY   260918C00500000",
                underlying_symbol="SPY", quantity=1, amount=200.0,
                instrument_type="Call",
            ),
            self._row(
                action="option_buy_to_open",
                trade_symbol="SPY   260918C00505000",
                underlying_symbol="SPY", quantity=1, amount=-80.0,
                instrument_type="Call",
            ),
            self._row(
                action="option_sell_to_open",
                trade_symbol="SPY   260918P00400000",
                underlying_symbol="SPY", quantity=1, amount=180.0,
                instrument_type="Put",
            ),
            self._row(
                action="option_buy_to_open",
                trade_symbol="SPY   260918P00395000",
                underlying_symbol="SPY", quantity=1, amount=-70.0,
                instrument_type="Put",
            ),
        ])
        out = _split_day_fills(df)
        assert out["fill_count"] == 4
        assert out["count"] == 2
        assert all(t.get("is_spread") for t in out["trades"])
        labels = " ".join(t["spread_label"] for t in out["trades"])
        assert "C spread" in labels
        assert "P spread" in labels

    def test_settlement_without_side_does_not_guess_a_spread(self):
        df = pd.DataFrame([
            self._row(
                action="option_expired",
                trade_symbol="SPY   260918C00500000",
                underlying_symbol="SPY", quantity=1, amount=0.0,
                realized_pnl=200.0, instrument_type="Call",
            ),
            self._row(
                action="option_expired",
                trade_symbol="SPY   260918C00505000",
                underlying_symbol="SPY", quantity=1, amount=0.0,
                realized_pnl=-80.0, instrument_type="Call",
            ),
        ])
        out = _split_day_fills(df)
        assert out["count"] == 2
        assert out["net_gl"] == 120.0

    def test_expiry_without_same_day_open_stays_one_row(self):
        expired = self._row(
            action="option_expired",
            trade_symbol="NVDA  260828C00230000",
            underlying_symbol="NVDA", quantity=1, price=None, amount=0.0,
            realized_pnl=616.33, instrument_type="Call",
        )
        out = _split_day_fills(pd.DataFrame([expired]))
        assert len(out["trades"]) == 1
        assert out["trades"][0]["verb"] == "Expired"
        assert out["trades"][0]["price"] is None

    def test_put_roll_succeeds_on_strike_down(self):
        df = pd.DataFrame([
            self._jpm("option_buy_to_close", "JPM   260821P00280000", -200),
            self._jpm("option_sell_to_open", "JPM   260828P00275000", 150),
        ])
        out = _split_day_fills(df)
        g = out["trades"][0]
        assert g["is_roll"] is True
        assert g["successful"] is True
        assert "strike" in g["success_bits"]
        assert "credit" not in g["success_bits"]

    def test_debit_same_strike_is_not_successful(self):
        df = pd.DataFrame([
            self._jpm("option_buy_to_close", "JPM   260821C00300000", -200),
            self._jpm("option_sell_to_open", "JPM   260918C00300000", 150),
        ])
        out = _split_day_fills(df)
        g = out["trades"][0]
        assert g["is_roll"] is True
        assert g["successful"] is False
        assert g["success_bits"] == []
        assert g["strike_dir"] == "same"

    def test_unpaired_open_stays_a_fill(self):
        df = pd.DataFrame([
            self._jpm("option_sell_to_open", "JPM   260828C00305000", 180),
        ])
        out = _split_day_fills(df)
        assert out["roll_count"] == 0
        assert out["trades"][0]["verb"] == "Sold to open"
        assert out["trades"][0]["amount"] is None
        assert out["trades"][0].get("is_roll") is not True

    def test_displayed_count_matches_rows_when_grouped(self):
        df = pd.DataFrame([
            self._jpm("option_buy_to_close", "JPM   260821C00300000", -120),
            self._jpm("option_sell_to_open", "JPM   260828C00305000", 180),
            self._row(action="equity_sell", trade_symbol="SPCE",
                      underlying_symbol="SPCE", quantity=1, price=3.07,
                      amount=3.07, instrument_type="Equity",
                      tenant_id="snaptrade:cam", account="Cameron Investment"),
        ])
        out = _split_day_fills(df)
        assert out["fill_count"] == 3
        assert out["count"] == 2
        assert out["roll_count"] == 1
        assert any(t["symbol"] == "SPCE" and not t.get("is_roll") for t in out["trades"])

    def test_colliding_account_labels_do_not_cross_pair(self):
        df = pd.DataFrame([
            self._jpm("option_buy_to_close", "JPM   260821C00300000", -120,
                      tenant_id="snaptrade:acct-a", account="Schwab Account"),
            self._jpm("option_sell_to_open", "JPM   260828C00305000", 180,
                      tenant_id="snaptrade:acct-b", account="Schwab Account"),
        ])
        out = _split_day_fills(df)
        assert out["roll_count"] == 0
        assert len(out["trades"]) == 2
        assert {t["verb"] for t in out["trades"]} == {"Bought to close", "Sold to open"}

    def test_equity_sell_shows_realized_not_proceeds(self):
        # The screenshot bug: 50 DELL @ $469.75 is +$23,487.50 of CASH,
        # not G/L. The dollar column must be sale vs cost.
        df = pd.DataFrame([self._row(
            action="equity_sell", trade_symbol="DELL",
            underlying_symbol="DELL", quantity=50, price=469.75,
            amount=23487.50, realized_pnl=412.18,
        )])
        out = _split_day_fills(df)
        r = out["trades"][0]
        assert r["verb"] == "Sold"
        assert r["cash_amount"] == 23487.50
        assert r["amount"] == 412.18
        assert r["realized_pnl"] == 412.18
        assert out["net_cash"] == 23487.50
        assert out["net_gl"] == 412.18

    def test_unmatched_close_is_dash_not_proceeds(self):
        df = pd.DataFrame([self._row(
            action="equity_sell", trade_symbol="DELL",
            underlying_symbol="DELL", quantity=50, price=469.75,
            amount=23487.50,
        )])
        out = _split_day_fills(df)
        assert out["trades"][0]["amount"] is None
        assert out["net_gl"] == 0.0
        assert out["net_cash"] == 23487.50

    def test_option_stc_uses_contract_realized(self):
        df = pd.DataFrame([self._row(
            action="option_sell_to_close",
            trade_symbol="MRVL  260904C00242500",
            underlying_symbol="MRVL", quantity=10, price=17.75,
            amount=17742.97, realized_pnl=2100.0, instrument_type="Call",
        )])
        out = _split_day_fills(df)
        r = out["trades"][0]
        assert r["verb"] == "Sold to close"
        assert r["cash_amount"] == 17742.97
        assert r["amount"] == 2100.0
        assert out["net_gl"] == 2100.0

    def test_split_option_close_credits_contract_realized_once(self):
        # DAY_TRADES_QUERY joins contract-grain realized_pnl to fill-grain
        # history. A close split into two broker fills must not report the
        # full contract result twice.
        contract = "MRVL  260904C00242500"
        df = pd.DataFrame([
            self._row(
                action="option_sell_to_close",
                trade_symbol=contract,
                underlying_symbol="MRVL", quantity=6, price=17.75,
                amount=10645.78, realized_pnl=2100.0,
                instrument_type="Call",
            ),
            self._row(
                action="option_sell_to_close",
                trade_symbol=contract,
                underlying_symbol="MRVL", quantity=4, price=17.75,
                amount=7097.19, realized_pnl=2100.0,
                instrument_type="Call",
            ),
        ])

        out = _split_day_fills(df)

        assert out["count"] == 2
        assert len(out["trades"]) == 2
        assert [r["amount"] for r in out["trades"]] == [2100.0, None]
        assert out["net_gl"] == 2100.0

    def test_same_contract_in_two_tenants_keeps_each_realized_result(self):
        contract = "MRVL  260904C00242500"
        df = pd.DataFrame([
            self._row(
                tenant_id="snaptrade:one",
                action="option_sell_to_close", trade_symbol=contract,
                underlying_symbol="MRVL", quantity=1, amount=1775.0,
                realized_pnl=210.0, instrument_type="Call",
            ),
            self._row(
                tenant_id="snaptrade:two",
                action="option_sell_to_close", trade_symbol=contract,
                underlying_symbol="MRVL", quantity=1, amount=1775.0,
                realized_pnl=180.0, instrument_type="Call",
            ),
        ])

        out = _split_day_fills(df)

        assert [r["amount"] for r in out["trades"]] == [210.0, 180.0]
        assert out["net_gl"] == 390.0

    def test_roll_dollar_is_closed_leg_gl_not_net_credit(self):
        btc = self._jpm("option_buy_to_close", "JPM   260821C00300000", -120)
        btc["realized_pnl"] = 80.0
        sto = self._jpm("option_sell_to_open", "JPM   260828C00305000", 180)
        out = _split_day_fills(pd.DataFrame([btc, sto]))
        g = out["trades"][0]
        assert g["is_roll"] is True
        assert g["amount"] == 80.0
        assert g["cash_amount"] == 60
        assert g["successful"] is True
        assert out["net_gl"] == 80.0

    def test_fill_carries_leg_tags(self):
        df = pd.DataFrame([self._row(
            action="option_expired",
            underlying_symbol="NVDA",
            trade_symbol="NVDA  260828C00180000",
            trade_date=date(2026, 8, 28),
            amount=0,
            realized_pnl=210.0,
            option_expiry=date(2026, 8, 28),
            leg_open_date=date(2026, 1, 15),
        )])
        tags = [{
            "tenant_id": "snaptrade:abc",
            "symbol": "NVDA",
            "leg_open_date": date(2026, 1, 15),
            "tag": "earningsfollower",
        }]
        out = _split_day_fills(df, tag_rows=tags)
        row = out["trades"][0]
        assert row["leg_open_date"] == date(2026, 1, 15)
        assert row["tags"] == ["earningsfollower"]

    def test_fill_without_matching_chapter_has_empty_tags(self):
        df = pd.DataFrame([self._row()])
        out = _split_day_fills(df, tag_rows=[{
            "tenant_id": "snaptrade:abc", "symbol": "AAPL",
            "leg_open_date": date(2026, 1, 1), "tag": "x",
        }])
        assert out["trades"][0]["leg_open_date"] is None
        assert out["trades"][0]["tags"] == []

    def test_roll_keeps_leg_open_date_for_tagging(self):
        btc = self._jpm("option_buy_to_close", "JPM   260821C00300000", -120)
        btc["realized_pnl"] = 80.0
        btc["leg_open_date"] = date(2026, 6, 1)
        sto = self._jpm("option_sell_to_open", "JPM   260828C00305000", 180)
        sto["leg_open_date"] = date(2026, 6, 1)
        out = _split_day_fills(pd.DataFrame([btc, sto]), tag_rows=[{
            "tenant_id": "snaptrade:abc", "symbol": "JPM",
            "leg_open_date": date(2026, 6, 1), "tag": "wheel",
        }])
        g = out["trades"][0]
        assert g["is_roll"] is True
        assert g["leg_open_date"] == date(2026, 6, 1)
        assert g["tags"] == ["wheel"]


class TestDailyReviewBatchIncludesTodayTrades:
    def test_today_trades_reuses_day_query_with_today_param(self):
        today = date(2026, 8, 13)
        batch = build_daily_review_batch("AND tenant_id IN ('snaptrade:abc')",
                                         today, date(2026, 8, 10))
        assert "today_trades" in batch
        sql, cfg = batch["today_trades"]
        assert "stg_history" in sql
        assert "int_closed_equity_legs" in sql
        assert "int_option_contracts" in sql
        assert "realized_pnl" in sql
        assert "trade_date = @day" in sql
        params = {p.name: p.value for p in cfg.query_parameters}
        assert params["day"] == today

    def test_today_trades_honors_trades_as_of(self):
        # Friday pre-market must query Thursday, not calendar Friday —
        # otherwise the new Trades Today empty-state lies.
        friday = date(2026, 8, 14)
        thursday = date(2026, 8, 13)
        batch = build_daily_review_batch(
            "AND tenant_id IN ('snaptrade:abc')",
            friday, date(2026, 8, 10), trades_as_of=thursday)
        params = {p.name: p.value
                  for p in batch["today_trades"][1].query_parameters}
        assert params["day"] == thursday

    def test_movers_bind_as_of_so_in_session_bars_cannot_leak_into_overview(self):
        friday = date(2026, 8, 28)
        thursday = date(2026, 8, 27)
        batch = build_daily_review_batch(
            "AND tenant_id IN ('snaptrade:abc')",
            friday, date(2026, 8, 24),
            trades_as_of=thursday, moves_as_of=thursday)
        sql, cfg = batch["today_moves"]
        assert "date <= @as_of" in sql
        assert {p.name: p.value for p in cfg.query_parameters}["as_of"] == thursday
        opt_sql, opt_cfg = batch["today_options_moves"]
        assert "date <= @as_of" in opt_sql
        assert {p.name: p.value for p in opt_cfg.query_parameters}["as_of"] == thursday
        from app.query_cache import make_key
        # Fills and option day-moves are different SQL and different
        # parameter names, so a Redis hit cannot swap the two frames.
        assert make_key(*batch["today_trades"]) != make_key(*batch["today_options_moves"])

    def test_market_context_is_capped_to_the_displayed_close(self):
        friday = date(2026, 8, 28)
        thursday = date(2026, 8, 27)
        batch = build_daily_review_batch(
            "AND tenant_id IN ('snaptrade:abc')",
            friday, date(2026, 8, 24),
            trades_as_of=thursday, moves_as_of=thursday)

        benchmark_sql, benchmark_cfg = batch["benchmark_snapshot"]
        assert (
            "BETWEEN DATE_SUB(@as_of, INTERVAL 70 DAY) AND @as_of"
            in benchmark_sql
        )
        benchmark_params = {
            p.name: p.value for p in benchmark_cfg.query_parameters
        }
        assert benchmark_params["as_of"] == thursday

        market_sql, market_cfg = batch["market_perf"]
        assert "date <= @as_of" in market_sql
        market_params = {
            p.name: p.value for p in market_cfg.query_parameters
        }
        assert market_params["as_of"] == thursday
        assert market_params["week_start"] == date(2026, 8, 24)

    def test_batch_defaults_as_of_to_today_for_existing_callers(self):
        today = date(2026, 8, 13)
        batch = build_daily_review_batch(
            "AND tenant_id IN ('snaptrade:abc')", today, date(2026, 8, 10))
        params = {p.name: p.value
                  for p in batch["today_moves"][1].query_parameters}
        assert params["as_of"] == today

    def test_batch_includes_ex_div_calendar_query(self):
        batch = build_daily_review_batch(
            "AND tenant_id IN ('snaptrade:abc')",
            date(2026, 8, 13), date(2026, 8, 10))
        assert "ex_div_calendar" in batch
        assert "stg_ex_div_calendar" in batch["ex_div_calendar"]
        assert "upcoming_divs" in batch


    def test_core_and_below_keys_partition_the_batch(self):
        from app.weekly_review import (
            OVERVIEW_BELOW_KEYS, OVERVIEW_CORE_KEYS, build_daily_review_batch,
        )
        batch = build_daily_review_batch(
            "AND tenant_id IN ('snaptrade:abc')",
            date(2026, 8, 13), date(2026, 8, 10))
        assert OVERVIEW_CORE_KEYS.isdisjoint(OVERVIEW_BELOW_KEYS)
        assert OVERVIEW_CORE_KEYS | OVERVIEW_BELOW_KEYS == set(batch)
        assert "attribution" in OVERVIEW_BELOW_KEYS
        assert "today_trades" in OVERVIEW_CORE_KEYS
        assert "close_sleeve" in OVERVIEW_CORE_KEYS

    def test_close_sleeve_is_the_session_cash_aggregate(self):
        friday = date(2026, 9, 18)
        batch = build_daily_review_batch(
            "AND tenant_id IN ('snaptrade:abc')",
            date(2026, 9, 21), date(2026, 9, 14),
            trades_as_of=friday, moves_as_of=friday)
        sql, cfg = batch["close_sleeve"]
        assert "mart_account_equity_daily" in sql
        select = sql.lower().split("from", 1)[0]
        assert "tenant_id" not in select
        assert "snaptrade:abc" in sql
        params = {p.name: p.value for p in cfg.query_parameters}
        assert params["as_of"] == friday
        assert "date = @as_of" in " ".join(CLOSE_SLEEVE_QUERY.lower().split())

    def test_invested_pct_matches_the_close_only_when_totals_agree(self):
        cash, invested, pct = _invested_from_close_sleeve(100000, 100000.4, 20000)
        assert (cash, invested, pct) == (20000.0, 80000.0, 80.0)
        # A live placeholder in the hero must not borrow close cash.
        assert _invested_from_close_sleeve(150000, 100000, 20000) == (None, None, None)
        assert _invested_from_close_sleeve(None, 100000, 20000) == (None, None, None)
        assert _invested_from_close_sleeve(0, 0, 0) == (None, None, None)


    def test_attribution_week_follows_the_close_not_calendar_monday(self):
        # Monday Sep 21 looking at Friday Sep 18. Calendar ISO Monday is
        # Sep 21 (still ahead of the close). The scorecard must use
        # Monday Sep 14, the week of the session on screen.
        friday = date(2026, 9, 18)
        monday_of_close = date(2026, 9, 14)
        calendar_monday = date(2026, 9, 21)
        batch = build_daily_review_batch(
            "AND tenant_id IN ('snaptrade:abc')",
            date(2026, 9, 21), calendar_monday,
            trades_as_of=friday, attribution_week=monday_of_close)
        assert "2026-09-14" in batch["attribution"]
        assert "2026-09-21" not in batch["attribution"]
        week_params = {
            p.name: p.value for p in batch["weekly_trades"][1].query_parameters
        }
        assert week_params["week_start"] == calendar_monday

    def test_options_moves_does_not_scan_all_symbols_prices(self):
        batch = build_daily_review_batch(
            "AND tenant_id IN ('snaptrade:abc')",
            date(2026, 8, 13), date(2026, 8, 10))
        opt_sql, _ = batch["today_options_moves"]
        assert "stg_daily_prices" not in opt_sql
        assert "date <= @as_of" in opt_sql
        assert "DATE_SUB(@as_of, INTERVAL 10 DAY)" in opt_sql


    def test_upcoming_divs_guards_zero_spacing(self):
        from app.weekly_review import UPCOMING_DIVIDENDS_QUERY
        assert "NULLIF(c.median_spacing_days, 0)" in UPCOMING_DIVIDENDS_QUERY


class TestReviewSessionDates:
    """Before the U.S. open, Daily Review describes the last completed
    session — not calendar-today's empty UTC-forward-filled row."""

    friday = date(2026, 8, 14)
    thursday = date(2026, 8, 13)
    wednesday = date(2026, 8, 12)

    def test_pre_market_friday_uses_thursday_fills(self):
        assert _trades_as_of_date(
            self.friday, {"state": "pre_market"}, et_today=self.friday
        ) == self.thursday

    def test_open_friday_uses_friday_fills(self):
        assert _trades_as_of_date(
            self.friday, {"state": "open"}, et_today=self.friday
        ) == self.friday

    def test_after_hours_friday_uses_friday_fills(self):
        assert _trades_as_of_date(
            self.friday, {"state": "after_hours"}, et_today=self.friday
        ) == self.friday

    def test_weekend_uses_friday_fills(self):
        saturday = date(2026, 8, 15)
        assert _trades_as_of_date(
            saturday, {"state": "weekend"}, et_today=saturday
        ) == self.friday

    def test_pt_thursday_evening_does_not_skip_to_wednesday(self):
        # 9pm PT Thursday = Friday 12am ET pre-market. User today is still
        # Thursday; walking back from Friday would wrongly show Wednesday.
        assert _trades_as_of_date(
            self.thursday, {"state": "pre_market"}, et_today=self.friday
        ) == self.thursday

    def test_snapshot_cutoff_drops_friday_forward_fill_before_the_bell(self):
        assert _snapshot_as_of_date(
            self.friday, {"state": "pre_market"}, et_today=self.friday
        ) == self.thursday
        assert _snapshot_as_of_date(
            self.friday, {"state": "open"}, et_today=self.friday
        ) == self.thursday

    def test_snapshot_cutoff_after_hours_is_previous_session(self):
        # Same-evening warehouse rows are not a finished recap — wait
        # until the next calendar day (Friday evening stays on Thursday;
        # Monday evening stays on Friday).
        assert _snapshot_as_of_date(
            self.friday, {"state": "after_hours"}, et_today=self.friday
        ) == self.thursday
        monday = date(2026, 8, 31)
        friday = date(2026, 8, 28)
        assert _snapshot_as_of_date(
            monday, {"state": "after_hours"}, et_today=monday
        ) == friday
        # yfinance having Monday's close must not promote an unfinished
        # warehouse row (the −$13k vs ~−$1k recap).
        assert _snapshot_as_of_date(
            monday, {"state": "after_hours"},
            close_as_of=monday, et_today=monday
        ) == friday

    def test_snapshot_cutoff_weekend_is_friday(self):
        saturday = date(2026, 8, 15)
        assert _snapshot_as_of_date(
            saturday, {"state": "weekend"}, et_today=saturday
        ) == self.friday

    def test_snapshot_cutoff_weekend_does_not_rewind_to_thursday(self):
        """Stale warehouse close must not hide Friday once the weekend starts."""
        saturday = date(2026, 8, 15)
        assert _snapshot_as_of_date(
            saturday, {"state": "weekend"},
            close_as_of=self.thursday, et_today=saturday
        ) == self.friday

    def test_session_is_live_only_open_or_after_hours(self):
        from app.weekly_review import _session_is_live
        assert _session_is_live({"state": "open"}) is True
        assert _session_is_live({"state": "after_hours"}) is True
        assert _session_is_live({"state": "weekend"}) is False
        assert _session_is_live({"state": "pre_market"}) is False

    def test_snapshot_cutoff_prefers_official_close_when_older(self):
        # Friday after-hours but yfinance hasn't published Friday's close
        # yet — the spine row is still Thursday's balance copied forward.
        assert _snapshot_as_of_date(
            self.friday, {"state": "after_hours"},
            close_as_of=self.thursday, et_today=self.friday
        ) == self.thursday

    def test_rewound_cutoff_rebinds_session_trades_to_same_date(self):
        # Tuesday's nominal recap is Monday, but the warehouse only proves
        # Friday's close. The header and fill query must both rewind to Friday.
        tuesday = date(2026, 9, 1)
        monday = date(2026, 8, 31)
        friday = date(2026, 8, 28)
        tenant_filter = "AND tenant_id IN ('snaptrade:abc')"
        batch = {"today_moves": pd.DataFrame({"today_date": [friday]})}

        (
            cutoff,
            trade_query,
            scorecard_week,
            attribution_query,
        ) = _review_session_cutoff_and_trade_query(
            tenant_filter,
            tuesday,
            {"state": "after_hours"},
            monday,
            batch,
            et_today=tuesday,
        )

        assert cutoff == friday
        assert trade_query is not None
        sql, cfg = trade_query
        assert tenant_filter in sql
        params = {p.name: p.value for p in cfg.query_parameters}
        assert params["day"] == cutoff
        assert scorecard_week == date(2026, 8, 24)
        assert attribution_query is not None
        assert "close_date >= DATE '2026-08-24'" in attribution_query

    def test_unchanged_cutoff_does_not_repeat_session_trade_query(self):
        batch = {
            "today_moves": pd.DataFrame({"today_date": [self.thursday]}),
        }
        (
            cutoff,
            trade_query,
            scorecard_week,
            attribution_query,
        ) = _review_session_cutoff_and_trade_query(
            "AND 1=0",
            self.friday,
            {"state": "after_hours"},
            self.thursday,
            batch,
            et_today=self.friday,
        )
        assert cutoff == self.thursday
        assert trade_query is None
        assert scorecard_week == date(2026, 8, 10)
        assert attribution_query is None

    def test_deferred_overview_rewinds_heatmap_and_trade_highlights(self, monkeypatch):
        """The skeleton's /overview/below fragment must use the hero's close.

        Tuesday's nominal recap is Monday, but movers only prove Friday. The
        deferred heatmap cutoff and its fill-derived highlights must both
        rewind to Friday instead of contradicting the already-rewound hero.
        """
        from types import SimpleNamespace

        import app.routes as routes
        import app.weekly_review as weekly_review
        from app import app

        tuesday = date(2026, 9, 1)
        monday = date(2026, 8, 31)
        friday = date(2026, 8, 28)
        tenant_id = "snaptrade:abc"
        observed = {"parallel_calls": []}

        all_keys = (
            set(weekly_review.OVERVIEW_BELOW_KEYS)
            | {
                "positions", "today_trades",
                "today_moves", "today_options_moves",
            }
        )
        monkeypatch.setattr(
            weekly_review,
            "build_daily_review_batch",
            lambda *args, **kwargs: {key: key for key in all_keys},
        )

        monday_fills = pd.DataFrame({
            "tenant_id": [tenant_id],
            "account": ["Main"],
            "underlying_symbol": ["MONDAY_FILL"],
        })
        friday_fills = pd.DataFrame({
            "tenant_id": [tenant_id],
            "account": ["Main"],
            "underlying_symbol": ["FRIDAY_FILL"],
        })

        def fake_parallel(_client, queries):
            observed["parallel_calls"].append(set(queries))
            if set(queries) == {"today_trades"}:
                _sql, cfg = queries["today_trades"]
                params = {p.name: p.value for p in cfg.query_parameters}
                assert params["day"] == friday
                return {"today_trades": friday_fills.copy()}
            if set(queries) == {"attribution"}:
                sql = queries["attribution"]
                assert "close_date >= DATE '2026-08-24'" in sql
                return {"attribution": pd.DataFrame()}

            assert "today_moves" in queries
            assert "today_options_moves" in queries
            frames = {key: pd.DataFrame() for key in queries}
            frames["today_moves"] = pd.DataFrame({
                "tenant_id": [tenant_id],
                "account": ["Main"],
                "today_date": [friday],
            })
            frames["today_trades"] = monday_fills.copy()
            return frames

        monkeypatch.setattr(weekly_review, "_bq_parallel", fake_parallel)
        monkeypatch.setattr(
            weekly_review, "_filter_df_by_tenant_ids",
            lambda df, _tenant_ids: df,
        )
        monkeypatch.setattr(
            weekly_review, "_build_open_position_strip",
            lambda *_args, **_kwargs: ([], []),
        )
        monkeypatch.setattr(
            weekly_review, "_us_market_session",
            lambda: {"state": "after_hours"},
        )
        monkeypatch.setattr(
            weekly_review, "_date_in_user_tz", lambda _tz: tuesday,
        )
        monkeypatch.setattr(
            weekly_review, "get_user_profile",
            lambda _uid: {"timezone": "America/New_York"},
        )
        monkeypatch.setattr(
            weekly_review, "get_bigquery_client", lambda: object(),
        )
        monkeypatch.setattr(
            weekly_review, "_tenants_for_scope", lambda _account: [tenant_id],
        )
        monkeypatch.setattr(
            weekly_review, "_tenant_sql_and",
            lambda _tenant_ids: f"AND tenant_id IN ('{tenant_id}')",
        )
        monkeypatch.setattr(
            weekly_review, "_user_account_list", lambda: ["Main"],
        )
        monkeypatch.setattr(
            weekly_review, "current_user", SimpleNamespace(id=9),
        )
        monkeypatch.setattr(
            routes, "_redirect_if_no_accounts", lambda: None,
        )
        monkeypatch.setattr(
            routes, "_tenant_label_map_for_user",
            lambda _uid: {tenant_id: "Main"},
        )

        def fake_apply(_context, _batch, **kwargs):
            observed["snap_cutoff"] = kwargs["snap_cutoff"]
            observed["scorecard_week"] = kwargs["scorecard_week"]

        def fake_split(frame, **_kwargs):
            observed["highlight_symbols"] = frame["underlying_symbol"].tolist()
            return {"symbols": observed["highlight_symbols"]}

        monkeypatch.setattr(
            weekly_review, "_apply_overview_below", fake_apply,
        )
        monkeypatch.setattr(weekly_review, "_split_day_fills", fake_split)
        monkeypatch.setattr(
            weekly_review, "render_template",
            lambda _template, **context: context,
        )

        with app.test_request_context("/overview/below"):
            context = weekly_review.overview_below.__wrapped__()

        assert context["review_date"] == friday
        assert observed["snap_cutoff"] == friday
        assert observed["scorecard_week"] == date(2026, 8, 24)
        assert observed["highlight_symbols"] == ["FRIDAY_FILL"]
        assert len(observed["parallel_calls"]) == 3

    def test_snapshot_cutoff_never_after_user_today(self):
        assert _snapshot_as_of_date(
            self.thursday, {"state": "pre_market"}, et_today=self.friday
        ) == self.thursday

    def test_overview_pending_session_after_hours_weekday(self):
        monday = date(2026, 8, 31)
        friday = date(2026, 8, 28)
        assert _overview_pending_session(
            monday, {"state": "after_hours"}, friday, et_today=monday
        ) == monday

    def test_overview_pending_session_during_open(self):
        assert _overview_pending_session(
            self.friday, {"state": "open"}, self.thursday, et_today=self.friday
        ) == self.friday

    def test_overview_pending_session_off_weekend_and_pre_market(self):
        saturday = date(2026, 8, 15)
        assert _overview_pending_session(
            saturday, {"state": "weekend"}, self.friday, et_today=saturday
        ) is None
        assert _overview_pending_session(
            self.friday, {"state": "pre_market"}, self.thursday,
            et_today=self.friday
        ) is None

    def test_overview_pending_banner_pre_market_vs_live_vs_after_hours(self):
        monday = date(2026, 8, 31)
        friday = date(2026, 8, 28)
        tuesday = date(2026, 9, 1)
        pre = _overview_pending_banner(
            tuesday, {"state": "pre_market"}, monday, et_today=tuesday)
        assert pre["live_link"] is False
        assert "Monday's close" in pre["text"]
        assert "9:30 ET" in pre["text"]
        assert "lands tomorrow" not in pre["text"]

        live = _overview_pending_banner(
            tuesday, {"state": "open"}, monday, et_today=tuesday)
        assert live["live_link"] is True
        assert "Tuesday's session is live" in live["text"]
        assert "Monday's close" in live["text"]
        assert "lands tomorrow" not in live["text"]

        after = _overview_pending_banner(
            tuesday, {"state": "after_hours"}, monday, et_today=tuesday)
        assert after["live_link"] is True
        assert "Tuesday's close isn't on Overview yet" in after["text"]
        assert "lands tomorrow" in after["text"]

        assert _overview_pending_banner(
            date(2026, 8, 15), {"state": "weekend"}, friday,
            et_today=date(2026, 8, 15),
        ) is None

    def test_frame_as_of_date_reads_movers_today_date(self):
        df = pd.DataFrame({"today_date": [self.thursday, self.wednesday]})
        assert _frame_as_of_date(df) == self.thursday

    def test_coerce_date_accepts_iso_string(self):
        assert _coerce_date("2026-08-13") == self.thursday

    def test_format_session_date_matches_pulse_label(self):
        assert _format_session_date(self.thursday) == "Thu Aug 13"
        assert _format_session_date("2026-08-28") == "Fri Aug 28"
        assert _format_session_date(None) is None

    def test_pulse_carries_date_label(self):
        pulse = _today_pulse([{
            "today_date": self.thursday,
            "comparisons": {"day": {"has_data": True, "delta": -1234.0}},
        }])
        assert pulse["delta"] == -1234
        assert pulse["date"] == "2026-08-13"
        assert pulse["date_label"] == "Thu Aug 13"


class TestDropStaleOptionRows:
    """Past-expiry / mart-Closed options must not stay on Daily Review."""

    today = date(2026, 8, 14)

    def _opt(self, **kw):
        row = {
            "tenant_id": "snaptrade:fn",
            "symbol": "FN",
            "trade_symbol": "FN    260807C00200000",
            "instrument_type": "Call",
            "option_expiry": date(2026, 8, 7),
            "option_strike": 200.0,
            "option_type": "C",
            "market_value": -150.0,
            "quantity": -1,
        }
        row.update(kw)
        return row

    def test_osi_key_ignores_spacing(self):
        a = self._opt(trade_symbol="FN    260807C00200000")
        b = self._opt(trade_symbol="FN 260807C00200000")
        assert _option_row_key(a) == _option_row_key(b)

    def test_drops_last_week_expiry(self):
        eq = {"tenant_id": "snaptrade:fn", "symbol": "FN",
              "trade_symbol": "FN", "instrument_type": "Equity",
              "option_expiry": None, "market_value": 10000.0, "quantity": 100}
        df = pd.DataFrame([self._opt(), eq])
        out = _drop_stale_option_rows(df, self.today)
        assert list(out["instrument_type"]) == ["Equity"]

    def test_keeps_live_expiry(self):
        live = self._opt(trade_symbol="FN    260821C00200000",
                         option_expiry=date(2026, 8, 21))
        df = pd.DataFrame([live])
        out = _drop_stale_option_rows(df, self.today)
        assert len(out) == 1

    def test_keeps_expiry_during_its_us_market_day(self):
        # A viewer in Tokyo is already on Saturday during Friday's U.S.
        # session. The caller must use the New York market date so this
        # still-live Friday contract is not treated as yesterday's expiry.
        live = self._opt(trade_symbol="FN    260814C00200000",
                         option_expiry=self.today)
        out = _drop_stale_option_rows(pd.DataFrame([live]), self.today)
        assert len(out) == 1

    def test_drops_closed_contract_still_in_snapshot(self):
        # Snapshot still has FN; contracts mart says nothing is Open.
        # Another symbol is Open so the frame is non-empty.
        snap = self._opt(trade_symbol="FN    260821C00200000",
                         option_expiry=date(2026, 8, 21))
        open_other = pd.DataFrame([{
            "tenant_id": "snaptrade:other",
            "symbol": "AAPL",
            "trade_symbol": "AAPL  260821C00200000",
        }])
        out = _drop_stale_option_rows(
            pd.DataFrame([snap]), self.today, open_contracts_df=open_other)
        assert out.empty

    def test_empty_open_contract_result_drops_all_snapshot_options(self):
        # A successful empty BQ result keeps its projected schema. That means
        # the mart authoritatively found zero open contracts, so an unexpired
        # broker snapshot row is stale rather than live.
        snap = self._opt(trade_symbol="FN    260821C00200000",
                         option_expiry=date(2026, 8, 21))
        open_none = pd.DataFrame(columns=[
            "tenant_id", "account", "trade_symbol", "symbol",
        ])
        out = _drop_stale_option_rows(
            pd.DataFrame([snap]), self.today, open_contracts_df=open_none)
        assert out.empty

    def test_schema_less_open_contract_failure_keeps_unexpired_options(self):
        # _bq_parallel returns a schema-less empty frame when a query fails.
        # Do not interpret that failure as proof that every option is Closed.
        live = self._opt(trade_symbol="FN    260821C00200000",
                         option_expiry=date(2026, 8, 21))
        out = _drop_stale_option_rows(
            pd.DataFrame([live]), self.today,
            open_contracts_df=pd.DataFrame())
        assert len(out) == 1

    def test_keeps_when_open_contracts_lists_it(self):
        live = self._opt(trade_symbol="FN    260821C00200000",
                         option_expiry=date(2026, 8, 21))
        open_df = pd.DataFrame([{
            "tenant_id": "snaptrade:fn",
            "symbol": "FN",
            "trade_symbol": "FN 260821C00200000",  # different spacing
        }])
        out = _drop_stale_option_rows(
            pd.DataFrame([live]), self.today, open_contracts_df=open_df)
        assert len(out) == 1


class TestTodayPageBatch:
    def test_today_batch_uses_calendar_today_for_moves_and_fills(self):
        from app.weekly_review import build_today_batch
        friday = date(2026, 8, 28)
        batch = build_today_batch("AND tenant_id IN ('snaptrade:abc')", friday)
        assert {p.name: p.value for p in batch["today_moves"][1].query_parameters}["as_of"] == friday
        assert {p.name: p.value for p in batch["today_trades"][1].query_parameters}["day"] == friday
        assert "open_options" in batch
        assert "positions" in batch

    def test_delay_copy_always_warns(self):
        from app.weekly_review import _today_delay_copy
        copy = _today_delay_copy({"state": "open"})
        assert "not the official close" in copy["shared"].lower()
        assert "open" in copy["extra"].lower()

    def test_delay_copy_weekend_points_at_overview(self):
        from app.weekly_review import _today_delay_copy
        copy = _today_delay_copy({"state": "weekend"})
        assert "overview" in copy["shared"].lower()
        assert "weekend" in copy["extra"].lower()
        assert "current session" not in copy["shared"].lower()


class TestOverviewVoice:
    def test_overview_template_does_not_say_today(self):
        from pathlib import Path
        src = (Path(__file__).resolve().parents[1]
               / "app/templates/weekly_review.html").read_text()
        assert "Trades Today" not in src
        assert "Today's Biggest" not in src
        assert "{% if _pulse_is_today %}Today:" not in src

    def test_overview_hero_is_value_then_moves(self):
        from pathlib import Path
        src = (Path(__file__).resolve().parents[1]
               / "app/templates/weekly_review.html").read_text()
        assert "Overview &middot;" not in src
        assert "Total value" in src
        assert "Share of book" in src
        assert "ov-date" in src
        assert "{% if today_pulse %}" not in src

    def test_weekly_review_url_is_overview(self):
        from app import app
        with app.test_request_context():
            from flask import url_for
            assert url_for("weekly_review") == "/overview"
            assert url_for("today_view") == "/today"

    def test_since_last_looked_lives_on_today_not_overview(self):
        from pathlib import Path
        root = Path(__file__).resolve().parents[1] / "app" / "templates"
        overview = (root / "weekly_review.html").read_text()
        today = (root / "today.html").read_text()
        partial = (root / "_since_last_looked.html").read_text()
        assert "Since you last looked" not in overview
        assert "{% include '_since_last_looked.html' %}" in today
        assert "Since you last looked" in partial


class TestOpenPositionStrip:
    def test_aggregates_equity_price_onto_strip(self):
        from datetime import date
        import pandas as pd
        from app.weekly_review import _build_open_position_strip

        df = pd.DataFrame([{
            "symbol": "SMTC",
            "trade_symbol": "SMTC",
            "instrument_type": "Equity",
            "market_value": 1000.0,
            "cost_basis": 900.0,
            "unrealized_pnl": 100.0,
            "current_price": 50.0,
            "quantity": 20,
            "option_strike": 0,
            "latest_stock_price": 50.0,
            "option_expiry": None,
            "option_type": None,
        }])
        strip, expiring = _build_open_position_strip(df, date(2026, 9, 1))
        assert len(strip) == 1
        assert strip[0]["symbol"] == "SMTC"
        assert strip[0]["price"] == 50.0
        assert expiring == []


class TestThisIsYouFacts:
    """First-week Overview strip: symbols held / next on the clock / style."""

    def test_empty_everything_still_returns_next_on_clock_fact(self):
        from app.weekly_review import _build_this_is_you_facts
        facts = _build_this_is_you_facts([], [], None)
        assert len(facts) == 1
        assert facts[0]["label"] == "Next on the clock"
        assert facts[0]["value"] == "Nothing"

    def test_symbols_held_counts_strip(self):
        from app.weekly_review import _build_this_is_you_facts
        strip = [{"symbol": "AAPL"}, {"symbol": "MSFT"}]
        facts = _build_this_is_you_facts(strip, [], None)
        held = next(f for f in facts if f["label"] == "Symbols held")
        assert held["value"] == "2"

    def test_next_on_clock_uses_nearest_expiring_option(self):
        from app.weekly_review import _build_this_is_you_facts
        expiring = [{"symbol": "TSLA", "days_to_exp": 3},
                    {"symbol": "NVDA", "days_to_exp": 9}]
        facts = _build_this_is_you_facts([], expiring, None)
        clock = next(f for f in facts if f["label"] == "Next on the clock")
        assert clock["value"] == "TSLA"
        assert "3 day" in clock["detail"]

    def test_style_fact_leans_income_when_premium_dominates(self):
        from app.weekly_review import _build_this_is_you_facts
        style_df = pd.DataFrame([
            {"tenant_id": "t1", "premium_collected": 500.0, "option_capital_paid": 50.0},
        ])
        facts = _build_this_is_you_facts([], [], style_df)
        style = next(f for f in facts if "Leaning" in f["label"])
        assert style["label"] == "Leaning income"
        assert style["value"] == "$500"

    def test_style_fact_leans_directional_when_paid_dominates(self):
        from app.weekly_review import _build_this_is_you_facts
        style_df = pd.DataFrame([
            {"tenant_id": "t1", "premium_collected": 20.0, "option_capital_paid": 400.0},
        ])
        facts = _build_this_is_you_facts([], [], style_df)
        style = next(f for f in facts if "Leaning" in f["label"])
        assert style["label"] == "Leaning directional"
        assert style["value"] == "$400"

    def test_style_fact_omitted_when_no_option_activity(self):
        from app.weekly_review import _build_this_is_you_facts
        style_df = pd.DataFrame([
            {"tenant_id": "t1", "premium_collected": 0.0, "option_capital_paid": 0.0},
        ])
        facts = _build_this_is_you_facts([], [], style_df)
        assert not any("Leaning" in f["label"] for f in facts)

    def test_style_fact_omitted_when_frame_none_or_empty(self):
        from app.weekly_review import _build_this_is_you_facts
        assert not any("Leaning" in f["label"]
                       for f in _build_this_is_you_facts([], [], None))
        assert not any("Leaning" in f["label"]
                       for f in _build_this_is_you_facts([], [], pd.DataFrame()))


class TestDayTradesSettlementQuery:
    """DAY_TRADES_QUERY dates expiry to Friday and drops the Monday post."""

    def test_history_excludes_settlement_actions(self):
        sql = DAY_TRADES_QUERY
        assert "AND action NOT IN (" in sql
        assert "'option_expired'" in sql
        assert "'option_assigned'" in sql
        assert "'option_exercised'" in sql

    def test_unions_contracts_on_realized_close_date(self):
        sql = DAY_TRADES_QUERY
        assert "int_option_contracts" in sql
        assert "realized_close_date = @day" in sql
        assert "ExpiredOTM" in sql
        assert "UNION ALL" in sql
        assert "option_expiry" in sql

    def test_joins_position_legs_for_tag_anchor(self):
        sql = DAY_TRADES_QUERY
        assert "int_position_legs" in sql
        assert "leg_open_date" in sql
        assert "last_activity_date >= @day" in sql

    def test_projects_direction_so_spreads_can_pair(self):
        sql = DAY_TRADES_QUERY
        assert "c.direction" in sql
        assert "WHEN f.action IN ('option_sell_to_open', 'option_buy_to_close')" in sql
        assert "THEN 'Sold'" in sql
        assert "THEN 'Bought'" in sql
        # Settlements pass the contract side through. History and the
        # UNION arm must both end on direction or the columns drift.
        assert "realized_pnl,\n        direction" in sql


class TestCoveredCallsWithoutShort:
    """Today notes Covered Call names holding stock with no short call."""

    def _row(self, symbol, shares, tenant="t1", account="Sara Investment"):
        return {
            "tenant_id": tenant, "account": account,
            "symbol": symbol, "shares": shares,
        }

    def test_empty_frame_is_none(self):
        assert _covered_calls_without_short(pd.DataFrame()) is None
        assert _covered_calls_without_short(None) is None

    def test_nvda_and_be_named(self):
        df = pd.DataFrame([
            self._row("NVDA", 100),
            self._row("BE", 200),
        ])
        out = _covered_calls_without_short(df)
        assert [r["symbol"] for r in out["rows"]] == ["BE", "NVDA"]
        assert out["rows"][0]["shares"] == 200
        assert out["rows"][1]["shares"] == 100

    def test_drops_lots_under_100_shares(self):
        df = pd.DataFrame([self._row("FOO", 50)])
        assert _covered_calls_without_short(df) is None

    def test_collapses_same_symbol_across_accounts(self):
        df = pd.DataFrame([
            self._row("NVDA", 100, tenant="t1", account="A"),
            self._row("NVDA", 200, tenant="t2", account="B"),
        ])
        out = _covered_calls_without_short(
            df, label_map={"t1": "A", "t2": "B"})
        assert len(out["rows"]) == 1
        assert out["rows"][0]["shares"] == 300
        assert out["rows"][0]["accounts"] == ["A", "B"]

    def test_query_requires_cc_history_and_no_open_short(self):
        from app.weekly_review import COVERED_CALL_UNWRITTEN_QUERY
        sql = COVERED_CALL_UNWRITTEN_QUERY
        assert "Covered Call" in sql
        assert "Partially Covered Call" in sql
        assert "int_option_contracts" in sql
        assert "HAVING SUM(quantity) >= 100" in sql
        assert "direction = 'Sold'" in sql
        assert "{tenant_filter}" in sql

    def test_today_batch_includes_unwritten_query(self):
        from datetime import date
        from app.weekly_review import build_today_batch
        batch = build_today_batch("", date(2026, 8, 31))
        assert "cc_unwritten" in batch

    def test_today_heading_and_overview_share_basis_are_labeled(self):
        from pathlib import Path
        root = Path(__file__).resolve().parents[1]
        today = (root / "app/templates/today.html").read_text()
        assert 'class="tt-cc-heading"' in today
        assert "those lots only" in today
        assert "Here are your covered call positions" not in today
        below = (root / "app/templates/_overview_below.html").read_text()
        assert "shares held" in below
        filters = (root / "app/templates/_account_scope_filters.html").read_text()
        assert "addEventListener" not in filters
        assert "scope-filters.js" in filters
        base = (root / "app/templates/base.html").read_text()
        assert "js/scope-filters.js" in base


def test_fill_count_copy_names_grouped_trades():
    from jinja2 import Environment
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "app/templates/_fill_count.html").read_text()
    tpl = Environment().from_string(src)
    assert tpl.render(count=2, fill_count=4) == "2 trades from 4 fills"
    assert tpl.render(count=1, fill_count=2) == "1 trade from 2 fills"
    assert tpl.render(count=4, fill_count=4) == "4 fills"
    assert tpl.render(count=1, fill_count=None) == "1 fill"

