"""Unit tests for app/weekly_review.py — new Daily Review helpers.

The page was rebuilt May 2026 as a single-mode "Daily Review" (the
Friday / Monday / Mid-Week mode toggle was removed). These tests pin
the new per-symbol attribution math + rollups so a future regression
doesn't quietly flip a column to zero or extrapolate a $0.50 capital
position to 10,000%/yr.

The endpoint name stayed `weekly_review` for url_for() compat, so the
module path is unchanged.
"""
from datetime import date

import pandas as pd

from app.weekly_review import (
    ANNUALIZED_DENOMINATOR_FLOOR,
    ANNUALIZED_MIN_DAYS,
    TODAY_OPTIONS_MOVES_QUERY,
    _aggregate_breakdown_by,
    _annualized_pct,
    _build_account_breakdown,
    _build_benchmark_rows,
    _build_benchmark_snapshot,
    _build_after_hours_movers,
    _build_breakdown_totals,
    _build_position_breakdown,
    _build_today_movers,
    _build_trades_this_week,
    _build_upcoming_dividends,
    _coerce_date,
    _group_day_rolls,
    _next_ex_div_on_or_after,
    _drop_stale_option_rows,
    _format_trade_contract,
    _frame_as_of_date,
    _option_row_key,
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
        """UTC tomorrow rows must not replace the latest U.S. trading day."""
        normalized = " ".join(TODAY_OPTIONS_MOVES_QUERY.lower().split())
        assert "max(date) as as_of_date" in normalized
        assert "analytics.stg_daily_prices" in normalized
        assert "m.date <= c.as_of_date" in normalized

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

    def test_btc_then_sto_call_credit_and_strike_up_is_successful(self):
        df = pd.DataFrame([
            self._jpm("option_buy_to_close", "JPM   260821C00300000", -120),
            self._jpm("option_sell_to_open", "JPM   260828C00305000", 180),
        ])
        out = _split_day_fills(df)
        assert out["count"] == 2
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

    def test_fill_count_stays_raw_when_grouped(self):
        df = pd.DataFrame([
            self._jpm("option_buy_to_close", "JPM   260821C00300000", -120),
            self._jpm("option_sell_to_open", "JPM   260828C00305000", 180),
            self._row(action="equity_sell", trade_symbol="SPCE",
                      underlying_symbol="SPCE", quantity=1, price=3.07,
                      amount=3.07, instrument_type="Equity",
                      tenant_id="snaptrade:cam", account="Cameron Investment"),
        ])
        out = _split_day_fills(df)
        assert out["count"] == 3
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

    def test_snapshot_cutoff_after_hours_is_today(self):
        assert _snapshot_as_of_date(
            self.friday, {"state": "after_hours"}, et_today=self.friday
        ) == self.friday

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

    def test_snapshot_cutoff_never_after_user_today(self):
        assert _snapshot_as_of_date(
            self.thursday, {"state": "pre_market"}, et_today=self.friday
        ) == self.thursday

    def test_frame_as_of_date_reads_movers_today_date(self):
        df = pd.DataFrame({"today_date": [self.thursday, self.wednesday]})
        assert _frame_as_of_date(df) == self.thursday

    def test_coerce_date_accepts_iso_string(self):
        assert _coerce_date("2026-08-13") == self.thursday

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

    def test_weekly_review_url_is_overview(self):
        from app import app
        with app.test_request_context():
            from flask import url_for
            assert url_for("weekly_review") == "/overview"
            assert url_for("today_view") == "/today"

