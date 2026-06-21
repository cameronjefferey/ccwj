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
    _aggregate_breakdown_by,
    _annualized_pct,
    _build_breakdown_totals,
    _build_position_breakdown,
    _build_today_movers,
    _build_trades_this_week,
    _build_upcoming_dividends,
    _format_trade_contract,
    _today_headline,
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
    def test_empty_input(self):
        result = _build_today_movers(None)
        assert result == {"winners": [], "losers": [], "total_impact": 0.0, "as_of": None}
        result = _build_today_movers(pd.DataFrame())
        assert result["winners"] == []

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
        assert result["losers"][0]["symbol"] == "TSLA"
        assert result["total_impact"] == 50.0
        assert result["as_of"] == "2026-05-18"


class TestBuildUpcomingDividends:
    def test_empty_input(self):
        assert _build_upcoming_dividends(None) == []
        assert _build_upcoming_dividends(pd.DataFrame()) == []

    def test_sorted_by_days_until(self):
        df = pd.DataFrame([
            {"symbol": "JEPI", "last_ex_div_date": date(2026, 4, 15),
             "last_amount_per_share": 0.45, "median_spacing_days": 30,
             "projected_next_ex_div_date": date(2026, 5, 25),
             "days_until_projected": 7,
             "sector": "Financial Services", "subsector": "Asset Management",
             "long_name": "JPMorgan EPI"},
            {"symbol": "SCHD", "last_ex_div_date": date(2026, 3, 20),
             "last_amount_per_share": 0.78, "median_spacing_days": 91,
             "projected_next_ex_div_date": date(2026, 6, 19),
             "days_until_projected": 32,
             "sector": "Financial Services", "subsector": "Asset Management",
             "long_name": "Schwab US Dividend ETF"},
            {"symbol": "BKH", "last_ex_div_date": date(2026, 3, 1),
             "last_amount_per_share": 0.665, "median_spacing_days": 91,
             "projected_next_ex_div_date": date(2026, 5, 31),
             "days_until_projected": 13,
             "sector": "Utilities", "subsector": "Diversified Utilities",
             "long_name": "Black Hills"},
        ])
        rows = _build_upcoming_dividends(df)
        # Sorted by days_until ascending: 7, 13, 32.
        assert [r["symbol"] for r in rows] == ["JEPI", "BKH", "SCHD"]


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

    def test_closed_expiry_in_week(self):
        df = pd.DataFrame([self._row()])
        out = _build_trades_this_week(
            df, self.WEEK_START, self.WEEK_END, label_map={"snaptrade:abc": "Sara Investment"}
        )
        assert out["count"] == 1
        assert out["closed_count"] == 1
        assert out["opened_count"] == 0  # opened last week (Jun 5), not this week
        assert out["realized_pnl"] == 226.0
        r = out["trades"][0]
        assert r["is_closed"] is True
        assert r["account_display"] == "Sara Investment"
        assert r["contract"] == "ASTS Jun 5 $102 Call"

    def test_opened_this_week_hides_synthetic_zero_trade_rows(self):
        df = pd.DataFrame([
            self._row(symbol="NEW", trade_symbol="NEW_session_1", status="Open",
                      open_date=date(2026, 6, 9), close_date=None, num_trades=1),
            self._row(symbol="SYN", trade_symbol="SYN_session_1", status="Open",
                      open_date=date(2026, 6, 9), close_date=None, num_trades=0),
        ])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        opened_syms = {r["symbol"] for r in out["trades"] if r["opened_this_week"]}
        assert "NEW" in opened_syms
        assert "SYN" not in opened_syms  # num_trades==0 synthetic snapshot open

    def test_open_and_close_same_week_is_single_row(self):
        # A same-week round trip is ONE trade group → ONE row (tagged closed
        # so the table shows its realized P&L). It still counts toward both
        # the opened and closed summary counters.
        df = pd.DataFrame([
            self._row(open_date=date(2026, 6, 9), close_date=date(2026, 6, 11)),
        ])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        assert out["count"] == 1
        assert out["closed_count"] == 1
        assert out["opened_count"] == 1
        r = out["trades"][0]
        assert r["is_closed"] is True
        assert r["opened_this_week"] is True

    def test_closed_outside_week_excluded(self):
        df = pd.DataFrame([
            self._row(close_date=date(2026, 6, 1), open_date=date(2026, 5, 28)),
        ])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        assert out["has_any"] is False

    def test_closed_row_result_is_realized(self):
        # Closed row → one number: realized lifetime P&L.
        df = pd.DataFrame([self._row()])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        r = out["trades"][0]
        assert r["result_kind"] == "realized"
        assert r["result_pnl"] == 226.0

    def test_open_row_result_is_unrealized(self):
        # Open row → unrealized G/L at the latest snapshot (open premium +
        # current value), NOT capital deployed.
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
        assert r["result_kind"] == "unrealized"
        assert r["result_pnl"] == 140.0
        assert r["current_market_value"] == 300.0

    def test_same_contract_open_and_closed_collapses_to_one_closed_row(self):
        # The bug the user saw: the SAME option contract surfaces as both an
        # opened-this-week group and a closed-this-week group. Collapse to a
        # SINGLE row — the Closed (realized) outcome wins.
        ts = "ASTS  260605C00102000"
        df = pd.DataFrame([
            self._row(trade_symbol=ts, status="Open", open_date=date(2026, 6, 9),
                      close_date=None, num_trades=1, total_pnl=0.0,
                      current_unrealized_pnl=50.0, current_market_value=120.0),
            self._row(trade_symbol=ts, status="Closed", open_date=date(2026, 6, 5),
                      close_date=date(2026, 6, 11), num_trades=2, total_pnl=226.0),
        ])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        assert out["count"] == 1
        r = out["trades"][0]
        assert r["is_closed"] is True
        assert r["result_kind"] == "realized"
        assert r["result_pnl"] == 226.0

    def test_distinct_contracts_same_underlying_stay_separate(self):
        # Two different strikes on the same underlying are different
        # contracts → two rows (not collapsed).
        df = pd.DataFrame([
            self._row(trade_symbol="ASTS  260605C00079000", status="Closed",
                      open_date=date(2026, 6, 5), close_date=date(2026, 6, 8),
                      total_pnl=-58.0),
            self._row(trade_symbol="ASTS  260612C00098000", status="Closed",
                      open_date=date(2026, 6, 8), close_date=date(2026, 6, 12),
                      total_pnl=505.0),
        ])
        out = _build_trades_this_week(df, self.WEEK_START, self.WEEK_END)
        assert out["count"] == 2
        assert out["closed_count"] == 2
