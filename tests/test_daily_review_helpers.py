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
    _build_account_breakdown,
    _build_benchmark_rows,
    _build_benchmark_snapshot,
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
