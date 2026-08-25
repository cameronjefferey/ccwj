"""Tests for the dividends-as-first-class P&L stream.

The product treats dividends like equity and option P&L:
  * total_pnl on positions/strategies includes attributed dividends
  * Buy-and-Hold positions where yield outweighs the price move
    (div > abs(trade_pnl) and div >= $50) get reclassified as Dividend.
    A coupon on a losing buy-and-hold stays Buy and Hold.
  * Weekly Review headline P&L (total_return) includes dividend cash flows

These tests pin the contract so a refactor doesn't quietly drop dividends
from the headline numbers (the previous regression that motivated this
change was that a dividend-focused buy-and-hold trader saw $0 P&L because
total_pnl was trade-only).
"""
from __future__ import annotations

import re

import pytest

from app.positions_page import yield_is_primary


def test_yield_is_primary_screenshot_rows():
    """Dad's /positions table: SCHD is a yield story; the rest are not."""
    assert yield_is_primary(118.38, 78.43) is True          # SCHD
    assert yield_is_primary(0.87, -20.85) is False          # QTUM Keeley
    assert yield_is_primary(0.52, -93.11) is False          # UFO Keeley
    assert yield_is_primary(15.80, -310.42) is False        # MSFT
    assert yield_is_primary(59.66, -2107.14) is False       # QTUM Sara
    assert yield_is_primary(3.13, -2223.25) is False        # VRT
    assert yield_is_primary(17.45, -6571.50) is False       # UFO Sara
    assert yield_is_primary(3000.0, -2000.0) is True        # JEPI-class: yield still wins
    assert yield_is_primary(40.0, 10.0) is False            # under $50 dust floor
    assert yield_is_primary(None, 100.0) is False


# ---------------------------------------------------------------------------
# Weekly Review aggregation
# ---------------------------------------------------------------------------
class TestAggregateWeeklyRowsIncludesDividends:
    """`_aggregate_weekly_rows` rolls per-account weekly rows into the
    review dict shown on /weekly-review. After the dividends-as-first-class
    change, dividends_amount and total_return must be present and correct.
    """

    def _aggregate(self, rows):
        from app.weekly_review import _aggregate_weekly_rows

        return _aggregate_weekly_rows(rows)

    def test_total_return_is_total_pnl_plus_dividends(self):
        rows = [
            {
                "account": "A",
                "trades_closed": 3,
                "total_pnl": 200.0,
                "dividends_amount": 50.0,
                "num_winners": 2,
                "num_losers": 1,
                "premium_received": 0.0,
                "premium_paid": 0.0,
                "trades_opened": 1,
                "best_pnl": 120.0,
                "best_symbol": "AAPL",
                "best_strategy": "Wheel",
                "best_trade_symbol": "AAPL_session_1",
                "best_close_date": "2025-04-01",
                "worst_pnl": -10.0,
                "worst_symbol": "MSFT",
                "worst_strategy": "Long Call",
                "worst_trade_symbol": "MSFT_session_2",
                "worst_close_date": "2025-04-03",
                "top_strategy": "Wheel",
                "top_strategy_win_rate": 0.75,
                "top_strategy_trades": 4,
                "top_strategy_pnl": 250.0,
            }
        ]
        out = self._aggregate(rows)
        assert out is not None
        assert out["total_pnl"] == pytest.approx(200.0)
        assert out["dividends_amount"] == pytest.approx(50.0)
        assert out["total_return"] == pytest.approx(250.0)

    def test_dividends_aggregate_across_accounts(self):
        rows = [
            {
                "account": "A",
                "trades_closed": 1,
                "total_pnl": 100.0,
                "dividends_amount": 25.0,
                "num_winners": 1,
                "num_losers": 0,
                "premium_received": 0,
                "premium_paid": 0,
                "trades_opened": 0,
            },
            {
                "account": "B",
                "trades_closed": 2,
                "total_pnl": -50.0,
                "dividends_amount": 75.0,
                "num_winners": 1,
                "num_losers": 1,
                "premium_received": 0,
                "premium_paid": 0,
                "trades_opened": 1,
            },
        ]
        out = self._aggregate(rows)
        assert out["total_pnl"] == pytest.approx(50.0)
        assert out["dividends_amount"] == pytest.approx(100.0)
        # total_return = 50 + 100 = 150, even though one account is down on trades
        assert out["total_return"] == pytest.approx(150.0)

    def test_no_dividends_falls_back_to_trade_only(self):
        rows = [
            {
                "account": "A",
                "trades_closed": 1,
                "total_pnl": 75.0,
                "dividends_amount": 0.0,
                "num_winners": 1,
                "num_losers": 0,
                "premium_received": 0,
                "premium_paid": 0,
                "trades_opened": 0,
            }
        ]
        out = self._aggregate(rows)
        assert out["total_pnl"] == pytest.approx(75.0)
        assert out["dividends_amount"] == pytest.approx(0.0)
        assert out["total_return"] == pytest.approx(75.0)

    def test_missing_dividends_field_is_treated_as_zero(self):
        # Older callers / tests that pre-date the dividends column should
        # still produce sane results — total_return == total_pnl when no
        # dividends key is present (defensive for staged rollout).
        rows = [
            {
                "account": "A",
                "trades_closed": 1,
                "total_pnl": 75.0,
                "num_winners": 1,
                "num_losers": 0,
                "premium_received": 0,
                "premium_paid": 0,
                "trades_opened": 0,
            }
        ]
        out = self._aggregate(rows)
        assert out["total_pnl"] == pytest.approx(75.0)
        assert out["dividends_amount"] == pytest.approx(0.0)
        assert out["total_return"] == pytest.approx(75.0)


# ---------------------------------------------------------------------------
# Routes-side date-filtered query
# ---------------------------------------------------------------------------
class TestDateFilteredQueryDividendInclusive:
    """`DATE_FILTERED_QUERY` re-aggregates int_strategy_classification with
    a date window for the /positions date filter. It must mirror
    positions_summary's dividends-as-first-class semantics: total_pnl
    includes dividends and Buy-and-Hold gets reclassified to "Dividend"
    only when yield outweighs the price move.

    These string-level checks catch regressions where someone copy-pastes
    the older trade-only block back in.
    """

    def _query(self) -> str:
        from app import routes

        return routes.DATE_FILTERED_QUERY

    def test_query_reclassifies_buy_and_hold_to_dividend(self):
        sql = self._query()
        # Look for the THEN 'Dividend' clause guarded by Buy and Hold +
        # the dividend-rank gate. We search loosely across whitespace so
        # cosmetic reformatting doesn't fail the test.
        normalized = re.sub(r"\s+", " ", sql)
        assert "'Dividend'" in normalized
        assert "Buy and Hold" in normalized
        assert "dividend_rank = 1" in normalized
        # Yield must outweigh the price move (either direction) and beat
        # the $50 dust floor. GREATEST(pnl, 0) is the old bug.
        assert "ABS(wa.total_pnl)" in normalized
        assert "attributed_dividend_income >= 50" in normalized
        assert "GREATEST" not in normalized.upper()

    def test_query_total_pnl_includes_attributed_dividends(self):
        sql = self._query()
        normalized = re.sub(r"\s+", " ", sql)
        # The final SELECT computes total_pnl as
        #   total_pnl + attributed_dividend_income
        assert "attributed_dividend_income" in normalized
        assert (
            "wa.total_pnl + wa.attributed_dividend_income" in normalized
            or "total_pnl + wa.attributed_dividend_income" in normalized
        )

    def test_query_total_return_is_alias_of_total_pnl(self):
        sql = self._query()
        normalized = re.sub(r"\s+", " ", sql)
        # Both total_pnl and total_return are derived from the same
        # expression — they must equal each other on every row.
        # We assert the alias is present.
        assert "AS total_return" in normalized

    def test_query_partitions_dividend_rank_by_tenant_id(self):
        # Tenancy hardening: dividend rank must be partitioned by tenant_id
        # (plus account/user_id/symbol) so colliding account labels across
        # physical accounts can't share a dividend ranking.
        sql = self._query()
        normalized = re.sub(r"\s+", " ", sql)
        assert "PARTITION BY ss.tenant_id, ss.account, ss.user_id, ss.symbol" in normalized

    def test_query_joins_dividends_on_user_id(self):
        sql = self._query()
        normalized = re.sub(r"\s+", " ", sql)
        # IS NOT DISTINCT FROM is the NULL-safe equality used everywhere
        # in the user_id-aware joins (see docs/USER_ID_TENANCY.md).
        assert "wdr.user_id IS NOT DISTINCT FROM d.user_id" in normalized


# ---------------------------------------------------------------------------
# dbt model invariants — string-level pin so regressions are caught at PR time
# ---------------------------------------------------------------------------
class TestPositionsSummaryModelDividendsFirstClass:
    """`positions_summary.sql` is the source of truth for the headline P&L
    column on /positions, /position/<symbol>, and /strategies. After this
    change it MUST:
      (1) fold attributed dividend income into total_pnl,
      (2) reclassify Buy-and-Hold to "Dividend" only when yield
          outweighs the price move (div > abs(pnl) and div >= $50),
      (3) keep total_return as a back-compat alias of total_pnl.

    String-level checks because we don't run BigQuery in unit tests.
    """

    @pytest.fixture
    def sql(self) -> str:
        path = "dbt/models/marts/positions_summary.sql"
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def test_total_pnl_includes_attributed_dividends(self, sql):
        normalized = re.sub(r"\s+", " ", sql)
        # The final SELECT must add attributed_dividend_income to total_pnl.
        assert "wad.total_pnl + wad.attributed_dividend_income" in normalized
        assert "as total_pnl" in normalized

    def test_dividend_strategy_reclassification(self, sql):
        normalized = re.sub(r"\s+", " ", sql)
        # We reclassify only when this strategy is the dividend-rank
        # holder, the strategy is currently 'Buy and Hold', and dividends
        # outweigh the price move — not GREATEST(pnl, 0).
        assert "'Dividend'" in normalized
        assert "wad.strategy = 'Buy and Hold'" in normalized
        assert "abs(wad.total_pnl)" in normalized
        assert "attributed_dividend_income >= 50" in normalized
        assert "greatest(wad.total_pnl, 0)" not in normalized

    def test_total_return_is_alias(self, sql):
        normalized = re.sub(r"\s+", " ", sql)
        # total_return uses the same expression as total_pnl.
        assert "as total_return" in normalized

    def test_trade_only_pnl_preserved_for_breakdown(self, sql):
        # Consumers that need pre-dividend P&L (e.g. waterfall recon) read
        # trade_only_pnl. Make sure it's still surfaced.
        normalized = re.sub(r"\s+", " ", sql)
        assert "as trade_only_pnl" in normalized


class TestWeeklySummaryModelHasDividendColumns:
    @pytest.fixture
    def sql(self) -> str:
        path = "dbt/models/marts/mart_weekly_summary.sql"
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def test_dividends_amount_column_present(self, sql):
        normalized = re.sub(r"\s+", " ", sql)
        assert "as dividends_amount" in normalized

    def test_total_return_column_present(self, sql):
        normalized = re.sub(r"\s+", " ", sql)
        assert "as total_return" in normalized

    def test_weekly_dividends_cte_reads_from_mart_daily_pnl(self, sql):
        # Single source of truth for dividend cash flows is mart_daily_pnl
        # (which itself reads stg_history). Don't let a future change pull
        # divs from a different source — that's how parallel derivations
        # drift apart.
        normalized = re.sub(r"\s+", " ", sql)
        assert "weekly_dividends" in normalized
        assert "mart_daily_pnl" in normalized
