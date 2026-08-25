"""Tests for the dividends-as-first-class P&L stream.

The product treats dividends like equity and option P&L:
  * total_pnl on positions/strategies includes attributed dividends
  * Buy-and-Hold is reclassified as "Dividend" only when the cash is a
    real yield (≥ 2.5% of invested and ≥ 15% of the P&L story) or
    dividends are ≥ 40% of (|price P&L| + dividends) — never just
    because a losing stock paid a coupon
  * Weekly Review headline P&L (total_return) includes dividend cash flows

These tests pin the contract so a refactor doesn't quietly drop dividends
from the headline numbers (the previous regression that motivated this
change was that a dividend-focused buy-and-hold trader saw $0 P&L because
total_pnl was trade-only).
"""
from __future__ import annotations

import re

import pytest


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
    only when the cash is a real yield (or ≥ 40% of the P&L story).

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
        # Yield path: ≥ 2.5% of invested (GREATEST of fill capital and
        # snapshot cost basis) and ≥ 15% of the P&L story. The old
        # `divs > GREATEST(price_pnl, 0)` rule is gone — that labeled
        # every underwater stock that paid a coupon.
        assert "0.025" in normalized
        assert "0.15" in normalized
        assert "0.40" in normalized
        assert "int_equity_fills" in normalized
        assert "GREATEST(wa.total_pnl, 0)" not in normalized
        assert "greatest(wa.total_pnl, 0)" not in normalized.lower()

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
      (2) reclassify Buy-and-Hold to "Dividend" only on a real yield
          (or ≥ 40% of the P&L story), never on `divs > greatest(pnl, 0)`,
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
        # holder, the strategy is currently 'Buy and Hold', and the
        # cash is a real yield (2.5% of invested + 15% of story) or
        # ≥ 40% of the P&L story. The old greatest(pnl, 0) loser-always-
        # wins rule must not return.
        assert "'Dividend'" in normalized
        assert "wad.strategy = 'Buy and Hold'" in normalized
        assert "0.025" in normalized
        assert "0.15" in normalized
        assert "0.40" in normalized
        assert "int_equity_fills" in normalized
        assert "greatest(wad.total_pnl, 0)" not in normalized.lower()

    def test_total_return_is_alias(self, sql):
        normalized = re.sub(r"\s+", " ", sql)
        # total_return uses the same expression as total_pnl.
        assert "as total_return" in normalized

    def test_trade_only_pnl_preserved_for_breakdown(self, sql):
        # Consumers that need pre-dividend P&L (e.g. waterfall recon) read
        # trade_only_pnl. Make sure it's still surfaced.
        normalized = re.sub(r"\s+", " ", sql)
        assert "as trade_only_pnl" in normalized


def _is_dividend_label(divs: float, price_pnl: float, invested: float) -> bool:
    """Mirror of the positions_summary Buy-and-Hold → Dividend CASE.

    Kept in Python so the 2026-08-25 screenshot rows (UFO / VRT / QTUM /
    MSFT vs SCHD / JEPI) stay pinned without a warehouse round-trip.
    """
    if divs <= 0:
        return False
    story = abs(price_pnl) + divs
    share = divs / story if story else 0.0
    if share >= 0.40:
        return True
    if invested >= 200 and (divs / invested) >= 0.025 and share >= 0.15:
        return True
    return False


class TestDividendYieldThreshold:
    """Incidental coupons on buy-and-hold names stay Buy and Hold."""

    def test_screenshot_losers_are_not_dividend(self):
        # Sara 401k UFO / VRT / QTUM and Cameron 401k MSFT from the
        # 2026-08-25 /positions?strategy=Dividend screenshot.
        assert not _is_dividend_label(17.45, -6571.50, 19968)
        assert not _is_dividend_label(3.13, -2223.25, 15086)
        assert not _is_dividend_label(59.66, -2107.14, 34896)
        assert not _is_dividend_label(15.80, -310.42, 4491)

    def test_real_yield_names_stay_dividend(self):
        assert _is_dividend_label(118.38, 78.43, 4871)       # SCHD (story ≥ 40%)
        assert _is_dividend_label(68049.70, -9653.04, 439075)  # JEPI
        assert _is_dividend_label(23250.92, 7258.95, 387262)   # JEPQ

    def test_spy_coupon_is_not_a_dividend_strategy(self):
        # VOO-class: 2.3% lifetime yield, price did the work.
        assert not _is_dividend_label(1143.91, 14729.13, 49711)


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
