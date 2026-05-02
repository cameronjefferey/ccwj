"""Unit tests for app/weekly_review.py pure helpers.

These cover small, deterministic utilities that don't require a live
BigQuery / Flask context. We pin behavior here instead of via end-to-end
rendering so a future refactor that re-introduces the call/put inversion
bug fails CI loudly.
"""
from datetime import date

import pytest

from app.weekly_review import (
    _build_behavior_sentence,
    _build_calendar_grid,
    _build_week_diary,
    _classify_expiring_moneyness,
    _neutral_market_line,
)


class TestClassifyExpiringMoneyness:
    """`_classify_expiring_moneyness` powers the ITM / ATM / OTM badge in
    the Weekly Review's "Expiring Soon" card. The previous implementation
    compared `option_type` (which is OSI single-char, "C"/"P") against
    "Call" — a comparison that's always False, so every contract was
    silently routed through the put branch. Calls were displayed with
    inverted ITM/OTM. These tests pin the corrected semantics.
    """

    def test_call_above_strike_is_itm(self):
        # AAPL $150 call, stock @ $160 → call is ITM by $10.
        itm, distance = _classify_expiring_moneyness(
            instrument_type="Call", option_type="C",
            stock_price=160.00, strike=150.00,
        )
        assert itm is True
        assert distance == 10.00

    def test_call_below_strike_is_otm_pltr_regression(self):
        # The exact PLTR 141 Call w/ stock @ $137.92 case the user
        # flagged. Old code said ITM $3.08; should be OTM $3.08.
        itm, distance = _classify_expiring_moneyness(
            instrument_type="Call", option_type="C",
            stock_price=137.92, strike=141.00,
        )
        assert itm is False, "Call below strike must be OTM (PLTR regression)"
        assert distance == -3.08

    def test_put_below_strike_is_itm(self):
        # SPY $500 put, stock @ $480 → put is ITM by $20.
        itm, distance = _classify_expiring_moneyness(
            instrument_type="Put", option_type="P",
            stock_price=480.00, strike=500.00,
        )
        assert itm is True
        assert distance == 20.00

    def test_put_above_strike_is_otm(self):
        # SPY $500 put, stock @ $510 → put is OTM by $10.
        itm, distance = _classify_expiring_moneyness(
            instrument_type="Put", option_type="P",
            stock_price=510.00, strike=500.00,
        )
        assert itm is False
        assert distance == -10.00

    def test_falls_back_to_osi_when_instrument_type_blank(self):
        # Some upstream rows might only carry the OSI char.
        itm, _ = _classify_expiring_moneyness(
            instrument_type=None, option_type="C",
            stock_price=160.00, strike=150.00,
        )
        assert itm is True

    def test_uses_instrument_type_when_osi_missing(self):
        itm, _ = _classify_expiring_moneyness(
            instrument_type="Put", option_type="",
            stock_price=480.00, strike=500.00,
        )
        assert itm is True

    def test_handles_full_call_string_in_osi_position(self):
        # Defensive: if a refactor ever swaps in "Call"/"Put" into the
        # option_type column, the .startswith("C")/("P") check still wins.
        itm, _ = _classify_expiring_moneyness(
            instrument_type="", option_type="Call",
            stock_price=160.00, strike=150.00,
        )
        assert itm is True

    @pytest.mark.parametrize("sp,k", [(0, 100), (100, 0), (None, 100), (100, None), (-1, 100)])
    def test_returns_none_when_inputs_invalid(self, sp, k):
        itm, distance = _classify_expiring_moneyness(
            instrument_type="Call", option_type="C",
            stock_price=sp, strike=k,
        )
        assert itm is None
        assert distance is None

    def test_unknown_option_side_returns_none(self):
        itm, distance = _classify_expiring_moneyness(
            instrument_type="Equity", option_type="",
            stock_price=100.00, strike=100.00,
        )
        assert itm is None
        assert distance is None


class TestBuildBehaviorSentence:
    """Hero behavior sentence — process-first 1-liner that anchors the page.
    These tests pin the major decision branches so we don't accidentally
    regress to a P&L-first headline (which the product manifesto explicitly
    rejects: 'process is the signal').
    """

    def test_midweek_no_closes_yet(self):
        s = _build_behavior_sentence(
            review={"trades_closed": 0}, behavior_mirror=None, mode="midweek",
        )
        assert "Mid-week" in s
        assert "still being written" in s

    def test_monday_clean_slate(self):
        s = _build_behavior_sentence(
            review={"trades_closed": 0}, behavior_mirror=None, mode="monday",
        )
        assert "clean slate" in s.lower()

    def test_friday_no_closes(self):
        s = _build_behavior_sentence(
            review={"trades_closed": 0}, behavior_mirror=None, mode="friday",
        )
        # Process-first wording: not "no money made" — "still in play."
        assert "still in play" in s

    def test_active_week_flagged(self):
        # 5 closes vs 2/week baseline → ratio 2.5x ≥ 1.6.
        bm = {
            "has_baseline": True,
            "volume": {"value": 5, "baseline": 2.0},
            "win_rate": {"value": 80, "baseline": 70, "diff": 10},
            "pnl": {"value": 500, "baseline": 200, "diff": 300},
        }
        s = _build_behavior_sentence(
            review={"trades_closed": 5}, behavior_mirror=bm, mode="friday",
        )
        assert "More active" in s
        assert "5 closes" in s

    def test_quiet_week_flagged(self):
        bm = {
            "has_baseline": True,
            "volume": {"value": 1, "baseline": 4.0},   # 0.25 ratio
            "win_rate": {"value": 100, "baseline": 70, "diff": 30},
            "pnl": {"value": 100, "baseline": 200, "diff": -100},
        }
        s = _build_behavior_sentence(
            review={"trades_closed": 1}, behavior_mirror=bm, mode="friday",
        )
        assert "selective" in s.lower()

    def test_consistent_week_default(self):
        # Volume in line, win rate in line — should default to "you traded
        # like you usually do."
        bm = {
            "has_baseline": True,
            "volume": {"value": 3, "baseline": 3.0},
            "win_rate": {"value": 70, "baseline": 70, "diff": 0},
            "pnl": {"value": 200, "baseline": 200, "diff": 0},
        }
        s = _build_behavior_sentence(
            review={"trades_closed": 3}, behavior_mirror=bm, mode="friday",
        )
        assert "like you usually do" in s

    def test_no_baseline_falls_back_to_count(self):
        s = _build_behavior_sentence(
            review={"trades_closed": 4}, behavior_mirror={"has_baseline": False}, mode="friday",
        )
        assert "baseline" in s.lower()


class TestNeutralMarketLine:
    """Market context replaces 'Outperforming/Trailing both indexes' badge.
    Per AGENTS.md: 'the market is framing, not scoring.'
    """

    def test_returns_none_when_no_market(self):
        assert _neutral_market_line(None) is None
        assert _neutral_market_line({}) is None

    def test_includes_both_indexes(self):
        s = _neutral_market_line({"spy_week_pct": 1.2, "qqq_week_pct": 1.5})
        assert "SPY +1.2%" in s
        assert "QQQ +1.5%" in s
        # Crucially: no judgment words.
        assert "outperform" not in s.lower()
        assert "trail" not in s.lower()
        assert "beating" not in s.lower()

    def test_handles_negative(self):
        s = _neutral_market_line({"spy_week_pct": -0.8, "qqq_week_pct": None})
        assert "-0.8%" in s
        assert "QQQ" not in s


class TestWeekDiary:
    """Mon→Fri timeline of activity. Centerpiece of the redesigned
    Weekly Review."""

    def _trades(self, **kw):
        # Helper to build a trade with sensible defaults.
        return {
            "symbol": kw.get("symbol", "PLTR"),
            "strategy": kw.get("strategy", "Covered Call"),
            "open_date": kw.get("open_date", ""),
            "close_date": kw.get("close_date", ""),
            "current_pnl": kw.get("current_pnl", None),
            "status": kw.get("status", "Closed"),
        }

    def test_returns_five_weekday_rows(self):
        diary = _build_week_diary(
            week_start=date(2026, 4, 27),  # Monday
            today=date(2026, 5, 1),         # Friday
            trades=[],
            daily_changes={},
            expiring_options=[],
        )
        assert len(diary) == 5
        assert [d["label"] for d in diary] == ["Mon", "Tue", "Wed", "Thu", "Fri"]

    def test_marks_today(self):
        diary = _build_week_diary(
            week_start=date(2026, 4, 27),
            today=date(2026, 4, 29),  # Wednesday
            trades=[],
            daily_changes={},
            expiring_options=[],
        )
        assert diary[2]["is_today"] is True
        assert diary[2]["is_future"] is False
        assert diary[3]["is_future"] is True
        assert diary[4]["is_future"] is True

    def test_summarizes_a_close(self):
        diary = _build_week_diary(
            week_start=date(2026, 4, 27),
            today=date(2026, 5, 1),
            trades=[self._trades(
                symbol="PLTR", strategy="PMCC",
                close_date="2026-04-29", current_pnl=398,
            )],
            daily_changes={},
            expiring_options=[],
        )
        wed = diary[2]
        assert "PLTR" in wed["summary"]
        assert "PMCC" in wed["summary"]
        assert wed["num_closes"] == 1

    def test_summarizes_an_open(self):
        diary = _build_week_diary(
            week_start=date(2026, 4, 27),
            today=date(2026, 5, 1),
            trades=[self._trades(
                symbol="COST", strategy="Cash-Secured Put",
                open_date="2026-04-28",
            )],
            daily_changes={},
            expiring_options=[],
        )
        tue = diary[1]
        assert "COST" in tue["summary"]
        assert "Cash-Secured Put" in tue["summary"]
        assert tue["num_opens"] == 1

    def test_quiet_day_label(self):
        diary = _build_week_diary(
            week_start=date(2026, 4, 27),
            today=date(2026, 5, 1),
            trades=[],
            daily_changes={},
            expiring_options=[],
        )
        # Mon-Thu (non-future) should all be "Quiet day"; Fri (today) is special.
        assert diary[0]["summary"] == "Quiet day."
        assert "Today" in diary[4]["summary"] or "nothing" in diary[4]["summary"].lower()

    def test_attaches_daily_change(self):
        diary = _build_week_diary(
            week_start=date(2026, 4, 27),
            today=date(2026, 5, 1),
            trades=[],
            daily_changes={date(2026, 4, 28): 250.0},
            expiring_options=[],
        )
        assert diary[1]["daily_change"] == 250.0
        assert diary[0]["daily_change"] is None


class TestCalendarGrid:
    """Rolling 4-week grid (replaces 'current calendar month' which was empty
    on the 1st of every month)."""

    def test_returns_four_rows_of_five_cells(self):
        grid = _build_calendar_grid({}, today=date(2026, 5, 1))
        assert len(grid) == 4
        for row in grid:
            assert len(row["cells"]) == 5

    def test_marks_today_only_once(self):
        grid = _build_calendar_grid({}, today=date(2026, 5, 1))
        today_cells = [c for r in grid for c in r["cells"] if c["is_today"]]
        assert len(today_cells) == 1
        assert today_cells[0]["date"] == date(2026, 5, 1)

    def test_marks_future_cells(self):
        # If today is Wed, Thu and Fri of this row should be is_future.
        grid = _build_calendar_grid({}, today=date(2026, 4, 29))
        last_row = grid[-1]
        assert last_row["cells"][2]["is_today"] is True       # Wed
        assert last_row["cells"][3]["is_future"] is True      # Thu
        assert last_row["cells"][4]["is_future"] is True      # Fri

    def test_propagates_daily_change(self):
        grid = _build_calendar_grid(
            {date(2026, 4, 14): 123.0}, today=date(2026, 5, 1),
        )
        # Find the cell for 4/14
        match = [c for r in grid for c in r["cells"] if c["date"] == date(2026, 4, 14)]
        assert len(match) == 1
        assert match[0]["daily_change"] == 123.0
        assert match[0]["has_data"] is True
