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
    _market_line_source,
    _neutral_market_line,
    _overview_radar,
    _overview_takeaway,
    _snapshot_placeholder_labels,
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

    def test_names_the_trailing_week_not_the_iso_week(self):
        s = _neutral_market_line({"spy_week_pct": 1.2, "qqq_week_pct": 1.5})
        assert "vs 1 week" in s
        assert "this week" not in s

    def test_benchmark_week_overrides_iso_week_zero(self):
        # Monday with one bar: MIN(close) since ISO Monday equals the
        # latest close, so the old hero line was SPY +0.0% · QQQ +0.0%
        # while the snapshot table showed the real trailing week.
        market = {"spy_week_pct": 0.0, "qqq_week_pct": 0.0, "spy_ytd_pct": 8.0}
        bench = [
            {"symbol": "SPY", "label": "S&P 500", "week_pct": 1.8},
            {"symbol": "QQQ", "label": "Nasdaq 100", "week_pct": -0.4},
        ]
        src = _market_line_source(market, bench)
        s = _neutral_market_line(src)
        assert "SPY +1.8%" in s
        assert "QQQ -0.4%" in s
        assert src["spy_ytd_pct"] == 8.0

    def test_falls_back_to_iso_week_when_benchmark_has_no_week(self):
        market = {"spy_week_pct": 0.6, "qqq_week_pct": None}
        src = _market_line_source(market, [{"symbol": "SPY", "week_pct": None}])
        assert _neutral_market_line(src) == _neutral_market_line(market)


class TestOverviewTakeaway:
    def test_labels_balance_delta_and_lists_benchmarks_without_comparing(self):
        benches = [
            {"label": "S&P 500", "day_pct": -0.72, "week_pct": 2.08},
            {"label": "Nasdaq 100", "day_pct": -0.84, "week_pct": 5.29},
        ]
        line = _overview_takeaway(0.13, 6.08, benches)
        assert line == (
            "Account value rose 0.13% since the prior close; "
            "one-week change: account +6.08%, S&P 500 +2.08%, "
            "Nasdaq 100 +5.29%."
        )

    def test_deposit_sized_move_never_claims_market_outperformance(self):
        benches = [
            {"label": "S&P 500", "day_pct": 0.4, "week_pct": 1.2},
            {"label": "Nasdaq 100", "day_pct": 0.2, "week_pct": 0.8},
        ]
        # Snapshot deltas include transfers: a $50k deposit into a $100k
        # account is a +50% balance move, not a +50% investment return.
        line = _overview_takeaway(50.0, 50.0, benches)
        assert line.startswith("Account value rose 50.00%")
        assert "ahead" not in line
        assert "behind" not in line
        assert "outperform" not in line

    def test_missing_indexes_stays_quiet(self):
        assert _overview_takeaway(None, None, []) is None
        assert _overview_takeaway(1.0, None, []) == (
            "Account value rose 1.00% since the prior close."
        )


class TestOverviewRadar:
    def test_places_earnings_and_ex_div_chips(self):
        radar = _overview_radar(
            date(2026, 9, 23),
            [{"symbol": "MU", "earnings_date": "2026-09-24",
              "company": "Micron Technology, Inc.",
              "earnings_date_display": "Thu Sep 24"}],
            [{"symbol": "SMTC", "expiry": "2026-10-02", "quantity": -1,
              "option_type": "Call", "strike": 75, "itm": False,
              "distance": 1.2, "unrealized_pnl": 40}],
            [{"symbol": "JEPI", "projected_date": "2026-10-01",
              "days_until": 8, "est_income": 180.32,
              "shares_held": 92, "last_amount_per_share": 1.96}],
            {"items": [{"symbol": "MU", "short_label": "$1040 call",
                        "expiry": "2026-09-25"}]},
        )
        assert radar["earnings"][0]["title"] == "MU earnings"
        assert "Micron Technology" in radar["earnings"][0]["sub"]
        assert radar["options"][0]["cls"] == "ex"
        assert "short 1 call" in radar["options"][0]["sub"]
        verdict = [e for e in radar["options"] if e["cls"] == "vd"]
        assert verdict and "MU" in verdict[0]["title"]
        assert radar["dividends"][0]["sub"] == "92 sh, $1.96 last"
        assert "est. +$180.32" in radar["dividends"][0]["title"]
        assert "Estimated dividends" in radar["summary"]

    def test_empty_window_is_none(self):
        assert _overview_radar(date(2026, 9, 23), [], [], [], None) is None

    def test_market_today_anchor_keeps_day_14_event_after_weekend(self):
        review_close = date(2026, 9, 18)
        market_today = date(2026, 9, 21)
        option = {
            "symbol": "XYZ",
            "expiry": "2026-10-05",
            "quantity": -1,
            "option_type": "Call",
            "strike": 100,
            "unrealized_pnl": 25,
        }
        # The old settled-close anchor silently dropped this valid day-14
        # event because it sat 17 days after Friday.
        assert _overview_radar(review_close, [], [option], [], None) is None
        radar = _overview_radar(market_today, [], [option], [], None)
        assert radar["options"][0]["title"] == "XYZ $100 call expires"


class TestSnapshotPlaceholderScope:
    def test_filtered_scope_drops_other_accounts(self):
        labels = ["Crypto", "Kids", "Retirement"]
        label_to_tid = {"Crypto": "t-crypto", "Kids": "t-kids", "Retirement": "t-ret"}
        out = _snapshot_placeholder_labels(
            labels, label_to_tid, ["t-crypto"], seen_accounts=set())
        assert out == ["Crypto"]

    def test_seen_snapshot_is_not_repeated(self):
        out = _snapshot_placeholder_labels(
            ["Crypto", "Kids"],
            {"Crypto": "t-crypto", "Kids": "t-kids"},
            ["t-crypto", "t-kids"],
            seen_accounts={"Crypto"},
        )
        assert out == ["Kids"]

    def test_admin_unscoped_keeps_every_label(self):
        out = _snapshot_placeholder_labels(
            ["Crypto", "Kids"], {"Crypto": "t-crypto"}, None, set())
        assert out == ["Crypto", "Kids"]


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
    """Rolling N-week grid (replaces 'current calendar month' which was empty
    on the 1st of every month). The grid renders DAILY_CALENDAR_WEEKS rows
    in the DOM but flags older rows with ``is_extra`` so the template can
    show the latest DAILY_CALENDAR_DEFAULT_WEEKS by default and reveal the
    rest behind a toggle."""

    def test_default_window_renders_full_history(self):
        from app.weekly_review import DAILY_CALENDAR_WEEKS

        grid = _build_calendar_grid({}, today=date(2026, 5, 1))
        assert len(grid) == DAILY_CALENDAR_WEEKS
        for row in grid:
            assert len(row["cells"]) == 5

    def test_extra_rows_are_the_oldest(self):
        from app.weekly_review import (
            DAILY_CALENDAR_WEEKS,
            DAILY_CALENDAR_DEFAULT_WEEKS,
        )

        grid = _build_calendar_grid({}, today=date(2026, 5, 1))
        extra_count = sum(1 for r in grid if r.get("is_extra"))
        assert extra_count == DAILY_CALENDAR_WEEKS - DAILY_CALENDAR_DEFAULT_WEEKS
        # Extra rows are the FIRST ones (oldest); default-visible rows are the
        # last DAILY_CALENDAR_DEFAULT_WEEKS rows (most recent, ending today).
        for r in grid[:extra_count]:
            assert r["is_extra"] is True
        for r in grid[extra_count:]:
            assert r["is_extra"] is False

    def test_default_weeks_arg_is_respected(self):
        # 6 fetched, only 2 visible → 4 extra (oldest).
        grid = _build_calendar_grid(
            {}, today=date(2026, 5, 1), weeks_back=6, default_weeks=2
        )
        assert len(grid) == 6
        extras = [r for r in grid if r["is_extra"]]
        visibles = [r for r in grid if not r["is_extra"]]
        assert len(extras) == 4
        assert len(visibles) == 2
        # Extras precede visibles in row order (oldest first).
        assert grid[0]["is_extra"] is True
        assert grid[-1]["is_extra"] is False

    def test_default_weeks_clamped_to_weeks_back(self):
        # If a caller asks for more visible weeks than fetched, no row is
        # marked extra (everything visible).
        grid = _build_calendar_grid(
            {}, today=date(2026, 5, 1), weeks_back=4, default_weeks=99
        )
        assert all(r["is_extra"] is False for r in grid)

    def test_window_is_configurable(self):
        grid = _build_calendar_grid({}, today=date(2026, 5, 1), weeks_back=4)
        assert len(grid) == 4
        grid26 = _build_calendar_grid({}, today=date(2026, 5, 1), weeks_back=26)
        assert len(grid26) == 26

    def test_marks_today_only_once(self):
        grid = _build_calendar_grid({}, today=date(2026, 5, 1))
        today_cells = [c for r in grid for c in r["cells"] if c["is_today"]]
        assert len(today_cells) == 1
        assert today_cells[0]["date"] == date(2026, 5, 1)

    def test_marks_future_cells(self):
        # If today is Wed, Thu and Fri of this row should be is_future.
        grid = _build_calendar_grid({}, today=date(2026, 4, 29), weeks_back=4)
        last_row = grid[-1]
        assert last_row["cells"][2]["is_today"] is True       # Wed
        assert last_row["cells"][3]["is_future"] is True      # Thu
        assert last_row["cells"][4]["is_future"] is True      # Fri

    def test_propagates_daily_change(self):
        grid = _build_calendar_grid(
            {date(2026, 4, 14): 123.0}, today=date(2026, 5, 1),
        )
        match = [c for r in grid for c in r["cells"] if c["date"] == date(2026, 4, 14)]
        assert len(match) == 1
        assert match[0]["daily_change"] == 123.0
        assert match[0]["has_data"] is True

    def test_grid_extends_far_enough_back_for_dividend_only_days(self):
        """Regression: with the new closed-trade + dividends data source, days
        with only a dividend payout (no trade closes) should still fill cells
        as far back as the rolling window allows."""
        today = date(2026, 5, 7)
        grid = _build_calendar_grid({date(2026, 2, 16): 412.5}, today=today)
        match = [c for r in grid for c in r["cells"] if c["date"] == date(2026, 2, 16)]
        assert len(match) == 1, "Feb 16 should be inside the 12-week window"
        assert match[0]["daily_change"] == 412.5
