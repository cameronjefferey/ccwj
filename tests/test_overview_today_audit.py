"""Overview and Today display totals match the cells a person sees.

Made-up figures only. A header that rounds the exact sum can disagree
with rows that were each already rounded to the nearest dollar. These
tests pin the displayed total to the sum of those displayed cells, the
year on dates outside the anchor year, and a trade-table total that
matches the header even when realized G/L is $0.
"""

from datetime import date
from pathlib import Path

import pandas as pd

from app.money import MINUS
from app.review_format import (
    mover_parts,
    session_date,
    signed_pct,
    signed_whole,
    whole_dollars,
)
from app.weekly_review import (
    _build_account_breakdown,
    _build_calendar_grid,
    _build_trades_this_week,
    _overview_radar,
    _split_day_fills,
    _today_totals,
)

ROOT = Path("app/templates")


def test_session_date_keeps_the_year_outside_the_anchor_year():
    anchor = date(2026, 10, 22)
    assert session_date(date(2026, 10, 22), "long", anchor) == "Thursday, Oct 22"
    assert session_date(date(2025, 12, 29), "dow", date(2026, 1, 5)) == "Mon Dec 29, 2025"
    assert session_date(date(2027, 1, 1), "short", date(2026, 12, 25)) == "Jan 1, 2027"
    assert session_date("not-a-date", "short", anchor) == "not-a-date"


def test_whole_dollars_match_format_rounding_and_drop_a_signed_zero():
    assert whole_dollars(1.5) == 2
    assert whole_dollars(2.5) == 2
    assert whole_dollars(-0.4) == 0
    assert whole_dollars(None) is None
    assert signed_whole(-0.4) == "$0"
    label = signed_whole(-1318)
    assert label.startswith(MINUS)
    assert "$1,318" in label
    assert "$-" not in label
    assert signed_pct(-0.42).startswith(MINUS)
    assert signed_pct(-0.004) == "0.00%"


def test_mover_parts_sum_the_rounded_pieces():
    parts = mover_parts({
        "total_impact": 10.6,
        "options_impact": 10.6,
        "dividends_impact": 10.6,
    })
    assert parts == {"stocks": 11, "options": 11, "dividends": 11, "impact": 33}
    assert mover_parts({
        "total_impact": None,
        "options_impact": None,
        "dividends_impact": None,
    }) is None


def _snap(value, day_delta, has_data=True):
    return {
        "account": "Example",
        "tenant_id": "snaptrade:example",
        "today_value": value,
        "comparisons": {
            "day": {"delta": day_delta, "delta_pct": 1.0, "has_data": has_data},
            "week": {"delta": None, "delta_pct": None, "has_data": False},
            "month": {"delta": None, "delta_pct": None, "has_data": False},
        },
        "vs_week_start": {"delta": None, "delta_pct": None, "has_data": False},
    }


def test_snapshot_total_is_the_sum_of_rounded_account_cells():
    snaps = [_snap(100.6, 1.6), _snap(100.6, 1.6)]
    total = _today_totals(snaps)
    assert [s["display_value"] for s in snaps] == [101, 101]
    assert [s["comparisons"]["day"]["display_delta"] for s in snaps] == [2, 2]
    assert total["today_value"] == 201.2
    assert total["display_value"] == 202
    assert total["comparisons"]["day"]["delta"] == 3.2
    assert total["comparisons"]["day"]["display_delta"] == 4

    dust = [_snap(100.0, -0.4), _snap(100.0, 1.6)]
    dust_total = _today_totals(dust)
    assert dust[0]["comparisons"]["day"]["display_delta"] == 0
    assert dust_total["comparisons"]["day"]["display_delta"] == 2

    quiet = [_snap(100.0, 50.0, has_data=False), _snap(100.0, 1.6)]
    quiet_total = _today_totals(quiet)
    assert quiet_total["comparisons"]["day"]["display_delta"] == 2
    assert quiet_total["comparisons"]["day"]["delta"] == 1.6

    alone = [_snap(100.6, 1.6)]
    assert _today_totals(alone) is None
    assert alone[0]["display_value"] == 101
    assert alone[0]["comparisons"]["day"]["display_delta"] == 2


def _attribution(**kw):
    base = {
        "tenant_id": "snaptrade:north",
        "account": "North",
        "user_id": 1,
        "symbol": "EX",
        "equity_pnl": 0.6,
        "option_pnl": 0.6,
        "dividend_income": 0.6,
        "net_pnl": 1.8,
        "equity_capital": 0.0,
        "option_capital_paid": 0.0,
        "option_premium_collected": 0.0,
        "current_equity_cost": 0.0,
        "num_open_groups": 1,
        "num_equity_legs": 1,
        "num_option_legs": 0,
        "first_open_date": date(2026, 1, 2),
        "last_activity_date": date(2026, 6, 1),
    }
    base.update(kw)
    return base


def test_performance_total_is_the_sum_of_rounded_stock_options_dividends():
    assert _build_account_breakdown(None) == {"rows": [], "totals": None}
    one = _build_account_breakdown(pd.DataFrame([_attribution()]))
    row = one["rows"][0]
    assert (row["equity_display"], row["option_display"], row["dividend_display"]) == (1, 1, 1)
    assert row["net_display"] == 3
    assert row["net_pnl"] == 1.8

    two = _build_account_breakdown(pd.DataFrame([
        _attribution(),
        _attribution(tenant_id="snaptrade:south", account="South", symbol="EX2"),
    ]))
    totals = two["totals"]
    assert totals["equity_display"] == 2
    assert totals["option_display"] == 2
    assert totals["dividend_display"] == 2
    assert totals["net_display"] == 6
    assert totals["net_pnl"] == 3.6
    assert totals["net_display"] != whole_dollars(totals["net_pnl"])


def test_trades_this_week_header_matches_the_sum_of_row_displays():
    week_start = date(2026, 6, 8)
    week_end = date(2026, 6, 14)
    empty = _build_trades_this_week(None, week_start, week_end)
    assert empty["realized_display"] == 0
    assert empty["result_display"] == 0

    def row(**kw):
        base = {
            "tenant_id": "snaptrade:north",
            "account": "North",
            "symbol": "EX",
            "trade_symbol": "EX    260619C00050000",
            "strategy": "Covered Call",
            "status": "Closed",
            "open_date": date(2025, 12, 15),
            "close_date": date(2026, 6, 10),
            "total_pnl": 10.6,
            "trade_cost": 10.6,
            "num_trades": 1,
            "current_unrealized_pnl": 0.0,
            "current_market_value": 0.0,
        }
        base.update(kw)
        return base

    mixed = _build_trades_this_week(pd.DataFrame([
        row(status="Closed", total_pnl=10.6, close_date=date(2026, 6, 10)),
        row(
            trade_symbol="EX    260626C00055000",
            status="Open",
            open_date=date(2026, 6, 9),
            close_date=None,
            total_pnl=0.0,
            current_unrealized_pnl=10.6,
        ),
    ]), week_start, week_end)
    trade = mixed["trades"][0]
    assert trade["realized_pnl"] == 10.6
    assert trade["unrealized_pnl"] == 10.6
    assert trade["result_pnl"] == 21.2
    assert trade["result_display"] == 22
    assert mixed["result_display"] == 22
    assert mixed["realized_display"] + mixed["unrealized_display"] == mixed["result_display"]


def _fill(**kw):
    base = {
        "tenant_id": "snaptrade:north",
        "account": "North",
        "user_id": 1,
        "trade_date": date(2026, 6, 10),
        "action": "equity_buy",
        "trade_symbol": "EX",
        "underlying_symbol": "EX",
        "description": "Example fill",
        "quantity": 1.0,
        "price": 10.0,
        "amount": -10.0,
        "instrument_type": "Equity",
        "realized_pnl": None,
    }
    base.update(kw)
    return base


def test_session_fills_keep_a_zero_realized_total():
    opened = _split_day_fills(pd.DataFrame([_fill()]))
    assert opened["has_gl"] is False
    assert opened["net_gl"] == 0.0

    flat = _split_day_fills(pd.DataFrame([
        _fill(action="equity_sell", amount=10.25, realized_pnl=10.25),
        _fill(action="equity_sell", amount=-10.25, realized_pnl=-10.25),
    ]))
    assert flat["has_gl"] is True
    assert flat["net_gl"] == 0.0

    zero = _split_day_fills(pd.DataFrame([
        _fill(action="equity_sell", amount=0.0, realized_pnl=0.0),
    ]))
    assert zero["has_gl"] is True
    assert zero["net_gl"] == 0.0


def test_calendar_and_radar_name_a_year_that_is_not_the_anchor_year():
    grid = _build_calendar_grid({}, date(2026, 1, 5), weeks_back=3, default_weeks=1)
    labels = [row["week_label"] for row in grid]
    assert any(label.endswith("2025") for label in labels)
    assert "2026" not in labels[-1]

    radar = _overview_radar(
        date(2026, 12, 25),
        [{"earnings_date": "2027-01-01", "symbol": "EX", "company": "Example Co"}],
        [],
        [],
        [],
    )
    nums = [day["num"] for day in radar["days"]]
    assert "25" in nums
    assert "Jan 1, 2027" in nums


def test_templates_show_matching_totals_and_keep_owner_layout():
    overview = (ROOT / "weekly_review.html").read_text()
    today = (ROOT / "today.html").read_text()
    day = (ROOT / "day_detail.html").read_text()
    below = (ROOT / "_overview_below.html").read_text()
    perf = (ROOT / "_overview_performance.html").read_text()
    styles = (ROOT / "_review_styles.html").read_text()
    base = (ROOT / "base.html").read_text()
    after = (ROOT / "_after_hours_movers.html").read_text()

    for src in (overview, today, day):
        assert 'class="tt-total"' in src or "tr.tt-total" in src
        assert "has_gl or" in src
        assert "money(" in src
    assert "today_movers.combined_impact is not none" in overview
    assert "mover_parts" in overview and "mover_parts" in today
    assert "session_date('dow', today)" in perf
    assert "week_start.strftime('%a %-d')" not in perf
    assert '<details class="disclosure" id="ov-performance" open>' in perf
    assert 'id="ht-overview-performance"' in overview
    assert overview.index('id="ht-overview-performance"') < overview.index('id="ov-happened"')
    assert "totals-row" in below
    assert "week_bits.result|signed_whole" in below
    assert "r.open_date|session_date('short', today)" in below
    assert "signed_whole" in after
    assert "@media (max-width: 480px)" in styles
    assert "@media (min-width: 900px)" in styles
    assert "grid-template-columns: max-content repeat(5, minmax(0, 1fr))" in styles

    privacy = base.index("Hide account names on screen")
    admin = base.rfind("{% if is_admin_user %}", 0, privacy)
    close = base.find("{% endif %}", privacy)
    assert admin != -1 and close != -1 and admin < privacy < close
    assert base.count("set_privacy_mode_route") == 1


def test_rendered_account_tables_match_the_header():
    from app import app

    snaps = [
        _snap(100.6, 1.6),
        _snap(100.6, 1.6),
    ]
    snaps[0]["account"] = "North"
    snaps[0]["tenant_id"] = "snaptrade:north"
    snaps[1]["account"] = "South"
    snaps[1]["tenant_id"] = "snaptrade:south"
    total = _today_totals(snaps)
    breakdown = _build_account_breakdown(pd.DataFrame([
        _attribution(equity_pnl=10.6, option_pnl=10.6, dividend_income=10.6, net_pnl=31.8),
        _attribution(
            tenant_id="snaptrade:south",
            account="South",
            symbol="EX2",
            equity_pnl=0.6,
            option_pnl=0.4,
            dividend_income=-0.4,
            net_pnl=0.6,
        ),
    ]))
    assert breakdown["totals"]["net_display"] == 34

    with app.test_request_context("/overview"):
        perf_html = app.jinja_env.get_template("_overview_performance.html").render(
            simple_view=False,
            week_start=date(2025, 12, 29),
            today=date(2026, 1, 5),
            account_breakdown=breakdown,
        )
        filters = app.jinja_env.from_string(
            "{{ d|session_date('dow', anchor) }}|{{ (m|mover_parts).impact }}|{{ n|signed_whole }}"
        ).render(
            d=date(2025, 12, 29),
            anchor=date(2026, 1, 5),
            m={"total_impact": 10.6, "options_impact": 10.6, "dividends_impact": 10.6},
            n=-0.4,
        )
        page = app.jinja_env.get_template("weekly_review.html").render(
            current_user=__import__("types").SimpleNamespace(is_authenticated=False),
            title="Overview",
            simple_view=False,
            scope_account_choices=[],
            account_groups=[],
            week_start=date(2025, 12, 29),
            week_end=date(2026, 1, 2),
            user_timezone="UTC",
            today=date(2026, 1, 5),
            review_date=date(2025, 12, 31),
            review_is_today=False,
            accounts=[],
            selected_account="",
            selected_tenant=None,
            selected_tenants=None,
            error=None,
            equity_snapshot=None,
            today_snapshots_by_account=snaps,
            today_snapshots_total=total,
            hero_account_value=total["today_value"],
            hero_display_value=total["display_value"],
            today_strip=[],
            expiring_options=[],
            upcoming_earnings_this_week=[],
            upcoming_earnings_next_week=[],
            upcoming_ex_dividends=[],
            today_movers={
                "winners": [],
                "losers": [],
                "options": [],
                "dividends": [{"symbol": "EX", "amount": -5.0}],
                "combined_impact": 26.8,
                "total_impact": 10.6,
                "options_impact": 10.6,
                "dividends_impact": 5.6,
                "as_of_label": "Wed Dec 31, 2025",
            },
            after_hours_movers=None,
            today_pulse=None,
            today_headline=None,
            from_upload=False,
            market=None,
            market_session={"state": "weekend", "label": "Weekend"},
            market_open_today=False,
            market_neutral_line=None,
            since_last_looked=None,
            calendar_grid=[],
            calendar_weeks_back=1,
            calendar_default_weeks=1,
            calendar_extra_weeks=0,
            daily_calendar_no_query_rows=True,
            trades_this_week={
                "trades": [{
                    "symbol": "EX",
                    "tenant_id": "snaptrade:north",
                    "account_display": "North",
                    "strategy": "Covered Call",
                    "contract": "1 contract",
                    "status": "Closed",
                    "is_closed": True,
                    "open_date": date(2025, 12, 15),
                    "close_date": date(2025, 12, 31),
                    "realized_pnl": 10.6,
                    "unrealized_pnl": 10.6,
                    "result_pnl": 21.2,
                    "realized_display": 11,
                    "unrealized_display": 11,
                    "result_display": 22,
                    "result_kind": "net",
                    "tags": [],
                    "traded_today": False,
                }],
                "count": 1,
                "opened_count": 1,
                "closed_count": 0,
                "realized_pnl": 10.6,
                "unrealized_pnl": 10.6,
                "realized_display": 11,
                "unrealized_display": 11,
                "result_display": 22,
                "has_any": True,
            },
            trades_week_start=date(2025, 12, 29),
            trades_week_is_prior=False,
            trades_today={
                "trades": [{
                    "verb": "Sold",
                    "verb_short": "Sold",
                    "action": "equity_sell",
                    "symbol": "EX",
                    "is_roll": False,
                    "is_spread": False,
                    "is_option": False,
                    "quantity": 1,
                    "price": 10,
                    "amount": 0.0,
                    "tenant_id": "snaptrade:north",
                    "account": "North",
                    "tags": [],
                    "leg_open_date": None,
                }],
                "count": 1,
                "fill_count": 1,
                "net_gl": 0.0,
                "has_gl": True,
                "symbols": ["EX"],
                "has_any": True,
            },
            all_user_tags=[],
            account_breakdown=breakdown,
            benchmark_snapshot=[],
            overview_below_deferred=False,
            overview_below_url="/overview/below",
            overview_performance_in_fragment=False,
            overview_daily_in_fragment=False,
            building_history=None,
            overview_prior_date=date(2025, 12, 30),
        )

    assert filters == f"Mon Dec 29, 2025|33|$0"
    assert "Mon Dec 29, 2025" in perf_html
    total_row = perf_html[perf_html.index("All accounts"):]
    assert "+$12" in total_row
    assert "+$34" in total_row
    assert "+$32" not in total_row

    assert page.count("$101") >= 2
    assert "$202" in page
    assert "$201" not in page
    assert "+$4" in page
    assert "Wednesday, Dec 31, 2025 close" in page
    assert "Close vs Tuesday, Dec 30, 2025" in page
    assert "$0.00 realized" in page
    assert "Total realized" in page
    assert page.count("$0.00") >= 2
    assert "Dec 15, 2025" in page
    assert "Dec 31, 2025" in page
    assert "+$22" in page
    div_at = page.index("dividends")
    assert "+$6" in page[div_at:div_at + 40]
    assert "-$5.00" in page
    for name in (
        "weekly_review.html",
        "today.html",
        "day_detail.html",
        "_overview_below.html",
        "_overview_performance.html",
    ):
        app.jinja_env.get_template(name)
