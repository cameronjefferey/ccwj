"""Scope continuity for Daily Review's time-machine links."""

from datetime import date
from html import unescape
import re
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import app.weekly_review as weekly_review
from app import app


TENANT = "snaptrade:tenant-a"
TENANTS = "snaptrade:tenant-a,snaptrade:tenant-b"


def _queries_for_path(rendered, path):
    hrefs = [unescape(href) for href in re.findall(r'href="([^"]+)"', rendered)]
    return [
        parse_qs(urlparse(href).query)
        for href in hrefs
        if urlparse(href).path == path
    ]


def test_heatmap_day_link_preserves_tenant_scope():
    today = date(2024, 1, 3)
    context = {
        "current_user": SimpleNamespace(is_authenticated=False),
        "title": "Overview",
        "mode": "daily",
        "week_start": date(2024, 1, 1),
        "week_end": date(2024, 1, 5),
        "user_timezone": "UTC",
        "today": today,
        "review_date": today,
        "review_is_today": False,
        "accounts": [],
        "selected_account": "",
        "selected_tenant": TENANT,
        "selected_tenants": TENANTS,
        "error": None,
        "equity_snapshot": None,
        "today_snapshots_by_account": [],
        "today_strip": [{"symbol": "X"}],
        "expiring_options": [],
        "upcoming_earnings_this_week": [],
        "upcoming_earnings_next_week": [],
        "upcoming_ex_dividends": [],
        "today_movers": None,
        "after_hours_movers": None,
        "today_pulse": None,
        "today_snapshots_total": None,
        "today_headline": None,
        "from_upload": False,
        "market": None,
        "market_session": {"state": "closed"},
        "market_open_today": False,
        "market_neutral_line": None,
        "since_last_looked": None,
        "calendar_grid": weekly_review._build_calendar_grid(
            {date(2024, 1, 2): 100.0}, today, weeks_back=1, default_weeks=1
        ),
        "calendar_weeks_back": 1,
        "calendar_default_weeks": 1,
        "calendar_extra_weeks": 0,
        "daily_calendar_no_query_rows": False,
        "trades_this_week": {
            "trades": [], "count": 0, "opened_count": 0,
            "closed_count": 0, "realized_pnl": 0.0,
            "unrealized_pnl": 0.0, "has_any": False,
        },
        "trades_today": {
            "trades": [{
                "verb": "Bought", "action": "equity_buy", "symbol": "AAPL",
                "trade_symbol": "AAPL", "description": "", "quantity": 100,
                "price": 185.2, "amount": None, "account": "Schwab",
                "is_option": False,
            }],
            "cash": [], "count": 1, "net_cash": -18520.0,
            "net_gl": 0.0,
            "symbols": ["AAPL"], "has_any": True,
        },
        "all_user_tags": [],
        "account_breakdown": {"rows": [], "totals": None, "benchmarks": []},
        "benchmark_snapshot": [],
    }

    with app.test_request_context(
        f"/daily-review?tenant={TENANT}&tenants={TENANTS}"
    ):
        rendered = app.jinja_env.get_template("weekly_review.html").render(**context)

    queries = _queries_for_path(rendered, "/daily-review/day/2024-01-02")
    assert queries
    assert queries[0]["tenant"] == [TENANT]
    assert queries[0]["tenants"] == [TENANTS]
    assert "1 symbol is currently held." in rendered
    assert "of your stories" not in rendered
    assert "Trades — Wed Jan 3" in rendered
    assert "Bought" in rendered
    assert "1 fill Wed" in rendered
    assert "AAPL" in rendered
    assert "Trades Today" not in rendered
    assert "Today's Biggest" not in rendered

    story_queries = _queries_for_path(rendered, "/story")
    assert story_queries
    assert story_queries[0]["tenant"] == [TENANT]
    assert story_queries[0]["tenants"] == [TENANTS]

    today_queries = _queries_for_path(rendered, "/today")
    assert today_queries
    assert today_queries[0]["tenants"] == [TENANTS]

    pos_queries = _queries_for_path(rendered, "/position/AAPL")
    assert pos_queries
    assert pos_queries[0]["tenants"] == [TENANTS]


def test_day_navigation_and_back_link_preserve_tenant_scope():
    with app.test_request_context(
        f"/daily-review/day/2024-01-03?tenant={TENANT}&tenants={TENANTS}"
    ):
        rendered = app.jinja_env.get_template("day_detail.html").render(
            current_user=SimpleNamespace(is_authenticated=False),
            title="Day",
            day=date(2024, 1, 3),
            day_label="Wednesday, January 3, 2024",
            is_weekend=False,
            prev_day=date(2024, 1, 2),
            next_day=date(2024, 1, 4),
            selected_account="",
            selected_tenant=TENANT,
            selected_tenants=TENANTS,
            user_accounts=[],
            account_rows=[],
            total_value=0,
            total_delta=0,
            total_delta_pct=None,
            trade_rows=[],
            cash_rows=[],
            option_rows=[],
            dividend_rows=[],
            market_rows=[],
            has_any=False,
        )

    paths = (
        "/daily-review/day/2024-01-02",
        "/daily-review/day/2024-01-04",
        "/overview",
    )
    for path in paths:
        queries = _queries_for_path(rendered, path)
        assert queries
        assert queries[0]["tenant"] == [TENANT]
        assert queries[0]["tenants"] == [TENANTS]


def test_future_day_redirect_preserves_tenant_scope():
    with app.test_request_context(
        f"/daily-review/day/2999-01-01?tenant={TENANT}&tenants={TENANTS}"
    ), patch(
        "app.routes._redirect_if_no_accounts", return_value=None
    ), patch.object(
        weekly_review, "current_user", SimpleNamespace(id=1)
    ), patch.object(
        weekly_review, "get_user_profile", return_value={"timezone": "UTC"}
    ):
        response = weekly_review.day_detail.__wrapped__("2999-01-01")

    query = parse_qs(urlparse(response.location).query)
    assert urlparse(response.location).path == "/today"
    assert query["tenant"] == [TENANT]
    assert query["tenants"] == [TENANTS]
