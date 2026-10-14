"""The private-account health check stays select-only and prints flags only."""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

from scripts.private_vanguard_health import (
    _ACCOUNTS_SQL,
    _GAP_LINE,
    _GAP_SQL,
    _HIDDEN_VANGUARD_SQL,
    _WAREHOUSE_LINE,
    _WAREHOUSE_SQL,
    attribute_fractions,
    connections_ok,
    format_report,
    gap_lines,
    holdings_is_fresh,
    is_vanguard,
    reconcile_line,
    statement_is_read,
    warehouse_report_line,
    warehouse_status,
)

ROOT = Path(__file__).resolve().parents[1]
_LINE = re.compile(
    r"^(connections|privacy|positions|overview|settings)=(PASS|FAIL)$"
    r"|^(warehouse)=(PASS|FAIL|off by about [1-9][0-9]*%)$"
)


def test_statements_are_prod_reads():
    for sql in (_HIDDEN_VANGUARD_SQL, _ACCOUNTS_SQL, _WAREHOUSE_SQL, _GAP_SQL):
        assert statement_is_read(sql)
    assert "ccwj-dbt.analytics.stg_history" in _WAREHOUSE_SQL
    assert "ccwj-dbt.analytics.stg_current" in _WAREHOUSE_SQL
    assert "ccwj-dbt.analytics.stg_account_balances" in _WAREHOUSE_SQL
    assert "analytics_dev" not in _WAREHOUSE_SQL
    assert "analytics_raw" not in _WAREHOUSE_SQL
    assert "row_type = 'cash'" in _WAREHOUSE_SQL
    assert "row_type = 'account_total'" in _WAREHOUSE_SQL
    assert "= 'vanguard'" in _WAREHOUSE_SQL
    assert "@tenants" not in _WAREHOUSE_SQL
    assert "admin_hidden_users" not in _WAREHOUSE_SQL
    assert "ccwj-dbt.analytics.stg_broker_vanguard_current" in _GAP_SQL
    assert "analytics_dev" not in _GAP_SQL
    assert "analytics_raw" not in _GAP_SQL
    assert "@tenants" not in _GAP_SQL
    assert statement_is_read("SELECT updated_at FROM users")
    assert statement_is_read("SELECT 1 -- do not update the row")
    assert not statement_is_read("UPDATE snaptrade_accounts SET connection_broken_at = NULL")
    assert not statement_is_read("SELECT 1; SELECT 2")
    assert not statement_is_read("WITH x AS (SELECT 1) INSERT INTO t SELECT 1")


def test_holdings_freshness_matches_banner_rules():
    today = date(2026, 10, 14)
    recent = today - timedelta(days=1)
    old = today - timedelta(days=10)
    assert holdings_is_fresh(last=recent, created=old, today=today, threshold=4)
    assert not holdings_is_fresh(last=old, created=old, today=today, threshold=4)
    assert holdings_is_fresh(
        last=old, created=today, today=today, threshold=4
    )
    assert holdings_is_fresh(
        last=old, created=old, today=today, threshold=4, unavailable=True
    )
    assert not holdings_is_fresh(last=None, created=old, today=today, threshold=4)
    assert holdings_is_fresh(
        last=today + timedelta(days=1), created=old, today=today, threshold=4
    )


def test_connections_require_every_row_clear_and_fresh():
    today = date(2026, 10, 14)
    fresh = {
        "connection_broken_at": None,
        "holdings_last_successful_sync": today - timedelta(days=1),
        "created_at": today - timedelta(days=30),
        "snaptrade_account_id": "a",
    }
    broken = dict(fresh, connection_broken_at=today, snaptrade_account_id="b")
    assert connections_ok([fresh], today=today, threshold=4, live={})
    assert connections_ok(
        [fresh],
        today=today,
        threshold=4,
        live={"a": {"last": None, "unavailable": False}},
    )
    assert not connections_ok([fresh, broken], today=today, threshold=4, live={})
    assert not connections_ok([], today=today, threshold=4, live={})
    assert not connections_ok(
        [fresh],
        today=today,
        threshold=4,
        live={"a": {"last": today - timedelta(days=10), "unavailable": False}},
    )


def test_reconcile_line_has_no_dollars():
    assert reconcile_line(10, 10.4) == "PASS"
    assert reconcile_line(10, 11) == "PASS"
    off = reconcile_line(50, 100)
    assert off == "off by about 50%"
    assert "$" not in off
    assert reconcile_line(None, 10) == "FAIL"
    assert warehouse_status(complete=False, staged=10, live=10) == "FAIL"
    assert warehouse_status(complete=True, staged=10, live=10) == "PASS"
    assert warehouse_report_line(complete=False, staged=10, balance=10) == "FAIL"
    assert warehouse_report_line(complete=True, staged=10, balance=10) == "PASS"
    mismatch = warehouse_report_line(complete=True, staged=50, balance=100)
    assert mismatch == "FAIL off by about 50%"
    assert _WAREHOUSE_LINE.match(mismatch)
    assert "$" not in mismatch


def test_vanguard_match_is_broker_not_a_nickname_fragment():
    assert is_vanguard({"broker_slug": "Vanguard", "account_name": "Retirement"})
    assert is_vanguard({"broker_slug": "Broker", "account_name": "Vanguard Account"})
    assert not is_vanguard({"broker_slug": "Schwab", "account_name": "Schwab Account"})


def test_report_lines_are_flags_only():
    lines = format_report(
        {
            "connections": True,
            "privacy": False,
            "warehouse": "off by about 2%",
            "positions": True,
            "overview": False,
            "settings": True,
        }
    )
    assert lines == [
        "connections=PASS",
        "privacy=FAIL",
        "warehouse=off by about 2%",
        "positions=PASS",
        "overview=FAIL",
        "settings=PASS",
    ]
    for line in lines:
        assert _LINE.match(line)
        assert "$" not in line


def test_run_keeps_flags_set_before_an_error(monkeypatch):
    import scripts.private_vanguard_health as mod

    def fill(report):
        report["privacy"] = True
        report["connections"] = True
        raise RuntimeError("stopped")

    monkeypatch.setattr(mod, "_fill_report", fill)
    report = mod.run()
    assert report["privacy"] is True
    assert report["connections"] is True
    assert report["positions"] is False


def test_secret_error_status_has_no_detail():
    from scripts.private_vanguard_health import _status_from_error

    class NotFound(Exception):
        pass

    class PermissionDenied(Exception):
        pass

    assert _status_from_error(NotFound("projects/x/secrets/name")) == "miss"
    assert _status_from_error(PermissionDenied("no")) == "denied"


def test_select_for_update_is_not_a_read():
    assert not statement_is_read("SELECT id FROM users FOR UPDATE")


def test_workflow_is_dispatch_only():
    text = (ROOT / ".github/workflows/private_vanguard_health.yml").read_text()
    assert "workflow_dispatch:" in text
    assert "pull_request:" not in text
    assert "schedule:" not in text
    assert "\n  push:" not in text
    assert "echo " not in text
    assert "secrets.DBT_KEYFILE_JSON" in text
    assert "analytics_dev" not in text
    mirror = (ROOT / ".github/workflows/dev_mirror.yml").read_text()
    assert "cursor/private-vanguard-health-631a" in mirror
    assert "private-health:" in mirror
    health = mirror.split("private-health:", 1)[1]
    assert "scripts/private_vanguard_health.py" in health
    assert "analytics_dev" not in health
    assert "dbt build" not in health
    assert "dev_clone_prod.py" not in health
    ci = (ROOT / ".github/workflows/ci.yml").read_text()
    assert "github.head_ref == 'cursor/private-vanguard-health-631a'" in ci
    ci_health = ci.split("private-health:", 1)[1]
    assert "scripts/private_vanguard_health.py --gap" in ci_health
    assert "analytics_dev" not in ci_health
    assert "BQ_DATASET" not in ci_health
    assert "DATABASE_URL" not in ci_health
    assert "SNAPTRADE" not in ci_health
    assert "secret-manager" not in ci_health


def _shares(**kwargs):
    base = {name: 0.0 for name in (
        "duplicate_or_missing_account",
        "money_market_both",
        "money_market_neither",
        "options_valuation",
        "fractional",
        "price_date",
    )}
    base.update(kwargs)
    return base


def test_query_hint_drops_quoted_text_and_amounts():
    from scripts.private_vanguard_health import _safe_hint

    assert _safe_hint("Unrecognized name: description at [12:3]") == (
        "Unrecognized name: description at [12:3]"
    )
    assert _safe_hint("Syntax error: Expected \")\" but got \"$\" at [4:8]") == ""
    assert _safe_hint("No matching signature for function foo at [9:1]") == (
        "No matching signature for function foo at [9:1]"
    )


def test_gap_lines_are_category_percents_only():
    exact = attribute_fractions(_shares(money_market_both=1.0), has_gap=True)
    lines = gap_lines(exact)
    assert lines[1] == "money_market_both=100%"
    assert all(_GAP_LINE.match(line) for line in lines)
    assert sum(int(line.split("=")[1][:-1]) for line in lines) == 100
    assert "$" not in " ".join(lines)

    timing = attribute_fractions(_shares(), has_gap=True)
    assert timing["price_date"] == 100
    assert sum(timing.values()) == 100

    stacked = attribute_fractions(
        _shares(money_market_both=0.4, options_valuation=0.6),
        has_gap=True,
    )
    assert stacked["money_market_both"] == 40
    assert stacked["options_valuation"] == 60
    assert stacked["price_date"] == 0

    priced = attribute_fractions(_shares(price_date=1.0), has_gap=True)
    assert priced["price_date"] == 100
    assert priced["money_market_both"] == 0

    quiet = attribute_fractions(_shares(money_market_both=1.0), has_gap=False)
    assert quiet == {name: 0 for name in quiet}
    assert not statement_is_read("SELECT symbol, tenant_id FROM t; DROP TABLE t")
