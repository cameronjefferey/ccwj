"""The private-account health check stays select-only and prints flags only."""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

from scripts.private_vanguard_health import (
    _ACCOUNTS_SQL,
    _HIDDEN_VANGUARD_SQL,
    _WAREHOUSE_SQL,
    connections_ok,
    format_report,
    holdings_is_fresh,
    is_vanguard,
    reconcile_line,
    statement_is_read,
    warehouse_status,
)

ROOT = Path(__file__).resolve().parents[1]
_LINE = re.compile(
    r"^(connections|privacy|positions|overview|settings)=(PASS|FAIL)$"
    r"|^(warehouse)=(PASS|FAIL|off by about [1-9][0-9]*%)$"
)


def test_statements_are_prod_reads():
    for sql in (_HIDDEN_VANGUARD_SQL, _ACCOUNTS_SQL, _WAREHOUSE_SQL):
        assert statement_is_read(sql)
    assert "ccwj-dbt.analytics.stg_history" in _WAREHOUSE_SQL
    assert "ccwj-dbt.analytics.stg_current" in _WAREHOUSE_SQL
    assert "ccwj-dbt.analytics.stg_account_balances" in _WAREHOUSE_SQL
    assert "analytics_dev" not in _WAREHOUSE_SQL
    assert "analytics_raw" not in _WAREHOUSE_SQL
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
    assert "scripts/private_vanguard_health.py" in ci_health
    assert "analytics_dev" not in ci_health
    assert "BQ_DATASET" not in ci_health
