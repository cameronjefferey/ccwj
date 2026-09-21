"""Smoke-test display bugs: counts, labels, timestamps, markdown, raw log."""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from app.execution_quality import held_to_expiry_kept
from app.insights import _md_to_html, strategy_display_label
from app.linked_accounts import distinct_broker_names, linked_account_entries
from app.position_detail import (
    attach_open_leg_dates,
    collapse_raw_trade_log,
    unique_open_strategy_names,
)
from app.trader_story import _profile_symbol_count, align_kept_at_expiry
from app.weekly_review import _align_market_week_with_benchmark, _neutral_market_line
from app import friendly_timestamp


def test_broker_count_is_institutions_not_accounts():
    accounts = [
        {"broker_slug": "Schwab"},
        {"broker_slug": "schwab"},
        {"broker_slug": "Coinbase"},
        {"broker_slug": "snaptrade"},
        {"broker_slug": ""},
    ]
    assert distinct_broker_names(accounts) == ["Schwab", "Coinbase"]


def test_linked_accounts_use_tenants_not_stale_labels():
    tenants = [
        {"tenant_id": "snaptrade:a", "broker_slug": "snaptrade",
         "account_name": "Schwab ••••3852", "display_nickname": "Cameron 401k"},
        {"tenant_id": "snaptrade:b", "broker_slug": "snaptrade",
         "account_name": "Schwab Account", "display_nickname": "Emmory"},
        {"tenant_id": "manual:manual:9:Old IRA", "broker_slug": "manual",
         "account_name": "Old IRA"},
    ]
    labels = {
        "snaptrade:a": "Cameron 401k",
        "snaptrade:b": "Emmory",
        "manual:manual:9:Old IRA": "Old IRA",
    }
    rows = linked_account_entries(tenants, labels)
    assert [r["label"] for r in rows] == ["Cameron 401k", "Emmory", "Old IRA"]
    removable = {r["label"]: r["removable"] for r in rows}
    assert removable["Old IRA"] is True
    assert removable["Emmory"] is False
    assert "Schwab ••••3852" not in {r["label"] for r in rows}


def test_market_header_uses_benchmark_week_not_zero_iso_week():
    market = {"spy_week_pct": 0.0, "qqq_week_pct": 0.0, "spy_ytd_pct": 12.0}
    snapshot = [
        {"symbol": "SPY", "week_pct": 1.35},
        {"symbol": "QQQ", "week_pct": 3.77},
    ]
    aligned = _align_market_week_with_benchmark(market, snapshot)
    line = _neutral_market_line(aligned)
    assert "SPY +1.4%" in line
    assert "QQQ +3.8%" in line
    assert "+0.0%" not in line
    assert aligned["spy_ytd_pct"] == 12.0


def test_duplicate_strategy_rows_keep_account():
    counts = {"Covered Call": 2, "Long Call": 1}
    labels = {"snaptrade:a": "Cameron 401k", "snaptrade:b": "Emmory"}
    assert strategy_display_label(
        "Covered Call", "Schwab Account", "snaptrade:a", counts, labels,
    ) == "Covered Call · Cameron 401k"
    assert strategy_display_label(
        "Long Call", "Schwab Account", "snaptrade:a", counts, labels,
    ) == "Long Call"


def test_markdown_hash_heading_and_escaped_list():
    html = str(_md_to_html(
        "# DTE Performance: Your Strongest Windows\n\n"
        "You hit **60%**.\n"
        "- <script>alert(1)</script>\n"
        "## The concentration issue\n"
    ))
    assert "<h2>DTE Performance: Your Strongest Windows</h2>" in html
    assert "<h2>The concentration issue</h2>" in html
    assert "<strong>60%</strong>" in html
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_friendly_timestamp_drops_raw_offset():
    raw = datetime(2026, 9, 20, 22, 36, 46, 361388, tzinfo=timezone.utc)
    text = friendly_timestamp(raw)
    assert text == "Sep 20, 2026, 10:36 PM UTC"
    assert "+00:00" not in text
    assert "361388" not in text
    eastern = friendly_timestamp(raw, "America/New_York")
    assert eastern.startswith("Sep 20, 2026, 6:36 PM")
    assert "+00:00" not in eastern


def test_kept_at_expiry_uses_exit_ledger():
    df = pd.DataFrame([
        {"close_type": "Expired", "direction": "Sold", "realized_pnl": 100.0},
        {"close_type": "ExpiredOTM", "direction": "Sold", "realized_pnl": 15.0},
    ])
    n, kept = held_to_expiry_kept(df)
    assert (n, kept) == (2, 115.0)
    profile = {
        "facts": [{
            "label": "Kept at expiry",
            "value": "$99",
            "tone": "pos",
            "detail": "2 short contracts rode to worthless expiry — you kept every dollar",
        }],
    }
    align_kept_at_expiry(profile, df)
    assert profile["facts"][0]["value"] == "$115"
    assert "2 short contracts" in profile["facts"][0]["detail"]


def test_profile_symbol_count_matches_summary_not_narrated_book():
    summary = pd.DataFrame({"symbol": ["AAPL", "aapl", "MSFT", ""]})
    assert _profile_symbol_count(summary, fallback=1) == 2


def test_open_leg_dates_come_from_the_session():
    positions = [{"tenant_id": "snaptrade:emmory", "leg_num": 1}]
    sessions = [{
        "tenant_id": "snaptrade:emmory",
        "display_leg": 1,
        "status": "Open",
        "open_date": "2024-03-01",
        "last_trade_date": "2026-09-18",
        "days_held": 900,
    }]
    attach_open_leg_dates(positions, sessions)
    assert positions[0]["open_date"] == "2024-03-01"
    assert positions[0]["close_date"] == ""
    assert positions[0]["days_held"] == 900


def test_raw_log_drops_shadow_dividend_and_duplicate_drip():
    rows = [
        {"tenant_id": "t", "trade_date": "2026-08-07", "action": "equity_buy",
         "is_dividend_reinvestment": True, "symbol": "COST", "quantity": 0.0016,
         "amount": -1.47, "description": "COSTCO WHSL CORP NEW"},
        {"tenant_id": "t", "trade_date": "2026-08-07", "action": "equity_buy",
         "is_dividend_reinvestment": True, "symbol": "COST", "quantity": 0.0016,
         "amount": -1.47, "description": "COSTCO WHSL CORP NEW"},
        {"tenant_id": "t", "trade_date": "2026-08-07", "action": "dividend",
         "symbol": "COST", "quantity": 0, "amount": 1.47, "description": "COSTCO DIV"},
        {"tenant_id": "t", "trade_date": "2026-08-07", "action": "other",
         "symbol": "COST", "quantity": 0, "amount": 1.47, "description": "COSTCO WHSL"},
        {"tenant_id": "t", "trade_date": "2026-02-13", "action": "equity_buy",
         "symbol": "COST", "quantity": 1, "amount": -864.61, "description": "BUY"},
    ]
    out = collapse_raw_trade_log(rows)
    actions = [r["action"] for r in out]
    assert actions == ["equity_buy", "dividend", "equity_buy"]
    assert out[0]["is_dividend_reinvestment"] is True


def test_open_strategies_are_unique_and_template_keeps_spacing():
    rows = [{"status": "Open", "strategy": "Buy and Hold"}] * 4
    rows.append({"status": "Closed", "strategy": "Covered Call"})
    assert unique_open_strategy_names(rows) == ["Buy and Hold"]
    html = Path("app/templates/position_detail.html").read_text()
    assert "is <strong>{{ open_strategy_names[0] }}</strong>." in html
    assert "{%- if kpis.num_winners" not in html
    assert "(closed legs:" in html


def test_pages_name_themselves_and_install_is_not_a_dead_link():
    base = Path("app/templates/base.html").read_text()
    assert 'id="ht-install-link"' in base
    assert 'href="#" id="ht-install-link"' not in base
    upload = Path("app/templates/upload.html").read_text()
    assert "#submitBtn:disabled" in upload
    today = Path("app/templates/today.html").read_text()
    assert "Covered-call inventory" in today
