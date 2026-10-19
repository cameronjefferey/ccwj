"""The Empower raw-shape probe is select-only and does not echo row values."""

from __future__ import annotations

from scripts.empower_shape_probe import (
    _assert_select_only,
    action_sql,
    balance_sql,
    broker_presence_sql,
    current_sql,
    discovery_sql,
    history_sql,
    is_safe_action_label,
    is_safe_slug,
    reconcile_sql,
    security_type_sql,
    totals_sql,
    warehouse_sql,
)


def test_safe_labels_reject_tickers_amounts_and_ids():
    assert is_safe_action_label("Cash Dividend")
    assert is_safe_action_label("Buy to Open")
    assert not is_safe_action_label("FUNDX")
    assert not is_safe_action_label("$40.00")
    assert not is_safe_action_label("snaptrade:efbfc5b5-9256-456b-a759-08e8f9b5e0bd")
    assert is_safe_slug("empower")
    assert is_safe_slug("empower-retirement")
    assert not is_safe_slug("empower ••••1234")
    assert not is_safe_slug("schwab")


def test_probe_sql_is_select_only_and_reads_raw_seeds():
    statements = [
        discovery_sql(),
        history_sql(),
        action_sql(),
        current_sql(),
        security_type_sql(),
        balance_sql(),
        reconcile_sql(),
        totals_sql(),
        broker_presence_sql(),
        warehouse_sql(),
    ]
    for sql in statements:
        _assert_select_only(sql)
        assert "INSERT" not in sql.upper()
        assert "{{" not in sql
        assert "ccwj-dbt.analytics_raw." in sql or "ccwj-dbt.analytics.stg_" in sql
        assert "analytics_backups" not in sql
    assert "ccwj-dbt.analytics_raw.trade_history" in history_sql()
    assert "account_total" in reconcile_sql()
    # Prod warehouse is read for presence flags only. No raw overwrite target.
    assert "ccwj-dbt.analytics.stg_history" in warehouse_sql()
    assert "analytics_raw" not in warehouse_sql()
