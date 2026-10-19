"""The Empower raw summary is select-only and is not executed here."""

from __future__ import annotations

from pathlib import Path

from scripts.empower_raw_activity_summary import (
    _assert_select_only,
    activity_sql,
    instrument_sql,
    is_safe_action_label,
    presence_sql,
)

ROOT = Path(__file__).resolve().parents[1]


def test_summary_sql_counts_types_and_signs_without_writing():
    statements = [activity_sql(), instrument_sql(), presence_sql()]
    for sql in statements:
        _assert_select_only(sql)
        assert "INSERT" not in sql.upper()
        assert "{{" not in sql
        assert "ccwj-dbt.analytics_raw." in sql
        assert "ccwj-dbt.analytics." not in sql
        assert "analytics_backups" not in sql
    activity = activity_sql()
    assert "GROUP BY action" in activity
    assert "COUNTIF(amount > 0) AS pos" in activity
    assert "COUNTIF(amount < 0) AS neg" in activity
    assert r"\d{6}[CP]\d{8}" in activity
    assert "SUM(" not in activity
    instrument = instrument_sql()
    assert "GROUP BY security_type" in instrument
    assert "Symbol" not in instrument


def test_action_labels_reject_tickers_and_amounts():
    assert is_safe_action_label("Cash Dividend")
    assert is_safe_action_label("Buy to Open")
    assert not is_safe_action_label("FUNDX")
    assert not is_safe_action_label("$40.00")


def test_dev_mirror_runs_the_summary_later_and_does_not_print_dollars():
    text = (ROOT / ".github/workflows/dev_mirror.yml").read_text()
    assert "empower_raw_activity_summary.py" in text
    assert "EMPOWER_PRINT_TOTALS" not in text
