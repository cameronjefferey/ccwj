"""The dev-dataset Vanguard check stays select-only and redacts row values."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.vanguard_dev_structural_check import (
    _assert_select_only,
    _structural_sql,
    allowed_actions,
    assert_debug_schema,
    assert_dev_profile,
    is_safe_action_label,
    result_exit,
    scrub_dbt_line,
)

ROOT = Path(__file__).resolve().parents[1]


def test_dev_profile_must_be_analytics_dev():
    assert_dev_profile("ccwj:\n  outputs:\n    dev:\n      dataset: analytics_dev\n")
    with pytest.raises(SystemExit):
        assert_dev_profile("dataset: analytics\n")
    with pytest.raises(SystemExit):
        assert_dev_profile("dataset: analytics_dev\ndataset: analytics\n")


def test_debug_log_refuses_prod_schema():
    assert_debug_schema("Connection:\n  schema: analytics_dev\n  location: us\n")
    stamped = "\x1b[0m03:53:44  Connection:\n\x1b[0m03:53:44    schema: analytics_dev\n"
    assert_debug_schema(stamped)
    with pytest.raises(SystemExit):
        assert_debug_schema("  schema: analytics\n")
    with pytest.raises(SystemExit):
        assert_debug_schema("  schema: analytics_dev\n  schema: analytics_raw\n")


def test_action_labels_reject_tickers_and_amounts():
    assert is_safe_action_label("Cash Dividend")
    assert is_safe_action_label("Buy to Open")
    assert not is_safe_action_label("FUNDX")
    assert not is_safe_action_label("SPXW 10/01/2026 7650.00 C")
    assert not is_safe_action_label("$40.00")


def test_scrub_keeps_dbt_summary_and_drops_values():
    assert scrub_dbt_line("Done. PASS=10 WARN=0 ERROR=0 SKIP=0")
    assert scrub_dbt_line("Failure in test accepted_values_Action")
    assert scrub_dbt_line("Got 2 results, configured to fail if != 0")
    assert scrub_dbt_line("account_value $287,353.26") is None
    assert (
        scrub_dbt_line(
            "Failure in test snaptrade:efbfc5b5-9256-456b-a759-08e8f9b5e0bd"
        )
        is None
    )
    assert scrub_dbt_line("select symbol from trade_history") is None


def test_allow_list_reads_the_vanguard_schema_yml():
    labels = allowed_actions(
        (ROOT / "dbt/models/staging/brokers/_stg_broker_vanguard.yml").read_text()
    )
    assert "Buy" in labels
    assert "Cash Dividend" in labels
    assert "ADR Mgmt Fee" in labels
    assert "Journal" in labels
    assert all(is_safe_action_label(label) for label in labels)


def test_structural_sql_is_select_only_against_dev_and_raw_reads():
    sql = _structural_sql()
    _assert_select_only(sql)
    assert "stg_broker_vanguard_history" in sql
    assert "ccwj-dbt.analytics_dev." in sql
    assert "ccwj-dbt.analytics_raw.account_balances" in sql
    assert "ccwj-dbt.analytics." not in sql
    assert "INSERT" not in sql.upper()
    assert "'absent'" not in sql


def test_fidelity_structural_sql_is_absent_when_raw_is_empty():
    sql = _structural_sql("fidelity")
    _assert_select_only(sql)
    assert "stg_broker_fidelity_history" in sql
    assert "stg_broker_vanguard_history" not in sql
    assert "ccwj-dbt.analytics_raw.trade_history" in sql
    assert "ccwj-dbt.analytics_raw.current_positions" in sql
    assert "'absent'" in sql
    assert "cash and money market" in sql
    assert "ccwj-dbt.analytics." not in sql
    assert "INSERT" not in sql.upper()


def test_fidelity_allow_list_stops_before_balance_row_types():
    labels = allowed_actions(
        (ROOT / "dbt/models/staging/brokers/_stg_broker_fidelity.yml").read_text(),
        broker="fidelity",
    )
    assert "Buy" in labels
    assert "Cash Dividend" in labels
    assert "cash" not in {label.casefold() for label in labels}
    assert "account_total" not in {label.casefold() for label in labels}


def test_result_exit_treats_absent_as_success():
    assert result_exit({"history_rows_gt_0": "absent", "current_rows_gt_0": "pass"}) == 0
    assert result_exit({"history_rows_gt_0": "fail"}) == 1
    assert result_exit({"history_null_symbol": "1.000"}) == 0


def test_dev_mirror_workflow_builds_dev_and_does_not_write_prod():
    text = (ROOT / ".github/workflows/dev_mirror.yml").read_text()
    steps = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )
    assert "dbt build --profiles-dir" in steps
    assert "profiles.dev.yml" in steps
    assert "DBT_RAW_DATASET: analytics_raw" in steps
    assert "BQ_DATASET: analytics_dev" in steps
    assert "dev_clone_prod.py --dest analytics_dev" in steps
    assert "workflow_call" in text
    assert "dbt_build:" in text
    assert "--broker fidelity" in steps
    ci = (ROOT / ".github/workflows/ci.yml").read_text()
    assert "vanguard-dev-dbt" not in ci
    # The one-shot analytics_dev build for this branch already ran
    # (Actions run 38590557268, commit 97ab3a63). Later pushes must not
    # rebuild the dev dataset.
    assert "fidelity-dev-dbt" not in ci
    assert "profiles.dev.yml" not in ci
    for forbidden in (
        "snapshot_guard",
        "repair_history_fill_dedup",
        "current_position_stock_price",
        "cache/flush",
        "bigquery_update",
    ):
        assert forbidden not in steps
