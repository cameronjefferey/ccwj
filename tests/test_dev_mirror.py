"""Local-dev mirror: clone safety, raw merge, warehouse tenant projection.

These pin the "develop locally against prod data" contract without hitting
BigQuery or prod Postgres. A regression here is how local pages go empty
while dbt/pytest stay green — the clone writes the wrong dataset, or the
raw merge drops the user's tenants, or linking mints a slug that collides
with UNIQUE(broker_slug, broker_uuid).
"""
from __future__ import annotations

import pandas as pd
import pytest

from scripts.dev_clone_prod import (
    FORBIDDEN_DEST,
    PROD_DATASET,
    assert_safe_dest,
    rewrite_dataset_refs,
)
from scripts.dev_refresh_raw import merge_seed_frames


def _load_link():
    # Hyphenated filename: load via importlib.
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "scripts" / "dev-link-prod-tenants.py"
    spec = importlib.util.spec_from_file_location("dev_link_prod_tenants", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Clone safety
# ---------------------------------------------------------------------------


def test_assert_safe_dest_accepts_analytics_dev():
    assert assert_safe_dest("analytics_dev") == "analytics_dev"


def test_assert_safe_dest_rejects_prod_and_raw_and_backups():
    for dest in FORBIDDEN_DEST | {PROD_DATASET, "analytics", "analytics_raw"}:
        with pytest.raises(ValueError, match="refusing"):
            assert_safe_dest(dest)


def test_assert_safe_dest_rejects_raw_seed_datasets():
    with pytest.raises(ValueError, match="raw"):
        assert_safe_dest("analytics_raw_dev")
    with pytest.raises(ValueError, match="raw"):
        assert_safe_dest("analytics_raw")


def test_assert_safe_dest_rejects_empty():
    with pytest.raises(ValueError, match="empty"):
        assert_safe_dest("")
    with pytest.raises(ValueError, match="empty"):
        assert_safe_dest("   ")


def test_rewrite_dataset_refs_rewrites_backtick_and_bare():
    sql = (
        "SELECT * FROM `ccwj-dbt.analytics.stg_history` h "
        "JOIN ccwj-dbt.analytics.positions_summary p USING (tenant_id)"
    )
    out = rewrite_dataset_refs(sql, "analytics", "analytics_dev")
    assert "`ccwj-dbt.analytics_dev.stg_history`" in out
    assert "ccwj-dbt.analytics_dev.positions_summary" in out
    assert "ccwj-dbt.analytics." not in out


def test_rewrite_dataset_refs_does_not_touch_sibling_datasets():
    """``analytics.`` must not rewrite analytics_dev / analytics_raw /
    analytics_backups — trailing-dot match is load-bearing."""
    sql = (
        "SELECT 1 FROM `ccwj-dbt.analytics_dev.foo` a, "
        "`ccwj-dbt.analytics_raw.trade_history` b, "
        "`ccwj-dbt.analytics_backups.snapshot_account_balances_daily_20260807` c, "
        "`ccwj-dbt.analytics.stg_history` d"
    )
    out = rewrite_dataset_refs(sql, "analytics", "analytics_dev")
    assert "`ccwj-dbt.analytics_dev.foo`" in out
    assert "`ccwj-dbt.analytics_raw.trade_history`" in out
    assert "`ccwj-dbt.analytics_backups.snapshot_account_balances_daily_20260807`" in out
    assert "`ccwj-dbt.analytics_dev.stg_history`" in out
    assert "`ccwj-dbt.analytics.stg_history`" not in out


def test_rewrite_dataset_refs_empty_is_noop():
    assert rewrite_dataset_refs("", "analytics", "analytics_dev") == ""
    assert rewrite_dataset_refs(None, "analytics", "analytics_dev") is None


# ---------------------------------------------------------------------------
# Raw merge (local tenants win)
# ---------------------------------------------------------------------------


def _seed(rows):
    return pd.DataFrame(rows, dtype=str)


def test_merge_prefers_local_rows_for_local_tenants():
    prod = _seed(
        [
            {"tenant_id": "snaptrade:aaa", "Amount": "prod-a"},
            {"tenant_id": "snaptrade:bbb", "Amount": "prod-b"},
        ]
    )
    dev = _seed(
        [
            {"tenant_id": "snaptrade:aaa", "Amount": "local-a"},
            {"tenant_id": "snaptrade:bbb", "Amount": "stale-b"},
        ]
    )
    merged, local_fresh = merge_seed_frames(
        prod, dev, local_ids={"snaptrade:aaa"}
    )
    by_t = dict(zip(merged["tenant_id"], merged["Amount"]))
    assert by_t["snaptrade:aaa"] == "local-a"
    assert by_t["snaptrade:bbb"] == "prod-b"  # not the stale dev copy
    assert local_fresh == {"snaptrade:aaa"}


def test_merge_mirrors_prod_when_local_tenant_has_no_dev_rows():
    """The --link case: tenant is in local Postgres but analytics_raw_dev
    has never been written for it — keep the prod copy."""
    prod = _seed([{"tenant_id": "snaptrade:aaa", "Amount": "prod-a"}])
    dev = _seed([{"tenant_id": "snaptrade:other", "Amount": "x"}])
    merged, local_fresh = merge_seed_frames(
        prod, dev, local_ids={"snaptrade:aaa"}
    )
    assert list(merged["Amount"]) == ["prod-a"]
    assert local_fresh == set()


def test_merge_empty_dev_returns_prod():
    prod = _seed([{"tenant_id": "snaptrade:aaa", "Amount": "prod-a"}])
    merged, local_fresh = merge_seed_frames(prod, None, local_ids={"snaptrade:aaa"})
    assert list(merged["Amount"]) == ["prod-a"]
    assert local_fresh == set()


# ---------------------------------------------------------------------------
# Warehouse → Postgres tenant projection
# ---------------------------------------------------------------------------


def test_split_tenant_id_snaptrade_and_demo():
    link = _load_link()
    assert link.split_tenant_id("snaptrade:bed78305-a764-4c4d-b4c7-fe59e391f661") == (
        "snaptrade",
        "bed78305-a764-4c4d-b4c7-fe59e391f661",
    )
    assert link.split_tenant_id("demo:demo-account") == ("demo", "demo-account")


def test_split_tenant_id_no_colon_does_not_invent_empty_uuid():
    link = _load_link()
    slug, uuid = link.split_tenant_id("orphan")
    assert slug == "snaptrade"
    assert uuid == "orphan"


def test_dim_row_to_tenant_uses_tenant_id_prefix_not_display_broker():
    """dim.broker_slug is schwab/alpaca (display). Postgres unique-keys on
    the aggregator slug that prefixes tenant_id (snaptrade). Mixing them
    would fork a second tenant on the next local SnapTrade connect."""
    link = _load_link()
    row = link.dim_row_to_tenant(
        {
            "tenant_id": "snaptrade:abc-123",
            "user_id": 9,
            "account_name": "Schwab Account",
            "broker_slug": "schwab",
            "aggregator_slug": "snaptrade",
            "broker_uuid": "abc-123",
        }
    )
    assert row["tenant_id"] == "snaptrade:abc-123"
    assert row["broker_slug"] == "snaptrade"
    assert row["broker_uuid"] == "abc-123"
    assert row["account_name"] == "Schwab Account"
    assert row["broker_label"] == "schwab"
    assert row["connection_status"] == "active"
    assert row["first_sync_completed"] is True
    # Every upsert column is present so _upsert_tenants can key on _TENANT_COLS.
    assert set(link._TENANT_COLS) <= set(row)


def test_dim_row_to_tenant_demo_prefix():
    link = _load_link()
    row = link.dim_row_to_tenant(
        {
            "tenant_id": "demo:demo-account",
            "account_name": "Demo Account",
            "broker_slug": "alpaca",
        }
    )
    assert row["broker_slug"] == "demo"
    assert row["broker_uuid"] == "demo-account"
    assert row["broker_label"] == "alpaca"
