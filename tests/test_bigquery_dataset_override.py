"""BQ_DATASET env-separation chokepoint (app/bigquery_client.py).

Local dev points the app at a dev dataset (``BQ_DATASET=analytics_dev``)
so dev-environment tenants never mix with production data. The override
rewrites the hardcoded ``ccwj-dbt.analytics.`` prefix in every query the
app submits; production (var unset / ``analytics``) must be a strict
no-op.
"""

from app.bigquery_client import _apply_dataset_override


SQL = """
    SELECT account, tenant_id
    FROM `ccwj-dbt.analytics.positions_summary` ps
    LEFT JOIN `ccwj-dbt.analytics.stg_history` h USING (tenant_id)
    WHERE 1=1
"""


def test_noop_when_env_unset(monkeypatch):
    monkeypatch.delenv("BQ_DATASET", raising=False)
    assert _apply_dataset_override(SQL) == SQL


def test_noop_when_env_is_canonical_dataset(monkeypatch):
    monkeypatch.setenv("BQ_DATASET", "analytics")
    assert _apply_dataset_override(SQL) == SQL


def test_rewrites_every_table_ref(monkeypatch):
    monkeypatch.setenv("BQ_DATASET", "analytics_dev")
    out = _apply_dataset_override(SQL)
    assert "ccwj-dbt.analytics." not in out
    assert "`ccwj-dbt.analytics_dev.positions_summary`" in out
    assert "`ccwj-dbt.analytics_dev.stg_history`" in out


def test_handles_empty_sql(monkeypatch):
    monkeypatch.setenv("BQ_DATASET", "analytics_dev")
    assert _apply_dataset_override("") == ""
    assert _apply_dataset_override(None) is None
