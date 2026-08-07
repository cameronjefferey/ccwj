"""Unit tests for the BigQuery seed store (app/seed_store.py) and the
rebuild dispatch that fires after changed writes.

All network-free: a fake BigQuery client simulates table storage
(including BigQuery's LACK of inherent row order — rows are stored
shuffled so the tests prove reads depend on the hidden ``_row_seq``
column, not on storage luck), and the GitHub dispatch is exercised
against a stubbed ``requests.post``.
"""
from __future__ import annotations

from io import StringIO

import pandas as pd
import pytest
from google.api_core.exceptions import NotFound

from app import seed_store
from app.seed_store import (
    SeedStoreError,
    is_production_store,
    read_seed_csv,
    write_seed_csvs,
)

HISTORY_PATH = "dbt/seeds/trade_history.csv"
CURRENT_PATH = "dbt/seeds/current_positions.csv"


# ---------------------------------------------------------------------------
# Fake BigQuery client
# ---------------------------------------------------------------------------


class _FakeJob:
    def __init__(self, df=None):
        self._df = df

    def to_dataframe(self):
        return self._df

    def result(self):
        return None


class _FakeClient:
    """Stores loaded DataFrames per table id. Rows are stored REVERSED to
    simulate BigQuery's undefined SELECT order; ``query`` honors an
    ``ORDER BY _row_seq`` clause so a correct reader gets original order
    back and a reader that forgot the ORDER BY would see reversed rows."""

    def __init__(self):
        self.tables = {}
        self.datasets_created = []
        self.fail_next_query = None
        self.fail_next_load = None

    def create_dataset(self, dataset_ref, exists_ok=False):
        self.datasets_created.append(str(dataset_ref))

    def query(self, sql):
        if self.fail_next_query is not None:
            exc, self.fail_next_query = self.fail_next_query, None
            raise exc
        table_id = sql.split("`")[1]
        if table_id not in self.tables:
            raise NotFound(f"Not found: table {table_id}")
        df = self.tables[table_id].copy()
        if "ORDER BY _row_seq" in sql:
            df = df.sort_values("_row_seq").reset_index(drop=True)
        return _FakeJob(df)

    def load_table_from_dataframe(self, df, table_id, job_config=None):
        if self.fail_next_load is not None:
            exc, self.fail_next_load = self.fail_next_load, None
            raise exc
        # Simulate no inherent order: persist rows reversed.
        self.tables[table_id] = df.iloc[::-1].reset_index(drop=True)
        return _FakeJob()


@pytest.fixture()
def client():
    return _FakeClient()


_CSV = (
    "Account,user_id,tenant_id,Date,Action,Symbol,Description,Quantity,Price,fees_and_comm,Amount\n"
    "Schwab Account,9,snaptrade:aaa,01/02/2025,Buy,AAPL,APPLE INC,10,200.0,,-2000.0\n"
    "Schwab Account,9,snaptrade:aaa,01/03/2025,Sell,AAPL,APPLE INC,10,210.0,0.04,2099.96\n"
    "Alpaca Paper Account,18,snaptrade:bbb,01/04/2025,Buy,MSFT,MICROSOFT,5,400.0,,-2000.0\n"
)


# ---------------------------------------------------------------------------
# Round-trip contract
# ---------------------------------------------------------------------------


def test_write_then_read_round_trips_byte_identical(client):
    write_seed_csvs([(HISTORY_PATH, _CSV)], client=client)
    assert read_seed_csv(HISTORY_PATH, client=client) == _CSV


def test_read_preserves_write_order_despite_storage_shuffle(client):
    """The fake stores rows reversed — original order must come back via
    _row_seq. (BigQuery tables have no inherent order; the merge layer's
    byte-exact no-op check depends on deterministic reads.)"""
    write_seed_csvs([(HISTORY_PATH, _CSV)], client=client)
    out = pd.read_csv(
        StringIO(read_seed_csv(HISTORY_PATH, client=client)),
        dtype=str, keep_default_na=False,
    )
    assert list(out["Symbol"]) == ["AAPL", "AAPL", "MSFT"]
    assert "_row_seq" not in out.columns


def test_empty_cells_round_trip_through_null(client):
    """"" on write → NULL in the table (matching dbt seed's loading of
    empty CSV cells) → "" again on read."""
    write_seed_csvs([(HISTORY_PATH, _CSV)], client=client)
    table_id = f"{seed_store.raw_project()}.{seed_store.raw_dataset()}.trade_history"
    stored = client.tables[table_id]
    # Row with no fees_and_comm must be NULL in storage, not "".
    aapl_buy = stored[stored["Action"] == "Buy"].iloc[0]
    assert aapl_buy["fees_and_comm"] is None
    # ...and come back as an empty CSV cell.
    out = pd.read_csv(
        StringIO(read_seed_csv(HISTORY_PATH, client=client)),
        dtype=str, keep_default_na=False,
    )
    assert (out.loc[out["Action"] == "Buy", "fees_and_comm"] == "").all()


def test_write_truncate_replaces_previous_rows(client):
    write_seed_csvs([(HISTORY_PATH, _CSV)], client=client)
    smaller = _CSV.rsplit("\n", 2)[0] + "\n"  # drop the MSFT row
    write_seed_csvs([(HISTORY_PATH, smaller)], client=client)
    assert read_seed_csv(HISTORY_PATH, client=client) == smaller


def test_header_only_seed_round_trips(client):
    header_only = _CSV.splitlines()[0] + "\n"
    write_seed_csvs([(HISTORY_PATH, header_only)], client=client)
    assert read_seed_csv(HISTORY_PATH, client=client) == header_only


# ---------------------------------------------------------------------------
# Fail-closed contract
# ---------------------------------------------------------------------------


def test_missing_table_reads_as_none(client):
    """First-ever write: table doesn't exist -> None (the GitHub-404
    analogue), NOT an exception and NOT an empty frame."""
    assert read_seed_csv(HISTORY_PATH, client=client) is None


def test_transient_query_error_raises_seed_store_error(client):
    """Anything other than NotFound must raise — a merge that treats a
    blip as 'no existing data' would wipe other tenants' rows."""
    client.fail_next_query = RuntimeError("BigQuery unavailable")
    with pytest.raises(SeedStoreError):
        read_seed_csv(HISTORY_PATH, client=client)


def test_failed_load_raises_seed_store_error(client):
    client.fail_next_load = RuntimeError("load job failed")
    with pytest.raises(SeedStoreError):
        write_seed_csvs([(HISTORY_PATH, _CSV)], client=client)


def test_unknown_seed_path_rejected(client):
    with pytest.raises(SeedStoreError):
        read_seed_csv("dbt/seeds/not_a_seed.csv", client=client)
    with pytest.raises(SeedStoreError):
        write_seed_csvs([("dbt/seeds/not_a_seed.csv", _CSV)], client=client)


# ---------------------------------------------------------------------------
# Environment separation
# ---------------------------------------------------------------------------


def test_default_dataset_is_production(monkeypatch):
    monkeypatch.delenv("BQ_RAW_DATASET", raising=False)
    assert seed_store.raw_dataset() == "analytics_raw"
    assert is_production_store() is True


def test_dev_dataset_is_not_production(monkeypatch):
    monkeypatch.setenv("BQ_RAW_DATASET", "analytics_raw_dev")
    assert seed_store.raw_dataset() == "analytics_raw_dev"
    assert is_production_store() is False


def test_dataset_override_routes_tables(monkeypatch, client):
    """The env knob must route BOTH reads and writes — a dev sync writing
    prod tables was the June 2026 cross-env incident class."""
    monkeypatch.setenv("BQ_RAW_DATASET", "analytics_raw_dev")
    write_seed_csvs([(HISTORY_PATH, _CSV)], client=client)
    assert list(client.tables) == ["ccwj-dbt.analytics_raw_dev.trade_history"]
    assert read_seed_csv(HISTORY_PATH, client=client) == _CSV


# ---------------------------------------------------------------------------
# Rebuild dispatch gating (app/upload.py)
# ---------------------------------------------------------------------------


def _upload():
    from app import upload
    return upload


def test_dispatch_skipped_outside_production_store(monkeypatch):
    monkeypatch.setenv("BQ_RAW_DATASET", "analytics_raw_dev")
    monkeypatch.setenv("GITHUB_PAT", "x" * 20)
    up = _upload()

    def _boom(*a, **k):
        raise AssertionError("must not call GitHub outside the prod store")

    monkeypatch.setattr(up.requests, "post", _boom)
    assert up._dispatch_warehouse_rebuild("test") is None


def test_dispatch_skipped_without_pat(monkeypatch):
    monkeypatch.delenv("BQ_RAW_DATASET", raising=False)
    monkeypatch.delenv("GITHUB_PAT", raising=False)
    up = _upload()

    def _boom(*a, **k):
        raise AssertionError("must not call GitHub without a PAT")

    monkeypatch.setattr(up.requests, "post", _boom)
    assert up._dispatch_warehouse_rebuild("test") is None


def test_dispatch_returns_marker_on_204(monkeypatch):
    monkeypatch.delenv("BQ_RAW_DATASET", raising=False)
    monkeypatch.setenv("GITHUB_PAT", "x" * 20)
    up = _upload()
    calls = {}

    class _Resp:
        status_code = 204
        text = ""

    def _fake_post(url, headers=None, json=None, timeout=None):
        calls["url"] = url
        calls["json"] = json
        return _Resp()

    monkeypatch.setattr(up.requests, "post", _fake_post)
    marker = up._dispatch_warehouse_rebuild("test")
    assert marker and marker.startswith("dispatch:")
    assert int(marker.split(":", 1)[1]) > 0
    assert "actions/workflows/bigquery_update.yml/dispatches" in calls["url"]
    assert calls["json"] == {"ref": "master"}


def test_dispatch_failure_returns_none_not_raise(monkeypatch):
    """A dispatch failure must never fail the sync — the write already
    landed; the scheduled nightly build is the backstop."""
    monkeypatch.delenv("BQ_RAW_DATASET", raising=False)
    monkeypatch.setenv("GITHUB_PAT", "x" * 20)
    up = _upload()

    class _Resp:
        status_code = 500
        text = "boom"

    monkeypatch.setattr(up.requests, "post", lambda *a, **k: _Resp())
    assert up._dispatch_warehouse_rebuild("test") is None
