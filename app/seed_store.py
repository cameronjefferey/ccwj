"""BigQuery-backed seed store — the storage layer under the seed merge.

Replaces the git-as-database flow (seed CSVs committed to GitHub via the
Contents/Git-Data API) with three raw tables in a dedicated BigQuery
dataset, written directly by the sync:

    ccwj-dbt.<BQ_RAW_DATASET>.trade_history
    ccwj-dbt.<BQ_RAW_DATASET>.current_positions
    ccwj-dbt.<BQ_RAW_DATASET>.account_balances

``BQ_RAW_DATASET`` defaults to ``analytics_raw`` (production). Local dev
sets ``BQ_RAW_DATASET=analytics_raw_dev`` so dev syncs never touch prod
rows (same discipline as the ``BQ_DATASET=analytics_dev`` read override).

Design contract (mirrors the battle-tested GitHub path byte-for-byte so
``_merge_seed_with_existing`` and every merge/dedup invariant in
``app/upload.py`` carry over unchanged):

- The unit of exchange is a CSV STRING. ``read_seed_csv`` serializes the
  table back to exactly the CSV text that was last written;
  ``write_seed_csvs`` parses CSV text and loads it. The merge layer never
  knows the storage moved.
- Row order is preserved via a hidden ``_row_seq`` INT64 column (BigQuery
  tables have no inherent order; the seed CSVs' append order is what the
  cross-source dedup and the byte-exact no-op check rely on). ``_row_seq``
  is stripped on read and is invisible to dbt (staging models select
  named columns only).
- All data columns are STRING and empty CSV cells round-trip through
  NULL, matching how ``dbt seed`` loaded the CSVs.
- Reads FAIL CLOSED: a query error raises ``SeedStoreError`` so a merge
  can never mistake a transient blip for "no existing data" and wipe
  other tenants' rows (the commit ``3f4aecb`` failure mode). Only a
  genuinely missing table (first-ever write) reads as ``None``.
- Writes are atomic per table: ``load_table_from_dataframe`` with
  ``WRITE_TRUNCATE`` either fully replaces the table or leaves it
  untouched. BigQuery time travel gives 7-day point-in-time recovery
  (``FOR SYSTEM_TIME AS OF``) on top.
"""
from __future__ import annotations

import logging
import os
from io import StringIO

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

_log = logging.getLogger(__name__)

# Seed-path → raw-table mapping. Keyed by the historical repo paths so
# every existing caller (and 50+ merge tests) keeps addressing seeds by
# the same identifiers; the path is now just a logical name.
SEED_TABLES = {
    "dbt/seeds/trade_history.csv": "trade_history",
    "dbt/seeds/current_positions.csv": "current_positions",
    "dbt/seeds/account_balances.csv": "account_balances",
}

_ROW_SEQ_COL = "_row_seq"

_PRODUCTION_RAW_DATASET = "analytics_raw"


class SeedStoreError(RuntimeError):
    """Raised when a seed-store read/write fails for any reason other than
    the table genuinely not existing yet. Callers must treat this as
    "abort the sync", never as "seed is empty" — see the module docstring
    and ``app.upload.SeedFetchError``."""


def raw_project() -> str:
    return os.environ.get("BQ_RAW_PROJECT", "ccwj-dbt").strip()


def raw_dataset() -> str:
    """Raw dataset name. Production: ``analytics_raw`` (default). Local
    dev sets ``BQ_RAW_DATASET=analytics_raw_dev`` for env separation."""
    return (os.environ.get("BQ_RAW_DATASET") or _PRODUCTION_RAW_DATASET).strip()


def is_production_store() -> bool:
    """True when writes land in the production raw dataset — used to gate
    the CI ``workflow_dispatch`` rebuild (dev builds locally instead)."""
    return raw_dataset() == _PRODUCTION_RAW_DATASET


def _table_id(path: str, dataset: str | None = None) -> str:
    table = SEED_TABLES.get(path)
    if not table:
        raise SeedStoreError(f"Unknown seed path {path!r} — no raw table mapping.")
    return f"{raw_project()}.{dataset or raw_dataset()}.{table}"


def _get_client():
    from app.bigquery_client import get_bigquery_client
    return get_bigquery_client()


def read_seed_csv(path: str, client=None, dataset: str | None = None) -> str | None:
    """Return the seed's CSV text (header + rows, in original write order),
    or ``None`` when the raw table does not exist yet (first-ever write —
    the analogue of the GitHub 404).

    Raises ``SeedStoreError`` on any other failure so the merge layer
    fails closed instead of treating a blip as an empty seed.

    ``dataset`` overrides the env-derived raw dataset (used by admin
    tooling like scripts/dev_refresh_raw.py that reads prod and writes
    dev in one process). App writers never pass it.
    """
    table_id = _table_id(path, dataset)
    client = client or _get_client()
    try:
        df = client.query(
            f"SELECT * FROM `{table_id}` ORDER BY {_ROW_SEQ_COL}"
        ).to_dataframe()
    except NotFound:
        return None
    except Exception as exc:
        raise SeedStoreError(
            f"BigQuery read of {table_id} failed: {exc}"
        ) from exc
    if _ROW_SEQ_COL in df.columns:
        df = df.drop(columns=[_ROW_SEQ_COL])
    # NULL cells were empty CSV cells on write; serialize them back to "".
    df = df.astype(object).where(pd.notna(df), "")
    return df.to_csv(index=False)


def write_seed_csvs(path_contents, client=None, dataset: str | None = None) -> None:
    """Atomically replace each raw table with the given CSV content.

    ``path_contents`` — iterable of ``(seed_path, csv_text)``. Each table
    load is atomic (``WRITE_TRUNCATE``: a failed job leaves the previous
    rows untouched). Raises ``SeedStoreError`` on the first failure; a
    partially-applied batch is safe because every individual table is
    still internally consistent and the next sync converges it.

    ``dataset`` overrides the env-derived raw dataset (admin tooling only).
    """
    client = client or _get_client()
    for path, content in path_contents:
        table_id = _table_id(path, dataset)
        try:
            df = pd.read_csv(StringIO(content), dtype=str, keep_default_na=False)
        except Exception as exc:
            raise SeedStoreError(
                f"Merged seed for {table_id} failed to parse — refusing to "
                f"write: {exc}"
            ) from exc
        # Empty cells -> NULL (matches how dbt seed loaded empty CSV cells).
        df = df.astype(object).where(df != "", None)
        df[_ROW_SEQ_COL] = range(len(df))
        schema = [
            bigquery.SchemaField(col, "STRING")
            for col in df.columns
            if col != _ROW_SEQ_COL
        ] + [bigquery.SchemaField(_ROW_SEQ_COL, "INT64")]
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        try:
            _ensure_dataset(client, dataset)
            job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()
        except Exception as exc:
            raise SeedStoreError(
                f"BigQuery load into {table_id} failed: {exc}"
            ) from exc
        _log.info("seed_store: wrote %s rows to %s", len(df), table_id)


def _ensure_dataset(client, dataset: str | None = None) -> None:
    """Create the raw dataset on first use (no-op afterwards)."""
    dataset_ref = bigquery.Dataset(f"{raw_project()}.{dataset or raw_dataset()}")
    dataset_ref.location = os.environ.get("BQ_LOCATION", "US").strip()
    try:
        client.create_dataset(dataset_ref, exists_ok=True)
    except Exception as exc:  # pragma: no cover (permissions edge)
        # A missing-permission failure surfaces on the load call anyway;
        # don't mask the real error here.
        _log.warning("seed_store: create_dataset skipped: %s", exc)
