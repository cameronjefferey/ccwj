#!/usr/bin/env python
"""Backfill the dev warehouse's accumulating snapshot tables from prod.

Why this exists
---------------
``snapshot_account_balances_daily`` and ``snapshot_options_market_values_daily``
are dbt ``check``-strategy snapshots stamped with ``current_date()`` — they
accrue ONE new version per day the build runs. Production rebuilds daily, so it
holds a continuous per-day history (the Daily Review's account snapshot row,
day-over-day deltas, and the Δ heatmap all read off that history). The dev
dataset (``analytics_dev``) only accrues a row on the days ``dev-refresh`` runs,
so it has a sparse, stale history (e.g. a single old day) — which makes the
Daily Review look nothing like prod (frozen values, empty "vs yesterday/1w/1m").

Rebuilding from seeds can't reconstruct that history. This script copies prod's
snapshot tables into the dev dataset so dev starts from prod's accumulated
history; the subsequent ``dbt build``/``dbt snapshot`` then MERGEs today's state
on top (the snapshot's ``unique_key`` + ``check`` strategy makes that idempotent).

Reads prod (``analytics``) read-only and writes only the dev dataset named by
``BQ_DATASET``. Refuses to run unless ``BQ_DATASET`` is a non-prod dataset.

Usage (called by scripts/dev-refresh.sh; safe to run standalone):
    BQ_DATASET=analytics_dev python scripts/dev_backfill_snapshots.py
"""
from __future__ import annotations

import os
import sys

from google.cloud import bigquery

PROJECT = "ccwj-dbt"
PROD_DATASET = "analytics"
SNAPSHOT_TABLES = [
    "snapshot_account_balances_daily",
    "snapshot_options_market_values_daily",
]


def main():
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    dev_dataset = (os.environ.get("BQ_DATASET") or "").strip()
    if not dev_dataset or dev_dataset == PROD_DATASET:
        sys.exit(
            f"refusing to run: BQ_DATASET='{dev_dataset or '<unset>'}' must be a "
            f"non-prod dataset (e.g. analytics_dev). This script copies prod "
            f"snapshots INTO the dev dataset and must never target prod."
        )

    # Raw client — NOT app.bigquery_client (that one rewrites analytics ->
    # analytics_dev, which would make the source == target).
    client = bigquery.Client(project=PROJECT)

    for tbl in SNAPSHOT_TABLES:
        src = f"`{PROJECT}.{PROD_DATASET}.{tbl}`"
        dst = f"`{PROJECT}.{dev_dataset}.{tbl}`"
        # CREATE OR REPLACE preserves the dbt snapshot metadata columns
        # (dbt_scd_id / dbt_valid_from / dbt_valid_to), so the next
        # `dbt snapshot` MERGE picks up cleanly from prod's latest versions.
        sql = f"CREATE OR REPLACE TABLE {dst} AS SELECT * FROM {src}"
        try:
            client.query(sql).result()
            n = list(
                client.query(f"SELECT COUNT(*) c FROM {dst}").result()
            )[0]["c"]
            print(f"  backfilled {dev_dataset}.{tbl} from prod ({n} rows)")
        except Exception as exc:  # noqa: BLE001
            # Don't hard-fail the whole refresh if one snapshot is missing in
            # prod (e.g. a fresh project) — warn and continue.
            print(f"  WARN: could not backfill {tbl}: {exc}")


if __name__ == "__main__":
    main()
