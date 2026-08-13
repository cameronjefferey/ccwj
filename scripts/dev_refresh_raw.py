#!/usr/bin/env python
"""Rebuild the dev raw seed dataset from prod + local syncs.

The dev warehouse mirror (``analytics_dev``) is built from a dev copy of
the raw seed tables (``analytics_raw_dev``) so local testing sees real
prod data WITHOUT local writes ever touching prod (the same environment
separation the retired ``dev-seeds`` git branch provided):

    analytics_raw_dev.<table> =
          prod analytics_raw rows   (tenants NOT freshly synced locally)
        + analytics_raw_dev rows    (local tenants with local sync data)

Local tenants are whatever the local Postgres ``broker_tenants`` table
says — never a hardcoded list, and never a numeric ``user_id`` (ids
collide across environments; tenant_id is the only cross-env-stable key).
A local tenant with NO local rows keeps its prod copy — this is the
"mirror a prod user to be them locally" case
(scripts/dev-link-prod-tenants.py). Same semantics as the retired
``scripts/merge_dev_seeds.py``, just BQ-to-BQ instead of git-to-git.

Called by scripts/dev-refresh.sh; safe to run directly too. Requires the
local .env (DATABASE_URL for broker_tenants) and BigQuery credentials.
"""
from __future__ import annotations

import os
import sys
from io import StringIO

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PROD_DATASET = "analytics_raw"
DEV_DATASET = "analytics_raw_dev"

SEED_PATHS = [
    "dbt/seeds/trade_history.csv",
    "dbt/seeds/current_positions.csv",
    "dbt/seeds/account_balances.csv",
]


def local_tenant_ids():
    from dotenv import load_dotenv

    load_dotenv()  # DATABASE_URL for app.db
    from app.db import fetch_all

    rows = fetch_all("SELECT tenant_id FROM broker_tenants")
    ids = {r["tenant_id"] for r in rows if r.get("tenant_id")}
    if not ids:
        # Fail loudly: an empty list would silently keep prod's stale
        # copies of local tenants alongside fresh local rows (dupes).
        raise SystemExit("dev_refresh_raw: local broker_tenants returned no tenant_ids")
    return ids


def _frame(csv_text):
    if not csv_text:
        return None
    return pd.read_csv(StringIO(csv_text), dtype=str, keep_default_na=False)


def merge_seed_frames(prod, dev, local_ids):
    """Merge prod raw rows with locally-synced tenants.

    Local tenants that already have rows in ``dev`` win (fresh local sync);
    every other tenant mirrors ``prod``. Stale copies of prod tenants left
    in ``dev`` from a previous refresh are dropped — never carried over.

    ``prod`` / ``dev`` are string-typed DataFrames with a ``tenant_id``
    column. ``local_ids`` is the set of tenant_ids in local Postgres.
    """
    if dev is None or dev.empty:
        return prod.copy(), set()
    dev_ids = set(dev["tenant_id"]) if "tenant_id" in dev.columns else set()
    local_fresh = {t for t in local_ids if t in dev_ids}
    prod_rows = prod[~prod["tenant_id"].isin(local_fresh)]
    dev_rows = dev[dev["tenant_id"].isin(local_fresh)]
    merged = pd.concat([prod_rows, dev_rows], ignore_index=True)
    merged = merged[
        list(prod.columns) + [c for c in dev.columns if c not in prod.columns]
    ]
    return merged, local_fresh


def main():
    local_ids = local_tenant_ids()
    print(f"local tenants: {len(local_ids)}")

    from app import seed_store

    client = seed_store._get_client()
    for path in SEED_PATHS:
        prod = _frame(seed_store.read_seed_csv(path, client=client, dataset=PROD_DATASET))
        if prod is None:
            raise SystemExit(f"dev_refresh_raw: prod table for {path} does not exist")
        try:
            dev = _frame(seed_store.read_seed_csv(path, client=client, dataset=DEV_DATASET))
        except seed_store.SeedStoreError:
            dev = None
        if dev is None:
            dev = prod.iloc[0:0]

        merged, local_fresh = merge_seed_frames(prod, dev, local_ids)

        seed_store.write_seed_csvs(
            [(path, merged.to_csv(index=False))], client=client, dataset=DEV_DATASET,
        )
        mirrored = len(local_ids) - len(local_fresh)
        print(
            f"{path}: {len(prod)} prod rows -> kept {len(prod_rows)}"
            f" + {len(dev_rows)} local = {len(merged)}"
            f" (local tenants: {len(local_fresh)} kept local,"
            f" {mirrored} mirrored from prod)"
        )


if __name__ == "__main__":
    main()
