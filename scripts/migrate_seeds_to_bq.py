#!/usr/bin/env python
"""One-time backfill: load the tenant seed CSVs into the BigQuery seed store.

Loads dbt/seeds/{trade_history,current_positions,account_balances}.csv into
``ccwj-dbt.<BQ_RAW_DATASET>.<table>`` (default ``analytics_raw``) using the
exact same write path the app uses (``app/seed_store.py``), then verifies
parity by reading every table back and comparing:

  1. cell-for-cell frame equality (columns, order, values) against the CSV
  2. per-(table, tenant_id) row counts, printed for the operator

Run this BEFORE flipping the dbt staging models from ``ref()`` to
``source()``. It is idempotent — re-running just re-truncates the raw
tables with the same rows.

Usage:
    python scripts/migrate_seeds_to_bq.py            # working-tree CSVs
    python scripts/migrate_seeds_to_bq.py --git-ref origin/master
    BQ_RAW_DATASET=analytics_raw_dev python scripts/migrate_seeds_to_bq.py

The module is imported standalone (not via the ``app`` package) so the
script needs no Flask/Postgres environment — only BigQuery credentials
(GOOGLE_APPLICATION_CREDENTIALS[_JSON_BASE64] or gcloud ADC).
"""
from __future__ import annotations

import argparse
import base64
import importlib.util
import json
import os
import subprocess
import sys
from io import StringIO

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SEED_PATHS = [
    "dbt/seeds/trade_history.csv",
    "dbt/seeds/current_positions.csv",
    "dbt/seeds/account_balances.csv",
]


def _load_seed_store():
    """Import app/seed_store.py as a standalone module (its module-level
    imports are pandas/google only, so no Flask app spin-up needed)."""
    path = os.path.join(REPO_ROOT, "app", "seed_store.py")
    spec = importlib.util.spec_from_file_location("seed_store_standalone", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_client():
    """BigQuery client with the same credential ladder as the app."""
    from google.cloud import bigquery
    from google.oauth2 import service_account

    b64 = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON_BASE64")
    if b64:
        info = json.loads(base64.b64decode(b64).decode())
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=creds.project_id)
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_path and os.path.exists(sa_path):
        creds = service_account.Credentials.from_service_account_file(sa_path)
        return bigquery.Client(credentials=creds, project=creds.project_id)
    return bigquery.Client(project="ccwj-dbt")


def _read_csv_text(seed_path: str, git_ref: str | None) -> str:
    if git_ref:
        out = subprocess.run(
            ["git", "-C", REPO_ROOT, "show", f"{git_ref}:{seed_path}"],
            check=True, capture_output=True,
        )
        return out.stdout.decode("utf-8")
    with open(os.path.join(REPO_ROOT, seed_path), encoding="utf-8") as f:
        return f.read()


def _frame(csv_text: str) -> pd.DataFrame:
    return pd.read_csv(StringIO(csv_text), dtype=str, keep_default_na=False)


def _tenant_counts(df: pd.DataFrame) -> pd.Series:
    if "tenant_id" not in df.columns:
        return pd.Series(dtype=int)
    return df["tenant_id"].replace("", "<blank>").value_counts().sort_index()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--git-ref", default=None,
        help="Read the seed CSVs from this git ref (e.g. origin/master) "
             "instead of the working tree.",
    )
    args = ap.parse_args()

    store = _load_seed_store()
    client = _make_client()
    dataset = store.raw_dataset()
    print(f"Target: {store.raw_project()}.{dataset}")
    print(f"Source: {'git ' + args.git_ref if args.git_ref else 'working tree'}")
    print("=" * 72)

    failures = 0
    for seed_path in SEED_PATHS:
        csv_text = _read_csv_text(seed_path, args.git_ref)
        src_df = _frame(csv_text)
        print(f"\n{seed_path}: {len(src_df)} rows, {len(src_df.columns)} cols")

        store.write_seed_csvs([(seed_path, csv_text)], client=client)

        # ---- Parity check: read back and compare cell-for-cell ----------
        roundtrip = store.read_seed_csv(seed_path, client=client)
        rt_df = _frame(roundtrip)
        if list(rt_df.columns) != list(src_df.columns):
            print(f"  FAIL column mismatch: {list(rt_df.columns)} != {list(src_df.columns)}")
            failures += 1
            continue
        if len(rt_df) != len(src_df):
            print(f"  FAIL row count: BQ={len(rt_df)} CSV={len(src_df)}")
            failures += 1
            continue
        if not rt_df.reset_index(drop=True).equals(src_df.reset_index(drop=True)):
            diff_mask = (rt_df.values != src_df.values)
            n_bad = int(diff_mask.sum())
            print(f"  FAIL {n_bad} cell(s) differ after round-trip")
            failures += 1
            continue
        print(f"  OK  round-trip identical ({len(rt_df)} rows)")

        # ---- Per-tenant counts (operator eyeball + audit trail) ---------
        src_counts = _tenant_counts(src_df)
        rt_counts = _tenant_counts(rt_df)
        if not src_counts.equals(rt_counts):
            print("  FAIL per-tenant counts diverge")
            failures += 1
            continue
        for tenant, n in src_counts.items():
            print(f"    {tenant}: {n}")

    print()
    print("=" * 72)
    if failures:
        print(f"DONE — {failures} FAILURE(S); do NOT flip dbt to source() yet")
    else:
        print("DONE — parity verified; safe to flip dbt staging to source()")
    return failures


if __name__ == "__main__":
    sys.exit(1 if main() else 0)
