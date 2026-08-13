#!/usr/bin/env python
"""Clone the production warehouse into ``analytics_dev``.

Why this exists
---------------
Local Flask reads ``BQ_DATASET=analytics_dev``. That dataset used to stay
fresh only when someone ran a full local ``dbt build`` (``dev-refresh.sh``),
which is the right tool for testing *dbt model changes* but the wrong tool
for "I want the app to show the same numbers as prod." Skipping the slow
rebuild left local pages empty or weeks behind — exactly when you most need
to catch a bug before paying customers see it.

This script copies every table (and rewrites every view) from prod
``analytics`` into ``analytics_dev``. It is:

- **Read-only on prod.** Refuses to run if the destination is a production
  dataset. Never sets a table expiration (Aug 2026 incident).
- **Fast.** BigQuery table COPY, not a second dbt graph. Typically a couple
  of minutes, not a full warehouse rebuild.
- **What the app actually reads.** After a clone, local ``./scripts/dev.sh``
  renders the same marts customers see. Use ``dev-refresh.sh`` only when you
  are testing *your* dbt models against real raw data.

Called by:

- ``./scripts/dev.sh --sync`` (local, on demand)
- ``.github/workflows/dev_mirror.yml`` after every prod warehouse /
  evening-prices build, so ``analytics_dev`` stays within one build of prod
  without anyone remembering to refresh.

It does NOT touch ``analytics_raw_dev`` (local SnapTrade/CSV syncs live
there; the next ``dev-refresh.sh`` merge preserves them).
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

PROJECT = "ccwj-dbt"
PROD_DATASET = "analytics"
DEV_DATASET = "analytics_dev"

# Destinations we will NEVER write. ``analytics_raw`` is the live seed store;
# ``analytics_backups`` is the snapshot-guard archive. A typo here is a
# production incident.
FORBIDDEN_DEST = frozenset(
    {PROD_DATASET, "analytics_raw", "analytics_backups"}
)

# Native tables we can COPY. MODELs (BQML) have no copy job; EXTERNAL tables
# point at GCS and stay shared on prod by design (see sources.yml).
COPYABLE_TYPES = frozenset({"TABLE", "SNAPSHOT", "CLONE", "MATERIALIZED_VIEW"})
VIEW_TYPES = frozenset({"VIEW"})
SKIP_TYPES = frozenset({"MODEL", "EXTERNAL"})

_MAX_COPY_WORKERS = 8
_VIEW_RETRIES = 6


def assert_safe_dest(dataset: str) -> str:
    """Return ``dataset`` or raise. Never let a clone target prod."""
    dest = (dataset or "").strip()
    if not dest:
        raise ValueError("destination dataset is empty")
    if dest in FORBIDDEN_DEST:
        raise ValueError(
            f"refusing to clone into {dest!r}: that dataset is production "
            f"(or a production backup). Destination must be a non-prod "
            f"dataset such as {DEV_DATASET!r}."
        )
    if "raw" in dest:
        raise ValueError(
            f"refusing to clone warehouse tables into {dest!r}: raw seed "
            f"datasets (analytics_raw / analytics_raw_dev) are a different "
            f"schema. This script only mirrors the app-read warehouse."
        )
    if dest == PROD_DATASET:
        raise ValueError("refusing to overwrite prod analytics")
    return dest


def rewrite_dataset_refs(sql: str, src: str, dst: str) -> str:
    """Rewrite ``project.src.`` refs in a view query to ``project.dst.``.

    The trailing-dot match is load-bearing: ``analytics.`` must not rewrite
    ``analytics_dev.`` / ``analytics_raw.`` / ``analytics_backups.``.
    """
    if not sql:
        return sql
    # Backtick-qualified: `ccwj-dbt.analytics.foo`
    pattern = rf"`{re.escape(PROJECT)}\.{re.escape(src)}\."
    sql = re.sub(pattern, f"`{PROJECT}.{dst}.", sql)
    # Bare-qualified: ccwj-dbt.analytics.foo
    pattern = rf"(?<![`\w]){re.escape(PROJECT)}\.{re.escape(src)}\."
    sql = re.sub(pattern, f"{PROJECT}.{dst}.", sql)
    return sql


def _client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT)


def _ensure_dest_dataset(client: bigquery.Client, dataset: str) -> None:
    ds_id = f"{PROJECT}.{dataset}"
    try:
        ds = client.get_dataset(ds_id)
    except NotFound:
        ds = bigquery.Dataset(ds_id)
        ds.location = "US"
        ds.default_table_expiration_ms = None
        ds.description = (
            "Dev mirror of production `analytics`. Cloned by "
            "scripts/dev_clone_prod.py after every prod warehouse build. "
            "Never set a default table expiration on this dataset."
        )
        client.create_dataset(ds)
        print(f"  created dataset {ds_id} (no default expiration)")
        return
    if ds.default_table_expiration_ms:
        # Belt and suspenders: the Aug 2026 incident was a leftover
        # default expiration silently deleting snapshot history.
        ds.default_table_expiration_ms = None
        client.update_dataset(ds, ["default_table_expiration_ms"])
        print(f"  cleared default table expiration on {ds_id}")


def _clear_table_expiration(client: bigquery.Client, table_id: str) -> None:
    try:
        table = client.get_table(table_id)
    except NotFound:
        return
    if table.expires is not None:
        table.expires = None
        client.update_table(table, ["expires"])


def _copy_one(client: bigquery.Client, src: str, dst: str) -> str:
    job = client.copy_table(
        src,
        dst,
        job_config=bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    _clear_table_expiration(client, dst)
    rows = client.get_table(dst).num_rows
    return f"copied {src} -> {dst} ({rows} rows)"


def _create_view(
    client: bigquery.Client, dst: str, view_sql: str, description: str | None
) -> None:
    table = bigquery.Table(dst)
    table.view_query = view_sql
    if description:
        table.description = description
    try:
        client.delete_table(dst, not_found_ok=True)
        client.create_table(table)
    except Exception:
        # CREATE OR REPLACE is friendlier when delete races a reader.
        client.query(
            f"CREATE OR REPLACE VIEW `{dst}` AS {view_sql}"
        ).result()
    _clear_table_expiration(client, dst)


def clone_dataset(
    client: bigquery.Client,
    *,
    src: str,
    dst: str,
    dry_run: bool = False,
) -> dict:
    """Copy ``src`` tables into ``dst`` and recreate views with rewritten refs.

    Returns a summary dict: ``copied``, ``views``, ``skipped``, ``failed``.
    """
    dst = assert_safe_dest(dst)
    if src == dst:
        raise ValueError(f"source and destination are the same dataset ({src})")

    _ensure_dest_dataset(client, dst)

    items = list(client.list_tables(f"{PROJECT}.{src}"))
    to_copy = []
    to_view = []
    skipped = []
    for item in items:
        ttype = (item.table_type or "TABLE").upper()
        name = item.table_id
        if ttype in SKIP_TYPES:
            skipped.append(f"{name} ({ttype})")
            continue
        if ttype in VIEW_TYPES:
            to_view.append(name)
            continue
        if ttype in COPYABLE_TYPES or ttype == "TABLE":
            to_copy.append(name)
            continue
        skipped.append(f"{name} ({ttype})")

    summary = {
        "copied": [],
        "views": [],
        "skipped": skipped,
        "failed": [],
    }

    if dry_run:
        print(f"  DRY RUN: would copy {len(to_copy)} tables, rewrite "
              f"{len(to_view)} views, skip {len(skipped)}")
        for name in to_copy:
            print(f"    TABLE {name}")
        for name in to_view:
            print(f"    VIEW  {name}")
        for name in skipped:
            print(f"    SKIP  {name}")
        return summary

    print(f"  copying {len(to_copy)} tables {src} -> {dst}")
    with ThreadPoolExecutor(max_workers=_MAX_COPY_WORKERS) as pool:
        futs = {
            pool.submit(
                _copy_one,
                client,
                f"{PROJECT}.{src}.{name}",
                f"{PROJECT}.{dst}.{name}",
            ): name
            for name in to_copy
        }
        for fut in as_completed(futs):
            name = futs[fut]
            try:
                msg = fut.result()
                print(f"    {msg}")
                summary["copied"].append(name)
            except Exception as exc:  # noqa: BLE001
                print(f"    FAIL {name}: {exc}", file=sys.stderr)
                summary["failed"].append(name)

    # Views may reference other views; retry until the set converges or we
    # give up. BigQuery validates view SQL at CREATE time.
    pending = list(to_view)
    last_errors: dict[str, Exception] = {}
    for attempt in range(1, _VIEW_RETRIES + 1):
        if not pending:
            break
        still = []
        last_errors = {}
        for name in pending:
            src_id = f"{PROJECT}.{src}.{name}"
            dst_id = f"{PROJECT}.{dst}.{name}"
            try:
                src_table = client.get_table(src_id)
                view_sql = rewrite_dataset_refs(
                    src_table.view_query or "", src, dst
                )
                _create_view(client, dst_id, view_sql, src_table.description)
                print(f"    view {name}")
                summary["views"].append(name)
            except Exception as exc:  # noqa: BLE001
                still.append(name)
                last_errors[name] = exc
        pending = still
        if pending and attempt < _VIEW_RETRIES:
            time.sleep(1)

    for name in pending:
        print(f"    FAIL view {name}: {last_errors.get(name)}", file=sys.stderr)
        summary["failed"].append(name)

    return summary


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Clone prod analytics into analytics_dev (never writes prod)."
    )
    parser.add_argument(
        "--dest",
        default=os.environ.get("BQ_DATASET") or DEV_DATASET,
        help=f"Destination dataset (default: $BQ_DATASET or {DEV_DATASET})",
    )
    parser.add_argument(
        "--source",
        default=PROD_DATASET,
        help=f"Source dataset (default: {PROD_DATASET})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be copied; write nothing.",
    )
    args = parser.parse_args(argv)

    try:
        dest = assert_safe_dest(args.dest)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(f"==> Cloning `{PROJECT}.{args.source}` -> `{PROJECT}.{dest}`")
    client = _client()
    summary = clone_dataset(
        client, src=args.source, dst=dest, dry_run=args.dry_run
    )
    n_fail = len(summary["failed"])
    print(
        f"==> Done. copied={len(summary['copied'])} views={len(summary['views'])} "
        f"skipped={len(summary['skipped'])} failed={n_fail}"
    )
    if n_fail:
        print(
            "    Some objects failed — analytics_dev may be a partial mirror. "
            "Re-run, or fall back to ./scripts/dev-refresh.sh for a full rebuild.",
            file=sys.stderr,
        )
        return 1
    if not args.dry_run:
        print(
            "    Local app (BQ_DATASET=analytics_dev) now reads this clone. "
            "Use ./scripts/dev-refresh.sh only when testing dbt model changes."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
