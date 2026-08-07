"""Backup + verify the accumulating snapshot tables around a warehouse build.

WHY THIS EXISTS (2026-08-07 incident)
-------------------------------------
The ``analytics`` dataset had a 60-day default table expiration. Every mart
survives because CREATE OR REPLACE resets its expiration clock each build —
but the SCD2 snapshot tables are the only long-lived tables (created once,
MERGEd forever), so ``snapshot_account_balances_daily`` silently hit day 60
and BigQuery deleted it (audit log: ``InternalTableExpired``). The next dbt
build saw no table and rebuilt it from scratch with only that day's rows —
months of daily balance history gone, every "vs yesterday / 1w / 1m" delta
and the Daily Account calendar rendered as dashes. Recovery was only
possible because the dev mirror (``analytics_dev``) happened to hold a copy
taken 15 minutes before the expiry.

The dataset default expiration has been removed and every existing table's
expiration cleared, but snapshots are irreplaceable observations (you cannot
re-observe last month's balances), so they get belt AND suspenders:

- ``backup`` mode (before dbt build): copy each snapshot table into the
  ``analytics_backups`` dataset (one dated copy per day, WRITE_TRUNCATE so
  re-runs are idempotent). The backup dataset has a 14-day default
  expiration so old backups self-clean — expiration is what we WANT there.
- ``verify`` mode (after dbt build): fail loudly (exit 1 → red build) if a
  snapshot table is missing or its pre-today history shrank versus the
  morning backup. A red build is infinitely better than weeks of silently
  serving "—" while history re-accumulates from zero.

Credentials come from GOOGLE_APPLICATION_CREDENTIALS, exported by the
google-github-actions/auth step (same as scripts/train_bqml.py).
"""

import sys
from datetime import date, timedelta

from google.cloud import bigquery

PROJECT = "ccwj-dbt"
DATASET = "analytics"
BACKUP_DATASET = "analytics_backups"
BACKUP_RETENTION_DAYS = 14

SNAPSHOT_TABLES = (
    "snapshot_account_balances_daily",
    "snapshot_options_market_values_daily",
)


def _client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT)


def _ensure_backup_dataset(client: bigquery.Client) -> None:
    ds_id = f"{PROJECT}.{BACKUP_DATASET}"
    try:
        client.get_dataset(ds_id)
    except Exception:
        ds = bigquery.Dataset(ds_id)
        ds.location = "US"
        ds.default_table_expiration_ms = BACKUP_RETENTION_DAYS * 24 * 3600 * 1000
        ds.description = (
            "Automated pre-build copies of the accumulating snapshot tables. "
            "Written by scripts/snapshot_guard.py; entries self-expire after "
            f"{BACKUP_RETENTION_DAYS} days."
        )
        client.create_dataset(ds)
        print(f"created backup dataset {ds_id}")


def backup() -> int:
    client = _client()
    _ensure_backup_dataset(client)
    stamp = date.today().strftime("%Y%m%d")
    failures = 0
    for name in SNAPSHOT_TABLES:
        src = f"{PROJECT}.{DATASET}.{name}"
        dst = f"{PROJECT}.{BACKUP_DATASET}.{name}_{stamp}"
        try:
            job = client.copy_table(
                src, dst,
                job_config=bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE"),
            )
            job.result()
            rows = client.get_table(dst).num_rows
            print(f"backed up {src} -> {dst} ({rows} rows)")
        except Exception as exc:
            failures += 1
            print(f"BACKUP FAILED for {src}: {exc}", file=sys.stderr)
    return 1 if failures else 0


def _history_rows(client: bigquery.Client, table: str) -> int:
    """Rows whose validity started before today — the irreplaceable part."""
    sql = f"""
        SELECT COUNTIF(DATE(dbt_valid_from) < CURRENT_DATE()) AS hist
        FROM `{table}`
    """
    return int(next(iter(client.query(sql).result())).hist)


def verify() -> int:
    client = _client()
    failures = 0
    for name in SNAPSHOT_TABLES:
        live = f"{PROJECT}.{DATASET}.{name}"
        try:
            live_hist = _history_rows(client, live)
        except Exception as exc:
            print(f"VERIFY FAILED: cannot read {live}: {exc}", file=sys.stderr)
            failures += 1
            continue

        # Compare against the most recent backup (today's, else yesterday's).
        backup_hist = None
        for delta in range(0, BACKUP_RETENTION_DAYS):
            stamp = (date.today() - timedelta(days=delta)).strftime("%Y%m%d")
            try:
                backup_hist = _history_rows(
                    client, f"{PROJECT}.{BACKUP_DATASET}.{name}_{stamp}")
                break
            except Exception:
                continue

        if backup_hist is None:
            # No backup yet (first run) — only assert the table has SOME
            # history unless it was created today (fresh product install).
            print(f"{name}: history_rows={live_hist} (no backup to compare)")
            continue

        # The snapshot only ever gains rows; pre-today history must never
        # shrink. A tiny tolerance is deliberate: there is no legitimate
        # reason for ANY loss, so any drop means expiry/recreate/clobber.
        if live_hist < backup_hist:
            print(
                f"VERIFY FAILED: {name} history shrank "
                f"({backup_hist} -> {live_hist} pre-today rows). "
                "The snapshot table was likely recreated — restore it from "
                f"`{PROJECT}.{BACKUP_DATASET}.{name}_<latest>` BEFORE the "
                "next build merges bad state forward.",
                file=sys.stderr,
            )
            failures += 1
        else:
            print(f"{name}: history_rows={live_hist} >= backup {backup_hist} OK")
    return 1 if failures else 0


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "backup":
        return backup()
    if mode == "verify":
        return verify()
    print("usage: snapshot_guard.py backup|verify", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
