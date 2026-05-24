#!/usr/bin/env python3
"""v2 SnapTrade-only cutover helper — seed truncate + Postgres cleanup guide.

This script supports Phase 6 production cutover. It does NOT auto-drop production
tables unless you pass ``--apply`` (and even then, only on explicit flags).

Usage (local / staging dry-run):
    HAPPYTRADER_SKIP_DB_INIT=1 python scripts/admin/v2_cutover_reset.py --truncate-seeds

Usage (apply seed truncate):
    python scripts/admin/v2_cutover_reset.py --truncate-seeds --apply

Usage (Postgres legacy table drop — requires DATABASE_URL):
    python scripts/admin/v2_cutover_reset.py --drop-legacy-tables --apply

See module docstring below for the full operator runbook.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SEED_DIR = REPO_ROOT / "dbt" / "seeds"

SEED_FILES = (
    "trade_history.csv",
    "current_positions.csv",
    "account_balances.csv",
    "demo_history.csv",
    "demo_current.csv",
)

# Columns preserved when truncating to header-only (v2 shape).
EXPECTED_HEADERS = {
    "trade_history.csv": [
        "account", "user_id", "tenant_id", "Date", "Action", "Symbol",
        "Description", "Quantity", "Price", "Fees & Comm", "Amount",
        "security_type", "option_expiry", "option_strike", "option_type",
    ],
    "current_positions.csv": [
        "account", "user_id", "tenant_id", "Symbol", "Description",
        "Quantity", "Price", "Price Change %", "Price Change $",
        "Market Value", "Day Change $", "Day Change %", "Cost Basis",
        "Gain/Loss $", "Gain/Loss %", "Reinvest?", "Reinvest Capital Gains?",
        "security_type", "option_expiry", "option_strike", "option_type",
        "Cost Per Share", "% of Account",
    ],
    "account_balances.csv": [
        "account", "user_id", "tenant_id", "row_type", "label", "amount",
    ],
}

LEGACY_TABLES = (
    "schwab_connections",
    "broker_accounts",
    "snaptrade_accounts",
    "snaptrade_connections",  # renamed to snaptrade_users in v2; drop old name if exists
)


def _read_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as f:
        row = next(csv.reader(f), None)
    return row or []


def truncate_seeds(*, apply: bool) -> int:
    """Rewrite seed CSVs to header-only rows."""
    changed = 0
    for name in SEED_FILES:
        path = SEED_DIR / name
        if not path.exists():
            print(f"SKIP (missing): {path}")
            continue
        header = _read_header(path)
        expected = EXPECTED_HEADERS.get(name)
        if expected and header != expected:
            print(f"WARN: {name} header differs from v2 expected shape")
            print(f"  on disk:  {header}")
            print(f"  expected: {expected}")
        if not apply:
            print(f"DRY-RUN truncate: {path} ({len(header)} columns, header-only)")
            changed += 1
            continue
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
        print(f"TRUNCATED: {path}")
        changed += 1
    return changed


def drop_legacy_tables(*, apply: bool) -> None:
    if not apply:
        print("DRY-RUN: would DROP TABLE IF EXISTS for:", ", ".join(LEGACY_TABLES))
        return
    os.environ.setdefault("HAPPYTRADER_SKIP_DB_INIT", "1")
    sys.path.insert(0, str(REPO_ROOT))
    from app.db import execute

    for table in LEGACY_TABLES:
        execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        print(f"DROPPED: {table}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--truncate-seeds", action="store_true", help="Rewrite seed CSVs to header-only")
    parser.add_argument("--drop-legacy-tables", action="store_true", help="DROP v1 Postgres tables")
    parser.add_argument("--apply", action="store_true", help="Execute writes (default is dry-run)")
    args = parser.parse_args()

    if not args.truncate_seeds and not args.drop_legacy_tables:
        parser.print_help()
        print(
            "\n"
            "═══════════════════════════════════════════════════════════════════\n"
            "Phase 6 production cutover — operator runbook\n"
            "═══════════════════════════════════════════════════════════════════\n"
            "\n"
            "Prerequisites:\n"
            "  • Phase 0 signed off (docs/PHASE0_SNAPTRADE_VALIDATION.md)\n"
            "  • Code through Phase 7 deployed (direct Schwab deleted)\n"
            "  • Users notified: historical data will not backfill\n"
            "\n"
            "Ordered steps:\n"
            "\n"
            "  1. MAINTENANCE — put app in maintenance or accept empty dashboards briefly.\n"
            "\n"
            "  2. TRUNCATE SEEDS (this repo → GitHub → BigQuery via Actions):\n"
            "       python scripts/admin/v2_cutover_reset.py --truncate-seeds --apply\n"
            "       git add dbt/seeds/*.csv && git commit && git push\n"
            "       Wait for bigquery_update.yml to finish (empty marts are expected).\n"
            "\n"
            "  3. POSTGRES — each live user disconnects old connectors and re-auths SnapTrade:\n"
            "       Profile → Accounts & data → Connect a broker\n"
            "       (Native Schwab OAuth no longer exists; SnapTrade only.)\n"
            "\n"
            "  4. DROP LEGACY TABLES (after all users re-linked):\n"
            "       DATABASE_URL=... python scripts/admin/v2_cutover_reset.py \\\n"
            "         --drop-legacy-tables --apply\n"
            "     Tables: schwab_connections, broker_accounts, snaptrade_accounts,\n"
            "             snaptrade_connections (if still present).\n"
            "     v2 tables broker_tenants + snaptrade_users remain.\n"
            "\n"
            "  5. RENDER — delete retired Schwab cron services in dashboard:\n"
            "       happy-trader-schwab-sync-close, happytrader-schwab-sync\n"
            "     Blueprint now only declares happytrader-snaptrade-sync.\n"
            "     Remove SCHWAB_APP_KEY / SCHWAB_APP_SECRET from web + cron env.\n"
            "\n"
            "  6. FIRST SYNC — each user: Sync all brokerages (full history toggle on first link).\n"
            "       Verify tenant_id populated: dbt test --select every_seed_row_has_tenant_id\n"
            "\n"
            "  7. SMOKE — Daily Review + Position Detail for Schwab + one dividend ETF + one option.\n"
            "\n"
            "Rollback is NOT supported: pre-v2 seed data is intentionally discarded.\n"
            "═══════════════════════════════════════════════════════════════════\n"
        )
        return 0

    if args.truncate_seeds:
        n = truncate_seeds(apply=args.apply)
        print(f"Seeds processed: {n}")
    if args.drop_legacy_tables:
        drop_legacy_tables(apply=args.apply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
