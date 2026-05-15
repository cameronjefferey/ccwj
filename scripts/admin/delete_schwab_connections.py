"""Delete rows from ``schwab_connections`` for one or more user_ids.

Use this when the Schwab refresh tokens for a stale user_id are
permanently dead (Schwab rejects them with ``invalid_grant`` /
``unsupported_token_type``) and the canonical owner has already
re-linked the same accounts under a fresh user_id. The dead rows
otherwise stay in Postgres forever and the daily cron logs ``Sync
failed`` for each one on every run, polluting the log without
actually breaking anything.

This is the **canonical-uid migration cleanup** referenced in
``.cursor/rules/position-detail-orphan-tenancy-reconciliation.mdc``:
when a user re-imports / re-numbers (e.g. user_id=7 → user_id=9 →
user_id=13 for the same physical Schwab accounts), the historical
trade rows in ``stg_history`` get re-attributed by the
``canonical_account_owner`` CTE so marts only show one canonical
view. The OAuth rows in Postgres ``schwab_connections``, however,
have no such backfill — they're keyed on ``(user_id,
account_number)`` and the dead generations linger. This script
removes them.

Safe to run because:
  * Only ``schwab_connections.user_id`` references ``users.id`` (FK),
    not the other direction. No cascade fanout.
  * Historical seed CSVs and BigQuery marts are untouched — those
    are append-only and re-attributed by the canonical-uid CTE in
    ``dbt/models/staging/stg_history.sql``.
  * Defaults to dry-run. Pass ``--apply`` to actually DELETE.

Usage:
    # Dry-run (default) — prints what would be deleted, changes nothing.
    DATABASE_URL=postgresql://... python -m scripts.admin.delete_schwab_connections 7 9

    # Apply.
    DATABASE_URL=postgresql://... python -m scripts.admin.delete_schwab_connections 7 9 --apply

Exit codes:
  0  — dry-run completed, OR apply succeeded.
  1  — no DATABASE_URL configured, or no rows match the supplied IDs.
  2  — apply ran but 0 rows were deleted (mismatch between dry-run
       count and apply count, treated as defensive failure).
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import List


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Delete schwab_connections rows for one or more user_ids."
    )
    p.add_argument(
        "user_ids",
        nargs="+",
        type=int,
        help="One or more numeric user_id values to delete connections for.",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually run the DELETE. Without this flag, only prints what "
        "would be deleted (dry-run, default).",
    )
    p.add_argument(
        "--db-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL. Defaults to env var DATABASE_URL.",
    )
    return p.parse_args()


def _print_rows(rows: list, header: str) -> None:
    print(f"\n{header} ({len(rows)} row{'s' if len(rows) != 1 else ''}):\n")
    if not rows:
        return
    width_uid = max(len("user_id"), max(len(str(r["user_id"])) for r in rows))
    width_acct = max(
        len("account_number"),
        max(len(str(r["account_number"])) for r in rows),
    )
    width_label = max(
        len("label"),
        max(
            len(str(r["display_nickname"] or r["account_name"] or ""))
            for r in rows
        ),
    )
    fmt = (
        f"  {{:>{width_uid}}}  {{:<{width_acct}}}  {{:<{width_label}}}  "
        f"{{:<26}}  {{:<26}}"
    )
    print(
        fmt.format(
            "user_id", "account_number", "label", "created_at",
            "refresh_token_invalid_at",
        )
    )
    print(
        fmt.format(
            "-" * width_uid, "-" * width_acct, "-" * width_label,
            "-" * 26, "-" * 26,
        )
    )
    for r in rows:
        label = r["display_nickname"] or r["account_name"] or ""
        invalid = (
            r["refresh_token_invalid_at"].isoformat()
            if r["refresh_token_invalid_at"]
            else "(never flagged)"
        )
        print(
            fmt.format(
                r["user_id"],
                r["account_number"],
                label,
                r["created_at"].isoformat() if r["created_at"] else "",
                invalid,
            )
        )


def _select_rows(cur, user_ids: List[int]) -> list:
    cur.execute(
        "SELECT user_id, account_number, account_name, display_nickname, "
        "created_at, refresh_token_invalid_at "
        "FROM schwab_connections "
        "WHERE user_id = ANY(%s) "
        "ORDER BY user_id, account_number",
        (user_ids,),
    )
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def main() -> int:
    args = _parse_args()
    db_url = args.db_url
    if not db_url:
        print(
            "ERROR: no DATABASE_URL set. Pass --db-url or export DATABASE_URL.",
            file=sys.stderr,
        )
        return 1

    import psycopg

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            rows = _select_rows(cur, args.user_ids)
            _print_rows(
                rows,
                f"Matching schwab_connections for user_ids={args.user_ids}",
            )
            if not rows:
                print(
                    "\nNo matching rows. Nothing to do.",
                    file=sys.stderr,
                )
                return 1

            if not args.apply:
                print(
                    f"\nDRY-RUN: pass --apply to delete the {len(rows)} row(s) "
                    "above.\n"
                )
                return 0

            cur.execute(
                "DELETE FROM schwab_connections WHERE user_id = ANY(%s)",
                (args.user_ids,),
            )
            deleted = cur.rowcount
            conn.commit()

            print(f"\nDeleted {deleted} row(s).")
            if deleted == 0:
                print(
                    "WARNING: dry-run found rows but apply deleted 0 — "
                    "likely a race or transaction issue.",
                    file=sys.stderr,
                )
                return 2
            if deleted != len(rows):
                print(
                    f"WARNING: dry-run showed {len(rows)} but apply "
                    f"deleted {deleted}. New rows may have appeared "
                    "between SELECT and DELETE.",
                    file=sys.stderr,
                )
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
