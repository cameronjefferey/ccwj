"""Backfill ``user_id`` into the committed dbt seed CSVs.

Stage 1 of the user_id-tenancy migration (see ``docs/USER_ID_TENANCY.md``).

The seed CSVs in ``dbt/seeds/`` carry an empty ``user_id`` column for
every row written before the migration. Now that Postgres knows
which ``user_id`` owns each ``account_name`` (via ``user_accounts``),
we can populate the column for legacy rows so the Stage 0/1 NULL
leniency leg in ``_user_scoped_filter`` / ``_filter_df_by_user`` can
eventually be removed.

What the script does:
  1. Connect to Postgres using ``DATABASE_URL`` (or ``--db-url``).
  2. Load ``user_accounts`` → build ``{lower(trim(account_name)) -> user_id}``.
  3. Detect duplicates (i.e. the ``investment1`` cross-tenant collision)
     and **refuse to backfill** any duplicated label. Those rows stay
     NULL until the cross-tenant guard quarantines one of the
     conflicting links. Backfilling them would put **the wrong user_id
     on the wrong rows** — exactly the leak this whole migration is
     designed to prevent.
  4. Rewrite ``dbt/seeds/{trade_history,current_positions,
     schwab_account_balances}.csv`` in place, populating the
     ``user_id`` column where it is currently empty and the
     ``account_name`` resolves to a single owner.
  5. Print a summary (rows backfilled / left NULL / skipped due to
     conflict) and exit non-zero if any conflict was hit (so CI can
     surface it).

The script is idempotent — re-running it on already-backfilled CSVs
is a no-op.

Usage:
    python -m scripts.backfill_seed_user_ids [--dry-run] [--db-url URL]
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, Tuple


SEED_DIR = Path(__file__).resolve().parent.parent / "dbt" / "seeds"

SEED_FILES = [
    SEED_DIR / "trade_history.csv",
    SEED_DIR / "current_positions.csv",
    SEED_DIR / "schwab_account_balances.csv",
]

ACCOUNT_COLS = {"Account", "account"}
USER_ID_COLS = {"user_id"}


def _norm(value: str) -> str:
    return (value or "").strip().lower()


def load_account_owners(db_url: str) -> Tuple[Dict[str, int], Counter]:
    """Return ``(unique_owners, conflicts)``.

    ``unique_owners[lower(trim(account_name))] = user_id`` only when the
    label has exactly one owner. Labels claimed by 2+ users go into
    ``conflicts`` and are deliberately omitted from ``unique_owners`` so
    we never put the wrong ``user_id`` on the wrong row.
    """
    import psycopg

    owners: Dict[str, set] = defaultdict(set)
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, lower(trim(account_name)) "
                "FROM user_accounts WHERE account_name IS NOT NULL"
            )
            for user_id, label in cur.fetchall():
                if not label:
                    continue
                owners[label].add(int(user_id))

    unique: Dict[str, int] = {}
    conflicts: Counter = Counter()
    for label, ids in owners.items():
        if len(ids) == 1:
            unique[label] = next(iter(ids))
        else:
            conflicts[label] = len(ids)
    return unique, conflicts


def _detect_account_col(fieldnames: Iterable[str]) -> str:
    for name in fieldnames:
        if name in ACCOUNT_COLS:
            return name
    raise SystemExit(f"No Account column in CSV header: {list(fieldnames)}")


def backfill_csv(
    path: Path,
    owners: Dict[str, int],
    *,
    dry_run: bool,
) -> Tuple[int, int, int]:
    """Returns (rows_backfilled, rows_left_null, rows_already_set)."""
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if "user_id" not in fieldnames:
            raise SystemExit(
                f"{path} is missing the 'user_id' column. Run the seed "
                f"schema migration first (Stage 0) before this script."
            )
        account_col = _detect_account_col(fieldnames)
        rows = list(reader)

    backfilled = already = left_null = 0
    for row in rows:
        existing = (row.get("user_id") or "").strip()
        if existing:
            already += 1
            continue
        label = _norm(row.get(account_col, ""))
        owner = owners.get(label)
        if owner is None:
            left_null += 1
            continue
        row["user_id"] = str(owner)
        backfilled += 1

    if not dry_run and backfilled:
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return backfilled, left_null, already


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres URL (defaults to DATABASE_URL env var).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't rewrite files; just print what would change.",
    )
    args = parser.parse_args()

    if not args.db_url:
        print(
            "DATABASE_URL is not set and --db-url was not passed.",
            file=sys.stderr,
        )
        return 2

    owners, conflicts = load_account_owners(args.db_url)
    print(f"Loaded {len(owners)} uniquely-owned account labels.")
    if conflicts:
        print(
            f"WARNING: {len(conflicts)} account label(s) are claimed by "
            f"multiple users — these will NOT be backfilled (rows stay "
            f"NULL until the cross-tenant guard quarantines one side):"
        )
        for label, n in conflicts.most_common():
            print(f"  - {label!r}  (claimed by {n} users)")

    total_back = total_null = total_already = 0
    for path in SEED_FILES:
        if not path.exists():
            print(f"  SKIP (missing): {path}")
            continue
        b, n, a = backfill_csv(path, owners, dry_run=args.dry_run)
        total_back += b
        total_null += n
        total_already += a
        print(
            f"  {path.name}: backfilled={b}  left_null={n}  "
            f"already_set={a}"
        )

    print(
        f"\nDone. backfilled={total_back}  left_null={total_null}  "
        f"already_set={total_already}  "
        f"{'(dry-run, no files rewritten)' if args.dry_run else ''}"
    )

    # Exit non-zero if conflicts exist so CI can surface the issue.
    return 1 if conflicts else 0


if __name__ == "__main__":
    raise SystemExit(main())
