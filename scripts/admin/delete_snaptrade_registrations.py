"""Delete SnapTrade registrations (connections + accounts) for one or
more user_ids.

Two tables hold the SnapTrade state for a user:
  * ``snaptrade_connections`` — per-user (snaptrade_user_id,
    snaptrade_secret) pair, the credentials SnapTrade issues when we
    register the user against our (CLIENT_ID, CONSUMER_KEY).
  * ``snaptrade_accounts`` — per-(user, brokerage_account) row,
    populated as the user links each downstream broker (Robinhood,
    Coinbase, Fidelity, ...).

Both must be removed for a clean re-registration. They share the same
``user_id FK → users.id ON DELETE CASCADE`` pattern, but no FK between
each other — deletion order doesn't matter.

When to use:
  * Sandbox → production credential swap. SnapTrade issues a
    ``snaptrade_user_id`` against the (CLIENT_ID, CONSUMER_KEY) pair
    that registered the user. Switching to a different consumer key
    (e.g. moving from sandbox to production) orphans every existing
    registration — those user_ids won't authenticate against the new
    key. Best path is delete the rows and have affected users re-link
    via /snaptrade/connect under the new credentials.
  * User reports their broker connection is permanently broken and
    they want a fresh start.

What it does NOT touch:
  * The Postgres ``users`` row itself.
  * Schwab connections (different table; see
    ``scripts/admin/delete_schwab_connections.py``).
  * Historical seed CSVs / BigQuery marts. Those are append-only and
    reattributed by the canonical-uid CTE in ``stg_history``.

Defaults to dry-run. Pass ``--apply`` to actually DELETE.

Usage:
    DATABASE_URL=postgresql://... python -m scripts.admin.delete_snaptrade_registrations 9 13
    DATABASE_URL=postgresql://... python -m scripts.admin.delete_snaptrade_registrations 9 13 --apply

Exit codes:
  0  — dry-run completed, OR apply succeeded.
  1  — no DATABASE_URL configured, or no rows match the supplied IDs.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import List


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Delete snaptrade_connections + snaptrade_accounts rows "
            "for one or more user_ids."
        )
    )
    p.add_argument(
        "user_ids",
        nargs="+",
        type=int,
        help="One or more numeric user_id values to delete SnapTrade "
        "registrations for.",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually run the DELETEs. Without this flag, only prints "
        "what would be deleted (dry-run, default).",
    )
    p.add_argument(
        "--db-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL. Defaults to env var DATABASE_URL.",
    )
    return p.parse_args()


def _print_table(rows: list, header: str) -> None:
    print(f"\n{header} ({len(rows)} row{'s' if len(rows) != 1 else ''}):\n")
    if not rows:
        return
    cols = list(rows[0].keys())
    widths = {c: max(len(c), max(len(str(r[c])) for r in rows)) for c in cols}
    fmt = "  " + "  ".join(f"{{:<{widths[c]}}}" for c in cols)
    print(fmt.format(*cols))
    print(fmt.format(*("-" * widths[c] for c in cols)))
    for r in rows:
        print(fmt.format(*(str(r[c]) if r[c] is not None else "" for c in cols)))


def _select_connections(cur, user_ids: List[int]) -> list:
    cur.execute(
        "SELECT user_id, snaptrade_user_id, created_at "
        "FROM snaptrade_connections "
        "WHERE user_id = ANY(%s) "
        "ORDER BY user_id",
        (user_ids,),
    )
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _select_accounts(cur, user_ids: List[int]) -> list:
    cur.execute(
        "SELECT user_id, account_name, brokerage_authorization_id, "
        "snaptrade_account_id, created_at "
        "FROM snaptrade_accounts "
        "WHERE user_id = ANY(%s) "
        "ORDER BY user_id, account_name",
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
            connections = _select_connections(cur, args.user_ids)
            accounts = _select_accounts(cur, args.user_ids)

            _print_table(
                connections,
                f"snaptrade_connections matching user_ids={args.user_ids}",
            )
            _print_table(
                accounts,
                f"snaptrade_accounts matching user_ids={args.user_ids}",
            )

            if not connections and not accounts:
                print(
                    "\nNo matching rows in either table. Nothing to do.",
                    file=sys.stderr,
                )
                return 1

            if not args.apply:
                print(
                    f"\nDRY-RUN: pass --apply to delete the "
                    f"{len(connections)} connection row(s) and "
                    f"{len(accounts)} account row(s) above.\n"
                )
                return 0

            cur.execute(
                "DELETE FROM snaptrade_accounts WHERE user_id = ANY(%s)",
                (args.user_ids,),
            )
            accounts_deleted = cur.rowcount

            cur.execute(
                "DELETE FROM snaptrade_connections WHERE user_id = ANY(%s)",
                (args.user_ids,),
            )
            connections_deleted = cur.rowcount

            conn.commit()

            print(
                f"\nDeleted {connections_deleted} snaptrade_connections row(s) "
                f"and {accounts_deleted} snaptrade_accounts row(s)."
            )
            print(
                "\nAffected users can now re-register via /snaptrade/connect "
                "and re-link each downstream broker."
            )
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
