"""Deregister ORPHANED SnapTrade users — registrations that exist on
SnapTrade's side but no longer map to any row in our ``snaptrade_connections``.

Why this exists
---------------
We register each HappyTrader user with SnapTrade under the label
``happytrader-<user_id>`` and store the issued ``(snaptrade_user_id,
snaptrade_secret)`` in ``snaptrade_connections``. If a HappyTrader user is
later deleted (or re-registered under a different label), the SnapTrade-side
user can be left behind. That orphan:

  * keeps firing ``ACCOUNT_HOLDINGS_UPDATED`` webhooks we can only DROP
    ("no HappyTrader user for SnapTrade userId=..."), and
  * counts as an active per-user registration on our SnapTrade bill.

The sibling ``delete_snaptrade_registrations.py`` deletes DB rows for a KNOWN
``user_id`` — it cannot help here, because an orphan has NO DB row (that IS the
orphan condition). This script diffs SnapTrade's OWN user list against our
``snaptrade_connections`` and deregisters the strays.

Real case (2026-07-09): a webhook arrived for ``happytrader-8`` while user 8
had no ``users`` row and no ``snaptrade_connections`` row — a leftover from a
deleted test user.

Safety
------
  * Dry-run by DEFAULT. Prints the orphan list; changes nothing.
  * ``--apply`` performs the (irreversible on SnapTrade's side) delete.
  * Only deletes SnapTrade users that are ABSENT from ``snaptrade_connections``.
    A user we still have credentials for is NEVER touched.
  * Optional ``--only happytrader-8 [happytrader-3 ...]`` restricts the action
    to an explicit allow-list (intersected with the computed orphan set), so a
    one-off cleanup can't accidentally sweep more than intended.

Requires (same env the app uses):
  * ``SNAPTRADE_CLIENT_ID`` + ``SNAPTRADE_CONSUMER_KEY``
  * ``DATABASE_URL`` (points at the SAME Postgres whose connections define
    "known"). Run it in the environment whose registrations you're cleaning —
    prod env for prod SnapTrade users.

Usage:
    # Dry-run (safe): list every orphan
    SNAPTRADE_CLIENT_ID=... SNAPTRADE_CONSUMER_KEY=... DATABASE_URL=... \
        python -m scripts.admin.deregister_orphan_snaptrade_users

    # Delete exactly one known orphan
    ... python -m scripts.admin.deregister_orphan_snaptrade_users \
        --only happytrader-8 --apply

    # Delete ALL computed orphans
    ... python -m scripts.admin.deregister_orphan_snaptrade_users --apply

Exit codes:
  0 — dry-run completed, OR apply succeeded (even if 0 orphans).
  1 — missing config (SnapTrade creds / DATABASE_URL) or an API/DB error.
"""
from __future__ import annotations

import argparse
import os
import sys


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Deregister SnapTrade users with no snaptrade_connections row."
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually deregister the orphans. Without this flag, only prints "
        "what would be deleted (dry-run, default).",
    )
    p.add_argument(
        "--only",
        nargs="+",
        default=None,
        metavar="SNAPTRADE_USER_ID",
        help="Restrict the action to these SnapTrade user id(s) (intersected "
        "with the computed orphan set). Safest for a one-off cleanup.",
    )
    p.add_argument(
        "--db-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL. Defaults to env var DATABASE_URL.",
    )
    return p.parse_args()


def _known_snaptrade_user_ids(db_url: str) -> set[str]:
    """The set of SnapTrade user ids we still hold credentials for."""
    import psycopg

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT snaptrade_user_id FROM snaptrade_connections")
            return {str(r[0]) for r in cur.fetchall() if r[0]}


def _list_snaptrade_user_ids(client) -> list[str]:
    """All user ids SnapTrade has registered under our (client_id,
    consumer_key). The SDK returns a bare JSON list of id strings."""
    resp = client.authentication.list_snap_trade_users()
    body = getattr(resp, "body", resp)
    if isinstance(body, dict):  # defensive — some SDK versions wrap it
        body = body.get("users", body.get("data", []))
    return [str(u) for u in (body or [])]


def main() -> int:
    args = _parse_args()

    client_id = os.environ.get("SNAPTRADE_CLIENT_ID", "").strip()
    consumer_key = os.environ.get("SNAPTRADE_CONSUMER_KEY", "").strip()
    if not client_id or not consumer_key:
        print(
            "ERROR: SNAPTRADE_CLIENT_ID and SNAPTRADE_CONSUMER_KEY must be set.",
            file=sys.stderr,
        )
        return 1
    if not args.db_url:
        print(
            "ERROR: no DATABASE_URL set. Pass --db-url or export DATABASE_URL.",
            file=sys.stderr,
        )
        return 1

    try:
        from snaptrade_client import SnapTrade
    except ImportError:
        print("ERROR: snaptrade_client not installed.", file=sys.stderr)
        return 1

    client = SnapTrade(consumer_key=consumer_key, client_id=client_id)

    try:
        registered = _list_snaptrade_user_ids(client)
    except Exception as exc:
        print(f"ERROR: could not list SnapTrade users: {exc}", file=sys.stderr)
        return 1

    try:
        known = _known_snaptrade_user_ids(args.db_url)
    except Exception as exc:
        print(f"ERROR: could not read snaptrade_connections: {exc}", file=sys.stderr)
        return 1

    orphans = sorted(set(registered) - known)

    if args.only:
        allow = set(args.only)
        skipped = allow - set(orphans)
        orphans = [o for o in orphans if o in allow]
        if skipped:
            print(
                "NOTE: ignoring --only ids that are NOT orphans (still have a "
                f"connection row): {sorted(skipped)}"
            )

    print(
        f"\nSnapTrade registered users: {len(registered)}   "
        f"known (in DB): {len(known)}   orphans to remove: {len(orphans)}\n"
    )
    for uid in orphans:
        print(f"  ORPHAN  {uid}")
    if not orphans:
        print("  (none)")
        return 0

    if not args.apply:
        print(
            f"\nDRY-RUN: pass --apply to deregister the {len(orphans)} orphan(s) "
            "above on SnapTrade. This is irreversible."
        )
        return 0

    deleted = 0
    failed = 0
    for uid in orphans:
        try:
            client.authentication.delete_snap_trade_user(user_id=uid)
            print(f"  deleted {uid}")
            deleted += 1
        except Exception as exc:
            print(f"  FAILED  {uid}: {exc}", file=sys.stderr)
            failed += 1

    print(f"\nDeregistered {deleted} orphan(s); {failed} failed.")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
