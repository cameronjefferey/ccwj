"""Backfill ``broker_account_id`` into the committed dbt seed CSVs.

Stage 1 of the broker-account-id migration — see
``docs/BROKER_ACCOUNT_ID_MIGRATION.md``.

Background
----------
Stage 0 made every Stage-0+ writer (Schwab, SnapTrade, manual upload)
stamp ``broker_account_id`` into every emitted seed row. Existing rows
in ``dbt/seeds/{trade_history,current_positions,account_balances}.csv``
that landed BEFORE Stage 0 still have an empty ``broker_account_id``
cell. This script fills them in.

What the script does
--------------------
1. Connects to Postgres using ``DATABASE_URL`` (or ``--db-url``).
2. For each unique ``(account_name, user_id)`` tuple in the seed CSVs:

   a. **Resolve the orphan user_id** (mirrors the ``account_owner`` CTE
      in ``stg_history.sql``): rows with ``user_id = ""`` get rewritten
      to the ONE non-empty ``user_id`` that exists for the same account
      label. Tuples whose account label has 0 or >1 non-empty owners
      stay NULL.

   b. **Infer the broker** from Postgres connections, in priority
      order: ``schwab_connections`` (uses ``account_hash`` or
      ``account_number`` as ``broker_external_id``) → ``snaptrade_accounts``
      (uses ``snaptrade_account_id``) → fallback ``broker_slug='manual'``
      with synthetic ``broker_external_id = f"manual:{account_name}"``
      (same shape the Stage 0 upload route uses for new manual
      uploads).

   c. **Upsert** the corresponding ``broker_accounts`` row and capture
      its ``id``. The upsert is idempotent on
      ``(user_id, broker_slug, broker_external_id)``.

3. Rewrites each seed CSV in place (or prints what it would do under
   ``--dry-run``), populating ``broker_account_id`` for every row
   whose ``(account_name, user_id)`` resolved to a real id. Rows that
   already have a non-empty ``broker_account_id`` are left untouched —
   the script is idempotent.

4. Prints a summary table and exits non-zero if any tuples could not
   be resolved (so CI can surface unresolved orphan accounts).

Idempotency
-----------
Re-running the script after a full apply is a no-op — every row
either already has the right ``broker_account_id`` (and is skipped) or
already maps to the same Postgres broker_accounts row (and resolves to
the same id). The Stage 0 `get_or_create_broker_account` semantics
make the resolution deterministic.

What it does NOT do
-------------------
- It does NOT touch demo seeds (``demo_history.csv``,
  ``demo_current.csv``). Demo data uses ``user_id=""`` and a fake
  account label — Stage 4 will decide how demo participates in the
  new tenancy. For now, demo rows stay NULL.
- It does NOT delete or modify existing populated broker_account_id
  cells. If a row already has one, it's preserved verbatim.
- It does NOT modify the Postgres ``broker_accounts`` table beyond
  the upserts needed to assign ids to the unique tuples found in the
  seeds. Pre-existing ``broker_accounts`` rows (from Stage 0 writers)
  are reused verbatim.

Usage
-----
    # Dry run — print what would change, don't write anything.
    python -m scripts.backfill_seed_broker_account_ids --dry-run

    # Apply — rewrite the seed CSVs in place.
    python -m scripts.backfill_seed_broker_account_ids
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple


SEED_DIR = Path(__file__).resolve().parent.parent / "dbt" / "seeds"

# Only the user-tied seeds get backfilled here. Demo seeds are
# intentionally skipped (see module docstring).
SEED_FILES = [
    SEED_DIR / "trade_history.csv",
    SEED_DIR / "current_positions.csv",
    SEED_DIR / "account_balances.csv",
]

ACCOUNT_COL_CANDIDATES = ("Account", "account")
SCHWAB_SLUG = "schwab"
SNAPTRADE_SLUG = "snaptrade"
MANUAL_SLUG = "manual"


# ---------------------------------------------------------------------------
# Pure helpers — extracted for testability (no DB / no filesystem access).
# ---------------------------------------------------------------------------


def detect_account_col(fieldnames) -> str:
    """Find the account column name in a CSV header."""
    for cand in ACCOUNT_COL_CANDIDATES:
        if cand in fieldnames:
            return cand
    raise ValueError(
        f"No Account column in CSV header (looked for {ACCOUNT_COL_CANDIDATES}): "
        f"{list(fieldnames)}"
    )


def _normalize_uid_cell(value: str) -> str:
    """Collapse the pandas-emitted ``"9.0"`` form back to ``"9"`` and
    treat empty / whitespace / NaN strings as the empty-tenant sentinel.

    Mirrors ``app.upload._normalize_uid`` and the staging models'
    ``safe_cast(safe_cast(... as float64) as int64)`` round-trip so
    the backfill agrees with what dbt will read.
    """
    s = (value or "").strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return ""
    try:
        return str(int(float(s)))
    except (TypeError, ValueError):
        return s


def resolve_orphan_user_ids(
    tuples_with_counts: Dict[Tuple[str, str], int],
) -> Dict[Tuple[str, str], str]:
    """Mirror of the dbt ``account_owner`` CTE in ``stg_history.sql``.

    Input: ``{(account, user_id): row_count}`` where ``user_id`` may be
    the empty string for orphan rows.

    Output: ``{(account, "") -> chosen_user_id}`` for every account
    label that has EXACTLY one non-empty user_id elsewhere in the
    seeds. Labels with zero or multiple non-empty owners are omitted —
    backfilling them is unsafe (we'd guess the wrong tenant).

    NOTE: This is the *seed-side* heuristic. It looks at what the
    warehouse currently sees, not at Postgres. That matches dbt's own
    resolution and keeps the script's behavior auditable without
    requiring DB access for the orphan-collapse decision.
    """
    per_account: Dict[str, set] = defaultdict(set)
    for (acct, uid), _count in tuples_with_counts.items():
        if uid:
            per_account[acct].add(uid)

    out: Dict[Tuple[str, str], str] = {}
    for (acct, uid), _count in tuples_with_counts.items():
        if uid:
            continue  # not an orphan
        owners = per_account.get(acct) or set()
        if len(owners) == 1:
            out[(acct, "")] = next(iter(owners))
    return out


def survey_seeds(paths: List[Path]) -> Dict[Tuple[str, str], int]:
    """Return ``{(account, user_id) -> row_count}`` across all paths.

    Reads every seed file once. ``user_id`` is normalized through
    ``_normalize_uid_cell`` so ``"9"`` and ``"9.0"`` collapse together.
    """
    out: Dict[Tuple[str, str], int] = defaultdict(int)
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            try:
                acct_col = detect_account_col(fieldnames)
            except ValueError:
                continue
            if "user_id" not in fieldnames:
                continue
            for row in reader:
                acct = (row.get(acct_col) or "").strip()
                if not acct:
                    continue
                uid = _normalize_uid_cell(row.get("user_id", ""))
                out[(acct, uid)] += 1
    return out


# ---------------------------------------------------------------------------
# Postgres lookups — kept thin so unit tests can monkeypatch them.
# ---------------------------------------------------------------------------


def lookup_broker_for_tuple(
    cursor, user_id: int, account_name: str,
) -> Tuple[str, str]:
    """Return ``(broker_slug, broker_external_id)`` for one tuple.

    Priority: schwab_connections → snaptrade_accounts → manual fallback.
    Manual fallback is symmetric with what the upload route does at
    write time for first-time uploads:
    ``broker_external_id = f"manual:{account_name}"``.
    """
    cursor.execute(
        "SELECT account_hash, account_number "
        "FROM schwab_connections "
        "WHERE user_id = %s AND account_name = %s "
        "LIMIT 1",
        (int(user_id), account_name),
    )
    row = cursor.fetchone()
    if row:
        account_hash, account_number = row
        ext_id = (account_hash or "").strip() or (account_number or "").strip()
        if ext_id:
            return SCHWAB_SLUG, ext_id

    cursor.execute(
        "SELECT snaptrade_account_id "
        "FROM snaptrade_accounts "
        "WHERE user_id = %s AND account_name = %s "
        "LIMIT 1",
        (int(user_id), account_name),
    )
    row = cursor.fetchone()
    if row and (row[0] or "").strip():
        return SNAPTRADE_SLUG, row[0].strip()

    # Manual fallback. Stable per (user_id, account_name) — same shape
    # the Stage 0 upload route uses for new uploads.
    return MANUAL_SLUG, f"manual:{account_name}"


def upsert_broker_account(
    cursor, user_id: int, broker_slug: str,
    broker_external_id: str, account_name: str,
) -> int:
    """Idempotent upsert. Returns the broker_accounts.id."""
    cursor.execute(
        "INSERT INTO broker_accounts "
        "(user_id, broker_slug, broker_external_id, account_name) "
        "VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (user_id, broker_slug, broker_external_id) "
        "DO UPDATE SET updated_at = NOW() "
        "RETURNING id",
        (int(user_id), broker_slug, broker_external_id, account_name),
    )
    return int(cursor.fetchone()[0])


def fetch_live_user_ids(cursor) -> set:
    """Return the set of ``users.id`` currently in Postgres.

    Seed CSVs can carry rows for user_ids that no longer exist (the
    Postgres user was deleted or renumbered, but their seed rows are
    still committed in GitHub). Attempting to insert a
    ``broker_accounts`` row with such a user_id trips the FK
    constraint. The backfill skips orphan-tenant user_ids and reports
    them as unresolvable instead of crashing.

    See broker-sync-safety SKILL.md (2026-05-11 entry) for the
    historical context — the same orphan-tenant data caused the
    cross-tenant rendering bug on /position/BE before this script
    existed.
    """
    cursor.execute("SELECT id FROM users")
    return {int(row[0]) for row in cursor.fetchall()}


def resolve_tuples_via_postgres(
    cursor,
    survey: Dict[Tuple[str, str], int],
    orphan_resolution: Dict[Tuple[str, str], str],
    *,
    live_user_ids: Optional[set] = None,
) -> Tuple[
    Dict[Tuple[str, str], int],
    Dict[Tuple[str, str], Tuple[str, str, int]],
    List[Tuple[Tuple[str, str], str]],
]:
    """Resolve every tuple to a broker_accounts.id.

    Returns ``(tuple_to_id, tuple_metadata, unresolved)`` where:

    - ``tuple_to_id[(acct, uid_in_seed)] = broker_accounts.id``
      keyed by the SEED's view of the row (so an orphan tuple
      ``("Foo", "")`` and its resolved tuple ``("Foo", "9")`` both
      map to the same id).
    - ``tuple_metadata[(acct, uid_in_seed)] = (broker_slug, ext_id, user_id_used)``
      for the audit log.
    - ``unresolved`` is the list of ``((acct, uid_in_seed), reason)``
      pairs that could not be backfilled. Reasons:
        ``"orphan_no_owner"``     — empty user_id + no other owner
        ``"orphan_ambiguous"``    — empty user_id + >1 owner
        ``"orphan_user_id"``      — user_id not present in Postgres
                                    ``users`` table (legacy/deleted user)
        ``"bad_user_id"``         — couldn't parse user_id as int

    ``live_user_ids`` can be passed in (for testability); if omitted,
    it's looked up via ``fetch_live_user_ids(cursor)`` once at the
    start of the loop.
    """
    tuple_to_id: Dict[Tuple[str, str], int] = {}
    metadata: Dict[Tuple[str, str], Tuple[str, str, int]] = {}
    unresolved: List[Tuple[Tuple[str, str], str]] = []

    if live_user_ids is None:
        live_user_ids = fetch_live_user_ids(cursor)

    # Cache by (resolved_user_id, account_name) so we don't re-upsert
    # the same broker_accounts row twice when both the orphan tuple
    # and the populated tuple resolve to the same effective user.
    cache: Dict[Tuple[int, str], int] = {}

    for (acct, uid_in_seed), _count in sorted(survey.items()):
        effective_uid_str = uid_in_seed or orphan_resolution.get((acct, uid_in_seed), "")
        if not effective_uid_str:
            # Empty user_id and no orphan resolution available.
            # Distinguish "no owner at all" from "ambiguous owner" so
            # the operator can act on each differently.
            same_account_uids = {
                uid for (a, uid), _ in survey.items()
                if a == acct and uid
            }
            reason = "orphan_ambiguous" if len(same_account_uids) > 1 else "orphan_no_owner"
            unresolved.append(((acct, uid_in_seed), reason))
            continue
        try:
            effective_uid = int(effective_uid_str)
        except (TypeError, ValueError):
            unresolved.append(((acct, uid_in_seed), "bad_user_id"))
            continue

        # Skip orphan-tenant user_ids — rows whose Postgres user has
        # been deleted. Attempting the upsert would trip the FK.
        if effective_uid not in live_user_ids:
            unresolved.append(((acct, uid_in_seed), "orphan_user_id"))
            continue

        cache_key = (effective_uid, acct)
        if cache_key in cache:
            tuple_to_id[(acct, uid_in_seed)] = cache[cache_key]
            slug, ext_id, _u = metadata.setdefault(
                (acct, uid_in_seed), ("", "", effective_uid),
            )
            metadata[(acct, uid_in_seed)] = (slug, ext_id, effective_uid)
            continue

        slug, ext_id = lookup_broker_for_tuple(cursor, effective_uid, acct)
        broker_account_id = upsert_broker_account(
            cursor, effective_uid, slug, ext_id, acct,
        )
        cache[cache_key] = broker_account_id
        tuple_to_id[(acct, uid_in_seed)] = broker_account_id
        metadata[(acct, uid_in_seed)] = (slug, ext_id, effective_uid)

    return tuple_to_id, metadata, unresolved


# ---------------------------------------------------------------------------
# Seed rewriting — same shape as scripts/backfill_seed_user_ids.py.
# ---------------------------------------------------------------------------


def backfill_csv(
    path: Path,
    tuple_to_id: Dict[Tuple[str, str], int],
    *,
    dry_run: bool,
) -> Tuple[int, int, int, int]:
    """Returns (rows_stamped, rows_left_null, rows_already_set, rows_changed_uid).

    A row counts as ``already_set`` if it already has a non-empty
    ``broker_account_id``. The script never overwrites a populated
    cell — Stage 0 writers always stamp the correct id at write time,
    so any existing value is authoritative.
    """
    if not path.exists():
        return 0, 0, 0, 0
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if "user_id" not in fieldnames or "broker_account_id" not in fieldnames:
            raise SystemExit(
                f"{path} is missing the 'user_id' or 'broker_account_id' "
                f"column. Run the Stage 0 schema migration first."
            )
        acct_col = detect_account_col(fieldnames)
        rows = list(reader)

    stamped = already = left_null = changed = 0
    for row in rows:
        existing = (row.get("broker_account_id") or "").strip()
        if existing:
            already += 1
            continue
        acct = (row.get(acct_col) or "").strip()
        uid_in_seed = _normalize_uid_cell(row.get("user_id", ""))
        broker_account_id = tuple_to_id.get((acct, uid_in_seed))
        if broker_account_id is None:
            left_null += 1
            continue
        row["broker_account_id"] = str(broker_account_id)
        stamped += 1

    if not dry_run and stamped:
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return stamped, left_null, already, changed


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


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
        help="Don't rewrite files or commit DB upserts; just print what would happen.",
    )
    args = parser.parse_args()

    if not args.db_url:
        print(
            "DATABASE_URL is not set and --db-url was not passed.",
            file=sys.stderr,
        )
        return 2

    # 1. Survey the seeds.
    survey = survey_seeds(SEED_FILES)
    print(f"Surveyed {sum(survey.values()):,} rows across "
          f"{len([p for p in SEED_FILES if p.exists()])} seeds.")
    print(f"Found {len(survey)} unique (account, user_id) tuples.")
    print()

    # 2. Resolve orphan user_ids via the in-Python account_owner heuristic.
    orphan_resolution = resolve_orphan_user_ids(survey)
    orphans = [k for k in survey if not k[1]]
    print(
        f"Orphan tuples (user_id empty): {len(orphans)}. "
        f"Resolved via single-owner rule: {len(orphan_resolution)}. "
        f"Unresolvable orphans: {len(orphans) - len(orphan_resolution)}."
    )
    print()

    # 3. Resolve each tuple to a broker_accounts.id via Postgres.
    import psycopg  # local import so unit tests can run without psycopg installed

    with psycopg.connect(args.db_url) as conn:
        # Disable autocommit so a failed upsert mid-flight rolls back the
        # whole script. We commit at the end on a successful pass.
        conn.autocommit = False
        with conn.cursor() as cur:
            tuple_to_id, metadata, unresolved = resolve_tuples_via_postgres(
                cur, survey, orphan_resolution,
            )

        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

    # 4. Report what we resolved.
    unresolved_by_key = {k: reason for (k, reason) in unresolved}
    print(f"{'Resolved':>8}  {'broker':>9}  {'eff_uid':>7}  "
          f"{'rows':>6}  account  /  (reason if unresolved)")
    print("-" * 90)
    for (acct, uid_in_seed), n in sorted(survey.items(), key=lambda x: (-x[1],)):
        bid = tuple_to_id.get((acct, uid_in_seed))
        if bid is None:
            reason = unresolved_by_key.get((acct, uid_in_seed), "?")
            print(f"  (skip)  {'(none)':>9}  {'(none)':>7}  "
                  f"{n:>6}  uid={uid_in_seed!r:<6}  {acct!r}  → {reason}")
            continue
        slug, ext_id, eff_uid = metadata[(acct, uid_in_seed)]
        print(f"  bid={bid:<3}  {slug:>9}  {eff_uid:>7}  "
              f"{n:>6}  uid={uid_in_seed!r:<6}  {acct!r}")
    print()

    # 4b. Group unresolved by reason for the operator summary.
    if unresolved:
        by_reason: Dict[str, int] = defaultdict(int)
        for _k, reason in unresolved:
            by_reason[reason] += 1
        print("Unresolved tuple breakdown:")
        for reason, cnt in sorted(by_reason.items()):
            print(f"  - {reason}: {cnt}")
        print()

    # 5. Rewrite each seed CSV with the stamps applied.
    total_s = total_n = total_a = 0
    for path in SEED_FILES:
        if not path.exists():
            print(f"  SKIP (missing): {path}")
            continue
        s, n, a, _ = backfill_csv(path, tuple_to_id, dry_run=args.dry_run)
        total_s += s
        total_n += n
        total_a += a
        print(f"  {path.name}: stamped={s}  left_null={n}  already_set={a}")
    print()

    print(
        f"Done. stamped={total_s}  left_null={total_n}  "
        f"already_set={total_a}  "
        f"{'(dry-run, no files rewritten)' if args.dry_run else ''}"
    )

    # Exit non-zero if any tuple was unresolved so CI / operator can
    # investigate. Already-populated rows that we didn't touch are fine.
    return 1 if unresolved else 0


if __name__ == "__main__":
    raise SystemExit(main())
