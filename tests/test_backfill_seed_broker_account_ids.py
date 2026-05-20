"""Tests for ``scripts/backfill_seed_broker_account_ids.py``.

Stage 1 of the broker-account-id migration —
see ``docs/BROKER_ACCOUNT_ID_MIGRATION.md``.

The script has three responsibilities; each gets its own test slice:

1. **Orphan resolution** (``resolve_orphan_user_ids``) — empty-``user_id``
   tuples get backfilled to the unique owner of the same account label
   when there's exactly one. This is the in-Python mirror of the dbt
   ``account_owner`` CTE in ``stg_history.sql``.

2. **Broker inference** (``lookup_broker_for_tuple``) — given a
   ``(user_id, account_name)``, pick the right ``broker_slug`` from
   Postgres connection tables, falling back to ``'manual'`` for rows
   that don't correspond to any broker connection.

3. **Seed rewriting** (``backfill_csv``) — idempotent in-place
   mutation: stamps the new column for rows that need it, preserves
   rows that already have a value, leaves the rest NULL.

Tests use a FakeCursor that records `execute` calls and returns
canned rows — no live Postgres required.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

from scripts import backfill_seed_broker_account_ids as bf


# ---------------------------------------------------------------------------
# FakeCursor — minimal psycopg cursor stub for the lookup/upsert paths.
# ---------------------------------------------------------------------------


class FakeCursor:
    """Bookkeeping-only psycopg cursor stand-in.

    ``schwab_rows`` and ``snaptrade_rows`` are dicts keyed by the
    ``(user_id, account_name)`` SQL parameters; the values are
    whatever the corresponding SELECT should return (single-row
    tuple or None).

    The INSERT path is recorded and returns a deterministic id
    derived from a simple counter, mimicking SERIAL behavior.
    """

    def __init__(self, schwab_rows=None, snaptrade_rows=None):
        self._schwab = schwab_rows or {}
        self._snaptrade = snaptrade_rows or {}
        self._last_result = None
        self._counter = 100  # next broker_accounts.id to hand out
        self.upserts = []  # list of (user_id, slug, ext_id, account_name)

    def execute(self, sql, params=()):
        sql_norm = " ".join(sql.split())
        if sql_norm.startswith("SELECT account_hash, account_number FROM schwab_connections"):
            user_id, account_name = params
            self._last_result = self._schwab.get((int(user_id), account_name))
        elif sql_norm.startswith("SELECT snaptrade_account_id FROM snaptrade_accounts"):
            user_id, account_name = params
            self._last_result = self._snaptrade.get((int(user_id), account_name))
        elif sql_norm.startswith("INSERT INTO broker_accounts"):
            user_id, slug, ext_id, account_name = params
            # Re-use ids for duplicate upserts so the test mirrors the real
            # UNIQUE (user_id, broker_slug, broker_external_id) constraint.
            key = (int(user_id), slug, ext_id)
            for existing in self.upserts:
                if (existing[0], existing[1], existing[2]) == key:
                    self._last_result = (existing[4],)
                    return
            self._counter += 1
            self.upserts.append((int(user_id), slug, ext_id, account_name, self._counter))
            self._last_result = (self._counter,)
        else:
            raise AssertionError(f"Unexpected SQL: {sql_norm}")

    def fetchone(self):
        return self._last_result


# ---------------------------------------------------------------------------
# Orphan resolution
# ---------------------------------------------------------------------------


def test_orphan_resolution_picks_unique_owner():
    """Empty user_id tuples get rewritten to the single non-empty owner
    when there's exactly one. Mirrors stg_history's account_owner CTE."""
    survey = {
        ("Schwab ••••0044", ""): 50,
        ("Schwab ••••0044", "8"): 200,  # only owner
        ("Cameron Investment", "9"): 30,  # no orphan to resolve
    }
    out = bf.resolve_orphan_user_ids(survey)
    assert out == {("Schwab ••••0044", ""): "8"}


def test_orphan_resolution_refuses_multiple_owners():
    """Sara Investment is claimed by 2 and 9 in production. Don't guess.

    Real warehouse failure mode: this is exactly the case that
    historically caused the cross-tenant rendering bug (IYW Emmory,
    May 2026 — see broker-sync-safety SKILL.md 2026-05-11 entry)."""
    survey = {
        ("Sara Investment", ""): 10,
        ("Sara Investment", "2"): 100,
        ("Sara Investment", "9"): 100,
    }
    out = bf.resolve_orphan_user_ids(survey)
    assert out == {}, (
        "orphan tuples with >1 non-empty owner must NOT be backfilled "
        "— picking wrong owner is exactly the failure mode the "
        "migration is designed to prevent"
    )


def test_orphan_resolution_no_owner_at_all_stays_null():
    """investment1-style rows: empty user_id, and the label doesn't
    exist under any other user_id. Stay NULL."""
    survey = {
        ("investment1", ""): 5,
    }
    assert bf.resolve_orphan_user_ids(survey) == {}


# ---------------------------------------------------------------------------
# Broker inference
# ---------------------------------------------------------------------------


def test_lookup_prefers_schwab_account_hash_over_account_number():
    """Schwab account_hash is the canonical stable handle; fall back to
    account_number only when hash is empty (legacy Schwab rows from
    before the hash column was populated)."""
    cursor = FakeCursor(
        schwab_rows={(8, "Schwab ••••0044"): ("HASH-ABC123", "0044")},
    )
    slug, ext_id = bf.lookup_broker_for_tuple(cursor, 8, "Schwab ••••0044")
    assert slug == "schwab"
    assert ext_id == "HASH-ABC123"


def test_lookup_falls_back_to_account_number_when_hash_blank():
    cursor = FakeCursor(
        schwab_rows={(8, "Schwab ••••0044"): ("", "0044")},
    )
    slug, ext_id = bf.lookup_broker_for_tuple(cursor, 8, "Schwab ••••0044")
    assert slug == "schwab"
    assert ext_id == "0044"


def test_lookup_returns_snaptrade_when_no_schwab_row():
    cursor = FakeCursor(
        snaptrade_rows={(14, "Fidelity ••••6342"): ("bed78305-aaaa-bbbb",)},
    )
    slug, ext_id = bf.lookup_broker_for_tuple(cursor, 14, "Fidelity ••••6342")
    assert slug == "snaptrade"
    assert ext_id == "bed78305-aaaa-bbbb"


def test_lookup_falls_back_to_manual_when_no_postgres_match():
    """Manual uploads (no broker connection) get
    ``broker_external_id=f'manual:{account_name}'`` — same shape the
    Stage 0 upload route writes for new uploads, so a re-upload after
    backfill resolves to the same broker_accounts row."""
    cursor = FakeCursor()
    slug, ext_id = bf.lookup_broker_for_tuple(cursor, 9, "Cameron Investment")
    assert slug == "manual"
    assert ext_id == "manual:Cameron Investment"


# ---------------------------------------------------------------------------
# resolve_tuples_via_postgres — end-to-end resolution loop
# ---------------------------------------------------------------------------


def test_resolve_uses_cache_to_avoid_double_upsert_for_orphan_plus_owner():
    """When both ``(acct, "")`` and ``(acct, "8")`` exist in the seed
    survey AND the orphan resolves to user_id 8, we must NOT upsert
    ``broker_accounts`` twice — the second call has to read the cache.

    Confirms the script doesn't accidentally create duplicate
    ``broker_accounts`` rows (it would be functionally idempotent
    thanks to the unique constraint, but the cache also short-circuits
    the SQL round-trip)."""
    cursor = FakeCursor(
        schwab_rows={(8, "Schwab ••••0044"): ("HASH-0044", "0044")},
    )
    survey = {
        ("Schwab ••••0044", ""): 50,
        ("Schwab ••••0044", "8"): 200,
    }
    orphan_resolution = {("Schwab ••••0044", ""): "8"}
    tuple_to_id, metadata, unresolved = bf.resolve_tuples_via_postgres(
        cursor, survey, orphan_resolution, live_user_ids={8},
    )
    assert unresolved == []
    # Both tuples map to the SAME broker_accounts.id
    assert tuple_to_id[("Schwab ••••0044", "")] == tuple_to_id[("Schwab ••••0044", "8")]
    # And only ONE upsert was issued
    schwab_upserts = [u for u in cursor.upserts if u[1] == "schwab"]
    assert len(schwab_upserts) == 1


def test_resolve_keeps_unresolved_orphans_out_of_tuple_to_id():
    cursor = FakeCursor()
    survey = {
        ("Sara Investment", ""): 10,
        ("Sara Investment", "2"): 100,
        ("Sara Investment", "9"): 100,
    }
    orphan_resolution = {}  # >1 owner → can't resolve
    tuple_to_id, metadata, unresolved = bf.resolve_tuples_via_postgres(
        cursor, survey, orphan_resolution, live_user_ids={2, 9},
    )
    unresolved_keys = [k for (k, _reason) in unresolved]
    unresolved_reasons = {k: r for (k, r) in unresolved}
    assert ("Sara Investment", "") in unresolved_keys
    assert unresolved_reasons[("Sara Investment", "")] == "orphan_ambiguous", (
        "two distinct populated owners must be reported as ambiguous, "
        "not as 'no_owner' — different operator action required"
    )
    assert ("Sara Investment", "") not in tuple_to_id
    # The two populated tuples STILL get resolved (they have real user_ids).
    assert ("Sara Investment", "2") in tuple_to_id
    assert ("Sara Investment", "9") in tuple_to_id


def test_resolve_mixes_brokers_per_user():
    """One user with both a Schwab account and a SnapTrade account
    must get DIFFERENT broker_accounts.id values for each — they're
    different tenants in the new model."""
    cursor = FakeCursor(
        schwab_rows={(9, "Schwab ••••9437"): ("HASH-9437", "9437")},
        snaptrade_rows={(9, "Coinbase Account"): ("snap-coin-uuid",)},
    )
    survey = {
        ("Schwab ••••9437", "9"): 100,
        ("Coinbase Account", "9"): 50,
        ("Manual Test Acct", "9"): 10,  # no connection → manual fallback
    }
    tuple_to_id, metadata, _u = bf.resolve_tuples_via_postgres(
        cursor, survey, {}, live_user_ids={9},
    )
    assert len(set(tuple_to_id.values())) == 3, "three tenants, three ids"
    assert metadata[("Schwab ••••9437", "9")][0] == "schwab"
    assert metadata[("Coinbase Account", "9")][0] == "snaptrade"
    assert metadata[("Manual Test Acct", "9")][0] == "manual"


def test_resolve_skips_user_ids_not_in_live_users_table():
    """The real-world finding from running Stage 1 against production:
    seed rows can carry a user_id whose Postgres user was deleted
    (the May 2026 cross-tenant rendering bug — see broker-sync-safety
    SKILL.md 2026-05-11 entry). Attempting to insert a broker_accounts
    row with that user_id trips the FK constraint. The backfill
    pre-checks against ``users.id`` and reports the tuples instead of
    crashing."""
    cursor = FakeCursor(
        schwab_rows={(13, "Cameron Investment"): ("HASH-13-CAM", "1234")},
    )
    survey = {
        ("Cameron Investment", "13"): 500,  # user 13 doesn't exist in Postgres
        ("Cameron Investment", "9"): 100,   # user 9 does
    }
    tuple_to_id, metadata, unresolved = bf.resolve_tuples_via_postgres(
        cursor, survey, {}, live_user_ids={9},  # user 13 NOT present
    )
    unresolved_keys = {k for (k, _r) in unresolved}
    unresolved_reasons = {k: r for (k, r) in unresolved}
    assert ("Cameron Investment", "13") in unresolved_keys
    assert unresolved_reasons[("Cameron Investment", "13")] == "orphan_user_id"
    assert ("Cameron Investment", "13") not in tuple_to_id
    # User 9's tuple still resolves — orphan user_id is per-tuple, not global.
    assert ("Cameron Investment", "9") in tuple_to_id


def test_resolve_orphan_with_no_owner_distinguishes_from_ambiguous():
    """Two failure shapes that look similar in the seed but need
    different operator action:

    - ``orphan_no_owner``:  empty user_id, account label has NO other
      owner anywhere. Probably truly orphan data — investigate why
      it exists.
    - ``orphan_ambiguous``: empty user_id, account label has 2+
      populated owners. Cross-tenant collision — the same fix that
      already exists in stg_history.account_owner CTE (refuse to pick
      one)."""
    cursor = FakeCursor()
    survey = {
        ("Truly Orphan", ""): 1,
        ("Shared Label", ""): 5,
        ("Shared Label", "2"): 50,
        ("Shared Label", "9"): 50,
    }
    _t, _m, unresolved = bf.resolve_tuples_via_postgres(
        cursor, survey, {}, live_user_ids={2, 9},
    )
    reasons = {k: r for (k, r) in unresolved}
    assert reasons[("Truly Orphan", "")] == "orphan_no_owner"
    assert reasons[("Shared Label", "")] == "orphan_ambiguous"


# ---------------------------------------------------------------------------
# Seed rewriting
# ---------------------------------------------------------------------------


@pytest.fixture
def seed_csv(tmp_path):
    """Build a fake trade_history.csv with mixed rows."""
    path = tmp_path / "trade_history.csv"
    rows = [
        # legacy row, no user_id, no broker_account_id
        {"Account": "Schwab ••••0044", "user_id": "",   "broker_account_id": "",
         "Date": "01/01/2025", "Action": "Buy", "Symbol": "AAPL",
         "Description": "", "Quantity": "10", "Price": "200",
         "fees_and_comm": "", "Amount": "-2000"},
        # post-tenancy row, has user_id, still no broker_account_id
        {"Account": "Schwab ••••0044", "user_id": "8",  "broker_account_id": "",
         "Date": "02/01/2025", "Action": "Sell", "Symbol": "AAPL",
         "Description": "", "Quantity": "10", "Price": "210",
         "fees_and_comm": "", "Amount": "2100"},
        # already-stamped row (e.g. a Stage 0 writer landed)
        {"Account": "Schwab ••••0044", "user_id": "8",  "broker_account_id": "999",
         "Date": "03/01/2025", "Action": "Buy", "Symbol": "MSFT",
         "Description": "", "Quantity": "5", "Price": "400",
         "fees_and_comm": "", "Amount": "-2000"},
        # tuple we can't resolve (orphan, no unique owner)
        {"Account": "investment1", "user_id": "", "broker_account_id": "",
         "Date": "04/01/2025", "Action": "Buy", "Symbol": "XYZ",
         "Description": "", "Quantity": "1", "Price": "10",
         "fees_and_comm": "", "Amount": "-10"},
    ]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return path


def test_backfill_csv_stamps_unresolved_only(seed_csv):
    tuple_to_id = {
        ("Schwab ••••0044", ""): 42,
        ("Schwab ••••0044", "8"): 42,
        # ``("investment1", "")`` deliberately absent — unresolved
    }
    stamped, left_null, already, _ = bf.backfill_csv(
        seed_csv, tuple_to_id, dry_run=False,
    )
    assert stamped == 2, "two NULL rows in the resolvable group must be stamped"
    assert already == 1, "the pre-stamped row must be preserved"
    assert left_null == 1, "investment1 row stays NULL"

    with seed_csv.open() as f:
        out = list(csv.DictReader(f))
    assert out[0]["broker_account_id"] == "42"
    assert out[1]["broker_account_id"] == "42"
    assert out[2]["broker_account_id"] == "999", "existing stamp preserved verbatim"
    assert out[3]["broker_account_id"] == "", "unresolved tuple left empty"


def test_backfill_csv_dry_run_does_not_touch_disk(seed_csv):
    before = seed_csv.read_text()
    tuple_to_id = {
        ("Schwab ••••0044", ""): 42,
        ("Schwab ••••0044", "8"): 42,
    }
    stamped, left_null, already, _ = bf.backfill_csv(
        seed_csv, tuple_to_id, dry_run=True,
    )
    assert stamped == 2  # counts the would-be stamps
    assert seed_csv.read_text() == before, "dry-run must NOT mutate the file"


def test_backfill_csv_is_idempotent(seed_csv):
    """Running the backfill twice must be a no-op the second time."""
    tuple_to_id = {
        ("Schwab ••••0044", ""): 42,
        ("Schwab ••••0044", "8"): 42,
    }
    bf.backfill_csv(seed_csv, tuple_to_id, dry_run=False)
    # All previously-NULL rows are now stamped, so the second pass
    # should report stamped=0 and already_set=3 (or 4 if you count the
    # unrelated already-stamped row).
    stamped2, left_null2, already2, _ = bf.backfill_csv(
        seed_csv, tuple_to_id, dry_run=False,
    )
    assert stamped2 == 0, "second pass must not re-stamp anything"
    assert already2 == 3, "all rows that the script CAN stamp are now stamped"


def test_normalize_uid_cell_matches_normalize_uid_contract():
    """The seed-side normalization must agree with
    ``app.upload._normalize_uid`` so the backfill's tuple key matches
    the writer's stamping decisions."""
    assert bf._normalize_uid_cell("9.0") == "9"
    assert bf._normalize_uid_cell("9") == "9"
    assert bf._normalize_uid_cell(" 9 ") == "9"
    assert bf._normalize_uid_cell("") == ""
    assert bf._normalize_uid_cell("nan") == ""
    assert bf._normalize_uid_cell("None") == ""
    assert bf._normalize_uid_cell("<NA>") == ""


def test_survey_seeds_normalizes_user_id_form(tmp_path):
    """A seed with mixed ``"9"`` and ``"9.0"`` cells must coalesce to a
    single tuple in the survey output."""
    path = tmp_path / "trade_history.csv"
    with path.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["Account", "user_id", "broker_account_id",
                        "Date", "Action", "Symbol", "Description",
                        "Quantity", "Price", "fees_and_comm", "Amount"],
        )
        w.writeheader()
        w.writerow({"Account": "Schwab ••••9437", "user_id": "9", "broker_account_id": "",
                    "Date": "01/01/2025", "Action": "Buy", "Symbol": "AAPL",
                    "Description": "", "Quantity": "1", "Price": "100",
                    "fees_and_comm": "", "Amount": "-100"})
        w.writerow({"Account": "Schwab ••••9437", "user_id": "9.0", "broker_account_id": "",
                    "Date": "01/02/2025", "Action": "Buy", "Symbol": "MSFT",
                    "Description": "", "Quantity": "1", "Price": "400",
                    "fees_and_comm": "", "Amount": "-400"})

    survey = bf.survey_seeds([path])
    assert survey == {("Schwab ••••9437", "9"): 2}, (
        "9 and 9.0 must collapse to the same tuple"
    )
