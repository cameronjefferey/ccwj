#!/usr/bin/env python
"""Mirror a PROD user's broker tenants into the LOCAL Postgres dev database.

Why this exists
---------------
Local dev is environment-separated from production (see AGENTS.md and
``.env``): the dev warehouse ``analytics_dev`` is a FULL MIRROR of prod seed
data (built by ``scripts/dev-refresh.sh``), but the Flask UI only shows a
logged-in user the warehouse rows whose ``tenant_id`` is in *their* local
Postgres ``broker_tenants`` table.

Local Postgres is a different database from prod, so prod users (and their
tenant rows) don't exist locally — which means you can't impersonate a prod
user and see your own scoped data out of the box. This script copies the
``broker_tenants`` rows for one prod user into your local Postgres under a
local user, preserving ``tenant_id`` verbatim (the warehouse join key). After
running it + ``scripts/dev-refresh.sh``, logging in as that local user shows
*exactly* your prod-scoped slice, behaving like production.

What it does NOT do
-------------------
- It NEVER writes to prod. Prod is opened read-only; all writes go to the
  database in ``DATABASE_URL`` (your local ``.env``).
- It does not re-sync via SnapTrade. Re-linking would mint NEW tenant_ids
  (``SNAPTRADE_USER_NAMESPACE=local`` namespaces local SnapTrade userIds),
  which would NOT match the prod seed rows. Copying the existing tenant_ids
  is the whole point.
- It does not copy numeric ``user_id`` across environments (ids collide
  across the two Postgres databases — that collision caused the June 2026
  "purge user 10" incident). The local user_id is resolved fresh from local
  Postgres; only ``tenant_id`` (env-stable) crosses over.

Usage
-----
    PROD_DATABASE_URL=postgresql://USER:PASS@HOST:PORT/DBNAME \
        python scripts/dev-link-prod-tenants.py --prod-username <name>

    # attach to a different local username (defaults to --prod-username):
    PROD_DATABASE_URL=... python scripts/dev-link-prod-tenants.py \
        --prod-username alice --local-username alice-local

    # preview without writing:
    PROD_DATABASE_URL=... python scripts/dev-link-prod-tenants.py \
        --prod-username alice --dry-run

Flags
-----
    --prod-username      (required) the prod account whose tenants to mirror
    --local-username     local account to attach them to (default: prod name)
    --create-local-user  create the local user if missing (random password;
                          set one afterward with `flask reset-password`)
    --include-inactive   also copy tenants whose connection_status != 'active'
    --dry-run            print what would change; write nothing
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import sys

import psycopg
from psycopg.rows import dict_row

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dotenv is a dev dependency
    load_dotenv = None


# Columns copied verbatim from prod broker_tenants. user_id is intentionally
# excluded — it is remapped to the LOCAL user's id (ids are not env-stable).
_TENANT_COLS = [
    "tenant_id",
    "broker_slug",
    "broker_uuid",
    "account_name",
    "account_mask",
    "broker_label",
    "snaptrade_connection_id",
    "connection_status",
    "connection_broken_at",
    "first_sync_completed",
    "display_nickname",
]


def _normalize_url(url: str) -> str:
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://") :]
    return url


def _connect(url: str, *, readonly: bool) -> psycopg.Connection:
    conn = psycopg.connect(_normalize_url(url), row_factory=dict_row, connect_timeout=15)
    if readonly:
        # Belt-and-suspenders: make it impossible for this connection to
        # mutate prod even if a future edit adds a stray write.
        conn.read_only = True
    return conn


def _resolve_prod_user(prod, username):
    row = prod.execute(
        "SELECT id, username FROM users WHERE username = %s", (username,)
    ).fetchone()
    if row is None:
        sys.exit(f"error: no prod user named {username!r} in PROD_DATABASE_URL.")
    return row


def _fetch_prod_tenants(prod, prod_user_id, include_inactive):
    sql = (
        f"SELECT {', '.join(_TENANT_COLS)} FROM broker_tenants WHERE user_id = %s"
    )
    if not include_inactive:
        sql += " AND connection_status = 'active'"
    sql += " ORDER BY created_at"
    return prod.execute(sql, (prod_user_id,)).fetchall()


def _resolve_local_user(local, username, *, create):
    row = local.execute(
        "SELECT id, username FROM users WHERE username = %s", (username,)
    ).fetchone()
    if row is not None:
        return row
    if not create:
        sys.exit(
            f"error: no local user named {username!r}.\n"
            f"  Create one first:  flask create-user --username {username}\n"
            f"  Or re-run with --create-local-user to create it automatically."
        )
    # Random throwaway password; the operator sets a real one afterward.
    pw_hash = _make_password_hash(secrets.token_urlsafe(16))
    local.execute(
        "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
        (username, pw_hash),
    )
    row = local.execute(
        "SELECT id, username FROM users WHERE username = %s", (username,)
    ).fetchone()
    print(
        f"  created local user {username!r} (id={row['id']}). Set a password "
        f"with:  flask reset-password --username {username}"
    )
    return row


def _make_password_hash(password: str) -> str:
    # Reuse the app's hashing so the row is valid even before the password
    # is reset. Falls back to werkzeug directly if app import is heavy.
    from werkzeug.security import generate_password_hash

    return generate_password_hash(password)


def _upsert_tenants(local, local_user_id, rows, *, dry_run):
    copied = 0
    for r in rows:
        values = [r[c] for c in _TENANT_COLS] + [local_user_id]
        if dry_run:
            print(
                f"  would link tenant {r['tenant_id']} "
                f"({r['account_name']!r}, status={r['connection_status']})"
            )
            copied += 1
            continue
        # tenant_id is the PK; on conflict re-point it at the local user and
        # refresh the display fields. UNIQUE(broker_slug, broker_uuid) holds
        # because those values are copied identically from prod.
        local.execute(
            "INSERT INTO broker_tenants "
            "(tenant_id, broker_slug, broker_uuid, account_name, account_mask, "
            " broker_label, snaptrade_connection_id, connection_status, "
            " connection_broken_at, first_sync_completed, display_nickname, user_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (tenant_id) DO UPDATE SET "
            "  user_id = EXCLUDED.user_id, "
            "  account_name = EXCLUDED.account_name, "
            "  account_mask = EXCLUDED.account_mask, "
            "  broker_label = EXCLUDED.broker_label, "
            "  snaptrade_connection_id = EXCLUDED.snaptrade_connection_id, "
            "  connection_status = EXCLUDED.connection_status, "
            "  connection_broken_at = EXCLUDED.connection_broken_at, "
            "  first_sync_completed = EXCLUDED.first_sync_completed, "
            "  display_nickname = EXCLUDED.display_nickname, "
            "  updated_at = NOW()",
            values,
        )
        # Legacy surfaces still read display labels from user_accounts; keep
        # it in sync so the account picker / older queries behave.
        local.execute(
            "INSERT INTO user_accounts (user_id, account_name) VALUES (%s, %s) "
            "ON CONFLICT (user_id, account_name) DO NOTHING",
            (local_user_id, r["account_name"]),
        )
        print(f"  linked tenant {r['tenant_id']} ({r['account_name']!r})")
        copied += 1
    return copied


def main():
    parser = argparse.ArgumentParser(
        description="Mirror a prod user's broker tenants into local Postgres."
    )
    parser.add_argument("--prod-username", default=None)
    parser.add_argument("--local-username", default=None)
    parser.add_argument(
        "--rows-file",
        default=None,
        help="JSON file of tenant rows to load instead of reading prod "
        "Postgres. Use when you can't reach prod directly (e.g. the password "
        "isn't available) but can export the rows another way. Each row must "
        "contain the broker_tenants display columns; user_id is ignored.",
    )
    parser.add_argument("--create-local-user", action="store_true")
    parser.add_argument("--include-inactive", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if load_dotenv:
        load_dotenv()  # local DATABASE_URL from .env

    local_url = os.environ.get("DATABASE_URL")
    if not local_url:
        sys.exit("error: DATABASE_URL (local) is not set. Add it to .env.")

    if args.rows_file:
        # Password-free path: rows already exported (e.g. via the Render MCP).
        local_username = args.local_username or args.prod_username
        if not local_username:
            sys.exit("error: --rows-file requires --local-username (or --prod-username).")
        with open(args.rows_file) as fh:
            raw = json.load(fh)
        # Keep only the columns we upsert; tolerate extra keys / missing
        # optional ones so an export can be a superset of the schema.
        tenants = [{c: row.get(c) for c in _TENANT_COLS} for row in raw]
        if not args.include_inactive:
            tenants = [t for t in tenants if t.get("connection_status") == "active"]
        print(f"==> Loaded {len(tenants)} tenant row(s) from {args.rows_file}")
    else:
        prod_url = os.environ.get("PROD_DATABASE_URL")
        if not args.prod_username:
            sys.exit("error: --prod-username is required (or use --rows-file).")
        if not prod_url:
            sys.exit(
                "error: PROD_DATABASE_URL is not set.\n"
                "  PROD_DATABASE_URL=postgresql://USER:PASS@HOST:PORT/DB "
                "python scripts/dev-link-prod-tenants.py --prod-username <name>\n"
                "  (or export the rows and use --rows-file)"
            )
        if _normalize_url(local_url) == _normalize_url(prod_url):
            sys.exit(
                "error: DATABASE_URL and PROD_DATABASE_URL point at the SAME "
                "database. This script must read prod and write local — refusing "
                "to run against a single database."
            )
        local_username = args.local_username or args.prod_username
        print(f"==> Reading prod tenants for {args.prod_username!r} (read-only)")
        with _connect(prod_url, readonly=True) as prod:
            prod_user = _resolve_prod_user(prod, args.prod_username)
            tenants = _fetch_prod_tenants(
                prod, prod_user["id"], args.include_inactive
            )

    if not tenants:
        sys.exit(
            f"error: prod user {args.prod_username!r} has no "
            f"{'tenants' if args.include_inactive else 'active tenants'} to copy."
        )
    print(f"    found {len(tenants)} tenant(s) to mirror")

    print(
        f"==> Writing into LOCAL Postgres as user {local_username!r}"
        + (" (dry run)" if args.dry_run else "")
    )
    # Ensure the local schema exists before we touch broker_tenants. Best
    # effort: if the app package can't import in this bare script context,
    # the tables almost certainly already exist (you've run the app), so we
    # warn and let the INSERTs surface any real "missing table" error.
    if not args.dry_run:
        try:
            repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if repo_root not in sys.path:
                sys.path.insert(0, repo_root)
            from app.models import init_db

            init_db()
        except Exception as exc:  # noqa: BLE001
            print(f"  (skipped local schema init: {exc})")

    with _connect(local_url, readonly=False) as local:
        local_user = _resolve_local_user(
            local, local_username, create=args.create_local_user and not args.dry_run
        )
        if args.dry_run and local_user is None:
            print(f"  (local user {local_username!r} would be created)")
            local_user_id = -1
        else:
            local_user_id = local_user["id"]
        copied = _upsert_tenants(
            local, local_user_id, tenants, dry_run=args.dry_run
        )

    verb = "would link" if args.dry_run else "linked"
    print(f"==> Done. {verb} {copied} tenant(s) to local user {local_username!r}.")
    if not args.dry_run:
        print(
            "    Next: run ./scripts/dev-refresh.sh (mirror prod data into "
            "analytics_dev), then log in locally as "
            f"{local_username!r} to see your prod-scoped view."
        )


if __name__ == "__main__":
    main()
