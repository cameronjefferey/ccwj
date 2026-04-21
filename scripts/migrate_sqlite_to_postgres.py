#!/usr/bin/env python3
"""
One-shot migration: copy a single user's data from the legacy SQLite DB
(``instance/happytrader.db``) into the configured Postgres database.

The SQLite DB is read-only — nothing is modified. The script is idempotent:
re-running will skip rows that already exist in Postgres.

Usage:
    # Make sure DATABASE_URL points at the target Postgres (use Render's
    # External Database URL when running from your laptop).
    DATABASE_URL=postgresql://... \\
        python scripts/migrate_sqlite_to_postgres.py \\
            --username happycameron \\
            --account investment1

By default the script copies:
  - the user row (preserving username + password_hash)
  - the requested account link
  - all uploads, insights, mirror scores tied to that user_id
  - any Schwab connections tied to that user_id
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path


def _load_env() -> None:
    """Load .env from the project root so DATABASE_URL is available."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass


def _open_sqlite(path: str) -> sqlite3.Connection:
    if not Path(path).exists():
        sys.exit(f"SQLite DB not found: {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _confirm_target() -> None:
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        sys.exit("DATABASE_URL is not set. Aborting.")
    # Mask password for display
    masked = url
    if "@" in url and "://" in url:
        scheme, rest = url.split("://", 1)
        if "@" in rest:
            creds, host = rest.split("@", 1)
            if ":" in creds:
                user, _ = creds.split(":", 1)
                masked = f"{scheme}://{user}:***@{host}"
    print(f"Target Postgres: {masked}")


def migrate(sqlite_path: str, username: str, account_name: str) -> None:
    _confirm_target()
    src = _open_sqlite(sqlite_path)

    # Import only after env is loaded so the pool sees DATABASE_URL.
    from app.db import fetch_one, get_conn
    from app.models import init_db

    print("Ensuring Postgres schema exists ...")
    init_db()

    user_row = src.execute(
        "SELECT id, username, password_hash FROM users WHERE username = ?",
        (username,),
    ).fetchone()
    if not user_row:
        sys.exit(f"User '{username}' not found in {sqlite_path}")
    src_user_id = user_row["id"]
    print(f"Source user: id={src_user_id} username={user_row['username']}")

    # Verify the account link exists in source
    acct_row = src.execute(
        "SELECT 1 FROM user_accounts WHERE user_id = ? AND account_name = ?",
        (src_user_id, account_name),
    ).fetchone()
    if not acct_row:
        sys.exit(f"Account '{account_name}' not linked to user '{username}' in source DB")

    # ------------------------------------------------------------------
    # Insert user (preserving password_hash). Get the Postgres-assigned id.
    # ------------------------------------------------------------------
    existing = fetch_one("SELECT id FROM users WHERE username = %s", (username,))
    with get_conn() as pg:
        with pg.cursor() as cur:
            if existing:
                pg_user_id = existing["id"]
                print(f"User already exists in Postgres (id={pg_user_id}); reusing.")
            else:
                cur.execute(
                    "INSERT INTO users (username, password_hash) VALUES (%s, %s) "
                    "RETURNING id",
                    (user_row["username"], user_row["password_hash"]),
                )
                pg_user_id = cur.fetchone()["id"]
                print(f"Inserted user into Postgres (id={pg_user_id}).")

            # ----------------------------------------------------------
            # Account link (only the one requested)
            # ----------------------------------------------------------
            cur.execute(
                "INSERT INTO user_accounts (user_id, account_name) VALUES (%s, %s) "
                "ON CONFLICT DO NOTHING",
                (pg_user_id, account_name),
            )
            print(f"Linked account: {account_name}")

            # ----------------------------------------------------------
            # Uploads
            # ----------------------------------------------------------
            uploads = src.execute(
                "SELECT account_name, history_rows, current_rows, uploaded_at "
                "FROM uploads WHERE user_id = ?",
                (src_user_id,),
            ).fetchall()
            for u in uploads:
                cur.execute(
                    "INSERT INTO uploads "
                    "(user_id, account_name, history_rows, current_rows, uploaded_at) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (pg_user_id, u["account_name"], u["history_rows"],
                     u["current_rows"], u["uploaded_at"]),
                )
            print(f"Copied {len(uploads)} uploads.")

            # ----------------------------------------------------------
            # Insights (latest-only is fine; copy all)
            # ----------------------------------------------------------
            insights = src.execute(
                "SELECT summary, full_analysis, generated_at FROM insights "
                "WHERE user_id = ?",
                (src_user_id,),
            ).fetchall()
            for ins in insights:
                cur.execute(
                    "INSERT INTO insights (user_id, summary, full_analysis, generated_at) "
                    "VALUES (%s, %s, %s, %s)",
                    (pg_user_id, ins["summary"], ins["full_analysis"], ins["generated_at"]),
                )
            print(f"Copied {len(insights)} insights.")

            # ----------------------------------------------------------
            # Mirror scores (idempotent via ON CONFLICT)
            # ----------------------------------------------------------
            scores = src.execute(
                """SELECT week_start_date, discipline_score, intent_score,
                          risk_alignment_score, consistency_score, mirror_score,
                          confidence_level, diagnostic_sentence, generated_at
                   FROM weekly_mirror_scores WHERE user_id = ?""",
                (src_user_id,),
            ).fetchall()
            for s in scores:
                cur.execute(
                    """INSERT INTO weekly_mirror_scores
                       (user_id, week_start_date, discipline_score, intent_score,
                        risk_alignment_score, consistency_score, mirror_score,
                        confidence_level, diagnostic_sentence, generated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (user_id, week_start_date) DO NOTHING""",
                    (pg_user_id, s["week_start_date"], s["discipline_score"],
                     s["intent_score"], s["risk_alignment_score"],
                     s["consistency_score"], s["mirror_score"],
                     s["confidence_level"], s["diagnostic_sentence"],
                     s["generated_at"]),
                )
            print(f"Copied {len(scores)} mirror scores.")

            # ----------------------------------------------------------
            # Schwab connections (verbatim — token not re-encrypted yet)
            # ----------------------------------------------------------
            schwab = src.execute(
                """SELECT account_hash, account_number, account_name, token_json,
                          created_at, updated_at
                   FROM schwab_connections WHERE user_id = ?""",
                (src_user_id,),
            ).fetchall()
            for c in schwab:
                cur.execute(
                    """INSERT INTO schwab_connections
                       (user_id, account_hash, account_number, account_name,
                        token_json, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (user_id, account_number) DO NOTHING""",
                    (pg_user_id, c["account_hash"], c["account_number"],
                     c["account_name"], c["token_json"], c["created_at"],
                     c["updated_at"]),
                )
            print(f"Copied {len(schwab)} Schwab connections.")

    src.close()
    print("\nMigration complete.")


def main() -> None:
    _load_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sqlite",
        default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                             "instance", "happytrader.db"),
        help="Path to source SQLite DB (default: instance/happytrader.db)",
    )
    parser.add_argument("--username", required=True, help="Username to migrate")
    parser.add_argument("--account", required=True,
                        help="Single account_name to copy across")
    args = parser.parse_args()

    # Make ``app`` importable when run as a script from the project root.
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    migrate(args.sqlite, args.username, args.account)


if __name__ == "__main__":
    main()
