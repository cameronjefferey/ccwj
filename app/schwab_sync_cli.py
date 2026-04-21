"""
CLI for daily Schwab sync. Run via cron:
  0 18 * * * cd /path/to/ccwj && .venv/bin/python -m app.schwab_sync_cli

Requires: SCHWAB_APP_KEY, SCHWAB_APP_SECRET in env (callback URL not needed for sync).
"""
import os
import sys

# Ensure we can import app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Minimal Flask app context for DB access
os.environ.setdefault("FLASK_APP", "app:app")


def main():
    from app.db import fetch_all
    from app.models import init_db

    init_db()
    rows = fetch_all("SELECT user_id, account_number FROM schwab_connections")

    if not rows:
        print("No Schwab connections to sync.")
        return 0

    from app.schwab import _get_schwab_client, _run_sync

    for row in rows:
        user_id = row["user_id"]
        account_number = row["account_number"]
        try:
            client = _get_schwab_client(user_id, account_number)
            if client:
                result = _run_sync(user_id, client)
                print(f"User {user_id}: {result.get('history_rows', 0)} history, {result.get('current_rows', 0)} positions")
            else:
                print(f"User {user_id}: No valid client (token expired? re-connect in app)")
        except Exception as e:
            print(f"User {user_id}: Sync failed: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
