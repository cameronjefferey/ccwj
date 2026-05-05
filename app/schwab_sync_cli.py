"""
CLI for daily Schwab sync. Run via cron:
  0 18 * * * cd /path/to/ccwj && .venv/bin/python -m app.schwab_sync_cli

Requires: SCHWAB_APP_KEY, SCHWAB_APP_SECRET in env (callback URL not needed for sync).

Exit codes:
  0  — at least one connection synced (or there were no connections to sync).
  1  — there are connections in the DB but every one failed (expired tokens,
       missing env, sync exceptions). Render flags the run red, so a system-wide
       problem is visible without cross-referencing GitHub Actions.
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

    total = len(rows)
    succeeded = 0
    pushed = 0
    expired = 0
    errors = 0
    push_skipped = 0
    last_skip_reason = None

    for row in rows:
        user_id = row["user_id"]
        account_number = row["account_number"]
        try:
            client = _get_schwab_client(user_id, account_number)
            if client:
                result = _run_sync(
                    user_id, client, account_number=account_number
                )
                succeeded += 1
                line = (
                    f"User {user_id} ({account_number}): "
                    f"{result.get('history_rows', 0)} history, "
                    f"{result.get('current_rows', 0)} positions"
                )
                if result.get("github_pushed"):
                    pushed += 1
                    line += " (GitHub seeds updated)"
                elif result.get("github_error"):
                    line += f" (GitHub: {result['github_error'][:120]})"
                elif result.get("github_seed_push_skipped"):
                    push_skipped += 1
                    reason = (
                        result.get("github_skip_reason")
                        or "GitHub seed push not configured."
                    )
                    last_skip_reason = reason
                    line += f" (GitHub skipped: {reason})"
                print(line)
            else:
                expired += 1
                print(
                    f"User {user_id} ({account_number}): "
                    "No valid client (token expired? re-connect in app)"
                )
        except Exception as e:
            errors += 1
            print(
                f"User {user_id} ({account_number}): Sync failed: {e}",
                file=sys.stderr,
            )

    # Summary line: greppable in Render logs and shows the actual
    # outcome instead of relying on the exit code alone.
    print(
        f"Sync summary: {succeeded}/{total} succeeded, "
        f"{pushed} pushed to GitHub, "
        f"{expired} invalid clients, {errors} errors"
    )
    if push_skipped and not pushed:
        # Every successful sync skipped the GitHub push for the same
        # config reason — surface it loudly so the operator doesn't
        # have to guess which env var is wrong. (We saw this when
        # GITHUB_PAT looked correct on the cron service but
        # GITHUB_REPO had a typo.)
        print(
            f"WARNING: {push_skipped} successful sync(s) did not reach GitHub. "
            f"Reason: {last_skip_reason}",
            file=sys.stderr,
        )

    # Exit non-zero only when *every* connection failed. A single
    # expired user among many is normal (they'll reconnect when they
    # next open the app); 0/N is a systemic problem (env var missing,
    # OAuth grant invalidated, etc.) and Render should flag it.
    if total > 0 and succeeded == 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
