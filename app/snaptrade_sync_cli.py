"""
CLI for daily SnapTrade sync. Run via cron:
  0 22 * * * cd /path/to/ccwj && .venv/bin/python -m app.snaptrade_sync_cli

Requires: SNAPTRADE_CLIENT_ID, SNAPTRADE_CONSUMER_KEY in env. The
``SNAPTRADE_REDIRECT_URI`` env var is only used by the OAuth callback
flow; the cron does not need it.

Exit codes:
  0  — at least one connection synced (or there were no connections to sync).
  1  — there are connections in the DB but every one failed (auth errors,
       network errors, sync exceptions). Render flags the run red, so a
       system-wide problem is visible without cross-referencing GitHub.
"""
import os
import sys

# Ensure we can import app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Minimal Flask app context for DB access
os.environ.setdefault("FLASK_APP", "app:app")


def main():
    from app.models import init_db, list_all_snaptrade_accounts

    init_db()
    rows = list_all_snaptrade_accounts() or []

    if not rows:
        print("No SnapTrade accounts to sync.")
        return 0

    from app.snaptrade import (
        _get_snaptrade_client,
        _routine_lookback_days,
        _sync_one_connection,
        SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS,
        snaptrade_enabled,
    )
    from app.snaptrade import _bulk_sync_lookback_days

    if not snaptrade_enabled():
        print("SnapTrade is not configured (missing env or SDK). Exiting.", file=sys.stderr)
        return 1

    client = _get_snaptrade_client()
    if client is None:
        print("Could not build SnapTrade client. Exiting.", file=sys.stderr)
        return 1

    total = len(rows)
    succeeded = 0
    pushed = 0
    broken = 0
    errors = 0
    push_skipped = 0
    last_skip_reason = None

    routine_days = _routine_lookback_days()
    full_days = SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS

    for row in rows:
        user_id = row["user_id"]
        snaptrade_account_id = row["snaptrade_account_id"]
        first_done = bool(row.get("first_sync_completed"))
        # Cron uses routine semantics — never force full-history. If a
        # row hasn't completed first sync yet (e.g. user connected but
        # never clicked Sync now in the UI), the per-row decision still
        # picks the full window so day-one history lands.
        lookback = _bulk_sync_lookback_days(
            first_done,
            force_full_history=False,
            routine_days=routine_days,
            full_days=full_days,
        )
        try:
            res = _sync_one_connection(user_id, row, lookback_days=lookback)
        except Exception as exc:
            errors += 1
            print(
                f"User {user_id} ({snaptrade_account_id}): unexpected sync error: {exc}",
                file=sys.stderr,
            )
            continue

        line = (
            f"User {user_id} ({row.get('account_name') or snaptrade_account_id}): "
            f"{res['history_rows']} history, {res['current_rows']} positions"
        )

        if res["ok"]:
            succeeded += 1
            if res["github_pushed"]:
                pushed += 1
                line += " (GitHub seeds updated)"
            elif res["github_error"]:
                line += f" (GitHub: {str(res['github_error'])[:120]})"
            elif res["github_seed_push_skipped"]:
                push_skipped += 1
                reason = (
                    res.get("github_skip_reason")
                    or "GitHub seed push not configured."
                )
                last_skip_reason = reason
                line += f" (GitHub skipped: {reason})"
            print(line)
        else:
            err = res["error"] or "unknown"
            if err == "connection_broken":
                broken += 1
                print(
                    f"User {user_id} ({snaptrade_account_id}): "
                    "broker connection needs reconnect (flagged in app)",
                    file=sys.stderr,
                )
            else:
                errors += 1
                print(
                    f"User {user_id} ({snaptrade_account_id}): sync failed: {err}",
                    file=sys.stderr,
                )

    print(
        f"SnapTrade sync summary: {succeeded}/{total} succeeded, "
        f"{pushed} pushed to GitHub, "
        f"{broken} broken connections, {errors} errors"
    )
    if push_skipped and not pushed:
        print(
            f"WARNING: {push_skipped} successful sync(s) did not reach GitHub. "
            f"Reason: {last_skip_reason}",
            file=sys.stderr,
        )

    if total > 0 and succeeded == 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
