"""
CLI for the SnapTrade sync BACKSTOP. The freshness driver is the
``ACCOUNT_HOLDINGS_UPDATED`` webhook (``app/webhooks.py``); this CLI is the
safety net for days a webhook delivery is missed. It runs on two Render crons:

  * happytrader-snaptrade-sync (23:00 UTC weekdays) — PLAIN read backstop
    (``force_refresh=False``): re-reads whatever SnapTrade already has cached.
    Cheap (no billed refresh), catches missed webhooks off-hours.

  * happytrader-snaptrade-refresh (~20:10 UTC weekdays, market close) —
    FORCE-REFRESH pass (``--force-refresh``): actively asks SnapTrade to repoll
    every broker BEFORE reading. This is the ONLY way to pull intraday changes
    from brokers SnapTrade does not poll in real-time — Schwab in particular is
    daily-only via SnapTrade regardless of the real-time plan (the plan governs
    SnapTrade's AUTOMATIC polling; a manual ``refresh_brokerage_authorization``
    is a separate, per-call-billed API). So a trader who closes/opens positions
    intraday sees them the same evening instead of waiting for SnapTrade's own
    once-a-day Schwab poll. Mirrors the in-product "Sync now" force-refresh.

--force-refresh flow (mirrors ``_sync_all_for_user``): fire a
``_force_refresh_brokerage`` for EVERY account up front, sleep ONE settle window
(``SNAPTRADE_CRON_FORCE_REFRESH_SETTLE_SECONDS``, default 90s) so the bulk repoll
adds a single wait rather than one per account, then read each with the normal
``defer_push=True`` path (no further per-account refresh — the up-front refresh
already fired, and the per-authorization throttle would block a second one). The
follow-up ACCOUNT_HOLDINGS_UPDATED webhook is still the guaranteed catch if a
read races SnapTrade's repoll; the batched push + monotonic merge make the
overlap harmless (newer data wins). BILLING: force-refresh is billed per call by
SnapTrade, so ONLY the market-close cron passes ``--force-refresh``; the 23:00
backstop stays a plain read.

Each account is synced with ``defer_push=True`` (fetch + normalize, no commit);
after the loop we push ONE batched seed commit via ``merge_and_push_seeds_batch``.
This replaced the old per-account push that fanned this cron out into ~14 GitHub
commits a night → ~14 ``Update Daily Position Performance`` runs (most instantly
cancelled by ``concurrency: cancel-in-progress``). One commit = one dbt build;
monotonic-merge semantics are preserved because the batch folds accounts in the
same order the sequential pushes used. Manual local invocation:
  cd /path/to/ccwj && .venv/bin/python -m app.snaptrade_sync_cli [--force-refresh]

Requires: SNAPTRADE_CLIENT_ID, SNAPTRADE_CONSUMER_KEY in env. The
``SNAPTRADE_REDIRECT_URI`` env var is only used by the OAuth callback
flow; the cron does not need it. ``--force-refresh`` can also be enabled via
``SNAPTRADE_CRON_FORCE_REFRESH=1`` for cron platforms that only set env vars.

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


def _notify_connection_dropped(user_id, snaptrade_account_id, row):
    """Send a one-time "reconnect your broker" email when a connection just
    broke. Idempotent via the email_sends log keyed on the account + the
    broken-at timestamp, so re-breaks after a reconnect notify again but a
    daily cron over a still-broken connection does not re-spam.

    Best-effort: never raises into the sync loop.
    """
    try:
        from app.email import send_connection_dropped_email, app_base_url
        from app.models import User, get_snaptrade_account, record_email_send

        user = User.get_by_id(user_id)
        if user is None or not (user.email or "").strip():
            return  # No address on file (legacy/CLI user) — nothing to send.

        acct = get_snaptrade_account(user_id, snaptrade_account_id) or {}
        broken_at = acct.get("connection_broken_at")
        broken_key = broken_at.isoformat() if hasattr(broken_at, "isoformat") else str(broken_at or "")
        dedupe_key = f"{snaptrade_account_id}:{broken_key}"

        if not record_email_send(
            "connection_dropped", dedupe_key, user_id=user_id, to_email=user.email
        ):
            return  # Already notified for this break.

        broker_slug = (row.get("broker_slug") or acct.get("broker_slug") or "").strip()
        broker_label = broker_slug.title() if broker_slug else "your broker"
        account_label = (
            (row.get("display_nickname") or acct.get("display_nickname") or "")
            or (row.get("account_name") or acct.get("account_name") or "")
        ).strip()
        reconnect_url = f"{app_base_url()}/profile?tab=account#snaptrade-sync"

        send_connection_dropped_email(
            to=user.email,
            username=user.username,
            broker_label=broker_label,
            account_label=account_label,
            reconnect_url=reconnect_url,
        )
        print(f"User {user_id} ({snaptrade_account_id}): sent reconnect email to {user.email}")
    except Exception as exc:  # pragma: no cover (defensive — email never blocks sync)
        print(
            f"User {user_id} ({snaptrade_account_id}): reconnect email failed: {exc}",
            file=sys.stderr,
        )


def _force_refresh_enabled(argv=None):
    """--force-refresh CLI flag OR SNAPTRADE_CRON_FORCE_REFRESH=1 env.

    The market-close cron passes the flag; the 23:00 backstop does not (a plain
    read is free, a force-refresh is billed per call — see module docstring).
    """
    argv = sys.argv[1:] if argv is None else argv
    if "--force-refresh" in argv:
        return True
    return (os.environ.get("SNAPTRADE_CRON_FORCE_REFRESH", "") or "").strip().lower() in (
        "1", "true", "yes", "on",
    )


def _force_refresh_all(rows):
    """Fire a broker repoll for every account UP FRONT, then sleep ONE settle
    window so the bulk request adds a single wait (not one per account).

    Mirrors ``_sync_all_for_user``'s batch-refresh pattern. Each refresh is
    throttled + non-fatal; a throttled/failed refresh just falls through to the
    normal read (same outcome as the plain backstop). Returns the count of
    accounts SnapTrade accepted a refresh for.
    """
    from app.snaptrade import (
        _force_refresh_brokerage,
        SNAPTRADE_CRON_FORCE_REFRESH_SETTLE_SECONDS,
    )

    refreshed = 0
    for row in rows:
        user_id = row["user_id"]
        snaptrade_account_id = row["snaptrade_account_id"]
        try:
            ok_r, msg_r, _rem = _force_refresh_brokerage(user_id, snaptrade_account_id)
            if ok_r:
                refreshed += 1
            print(
                f"User {user_id} ({row.get('account_name') or snaptrade_account_id}): "
                f"force-refresh {'ok' if ok_r else 'skipped'} — {msg_r}"
            )
        except Exception as exc:
            print(
                f"User {user_id} ({snaptrade_account_id}): force-refresh raised "
                f"(non-fatal): {exc}",
                file=sys.stderr,
            )

    if refreshed:
        import time
        print(
            f"Requested {refreshed} broker repoll(s); waiting "
            f"{SNAPTRADE_CRON_FORCE_REFRESH_SETTLE_SECONDS}s for SnapTrade to settle…"
        )
        time.sleep(SNAPTRADE_CRON_FORCE_REFRESH_SETTLE_SECONDS)
    return refreshed


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
    from app.upload import (
        _upload_github_config_ok,
        merge_and_push_seeds_batch,
    )

    if not snaptrade_enabled():
        print("SnapTrade is not configured (missing env or SDK). Exiting.", file=sys.stderr)
        return 1

    client = _get_snaptrade_client()
    if client is None:
        print("Could not build SnapTrade client. Exiting.", file=sys.stderr)
        return 1

    # Market-close pass: ask SnapTrade to repoll every broker BEFORE reading, so
    # brokers it does not poll in real-time (Schwab) surface the day's fills the
    # same evening. Billed per call, so gated behind --force-refresh.
    force_refresh = _force_refresh_enabled()
    if force_refresh:
        print(f"Force-refresh pass: requesting broker repoll for {len(rows)} account(s).")
        _force_refresh_all(rows)

    total = len(rows)
    succeeded = 0
    broken = 0
    errors = 0

    routine_days = _routine_lookback_days()
    full_days = SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS

    # Each account is synced with defer_push=True (fetch + normalize, NO
    # commit). We collect every account's frames and push ONE batched commit
    # at the end. Rationale: a per-account push fanned this cron out into ~14
    # GitHub commits a night → ~14 workflow runs (most instantly cancelled by
    # cancel-in-progress). One commit = one dbt build. Monotonic-merge
    # semantics are preserved because the batch folds accounts in the same
    # order sequential pushes did.
    batch_entries = []

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
            res = _sync_one_connection(
                user_id, row, lookback_days=lookback, defer_push=True,
            )
        except Exception as exc:
            errors += 1
            print(
                f"User {user_id} ({snaptrade_account_id}): unexpected sync error: {exc}",
                file=sys.stderr,
            )
            continue

        if res["ok"]:
            succeeded += 1
            print(
                f"User {user_id} ({row.get('account_name') or snaptrade_account_id}): "
                f"{res['history_rows']} history, {res['current_rows']} positions"
            )
            frames = res.get("frames")
            if frames and frames.get("current_df") is not None:
                batch_entries.append(frames)
        else:
            err = res["error"] or "unknown"
            if err == "connection_broken":
                broken += 1
                print(
                    f"User {user_id} ({snaptrade_account_id}): "
                    "broker connection needs reconnect (flagged in app)",
                    file=sys.stderr,
                )
                _notify_connection_dropped(user_id, snaptrade_account_id, row)
            else:
                errors += 1
                print(
                    f"User {user_id} ({snaptrade_account_id}): sync failed: {err}",
                    file=sys.stderr,
                )

    # Single batched push for every account synced this run.
    pushed_note = ""
    if batch_entries:
        ok_cfg, cfg_err = _upload_github_config_ok()
        if not ok_cfg:
            print(
                f"WARNING: {len(batch_entries)} synced account(s) did not reach "
                f"GitHub. Reason: {cfg_err or 'GitHub seed push not configured.'}",
                file=sys.stderr,
            )
        else:
            n = len(batch_entries)
            commit_message = _batch_commit_message(batch_entries, force_refresh=force_refresh)
            try:
                ok, err, _sha, no_changes, n_pushed = merge_and_push_seeds_batch(
                    batch_entries, commit_message=commit_message,
                )
            except Exception as exc:
                ok, err, no_changes, n_pushed = False, str(exc), False, 0
            if ok and not no_changes:
                pushed_note = f", 1 batched push ({n_pushed} accounts) → GitHub"
            elif ok and no_changes:
                pushed_note = f", no changes across {n} accounts (no push)"
            else:
                pushed_note = f", batched push FAILED: {str(err)[:160]}"
                print(f"WARNING: batched seed push failed: {err}", file=sys.stderr)

    mode = "market-close force-refresh" if force_refresh else "backstop"
    print(
        f"SnapTrade {mode} sync summary: {succeeded}/{total} succeeded, "
        f"{broken} broken connections, {errors} errors{pushed_note}"
    )

    if total > 0 and succeeded == 0:
        return 1
    return 0


def _batch_commit_message(entries, *, force_refresh=False):
    """Human-readable one-liner + per-account detail for the batched commit."""
    n = len(entries)
    kind = "market-close force-refresh" if force_refresh else "nightly backstop"
    header = f"SnapTrade {kind} sync: {n} account{'s' if n != 1 else ''}"
    lines = [header]
    for e in entries:
        acct = e.get("account_name") or "?"
        cur = e.get("current_df")
        cur_n = 0 if cur is None else len(cur)
        if e.get("skip_history"):
            lines.append(f"- {acct}: positions only ({cur_n} lines)")
        else:
            hist = e.get("history_df")
            hist_n = 0 if hist is None else len(hist)
            lines.append(f"- {acct}: {hist_n} tx, {cur_n} open lines")
    return "\n".join(lines)


if __name__ == "__main__":
    sys.exit(main())
