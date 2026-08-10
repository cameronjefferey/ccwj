"""Daily reverse-trial lifecycle cron (see app/plan.py for the state model).

Run on Render as ``python -m app.plan_lifecycle_cli`` (cron
``happytrader-plan-lifecycle`` in app/render.yaml). One pass a day over every
``plan='trial'`` user whose clock is running:

- day 23+  → "one week of full access left" email
- day 30+  → "your mirror is now frozen" email (freeze itself is derived from
             the clock — no state flip needed; syncs are already blocked by
             the ``user_sync_allowed`` chokepoint in ``_sync_one_connection``)
- day 53+  → "broker connection removed in 7 days" email
- day 60+  → PERFORM the disconnect (SnapTrade authorizations removed, the
             SnapTrade user deleted so aggregator per-account billing stops,
             local snaptrade rows deleted, broker_tenants marked
             'disconnected' but KEPT — warehouse data is never purged), then
             the "disconnected" email

Every email is idempotent via ``record_email_send`` — the dedupe key embeds
the trial episode anchor (``trial_started_at`` date), so a reset trial clock
starts a fresh episode and can email again. The disconnect itself is
naturally idempotent: a second pass finds no snaptrade_accounts rows and
no-ops. Admins and the demo user are excluded (they are exempt from freezing
in the first place — app/plan.py).

Emails are TRANSACTIONAL account-state notices (no opt-out), same class as
``connection_dropped``.
"""
import sys
from datetime import datetime, timezone

from app.plan import TRIAL_DAYS, GRACE_DAYS

# Milestone day offsets (>= comparisons so a skipped cron day still catches up)
WEEK_LEFT_DAY = TRIAL_DAYS - 7        # 23
FROZEN_DAY = TRIAL_DAYS               # 30
DISCONNECT_WARNING_DAY = TRIAL_DAYS + GRACE_DAYS - 7  # 53
DISCONNECT_DAY = TRIAL_DAYS + GRACE_DAYS              # 60


def _list_running_trials():
    """Every trial user whose clock is running, minus demo/admins."""
    from app.db import fetch_all
    from app.models import is_admin
    from app.utils import DEMO_USERNAME

    rows = fetch_all(
        "SELECT id AS user_id, username, email, trial_started_at "
        "FROM users "
        "WHERE plan = 'trial' AND trial_started_at IS NOT NULL "
        "ORDER BY id",
    )
    out = []
    for r in rows or []:
        uname = (r.get("username") or "").strip().lower()
        if uname == DEMO_USERNAME or is_admin(uname):
            continue
        out.append(r)
    return out


def _days_elapsed(ts, now=None):
    now = now or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max(0, (now - ts).days)


def _episode_anchor(ts):
    return ts.date().isoformat()


def _send_milestone(kind, rec, send_fn, **kwargs):
    """Dedupe + send one lifecycle email. Returns 'sent' | 'skipped' | 'no_email'."""
    from app.models import record_email_send

    email = (rec.get("email") or "").strip()
    if not email:
        return "no_email"
    dedupe_key = f"{rec['user_id']}:{_episode_anchor(rec['trial_started_at'])}"
    if not record_email_send(kind, dedupe_key, user_id=rec["user_id"], to_email=email):
        return "skipped"
    try:
        send_fn(to=email, username=rec["username"], **kwargs)
        return "sent"
    except Exception as exc:
        print(f"User {rec['user_id']}: {kind} email failed: {exc}", file=sys.stderr)
        return "skipped"


def disconnect_user_brokerages(user_id):
    """Remove every SnapTrade authorization for a user, delete their SnapTrade
    user (stops aggregator billing), clean local rows, and mark
    broker_tenants disconnected (rows kept — tenancy + warehouse survive).

    Order matters: revoke authorizations at SnapTrade FIRST (needs the stored
    user secret), then delete the SnapTrade user, then drop local rows. Any
    SnapTrade API failure is logged and the local cleanup proceeds — the
    delete_snap_trade_user call is the billing backstop, and a leftover
    authorization with no SnapTrade user is inert.

    Returns the number of local account rows removed.
    """
    from app.models import (
        get_snaptrade_accounts,
        get_snaptrade_user,
        mark_broker_tenants_disconnected,
        remove_snaptrade_account,
        remove_snaptrade_user,
    )
    from app.snaptrade import _get_snaptrade_client

    acc_rows = get_snaptrade_accounts(user_id) or []
    snap = get_snaptrade_user(user_id)
    client = None
    try:
        client = _get_snaptrade_client()
    except Exception as exc:
        print(f"User {user_id}: SnapTrade client unavailable ({exc}); "
              "cleaning local rows only.", file=sys.stderr)

    if snap and client:
        for row in acc_rows:
            # Mirror the interactive disconnect route: prefer the cached
            # brokerage_authorization_id, fall back to the account id.
            auth_id = (
                row.get("brokerage_authorization_id")
                or row["snaptrade_account_id"]
            )
            try:
                client.connections.remove_brokerage_authorization(
                    user_id=snap["snaptrade_user_id"],
                    user_secret=snap["snaptrade_secret"],
                    authorization_id=auth_id,
                )
                print(f"User {user_id}: removed authorization {auth_id}")
            except Exception as exc:
                print(f"User {user_id}: remove_brokerage_authorization "
                      f"({auth_id}) failed: {exc}", file=sys.stderr)
        try:
            client.authentication.delete_snap_trade_user(
                user_id=snap["snaptrade_user_id"]
            )
            print(f"User {user_id}: SnapTrade user deleted")
        except Exception as exc:
            print(f"User {user_id}: delete_snap_trade_user failed: {exc}",
                  file=sys.stderr)

    removed = 0
    for row in acc_rows:
        try:
            remove_snaptrade_account(user_id, row["snaptrade_account_id"])
            removed += 1
        except Exception as exc:
            print(f"User {user_id}: local account row removal failed: {exc}",
                  file=sys.stderr)
    if snap:
        try:
            remove_snaptrade_user(user_id)
        except Exception as exc:
            print(f"User {user_id}: snaptrade_connections cleanup failed: {exc}",
                  file=sys.stderr)
    mark_broker_tenants_disconnected(user_id)
    return removed


def run_plan_lifecycle(now=None):
    from datetime import timedelta

    from app.email import (
        app_base_url,
        send_disconnect_warning_email,
        send_disconnected_email,
        send_trial_frozen_email,
        send_trial_week_left_email,
    )
    pricing_url = f"{app_base_url()}/pricing"
    counts = {"week_left": 0, "frozen": 0, "warning": 0, "disconnected": 0}

    for rec in _list_running_trials():
        started = rec["trial_started_at"]
        days = _days_elapsed(started, now=now)
        frozen_on = (started + timedelta(days=FROZEN_DAY)).date().isoformat()
        disconnect_on = (started + timedelta(days=DISCONNECT_DAY)).date().isoformat()

        if WEEK_LEFT_DAY <= days < FROZEN_DAY:
            if _send_milestone(
                "plan_trial_week_left", rec, send_trial_week_left_email,
                days_left=max(1, TRIAL_DAYS - days), frozen_on=frozen_on,
                pricing_url=pricing_url,
            ) == "sent":
                counts["week_left"] += 1

        if FROZEN_DAY <= days < DISCONNECT_DAY:
            if _send_milestone(
                "plan_trial_frozen", rec, send_trial_frozen_email,
                disconnect_on=disconnect_on, pricing_url=pricing_url,
            ) == "sent":
                counts["frozen"] += 1

        if DISCONNECT_WARNING_DAY <= days < DISCONNECT_DAY:
            if _send_milestone(
                "plan_disconnect_warning", rec, send_disconnect_warning_email,
                disconnect_on=disconnect_on, pricing_url=pricing_url,
            ) == "sent":
                counts["warning"] += 1

        if days >= DISCONNECT_DAY:
            # State-driven, not dedupe-driven: attempt the disconnect whenever
            # the user still HAS SnapTrade rows (self-healing if a previous
            # run died mid-way; a completed disconnect leaves no rows, so
            # later passes no-op). CSV-only users have nothing to disconnect
            # and correctly get no "connection removed" email. Only the EMAIL
            # is deduped per episode.
            from app.models import get_snaptrade_accounts
            if get_snaptrade_accounts(rec["user_id"]):
                removed = disconnect_user_brokerages(rec["user_id"])
                print(f"User {rec['user_id']}: disconnected at day {days} "
                      f"({removed} account rows removed)")
                counts["disconnected"] += 1
                _send_milestone(
                    "plan_disconnected", rec, send_disconnected_email,
                    pricing_url=pricing_url,
                )

    print(
        "plan_lifecycle: "
        f"{counts['week_left']} week-left, {counts['frozen']} frozen, "
        f"{counts['warning']} disconnect-warnings, "
        f"{counts['disconnected']} disconnected"
    )
    return counts


def main(argv=None):
    from app.models import init_db
    init_db()
    run_plan_lifecycle()
    return 0


if __name__ == "__main__":
    sys.exit(main())
