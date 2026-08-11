"""Reverse-trial plan state — derivation, gating, and UI banner data.

The billing model (Aug 2026): every new signup is ``plan='trial'`` and gets
the FULL product, no card. The trial clock starts at FIRST DATA (first
successful broker sync or first CSV upload), not at signup — the 30 days
exist to let the trader live through a monthly options cycle with real data.
At day 30 the account FREEZES (syncs stop; every page stays readable). For
the next 30 days the SnapTrade connection stays alive so subscribing resumes
instantly; at day 60 the daily lifecycle cron (``app/plan_lifecycle_cli.py``)
removes the brokerage authorizations and deletes the SnapTrade user so
aggregator per-account billing stops. Warehouse data is NEVER purged — a
returning subscriber reconnects and their history is still there.

Only ``users.plan`` (trial|beta|active) and ``users.trial_started_at`` are
STORED; everything else is DERIVED from the clock, so no cron has to flip
states. ``plan='active'`` is the entire Stripe seam: ``app/billing.py`` sets
it when a subscription starts and reverts it on cancellation (with
``trial_started_at`` intact, the account lands straight back in frozen).

Exemptions: admins, the shared demo user, and grandfathered beta users never
freeze. All derivation FAILS OPEN (a DB hiccup must never block a legitimate
sync or lock a page).
"""
import logging
from datetime import datetime, timezone

from flask import flash, redirect, request, url_for, jsonify
from flask_login import current_user

from app.db import execute, fetch_one

_log = logging.getLogger(__name__)

TRIAL_DAYS = 30
GRACE_DAYS = 30  # frozen-but-connected window after the trial ends

PLAN_TRIAL = "trial"
PLAN_BETA = "beta"
PLAN_ACTIVE = "active"
VALID_PLANS = (PLAN_TRIAL, PLAN_BETA, PLAN_ACTIVE)

# Derived states
STATE_NO_DATA = "no_data"          # trial, clock not started (no data yet)
STATE_TRIALING = "trialing"        # trial, day < 30
STATE_FROZEN = "frozen"            # trial, day 30-59 — syncs stop, data readable
STATE_GRACE_EXPIRED = "grace_expired"  # trial, day 60+ — broker disconnected
STATE_ACTIVE = "active"
STATE_BETA = "beta"

# Banner-only pseudo-state: a paying subscriber who has cancelled and is
# riding out the period they already paid for. Never returned by
# derive_plan_state (they're a full 'active' user until the period ends).
STATE_ACTIVE_CANCELING = "active_canceling"

_SYNC_BLOCKED_STATES = (STATE_FROZEN, STATE_GRACE_EXPIRED)


def _as_date(ts):
    """Date part of a timestamp, or None."""
    try:
        return ts.date() if ts is not None else None
    except AttributeError:
        return None


def _days_since(ts, now=None):
    if ts is None:
        return None
    now = now or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max(0, (now - ts).days)


def derive_plan_state(plan, trial_started_at, *, exempt=False, now=None):
    """Pure derivation — no I/O. ``exempt`` covers admins and the demo user
    (treated as beta: never freezes)."""
    if exempt:
        return STATE_BETA
    plan = (plan or PLAN_TRIAL).strip().lower()
    if plan == PLAN_BETA:
        return STATE_BETA
    if plan == PLAN_ACTIVE:
        return STATE_ACTIVE
    days = _days_since(trial_started_at, now=now)
    if days is None:
        return STATE_NO_DATA
    if days < TRIAL_DAYS:
        return STATE_TRIALING
    if days < TRIAL_DAYS + GRACE_DAYS:
        return STATE_FROZEN
    return STATE_GRACE_EXPIRED


def get_user_plan_row(user_id):
    """``{plan, trial_started_at, username}`` plus the Stripe mirror columns
    (see app/billing.py), or None. Never raises.

    The Stripe columns are selected in the same query so the per-request
    banner costs one read, with a narrow fallback for databases where
    ``_migrate_users_stripe_columns`` hasn't run yet — otherwise a missing
    column would fail the whole read and silently exempt every user.
    """
    try:
        return fetch_one(
            "SELECT plan, trial_started_at, username, subscription_status, "
            "subscription_cancel_at_period_end, subscription_current_period_end "
            "FROM users WHERE id = %s",
            (user_id,),
        )
    except Exception as exc:
        _log.warning("get_user_plan_row(%s) wide read failed: %s", user_id, exc)
    try:
        return fetch_one(
            "SELECT plan, trial_started_at, username FROM users WHERE id = %s",
            (user_id,),
        )
    except Exception as exc:
        _log.warning("get_user_plan_row(%s) failed: %s", user_id, exc)
        return None


def _is_exempt_username(username):
    from app.models import is_admin
    from app.utils import DEMO_USERNAME

    uname = (username or "").strip().lower()
    if uname == DEMO_USERNAME:
        return True
    try:
        return is_admin(uname)
    except Exception:
        return False


def plan_state(user_id, now=None):
    """Derived plan state for a user id. FAILS OPEN to beta (exempt) when the
    row can't be read — a DB hiccup must never freeze a legitimate user."""
    row = get_user_plan_row(user_id)
    if not row:
        return STATE_BETA
    return derive_plan_state(
        row.get("plan"),
        row.get("trial_started_at"),
        exempt=_is_exempt_username(row.get("username")),
        now=now,
    )


def user_sync_allowed(user_id):
    """False only when the trial has lapsed (frozen / grace-expired). This is
    the MANDATORY chokepoint check — ``_sync_one_connection`` calls it, so
    webhook, cron, and manual syncs are all covered."""
    return plan_state(user_id) not in _SYNC_BLOCKED_STATES


def start_trial_clock(user_id):
    """Stamp ``trial_started_at`` at the first-data moment (first successful
    sync or first CSV upload). Once-only by construction (WHERE ... IS NULL)
    and a no-op for beta/active users. Best-effort — never raises."""
    try:
        execute(
            "UPDATE users SET trial_started_at = NOW() "
            "WHERE id = %s AND trial_started_at IS NULL AND plan = %s",
            (user_id, PLAN_TRIAL),
        )
    except Exception as exc:
        _log.warning("start_trial_clock(%s) failed: %s", user_id, exc)


def set_user_plan(user_id, plan):
    """Admin / future-Stripe lever. Returns True on success."""
    plan = (plan or "").strip().lower()
    if plan not in VALID_PLANS:
        return False
    try:
        execute(
            "UPDATE users SET plan = %s, plan_updated_at = NOW() WHERE id = %s",
            (plan, user_id),
        )
        return True
    except Exception as exc:
        _log.warning("set_user_plan(%s, %s) failed: %s", user_id, plan, exc)
        return False


def reset_trial_clock(user_id):
    """Admin lever: restart the 30-day window from now (manual comp/extend)."""
    try:
        execute(
            "UPDATE users SET trial_started_at = NOW(), plan_updated_at = NOW() "
            "WHERE id = %s",
            (user_id,),
        )
        return True
    except Exception as exc:
        _log.warning("reset_trial_clock(%s) failed: %s", user_id, exc)
        return False


def plan_status_for_banner(user_id, now=None):
    """Everything base.html needs to render the trial/freeze banner, or None
    when no banner applies (beta / active / no-data / anonymous)."""
    row = get_user_plan_row(user_id)
    if not row:
        return None
    state = derive_plan_state(
        row.get("plan"),
        row.get("trial_started_at"),
        exempt=_is_exempt_username(row.get("username")),
        now=now,
    )
    # Subscribers only get a banner when their subscription is winding down:
    # a pending cancellation means the mirror freezes on a known date, and
    # that's exactly the kind of thing the product should say out loud rather
    # than spring on them.
    if state == STATE_ACTIVE:
        if not row.get("subscription_cancel_at_period_end"):
            return None
        return {
            "state": STATE_ACTIVE_CANCELING,
            "days_elapsed": None,
            "days_left": None,
            "frozen_on": _as_date(row.get("subscription_current_period_end")),
            "disconnect_on": None,
            "disconnect_in_days": None,
        }
    if state not in (STATE_TRIALING, STATE_FROZEN, STATE_GRACE_EXPIRED):
        return None
    days = _days_since(row.get("trial_started_at"), now=now) or 0
    started = row.get("trial_started_at")
    from datetime import timedelta

    frozen_on = (started + timedelta(days=TRIAL_DAYS)).date() if started else None
    disconnect_on = (
        (started + timedelta(days=TRIAL_DAYS + GRACE_DAYS)).date() if started else None
    )
    return {
        "state": state,
        "days_elapsed": days,
        "days_left": max(0, TRIAL_DAYS - days),
        "frozen_on": frozen_on,
        "disconnect_on": disconnect_on,
        "disconnect_in_days": max(0, TRIAL_DAYS + GRACE_DAYS - days),
    }


def plan_block_writes(action: str = "this action"):
    """Short-circuit data-write POST handlers when the signed-in user's trial
    has lapsed. Mirrors ``demo_block_writes`` in app/utils.py: returns a
    response (redirect/JSON) when blocked, None when the caller should
    continue. Applied to broker connect / sync / refresh and CSV upload —
    frozen accounts stay fully READABLE, they just can't ingest new data."""
    try:
        if not current_user.is_authenticated:
            return None
        state = plan_state(current_user.id)
    except Exception:
        return None
    if state not in _SYNC_BLOCKED_STATES:
        return None

    msg = (
        f"Your free trial has ended, so {action} is paused. "
        "Your data is safe and every page stays readable — see the Pricing "
        "page to keep your mirror updating."
    )

    wants_json = (
        request.path.startswith("/api/")
        or request.accept_mimetypes.best == "application/json"
        or (request.headers.get("X-Requested-With", "") == "XMLHttpRequest")
    )
    if wants_json:
        return jsonify({"error": "plan_frozen", "message": msg}), 403

    flash(msg, "warning")
    return redirect(url_for("pricing"))
