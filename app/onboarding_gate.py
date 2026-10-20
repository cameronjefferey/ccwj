"""Whether the first-connect survey should appear.

The survey is first-time onboarding. It is not a step when the person
has already saved answers, skipped the survey, or already had a linked
brokerage or uploaded book before this connect. How many accounts exist
*after* the new connection is saved is not the signal: a first connect
and a second connect both leave rows on the profile.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


# A brand-new connection is still "this connect" for the rest of the
# processing wait (the warehouse build is often 15–40 minutes). An
# account linked before that window is a prior connection.
_PRIOR_ACCOUNT_GRACE = timedelta(hours=12)


def _as_utc(value):
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def account_is_established(row) -> bool:
    """True when this SnapTrade row has already been through a sync."""
    if not row:
        return False
    if row.get("first_sync_completed"):
        return True
    if row.get("holdings_last_successful_sync") or row.get("last_sync_at"):
        return True
    return False


def account_predates_this_connect(row, *, now=None) -> bool:
    """True when the row was linked well before the current connect."""
    created = _as_utc((row or {}).get("created_at"))
    if created is None:
        return False
    current = _as_utc(now) or datetime.now(timezone.utc)
    return current - created > _PRIOR_ACCOUNT_GRACE


def _saved_onboarding(user_id) -> bool:
    """True when a survey row exists, including a stored skip."""
    if not user_id:
        return False
    try:
        from app.models import get_onboarding_response
        return get_onboarding_response(user_id) is not None
    except Exception:
        return False


def first_connect_needs_quiz(user_id, prior_accounts, prior_tenant_ids=None) -> bool:
    """True only for a first brokerage connect that has not been onboarded.

    ``prior_accounts`` and ``prior_tenant_ids`` are the rows that existed
    *before* this portal return wrote the new connection. A saved survey,
    any of those accounts, or any earlier tenant (including a CSV upload)
    means onboarding already happened.
    """
    if not user_id:
        return False
    if _saved_onboarding(user_id):
        return False
    if prior_accounts:
        return False
    prior_tenants = [t for t in (prior_tenant_ids or []) if t]
    if prior_tenants:
        return False
    return True


def onboarding_quiz_still_pending(user_id) -> bool:
    """Re-check at page render, so a stale ``?first=1`` cannot reopen it.

    A just-added account (no completed sync, created inside the grace
    window) does not count as "already onboarded". An older account, a
    finished sync, a saved survey, or a tenant that is not one of those
    new SnapTrade rows does.
    """
    if not user_id:
        return False
    if _saved_onboarding(user_id):
        return False
    try:
        from app.models import get_snaptrade_accounts, get_tenant_ids_for_user
        accounts = get_snaptrade_accounts(user_id) or []
        tenant_ids = [t for t in (get_tenant_ids_for_user(user_id) or []) if t]
    except Exception:
        # Fail closed against re-showing the survey. A first connect
        # with a healthy read still returns True below.
        return False
    if any(account_is_established(row) or account_predates_this_connect(row) for row in accounts):
        return False
    snap_ids = {
        str(row.get("tenant_id") or "")
        for row in accounts
        if row.get("tenant_id")
    }
    if any(str(tid) not in snap_ids for tid in tenant_ids):
        return False
    return True
