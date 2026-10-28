"""Whether the first-connect survey should appear.

The survey is first-time onboarding. It is not a step when the person
has already saved answers, skipped the survey, or already had a real
brokerage or uploaded book before this connect. Alpaca Paper does not
count as that book: a paper-only first connect still shows the survey,
and a later real brokerage shows it too until they save or skip.
How many accounts exist *after* the new connection is saved is not the
signal: a first connect and a second connect both leave rows on the
profile.
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
    """True when this SnapTrade row has finished its first history sync.

    A background pull stamps ``last_sync_at`` and
    ``holdings_last_successful_sync`` as soon as it reads the broker,
    including a failed attempt and a holdings-only read while history
    is still pending. Those timestamps are not proof the person already
    finished onboarding. Only ``first_sync_completed`` is. An older row
    that predates the flag (or never had it set) is still "already
    here" via ``account_predates_this_connect``, and any account that
    existed before this portal return skips the quiz in
    ``first_connect_needs_quiz``.
    """
    if not row:
        return False
    return bool(row.get("first_sync_completed"))


def account_still_awaiting_first_sync(row) -> bool:
    """True when this row is an unfinished connect from the last 12 hours.

    ``first_sync_completed`` defaults to false and is not backfilled, so
    an older account can sit on false forever. That is not a recovery
    of the portal return happening now. A missing ``created_at`` still
    counts as awaiting, matching rows written before the column was
    read by this check.
    """
    if not row:
        return False
    if row.get("first_sync_completed") is None or bool(row.get("first_sync_completed")):
        return False
    if account_predates_this_connect(row):
        return False
    return True


def account_predates_this_connect(row, *, now=None) -> bool:
    """True when the row was linked well before the current connect."""
    created = _as_utc((row or {}).get("created_at"))
    if created is None:
        return False
    current = _as_utc(now) or datetime.now(timezone.utc)
    return current - created > _PRIOR_ACCOUNT_GRACE


def account_is_paper(row) -> bool:
    """True for an Alpaca Paper SnapTrade row. A live Alpaca account is not.

    Paper is practice money. It must not count as "already onboarded"
    for the survey. ``broker_slug`` is included because some rows store
    the brokerage there and not in ``account_name``.
    """
    if not row:
        return False
    from app.paper_accounts import is_paper_row
    if is_paper_row(row):
        return True
    slug = str(row.get("broker_slug") or "").casefold()
    return "alpaca paper" in slug or "alpaca-paper" in slug


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
    """True when this connect still needs the first-time survey.

    ``prior_accounts`` and ``prior_tenant_ids`` are the rows that existed
    *before* this portal return wrote the new connection. A saved survey
    (including a skip), a real brokerage, or a CSV upload means
    onboarding already happened. Alpaca Paper accounts do not: the
    survey still shows on that first paper connect, and again when a
    real brokerage is added later, until they save or skip.
    """
    if not user_id:
        return False
    if _saved_onboarding(user_id):
        return False
    prior = list(prior_accounts or [])
    if any(not account_is_paper(row) for row in prior):
        return False
    paper_ids = {
        str(row.get("tenant_id") or "")
        for row in prior
        if row.get("tenant_id") and account_is_paper(row)
    }
    prior_tenants = [str(t) for t in (prior_tenant_ids or []) if t]
    # A tenant that is not one of those paper rows is a real book
    # (CSV, or a brokerage that is not in the SnapTrade list).
    if any(tid not in paper_ids for tid in prior_tenants):
        return False
    return True


def onboarding_quiz_still_pending(user_id) -> bool:
    """Re-check at page render, so a stale ``?first=1`` cannot reopen it.

    A just-added real account (first history sync not finished, created
    inside the grace window) does not count as "already onboarded",
    even after the background pull stamps a sync time. An older real
    account, a finished first sync on a real account, a saved survey,
    or a tenant that is not one of the SnapTrade rows does. Alpaca
    Paper never counts: a finished paper sync is not the survey.
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
    real_accounts = [row for row in accounts if not account_is_paper(row)]
    if any(
        account_is_established(row) or account_predates_this_connect(row)
        for row in real_accounts
    ):
        return False
    snap_ids = {
        str(row.get("tenant_id") or "")
        for row in accounts
        if row.get("tenant_id")
    }
    if any(str(tid) not in snap_ids for tid in tenant_ids):
        return False
    return True
