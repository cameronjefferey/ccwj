"""Who may spend paid LLM tokens.

The AI add-on is a SECOND Stripe subscription. It must never be inferred
from ``users.plan`` — a Pro subscriber without the add-on stays on
Haiku/Flash, and an AI cancellation must not freeze the mirror.

Fail CLOSED: a missing column, a DB hiccup, or an unknown user does not
unlock Sonnet/Opus. Admins and grandfathered beta users are the only
non-Stripe exceptions. The shared demo user is never treated as paid
(Ask AI is already write-blocked for demo).
"""
from __future__ import annotations

import logging

from app.db import fetch_one
from app.plan import PLAN_BETA, get_user_plan_row

_log = logging.getLogger(__name__)


def user_can_use_paid_llm(user_id) -> bool:
    """True when this user may select and invoke a ``tier='paid'`` model."""
    if user_id is None:
        return False
    try:
        from app.models import is_admin
        from app.utils import DEMO_USERNAME

        row = get_user_plan_row(user_id)
        username = ((row or {}).get("username") or "").strip().lower()
        if username == DEMO_USERNAME:
            return False
        if username and is_admin(username):
            return True
        if row and (row.get("plan") or "").strip().lower() == PLAN_BETA:
            return True

        from app.billing import PAYING_STATUSES

        ai = fetch_one(
            "SELECT ai_subscription_status FROM users WHERE id = %s",
            (user_id,),
        )
        if not ai:
            return False
        return (ai.get("ai_subscription_status") or "") in PAYING_STATUSES
    except Exception as exc:
        _log.warning("user_can_use_paid_llm(%s) failed closed: %s", user_id, exc)
        return False
