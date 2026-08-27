"""Early-brokerage cohort: first users of a SnapTrade broker we haven't modeled.

SnapTrade can connect ~20 brokerages. Dedicated dbt adapters exist only for
schwab / alpaca / fidelity / interactive (IBKR); everything else flows
through ``stg_broker_other_*``. The first ``EARLY_BROKER_USER_CAP`` users
who connect an unmodeled brokerage get a thank-you note and six months
of HappyTrader Pro free (Stripe Checkout ``trial_period_days``, Pro
price only — this Stripe account is shared with sibling products, so we
do not use an account-wide coupon). An optional ``EARLY_BROKER_PROMO_CODE``
can still be shown if you create a matching Stripe promotion code.

The cohort flag is stamped ONCE at first connect and kept, so the note
still shows after the 11th user of that brokerage arrives.
"""
from __future__ import annotations

import os
import re

# Must stay aligned with dbt/macros/broker_slug_from_account.sql known_brokers().
MODELED_BROKER_KEYS = frozenset({"schwab", "alpaca", "fidelity", "interactive"})
EARLY_BROKER_USER_CAP = 10
EARLY_BROKER_TRIAL_DAYS_DEFAULT = 180

_DISPLAY = {
    "schwab": "Charles Schwab",
    "fidelity": "Fidelity",
    "vanguard": "Vanguard",
    "robinhood": "Robinhood",
    "interactive": "Interactive Brokers",
    "ibkr": "Interactive Brokers",
    "alpaca": "Alpaca",
    "wealthsimple": "Wealthsimple",
    "etrade": "E*TRADE",
    "tastytrade": "tastytrade",
    "webull": "Webull",
    "coinbase": "Coinbase",
}


def broker_key(raw) -> str:
    """Normalize a SnapTrade institution name / slug to the dbt first-token key."""
    s = re.sub(r"[^a-z0-9]+", " ", (raw or "").strip().lower()).strip()
    if not s:
        return ""
    if s.startswith("charles schwab") or s.split()[0] == "schwab":
        return "schwab"
    if s.startswith("interactive") or s.split()[0] in ("ibkr", "ib"):
        return "interactive"
    return s.split()[0]


def is_modeled_broker(raw) -> bool:
    return broker_key(raw) in MODELED_BROKER_KEYS


def display_name(raw) -> str:
    key = broker_key(raw)
    if key in _DISPLAY:
        return _DISPLAY[key]
    label = (raw or "").strip()
    return label or "your broker"


def promo_code() -> str | None:
    code = (os.environ.get("EARLY_BROKER_PROMO_CODE") or "").strip()
    return code or None


def trial_days() -> int:
    """Days of Pro free for the early-broker cohort. 0 disables the trial."""
    raw = (os.environ.get("EARLY_BROKER_TRIAL_DAYS") or str(EARLY_BROKER_TRIAL_DAYS_DEFAULT)).strip()
    try:
        n = int(raw)
    except ValueError:
        n = EARLY_BROKER_TRIAL_DAYS_DEFAULT
    return n if n > 0 else 0


def pro_trial_days_for_user(user_id) -> int | None:
    """Checkout ``trial_period_days`` for this user, or None."""
    note = early_broker_notice_for_user(user_id)
    if not note:
        return None
    days = note.get("trial_days")
    return int(days) if days else None


def notice_from_accounts(rows, *, promo=None) -> dict | None:
    """Template context if this user is in the early cohort for any linked broker."""
    cohort = [r for r in (rows or []) if r.get("early_broker_cohort")]
    if not cohort:
        return None
    raw = cohort[0].get("broker_slug") or ""
    code = promo if promo is not None else promo_code()
    days = trial_days()
    return {
        "broker_key": broker_key(raw) or "broker",
        "broker_name": display_name(raw),
        "promo_code": (code or "").strip() or None,
        "trial_days": days or None,
        "trial_months": (days // 30) if days else None,
    }


def early_broker_notice_for_user(user_id) -> dict | None:
    if not user_id:
        return None
    try:
        from app.utils import is_demo_user
        if is_demo_user():
            return None
    except Exception:
        pass
    from app.models import get_snaptrade_accounts
    try:
        return notice_from_accounts(get_snaptrade_accounts(user_id))
    except Exception:
        return None


def count_users_for_broker_key(key, rows) -> int:
    if not key:
        return 0
    return len({
        r.get("user_id")
        for r in (rows or [])
        if r.get("user_id") is not None and broker_key(r.get("broker_slug")) == key
    })


def maybe_stamp_early_broker_cohort(user_id, snaptrade_account_id, broker_slug) -> bool:
    """Mark this account as early-cohort if the brokerage is unmodeled and
    still under the user cap. Returns True when stamped. Fail-open."""
    key = broker_key(broker_slug)
    if not user_id or not snaptrade_account_id or not key or key in MODELED_BROKER_KEYS:
        return False
    from app.db import execute, fetch_all
    try:
        rows = fetch_all(
            "SELECT DISTINCT user_id, broker_slug FROM snaptrade_accounts"
        )
        if count_users_for_broker_key(key, rows) > EARLY_BROKER_USER_CAP:
            return False
        execute(
            "UPDATE snaptrade_accounts SET early_broker_cohort = TRUE "
            "WHERE user_id = %s AND snaptrade_account_id = %s",
            (user_id, snaptrade_account_id),
        )
        return True
    except Exception:
        return False


def backfill_early_broker_cohort() -> int:
    """Stamp the first ``EARLY_BROKER_USER_CAP`` users per unmodeled broker.

    Idempotent. Used at migrate time so people who connected Robinhood (etc.)
    before this note shipped still get the thank-you.
    """
    from app.db import execute, fetch_all

    rows = fetch_all(
        "SELECT user_id, snaptrade_account_id, broker_slug, created_at, "
        "early_broker_cohort FROM snaptrade_accounts"
    ) or []
    by_key: dict[str, list] = {}
    for r in rows:
        key = broker_key(r.get("broker_slug"))
        if not key or key in MODELED_BROKER_KEYS:
            continue
        by_key.setdefault(key, []).append(r)

    stamped = 0
    for group in by_key.values():
        group.sort(key=lambda r: r.get("created_at") or 0)
        early_uids = []
        for r in group:
            uid = r.get("user_id")
            if uid is None or uid in early_uids:
                continue
            early_uids.append(uid)
            if len(early_uids) >= EARLY_BROKER_USER_CAP:
                break
        early_set = set(early_uids)
        for r in group:
            if r.get("user_id") not in early_set or r.get("early_broker_cohort"):
                continue
            execute(
                "UPDATE snaptrade_accounts SET early_broker_cohort = TRUE "
                "WHERE user_id = %s AND snaptrade_account_id = %s",
                (r.get("user_id"), r.get("snaptrade_account_id")),
            )
            stamped += 1
    return stamped
