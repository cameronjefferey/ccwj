"""Admin overview dashboard — how the site is doing.

Postgres-only (users, plans, connections, feedback, a light page-view log).
No warehouse queries: this page should load even when BigQuery is unhappy.
"""
from __future__ import annotations

import logging
import os
from collections import Counter
from datetime import datetime, timezone

from flask import request

from app.db import execute, fetch_all, fetch_one
from app.plan import (
    STATE_ACTIVE,
    STATE_BETA,
    STATE_FROZEN,
    STATE_GRACE_EXPIRED,
    STATE_NO_DATA,
    STATE_TRIALING,
    _days_since,
    derive_plan_state,
)
from app.utils import DEMO_USERNAME

_log = logging.getLogger(__name__)

PLAN_LABELS = {
    STATE_ACTIVE: "Paying",
    STATE_TRIALING: "Trial (in window)",
    STATE_NO_DATA: "Signed up, no data yet",
    STATE_FROZEN: "Frozen (day 30–59)",
    STATE_GRACE_EXPIRED: "Disconnected (day 60+)",
    STATE_BETA: "Beta / grandfathered",
    "admin": "Admin",
    "demo": "Demo",
}

PLAN_TONE = {
    STATE_ACTIVE: "ok",
    STATE_TRIALING: "info",
    STATE_NO_DATA: "warn",
    STATE_FROZEN: "warn",
    STATE_GRACE_EXPIRED: "bad",
    STATE_BETA: "muted",
    "admin": "muted",
    "demo": "muted",
}

PAGE_LABELS = {
    "weekly_review": "Daily Review",
    "day_detail": "Day review",
    "positions": "Positions",
    "position_detail": "Position detail",
    "accounts": "Accounts",
    "trader_story": "Trader Profile",
    "strategies": "Strategies",
    "sectors": "Sectors",
    "earnings": "Earnings",
    "insights": "AI Insights",
    "upload": "Upload",
    "upload_processing": "Upload processing",
    "profile": "Profile",
    "get_started": "Get started",
    "snaptrade_accounts": "Connected accounts",
    "pricing": "Pricing",
    "faq": "FAQ",
    "signup": "Sign up",
    "index": "Home",
    "feature_detail": "Feature page",
    "contact": "Contact",
}

# Logged-out HTML pages worth counting (interest before signup).
_PUBLIC_ENDPOINTS = frozenset({
    "index", "pricing", "faq", "signup", "feature_detail", "contact",
})

_SKIP_ENDPOINTS = frozenset({
    "static",
    "healthz",
    "healthz_db",
    "api_github_workflow_status",
    "api_nav_symbols",
    "internal_cache_flush",
    "stripe_webhook",
    "snaptrade_webhook",
})
_SKIP_PREFIXES = (
    "/api/", "/internal/", "/webhooks/", "/static/", "/sw.js", "/healthz",
)


def _admin_usernames():
    return {
        u.strip().lower()
        for u in (os.environ.get("ADMIN_USERS", "") or "").split(",")
        if u.strip()
    }


def _q(sql, params=()):
    try:
        return fetch_all(sql, params) or []
    except Exception as exc:
        _log.warning("admin overview query failed: %s", exc)
        return []


def _q1(sql, params=()):
    try:
        return fetch_one(sql, params)
    except Exception as exc:
        _log.warning("admin overview query failed: %s", exc)
        return None


def should_record_page_view(req, response) -> bool:
    """True for an authenticated HTML navigation we want on the dashboard."""
    if req.method != "GET":
        return False
    if response.status_code != 200:
        return False
    if req.headers.get("X-HT-Full"):
        return False
    path = req.path or ""
    if any(path.startswith(p) for p in _SKIP_PREFIXES):
        return False
    ep = req.endpoint or ""
    if not ep or ep in _SKIP_ENDPOINTS or ep.startswith("static") or ep.startswith("admin_"):
        return False
    if ep.endswith("_fragment"):
        return False
    return True


def should_insert_page_view(req, response, *, authenticated: bool, username=None) -> bool:
    """Whether this request should land in usage_events."""
    if not should_record_page_view(req, response):
        return False
    uname = (username or "").strip().lower()
    if uname == DEMO_USERNAME:
        return False
    if authenticated:
        return True
    return (req.endpoint or "") in _PUBLIC_ENDPOINTS


def record_page_view(response) -> None:
    """Best-effort insert. Never raises; never delays the response on failure."""
    try:
        from flask_login import current_user
        authenticated = bool(getattr(current_user, "is_authenticated", False))
        username = getattr(current_user, "username", None) if authenticated else None
        if not should_insert_page_view(
            request, response, authenticated=authenticated, username=username,
        ):
            return
        uid = getattr(current_user, "id", None) if authenticated else None
        execute(
            "INSERT INTO usage_events (user_id, endpoint, path, status_code) "
            "VALUES (%s, %s, %s, %s)",
            (uid, request.endpoint, request.path, int(response.status_code)),
        )
    except Exception as exc:
        _log.debug("usage_events insert skipped: %s", exc)


def _fetch_users():
    rows = _q(
        """
        SELECT u.id, u.username, u.plan, u.trial_started_at,
               u.subscription_status, u.subscription_cancel_at_period_end,
               u.ai_subscription_status,
               up.created_at AS signed_up_at, up.display_name, u.email
        FROM users u
        LEFT JOIN user_profiles up ON up.user_id = u.id
        ORDER BY COALESCE(up.created_at, to_timestamp(0)) DESC
        """
    )
    if rows:
        return rows
    return _q(
        """
        SELECT u.id, u.username, u.plan, u.trial_started_at,
               u.subscription_status, u.subscription_cancel_at_period_end,
               NULL::text AS ai_subscription_status,
               up.created_at AS signed_up_at, up.display_name, u.email
        FROM users u
        LEFT JOIN user_profiles up ON up.user_id = u.id
        ORDER BY COALESCE(up.created_at, to_timestamp(0)) DESC
        """
    )


def _classify_users(user_rows, admin_names):
    mix = Counter()
    real = []
    paying = 0
    ai_addon = 0
    canceling = 0
    for r in user_rows:
        uname = (r.get("username") or "").strip().lower()
        if uname == DEMO_USERNAME:
            mix["demo"] += 1
            r["kind"] = "demo"
            r["plan_state"] = "demo"
            continue
        if uname in admin_names:
            mix["admin"] += 1
            r["kind"] = "admin"
            r["plan_state"] = "admin"
            continue
        st = derive_plan_state(
            r.get("plan"), r.get("trial_started_at"), exempt=False,
        )
        r["kind"] = "real"
        r["plan_state"] = st
        r["trial_days"] = _days_since(r.get("trial_started_at"))
        mix[st] += 1
        real.append(r)
        if st == STATE_ACTIVE:
            paying += 1
            if r.get("subscription_cancel_at_period_end"):
                canceling += 1
        ai_st = (r.get("ai_subscription_status") or "").strip().lower()
        if ai_st in ("active", "trialing"):
            ai_addon += 1
    return {
        "mix": mix,
        "real": real,
        "paying": paying,
        "ai_addon": ai_addon,
        "canceling": canceling,
    }


def _mix_rows(mix):
    order = (
        STATE_ACTIVE, STATE_TRIALING, STATE_NO_DATA, STATE_FROZEN,
        STATE_GRACE_EXPIRED, STATE_BETA, "admin", "demo",
    )
    total = sum(mix.values()) or 1
    out = []
    for key in order:
        n = int(mix.get(key) or 0)
        if n == 0:
            continue
        out.append({
            "key": key,
            "label": PLAN_LABELS.get(key, key),
            "n": n,
            "pct": round(100.0 * n / total, 1),
            "tone": PLAN_TONE.get(key, "muted"),
        })
    return out


def _demo_sql():
    """Exclude the shared demo seat from interest rankings."""
    return "COALESCE(LOWER(u.username), '') <> %s", [DEMO_USERNAME]


def _with_interest_bars(rows):
    """Bar width from unique people when anyone is signed in, else raw views."""
    if not rows:
        return rows
    peak_people = max(int(r.get("users") or 0) for r in rows)
    use = "users" if peak_people else "hits"
    peak = max(int(r.get(use) or 0) for r in rows) or 1
    for r in rows:
        r["label"] = PAGE_LABELS.get(r.get("endpoint") or "", r.get("endpoint") or "—")
        r["pct"] = round(100.0 * int(r.get(use) or 0) / peak, 1)
    return rows


def _symbol_from_position_path(path):
    raw = (path or "").strip()
    if not raw.startswith("/position/"):
        return None
    rest = raw[len("/position/"):].split("/")[0].strip()
    return rest.upper() if rest else None


def _attention(classified, broken, open_feedback, failed_syncs, pending_first_sync):
    items = []
    frozen_n = int(classified["mix"].get(STATE_FROZEN) or 0)
    gone_n = int(classified["mix"].get(STATE_GRACE_EXPIRED) or 0)
    if broken:
        items.append({
            "tone": "bad",
            "title": f"{len(broken)} broker connection{'s' if len(broken) != 1 else ''} broken",
            "detail": "Syncs are failing. People see stale numbers until they reconnect.",
            "href": None,
        })
    if open_feedback:
        items.append({
            "tone": "warn",
            "title": f"{open_feedback} open feedback note{'s' if open_feedback != 1 else ''}",
            "detail": "Unread reports from testers.",
            "href": "admin_feedback",
        })
    if failed_syncs:
        items.append({
            "tone": "warn",
            "title": f"{failed_syncs} failed sync run{'s' if failed_syncs != 1 else ''} in the last 24h",
            "detail": "SnapTrade observation log — check connected accounts.",
            "href": None,
        })
    if pending_first_sync:
        items.append({
            "tone": "info",
            "title": f"{pending_first_sync} account{'s' if pending_first_sync != 1 else ''} still waiting on first sync",
            "detail": "Connected, but warehouse data has not landed yet.",
            "href": None,
        })
    if frozen_n:
        items.append({
            "tone": "warn",
            "title": f"{frozen_n} trial{'s' if frozen_n != 1 else ''} frozen",
            "detail": "Readable, syncs stopped. Day 30–59 win-back window.",
            "href": "admin_users",
        })
    if gone_n:
        items.append({
            "tone": "bad",
            "title": f"{gone_n} trial{'s' if gone_n != 1 else ''} past day 60",
            "detail": "Broker disconnected. History is still in the warehouse.",
            "href": "admin_users",
        })
    if classified["canceling"]:
        items.append({
            "tone": "info",
            "title": f"{classified['canceling']} paying user{'s' if classified['canceling'] != 1 else ''} canceling at period end",
            "detail": "Still have Pro until the paid period ends.",
            "href": "admin_users",
        })
    if not items:
        items.append({
            "tone": "ok",
            "title": "Nothing looks on fire",
            "detail": "No broken connections, open feedback, or frozen trials in this snapshot.",
            "href": None,
        })
    return items


def build_admin_overview():
    """Assemble the /admin dashboard payload. Never raises."""
    admin_names = _admin_usernames()
    user_rows = _fetch_users()
    classified = _classify_users(user_rows, admin_names)
    real = classified["real"]
    real_n = len(real)

    broken = _q(
        """
        SELECT u.username, a.account_name, a.broker_slug,
               a.connection_broken_at, a.last_sync_error
        FROM snaptrade_accounts a
        JOIN users u ON u.id = a.user_id
        WHERE a.connection_broken_at IS NOT NULL
        ORDER BY a.connection_broken_at DESC
        LIMIT 25
        """
    )
    open_fb = _q1(
        "SELECT COUNT(*) AS n FROM feedback WHERE resolved_at IS NULL"
    )
    open_feedback = int((open_fb or {}).get("n") or 0)
    failed = _q1(
        """
        SELECT COUNT(*) AS n FROM snaptrade_sync_observations
        WHERE ok = FALSE AND cron_run_at > NOW() - INTERVAL '24 hours'
        """
    )
    failed_syncs = int((failed or {}).get("n") or 0)
    pending = _q1(
        """
        SELECT COUNT(*) AS n FROM snaptrade_accounts
        WHERE first_sync_completed = FALSE
          AND created_at < NOW() - INTERVAL '1 hour'
        """
    )
    pending_first_sync = int((pending or {}).get("n") or 0)

    demo_sql, demo_params = _demo_sql()
    views_7d = _with_interest_bars(_q(
        f"""
        SELECT e.endpoint, COUNT(*) AS hits,
               COUNT(DISTINCT e.user_id) AS users
        FROM usage_events e
        LEFT JOIN users u ON u.id = e.user_id
        WHERE e.created_at > NOW() - INTERVAL '7 days'
          AND {demo_sql}
        GROUP BY e.endpoint
        ORDER BY users DESC, hits DESC
        LIMIT 15
        """,
        tuple(demo_params),
    ))
    symbol_rows = _q(
        f"""
        SELECT e.path, COUNT(*) AS hits,
               COUNT(DISTINCT e.user_id) AS users
        FROM usage_events e
        LEFT JOIN users u ON u.id = e.user_id
        WHERE e.created_at > NOW() - INTERVAL '7 days'
          AND e.endpoint = 'position_detail'
          AND e.path LIKE '/position/%%'
          AND {demo_sql}
        GROUP BY e.path
        ORDER BY users DESC, hits DESC
        LIMIT 8
        """,
        tuple(demo_params),
    )
    symbols_7d = []
    peak_sym = 0
    for row in symbol_rows:
        sym = _symbol_from_position_path(row.get("path"))
        if not sym:
            continue
        users_n = int(row.get("users") or 0)
        hits_n = int(row.get("hits") or 0)
        peak_sym = max(peak_sym, users_n or hits_n)
        symbols_7d.append({
            "symbol": sym,
            "hits": hits_n,
            "users": users_n,
        })
    peak_sym = peak_sym or 1
    for row in symbols_7d:
        bar_n = row["users"] or row["hits"]
        row["pct"] = round(100.0 * bar_n / peak_sym, 1)

    unique_7d = _q1(
        f"""
        SELECT COUNT(DISTINCT e.user_id) AS n
        FROM usage_events e
        LEFT JOIN users u ON u.id = e.user_id
        WHERE e.created_at > NOW() - INTERVAL '7 days'
          AND e.user_id IS NOT NULL
          AND {demo_sql}
        """,
        tuple(demo_params),
    )
    hits_7d = _q1(
        f"""
        SELECT COUNT(*) AS n
        FROM usage_events e
        LEFT JOIN users u ON u.id = e.user_id
        WHERE e.created_at > NOW() - INTERVAL '7 days'
          AND {demo_sql}
        """,
        tuple(demo_params),
    )
    logins_7d = _q1(
        """
        SELECT COUNT(*) AS n FROM login_attempts
        WHERE success = TRUE AND created_at > NOW() - INTERVAL '7 days'
        """
    )
    uploads_7d = _q1(
        """
        SELECT COUNT(*) AS n FROM uploads
        WHERE uploaded_at > NOW() - INTERVAL '7 days'
        """
    )
    insights_7d = _q1(
        """
        SELECT COUNT(*) AS n FROM insight_messages
        WHERE role = 'user' AND created_at > NOW() - INTERVAL '7 days'
        """
    )
    has_usage = int((hits_7d or {}).get("n") or 0) > 0

    brokers = _q(
        """
        SELECT LOWER(broker_slug) AS broker,
               COUNT(*) AS accounts,
               COUNT(DISTINCT user_id) AS users
        FROM snaptrade_accounts
        GROUP BY 1
        ORDER BY accounts DESC
        """
    )
    signups = _q(
        """
        SELECT date_trunc('week', up.created_at)::date AS week, COUNT(*) AS n
        FROM user_profiles up
        JOIN users u ON u.id = up.user_id
        WHERE up.created_at IS NOT NULL
          AND LOWER(u.username) <> %s
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 10
        """,
        (DEMO_USERNAME,),
    )

    recent = []
    for r in real[:12]:
        recent.append({
            "id": r.get("id"),
            "username": r.get("username"),
            "display_name": r.get("display_name"),
            "plan_state": r.get("plan_state"),
            "plan_label": PLAN_LABELS.get(r.get("plan_state"), r.get("plan_state")),
            "tone": PLAN_TONE.get(r.get("plan_state"), "muted"),
            "trial_days": r.get("trial_days"),
            "signed_up_at": r.get("signed_up_at"),
            "email": r.get("email"),
        })

    last_event = _q1("SELECT MAX(created_at) AS ts FROM usage_events")

    return {
        "generated_at": datetime.now(timezone.utc),
        "total_users": len(user_rows),
        "real_users": real_n,
        "paying": classified["paying"],
        "ai_addon": classified["ai_addon"],
        "canceling": classified["canceling"],
        "mix_rows": _mix_rows(classified["mix"]),
        "attention": _attention(
            classified, broken, open_feedback, failed_syncs, pending_first_sync,
        ),
        "broken": broken,
        "open_feedback": open_feedback,
        "views_7d": views_7d,
        "symbols_7d": symbols_7d,
        "unique_7d": int((unique_7d or {}).get("n") or 0),
        "hits_7d": int((hits_7d or {}).get("n") or 0),
        "logins_7d": int((logins_7d or {}).get("n") or 0),
        "uploads_7d": int((uploads_7d or {}).get("n") or 0),
        "insights_7d": int((insights_7d or {}).get("n") or 0),
        "has_usage": has_usage,
        "last_event_at": (last_event or {}).get("ts"),
        "brokers": brokers,
        "signups": list(reversed(signups)),
        "recent": recent,
    }
