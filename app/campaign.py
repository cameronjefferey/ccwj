"""Reddit campaign attribution for /start.

First-party events are the source of truth. A visit is one landing with a
stable visit id; a refresh of the same ad does not count again. A new
utm_source / utm_campaign / utm_content mints a new visit. The cookie is
what signup reads, so a person who uses the nav Sign Up link still gets
stamped.

Reddit's pixel is optional (REDDIT_PIXEL_ID). PageVisit fires on /start.
SignUp fires on the next HTML page after a successful signup. Reddit only
attributes that event when their own click id is present.
"""
from __future__ import annotations

import logging
import re
import uuid

from flask import current_app, g, request, session

_log = logging.getLogger(__name__)

COOKIE = "ht_acq"
COOKIE_DAYS = 30
_VISIT_RE = re.compile(r"^[a-f0-9]{32}$")
_UTM_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]{0,63}$")
_PIXEL_RE = re.compile(r"^[A-Za-z0-9_\-]{4,80}$")
_UTM_FIELDS = ("utm_source", "utm_campaign", "utm_content")
CLICK_EVENTS = {
    "signup": "signup_click",
    "demo": "demo_click",
}
# Button copy and the place stored on the click. Order is the page order.
CTA_PLACES = {
    "hero": {
        "where": "Hero",
        "primary": "See your trades",
        "demo": "Look at the demo",
    },
    "trades": {
        "where": "The trades",
        "primary": "See the sequence",
        "demo": "Look at an example",
    },
    "chart": {
        "where": "The chart",
        "primary": "See your chart",
        "demo": "Look at this chart",
    },
    "story": {
        "where": "The review",
        "primary": "Read the trades",
        "demo": "Read this review",
    },
    "profile": {
        "where": "The profile",
        "primary": "See your profile",
        "demo": "Look at a profile",
    },
    "fit": {
        "where": "Strategy fit",
        "primary": "See your strategies",
        "demo": "Look at the matrix",
    },
    "close": {
        "where": "Close",
        "primary": "Start the 30 days",
        "demo": "Look at the demo",
    },
}


def clean_utm(value) -> str:
    text = (value or "").strip()
    if not text or not _UTM_RE.match(text):
        return ""
    return text[:64]


def parse_utms(args) -> dict:
    return {field: clean_utm(args.get(field)) for field in _UTM_FIELDS}


def utm_query(args) -> dict:
    """Non-empty UTMs safe to put back on a redirect link."""
    return {key: value for key, value in parse_utms(args).items() if value}


def decode_cookie(raw) -> dict | None:
    if not raw or not isinstance(raw, str):
        return None
    parts = raw.split("|")
    if len(parts) < 4:
        return None
    visit_id = parts[0]
    if not _VISIT_RE.match(visit_id):
        return None
    return {
        "visit_id": visit_id,
        "utm_source": clean_utm(parts[1]),
        "utm_campaign": clean_utm(parts[2]),
        "utm_content": clean_utm(parts[3]),
        "visit_logged": (parts[4] if len(parts) > 4 else "") == "1",
    }


def encode_cookie(attr, *, visit_logged: bool) -> str:
    return "|".join([
        attr["visit_id"],
        attr.get("utm_source") or "",
        attr.get("utm_campaign") or "",
        attr.get("utm_content") or "",
        "1" if visit_logged else "0",
    ])


def merge_attribution(existing, incoming) -> tuple[dict, bool]:
    """Return (attribution, should_log_visit).

    A bare reload keeps the visit. A different ad (any UTM changed) starts
    a new one. The first hit, including a direct /start with no UTMs, logs.
    """
    incoming = incoming or {}
    incoming_has = any(incoming.get(field) for field in _UTM_FIELDS)
    if existing is None:
        return {
            "visit_id": uuid.uuid4().hex,
            "utm_source": incoming.get("utm_source") or "",
            "utm_campaign": incoming.get("utm_campaign") or "",
            "utm_content": incoming.get("utm_content") or "",
            "visit_logged": False,
        }, True
    changed = incoming_has and any(
        (incoming.get(field) or "") != (existing.get(field) or "")
        for field in _UTM_FIELDS
    )
    if changed:
        return {
            "visit_id": uuid.uuid4().hex,
            "utm_source": incoming.get("utm_source") or "",
            "utm_campaign": incoming.get("utm_campaign") or "",
            "utm_content": incoming.get("utm_content") or "",
            "visit_logged": False,
        }, True
    return existing, not existing.get("visit_logged")


def record_event(event: str, attr: dict, *, user_id=None, referrer=None, place=None) -> None:
    """Best-effort insert. Never raises — a missing table must not 500 /start."""
    try:
        from app.db import execute
        visit_id = attr.get("visit_id") or ""
        place = place if place in CTA_PLACES else None
        execute(
            """
            INSERT INTO campaign_events
                (event, visit_id, session_id, user_id, utm_source,
                 utm_campaign, utm_content, referrer, place)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event,
                visit_id,
                visit_id,
                user_id,
                attr.get("utm_source") or None,
                attr.get("utm_campaign") or None,
                attr.get("utm_content") or None,
                ((referrer or "")[:512] or None),
                place,
            ),
        )
    except Exception as exc:
        _log.warning("campaign event %s skipped: %s", event, exc)


def begin_visit() -> dict:
    incoming = parse_utms(request.args)
    existing = decode_cookie(request.cookies.get(COOKIE))
    attr, log_visit = merge_attribution(existing, incoming)
    if log_visit:
        record_event("visit", attr, referrer=request.referrer)
    attr["visit_logged"] = True
    return attr


def log_click(event: str, place: str | None = None) -> dict:
    incoming = parse_utms(request.args)
    existing = decode_cookie(request.cookies.get(COOKIE))
    attr, log_visit = merge_attribution(existing, incoming)
    if log_visit:
        record_event("visit", attr, referrer=request.referrer)
    record_event(event, attr, referrer=request.referrer, place=place)
    attr["visit_logged"] = True
    return attr


def attach_cookie(response, attr):
    response.set_cookie(
        COOKIE,
        encode_cookie(attr, visit_logged=True),
        max_age=COOKIE_DAYS * 24 * 60 * 60,
        httponly=True,
        samesite="Lax",
        secure=bool(current_app.config.get("SESSION_COOKIE_SECURE")),
        path="/",
    )
    return response


def stamp_signup(user_id) -> None:
    """Write acquisition onto the new user and log a signup event.

    Always arms the Reddit SignUp pixel. Reddit drops the event when the
    click was not theirs. Direct signups from the homepage have no cookie
    and are not written onto the user.
    """
    session["ht_reddit_signup"] = True
    attr = decode_cookie(request.cookies.get(COOKIE))
    if not attr or not user_id:
        return
    try:
        from app.db import execute
        execute(
            """
            UPDATE users
               SET acquisition_source = %s,
                   acquisition_campaign = %s,
                   acquisition_content = %s
             WHERE id = %s
            """,
            (
                attr.get("utm_source") or None,
                attr.get("utm_campaign") or None,
                attr.get("utm_content") or None,
                user_id,
            ),
        )
    except Exception as exc:
        _log.warning("campaign stamp skipped for user %s: %s", user_id, exc)
    record_event("signup", attr, user_id=user_id, referrer=request.referrer)


def reddit_pixel_context() -> dict:
    pixel = (current_app.config.get("REDDIT_PIXEL_ID") or "").strip()
    pending_signup = bool(session.pop("ht_reddit_signup", None))
    if not _PIXEL_RE.match(pixel):
        return {"reddit_pixel_id": "", "reddit_pixel_event": ""}
    if pending_signup:
        event = "SignUp"
    elif getattr(g, "campaign_pixel_event", None) == "PageVisit":
        event = "PageVisit"
    else:
        event = ""
    if not event:
        return {"reddit_pixel_id": "", "reddit_pixel_event": ""}
    return {"reddit_pixel_id": pixel, "reddit_pixel_event": event}


def summarize_funnel(events, connected_user_ids) -> list[dict]:
    """Roll campaign_events into one row per campaign × creative.

    Visits and clicks are distinct visit ids. Signups and connected are
    distinct user ids. Connected means that user has a broker_tenants row
    (SnapTrade or a CSV upload) — a signup that never links is not a win.
    """
    connected = set(connected_user_ids or ())
    buckets: dict[tuple, dict] = {}
    for ev in events or []:
        campaign = (ev.get("utm_campaign") or "").strip() or "(none)"
        creative = (ev.get("utm_content") or "").strip() or "(none)"
        key = (campaign, creative)
        bucket = buckets.get(key)
        if bucket is None:
            bucket = {
                "campaign": campaign,
                "creative": creative,
                "visits": set(),
                "signup_clicks": set(),
                "demo_clicks": set(),
                "signups": set(),
            }
            buckets[key] = bucket
        visit_id = ev.get("visit_id") or ""
        user_id = ev.get("user_id")
        kind = ev.get("event")
        if kind == "visit" and visit_id:
            bucket["visits"].add(visit_id)
        elif kind == "signup_click" and visit_id:
            bucket["signup_clicks"].add(visit_id)
        elif kind == "demo_click" and visit_id:
            bucket["demo_clicks"].add(visit_id)
        elif kind == "signup" and user_id:
            bucket["signups"].add(user_id)

    rows = []
    for bucket in buckets.values():
        visits_n = len(bucket["visits"])
        signups = bucket["signups"]
        signups_n = len(signups)
        connected_n = len(signups & connected)
        rows.append({
            "campaign": bucket["campaign"],
            "creative": bucket["creative"],
            "visits": visits_n,
            "signup_clicks": len(bucket["signup_clicks"]),
            "demo_clicks": len(bucket["demo_clicks"]),
            "signups": signups_n,
            "connected": connected_n,
            "signup_rate": (
                round(100.0 * signups_n / visits_n, 1) if visits_n else None
            ),
            "connect_rate": (
                round(100.0 * connected_n / signups_n, 1) if signups_n else None
            ),
        })
    rows.sort(key=lambda row: (-row["visits"], row["campaign"], row["creative"]))
    return rows


def summarize_places(events) -> list[dict]:
    """Distinct visits that clicked each landing-page button.

    Signup clicks and demo clicks are counted apart. A click with no place
    is an older row from before the buttons were tagged.
    """
    buckets = {
        place: {"signup": set(), "demo": set()} for place in CTA_PLACES
    }
    untagged = {"signup": set(), "demo": set()}
    for ev in events or []:
        visit_id = ev.get("visit_id") or ""
        kind = ev.get("event")
        if kind == "signup_click":
            slot = "signup"
        elif kind == "demo_click":
            slot = "demo"
        else:
            continue
        if not visit_id:
            continue
        place = (ev.get("place") or "").strip()
        if place in buckets:
            buckets[place][slot].add(visit_id)
        else:
            untagged[slot].add(visit_id)

    rows = []
    for place, spec in CTA_PLACES.items():
        rows.append({
            "place": place,
            "where": spec["where"],
            "primary": spec["primary"],
            "demo": spec["demo"],
            "signup_clicks": len(buckets[place]["signup"]),
            "demo_clicks": len(buckets[place]["demo"]),
        })
    if untagged["signup"] or untagged["demo"]:
        rows.append({
            "place": "",
            "where": "Earlier",
            "primary": "See your trades",
            "demo": "Look at the demo",
            "signup_clicks": len(untagged["signup"]),
            "demo_clicks": len(untagged["demo"]),
        })
    return rows


def filter_funnel_events(events, *, campaign="", creative=""):
    campaign = (campaign or "").strip()
    creative = (creative or "").strip()
    kept = []
    for ev in events or []:
        ev_campaign = (ev.get("utm_campaign") or "").strip() or "(none)"
        ev_creative = (ev.get("utm_content") or "").strip() or "(none)"
        if campaign and ev_campaign != campaign:
            continue
        if creative and ev_creative != creative:
            continue
        kept.append(ev)
    return kept
