"""First-party funnel analytics.

Events carry a path, a referrer host+path, UTM fields, and the Reddit click
id. They do not carry email, name, IP, or a raw query string. ``user_id``
is the internal account id, written only after signup.

``ht_touch`` is the first-party cookie. First-touch is whatever arrived on
the first visit. Last-touch updates when a later visit brings new UTMs or
a new ``rdt_cid``. Signup copies both onto ``users``.
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import threading
import time
import uuid
from urllib.parse import urlparse

from flask import current_app, g, has_request_context, request, session

_log = logging.getLogger(__name__)

COOKIE = "ht_touch"
COOKIE_DAYS = 90
_VISIT_RE = re.compile(r"^[a-f0-9]{32}$")
_UTM_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]{0,63}$")
_CLICK_RE = re.compile(r"^[A-Za-z0-9._\-]{4,200}$")
UTM_FIELDS = (
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
)
_TOUCH_KEYS = UTM_FIELDS + ("rdt_cid", "referrer", "landing", "variant")

# first=True: one row per user (or per visit, while anonymous).
# reddit: Conversions API / pixel standard event, or None.
EVENTS = {
    "landing_view": {"first": False, "reddit": "PageVisit"},
    "demo_start": {"first": True, "reddit": None},
    "signup_started": {"first": True, "reddit": None},
    "signup_completed": {"first": True, "reddit": "SignUp"},
    "route_paper": {"first": True, "reddit": None},
    "route_broker": {"first": True, "reddit": None},
    "lesson_started": {"first": True, "reddit": None},
    "lesson_completed": {"first": True, "reddit": None},
    "practice_reviewed": {"first": True, "reddit": None},
    "practice_placed": {"first": True, "reddit": None},
    "paper_connected": {"first": True, "reddit": "Lead"},
    "broker_connected": {"first": True, "reddit": "Lead"},
    "checkout_started": {"first": True, "reddit": None},
    "paid": {"first": True, "reddit": "Purchase"},
    # Logged-out public pages. These stay first-party even when an ad
    # pixel is off. detail carries a CTA name, a scroll mark, or a video id.
    "page_view": {"first": False, "reddit": None},
    "cta_click": {"first": False, "reddit": None},
    "scroll_depth": {"first": False, "reddit": None},
    "video_play": {"first": False, "reddit": None},
}
# The browser beacon may only name these. Paid and signup stay server-side.
BEACON_EVENTS = frozenset({
    "lesson_started",
    "lesson_completed",
    "landing_view",
    "cta_click",
    "scroll_depth",
    "video_play",
})
ACQ_STEPS = (
    ("visitors", "Visitors"),
    ("cta", "CTA clicks"),
    ("signups", "Signups"),
    ("lessons", "First lesson"),
    ("paper", "Paper connect"),
    ("broker", "Real broker connect"),
    ("paid", "Paid"),
)
_RANGE_WHERE = {
    "today": (
        "(created_at AT TIME ZONE 'America/New_York')::date "
        "= (NOW() AT TIME ZONE 'America/New_York')::date"
    ),
    "7d": "created_at >= NOW() - INTERVAL '7 days'",
    "30d": "created_at >= NOW() - INTERVAL '30 days'",
}
_RANGE_DAYS = {"today": 1, "7d": 7, "30d": 30}
FUNNEL_STEPS = (
    ("landing_view", "Landing view"),
    ("demo_start", "Demo start"),
    ("signup_started", "Signup started"),
    ("signup_completed", "Signup completed"),
    ("route_paper", "Chose learning / paper"),
    ("route_broker", "Chose connect broker"),
    ("lesson_started", "First lesson started"),
    ("lesson_completed", "First lesson completed"),
    ("practice_reviewed", "First practice trade reviewed"),
    ("practice_placed", "First practice trade placed"),
    ("paper_connected", "Paper account connected"),
    ("broker_connected", "Real broker connected"),
    ("checkout_started", "Checkout started"),
    ("paid", "Paid"),
)
_LANDING_EXACT = frozenset({
    "/", "/index", "/start", "/pricing", "/signup", "/learn", "/learn/",
})
_PIXEL_EXACT = _LANDING_EXACT | {"/faq"}


def clean_utm(value) -> str:
    text = (value or "").strip()
    if not text or not _UTM_RE.match(text):
        return ""
    return text[:64]


def clean_click_id(value) -> str:
    text = (value or "").strip()
    if not text or not _CLICK_RE.match(text):
        return ""
    return text[:200]


def clean_referrer(value) -> str:
    """Host and path only. Query strings can carry tokens."""
    text = (value or "").strip()
    if not text or len(text) > 1000:
        return ""
    try:
        parsed = urlparse(text)
    except Exception:
        return ""
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in ("http", "https") or not host:
        return ""
    if host in ("happytrader.me", "www.happytrader.me"):
        return ""
    path = parsed.path or ""
    if len(path) > 200:
        path = path[:200]
    return f"{parsed.scheme}://{host}{path}"[:300]


def _empty_touch() -> dict:
    return {key: "" for key in _TOUCH_KEYS}


def _touch_from_mapping(raw) -> dict:
    touch = _empty_touch()
    if not isinstance(raw, dict):
        return touch
    for field in UTM_FIELDS:
        touch[field] = clean_utm(raw.get(field))
    touch["rdt_cid"] = clean_click_id(raw.get("rdt_cid"))
    touch["referrer"] = clean_referrer(raw.get("referrer"))
    touch["landing"] = clean_utm(raw.get("landing"))
    touch["variant"] = clean_utm(raw.get("variant"))
    return touch


def _has_campaign_signal(touch: dict) -> bool:
    return any(touch.get(field) for field in UTM_FIELDS) or bool(touch.get("rdt_cid"))


def parse_request_touch() -> dict:
    if not has_request_context():
        return _empty_touch()
    args = request.args
    touch = _empty_touch()
    for field in UTM_FIELDS:
        touch[field] = clean_utm(args.get(field))
    touch["rdt_cid"] = clean_click_id(args.get("rdt_cid"))
    touch["referrer"] = clean_referrer(request.referrer)
    slug = (request.path or "").split("/")
    # /go/<slug> is the campaign. Only an allow-listed headline variant sticks.
    if len(slug) == 3 and slug[1] == "go" and slug[2]:
        from app.go_landings import LANDINGS, allowed_variant
        page_slug = clean_utm(slug[2])
        if page_slug in LANDINGS:
            touch["landing"] = page_slug
            touch["variant"] = allowed_variant(page_slug, args.get("v"))
    return touch


def _pack_touch(touch: dict) -> dict:
    return {
        "s": touch.get("utm_source") or "",
        "m": touch.get("utm_medium") or "",
        "c": touch.get("utm_campaign") or "",
        "n": touch.get("utm_content") or "",
        "t": touch.get("utm_term") or "",
        "r": touch.get("rdt_cid") or "",
        "f": touch.get("referrer") or "",
        "l": touch.get("landing") or "",
        "a": touch.get("variant") or "",
    }


def _unpack_touch(raw) -> dict:
    if not isinstance(raw, dict):
        return _empty_touch()
    return _touch_from_mapping({
        "utm_source": raw.get("s"),
        "utm_medium": raw.get("m"),
        "utm_campaign": raw.get("c"),
        "utm_content": raw.get("n"),
        "utm_term": raw.get("t"),
        "rdt_cid": raw.get("r"),
        "referrer": raw.get("f"),
        "landing": raw.get("l"),
        "variant": raw.get("a"),
    })


def encode_touch_cookie(attr: dict) -> str:
    payload = {
        "v": attr["visit_id"],
        "ft": _pack_touch(attr.get("ft") or {}),
        "lt": _pack_touch(attr.get("lt") or {}),
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_touch_cookie(raw) -> dict | None:
    if not raw or not isinstance(raw, str) or len(raw) > 3500:
        return None
    try:
        padded = raw + "=" * (-len(raw) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    visit_id = payload.get("v") or ""
    if not _VISIT_RE.match(visit_id):
        return None
    return {
        "visit_id": visit_id,
        "ft": _unpack_touch(payload.get("ft")),
        "lt": _unpack_touch(payload.get("lt")),
    }


def merge_touch(existing, incoming) -> tuple[dict, bool]:
    """Return (attribution, cookie_dirty).

    First-touch sticks. Last-touch moves when the new request carries a
    UTM or Reddit click id that is not already the last touch.
    """
    incoming = _touch_from_mapping(incoming or {})
    if existing is None:
        return {
            "visit_id": uuid.uuid4().hex,
            "ft": dict(incoming),
            "lt": dict(incoming),
        }, True
    ft = _touch_from_mapping(existing.get("ft"))
    lt = _touch_from_mapping(existing.get("lt"))
    dirty = False
    if not ft.get("referrer") and incoming.get("referrer"):
        ft["referrer"] = incoming["referrer"]
        dirty = True
    if _has_campaign_signal(incoming):
        changed = any(
            (incoming.get(key) or "") != (lt.get(key) or "")
            for key in UTM_FIELDS + ("rdt_cid",)
            if incoming.get(key)
        )
        if changed or not _has_campaign_signal(lt):
            merged = dict(lt)
            for key in UTM_FIELDS + ("rdt_cid",):
                if incoming.get(key):
                    merged[key] = incoming[key]
            if incoming.get("referrer"):
                merged["referrer"] = incoming["referrer"]
            lt = merged
            dirty = True
            if not _has_campaign_signal(ft):
                ft = dict(merged)
    elif incoming.get("referrer") and not lt.get("referrer"):
        lt["referrer"] = incoming["referrer"]
        dirty = True
    dirty = _apply_landing(ft, lt, incoming) or dirty
    return {
        "visit_id": existing["visit_id"],
        "ft": ft,
        "lt": lt,
    }, dirty


def _apply_landing(ft: dict, lt: dict, incoming: dict) -> bool:
    """First landing sticks. The latest /go visit moves last-touch."""
    dirty = False
    landing = incoming.get("landing") or ""
    variant = incoming.get("variant") or ""
    if landing and not ft.get("landing"):
        ft["landing"] = landing
        dirty = True
    if landing and lt.get("landing") != landing:
        lt["landing"] = landing
        dirty = True
    if variant and not ft.get("variant"):
        ft["variant"] = variant
        dirty = True
    if variant and lt.get("variant") != variant:
        lt["variant"] = variant
        dirty = True
    return dirty


def device_type(user_agent=None) -> str:
    """Coarse device class. The raw user agent is not stored."""
    if user_agent is None and has_request_context():
        user_agent = request.headers.get("User-Agent") or ""
    ua = (user_agent or "").lower()
    if "ipad" in ua or "tablet" in ua:
        return "tablet"
    if any(tok in ua for tok in ("mobi", "iphone", "android", "phone")):
        return "phone"
    return "desktop"


def tracking_opt_out() -> bool:
    """Do Not Track or Global Privacy Control. Ad pixels stay off."""
    if not has_request_context():
        return False
    dnt = (request.headers.get("DNT") or "").strip()
    gpc = (request.headers.get("Sec-GPC") or "").strip()
    return dnt == "1" or gpc == "1"


def _db_ready() -> bool:
    return bool((os.environ.get("DATABASE_URL") or "").strip())


def current_attribution() -> dict:
    if has_request_context():
        cached = getattr(g, "funnel_attr", None)
        if cached:
            return cached
    existing = None
    if has_request_context():
        existing = decode_touch_cookie(request.cookies.get(COOKIE))
    incoming = parse_request_touch()
    attr, dirty = merge_touch(existing, incoming)
    attr["_dirty"] = dirty or existing is None
    if has_request_context():
        g.funnel_attr = attr
    return attr


def attach_cookie(response, attr=None):
    attr = attr or current_attribution()
    if not attr.get("_dirty") and request.cookies.get(COOKIE):
        return response
    secure = False
    try:
        secure = bool(current_app.config.get("SESSION_COOKIE_SECURE"))
    except Exception:
        secure = False
    response.set_cookie(
        COOKIE,
        encode_touch_cookie(attr),
        max_age=COOKIE_DAYS * 24 * 60 * 60,
        httponly=True,
        samesite="Lax",
        secure=secure,
        path="/",
    )
    return response


def _viewer_user_id():
    if not has_request_context():
        return None
    try:
        from flask_login import current_user
        if current_user.is_authenticated:
            uid = getattr(current_user, "id", None)
            if isinstance(uid, int):
                return uid
            if isinstance(uid, str) and uid.isdigit():
                return int(uid)
    except Exception:
        return None
    return None


def _already(event: str, user_id, visit_id) -> bool:
    from app.db import fetch_one
    if user_id:
        row = fetch_one(
            "SELECT 1 FROM funnel_events WHERE event = %s AND user_id = %s LIMIT 1",
            (event, user_id),
        )
        return bool(row)
    if not visit_id:
        return False
    row = fetch_one(
        "SELECT 1 FROM funnel_events WHERE event = %s AND visit_id = %s LIMIT 1",
        (event, visit_id),
    )
    return bool(row)


def _recent_same(event: str, visit_id: str, path: str, detail: str, hours: int) -> bool:
    from app.db import fetch_one
    if not visit_id:
        return False
    if detail:
        row = fetch_one(
            """
            SELECT 1 FROM funnel_events
             WHERE event = %s AND visit_id = %s AND path = %s AND detail = %s
               AND created_at > NOW() - (%s || ' hours')::interval
             LIMIT 1
            """,
            (event, visit_id, path, detail, str(int(hours))),
        )
    else:
        row = fetch_one(
            """
            SELECT 1 FROM funnel_events
             WHERE event = %s AND visit_id = %s AND path = %s
               AND created_at > NOW() - (%s || ' hours')::interval
             LIMIT 1
            """,
            (event, visit_id, path, str(int(hours))),
        )
    return bool(row)


def log_event(
    event: str,
    *,
    user_id=None,
    path=None,
    referrer=None,
    event_id=None,
    first=None,
    detail=None,
) -> str | None:
    """Insert one funnel row. Never raises. Returns the event id or None."""
    spec = EVENTS.get(event)
    if spec is None or not _db_ready():
        return None
    try:
        attr = current_attribution()
        visit_id = attr.get("visit_id") or ""
        lt = attr.get("lt") or {}
        if user_id is None:
            user_id = _viewer_user_id()
        if path is None and has_request_context():
            path = (request.path or "")[:300]
        path = (path or "")[:300]
        if referrer is None:
            referrer = lt.get("referrer") or ""
        referrer = clean_referrer(referrer) or (referrer or "")[:300]
        if first is None:
            first = bool(spec["first"])
        detail = clean_utm(detail) if detail else ""
        device = device_type() if has_request_context() else ""
        landing = lt.get("landing") or ""
        variant = lt.get("variant") or ""
        if event == "landing_view" and _recent_same(event, visit_id, path, "", 12):
            return None
        if event == "page_view" and _recent_same(event, visit_id, path, "", 1):
            return None
        if event in ("cta_click", "scroll_depth", "video_play") and _recent_same(
            event, visit_id, path, detail, 12,
        ):
            return None
        if first and _already(event, user_id, visit_id):
            return None
        if (
            event_id is None
            and event == "landing_view"
            and has_request_context()
        ):
            # The pixel tag is rendered before this after-request log.
            # Reuse the id it already printed so Reddit can dedup.
            event_id = getattr(g, "_funnel_pagevisit_id", None)
        event_id = event_id or uuid.uuid4().hex
        if event == "landing_view" and has_request_context():
            g._funnel_pagevisit_id = event_id
        from app.db import execute
        execute(
            """
            INSERT INTO funnel_events
                (event, event_id, visit_id, user_id, path, referrer,
                 utm_source, utm_medium, utm_campaign, utm_content, utm_term,
                 rdt_cid, landing, variant, device, detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s)
            """,
            (
                event,
                event_id,
                visit_id or None,
                user_id,
                path or None,
                referrer or None,
                lt.get("utm_source") or None,
                lt.get("utm_medium") or None,
                lt.get("utm_campaign") or None,
                lt.get("utm_content") or None,
                lt.get("utm_term") or None,
                lt.get("rdt_cid") or None,
                landing or None,
                variant or None,
                device or None,
                detail or None,
            ),
        )
        reddit_name = spec["reddit"]
        if reddit_name:
            _arm_reddit(event, reddit_name, event_id, user_id, lt.get("rdt_cid"))
        return event_id
    except Exception as exc:
        _log.warning("funnel event %s skipped: %s", event, exc)
        return None


def _arm_reddit(event, reddit_name, event_id, user_id, click_id):
    """Pixel flag for the next HTML page, plus a server-side conversion."""
    if reddit_name == "PageVisit":
        path = ""
        if has_request_context():
            path = request.path or ""
        # Server PageVisit only on the ad and home landings. The pixel
        # still fires in the browser on the other public pages.
        if path not in ("/", "/index", "/start") and not _is_go_path(path):
            return
        if getattr(g, "_funnel_pagevisit_sent", False):
            return
        g._funnel_pagevisit_sent = True
    if reddit_name == "Lead" and _lead_already_other(event, user_id):
        # The funnel row for this connect is kept. Reddit Lead fires once.
        return
    if (
        has_request_context()
        and reddit_name != "PageVisit"
        and not tracking_opt_out()
    ):
        session[f"ht_reddit_{reddit_name.lower()}"] = event_id
        session.modified = True
    if reddit_name == "PageVisit" and has_request_context():
        g._funnel_pagevisit_id = event_id
    send_capi(reddit_name, event_id, click_id=click_id, user_id=user_id)


def _lead_already_other(event, user_id) -> bool:
    other = "paper_connected" if event == "broker_connected" else "broker_connected"
    try:
        return _already(other, user_id, current_attribution().get("visit_id"))
    except Exception:
        return False


def on_signup(user_id) -> None:
    """Signup completed: copy first- and last-touch, then log the event."""
    if not user_id:
        return
    try:
        persist_signup(user_id)
    except Exception as exc:
        _log.warning("funnel persist skipped for user %s: %s", user_id, exc)
    if tracking_opt_out():
        _mark_ads_opt_out(user_id)
    event_id = uuid.uuid4().hex
    logged = log_event(
        "signup_completed", user_id=user_id, event_id=event_id, path="/signup",
    )
    if has_request_context():
        session["ht_reddit_signup"] = logged or event_id
        session.modified = True


def _mark_ads_opt_out(user_id) -> None:
    if not _db_ready():
        return
    from app.db import execute
    execute(
        "UPDATE users SET acquisition_ads_opt_out = TRUE WHERE id = %s",
        (user_id,),
    )


def persist_signup(user_id) -> None:
    """Write first-touch and last-touch. Marks the row captured.

    Empty campaign fields stay NULL so a /start-only cookie can still fill
    source/campaign/content. Once captured, later stamps do not overwrite.
    """
    if not user_id or not has_request_context() or not _db_ready():
        return
    attr = current_attribution()
    if not request.cookies.get(COOKIE) and not attr.get("_dirty"):
        # A brand-new cookie minted on the signup POST itself still counts.
        if not attr.get("visit_id"):
            return
    ft = attr.get("ft") or {}
    lt = attr.get("lt") or {}
    if not any(ft.get(k) or lt.get(k) for k in _TOUCH_KEYS):
        return
    from app.db import execute
    execute(
        """
        UPDATE users
           SET acquisition_captured = TRUE,
               acquisition_source = COALESCE(NULLIF(%s, ''), acquisition_source),
               acquisition_medium = COALESCE(NULLIF(%s, ''), acquisition_medium),
               acquisition_campaign = COALESCE(NULLIF(%s, ''), acquisition_campaign),
               acquisition_content = COALESCE(NULLIF(%s, ''), acquisition_content),
               acquisition_term = COALESCE(NULLIF(%s, ''), acquisition_term),
               acquisition_click_id = COALESCE(NULLIF(%s, ''), acquisition_click_id),
               acquisition_referrer = COALESCE(NULLIF(%s, ''), acquisition_referrer),
               acquisition_lt_source = NULLIF(%s, ''),
               acquisition_lt_medium = NULLIF(%s, ''),
               acquisition_lt_campaign = NULLIF(%s, ''),
               acquisition_lt_content = NULLIF(%s, ''),
               acquisition_lt_term = NULLIF(%s, ''),
               acquisition_lt_click_id = NULLIF(%s, ''),
               acquisition_lt_referrer = NULLIF(%s, ''),
               acquisition_landing = COALESCE(NULLIF(%s, ''), acquisition_landing),
               acquisition_variant = COALESCE(NULLIF(%s, ''), acquisition_variant),
               acquisition_lt_landing = NULLIF(%s, ''),
               acquisition_lt_variant = NULLIF(%s, '')
         WHERE id = %s
        """,
        (
            ft.get("utm_source") or "",
            ft.get("utm_medium") or "",
            ft.get("utm_campaign") or "",
            ft.get("utm_content") or "",
            ft.get("utm_term") or "",
            ft.get("rdt_cid") or "",
            ft.get("referrer") or "",
            lt.get("utm_source") or "",
            lt.get("utm_medium") or "",
            lt.get("utm_campaign") or "",
            lt.get("utm_content") or "",
            lt.get("utm_term") or "",
            lt.get("rdt_cid") or "",
            lt.get("referrer") or "",
            ft.get("landing") or "",
            ft.get("variant") or "",
            lt.get("landing") or "",
            lt.get("variant") or "",
            user_id,
        ),
    )


def note_paid(user_id, subscription_id=None) -> None:
    """One paid event per user. conversion id is stable for pixel/CAPI dedup."""
    event_id = None
    if subscription_id:
        safe = re.sub(r"[^A-Za-z0-9]", "", str(subscription_id))[:40]
        if safe:
            event_id = f"purchase{safe}"
    logged = log_event("paid", user_id=user_id, event_id=event_id, path="/billing")
    if has_request_context() and logged:
        session["ht_reddit_purchase"] = logged
        session.modified = True


def _is_go_path(path: str) -> bool:
    return path.startswith("/go/") and path.count("/") == 2


def _is_landing_path(path: str) -> bool:
    if path in _LANDING_EXACT or _is_go_path(path):
        return True
    return path.startswith("/learn/") and path != "/learn/progress"


def _is_public_path(path: str) -> bool:
    """Logged-out pages that record a first-party page view."""
    if path in _PIXEL_EXACT or path in ("/login",):
        return True
    if path.startswith("/learn/") and path != "/learn/progress":
        return True
    if _is_go_path(path) or path.startswith("/demo/"):
        return True
    return False


def _pixel_path(path: str) -> bool:
    if path in _PIXEL_EXACT or _is_go_path(path):
        return True
    return path.startswith("/learn/") and "progress" not in path


def observe_response(response):
    """Log funnel steps from the request that just finished. Never raises."""
    try:
        _observe(response)
    except Exception as exc:
        _log.warning("funnel observe skipped: %s", exc)
    try:
        if has_request_context():
            path = request.path or ""
            if not path.startswith("/static") and not path.startswith("/healthz"):
                attach_cookie(response)
    except Exception as exc:
        _log.warning("funnel cookie skipped: %s", exc)
    return response


def _observe(response):
    if not has_request_context():
        return
    path = request.path or ""
    if (
        path.startswith("/static")
        or path.startswith("/healthz")
        or path.startswith("/funnel/")
        or path == "/favicon.ico"
    ):
        return
    status = response.status_code
    if status >= 400:
        return
    loc = response.headers.get("Location") or ""
    method = request.method
    if method == "GET" and status == 200 and _is_landing_path(path):
        log_event("landing_view", path=path, referrer=clean_referrer(request.referrer))
    if (
        method == "GET"
        and status == 200
        and _is_public_path(path)
        and _viewer_user_id() is None
    ):
        log_event("page_view", path=path, referrer=clean_referrer(request.referrer))
    if method == "GET" and path == "/signup" and status == 200:
        log_event("signup_started", path=path)
    if (
        method == "GET"
        and status == 200
        and path.startswith("/learn/")
        and path not in ("/learn/", "/learn/progress")
        and "/" not in path[len("/learn/"):]
    ):
        log_event("lesson_started", path=path)
    if method == "POST" and path == "/learn/progress" and status == 200:
        body = request.get_json(silent=True) or {}
        if isinstance(body, dict) and body.get("done"):
            log_event("lesson_completed", path=path)
    if method == "POST" and path == "/demo/start" and status == 302 and "/overview" in loc:
        log_event("demo_start", path="/demo/start")
    if method == "POST" and path == "/get-started/paper" and status == 302:
        log_event("route_paper", path=path)
    if (
        method == "GET"
        and path == "/get-started"
        and (request.args.get("path") or "") == "broker"
        and status == 200
    ):
        log_event("route_broker", path=path)
    if method == "POST" and path == "/practice/review" and status == 302 and "confirm=1" in loc:
        log_event("practice_reviewed", path=path)
    if method == "POST" and path == "/practice/place" and status == 302 and "placed=1" in loc:
        log_event("practice_placed", path=path)
    if method == "GET" and path == "/snaptrade/callback" and status == 302:
        if "paper=1" in loc or loc.rstrip("/").endswith("/practice"):
            log_event("paper_connected", path=path)
        elif "connecting=1" in loc:
            log_event("broker_connected", path=path)
    if method == "POST" and path == "/billing/checkout" and status in (302, 303):
        if "stripe.com" in loc:
            log_event("checkout_started", path=path)


def beacon(payload) -> tuple[dict, int]:
    if not isinstance(payload, dict):
        return {"ok": False}, 400
    event = (payload.get("event") or "").strip()
    if event not in BEACON_EVENTS:
        return {"ok": False}, 400
    # Ignore anything that looks like an identity field. Path is optional
    # and must be a same-site path with no query.
    for banned in ("email", "ip", "name", "user_id", "phone"):
        if banned in payload:
            return {"ok": False}, 400
    path = payload.get("path") or (request.path if has_request_context() else "")
    if not isinstance(path, str) or not path.startswith("/") or "?" in path or len(path) > 300:
        path = request.path if has_request_context() else ""
    detail = payload.get("detail") or ""
    if not isinstance(detail, str):
        return {"ok": False}, 400
    detail = clean_utm(detail)
    if event in ("cta_click", "scroll_depth", "video_play") and not detail:
        return {"ok": False}, 400
    if event == "scroll_depth" and detail not in ("25", "50", "75", "100"):
        return {"ok": False}, 400
    log_event(event, path=path, detail=detail)
    return {"ok": True}, 200


def _pixel_id() -> str:
    try:
        pixel = (current_app.config.get("REDDIT_PIXEL_ID") or "").strip()
    except Exception:
        pixel = ""
    if not re.match(r"^[A-Za-z0-9_\-]{4,80}$", pixel):
        return ""
    return pixel


def _event_id_from_flag(raw) -> str:
    if isinstance(raw, str) and re.match(r"^[A-Za-z0-9]{8,80}$", raw):
        return raw
    return uuid.uuid4().hex


def _pop_pixel_event(session_key, name) -> dict | None:
    if not has_request_context():
        return None
    raw = session.pop(session_key, None)
    if not raw:
        return None
    session.modified = True
    return {"name": name, "event_id": _event_id_from_flag(raw)}


def reddit_pixel_events() -> list[dict]:
    """Events the browser pixel should fire on this response. Empty when
    the pixel id is unset or the browser sent DNT / GPC."""
    if tracking_opt_out():
        # Still consume one-shot flags so they cannot fire on a later page
        # after the header disappears.
        if has_request_context():
            for key in ("ht_reddit_signup", "ht_reddit_lead", "ht_reddit_purchase"):
                session.pop(key, None)
        return []
    if not _pixel_id():
        return []
    events = []
    signup = _pop_pixel_event("ht_reddit_signup", "SignUp")
    if signup:
        events.append(signup)
    lead = _pop_pixel_event("ht_reddit_lead", "Lead")
    if lead:
        events.append(lead)
    purchase = _pop_pixel_event("ht_reddit_purchase", "Purchase")
    if purchase:
        events.append(purchase)
    path = request.path if has_request_context() else ""
    pagevisit = False
    if has_request_context() and getattr(g, "campaign_pixel_event", None) == "PageVisit":
        pagevisit = True
    elif _pixel_path(path):
        pagevisit = True
    if pagevisit:
        shared = ""
        if has_request_context():
            shared = getattr(g, "_funnel_pagevisit_id", None) or ""
            if not shared:
                shared = uuid.uuid4().hex
                g._funnel_pagevisit_id = shared
        events.append({
            "name": "PageVisit",
            "event_id": shared or uuid.uuid4().hex,
        })
    return events


# Pixel names stay PascalCase (rdt('track', name)). CAPI v3 wants
# UPPER_SNAKE tracking_type. https://ads-api.reddit.com/docs/v3/guides/programs/capi/migration
_CAPI_TRACKING = {
    "PageVisit": "PAGE_VISIT",
    "SignUp": "SIGN_UP",
    "Lead": "LEAD",
    "Purchase": "PURCHASE",
}


def _capi_event_source_url() -> str:
    """Scheme, host, and path. The query string can carry click ids and tokens."""
    if not has_request_context():
        return ""
    try:
        parsed = urlparse(request.url or "")
    except Exception:
        return ""
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path or '/'}"


def send_capi(event_name: str, event_id: str, *, click_id=None, user_id=None) -> bool:
    """Reddit Conversions API v3. No-op without REDDIT_CAPI_TOKEN or when opted out.

    ``event_id`` is the pixel ``conversionId``. CAPI sends it as
    ``metadata.conversion_id`` so Reddit can dedup the two.
    The body has the click id and a hash of the internal user id. No email.
    """
    if tracking_opt_out() or _user_opted_out(user_id):
        return False
    tracking = _CAPI_TRACKING.get(event_name or "")
    if not tracking:
        return False
    try:
        token = (current_app.config.get("REDDIT_CAPI_TOKEN") or "").strip()
        pixel = _pixel_id()
    except Exception:
        token = (os.environ.get("REDDIT_CAPI_TOKEN") or "").strip()
        pixel = (os.environ.get("REDDIT_PIXEL_ID") or "").strip()
    if not token or not pixel or not event_id:
        return False
    event = {
        "event_at": int(time.time() * 1000),
        "action_source": "WEBSITE",
        "type": {"tracking_type": tracking},
        "metadata": {"conversion_id": event_id},
    }
    if click_id:
        event["click_id"] = click_id
    source = _capi_event_source_url()
    if source:
        event["event_source_url"] = source
    if user_id:
        event["user"] = {
            "external_id": hashlib.sha256(str(user_id).encode()).hexdigest(),
        }
    body = {"data": {"events": [event]}}
    url = f"https://ads-api.reddit.com/api/v3/pixels/{pixel}/conversion_events"

    def _post():
        try:
            import urllib.request
            req = urllib.request.Request(
                url,
                data=json.dumps(body).encode(),
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "User-Agent": "HappyTrader/funnel",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                resp.read()
        except Exception as exc:
            _log.warning("reddit capi %s skipped: %s", event_name, exc)

    if os.environ.get("FUNNEL_CAPI_SYNC") == "1":
        _post()
    else:
        threading.Thread(target=_post, daemon=True).start()
    return True


def _user_opted_out(user_id) -> bool:
    if not user_id or has_request_context() or not _db_ready():
        return False
    try:
        from app.db import fetch_one
        row = fetch_one(
            "SELECT acquisition_ads_opt_out FROM users WHERE id = %s",
            (user_id,),
        )
        return bool(row and row.get("acquisition_ads_opt_out"))
    except Exception:
        return False


def conversion_rates(counts: dict) -> list[dict]:
    """Each step as a count and a percent of visitors. Zero visitors stay 0."""
    visitors = int((counts or {}).get("visitors") or 0)
    rows = []
    for key, label in ACQ_STEPS:
        n = int((counts or {}).get(key) or 0)
        rate = round(100.0 * n / visitors, 1) if visitors else 0.0
        rows.append({"key": key, "label": label, "count": n, "rate": rate})
    return rows


def _range_key() -> str:
    raw = ""
    if has_request_context():
        raw = (request.args.get("range") or "").strip()
    if raw not in _RANGE_WHERE:
        return "7d"
    return raw


def _referrer_bucket(host: str) -> str:
    text = (host or "").lower()
    if "youtube.com" in text or "youtu.be" in text:
        return "YouTube"
    if "reddit.com" in text:
        return "Reddit"
    return host or "(none)"


def _empty_acquisition(range_key="7d") -> dict:
    from app.go_landings import LANDING_SLUGS
    return {
        "range_key": range_key,
        "days": [],
        "pages": [],
        "campaigns": [
            {"landing": slug, "steps": conversion_rates({})}
            for slug in LANDING_SLUGS
        ],
        "sources": [],
        "devices": [],
        "referrers": [
            {"host": "YouTube", "visitors": 0},
            {"host": "Reddit", "visitors": 0},
        ],
    }


def build_acquisition(range_key=None) -> dict:
    """Logged-out visitors, campaign funnel, and referrers for one window."""
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    from app.db import fetch_all
    from app.go_landings import LANDING_SLUGS

    range_key = range_key or _range_key()
    if range_key not in _RANGE_WHERE:
        range_key = "7d"
    window = _RANGE_WHERE[range_key]
    empty = _empty_acquisition(range_key)
    if not _db_ready():
        return empty
    try:
        day_rows = fetch_all(
            f"""
            SELECT to_char(created_at AT TIME ZONE 'America/New_York', 'YYYY-MM-DD') AS day,
                   COUNT(*) FILTER (WHERE event = 'page_view')::int AS views,
                   COUNT(DISTINCT visit_id) FILTER (WHERE event = 'page_view')::int AS visitors
              FROM funnel_events
             WHERE {window}
             GROUP BY 1
            """
        )
        pages = fetch_all(
            f"""
            SELECT COALESCE(path, '/') AS path,
                   COUNT(*)::int AS views,
                   COUNT(DISTINCT visit_id)::int AS visitors
              FROM funnel_events
             WHERE event = 'page_view' AND {window}
             GROUP BY 1
             ORDER BY views DESC, visitors DESC
             LIMIT 12
            """
        )
        campaigns = fetch_all(
            f"""
            SELECT COALESCE(NULLIF(landing, ''), '(none)') AS landing,
                   COUNT(DISTINCT visit_id) FILTER (WHERE event = 'page_view')::int AS visitors,
                   COUNT(DISTINCT visit_id) FILTER (WHERE event = 'cta_click')::int AS cta,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id))
                     FILTER (WHERE event = 'signup_completed')::int AS signups,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id))
                     FILTER (WHERE event = 'lesson_started')::int AS lessons,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id))
                     FILTER (WHERE event = 'paper_connected')::int AS paper,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id))
                     FILTER (WHERE event = 'broker_connected')::int AS broker,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id))
                     FILTER (WHERE event = 'paid')::int AS paid
              FROM funnel_events
             WHERE {window}
             GROUP BY 1
            """
        )
        sources = fetch_all(
            f"""
            SELECT COALESCE(NULLIF(utm_source, ''), '(none)') AS source,
                   COALESCE(NULLIF(utm_campaign, ''), '(none)') AS campaign,
                   COALESCE(NULLIF(utm_content, ''), '(none)') AS content,
                   COUNT(DISTINCT visit_id) FILTER (WHERE event = 'page_view')::int AS visitors,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id))
                     FILTER (WHERE event = 'signup_completed')::int AS signups
              FROM funnel_events
             WHERE {window}
             GROUP BY 1, 2, 3
             ORDER BY visitors DESC, signups DESC
             LIMIT 30
            """
        )
        devices = fetch_all(
            f"""
            SELECT COALESCE(NULLIF(device, ''), 'desktop') AS device,
                   COUNT(DISTINCT visit_id) FILTER (WHERE event = 'page_view')::int AS visitors,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id))
                     FILTER (WHERE event = 'signup_completed')::int AS signups
              FROM funnel_events
             WHERE {window}
             GROUP BY 1
             ORDER BY visitors DESC
            """
        )
        referrers = fetch_all(
            f"""
            SELECT substring(referrer from '^https?://([^/]+)') AS host,
                   COUNT(DISTINCT visit_id)::int AS visitors
              FROM funnel_events
             WHERE event = 'page_view'
               AND referrer IS NOT NULL
               AND {window}
             GROUP BY 1
             ORDER BY visitors DESC
             LIMIT 20
            """
        )
    except Exception as exc:
        _log.warning("acquisition query failed: %s", exc)
        return empty

    by_day = {
        row["day"]: {
            "views": int(row["views"] or 0),
            "visitors": int(row["visitors"] or 0),
        }
        for row in day_rows or []
    }
    today = datetime.now(ZoneInfo("America/New_York")).date()
    span = _RANGE_DAYS[range_key]
    days = []
    for offset in range(span - 1, -1, -1):
        day = (today - timedelta(days=offset)).isoformat()
        found = by_day.get(day) or {"views": 0, "visitors": 0}
        days.append({"day": day, "views": found["views"], "visitors": found["visitors"]})

    by_landing = {row["landing"]: row for row in campaigns or []}
    ordered = list(LANDING_SLUGS)
    for key in by_landing:
        if key not in ordered:
            ordered.append(key)
    campaign_rows = []
    for slug in ordered:
        raw = by_landing.get(slug) or {}
        campaign_rows.append({
            "landing": slug,
            "steps": conversion_rates(raw),
        })

    buckets = {"YouTube": 0, "Reddit": 0}
    extras = []
    for row in referrers or []:
        label = _referrer_bucket(row.get("host") or "")
        n = int(row.get("visitors") or 0)
        if label in buckets:
            buckets[label] += n
        else:
            extras.append({"host": label, "visitors": n})
    referrer_rows = [
        {"host": "YouTube", "visitors": buckets["YouTube"]},
        {"host": "Reddit", "visitors": buckets["Reddit"]},
    ] + extras[:8]

    return {
        "range_key": range_key,
        "days": days,
        "pages": pages or [],
        "campaigns": campaign_rows,
        "sources": sources or [],
        "devices": devices or [],
        "referrers": referrer_rows,
    }


def build_admin_analytics() -> dict:
    """Signups, funnel, UTM, and YouTube referrals for the last 30 days."""
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    from app.db import fetch_all

    empty = {
        "days": [],
        "steps": [],
        "sources": [],
        "youtube": {"visits": 0, "signups": 0},
        "acquisition": _empty_acquisition(_range_key()),
    }
    if not _db_ready():
        return empty
    try:
        day_rows = fetch_all(
            """
            SELECT to_char(created_at AT TIME ZONE 'America/New_York', 'YYYY-MM-DD') AS day,
                   COUNT(*)::int AS signups
              FROM funnel_events
             WHERE event = 'signup_completed'
               AND created_at >= NOW() - INTERVAL '30 days'
             GROUP BY 1
            """
        )
        counts = fetch_all(
            """
            SELECT event,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id))::int AS n
              FROM funnel_events
             WHERE created_at >= NOW() - INTERVAL '30 days'
             GROUP BY event
            """
        )
        sources = fetch_all(
            """
            SELECT COALESCE(NULLIF(utm_source, ''), '(none)') AS source,
                   COALESCE(NULLIF(utm_campaign, ''), '(none)') AS campaign,
                   COUNT(DISTINCT visit_id) FILTER (WHERE event = 'landing_view')::int AS visits,
                   COUNT(DISTINCT user_id) FILTER (WHERE event = 'signup_completed')::int AS signups,
                   COUNT(DISTINCT user_id) FILTER (WHERE event = 'broker_connected')::int AS brokers,
                   COUNT(DISTINCT user_id) FILTER (WHERE event = 'paid')::int AS paid
              FROM funnel_events
             WHERE created_at >= NOW() - INTERVAL '30 days'
             GROUP BY 1, 2
             ORDER BY visits DESC, signups DESC
             LIMIT 40
            """
        )
        yt = fetch_all(
            """
            SELECT
              COUNT(DISTINCT visit_id) FILTER (
                WHERE event = 'landing_view'
                  AND (referrer ILIKE '%%youtube.com%%' OR referrer ILIKE '%%youtu.be%%'
                       OR utm_source ILIKE 'youtube')
              )::int AS visits,
              COUNT(DISTINCT user_id) FILTER (
                WHERE event = 'signup_completed'
                  AND (referrer ILIKE '%%youtube.com%%' OR referrer ILIKE '%%youtu.be%%'
                       OR utm_source ILIKE 'youtube')
              )::int AS signups
              FROM funnel_events
             WHERE created_at >= NOW() - INTERVAL '30 days'
            """
        )
    except Exception as exc:
        _log.warning("admin analytics query failed: %s", exc)
        empty["acquisition"] = build_acquisition()
        return empty

    by_day = {row["day"]: int(row["signups"] or 0) for row in day_rows or []}
    today = datetime.now(ZoneInfo("America/New_York")).date()
    days = []
    for offset in range(29, -1, -1):
        day = (today - timedelta(days=offset)).isoformat()
        days.append({"day": day, "signups": by_day.get(day, 0)})

    by_event = {row["event"]: int(row["n"] or 0) for row in counts or []}
    landing_n = by_event.get("landing_view") or 0
    steps = []
    for key, label in FUNNEL_STEPS:
        n = by_event.get(key) or 0
        steps.append({
            "key": key,
            "label": label,
            "count": n,
            "of_landing": (round(100.0 * n / landing_n, 1) if landing_n else None),
        })
    yt_row = (yt or [{}])[0] or {}
    return {
        "days": days,
        "steps": steps,
        "sources": sources or [],
        "youtube": {
            "visits": int(yt_row.get("visits") or 0),
            "signups": int(yt_row.get("signups") or 0),
        },
        "acquisition": build_acquisition(),
    }


def register(app):
    from app.extensions import csrf, limiter

    def _beacon():
        payload = request.get_json(silent=True)
        if payload is None and request.form:
            payload = request.form.to_dict()
        body, status = beacon(payload)
        from flask import jsonify
        return jsonify(body), status

    guarded = csrf.exempt(limiter.limit("120 per minute; 2000 per hour")(_beacon))
    app.add_url_rule(
        "/funnel/beacon",
        endpoint="funnel_beacon",
        view_func=guarded,
        methods=["POST"],
    )
