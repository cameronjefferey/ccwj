"""First-party funnel analytics.

Events carry a path, a referrer host+path, UTM fields, the Reddit click
id, and the user agent (bot filtering only — it is not shown in admin).
They do not carry email, name, IP, or a raw query string. ``user_id``
is the internal account id, written only after signup.

``ht_touch`` is the first-party cookie. First-touch is whatever arrived on
the first visit. Last-touch updates when a later visit brings new UTMs or
a new ``rdt_cid``. Signup copies both onto ``users``.
"""
from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import logging
import os
import re
import threading
import time
import uuid
from datetime import date, timedelta
from urllib.parse import urlparse

from flask import current_app, g, has_request_context, request, session

_log = logging.getLogger(__name__)

COOKIE = "ht_touch"
COOKIE_DAYS = 90
INTERNAL_COOKIE = "ht_internal"
INTERNAL_COOKIE_DAYS = 365
# Owner and screenshot logins. These stay out of Acquisition even when
# ADMIN_USERS is empty or incomplete. ``demo`` is the public demo and
# is not in this set.
_OWNER_USERNAMES = frozenset({
    "cameron",
    "cameron3",
    "happycameron",
    "testingcameron",
    "testingcameron1",
})
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
    # pixel is off. detail carries a CTA name, a scroll mark, a time
    # bucket, or a video id.
    "page_view": {"first": False, "reddit": None},
    "cta_click": {"first": False, "reddit": None},
    "scroll_depth": {"first": False, "reddit": None},
    "time_on_page": {"first": False, "reddit": None},
    "video_play": {"first": False, "reddit": None},
}
# The browser beacon may only name these. Paid and signup stay server-side.
# client_seen is not a countable row — it marks the visit's rows as a browser.
BEACON_EVENTS = frozenset({
    "lesson_started",
    "lesson_completed",
    "landing_view",
    "cta_click",
    "scroll_depth",
    "time_on_page",
    "video_play",
    "client_seen",
})
# Case-insensitive. Shared with Postgres ``user_agent ~*`` in
# ``backfill_bot_user_agents``, so the syntax stays valid in both engines.
# ``bot`` must end a token (Googlebot, Applebot, bingbot, redditbot) and
# must not match inside a longer word such as "bottom". Named crawlers
# stay in the pattern so a shorter UA still matches. ``curl`` is the
# whole token or a curl/version string. Link-preview and non-JS fetchers
# that do not say "bot" are listed on their own.
_BOT_UA_PATTERN = (
    r"bot(?:[^A-Za-z]|$)|"
    r"crawler|spider|headlesschrome|python-requests|curl/|"
    r"(^|[^A-Za-z])curl(?:[^A-Za-z]|$)|"
    r"playwright|puppeteer|slackbot|twitterbot|facebookexternalhit|redditbot|"
    r"embedly|snap url preview|whatsapp|telegrambot|discordbot|linkedinbot|"
    r"slack-imgproxy|meta-externalagent|gptbot|claudebot|perplexitybot|"
    r"bytespider|ahrefsbot|semrushbot"
)
_LEFT_OUT_LABELS = {
    "bot": "Bot",
    "internal": "Our own traffic",
    "early": "Left before tracking loaded",
}
_BOT_UA_RE = re.compile(_BOT_UA_PATTERN, re.IGNORECASE)
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
# Scroll marks the browser already sends. A visit that reached 75% also
# has rows for 25 and 50, because the client posts every mark it crossed.
SCROLL_COLUMNS = (
    ("25", "s25"),
    ("50", "s50"),
    ("75", "s75"),
    ("100", "s100"),
)
# Visible-time buckets, in seconds. The label is what Acquisition shows.
TIME_BUCKETS = (
    ("5", "5s", "t5"),
    ("15", "15s", "t15"),
    ("30", "30s", "t30"),
    ("60", "1m", "t60"),
    ("120", "2m", "t120"),
    ("300", "5m", "t300"),
)
_SCROLL_DETAILS = frozenset(mark for mark, _key in SCROLL_COLUMNS)
_TIME_DETAILS = frozenset(bucket for bucket, _label, _key in TIME_BUCKETS)
# The only live ad. Acquisition counts this page and signups attributed
# to this campaign. Other /go/* landings stay out of the page.
REAL_PNL_PATH = "/go/real-pnl"
REAL_PNL_CAMPAIGN = "real-pnl"
# Paid tab day buckets. "Today" and By day already cut on this zone.
_ANALYTICS_TZ = "America/New_York"
# Reddit ads for /go/real-pnl launched on this calendar day. The Paid
# tab counts only events on or after it, so pre-launch traffic does not
# dilute scroll, time-on-page, and signup rates. The boundary is midnight
# in _ANALYTICS_TZ, the same day cut the page already uses.
PAID_CAMPAIGN_START = date(2026, 10, 5)
_MONTHS = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
)
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
    """Coarse device class. The raw user agent is stored for bot filtering
    and is not shown in the admin tables.

    A bare Reddit in-app string (``Reddit/Version …/iOS …``) has no
    iPhone or Mobile token. ``ios`` as its own token is a phone. iPad
    stays a tablet because that check runs first.
    """
    if user_agent is None and has_request_context():
        user_agent = request.headers.get("User-Agent") or ""
    ua = (user_agent or "").lower()
    if "ipad" in ua or "tablet" in ua:
        return "tablet"
    if any(tok in ua for tok in ("mobi", "iphone", "android", "phone")):
        return "phone"
    if re.search(r"(^|[^a-z])ios([^a-z]|$)", ua):
        return "phone"
    return "desktop"


def is_bot_user_agent(user_agent) -> bool:
    """True for crawlers, headless browsers, HTTP clients, and link unfurlers."""
    if not user_agent:
        return False
    return _BOT_UA_RE.search(str(user_agent)) is not None


def internal_usernames() -> set[str]:
    """Accounts whose traffic stays out of Acquisition.

    Owner and test logins, ``INTERNAL_USERS``, and ``ADMIN_USERS``.
    ``demo`` is never internal — that username is the public demo.
    """
    names = set(_OWNER_USERNAMES)
    for env_name in ("INTERNAL_USERS", "ADMIN_USERS"):
        for part in (os.environ.get(env_name) or "").split(","):
            part = part.strip().lower()
            if part and part != "demo" and re.fullmatch(r"[a-z0-9_]+", part):
                names.add(part)
    return names


def ip_is_internal(ip: str) -> bool:
    """True when ``ip`` is listed in ``INTERNAL_IPS`` (exact or CIDR)."""
    raw = (os.environ.get("INTERNAL_IPS") or "").strip()
    if not raw or not ip or ip == "unknown":
        return False
    try:
        addr = ipaddress.ip_address(ip.strip())
    except ValueError:
        return False
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            if "/" in part:
                if addr in ipaddress.ip_network(part, strict=False):
                    return True
            elif addr == ipaddress.ip_address(part):
                return True
        except ValueError:
            continue
    return False


def _viewer_username():
    if not has_request_context():
        return None
    try:
        from flask_login import current_user
        if current_user.is_authenticated:
            name = getattr(current_user, "username", None)
            if isinstance(name, str) and name.strip():
                return name.strip()
    except Exception:
        return None
    return None


def is_internal_account(username) -> bool:
    if not username:
        return False
    from app.models import is_admin
    if is_admin(username):
        return True
    return username.strip().lower() in internal_usernames()


def request_is_internal() -> bool:
    """Signed-in owners, the opt-out cookie, ``?ht_internal=1``, or INTERNAL_IPS."""
    if not has_request_context():
        return False
    if (request.args.get("ht_internal") or "") == "1":
        return True
    if (request.cookies.get(INTERNAL_COOKIE) or "") == "1":
        return True
    try:
        from app.client_ip import real_client_ip
        if ip_is_internal(real_client_ip()):
            return True
    except Exception:
        pass
    return is_internal_account(_viewer_username())


def _internal_reason() -> str:
    if not has_request_context():
        return ""
    if (request.args.get("ht_internal") or "") == "1":
        return "query"
    if (request.cookies.get(INTERNAL_COOKIE) or "") == "1":
        return "cookie"
    try:
        from app.client_ip import real_client_ip
        if ip_is_internal(real_client_ip()):
            return "ip"
    except Exception:
        pass
    if is_internal_account(_viewer_username()):
        return "account"
    return ""


def note_internal_visit(reason: str) -> None:
    """Remember this anonymous cookie so later logged-out hits stay out."""
    if not _db_ready() or not has_request_context():
        return
    visit_id = (current_attribution().get("visit_id") or "")
    if not visit_id:
        return
    try:
        from app.db import execute
        execute(
            """
            INSERT INTO funnel_internal_visits (visit_id, reason)
            VALUES (%s, %s)
            ON CONFLICT (visit_id) DO NOTHING
            """,
            (visit_id, (reason or "")[:80]),
        )
    except Exception as exc:
        _log.warning("internal visit note skipped: %s", exc)


def _cookie_secure() -> bool:
    try:
        return bool(current_app.config.get("SESSION_COOKIE_SECURE"))
    except Exception:
        return False


def _attach_internal_cookie(response):
    if not has_request_context() or not request_is_internal():
        return response
    if (request.cookies.get(INTERNAL_COOKIE) or "") == "1":
        return response
    response.set_cookie(
        INTERNAL_COOKIE,
        "1",
        max_age=INTERNAL_COOKIE_DAYS * 24 * 60 * 60,
        httponly=True,
        samesite="Lax",
        secure=_cookie_secure(),
        path="/",
    )
    return response


def _internal_username_sql() -> str:
    """Quoted username list for the human and left-out filters."""
    return ", ".join("'" + name + "'" for name in sorted(internal_usernames()))


def page_wants_client_beacon(path=None) -> bool:
    """Logged-out pages that insert a funnel row a browser beacon can mark.

    Same paths as ``_is_public_path``. The inline head script uses this
    so a bounce does not wait on ``funnel.js``.
    """
    if path is None and has_request_context():
        path = request.path or ""
    return _is_public_path(path or "")


def human_traffic_sql() -> str:
    """Rows that are not bots and not an internal account or visit.

    A page view logged before login is dropped when that same visit later
    has an owner or test ``user_id``, even if the visit was never stamped
    in ``funnel_internal_visits``. A scroll or time row is dropped when
    any row on that visit is a bot, so a headless page view cannot keep
    a later beacon that omitted the crawler user agent. ``client_beacon``
    is separate: old rows
    are NULL and still count. A new page view or signup start stays
    FALSE until the inline head beacon sets TRUE.

    A NULL ``visit_id`` or ``user_id`` never matches the equality inside
    these ``NOT EXISTS`` checks, so those rows stay. Do not wrap them in
    ``visit_id IS NULL OR NOT EXISTS``: that form stops Postgres from
    hashing the anti-join and prices each analytics aggregate at tens of
    millions, which is what made ``/admin/analytics`` run until the
    proxy gave up.
    """
    quoted = _internal_username_sql()
    return (
        "(COALESCE(is_bot, FALSE) = FALSE "
        "AND NOT EXISTS ("
        "SELECT 1 FROM funnel_internal_visits iv "
        "WHERE iv.visit_id = funnel_events.visit_id) "
        "AND NOT EXISTS ("
        "SELECT 1 FROM users u WHERE u.id = funnel_events.user_id "
        f"AND lower(u.username) IN ({quoted})) "
        "AND NOT EXISTS ("
        "SELECT 1 FROM funnel_events owner_hit "
        "JOIN users u ON u.id = owner_hit.user_id "
        "WHERE owner_hit.visit_id = funnel_events.visit_id "
        f"AND lower(u.username) IN ({quoted})) "
        "AND NOT EXISTS ("
        "SELECT 1 FROM funnel_events bot_hit "
        "WHERE bot_hit.visit_id = funnel_events.visit_id "
        "AND COALESCE(bot_hit.is_bot, FALSE) = TRUE))"
    )


def counted_traffic_sql() -> str:
    """Human rows. Page views and signup starts need a browser beacon.

    NULL beacons still count (rows from before the beacon column). A new
    row is inserted FALSE until the inline head script posts
    ``client_seen`` (``funnel.js`` posts it only when that script did
    not). Signup starts used to count those FALSE rows, so a prefetch,
    a curl, or a browser that never ran JavaScript looked like someone
    who opened the form. Completions stay in the count either way: the
    account exists even when the beacon never arrives.
    """
    return (
        f"{human_traffic_sql()} "
        "AND (event NOT IN ('page_view', 'signup_started') "
        "OR client_beacon IS DISTINCT FROM FALSE)"
    )


def backfill_bot_user_agents() -> None:
    """Mark stored user agents that match the bot list. NULL agents stay."""
    if not _db_ready():
        return
    from app.db import execute
    execute(
        """
        UPDATE funnel_events
           SET is_bot = TRUE
         WHERE user_agent IS NOT NULL
           AND user_agent ~* %s
           AND is_bot IS DISTINCT FROM TRUE
        """,
        (_BOT_UA_PATTERN,),
    )


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
        user_agent = ""
        if has_request_context():
            user_agent = (request.headers.get("User-Agent") or "")[:512]
        landing = lt.get("landing") or ""
        variant = lt.get("variant") or ""
        if event == "landing_view" and _recent_same(event, visit_id, path, "", 12):
            return None
        if event == "page_view" and _recent_same(event, visit_id, path, "", 1):
            return None
        if event in ("cta_click", "scroll_depth", "time_on_page", "video_play") and _recent_same(
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
                 rdt_cid, landing, variant, device, detail,
                 user_agent, is_bot, client_beacon)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, FALSE)
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
                user_agent or None,
                is_bot_user_agent(user_agent),
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
            if (
                not path.startswith("/static")
                and not path.startswith("/healthz")
                and path != "/version"
            ):
                attach_cookie(response)
                _attach_internal_cookie(response)
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
        or path == "/version"
        or path.startswith("/funnel/")
        or path == "/favicon.ico"
    ):
        return
    if request_is_internal():
        note_internal_visit(_internal_reason())
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
    # Same audience as the /signup page view: a logged-out 200. A signed-in
    # visitor is redirected before this, and a reload of the same visit is
    # dropped by the first=True dedupe inside log_event.
    if (
        method == "GET"
        and path == "/signup"
        and status == 200
        and _viewer_user_id() is None
    ):
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


def _mark_client_beacon() -> None:
    """The browser ran JavaScript. Page views for this cookie now count."""
    if not _db_ready() or not has_request_context():
        return
    visit_id = (current_attribution().get("visit_id") or "")
    if not visit_id:
        return
    try:
        from app.db import execute
        execute(
            """
            UPDATE funnel_events
               SET client_beacon = TRUE
             WHERE visit_id = %s
               AND created_at > NOW() - INTERVAL '2 days'
               AND client_beacon IS DISTINCT FROM TRUE
            """,
            (visit_id,),
        )
    except Exception as exc:
        _log.warning("client beacon skipped: %s", exc)


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
    if request_is_internal():
        note_internal_visit(_internal_reason())
    if event == "client_seen":
        _mark_client_beacon()
        return {"ok": True}, 200
    path = payload.get("path") or (request.path if has_request_context() else "")
    if not isinstance(path, str) or not path.startswith("/") or "?" in path or len(path) > 300:
        path = request.path if has_request_context() else ""
    detail = payload.get("detail") or ""
    if not isinstance(detail, str):
        return {"ok": False}, 400
    detail = clean_utm(detail)
    if event in ("cta_click", "scroll_depth", "time_on_page", "video_play") and not detail:
        return {"ok": False}, 400
    if event == "scroll_depth" and detail not in _SCROLL_DETAILS:
        return {"ok": False}, 400
    if event == "time_on_page" and detail not in _TIME_DETAILS:
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


def ads_tracking_blocked() -> bool:
    """Internal visitors and bots never fire the pixel or Conversions API.

    Covers ``?ht_internal=1``, the ``ht_internal`` cookie, internal IPs,
    signed-in owner accounts, and crawler user agents.
    """
    if not has_request_context():
        return False
    if request_is_internal():
        return True
    return is_bot_user_agent(request.headers.get("User-Agent") or "")


def _consume_pixel_flags() -> None:
    """Drop one-shot events so a later page cannot fire them."""
    if not has_request_context():
        return
    for key in ("ht_reddit_signup", "ht_reddit_lead", "ht_reddit_purchase"):
        session.pop(key, None)


def reddit_pixel_events() -> list[dict]:
    """Events the browser pixel should fire on this response. Empty when
    the pixel id is unset, the browser sent DNT / GPC, or the visit is
    internal or a bot."""
    if tracking_opt_out() or ads_tracking_blocked():
        _consume_pixel_flags()
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


def _capi_test_id() -> str:
    """Events Manager test id. Empty means the field is left off the body."""
    try:
        raw = current_app.config.get("REDDIT_CAPI_TEST_ID")
    except Exception:
        raw = os.environ.get("REDDIT_CAPI_TEST_ID")
    return (raw or "").strip()


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
    if tracking_opt_out() or ads_tracking_blocked() or _user_opted_out(user_id):
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
    # test_id sits on data, beside events — Reddit's Event testing field.
    # Omit the key entirely when unset so production payloads stay clean.
    data = {"events": [event]}
    test_id = _capi_test_id()
    if test_id:
        data = {"test_id": test_id, "events": [event]}
    body = {"data": data}
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


def _pct(count, visitors) -> float:
    visitors = int(visitors or 0)
    if not visitors:
        return 0.0
    return round(100.0 * int(count or 0) / visitors, 1)


def click_target_label(detail, path=None) -> str:
    """Button label from a click row, or the path when the label is blank."""
    label = (detail or "").strip()
    if label:
        return label
    fallback = (path or "").strip()
    return fallback or "(unknown)"


def _destination_rows(rows, page_visitors) -> list[dict]:
    """Click targets for one landing. Video starts keep a video: prefix."""
    out = []
    for row in rows or []:
        event = (row.get("event") or "").strip()
        label = click_target_label(row.get("detail"), row.get("path"))
        if event == "video_play" and not label.startswith("video:"):
            label = "video:" + label
        n_vis = int(row.get("visitors") or 0)
        n_clicks = int(row.get("clicks") or 0)
        out.append({
            "target": label,
            "visitors": n_vis,
            "clicks": n_clicks,
            "rate": _pct(n_vis, page_visitors),
        })
    return out


def _as_int(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def time_bucket_label(seconds) -> str | None:
    """Display label for a dwell bucket. Unknown numbers stay as seconds."""
    sec = _as_int(seconds)
    if sec is None:
        return None
    for bucket, label, _key in TIME_BUCKETS:
        if int(bucket) == sec:
            return label
    return f"{sec}s"


def _engagement_metrics(row, visitors, median_sec) -> dict:
    row = row or {}
    visitors = int(visitors or 0)
    scroll = []
    for mark, key in SCROLL_COLUMNS:
        count = int(row.get(key) or 0)
        scroll.append({"mark": mark, "count": count, "rate": _pct(count, visitors)})
    time_cells = []
    for bucket, label, key in TIME_BUCKETS:
        count = int(row.get(key) or 0)
        time_cells.append({
            "bucket": bucket,
            "label": label,
            "count": count,
            "rate": _pct(count, visitors),
        })
    sec = _as_int(median_sec)
    return {
        "scroll": scroll,
        "time": time_cells,
        "time_median": time_bucket_label(sec),
        "time_median_sec": sec,
    }


def _time_details_sql() -> str:
    return ", ".join(f"'{bucket}'" for bucket, _label, _key in TIME_BUCKETS)


def _real_pnl_signup_sql() -> str:
    """signup_completed attributed to the live landing or its utm campaign."""
    camp = REAL_PNL_CAMPAIGN
    return (
        "(event = 'signup_completed' AND "
        f"(landing = '{camp}' OR utm_campaign = '{camp}'))"
    )


def _paid_since_label() -> str:
    return f"{_MONTHS[PAID_CAMPAIGN_START.month - 1]} {PAID_CAMPAIGN_START.day}"


def _paid_campaign_since_sql() -> str:
    """Inclusive analytics-zone calendar day of the Reddit ads launch."""
    return (
        f"(created_at AT TIME ZONE '{_ANALYTICS_TZ}')::date "
        f">= DATE '{PAID_CAMPAIGN_START.isoformat()}'"
    )


def _paid_window_sql(range_key: str) -> str:
    """Range picker, floored at the ads launch.

    Today / 7 days / 30 days keep their existing predicates. Events
    before PAID_CAMPAIGN_START stay out even when the range starts earlier.
    """
    return f"({_RANGE_WHERE[range_key]}) AND {_paid_campaign_since_sql()}"


def _analytics_today():
    from datetime import datetime
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo(_ANALYTICS_TZ)).date()


def _paid_day_span(today, range_key: str):
    """Calendar days in the range, none earlier than the ads launch."""
    span = _RANGE_DAYS[range_key]
    start = today - timedelta(days=span - 1)
    if start < PAID_CAMPAIGN_START:
        start = PAID_CAMPAIGN_START
    days = []
    cursor = start
    while cursor <= today:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _empty_left_out() -> dict:
    return {
        "bot": 0,
        "internal": 0,
        "early": 0,
        "early_reddit": 0,
        "early_other": 0,
        "browsers": [],
    }


def _truncate_user_agent(value) -> str:
    """Display form of a stored user agent. No other identity fields."""
    text = (value or "").replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return "Unknown"
    if len(text) > 120:
        return text[:120]
    return text


def _left_out_ctes(path: str, window: str) -> str:
    """One reason per /go/real-pnl page-view visit, in priority order.

    Bot (this row or any bot hit on the visit), then our own traffic
    (internal-visit stamp or an owner/admin/test username on the visit),
    then left before tracking loaded (``client_beacon`` FALSE and neither
    of the above). ``Reddit/`` is a sub-count of that last bucket. The
    SELECT lists that follow must not project visit id, IP, email, or
    click id.

    Bot visits and owner visits are scanned once and joined. A correlated
    ``EXISTS`` on ``funnel_events.visit_id`` here was a sequential scan
    per visit (no index, and the probe could not be hashed). Two of those
    queries on the Paid tab were enough for ``/admin/analytics`` to sit
    until the proxy timed out with no error page.
    """
    quoted = _internal_username_sql()
    return f"""
    WITH page_rows AS (
        SELECT visit_id,
               created_at,
               COALESCE(is_bot, FALSE) AS row_bot,
               client_beacon IS FALSE AS beacon_false,
               user_agent,
               COALESCE(NULLIF(device, ''), 'desktop') AS device
          FROM funnel_events
         WHERE event = 'page_view'
           AND path = '{path}'
           AND visit_id IS NOT NULL
           AND {window}
    ),
    flags AS (
        SELECT visit_id,
               BOOL_OR(row_bot) AS row_bot,
               BOOL_OR(beacon_false) AS beacon_false,
               BOOL_OR(COALESCE(user_agent, '') LIKE '%%Reddit/%%') AS reddit_app
          FROM page_rows
         GROUP BY visit_id
    ),
    latest AS (
        SELECT DISTINCT ON (visit_id)
               visit_id,
               user_agent,
               device
          FROM page_rows
         ORDER BY visit_id, created_at DESC
    ),
    bot_visits AS (
        SELECT DISTINCT bot_hit.visit_id
          FROM funnel_events bot_hit
         WHERE bot_hit.visit_id IS NOT NULL
           AND COALESCE(bot_hit.is_bot, FALSE)
    ),
    owner_visits AS (
        SELECT DISTINCT owner_hit.visit_id
          FROM funnel_events owner_hit
          JOIN users u ON u.id = owner_hit.user_id
         WHERE owner_hit.visit_id IS NOT NULL
           AND lower(u.username) IN ({quoted})
    ),
    marked AS (
        SELECT l.visit_id,
               l.user_agent,
               l.device,
               f.reddit_app,
               CASE
                 WHEN f.row_bot OR bv.visit_id IS NOT NULL THEN 'bot'
                 WHEN iv.visit_id IS NOT NULL OR ov.visit_id IS NOT NULL THEN 'internal'
                 WHEN f.beacon_false THEN 'early'
                 ELSE NULL
               END AS reason
          FROM flags f
          JOIN latest l ON l.visit_id = f.visit_id
          LEFT JOIN bot_visits bv ON bv.visit_id = f.visit_id
          LEFT JOIN funnel_internal_visits iv ON iv.visit_id = f.visit_id
          LEFT JOIN owner_visits ov ON ov.visit_id = f.visit_id
    )
    """


def _left_out_from_rows(reason_row, browser_rows) -> dict:
    """Shape the left-out queries into the Paid disclosure."""
    reason_row = reason_row or {}
    bot = int(reason_row.get("bot") or 0)
    internal = int(reason_row.get("internal") or 0)
    early = int(reason_row.get("early") or 0)
    early_reddit = int(reason_row.get("early_reddit") or 0)
    early_other = int(reason_row.get("early_other") or 0)
    browsers = []
    for row in browser_rows or []:
        reason = (row.get("reason") or "").strip()
        if reason not in _LEFT_OUT_LABELS:
            continue
        raw_ua = row.get("user_agent")
        ua = raw_ua if isinstance(raw_ua, str) else ""
        browsers.append({
            "user_agent": _truncate_user_agent(ua),
            "device": device_type(ua) if ua else (row.get("device") or "desktop"),
            "reason": reason,
            "reason_label": _LEFT_OUT_LABELS[reason],
            "count": int(row.get("n") or 0),
        })
    return {
        "bot": bot,
        "internal": internal,
        "early": early,
        "early_reddit": early_reddit,
        "early_other": early_other,
        "browsers": browsers,
    }


def _empty_acquisition(range_key="7d") -> dict:
    metrics = _engagement_metrics({}, 0, None)
    return {
        "range_key": range_key,
        "since_label": _paid_since_label(),
        "visitors": 0,
        "views": 0,
        "signups": 0,
        "signup_rate": 0.0,
        "days": [],
        "scroll": metrics["scroll"],
        "time": metrics["time"],
        "time_median": None,
        "time_median_sec": None,
        "sources": [],
        "devices": [],
        "destinations": [],
        "filtered_out": 0,
        "left_out": _empty_left_out(),
    }


def build_acquisition(range_key=None) -> dict:
    """Real people on /go/real-pnl: visitors, scroll, dwell, signups, clicks.

    Other landings are not in this card. Counts use counted_traffic_sql,
    so bots, headless browsers, unbeaconed page views, internal visits,
    and owner or test accounts (including a later login on the same
    visit) stay out. Click rows are the cta_click and video_play events
    funnel.js already writes for this path. The window is the selected
    range floored at PAID_CAMPAIGN_START (Reddit ads launch).
    """
    from app.db import fetch_all

    range_key = range_key or _range_key()
    if range_key not in _RANGE_WHERE:
        range_key = "7d"
    window = _paid_window_sql(range_key)
    counted = counted_traffic_sql()
    empty = _empty_acquisition(range_key)
    if not _db_ready():
        return empty
    path = REAL_PNL_PATH
    signup = _real_pnl_signup_sql()
    on_page = (
        f"path = '{path}' AND event IN "
        "('page_view', 'scroll_depth', 'time_on_page')"
    )
    attributed = (
        f"((event = 'page_view' AND path = '{path}') OR {signup})"
    )
    scroll_cols = ",\n               ".join(
        "COUNT(DISTINCT visit_id) FILTER "
        f"(WHERE event = 'scroll_depth' AND detail = '{mark}')::int AS {key}"
        for mark, key in SCROLL_COLUMNS
    )
    time_cols = ",\n               ".join(
        "COUNT(DISTINCT visit_id) FILTER "
        f"(WHERE event = 'time_on_page' AND detail = '{bucket}')::int AS {key}"
        for bucket, _label, key in TIME_BUCKETS
    )
    try:
        totals = fetch_all(
            f"""
            SELECT COUNT(*) FILTER (WHERE event = 'page_view')::int AS views,
                   COUNT(DISTINCT visit_id) FILTER (WHERE event = 'page_view')::int AS visitors,
                   {scroll_cols},
                   {time_cols}
              FROM funnel_events
             WHERE {on_page}
               AND {window}
               AND {counted}
            """
        )
        signup_rows = fetch_all(
            f"""
            SELECT COUNT(DISTINCT COALESCE(user_id::text, visit_id))::int AS signups
              FROM funnel_events
             WHERE {signup}
               AND {window}
               AND {counted}
            """
        )
        day_rows = fetch_all(
            f"""
            SELECT to_char(created_at AT TIME ZONE '{_ANALYTICS_TZ}', 'YYYY-MM-DD') AS day,
                   COUNT(*) FILTER (
                       WHERE event = 'page_view' AND path = '{path}'
                   )::int AS views,
                   COUNT(DISTINCT visit_id) FILTER (
                       WHERE event = 'page_view' AND path = '{path}'
                   )::int AS visitors,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id)) FILTER (
                       WHERE {signup}
                   )::int AS signups
              FROM funnel_events
             WHERE {attributed}
               AND {window}
               AND {counted}
             GROUP BY 1
            """
        )
        median_rows = fetch_all(
            f"""
            SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY max_sec) AS median_sec
              FROM (
                    SELECT visit_id,
                           MAX(detail::int) AS max_sec
                      FROM funnel_events
                     WHERE event = 'time_on_page'
                       AND path = '{path}'
                       AND detail IN ({_time_details_sql()})
                       AND visit_id IS NOT NULL
                       AND {window}
                       AND {counted}
                     GROUP BY visit_id
                   ) dwell
            """
        )
        sources = fetch_all(
            f"""
            SELECT COALESCE(NULLIF(utm_source, ''), '(none)') AS source,
                   COALESCE(NULLIF(utm_campaign, ''), '(none)') AS campaign,
                   COALESCE(NULLIF(utm_content, ''), '(none)') AS content,
                   COUNT(DISTINCT visit_id) FILTER (
                       WHERE event = 'page_view' AND path = '{path}'
                   )::int AS visitors,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id)) FILTER (
                       WHERE {signup}
                   )::int AS signups
              FROM funnel_events
             WHERE {attributed}
               AND {window}
               AND {counted}
             GROUP BY 1, 2, 3
             ORDER BY visitors DESC, signups DESC
             LIMIT 30
            """
        )
        devices = fetch_all(
            f"""
            SELECT COALESCE(NULLIF(device, ''), 'desktop') AS device,
                   COUNT(DISTINCT visit_id) FILTER (
                       WHERE event = 'page_view' AND path = '{path}'
                   )::int AS visitors,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id)) FILTER (
                       WHERE {signup}
                   )::int AS signups
              FROM funnel_events
             WHERE {attributed}
               AND {window}
               AND {counted}
             GROUP BY 1
             ORDER BY visitors DESC
            """
        )
        destinations = fetch_all(
            f"""
            SELECT event,
                   detail,
                   path,
                   COUNT(*)::int AS clicks,
                   COUNT(DISTINCT visit_id)::int AS visitors
              FROM funnel_events
             WHERE event IN ('cta_click', 'video_play')
               AND path = '{path}'
               AND {window}
               AND {counted}
             GROUP BY 1, 2, 3
             ORDER BY visitors DESC, clicks DESC, detail
             LIMIT 40
            """
        )
        left_ctes = _left_out_ctes(path, window)
        reason_rows = fetch_all(
            f"""
            {left_ctes}
            SELECT COUNT(*) FILTER (WHERE reason = 'bot')::int AS bot,
                   COUNT(*) FILTER (WHERE reason = 'internal')::int AS internal,
                   COUNT(*) FILTER (WHERE reason = 'early')::int AS early,
                   COUNT(*) FILTER (
                       WHERE reason = 'early' AND reddit_app
                   )::int AS early_reddit,
                   COUNT(*) FILTER (
                       WHERE reason = 'early' AND NOT reddit_app
                   )::int AS early_other
              FROM marked
            """
        )
        browser_rows = fetch_all(
            f"""
            {left_ctes}
            SELECT left(COALESCE(user_agent, ''), 120) AS user_agent,
                   reason,
                   COUNT(*)::int AS n
              FROM marked
             WHERE reason IS NOT NULL
             GROUP BY 1, 2
             ORDER BY n DESC, user_agent, reason
             LIMIT 15
            """
        )
    except Exception as exc:
        _log.warning("acquisition query failed: %s", exc)
        return empty

    total = (totals or [None])[0] or {}
    visitors = int(total.get("visitors") or 0)
    views = int(total.get("views") or 0)
    signups = int(((signup_rows or [None])[0] or {}).get("signups") or 0)
    median_sec = ((median_rows or [None])[0] or {}).get("median_sec")
    metrics = _engagement_metrics(total, visitors, median_sec)

    by_day = {
        row["day"]: {
            "views": int(row["views"] or 0),
            "visitors": int(row["visitors"] or 0),
            "signups": int(row["signups"] or 0),
        }
        for row in day_rows or []
    }
    today = _analytics_today()
    days = []
    for day in _paid_day_span(today, range_key):
        key = day.isoformat()
        found = by_day.get(key) or {"views": 0, "visitors": 0, "signups": 0}
        days.append({
            "day": key,
            "views": found["views"],
            "visitors": found["visitors"],
            "signups": found["signups"],
        })

    left_out = _left_out_from_rows((reason_rows or [None])[0], browser_rows)
    return {
        "range_key": range_key,
        "since_label": _paid_since_label(),
        "visitors": visitors,
        "views": views,
        "signups": signups,
        "signup_rate": _pct(signups, visitors),
        "days": days,
        "scroll": metrics["scroll"],
        "time": metrics["time"],
        "time_median": metrics["time_median"],
        "time_median_sec": metrics["time_median_sec"],
        "sources": sources or [],
        "devices": devices or [],
        "destinations": _destination_rows(destinations, visitors),
        "filtered_out": (
            left_out["bot"] + left_out["internal"] + left_out["early"]
        ),
        "left_out": left_out,
    }


def _with_signup_rate(rows) -> list[dict]:
    out = []
    for row in rows or []:
        n_visitors = int(row.get("visitors") or 0)
        n_signups = int(row.get("signups") or 0)
        out.append({
            "source": row.get("source") or "Direct",
            "visitors": n_visitors,
            "signups": n_signups,
            "rate": _pct(n_signups, n_visitors),
        })
    return out


def _referrer_bucket(host: str) -> str:
    text = (host or "").lower()
    if "youtube.com" in text or "youtu.be" in text:
        return "YouTube"
    if "reddit.com" in text:
        return "Reddit"
    return host or "(none)"


def _analytics_tab() -> str:
    raw = ""
    if has_request_context():
        raw = (request.args.get("tab") or "").strip().lower()
    if raw == "product":
        return "product"
    return "paid"


def _empty_product(range_key="7d") -> dict:
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
        "origins": [],
        "filtered_out": 0,
        "signup_days": [],
        "steps": [
            {"key": key, "label": label, "count": 0, "of_landing": None}
            for key, label in FUNNEL_STEPS
        ],
        "channels": [],
        "youtube": {"visits": 0, "signups": 0},
    }


def build_product_report(range_key=None) -> dict:
    """Product funnel and engagement, off the paid campaign card.

    The selected window covers visitors, pages, each landing's ladder
    (visitors → CTA → signup → lesson → paper → broker → paid), sources,
    devices, and referrers. Signup days, the step ladder, YouTube, and
    source × campaign stay on a fixed 30-day window, the same cut this
    page used before the paid card was narrowed to /go/real-pnl.
    The human filter matches the paid card.
    """
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    from app.db import fetch_all
    from app.go_landings import LANDING_SLUGS

    range_key = range_key or _range_key()
    if range_key not in _RANGE_WHERE:
        range_key = "7d"
    window = _RANGE_WHERE[range_key]
    counted = counted_traffic_sql()
    empty = _empty_product(range_key)
    if not _db_ready():
        return empty
    last_30 = "created_at >= NOW() - INTERVAL '30 days'"
    try:
        day_rows = fetch_all(
            f"""
            SELECT to_char(created_at AT TIME ZONE 'America/New_York', 'YYYY-MM-DD') AS day,
                   COUNT(*) FILTER (WHERE event = 'page_view')::int AS views,
                   COUNT(DISTINCT visit_id) FILTER (WHERE event = 'page_view')::int AS visitors
              FROM funnel_events
             WHERE {window}
               AND {counted}
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
               AND {counted}
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
               AND {counted}
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
               AND {counted}
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
               AND {counted}
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
               AND {counted}
             GROUP BY 1
             ORDER BY visitors DESC
             LIMIT 20
            """
        )
        origins = fetch_all(
            f"""
            SELECT CASE
                     WHEN NULLIF(utm_source, '') IS NOT NULL THEN
                       utm_source || CASE
                         WHEN NULLIF(utm_campaign, '') IS NOT NULL
                         THEN ' / ' || utm_campaign
                         ELSE ''
                       END
                     WHEN substring(referrer from '^https?://([^/]+)') IS NOT NULL
                     THEN substring(referrer from '^https?://([^/]+)')
                     ELSE 'Direct'
                   END AS source,
                   COUNT(DISTINCT visit_id) FILTER (WHERE event = 'page_view')::int AS visitors,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id))
                     FILTER (WHERE event = 'signup_completed')::int AS signups
              FROM funnel_events
             WHERE {window}
               AND {counted}
             GROUP BY 1
             ORDER BY visitors DESC, signups DESC
             LIMIT 30
            """
        )
        filtered_rows = fetch_all(
            f"""
            SELECT COUNT(DISTINCT visit_id)::int AS n
              FROM funnel_events
             WHERE event = 'page_view'
               AND {window}
               AND NOT (
                 {human_traffic_sql()}
                 AND client_beacon IS DISTINCT FROM FALSE
               )
            """
        )
        signup_day_rows = fetch_all(
            f"""
            SELECT to_char(created_at AT TIME ZONE 'America/New_York', 'YYYY-MM-DD') AS day,
                   COUNT(*)::int AS signups
              FROM funnel_events
             WHERE event = 'signup_completed'
               AND {last_30}
               AND {counted}
             GROUP BY 1
            """
        )
        counts = fetch_all(
            f"""
            SELECT event,
                   COUNT(DISTINCT COALESCE(user_id::text, visit_id))::int AS n
              FROM funnel_events
             WHERE {last_30}
               AND {counted}
             GROUP BY event
            """
        )
        channels = fetch_all(
            f"""
            SELECT COALESCE(NULLIF(utm_source, ''), '(none)') AS source,
                   COALESCE(NULLIF(utm_campaign, ''), '(none)') AS campaign,
                   COUNT(DISTINCT visit_id) FILTER (WHERE event = 'landing_view')::int AS visits,
                   COUNT(DISTINCT user_id) FILTER (WHERE event = 'signup_completed')::int AS signups,
                   COUNT(DISTINCT user_id) FILTER (WHERE event = 'broker_connected')::int AS brokers,
                   COUNT(DISTINCT user_id) FILTER (WHERE event = 'paid')::int AS paid
              FROM funnel_events
             WHERE {last_30}
               AND {counted}
             GROUP BY 1, 2
             ORDER BY visits DESC, signups DESC
             LIMIT 40
            """
        )
        yt = fetch_all(
            f"""
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
             WHERE {last_30}
               AND {counted}
            """
        )
    except Exception as exc:
        _log.warning("product analytics query failed: %s", exc)
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

    by_signup_day = {row["day"]: int(row["signups"] or 0) for row in signup_day_rows or []}
    signup_days = []
    for offset in range(29, -1, -1):
        day = (today - timedelta(days=offset)).isoformat()
        signup_days.append({"day": day, "signups": by_signup_day.get(day, 0)})

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
        "range_key": range_key,
        "days": days,
        "pages": pages or [],
        "campaigns": campaign_rows,
        "sources": sources or [],
        "devices": devices or [],
        "referrers": referrer_rows,
        "origins": _with_signup_rate(origins),
        "filtered_out": int(((filtered_rows or [{}])[0] or {}).get("n") or 0),
        "signup_days": signup_days,
        "steps": steps,
        "channels": channels or [],
        "youtube": {
            "visits": int(yt_row.get("visits") or 0),
            "signups": int(yt_row.get("signups") or 0),
        },
    }


def build_admin_analytics() -> dict:
    """Paid /go/real-pnl card, or the product funnel on its own tab.

    The product queries run only when the Product tab is open, so the
    paid card stays the focused campaign view.
    """
    tab = _analytics_tab()
    if tab == "product":
        return {
            "tab": "product",
            "acquisition": None,
            "product": build_product_report(),
        }
    return {
        "tab": "paid",
        "acquisition": build_acquisition(),
        "product": None,
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
    app.add_template_global(page_wants_client_beacon, name="page_wants_client_beacon")
    app.add_url_rule(
        "/funnel/beacon",
        endpoint="funnel_beacon",
        view_func=guarded,
        methods=["POST"],
    )
