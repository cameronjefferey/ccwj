"""Public demo sessions that are not the shared Postgres ``demo`` user.

``/demo/start`` used to call ``login_user`` on ``User.get_by_username("demo")``.
Flask-Login stored that row's serial id in the session as ``_user_id``. On a
database where ``demo`` was the first insert, every visitor was user 1.

A per-visitor Postgres user cannot own ``tenant_id='demo:demo-account'``
(``broker_tenants.tenant_id`` is a primary key, one tenant, one owner). Cloning
a user plus a tenant per visit would also write a row we then have to expire.

Each demo visit therefore gets a Flask-Login id ``demo-session:<token>`` that
is not a ``users.id``. Tenant reads resolve only to ``demo:demo-account``.
Queries that pass that id into Postgres are refused. Writes are refused in
``before_request`` as well as by the existing ``demo_block_writes`` checks.

Tradeoff: visitors still share the read-only warehouse mirror (that is the
product). They do not share a Postgres identity, so a demo session cannot
read or update another user's profile, tags, billing, or broker rows, and
it cannot become an admin even if ``ADMIN_USERS`` contains ``demo``.
"""
from __future__ import annotations

import json
import logging
import os
import re
import secrets
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from flask_login import UserMixin

log = logging.getLogger("happytrader.demo")

DEMO_SESSION_PREFIX = "demo-session:"
DEMO_TENANT_ID = "demo:demo-account"
DEMO_ACCOUNT_NAME = "Demo Account"

PAGE_CAP = 150
IP_SESSION_CAP = 10
IDLE_SECONDS = 30 * 60
HARD_SECONDS = 2 * 60 * 60
VISITOR_COOKIE = "ht_demo"
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{16,64}$")

TURNSTILE_VERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"

# App surfaces a crawler burns through once it is inside the demo.
# ``/learn`` is intentionally absent. ``/earningsfollower`` is a public
# bridge and must not match the ``/earnings`` prefix.
APP_PREFIXES = (
    "/overview",
    "/daily-review",
    "/weekly-review",
    "/today",
    "/positions",
    "/position",
    "/accounts",
    "/strategies",
    "/strategy-fit",
    "/sectors",
    "/industries",
    "/story",
    "/insights",
    "/earnings",
    "/symbols",
    "/wealth",
    "/profile",
    "/upload",
    "/settings",
    "/get-started",
    "/snaptrade",
    "/admin",
    "/billing",
    "/share",
    "/api/",
    "/internal/",
)

_lock = threading.Lock()
_memory_pages: dict[str, int] = {}
_memory_ips: dict[str, int] = {}
_memory_minted: dict[str, float] = {}
_redis_client = None
_redis_checked = False
_redis_warned = False
_turnstile_warned = False


class DemoSessionUser(UserMixin):
    """Authenticated principal that is not a row in ``users``."""

    is_ephemeral_demo = True

    def __init__(self, token: str):
        self.token = token
        self.username = "demo"
        self.email = None
        self.password_hash = None

    @property
    def id(self):
        return f"{DEMO_SESSION_PREFIX}{self.token}"

    def get_id(self):
        return self.id

    def check_password(self, _password: str) -> bool:
        return False


def is_ephemeral_demo_user(user=None) -> bool:
    """True only for a real demo-session principal.

    ``is True`` so a ``MagicMock`` current_user (tests stub the proxy)
    does not look like a demo session. A mock's missing attribute is
    another mock, which is truthy.
    """
    if user is None:
        from flask_login import current_user
        user = current_user
    return getattr(user, "is_ephemeral_demo", False) is True


def is_ephemeral_demo_id(value) -> bool:
    return (
        isinstance(value, str)
        and value.startswith(DEMO_SESSION_PREFIX)
        and len(value) > len(DEMO_SESSION_PREFIX)
    )


def is_demo_session_id(value) -> bool:
    """Alias for ``is_ephemeral_demo_id``. Demo Flask-Login ids are not ``users.id``."""
    return is_ephemeral_demo_id(value)


def numeric_user_id(user_id):
    """Postgres ``users.id``, or None when the value is not a numeric id.

    Public demo sessions use Flask-Login ids ``demo-session:<token>``.
    ``int()`` on that string raises ``ValueError`` and 500s any page that
    casts the session id. Reads return an empty result when this is None.
    Writes must not insert.
    """
    if user_id is None or isinstance(user_id, bool):
        return None
    if is_ephemeral_demo_id(user_id):
        return None
    if isinstance(user_id, int):
        return user_id
    if isinstance(user_id, float):
        if user_id != user_id:
            return None
        try:
            as_int = int(user_id)
        except (TypeError, ValueError, OverflowError):
            return None
        if as_int != user_id:
            return None
        return as_int
    text = str(user_id).strip()
    if not text or is_ephemeral_demo_id(text):
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def token_from_id(value) -> str | None:
    if not is_ephemeral_demo_id(value):
        return None
    return value[len(DEMO_SESSION_PREFIX):]


def demo_tenant_row() -> dict:
    """The public mirror tenant, with the owning Postgres user id removed."""
    row = None
    try:
        from app.models import get_broker_tenant
        row = get_broker_tenant(DEMO_TENANT_ID)
    except Exception as exc:
        log.warning("demo tenant lookup failed (using a static row): %s", exc)
    if row:
        out = dict(row)
    else:
        out = {
            "tenant_id": DEMO_TENANT_ID,
            "broker_slug": "demo",
            "broker_uuid": "demo-account",
            "account_name": DEMO_ACCOUNT_NAME,
            "account_mask": None,
            "broker_label": None,
            "institution_account_id": None,
            "snaptrade_connection_id": None,
            "connection_status": "active",
            "connection_broken_at": None,
            "first_sync_completed": True,
            "display_nickname": DEMO_ACCOUNT_NAME,
            "created_at": None,
            "updated_at": None,
        }
    out["tenant_id"] = DEMO_TENANT_ID
    out["user_id"] = None
    out["account_name"] = out.get("account_name") or DEMO_ACCOUNT_NAME
    return out


def is_app_path(path: str) -> bool:
    path = path or ""
    if path.startswith("/earningsfollower"):
        return False
    if path.startswith("/learn"):
        return False
    for prefix in APP_PREFIXES:
        if path == prefix or path.startswith(prefix + "/") or path.startswith(prefix):
            # ``/position`` matches ``/positions`` because of startswith.
            # Both are app pages, so that overlap is intentional.
            if prefix == "/earnings" and path.startswith("/earningsfollower"):
                return False
            return True
    return False


def turnstile_keys() -> tuple[str, str]:
    site = (os.environ.get("TURNSTILE_SITE_KEY") or "").strip()
    secret = (os.environ.get("TURNSTILE_SECRET_KEY") or "").strip()
    return site, secret


def verify_turnstile(token: str, remote_ip: str, *, fail_open: bool = False) -> bool:
    """True when the widget passed, or when Turnstile is not configured.

    Unset keys skip the check and log once so the demo keeps working on a
    deploy that has not created a Cloudflare widget yet. When keys are set,
    a missing token, a network error, or ``success: false`` fails closed.

    Signup passes ``fail_open=True``. A script that never loads (no token)
    or a Cloudflare/network error must not reject the account. A completed
    challenge with ``success: false`` still rejects. The signup rate limit
    stays in front of this check.
    """
    global _turnstile_warned
    site, secret = turnstile_keys()
    if not site or not secret:
        if not _turnstile_warned:
            _turnstile_warned = True
            log.warning(
                "TURNSTILE_SITE_KEY / TURNSTILE_SECRET_KEY unset; "
                "demo start is not challenging visitors"
            )
        return True
    if not (token or "").strip():
        if fail_open:
            log.warning(
                "Turnstile token missing; continuing because the widget "
                "script did not load"
            )
            return True
        return False
    payload = urllib.parse.urlencode({
        "secret": secret,
        "response": token,
        "remoteip": remote_ip or "",
    }).encode()
    req = urllib.request.Request(TURNSTILE_VERIFY_URL, data=payload)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        if fail_open:
            log.warning("Turnstile verify failed open: %s", exc)
            return True
        log.warning("Turnstile verify failed closed: %s", exc)
        return False
    if isinstance(body, dict) and body.get("success"):
        return True
    if fail_open and isinstance(body, dict):
        errors = body.get("error-codes") or []
        infra = {"internal-error"}
        if errors and set(errors) <= infra:
            log.warning("Turnstile verify failed open: %s", errors)
            return True
    return False


def _redis():
    """Lazy Redis from ``REDIS_URL``. Memory fallback logs once."""
    global _redis_client, _redis_checked, _redis_warned
    if (os.environ.get("DEMO_CAPS_MEMORY") or "").strip() == "1":
        return None
    url = (os.environ.get("REDIS_URL") or "").strip()
    if not url:
        if not _redis_warned:
            _redis_warned = True
            log.warning(
                "REDIS_URL unset; demo session caps are in-memory for this process"
            )
        return None
    if _redis_checked:
        return _redis_client
    _redis_checked = True
    try:
        import redis
        client = redis.Redis.from_url(
            url, socket_connect_timeout=1, socket_timeout=1
        )
        client.ping()
        _redis_client = client
        return client
    except Exception as exc:
        log.warning("REDIS_URL set but demo caps fell back to memory: %s", exc)
        _redis_client = None
        return None


def _today_key(ip: str) -> str:
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"{ip}|{day}"


def reserve_demo_ip(ip: str) -> bool:
    """Count a new demo session for this IP. False when today's cap is hit."""
    ip = (ip or "unknown").strip() or "unknown"
    client = _redis()
    if client is not None:
        try:
            key = f"demo:ip:{_today_key(ip)}"
            count = int(client.incr(key))
            if count == 1:
                client.expire(key, 36 * 60 * 60)
            return count <= IP_SESSION_CAP
        except Exception as exc:
            log.warning("demo IP cap redis failed; using memory: %s", exc)
    bucket = _today_key(ip)
    with _lock:
        count = _memory_ips.get(bucket, 0) + 1
        _memory_ips[bucket] = count
        return count <= IP_SESSION_CAP


def remember_demo_visitor(token: str, started_at: float) -> None:
    """Remember a minted demo token so the same browser can resume it."""
    if not token or not _TOKEN_RE.match(token):
        return
    started = float(started_at)
    with _lock:
        _memory_minted[token] = started
    client = _redis()
    if client is None:
        return
    try:
        client.setex(
            f"demo:mint:{token}",
            HARD_SECONDS + 60,
            str(started),
        )
    except Exception as exc:
        log.warning("demo visitor redis failed; memory still holds it: %s", exc)


def demo_visitor_started_at(token: str | None) -> float | None:
    """Started-at for a token this process (or Redis) actually minted."""
    token = (token or "").strip()
    if not _TOKEN_RE.match(token):
        return None
    client = _redis()
    if client is not None:
        try:
            raw = client.get(f"demo:mint:{token}")
            if raw:
                return float(raw)
        except Exception as exc:
            log.warning("demo visitor lookup failed; using memory: %s", exc)
    with _lock:
        started = _memory_minted.get(token)
    return float(started) if started is not None else None


def reusable_demo_visitor() -> tuple[str, float] | None:
    """Cookie token that is still inside the 2-hour hard window."""
    from flask import request

    token = (request.cookies.get(VISITOR_COOKIE) or "").strip()
    started = demo_visitor_started_at(token)
    if started is None:
        return None
    if (time.time() - started) > HARD_SECONDS:
        return None
    return token, started


def demo_start_reuses_visitor() -> bool:
    """Limiter exemption: a live demo, or a return with the visitor cookie.

    A second click from the same browser is the same session. It must not
    burn the per-IP start cap. A missing or forged cookie still counts.
    """
    from flask import request

    if request.method != "POST":
        return False
    try:
        if is_ephemeral_demo_user():
            return True
    except Exception:
        pass
    try:
        return reusable_demo_visitor() is not None
    except Exception:
        return False


def attach_demo_visitor_cookie(response, token: str, started_at: float):
    """First-party cookie so the next /demo/start resumes this session."""
    from flask import current_app

    remaining = int(HARD_SECONDS - (time.time() - float(started_at)))
    if remaining < 1 or not _TOKEN_RE.match(token or ""):
        return response
    secure = False
    try:
        secure = bool(current_app.config.get("SESSION_COOKIE_SECURE"))
    except Exception:
        secure = False
    response.set_cookie(
        VISITOR_COOKIE,
        token,
        max_age=remaining,
        httponly=True,
        samesite="Lax",
        secure=secure,
        path="/",
    )
    return response


def note_demo_page(token: str) -> bool:
    """Count one app page. False when this session is over the cap."""
    if not token:
        return False
    client = _redis()
    if client is not None:
        try:
            key = f"demo:pages:{token}"
            count = int(client.incr(key))
            ttl = client.ttl(key)
            if ttl is None or int(ttl) < 0:
                client.expire(key, HARD_SECONDS + 60)
            return count <= PAGE_CAP
        except Exception as exc:
            log.warning("demo page cap redis failed; using memory: %s", exc)
    with _lock:
        count = _memory_pages.get(token, 0) + 1
        _memory_pages[token] = count
        return count <= PAGE_CAP


def reset_demo_caps() -> None:
    """Test helper. Drops the in-process counters and the Redis client cache."""
    global _redis_client, _redis_checked, _redis_warned, _turnstile_warned
    with _lock:
        _memory_pages.clear()
        _memory_ips.clear()
        _memory_minted.clear()
    _redis_client = None
    _redis_checked = False
    _redis_warned = False
    _turnstile_warned = False


def end_demo_session(message: str):
    """Drop the demo principal. On the gate itself, keep rendering it."""
    from flask import flash, redirect, request, session, url_for
    from flask_login import logout_user

    logout_user()
    session.clear()
    flash(message, "info")
    if (request.path or "").startswith("/demo/"):
        return None
    return redirect(url_for("demo_start"))


def limited_page(reason: str):
    from flask import make_response, render_template

    try:
        body = render_template(
            "demo_limited.html",
            title="Demo limit",
            reason=reason,
        )
    except Exception:
        body = (
            "<div class='card p-4'><h1>Demo limit</h1>"
            f"<p>{reason}</p></div>"
        )
    return make_response(body, 429)


def guard_demo_request():
    """before_request body: retire the shared demo cookie, cap pages, block writes."""
    from flask import request, session
    from flask_login import current_user

    path = request.path or ""
    if path.startswith("/static/") or path.startswith("/healthz") or path == "/version":
        return None

    ephemeral = is_ephemeral_demo_user(current_user)
    username = (getattr(current_user, "username", None) or "")
    shared_demo = (
        getattr(current_user, "is_authenticated", False)
        and not ephemeral
        and username.lower() == "demo"
    )
    if shared_demo and is_app_path(path):
        return end_demo_session(
            "The shared demo session has ended. Start a new demo from the homepage."
        )

    if not ephemeral:
        return None

    now = time.time()
    started = session.get("_demo_started_at")
    try:
        started_f = float(started) if started is not None else None
    except (TypeError, ValueError):
        started_f = None
    if started_f is None or (now - started_f) > HARD_SECONDS:
        return end_demo_session("This demo session hit the 2-hour limit.")
    last = session.get("_last_activity_ts")
    try:
        last_f = float(last) if last is not None else None
    except (TypeError, ValueError):
        last_f = None
    if last_f is not None and (now - last_f) > IDLE_SECONDS:
        return end_demo_session(
            "This demo session timed out after 30 minutes of inactivity."
        )

    if request.method not in ("GET", "HEAD", "OPTIONS") and not path.startswith("/learn"):
        allowed = {("POST", "/logout"), ("POST", "/demo/start")}
        if (request.method, path) not in allowed:
            from app.utils import demo_block_writes
            blocked = demo_block_writes("that")
            if blocked is not None:
                return blocked

    if request.method in ("GET", "HEAD") and is_app_path(path):
        token = session.get("_demo_token") or token_from_id(
            getattr(current_user, "id", None)
        )
        if not note_demo_page(token or ""):
            return limited_page(
                "This demo session has viewed 150 pages. "
                "Create an account to keep going on your own data."
            )
    return None


def demo_heavy_exempt() -> bool:
    """flask-limiter exempt_when: limit only ephemeral demo hits on app pages."""
    try:
        from flask import request
        from flask_login import current_user
        if not is_ephemeral_demo_user(current_user):
            return True
        return not is_app_path(request.path or "")
    except Exception:
        return True
