"""Instant-shell ("skeleton") rendering for BigQuery-heavy pages.

The heavy pages (Daily Review, Positions, Accounts, Position Detail) block
1-6s on cold BigQuery queries before the browser gets a single byte — the
user stares at a frozen white screen. This module makes those pages paint
a shimmer shell in ~100ms and fill in the real page when the data lands.

Mechanism — deliberately boring, no client-side hydration framework:

1. A plain navigation GET returns ``_skeleton.html`` (zero BQ work, and
   the shell is a standalone document so base-template context processors
   do not run their Postgres round-trips).
2. The shell's inline JS re-requests the SAME url with an ``X-HT-Full: 1``
   header. That request runs the page's normal, untouched server render.
3. The shell replaces the document via ``document.open()/write()/close()``
   — a fresh parse, so every inline script executes with natural
   semantics: DOMContentLoaded fires again, Chart.js inits run, defer
   scripts load from browser cache. No script re-execution hackery.
4. If the full render was fast (warm query cache), the wrapper sets a
   per-endpoint cookie AND a Redis sentinel; while present the server
   skips the shell entirely so warm navigations don't pay the extra
   round-trip. The post-rebuild warmer also stamps the sentinel so the
   first visit after a warehouse rebuild can skip the shell too.

Redirect-aware: if the full fetch lands somewhere else (login, or
``_redirect_if_no_accounts`` bouncing to /get-started), the shell follows
with ``location.replace`` instead of writing the wrong page in place.
"""

import functools
import time

from flask import g, make_response, render_template, request

from app import app

# Cached Today/Positions still take ~2.4–3.6s of Python + HTML. The old
# 1.5s bar meant the warm cookie almost never set, so every click paid
# two round-trips (shell + full). 4s covers a warm cache hit without
# treating a 10s cold Overview as "warm".
_FAST_RENDER_SECONDS = 4.0

# How long a "warm" cookie suppresses the shell. Matches the shared L2
# query-cache TTL (24h, close-based). A warehouse rebuild flushes Redis
# and the warmer restamps the sentinel; the cookie may last until then
# and the next full render is still correct (just not shimmer-gated).
_WARM_COOKIE_MAX_AGE = 86400

_FULL_HEADER = "X-HT-Full"


def is_skeleton_render() -> bool:
    """True while serving the instant shell (no warehouse, no Postgres nav)."""
    return bool(getattr(g, "_ht_skeleton", False))


def _cookie_name():
    return "ht_fast_" + (request.endpoint or "page").replace(".", "_")


def _warm_extra():
    """Disambiguate Position Detail sentinels by symbol; other pages are one key."""
    if (request.endpoint or "") == "position_detail":
        return ((request.view_args or {}).get("symbol") or "").strip()
    return ""


def _request_has_account_filter():
    """Filtered scopes are different cache keys; don't skip the shell on the
    all-accounts sentinel when the user picked ?tenants= / ?groups=."""
    args = request.args
    return bool(
        (args.get("tenants") or "").strip()
        or (args.get("tenant") or "").strip()
        or (args.get("groups") or "").strip()
        or (args.get("account") or "").strip()
    )


def _redis_says_warm():
    """Post-rebuild warmer (and prior fast renders) stamp this per user+page."""
    try:
        from flask_login import current_user
        from app.query_cache import is_page_warm
    except Exception:
        return False
    uid = None
    try:
        if getattr(current_user, "is_authenticated", False):
            uid = current_user.id
    except Exception:
        return False
    if uid is None:
        return False
    if _request_has_account_filter():
        # Individual-tenant warmer stamps extra=tenant_id; honor a single
        # ?tenant= / ?tenants=<one> match so picking one Schwab account
        # after a rebuild can still skip the shell.
        extra = (request.args.get("tenant") or "").strip()
        if not extra:
            tenants = (request.args.get("tenants") or "").strip()
            parts = [p.strip() for p in tenants.split(",") if p.strip()]
            extra = parts[0] if len(parts) == 1 else ""
        if extra:
            return is_page_warm(uid, request.endpoint or "page", extra)
        return False
    return is_page_warm(uid, request.endpoint or "page", _warm_extra())


def _wants_full_page(endpoint_cookie):
    if request.headers.get(_FULL_HEADER) == "1":
        return True
    if request.args.get("_full") == "1":  # JS-error fallback path only
        return True
    if request.cookies.get(endpoint_cookie):
        return True
    if _redis_says_warm():
        return True
    # Only genuine browser NAVIGATIONS get the shell. Every modern browser
    # stamps top-level navigations with Sec-Fetch-Mode: navigate; test
    # clients, curl, cron monitors and older browsers don't — they must get
    # the real page in one round-trip (progressive enhancement).
    return request.headers.get("Sec-Fetch-Mode") != "navigate"


def _stamp_warm(endpoint_cookie):
    """Cookie + Redis sentinel so the next navigation (any device) skips the shell."""
    try:
        from flask_login import current_user
        from app.query_cache import mark_page_warm
        uid = current_user.id if getattr(current_user, "is_authenticated", False) else None
        if uid is not None:
            extra = _warm_extra()
            if _request_has_account_filter():
                extra = (request.args.get("tenant") or "").strip() or extra
            mark_page_warm(uid, request.endpoint or "page", extra)
    except Exception:
        pass
    return endpoint_cookie


def skeleton_page(view):
    """Decorate a heavy page view: serve an instant shimmer shell first.

    Apply INSIDE ``@login_required`` (closest to the function) so auth
    redirects still fire before the shell is ever served.
    """

    @functools.wraps(view)
    def wrapper(*args, **kwargs):
        cookie_name = _cookie_name()

        if not _wants_full_page(cookie_name):
            g._ht_skeleton = True
            return render_template("_skeleton.html")

        started = time.monotonic()
        resp = make_response(view(*args, **kwargs))
        elapsed = time.monotonic() - started
        if resp.status_code == 200 and elapsed < _FAST_RENDER_SECONDS:
            _stamp_warm(cookie_name)
            resp.set_cookie(
                cookie_name, "1",
                max_age=_WARM_COOKIE_MAX_AGE,
                httponly=True, samesite="Lax",
                secure=not app.debug,
            )
        return resp

    return wrapper
