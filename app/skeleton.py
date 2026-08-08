"""Instant-shell ("skeleton") rendering for BigQuery-heavy pages.

The heavy pages (Daily Review, Positions, Accounts, Position Detail) block
1-6s on cold BigQuery queries before the browser gets a single byte — the
user stares at a frozen white screen. This module makes those pages paint
a shimmer shell in ~100ms and fill in the real page when the data lands.

Mechanism — deliberately boring, no client-side hydration framework:

1. A plain navigation GET returns ``_skeleton.html`` (zero BQ work; the
   base-template context processors are all Postgres-only).
2. The shell's inline JS re-requests the SAME url with an ``X-HT-Full: 1``
   header. That request runs the page's normal, untouched server render.
3. The shell replaces the document via ``document.open()/write()/close()``
   — a fresh parse, so every inline script executes with natural
   semantics: DOMContentLoaded fires again, Chart.js inits run, defer
   scripts load from browser cache. No script re-execution hackery.
4. If the full render was fast (warm query cache), the wrapper sets a
   short-lived per-endpoint cookie; while present the server skips the
   shell entirely so warm navigations don't pay the extra round-trip.

Redirect-aware: if the full fetch lands somewhere else (login, or
``_redirect_if_no_accounts`` bouncing to /get-started), the shell follows
with ``location.replace`` instead of writing the wrong page in place.
"""

import functools
import time

from flask import make_response, render_template, request

from app import app

# Full render faster than this ⇒ the query cache is warm for this user and
# the shell round-trip would only ADD latency; mark the browser warm.
_FAST_RENDER_SECONDS = 1.5

# How long a "warm" cookie suppresses the shell. Matches the L1 query-cache
# TTL (10 min) — after that the cache may be cold again and the shell earns
# its keep.
_WARM_COOKIE_MAX_AGE = 600

_FULL_HEADER = "X-HT-Full"


def _wants_full_page(endpoint_cookie):
    if request.headers.get(_FULL_HEADER) == "1":
        return True
    if request.args.get("_full") == "1":  # JS-error fallback path only
        return True
    if request.cookies.get(endpoint_cookie):
        return True
    # Only genuine browser NAVIGATIONS get the shell. Every modern browser
    # stamps top-level navigations with Sec-Fetch-Mode: navigate; test
    # clients, curl, cron monitors and older browsers don't — they must get
    # the real page in one round-trip (progressive enhancement).
    return request.headers.get("Sec-Fetch-Mode") != "navigate"


def skeleton_page(view):
    """Decorate a heavy page view: serve an instant shimmer shell first.

    Apply INSIDE ``@login_required`` (closest to the function) so auth
    redirects still fire before the shell is ever served.
    """

    @functools.wraps(view)
    def wrapper(*args, **kwargs):
        cookie_name = "ht_fast_" + (request.endpoint or "page").replace(".", "_")

        if not _wants_full_page(cookie_name):
            return render_template("_skeleton.html")

        started = time.monotonic()
        resp = make_response(view(*args, **kwargs))
        elapsed = time.monotonic() - started
        if resp.status_code == 200 and elapsed < _FAST_RENDER_SECONDS:
            resp.set_cookie(
                cookie_name, "1",
                max_age=_WARM_COOKIE_MAX_AGE,
                httponly=True, samesite="Lax",
                secure=not app.debug,
            )
        return resp

    return wrapper
