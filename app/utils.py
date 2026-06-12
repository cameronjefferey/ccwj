# app/utils.py
import os
from urllib.parse import urlencode, urlparse

from flask import abort, current_app, flash, jsonify, redirect, request, url_for
from flask_login import current_user

# Post-login ?next= must stay on this site (relative path + query only).
_MAX_INTERNAL_NEXT_LEN = 2048


def safe_internal_next(raw) -> str | None:
    """
    Validate a redirect target after login: same-origin path and query only.
    Rejects full URLs, scheme-relative //... open redirects, and overlong values.
    """
    if raw is None:
        return None
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s or len(s) > _MAX_INTERNAL_NEXT_LEN:
        return None
    if not s.startswith("/") or s.startswith("//"):
        return None
    if "\\" in s or "\x00" in s:
        return None
    p = urlparse(s)
    if p.netloc:
        return None
    return s


def read_sql_file(filename: str) -> str:
    sql_path = os.path.join("app", "queries", filename)
    with open(sql_path, "r") as f:
        return f.read()


# ---------------------------------------------------------------------------
# EarningsFollower deep-links
# ---------------------------------------------------------------------------
#
# EarningsFollower is a separate, already-deployed product (a themed earnings
# calendar). We connect to it with URL deep-links only — no API calls, no
# changes on the EarningsFollower side. It's a client-side SPA that serves the
# same shell at "/" and 404s on path routes, so the only server-guaranteed
# target is the home URL. We append query params (?symbol / ?theme / ?tab) as a
# BEST-EFFORT: if/when the SPA learns to read them it filters; until then the
# user still lands on a useful calendar. The base URL comes from config
# (EARNINGS_FOLLOWER_URL) so it can move off its current Render hostname.

# Curated subsector/sector -> EarningsFollower theme map. EarningsFollower
# organizes its calendar by theme (AI / space / quantum / semis), which is a
# looser concept than yfinance's sector/subsector taxonomy, so this is a small
# hand-maintained map rather than an exhaustive lookup. Unmapped values simply
# omit the ?theme param (we still pass the symbol). Keys are lowercased.
_EARNINGS_FOLLOWER_THEME_MAP = {
    "semiconductors": "semis",
    "semiconductor equipment & materials": "semis",
    "semiconductor equipment and materials": "semis",
}


def earnings_follower_theme_for(sector=None, subsector=None):
    """Map a yfinance sector/subsector to an EarningsFollower theme slug, or
    ``None`` when there's no confident mapping. Subsector wins over sector
    because it's the more specific signal (e.g. "Semiconductors")."""
    for value in (subsector, sector):
        if value:
            theme = _EARNINGS_FOLLOWER_THEME_MAP.get(str(value).strip().lower())
            if theme:
                return theme
    return None


def earnings_follower_url(symbol=None, theme=None, tab=None, sector=None, subsector=None):
    """Build a deep-link into the EarningsFollower web app.

    Returns the configured base (home) URL with best-effort query params
    appended. With no arguments you get the bare home URL. ``theme`` may be
    passed explicitly, or derived from ``sector``/``subsector`` via
    :func:`earnings_follower_theme_for`. Falls back to the public hostname if
    config isn't available (e.g. outside an app context in a unit test)."""
    try:
        base = (current_app.config.get("EARNINGS_FOLLOWER_URL") or "").rstrip("/")
    except RuntimeError:
        base = ""
    if not base:
        base = "https://earningsfollower-web.onrender.com"

    if theme is None:
        theme = earnings_follower_theme_for(sector=sector, subsector=subsector)

    params = {}
    if symbol:
        params["symbol"] = str(symbol).strip().upper()
    if theme:
        params["theme"] = str(theme).strip()
    if tab:
        params["tab"] = str(tab).strip()

    if not params:
        return base
    return f"{base}?{urlencode(params)}"


# ---------------------------------------------------------------------------
# Demo user write-protection
# ---------------------------------------------------------------------------
#
# The ``demo`` account is shared: anyone hitting ``/demo/start`` without
# signing up is logged in as the same Postgres user. That's fine for read
# pages (every visitor sees the same canned weekly review, mirror score,
# strategies, insights) but every write would let one stranger overwrite
# what the next stranger sees: rename the demo profile, publish or delete
# community trades, regenerate insights, replace the seed CSVs by uploading
# a different broker's export, etc.
#
# To keep the demo a faithful, predictable showcase we block writes server-
# side. Each route that mutates per-user state calls ``demo_block_writes``
# at the top of its POST handler. JSON endpoints get a 403 with a stable
# error code so the client can render an explanation; HTML form posts get
# a flash + redirect back to where they came from.
# ---------------------------------------------------------------------------

DEMO_USERNAME = "demo"


def is_demo_user() -> bool:
    """True iff the currently authenticated session is the shared demo user."""
    try:
        if not current_user.is_authenticated:
            return False
        return (current_user.username or "").lower() == DEMO_USERNAME
    except Exception:
        return False


def demo_block_writes(action: str = "this action"):
    """
    Short-circuit a POST handler when the demo user is signed in.

    Returns a Flask response (redirect/JSON) when the request must be blocked,
    or ``None`` when the caller should continue. Routes use it like::

        @app.route("/community/post", methods=["POST"])
        def submit_post():
            blocked = demo_block_writes("posting to the community")
            if blocked:
                return blocked
            ...

    Behaviour:
    - JSON / API requests get HTTP 403 with ``{"error": "demo_read_only", ...}``
      so client code can show its own banner.
    - Everything else gets a flash + 302 to ``next`` (when safe) or to the
      page the user was on. Falls back to the home/weekly-review page.
    """
    if not is_demo_user():
        return None

    msg = (
        f"The demo is read-only — sign up for a real account to try {action}. "
        "Your changes wouldn't persist for other people anyway."
    )

    wants_json = (
        request.path.startswith("/api/")
        or request.accept_mimetypes.best == "application/json"
        or (request.headers.get("X-Requested-With", "") == "XMLHttpRequest")
    )
    if wants_json:
        return (
            jsonify({"error": "demo_read_only", "message": msg}),
            403,
        )

    flash(msg, "info")

    nxt = safe_internal_next(request.form.get("next") or request.args.get("next"))
    if nxt:
        return redirect(nxt)

    referrer = request.referrer or ""
    if referrer.startswith(request.host_url):
        return redirect(referrer)

    try:
        return redirect(url_for("weekly_review"))
    except Exception:
        return redirect("/")
