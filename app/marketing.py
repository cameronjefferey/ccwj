"""Marketing, static, onboarding, and health-check routes.

Everything here is either logged-out surface (landing, pricing, FAQ,
privacy/terms, sitemap/robots, feature pages), lightweight infra
(healthz probes, ping), or the pre-data onboarding flow (get-started,
feedback, onboarding survey, Pro waitlist). None of it reads the
warehouse beyond get-started's single has-data COUNT(*).

Extracted verbatim from app/routes.py (routes.py refactor, Aug 2026).
Routes register on import via @app.route — endpoint names unchanged.
"""

from flask import render_template, request, redirect, url_for, Response, flash, abort
from flask_login import login_required, current_user

from app import app
from app.extensions import limiter
from app.bigquery_client import get_bigquery_client
from app.models import get_tenant_ids_for_user
from app.tenant_scope import tenant_sql_filter as _tenant_sql_filter


# ------------------------------------------------------------------
# Feature pages for marketing (logged-out)
# ------------------------------------------------------------------
FEATURES = {
    "strategy-auto-detection": {
        "title": "Strategy auto-detection",
        "subtitle": "Every position classified automatically—no manual tagging.",
        "demo_partial": "features/_demo_strategy.html",
        "value_bullets": [
            "Covered Calls, Cash-Secured Puts, Wheels, spreads, and Buy and Hold—all identified from your trade data.",
            "See exactly which strategies drive your returns and which drain performance.",
            "Stop guessing. Know whether the Wheel is outperforming CSPs for your portfolio.",
        ],
    },
    "ai-trading-insights": {
        "title": "AI trading insights",
        "subtitle": "Personalized analysis of your trading style and performance.",
        "demo_partial": "features/_demo_insights.html",
        "value_bullets": [
            "Get a data-driven overview: what's working, what's leaking, and why.",
            "Observations grounded in your actual trades—not generic advice.",
            "The 'wow' moment when the app shows it truly understands your trading.",
        ],
    },
    "performance-charts": {
        "title": "Performance charts",
        "subtitle": "Cumulative P&L over time, broken down by equity, options, and dividends.",
        "demo_partial": "features/_demo_charts.html",
        "value_bullets": [
            "Visualize your progress. See how each strategy contributes over time.",
            "Portfolio-wide and per-account charts so nothing stays hidden.",
            "The full picture—not just today's balance, but the journey.",
        ],
    },
    "position-detail": {
        "title": "Position detail",
        "subtitle": "Drill into any symbol: trades, strategies, and cumulative P&L.",
        "demo_partial": "features/_demo_position.html",
        "value_bullets": [
            "Click any symbol to see its full story: every trade, every strategy, every dollar.",
            "Understand why a position performed the way it did—before your next move.",
            "Trade history, current positions, and charts in one place.",
        ],
    },
    "multi-account": {
        "title": "Multi-account",
        "subtitle": "Track all your Schwab accounts in one place.",
        "demo_partial": "features/_demo_multiaccount.html",
        "value_bullets": [
            "IRA, taxable, joint—see portfolio-wide metrics and per-account breakdowns.",
            "Filter by account on every view: positions, tax center, performance.",
            "One dashboard. All your accounts.",
        ],
    },
}


@app.route("/features/<slug>")
def feature_detail(slug):
    """Feature detail page with demo and value prop."""
    if slug == "ai-trading-insights" and not app.config.get("INSIGHTS_ENABLED", True):
        abort(404)
    feature = FEATURES.get(slug)
    if not feature:
        abort(404)
    return render_template(
        "features/detail.html",
        title=feature["title"],
        feature=feature,
        all_features=FEATURES,
        current_slug=slug,
    )


@app.route("/pricing")
def pricing():
    """Pricing placeholder for marketing."""
    waitlisted = False
    try:
        if current_user.is_authenticated:
            from app.models import is_user_on_pro_waitlist
            waitlisted = is_user_on_pro_waitlist(current_user.id)
    except Exception:
        waitlisted = False
    return render_template(
        "pricing.html",
        title="Pricing",
        pro_waitlisted=waitlisted,
    )


@app.route("/pro/waitlist", methods=["POST"])
def pro_waitlist():
    """Add an email (or current user) to the Pro tier waitlist."""
    from app.models import add_pro_waitlist_entry
    from app.utils import demo_block_writes

    # Demo: every visitor would be 'demo' on the waitlist, which is noise
    # and would confuse outreach later.
    blocked = demo_block_writes("joining the Pro waitlist")
    if blocked:
        return blocked

    email = (request.form.get("email") or "").strip().lower()
    user_id = current_user.id if current_user.is_authenticated else None

    if not user_id and not email:
        flash("Enter an email address so we can notify you.", "warning")
        return redirect(url_for("pricing"))

    if not user_id:
        # Light email validation
        if "@" not in email or "." not in email or len(email) > 320:
            flash("That email doesn't look right. Try again?", "warning")
            return redirect(url_for("pricing"))

    try:
        add_pro_waitlist_entry(user_id=user_id, email=email or None)
        flash("You're on the waitlist. We'll be in touch when Pro is ready.", "success")
    except Exception as exc:
        app.logger.exception("Pro waitlist signup failed: %s", exc)
        flash("Couldn't add you to the waitlist right now. Try again in a moment.", "danger")

    return redirect(url_for("pricing"))


# ------------------------------------------------------------------
# Beta feedback inbox
# ------------------------------------------------------------------


@app.route("/feedback", methods=["POST"])
@limiter.limit("5 per minute; 30 per hour")
def submit_feedback():
    """
    Footer Send-Feedback button posts here.

    Anonymous users CAN submit (we capture their IP for spam triage) so
    a tester who hits a 500 on a logged-out page can still report it.
    Demo user is allowed — feedback from the demo seat is signal, not
    noise. We hard-cap the body at 4 KB in the model layer.

    Returns JSON for XHR clients (the modal uses fetch) and redirects
    for plain form submits so the route degrades gracefully without JS.
    """
    from app.models import save_feedback

    body = (request.form.get("body") or request.form.get("message") or "").strip()
    page_path = (request.form.get("page_path") or request.referrer or "")[:512]

    user_id = current_user.id if current_user.is_authenticated else None
    username = current_user.username if current_user.is_authenticated else None

    wants_json = (
        request.accept_mimetypes.best == "application/json"
        or request.headers.get("X-Requested-With", "") == "XMLHttpRequest"
    )

    if not body:
        if wants_json:
            return {"ok": False, "error": "Tell us what's up — the message can't be empty."}, 400
        flash("Tell us what's up — the message can't be empty.", "warning")
        return redirect(request.referrer or url_for("index"))

    new_id = save_feedback(
        user_id=user_id,
        username=username,
        body=body,
        page_path=page_path or None,
        user_agent=(request.headers.get("User-Agent") or "")[:512] or None,
        ip_address=request.remote_addr,
    )

    if new_id is None:
        if wants_json:
            return {"ok": False, "error": "We couldn't save that just now. Try again in a minute."}, 500
        flash("We couldn't save that just now. Try again in a minute.", "danger")
        return redirect(request.referrer or url_for("index"))

    if wants_json:
        return {"ok": True, "id": new_id}
    flash("Thanks — feedback received. We read every message.", "success")
    return redirect(request.referrer or url_for("index"))


# ------------------------------------------------------------------
# Onboarding survey (multi-section wizard during first sync wait)
# ------------------------------------------------------------------
#
# Posted by the wizard on /sync/processing. Validates that every
# required question has an answer, packages the form into a single
# JSONB blob via save_onboarding_response, and returns JSON. The
# form-side JS swaps to a thank-you note on success and clears the
# "hold redirect" flag so the sync poll on the same page can take
# the user to Daily Review.

# Required radio/textarea keys the wizard MUST answer before submit.
# Free-text "_other" siblings are optional and only saved when the
# matching radio's value is "other". The list lives next to the route
# (not in models.py) on purpose: the form's contract is a
# request-layer concern, while the storage shape is a single JSONB
# blob — see AGENTS.md note on JSONB-flexibility for this table.
_ONBOARDING_REQUIRED_KEYS: tuple[str, ...] = (
    "why_here",
    "worth_paying_for",
    "trading_years",
    "primary_style",
    "trade_frequency",
    "position_count",
    "best_at",
    "worst_at",
    "discipline_self",
    "trade_notes",
    "help_most",          # multi-select; at least one option required
    "one_thing",          # textarea, min 10 non-whitespace chars
    "comfort",
)

# Optional adjunct keys — saved only when present and non-empty.
_ONBOARDING_OPTIONAL_KEYS: tuple[str, ...] = (
    "why_here_other",
    "worth_paying_for_other",
    "best_at_other",
    "worst_at_other",
    "help_most_other",
)

_ONBOARDING_MAX_FIELD_LEN = 1000
_ONBOARDING_MIN_ONE_THING_LEN = 10


@app.route("/onboarding/why-here", methods=["POST"])
@login_required
@limiter.limit("10 per minute; 60 per hour")
def submit_onboarding_why_here():
    """Save the wizard's full answer set for the current user (upsert)."""
    from app.models import save_onboarding_response

    wants_json = (
        request.accept_mimetypes.best == "application/json"
        or request.headers.get("X-Requested-With", "") == "XMLHttpRequest"
    )

    answers: dict[str, object] = {}

    # Required scalar fields (radios / textareas). ``help_most`` is
    # the one multi-select; pull both bracketed and bare names so the
    # form can use either ``name="help_most"`` or ``help_most[]``.
    for key in _ONBOARDING_REQUIRED_KEYS:
        if key == "help_most":
            vals = request.form.getlist("help_most[]") or request.form.getlist("help_most")
            cleaned = [v.strip()[:_ONBOARDING_MAX_FIELD_LEN] for v in vals if v and v.strip()]
            if cleaned:
                answers[key] = cleaned
        else:
            v = (request.form.get(key) or "").strip()
            if v:
                answers[key] = v[:_ONBOARDING_MAX_FIELD_LEN]

    # Optional free-text adjuncts (the "Something else: ___" boxes).
    for key in _ONBOARDING_OPTIONAL_KEYS:
        v = (request.form.get(key) or "").strip()
        if v:
            answers[key] = v[:_ONBOARDING_MAX_FIELD_LEN]

    missing = [k for k in _ONBOARDING_REQUIRED_KEYS if not answers.get(k)]
    one_thing = answers.get("one_thing")
    if isinstance(one_thing, str) and len(one_thing.strip()) < _ONBOARDING_MIN_ONE_THING_LEN:
        missing.append("one_thing")

    if missing:
        msg = "A couple of answers are still missing — finish those and resend."
        payload = {"ok": False, "error": msg, "missing": missing}
        if wants_json:
            return payload, 400
        flash(msg, "warning")
        return redirect(request.referrer or url_for("index"))

    ok = save_onboarding_response(user_id=current_user.id, answers=answers)
    if not ok:
        msg = "We couldn't save that just now. Try again in a minute."
        if wants_json:
            return {"ok": False, "error": msg}, 500
        flash(msg, "danger")
        return redirect(request.referrer or url_for("index"))

    # Weekly summary email opt-out toggle from the onboarding wizard.
    # The checkbox ships checked (opt-out): present => opted in, absent =>
    # the user turned it off before finishing. Mirrors the Weekly summary
    # control on the profile notifications tab (digest_email).
    try:
        from app.models import update_user_profile

        update_user_profile(
            current_user.id,
            digest_email=(request.form.get("digest_email") == "on"),
        )
    except Exception as exc:  # pragma: no cover - best-effort, non-blocking
        app.logger.warning("onboarding digest_email opt-in save failed: %s", exc)

    if wants_json:
        return {"ok": True}
    flash("Thanks — saved.", "success")
    return redirect(request.referrer or url_for("index"))


@app.route("/faq")
def faq():
    """FAQ page for marketing."""
    return render_template("faq.html", title="FAQ")


@app.route("/privacy")
def privacy():
    """Plain-English privacy policy."""
    return render_template("privacy.html", title="Privacy")


@app.route("/terms")
def terms():
    """Plain-English terms of service."""
    return render_template("terms.html", title="Terms")


@app.route("/contact")
def contact():
    """Contact / support page."""
    return render_template("contact.html", title="Contact")


@app.route("/sitemap.xml")
def sitemap():
    """Simple sitemap for SEO."""
    base = request.url_root.rstrip("/")
    pages = [
        ("", "daily", "1.0"),
        ("/pricing", "monthly", "0.8"),
        ("/faq", "monthly", "0.7"),
    ]
    for slug in FEATURES:
        if slug == "ai-trading-insights" and not app.config.get("INSIGHTS_ENABLED", True):
            continue
        pages.append((f"/features/{slug}", "monthly", "0.7"))
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    for path, freq, prio in pages:
        xml += f"  <url><loc>{base}{path}</loc><changefreq>{freq}</changefreq><priority>{prio}</priority></url>\n"
    xml += "</urlset>"
    return Response(xml, mimetype="application/xml")


@app.route("/sw.js")
def service_worker():
    """Serve the service worker from the ORIGIN ROOT so its scope covers the
    whole app (a worker served under /static/ could only control /static/).
    no-cache so a deploy's new worker version is picked up on next visit."""
    resp = app.send_static_file("sw.js")
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/offline")
def offline():
    """Offline fallback for the PWA — precached by the service worker at
    install and served on navigation when the network is unreachable."""
    return render_template("offline.html")


@app.route("/robots.txt")
def robots():
    """Basic robots.txt for crawlers."""
    base = request.url_root.rstrip("/")
    return Response(
        f"User-agent: *\nAllow: /\nDisallow: /positions\nDisallow: /upload\nDisallow: /insights\nDisallow: /settings\nDisallow: /accounts\nDisallow: /symbols\nDisallow: /position/\nSitemap: {base}/sitemap.xml\n",
        mimetype="text/plain",
    )


@app.route("/")
@app.route("/index")
def index():
    """Public landing page, or redirect to weekly review (home) if logged in."""
    if current_user.is_authenticated:
        return redirect(url_for("weekly_review"))
    return render_template("landing.html", title="Home")


@app.route("/healthz")
def healthz():
    """Liveness probe — does NOT touch DB or BigQuery so it stays green even
    if Postgres is briefly unreachable. Render uses this to know the worker
    process itself is alive.

    The body also reports whether Sentry is wired (a boolean, not the DSN)
    so error-monitoring coverage is verifiable from the outside — Render's
    MCP/API can't read env vars, and a missing SENTRY_DSN otherwise fails
    silent (the init is env-gated in app/__init__.py).
    """
    import os as _os
    sentry_on = "on" if (_os.environ.get("SENTRY_DSN", "").strip()) else "off"
    return (
        f"ok sentry={sentry_on}",
        200,
        {"Content-Type": "text/plain", "Cache-Control": "no-store"},
    )


@app.route("/healthz/db")
def healthz_db():
    """Readiness probe — confirms Postgres pool can hand out a connection
    in well under gunicorn's request timeout. Returns 503 fast on failure
    rather than hanging the request."""
    from app.db import get_conn
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return ("ok", 200, {"Content-Type": "text/plain", "Cache-Control": "no-store"})
    except Exception as exc:
        app.logger.warning("healthz/db failed: %s", exc)
        return (f"db_unavailable: {exc.__class__.__name__}", 503,
                {"Content-Type": "text/plain", "Cache-Control": "no-store"})


@app.route("/get-started")
@login_required
def get_started():
    """Onboarding checklist for new users — tracks real progress."""
    tenant_ids = get_tenant_ids_for_user(current_user.id) or []
    has_uploaded = len(tenant_ids) > 0

    # Check if data is actually available in BigQuery. We swallow the
    # exception so a transient BQ outage doesn't break the onboarding
    # page (the user can still see step 1/2/3 and the "refresh to check"
    # link), but the failure is logged so the operator can spot a
    # genuinely stuck pipeline. AGENTS.md flagged the silent pass as
    # known debt — replace with a logged warning.
    has_data = False
    if has_uploaded:
        try:
            client = get_bigquery_client()
            where = _tenant_sql_filter(tenant_ids)
            check_q = f"SELECT COUNT(*) AS cnt FROM `ccwj-dbt.analytics.positions_summary` {where}"
            from app.query_cache import cached_query_df
            result = cached_query_df(client, check_q, label="get_started_has_data")
            has_data = int(result.iloc[0]["cnt"]) > 0 if not result.empty else False
        except Exception as exc:
            app.logger.warning(
                "get_started has_data check failed for user_id=%s: %s",
                current_user.id, exc,
            )

    snaptrade_enabled = False
    snaptrade_connected = False
    snaptrade_full_history_days = 1825
    snaptrade_routine_days = 60
    try:
        from app.snaptrade import (
            snaptrade_enabled as _snaptrade_enabled_fn,
            _routine_lookback_days,
            SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS,
        )
        from app.models import get_snaptrade_accounts as _get_snaptrade_accounts

        snaptrade_enabled = bool(_snaptrade_enabled_fn())
        snaptrade_full_history_days = int(SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS)
        snaptrade_routine_days = int(_routine_lookback_days())
        snaptrade_connected = bool(_get_snaptrade_accounts(current_user.id))
    except Exception as exc:
        app.logger.warning(
            "get_started snaptrade enable check failed for user_id=%s: %s",
            current_user.id, exc,
        )

    return render_template(
        "get_started.html",
        title="Get Started",
        has_uploaded=has_uploaded,
        has_data=has_data,
        snaptrade_enabled=snaptrade_enabled,
        snaptrade_connected=snaptrade_connected,
        snaptrade_full_history_days=snaptrade_full_history_days,
        snaptrade_routine_days=snaptrade_routine_days,
    )


@app.route("/ping")
@limiter.exempt
def ping():
    return "Flask app is alive"
