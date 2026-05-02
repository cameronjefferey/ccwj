import hashlib
import os
import time

import sentry_sdk
from flask import Flask, render_template, request, session, redirect, url_for, flash, jsonify
from flask_login import LoginManager, current_user, logout_user
from werkzeug.middleware.proxy_fix import ProxyFix
from config import Config
from sentry_sdk.integrations.flask import FlaskIntegration

# Set SENTRY_DSN in the environment to enable (no default — avoids sending
# production errors to a shared project by mistake).
_sentry_dsn = os.environ.get("SENTRY_DSN", "").strip() or None


def _scrub_sentry_event(event, hint):
    """Remove sensitive finance/trading data from Sentry events."""
    req = event.get("request", {})
    # Remove request body (form data, JSON) and cookies
    req.pop("data", None)
    req.pop("query_string", None)
    req.pop("cookies", None)
    # Scrub sensitive headers
    headers = req.get("headers") or {}
    if isinstance(headers, dict):
        headers = dict(headers)
        for k in list(headers.keys()):
            if k.lower() in ("authorization", "cookie", "x-api-key"):
                headers[k] = "[Filtered]"
        req["headers"] = headers
    event["request"] = req
    # Scrub breadcrumbs that might contain sensitive data
    for crumb in event.get("breadcrumbs", []) or []:
        if isinstance(crumb.get("data"), dict):
            for key in ("password", "token", "account", "account_number", "thesis", "notes", "reflection"):
                if key in crumb["data"]:
                    crumb["data"][key] = "[Filtered]"
    return event


if _sentry_dsn:
    sentry_sdk.init(
        dsn=_sentry_dsn,
        integrations=[FlaskIntegration()],
        send_default_pii=False,
        traces_sample_rate=1.0,
        before_send=_scrub_sentry_event,
    )

app = Flask(__name__)
app.config.from_object(Config)


from app.option_formatting import format_option_symbol as _format_option_symbol

app.add_template_filter(_format_option_symbol, name="option_symbol")


@app.context_processor
def _inject_feature_flags():
    from flask import current_app
    from flask_login import current_user
    from app.models import is_admin

    is_admin_user = False
    try:
        if current_user.is_authenticated:
            is_admin_user = is_admin(current_user.username)
    except Exception:
        is_admin_user = False

    return {
        "insights_enabled": current_app.config.get("INSIGHTS_ENABLED", True),
        "is_admin_user": is_admin_user,
    }


# Behind Render / other reverse proxies: trust X-Forwarded-* so request.host /
# request.scheme / url_for(..., _external=True) match the public URL.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)


@app.errorhandler(404)
def not_found(e):
    return render_template("404.html", title="Page not found"), 404


@app.errorhandler(500)
def internal_error(e):
    """Make sure 500s are *logged* with a full traceback. Flask's default
    handler logs the message but not always the traceback when something
    re-raises in middleware or before_request hooks. We always log + render
    a small friendly page (or fall back to plain text if even that fails)."""
    import traceback
    tb = traceback.format_exc()
    app.logger.error("500 on %s %s\n%s", request.method, request.path, tb)
    try:
        return render_template("500.html", title="Something went wrong"), 500
    except Exception:
        return ("Something went wrong on our end. The team has been notified. "
                "Try refreshing in a minute."), 500

# Flask-Login setup
login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.login_message = 'Please log in to access this page.'
login_manager.login_message_category = 'info'
login_manager.init_app(app)


@login_manager.user_loader
def load_user(user_id):
    from app.models import User
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return None
    try:
        return User.get_by_id(uid)
    except Exception as e:
        # After idle, DB can hiccup once; db layer retries, but a hard failure
        # should not 500 every page—treat as logged out so the user can refresh / log in.
        if app.debug:
            raise
        app.logger.warning("load_user failed for id=%s: %s", uid, e)
        return None


def _set_sentry_user():
    """Identify user in Sentry by hashed ID only (no PII)."""
    if current_user.is_authenticated:
        anon = hashlib.sha256(str(current_user.id).encode()).hexdigest()[:16]
        sentry_sdk.set_user({"id": anon})


@login_manager.unauthorized_handler
def _login_required_redirect():
    """Flask-Login: prefer JSON 401 for API when client expects JSON."""
    if request.path.startswith("/api/") or request.accept_mimetypes.best == "application/json":
        return jsonify({"error": "login_required"}), 401
    # Path + query only (not request.url) so ?next= stays a relative path in the
    # login form and is not a full https://... string that breaks unencoded form actions.
    nxt = request.full_path
    if not nxt.startswith("/"):
        nxt = request.path
    return redirect(url_for("login", next=nxt))


_SESSION_LAST_KEY = "_last_activity_ts"


def _check_session_idle():
    minutes = int(app.config.get("SESSION_IDLE_TIMEOUT_MINUTES", 0) or 0)
    if minutes <= 0:
        return None
    # Healthcheck endpoints must NEVER hit the DB (otherwise Render's probe
    # will fail during a pool stall and mark the pod unhealthy at the worst
    # possible moment). current_user.is_authenticated triggers the user
    # loader, which queries Postgres — short-circuit before that.
    if request.path.startswith("/healthz") or request.path.startswith("/static/"):
        return None
    if not current_user.is_authenticated:
        return None
    now = time.time()
    last = session.get(_SESSION_LAST_KEY)
    limit = minutes * 60.0
    if last is not None and (now - last) > limit:
        logout_user()
        session.clear()
        if request.path.startswith("/api/"):
            return jsonify({"error": "session_expired", "message": "Session timed out from inactivity."}), 401
        flash("You were logged out after a period of inactivity. Please sign in again.", "info")
        nxt = request.full_path
        if not nxt.startswith("/"):
            nxt = request.path
        return redirect(url_for("login", next=nxt))
    return None


def _touch_session_last_activity():
    if current_user.is_authenticated and not request.path.startswith("/static/"):
        session[_SESSION_LAST_KEY] = time.time()
        session.modified = True


@app.before_request
def _before_request_sentry_user():
    if _sentry_dsn:
        _set_sentry_user()
    idle = _check_session_idle()
    if idle is not None:
        return idle


@app.after_request
def _after_request_touch_session_activity(response):
    _touch_session_last_activity()
    return response


from app.extensions import csrf, limiter

csrf.init_app(app)
limiter.init_app(app)

# Initialize the database and seed users from env
from app.models import init_db, seed_users_from_env, ensure_demo_user
init_db()
seed_users_from_env()
ensure_demo_user()

from app import routes
from app import auth
from app import upload
from app import insights
from app import strategy_fit_insights  # noqa: F401  registers /strategy-fit/insights/* routes
from app import weekly_review
from app import admin  # noqa: F401  registers /admin/* routes
from app import schwab
from app import first_look
from app import strategies
from app import profile_community
