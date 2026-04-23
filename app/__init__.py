import hashlib
import os

import sentry_sdk
from flask import Flask, render_template
from flask_login import LoginManager
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

# Behind Render / other reverse proxies: trust X-Forwarded-* so request.host /
# request.scheme / url_for(..., _external=True) match the public URL.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)


@app.errorhandler(404)
def not_found(e):
    return render_template("404.html", title="Page not found"), 404

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
    from flask_login import current_user
    if current_user.is_authenticated:
        anon = hashlib.sha256(str(current_user.id).encode()).hexdigest()[:16]
        sentry_sdk.set_user({"id": anon})


@app.before_request
def _before_request_sentry_user():
    if _sentry_dsn:
        _set_sentry_user()


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
from app import weekly_review
from app import schwab
from app import first_look
from app import strategies
from app import profile_community
