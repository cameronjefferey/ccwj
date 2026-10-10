import hashlib
import os
import time
from datetime import datetime

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


def _sentry_traces_sample_rate() -> float:
    # 100% tracing on every ad click adds latency and burns the Sentry quota.
    # Set SENTRY_TRACES_SAMPLE_RATE=1 to trace everything.
    raw = (os.environ.get("SENTRY_TRACES_SAMPLE_RATE") or "0.1").strip()
    try:
        rate = float(raw)
    except ValueError:
        rate = 1.0
    if rate < 0:
        return 0.0
    if rate > 1:
        return 1.0
    return rate


if _sentry_dsn:
    sentry_sdk.init(
        dsn=_sentry_dsn,
        integrations=[FlaskIntegration()],
        send_default_pii=False,
        traces_sample_rate=_sentry_traces_sample_rate(),
        before_send=_scrub_sentry_event,
    )

app = Flask(__name__)
app.config.from_object(Config)


from app.option_formatting import (
    compact_contract_label as _compact_contract_label,
    format_option_symbol as _format_option_symbol,
)

app.add_template_filter(_format_option_symbol, name="option_symbol")
app.add_template_filter(_compact_contract_label, name="compact_contract")


def _account_label_filter(account_name, tenant_id=None):
    """Render a warehouse ``account`` string as the user's nickname.

    Prefer ``{{ row.account | account_label(row.tenant_id) }}`` — several
    SnapTrade Schwab tenants share the broker name ``Schwab Account`` and
    a name-only map cannot pick the right nickname. ``tenant_id`` looks
    up ``broker_tenants.display_nickname`` (disambiguated when needed).

    Name-only calls still work when that broker name is unique for the
    user, or when the value is already a nickname / picker label.
    """
    if not account_name and not tenant_id:
        return account_name
    try:
        from app.privacy import privacy_mode_on, shown_account
        if privacy_mode_on():
            return shown_account(account_name, tenant_id)
    except Exception:
        pass
    try:
        from flask import g
        from flask_login import current_user
        if not current_user.is_authenticated:
            return account_name
        maps = getattr(g, "_account_display_maps", None)
        if maps is None:
            from app.models import get_broker_tenants_for_user
            from app.routes import (
                _disambiguated_tenant_labels,
                _unique_account_name_labels,
            )
            rows = get_broker_tenants_for_user(current_user.id) or []
            tmap = _disambiguated_tenant_labels(rows)
            umap = _unique_account_name_labels(rows, tmap)
            maps = (tmap, umap)
            g._account_display_maps = maps
        from app.routes import _resolve_account_display
        return _resolve_account_display(
            account_name, tenant_id,
            tenant_labels=maps[0], unique_name_labels=maps[1],
        )
    except Exception:
        return account_name


app.add_template_filter(_account_label_filter, name="account_label")


def _plain_label_filter(value):
    from app.grouped_trades import plain_label
    return plain_label(value)


app.add_template_filter(_plain_label_filter, name="plain_label")


def _account_mask_filter(raw):
    from app.privacy import mask_secret, privacy_mode_on
    if privacy_mode_on() and raw:
        return mask_secret(raw)
    from app.linked_accounts import format_account_mask
    return format_account_mask(raw)


app.add_template_filter(_account_mask_filter, name="account_mask")


def _privacy_balance_filter(value, digits=0):
    from app.privacy import format_privacy_balance
    return format_privacy_balance(value, digits)


def _privacy_signed_filter(value, digits=0):
    from app.privacy import format_privacy_signed
    return format_privacy_signed(value, digits)


def _privacy_hide_filter(value):
    from app.privacy import mask_secret
    return mask_secret(value)


def _privacy_account_filter(name, tenant_id=None):
    """Mask a picker nickname. Leave it unchanged when privacy mode is off.

    Header and upload pickers already show the nickname (#156). Running
    them through ``account_label`` would swap that for the disambiguated
    broker label. This filter only applies Account N.
    """
    from app.privacy import shown_account
    return shown_account(name, tenant_id)


app.add_template_filter(_privacy_balance_filter, name="privacy_balance")
app.add_template_filter(_privacy_signed_filter, name="privacy_signed")
app.add_template_filter(_privacy_hide_filter, name="privacy_hide")
app.add_template_filter(_privacy_account_filter, name="privacy_account")


def friendly_timestamp(value, tz_name=None):
    """Render a database timestamp as a short local time.

    ``2026-09-20 22:36:46.361388+00:00`` becomes
    ``Sep 20, 2026, 6:36 PM EDT`` in America/New_York, otherwise a UTC
    clock time. Unparseable values pass through so a bad cell does not
    blank the row.
    """
    from datetime import datetime, timezone

    if value is None or value == "":
        return ""
    dt = value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text
    if not isinstance(dt, datetime):
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    zone_label = "UTC"
    shown = dt.astimezone(timezone.utc)
    if tz_name:
        try:
            from zoneinfo import ZoneInfo
            shown = dt.astimezone(ZoneInfo(tz_name))
            zone_label = shown.tzname() or tz_name
        except Exception:
            shown = dt.astimezone(timezone.utc)
            zone_label = "UTC"
    hour = shown.strftime("%I").lstrip("0") or "12"
    return f"{shown.strftime('%b')} {shown.day}, {shown.year}, {hour}:{shown.strftime('%M %p')} {zone_label}"


def _viewer_profile():
    """Signed-in user's profile, once per request. None when logged out."""
    from flask import g, has_request_context

    if not has_request_context():
        return None
    cached = getattr(g, "_viewer_profile", "__unset__")
    if cached != "__unset__":
        return cached
    cached = None
    try:
        from flask_login import current_user
        if getattr(current_user, "is_authenticated", False):
            from app.models import get_user_profile
            cached = get_user_profile(current_user.id)
    except Exception:
        cached = None
    g._viewer_profile = cached
    return cached


def _friendly_time_filter(value):
    tz_name = None
    try:
        profile = _viewer_profile()
        if profile:
            tz_name = profile.get("timezone")
    except Exception:
        tz_name = None
    return friendly_timestamp(value, tz_name)


app.add_template_filter(_friendly_time_filter, name="friendly_time")


def _sync_status_label_filter(row):
    from app.linked_accounts import sync_status_label
    return sync_status_label(row)


app.add_template_filter(_sync_status_label_filter, name="sync_status_label")


def _parse_iso_date(value):
    """First 10 characters as a date, or None when it is not ISO."""
    if value is None or value == "":
        return None
    text = str(value)[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return None


def _human_date(value):
    """ISO ``2026-09-21`` → ``Sep 21, 2026``. Other strings pass through."""
    parsed = _parse_iso_date(value)
    if parsed is None:
        return "" if value is None or value == "" else str(value)
    return parsed.strftime("%b %-d, %Y")


def _compact_date(value):
    """Dense table date. Current year drops the year (``Sep 25``).

    Any other year keeps a short year (``Apr 23 '25``) so a multi-year
    book stays readable without the width of ``Sep 25, 2026``.
    """
    parsed = _parse_iso_date(value)
    if parsed is None:
        return "" if value is None or value == "" else str(value)
    if parsed.year == datetime.now().year:
        return parsed.strftime("%b %-d")
    return parsed.strftime("%b %-d '%y")


def _win_rate_label(rate, winners=None, losers=None):
    """Em dash when nothing has closed. 0/0 is not a 0% win rate."""
    if winners is not None or losers is not None:
        try:
            closed = int(winners or 0) + int(losers or 0)
        except (TypeError, ValueError):
            closed = 0
        if closed <= 0:
            return "—"
    if rate is None:
        return "—"
    try:
        number = float(rate)
    except (TypeError, ValueError):
        return "—"
    if number != number:
        return "—"
    return "{:.0%}".format(number)


app.add_template_filter(_human_date, name="human_date")
app.add_template_filter(_compact_date, name="compact_date")
app.add_template_global(_win_rate_label, name="win_rate_label")


def history_window_label(days) -> str:
    """1825 → ``5 years``. Anything else stays in days."""
    try:
        n = int(days)
    except (TypeError, ValueError):
        return ""
    if n >= 365 and n % 365 == 0:
        years = n // 365
        return "1 year" if years == 1 else f"{years} years"
    if n == 1:
        return "1 day"
    return f"{n} days"


app.add_template_filter(history_window_label, name="history_window")


from app.utils import earnings_follower_url as _earnings_follower_url

app.add_template_global(_earnings_follower_url, name="earnings_follower_url")


def _current_year() -> int:
    """Year for the footer copyright notice. Template global (not a
    context-processor key) so it's available even on the standalone
    skeleton shell template."""
    import datetime

    return datetime.datetime.now().year


app.add_template_global(_current_year, name="current_year")

from app.glossary import render_term as _render_term
from app.glossary import render_term_link as _render_term_link
from app.glossary import render_term_mark as _render_term_mark

app.add_template_global(_render_term, name="term")
app.add_template_global(_render_term_link, name="term_link")
app.add_template_global(_render_term_mark, name="term_mark")


def _shell_account_scope():
    """Account picker rows for the signed-in shell. Callers time this."""
    from flask import request as _req
    from flask_login import current_user
    from app.models import (
        get_broker_tenants_for_user as _get_broker_tenants_for_user,
        list_account_groups as _list_account_groups,
    )
    from app.account_scope import picker_nickname_choices
    from app.privacy import sort_masked_account_choices
    from app.routes import (
        _account_rename_urls_for_rows,
        _blank_query_text,
        _groups_query_value,
        _picker_tenant_ids,
        _requested_account,
        _requested_group_ids,
        _scope_filter_options,
        _scope_query_string,
        _tenant_label_map_for_user,
    )

    account_groups = _list_account_groups(current_user.id) or []
    selected_group_ids = _requested_group_ids()
    groups_query = _groups_query_value()
    owned_rows = _get_broker_tenants_for_user(current_user.id) or []
    label_map = _tenant_label_map_for_user(current_user.id) or {}
    account_rename_urls = _account_rename_urls_for_rows(owned_rows)
    # Header picker: nicknames only. Masks and "Schwab Account"
    # stay out of this menu. Table cells still use account_label.
    scope_account_choices = sort_masked_account_choices(
        picker_nickname_choices(owned_rows)
    )
    try:
        args = _req.args
    except Exception:
        args = {}
    selected_tenant_ids = _picker_tenant_ids(args, owned_rows, label_map)
    tenants_query = ",".join(selected_tenant_ids) if selected_tenant_ids else None
    scope_query_string = _scope_query_string(args)
    visible_account_groups, visible_account_choices = _scope_filter_options(
        account_groups, selected_group_ids, selected_tenant_ids,
        scope_account_choices,
    )
    visible_account_choices = sort_masked_account_choices(visible_account_choices)
    scope_is_filtered = bool(
        selected_group_ids or selected_tenant_ids
        or _requested_account(args)
        or _blank_query_text(args.get("tenant"))
    )
    return (
        account_groups, selected_group_ids, groups_query,
        visible_account_groups, visible_account_choices,
        scope_account_choices, selected_tenant_ids, tenants_query,
        scope_query_string, scope_is_filtered, account_rename_urls,
    )


@app.context_processor
def _inject_feature_flags():
    from flask import current_app, g
    from flask_login import current_user
    from app.models import is_admin
    from app.utils import is_demo_user

    # Instant shell: skip Postgres (groups, plan, history-since, tenant
    # picker). The skeleton template is a standalone document and does not
    # read these keys; returning empties keeps any stray base.html include safe.
    if getattr(g, "_ht_skeleton", False):
        return {
            "insights_enabled": current_app.config.get("INSIGHTS_ENABLED", True),
            "earnings_follower_enabled": current_app.config.get("EARNINGS_FOLLOWER_ENABLED", True),
            "earnings_follower_base_url": current_app.config.get("EARNINGS_FOLLOWER_URL", ""),
            "is_admin_user": False,
            "is_demo_user": False,
            "signup_enabled": current_app.config.get("SIGNUP_ENABLED", True),
            "signup_invite_required": bool(current_app.config.get("SIGNUP_INVITE_CODE", "")),
            "plan_status": None,
            "history_since": None,
            "account_groups": [],
            "selected_group_ids": [],
            "groups_query": None,
            "visible_account_groups": [],
            "visible_account_choices": [],
            "scope_account_choices": [],
            "selected_tenant_ids": [],
            "tenants_query": None,
            "scope_query_string": "",
            "scope_is_filtered": False,
            "account_rename_urls": {},
            "billing_enabled": False,
            "price_monthly": None,
            "price_annual": None,
            "price_annual_equiv": None,
            "ai_billing_enabled": False,
            "price_ai": None,
            "compact_tables": False,
            "privacy_mode": False,
            "simple_view": False,
            "has_paper_account": False,
            "has_real_brokerage": False,
        }

    is_admin_user = False
    try:
        if current_user.is_authenticated:
            is_admin_user = is_admin(current_user.username)
    except Exception:
        is_admin_user = False

    # Reverse-trial banner data (app/plan.py). One users-row read per request
    # for authenticated users, cached on flask.g for that request only.
    # Plan, Simple/Full, and Stripe columns are not kept in the process
    # shell cache — a webhook or view write on another worker has to be
    # visible on the next page. None for beta/active/no-data so beta users
    # and subscribers pay nothing visually.
    plan_status = None
    try:
        if current_user.is_authenticated:
            from flask import g
            from app.request_timing import stage
            with stage("plan"):
                plan_status = getattr(g, "_plan_status", "__unset__")
                if plan_status == "__unset__":
                    from app.plan import plan_status_for_banner
                    plan_status = plan_status_for_banner(current_user.id)
                    g._plan_status = plan_status
    except Exception:
        plan_status = None

    # Date the scoped brokerage account was connected — used by the
    # dismissible history-window note on all-time surfaces.
    history_since = None
    try:
        if current_user.is_authenticated:
            from flask import g
            from app.request_timing import stage
            with stage("history"):
                history_since = getattr(g, "_history_since", "__unset__")
                if history_since == "__unset__":
                    import sys
                    from werkzeug.exceptions import HTTPException
                    # The 404 error page renders inside the view's abort.
                    # Calling scope again would abort a second time and
                    # turn that 404 into a 500. A normal render still
                    # denies a private ?tenant= here.
                    if isinstance(sys.exc_info()[1], HTTPException):
                        history_since = None
                    else:
                        from app.accounts_page import _account_created_for_scope
                        from app.routes import _tenants_for_scope
                        history_since = _account_created_for_scope(_tenants_for_scope())
                        g._history_since = history_since
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            raise
        history_since = None

    account_groups = []
    selected_group_ids = []
    groups_query = None
    visible_account_groups = []
    visible_account_choices = []
    scope_account_choices = []
    selected_tenant_ids = []
    tenants_query = None
    scope_query_string = ""
    scope_is_filtered = False
    account_rename_urls = {}
    try:
        if current_user.is_authenticated:
            from app.request_timing import stage
            with stage("groups"):
                (
                    account_groups, selected_group_ids, groups_query,
                    visible_account_groups, visible_account_choices,
                    scope_account_choices, selected_tenant_ids, tenants_query,
                    scope_query_string, scope_is_filtered, account_rename_urls,
                ) = _shell_account_scope()
    except Exception:
        account_groups = []
        selected_group_ids = []
        groups_query = None
        visible_account_groups = []
        visible_account_choices = []
        scope_account_choices = []
        selected_tenant_ids = []
        tenants_query = None
        scope_query_string = ""
        scope_is_filtered = False
        account_rename_urls = {}

    # Stripe is only offered when fully configured (keys + both price IDs);
    # checkout routes refuse otherwise so a half-configured deploy cannot
    # charge. The pricing page still shows live signup copy either way.
    try:
        from app.billing import (
            PRICE_AI_MONTHLY_DISPLAY,
            PRICE_ANNUAL_DISPLAY,
            PRICE_ANNUAL_MONTHLY_EQUIV,
            PRICE_MONTHLY_DISPLAY,
            ai_addon_enabled,
            stripe_enabled,
        )
        billing_enabled = stripe_enabled()
        price_monthly = PRICE_MONTHLY_DISPLAY
        price_annual = PRICE_ANNUAL_DISPLAY
        price_annual_equiv = PRICE_ANNUAL_MONTHLY_EQUIV
        ai_billing_enabled = ai_addon_enabled()
        price_ai = PRICE_AI_MONTHLY_DISPLAY
    except Exception:
        billing_enabled = False
        price_monthly = price_annual = price_annual_equiv = None
        ai_billing_enabled = False
        price_ai = None

    compact_tables = False
    privacy_mode = False
    simple_view = False
    has_paper_account = False
    # Failed account lookup keeps the full-app nav (Practice in Account).
    has_real_brokerage = True
    full_view_offer = False
    try:
        if current_user.is_authenticated:
            _prof = _viewer_profile() or {}
            compact_tables = bool(_prof.get("compact_tables"))
            from app.privacy import privacy_mode_on
            privacy_mode = privacy_mode_on()
            from app.paper_accounts import full_view_offer_open, viewer_flags
            from app.plan import user_has_real_brokerage
            from app.request_timing import stage
            with stage("view"):
                simple_view, has_paper_account = viewer_flags(current_user.id)
                full_view_offer = full_view_offer_open(current_user.id)
                looked_up = user_has_real_brokerage(current_user.id)
                has_real_brokerage = looked_up is not False
        else:
            has_real_brokerage = False
    except Exception:
        compact_tables = False
        privacy_mode = False
        simple_view = False
        has_paper_account = False
        has_real_brokerage = True
        full_view_offer = False

    return {
        "insights_enabled": current_app.config.get("INSIGHTS_ENABLED", True),
        "earnings_follower_enabled": current_app.config.get("EARNINGS_FOLLOWER_ENABLED", True),
        "earnings_follower_base_url": current_app.config.get("EARNINGS_FOLLOWER_URL", ""),
        "is_admin_user": is_admin_user,
        "is_demo_user": is_demo_user(),
        "signup_enabled": current_app.config.get("SIGNUP_ENABLED", True),
        "signup_invite_required": bool(current_app.config.get("SIGNUP_INVITE_CODE", "")),
        "plan_status": plan_status,
        "history_since": history_since,
        "account_groups": account_groups,
        "selected_group_ids": selected_group_ids,
        "groups_query": groups_query,
        "visible_account_groups": visible_account_groups,
        "visible_account_choices": visible_account_choices,
        "scope_account_choices": scope_account_choices,
        "selected_tenant_ids": selected_tenant_ids,
        "tenants_query": tenants_query,
        "scope_query_string": scope_query_string,
        "scope_is_filtered": scope_is_filtered,
        "account_rename_urls": account_rename_urls,
        "billing_enabled": billing_enabled,
        "price_monthly": price_monthly,
        "price_annual": price_annual,
        "price_annual_equiv": price_annual_equiv,
        "ai_billing_enabled": ai_billing_enabled,
        "price_ai": price_ai,
        "compact_tables": compact_tables,
        "privacy_mode": privacy_mode,
        "simple_view": simple_view,
        "has_paper_account": has_paper_account,
        "has_real_brokerage": has_real_brokerage,
        "full_view_offer": full_view_offer,
    }


@app.context_processor
def _inject_reddit_pixel():
    """PageVisit on /start, SignUp on the next page after signup. No-op
    when REDDIT_PIXEL_ID is unset."""
    try:
        from app.campaign import reddit_pixel_context
        return reddit_pixel_context()
    except Exception:
        return {"reddit_pixel_id": "", "reddit_pixel_event": ""}


# Behind Render / other reverse proxies: trust X-Forwarded-* so request.host /
# request.scheme / url_for(..., _external=True) match the public URL.
# PROXY_X_FOR is the hop count (default 1, Render's single proxy). Once the
# site is proxied by Cloudflare, rate limits use CF-Connecting-IP instead.
from app.client_ip import proxy_x_for_hops
_proxy_hops = proxy_x_for_hops()
app.wsgi_app = ProxyFix(
    app.wsgi_app, x_for=_proxy_hops, x_proto=1, x_host=1, x_prefix=1
)


@app.before_request
def _redirect_onrender_host():
    """Close the *.onrender.com origin. Health checks stay on this host."""
    from app.security_headers import redirect_onrender_origin
    return redirect_onrender_origin()


@app.errorhandler(404)
def not_found(e):
    return render_template("404.html", title="Page not found"), 404


@app.errorhandler(429)
def too_many_requests(e):
    """flask-limiter raises RateLimitExceeded → 429."""
    from flask import jsonify

    description = getattr(e, "description", "Too many requests, slow down.")

    wants_json = (
        request.path.startswith("/api/")
        or request.path == "/insights/ask"
        or request.accept_mimetypes.best == "application/json"
        or (request.headers.get("X-Requested-With", "") == "XMLHttpRequest")
    )
    if wants_json:
        return (
            jsonify({
                "error": "rate_limited",
                "message": str(description),
            }),
            429,
        )

    if request.path == "/demo/start":
        from app.demo_guard import IP_SESSION_CAP, limited_page

        return limited_page(
            f"This network has started {IP_SESSION_CAP} demos today. "
            "Create an account to keep going on your own data."
        )

    try:
        return render_template("429.html", title="Slow down"), 429
    except Exception:
        return (
            "You're going a little fast for our beta — give it a minute and try again.",
            429,
        )


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

@app.before_request
def _begin_request_timing():
    """Start the timing slot before Flask-Login loads the user.

    ``load_user`` records the user stage into this slot. Resetting again
    in a later hook would drop it.
    """
    from app.request_timing import begin_request_timing
    begin_request_timing()


# Flask-Login setup
login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.login_message = 'Please log in to access this page.'
login_manager.login_message_category = 'info'
login_manager.init_app(app)


@login_manager.user_loader
def load_user(user_id):
    from app.db import bind_request_thread
    from app.request_timing import stage

    bind_request_thread()
    with stage("user"):
        return _load_user(user_id)


def _load_user(user_id):
    from app.demo_guard import (
        DemoSessionUser,
        is_ephemeral_demo_id,
        numeric_user_id,
        token_from_id,
    )
    if is_ephemeral_demo_id(user_id):
        token = token_from_id(user_id)
        if not token:
            return None
        return DemoSessionUser(token)
    from app.models import User
    uid = numeric_user_id(user_id)
    if uid is None:
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
    if (
        request.path.startswith("/healthz")
        or request.path == "/version"
        or request.path.startswith("/static/")
    ):
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
        # Real accounts keep a persistent cookie (PERMANENT_SESSION_LIFETIME).
        # Demo sessions do not: a 7-day cookie re-issued on every request is
        # what let a crawler keep the shared demo login.
        from app.demo_guard import is_ephemeral_demo_user
        username = getattr(current_user, "username", "") or ""
        try:
            username = username.lower()
        except Exception:
            username = ""
        demo_session = is_ephemeral_demo_user(current_user) or username == "demo"
        session.permanent = not demo_session
        session[_SESSION_LAST_KEY] = time.time()
        session.modified = True


@app.before_request
def _before_request_sentry_user():
    from flask import g
    from app.db import bind_request_thread

    g._req_start = time.perf_counter()
    bind_request_thread()
    try:
        from app import query_cache
        query_cache.start_request_stats()
    except Exception:
        pass
    if _sentry_dsn:
        _set_sentry_user()
    idle = _check_session_idle()
    if idle is not None:
        return idle
    from app.demo_guard import guard_demo_request
    blocked = guard_demo_request()
    if blocked is not None:
        return blocked


@app.after_request
def _after_request_touch_session_activity(response):
    _touch_session_last_activity()
    return response


@app.after_request
def _after_request_usage_event(response):
    try:
        from app.admin_overview import record_page_view
        record_page_view(response)
    except Exception:
        pass
    try:
        from app.funnel import observe_response
        observe_response(response)
    except Exception:
        pass
    return response


import logging as _logging
import sys as _sys

# Dedicated stdout logger for REQUEST_TIMING. Flask's app.logger defaults to
# WARNING under gunicorn (no INFO handler), so app.logger.info(...) lines never
# reach Render's log stream. A small dedicated logger with its own stdout
# handler and propagate=False guarantees the timing lines show up without
# changing the level/handlers of every other app.logger call.
_timing_logger = _logging.getLogger("happytrader.timing")
if not _timing_logger.handlers:
    _th = _logging.StreamHandler(_sys.stdout)
    _th.setFormatter(_logging.Formatter("%(message)s"))
    _timing_logger.addHandler(_th)
    _timing_logger.setLevel(_logging.INFO)
    _timing_logger.propagate = False


@app.after_request
def _after_request_timing(response):
    """Log a one-line REQUEST_TIMING per page and expose Server-Timing.

    ``total_ms`` is the whole request. ``bq_ms`` is the summed wall-clock of
    the cold (cache-miss) BigQuery executions across ALL threads (query
    counters are thread-aware via a ContextVar, so ``_bq_parallel`` fan-out
    is counted); ``slow=<label>:<ms>`` names the single slowest cold query;
    ``steps=chart:..,matrix:..`` breaks out the heavy Python builders. A
    cold page shows ``qmiss`` high and ``total_ms ≈ bq_ms + steps``; a warm
    reload shows ``qhit`` high and a much smaller ``total_ms``. The
    ``Server-Timing`` header surfaces total_ms in the browser devtools
    Network tab (Timing) with no extra tooling.
    """
    try:
        from flask import g
        from app import query_cache
        # Skip static assets / health probes — pure noise.
        path = request.path or ""
        if path.startswith("/static/") or path.startswith("/healthz") or path == "/version":
            return response
        start = getattr(g, "_req_start", None)
        if start is None:
            return response
        total_ms = (time.perf_counter() - start) * 1000.0
        stats = query_cache.get_request_stats()
        from app.db import request_connect_count
        from app.request_timing import format_hooks, hook_ms, outbound_ms_and_count

        out_ms, out_n = outbound_ms_and_count()
        response.headers["Server-Timing"] = (
            f"total;dur={total_ms:.0f}, out;dur={out_ms:.0f}"
        )
        had_work = stats is not None and bool(
            stats.query_hits or stats.query_miss
            or stats.payload_hits or stats.payload_miss
        )
        try:
            signed_in = bool(getattr(current_user, "is_authenticated", False))
        except Exception:
            signed_in = False
        # Signed-in pages always log. The shell (plan, tenants, freshness)
        # was a multi-second tax with no BigQuery work, so a "slow or BQ"
        # filter hid it.
        if had_work or total_ms > 500 or signed_in or out_n:
            _timing_logger.info(
                "REQUEST_TIMING path=%s status=%s total_ms=%.0f "
                "out_ms=%.0f out_n=%s db_conn=%s %s %s",
                path, response.status_code, total_ms,
                out_ms, out_n, request_connect_count(),
                format_hooks(hook_ms()),
                query_cache.format_stats(stats),
            )
    except Exception:
        pass
    return response


from app.extensions import csrf, limiter

csrf.init_app(app)


@app.before_request
def _sync_rate_limit_enabled():
    """Honor ``RATELIMIT_ENABLED`` on each request.

    ``Limiter.init_app`` copies the flag once. Tests (and a misconfigured
    deploy that sets the flag after startup) flip ``app.config`` later.
    The copy would keep limiting anyway.
    """
    limiter.enabled = bool(app.config.get("RATELIMIT_ENABLED", True))


limiter.init_app(app)


def _demo_heavy_key():
    try:
        return f"demo-heavy:{current_user.get_id()}"
    except Exception:
        return "demo-heavy:anon"


from app.demo_guard import demo_heavy_exempt


@app.before_request
@limiter.limit(
    "20 per minute;150 per day",
    exempt_when=demo_heavy_exempt,
    key_func=_demo_heavy_key,
)
def _limit_demo_heavy_pages():
    """Per demo session, on the pages a crawler walks."""
    return None


@app.after_request
def _security_headers(response):
    from app.security_headers import apply_security_headers
    return apply_security_headers(response)


@app.teardown_request
def _close_request_db(_exc):
    from app.db import close_request_connection
    close_request_connection()

# Initialize the database and seed users from env.
#
# HAPPYTRADER_SKIP_DB_INIT=1 lets unit-test runs that import `app.*` skip the
# Postgres bootstrap. Production never sets it; Render always has DATABASE_URL.
from app.models import init_db, seed_users_from_env, ensure_demo_user
if os.environ.get("HAPPYTRADER_SKIP_DB_INIT") != "1":
    init_db()
    seed_users_from_env()
    ensure_demo_user()

from app import routes
from app import marketing  # noqa: F401  registers marketing/static/health routes
from app import go_landings  # noqa: F401  registers /go/<slug> ad landings
from app import learn  # noqa: F401  registers /learn
from app import position_detail  # noqa: F401  registers /position/<symbol> + tag routes
from app import positions_page  # noqa: F401  registers /positions (also imported by routes facade)
from app import symbols_page  # noqa: F401  registers /symbols (also imported by routes facade)
from app import sectors_page  # noqa: F401  registers /sectors + /industries
from app import strategy_fit  # noqa: F401  registers /strategy-fit
from app import accounts_page  # noqa: F401  registers /accounts + /accounts/breakdown
from app import earnings_page  # noqa: F401  registers /earnings
from app import trader_story  # noqa: F401  registers /story (the trader novel)
from app import auth
from app import upload
from app import insights
from app import strategy_fit_insights  # noqa: F401  registers /strategy-fit/insights/* routes
from app import weekly_review
from app import wealth  # noqa: F401  registers /wealth route
from app import admin  # noqa: F401  registers /admin/* routes
from app import snaptrade  # noqa: F401  registers /snaptrade/* routes
from app import paper_practice  # noqa: F401  registers /practice (Alpaca Paper ticket)
from app import first_look
from app import strategies
from app import profile_page  # noqa: F401  registers /profile (settings hub)
from app import webhooks  # noqa: F401  registers /webhooks/* routes
from app import billing  # noqa: F401  registers /billing/* + /webhooks/stripe
from app import cache_ops  # noqa: F401  registers /internal/cache/flush (rebuild-triggered flush + warm)
from app.privacy import register_privacy_routes
register_privacy_routes(app)
from app.funnel import register as register_funnel
register_funnel(app)
from app import share_card  # noqa: F401  registers /share/card.png

# After the session-idle before_request so a timed-out session is logged
# out before we redirect into a saved account scope.
from app.account_scope import register_account_scope
register_account_scope(app)
