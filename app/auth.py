import hmac
import os
import re
import secrets
import click
from flask import render_template, redirect, url_for, request, flash, abort, jsonify, session
from flask_login import login_required, login_user, logout_user, current_user
from app import app
from app.email import send_password_reset_email, send_welcome_verify_email
from app.extensions import csrf, limiter
from app.models import (
    PASSWORD_RESET_TOKEN_TTL,
    User,
    consume_email_verification_token,
    consume_password_reset_token,
    email_needs_verification,
    get_accounts_for_user,
    get_user_profile,
    login_lockout_remaining_seconds,
    mint_email_verification_token,
    mint_password_reset_token,
    peek_password_reset_token,
    record_login_attempt,
    unsubscribe_user_by_token,
)
from app.client_ip import real_client_ip
from app.demo_guard import (
    DemoSessionUser,
    IP_SESSION_CAP,
    attach_demo_visitor_cookie,
    demo_start_reuses_visitor,
    is_ephemeral_demo_user,
    limited_page,
    remember_demo_visitor,
    reserve_demo_ip,
    reusable_demo_visitor,
    turnstile_keys,
    verify_turnstile,
)
from app.utils import demo_block_writes, is_demo_user, safe_internal_next


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _validate_email(raw):
    """Lightweight format check. Returns (cleaned_email_or_None, error_or_None).
    Empty input returns (None, None) so the caller can decide if email is
    required for that flow."""
    email = (raw or "").strip().lower()
    if not email:
        return None, None
    if len(email) > 320 or not _EMAIL_RE.match(email):
        return None, "That email address doesn't look right."
    return email, None

# Profile default "home" after login. Keys must match profile_page _ALLOWED_DEFAULT_ROUTE.
# "symbols" is a legacy stored value from the retired Daily P&L page — it maps
# to /positions now (profile_page coerces it on read too).
_LANDING = {
    "weekly_review": "weekly_review",
    "positions": "positions",
    "strategies": "strategies",
    "insights": "insights",
    "accounts": "accounts",
    "symbols": "positions",
}


def _user_id_is_demo(user_id) -> bool:
    """True when this id is the shared Postgres ``demo`` row."""
    if user_id is None:
        return False
    row = User.get_by_id(user_id)
    return row is not None and (row.username or "").lower() == "demo"


def _lookup_login_user(identifier):
    """Return a user for either username or email, preferring exact username."""
    ident = (identifier or "").strip()
    if not ident:
        return None
    user = User.get_by_username(ident)
    if user is not None:
        return user
    if "@" not in ident:
        return None
    email, err = _validate_email(ident)
    if err or not email:
        return None
    return User.get_by_email(email)


def _login_attempt_keys(identifier, user=None):
    """Keys that represent the same login target for lockout accounting."""
    keys = []
    for key in (
        identifier,
        getattr(user, "username", None),
        getattr(user, "email", None),
    ):
        clean = (key or "").strip().lower()
        if clean and clean not in keys:
            keys.append(clean)
    return keys


def _login_lockout_remaining(keys):
    return max((login_lockout_remaining_seconds(key) for key in keys), default=0)


def _record_login_attempt_for_keys(keys, *, success, ip_address, user_agent):
    for key in keys:
        record_login_attempt(
            key,
            success=success,
            ip_address=ip_address,
            user_agent=user_agent,
        )


def _landing_endpoint(prof) -> str:
    dr = ((prof or {}).get("default_route") or "weekly_review").strip()
    if dr == "insights" and not app.config.get("INSIGHTS_ENABLED", True):
        dr = "weekly_review"
    return _LANDING.get(dr, "weekly_review")


def _release_shared_demo_session() -> bool:
    """Log out the shared demo account so an auth page can render.

    ``/demo/start`` used to sign the visitor in as the shared ``demo`` user.
    ``/login`` and ``/signup`` treat every authenticated session as
    already inside the app and redirect to Overview. That trapped the
    demo banner's "Create your own account" button — and a typed
    ``/login`` or ``/signup`` — back on Overview. A real account still
    bounces. Returns True when a demo session was cleared.
    """
    if not is_demo_user():
        return False
    logout_user()
    return True


@app.route("/login", methods=["GET", "POST"])
@limiter.limit("20 per minute")
def login():
    if current_user.is_authenticated and not _release_shared_demo_session():
        dest = safe_internal_next(request.args.get("next"))
        if dest:
            return redirect(dest)
        return redirect(url_for("weekly_review"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        remember = request.form.get("remember") == "on"
        user = _lookup_login_user(username)
        attempt_keys = _login_attempt_keys(username, user)

        # Per-account lockout: stop credential-stuffing that cycles IPs.
        # The IP-keyed flask-limiter cap above blocks one host hammering
        # us; this catches a botnet probing a single account from many
        # hosts. Email login aliases share the canonical user's bucket so
        # switching between username and email cannot bypass cooldown.
        remaining = _login_lockout_remaining(attempt_keys)
        if remaining > 0:
            mins = max(1, (remaining + 59) // 60)
            # Don't disclose whether the username exists. Same message
            # whether it's a real user mid-attack or a typo'd handle.
            flash(
                f"Too many failed attempts for that account. Try again in "
                f"about {mins} minute{'s' if mins != 1 else ''}, or use "
                f"'Forgot password' to reset.",
                "danger",
            )
            return redirect(url_for("login"))

        # The shared ``demo`` username is not a personal login. Reject it
        # before the password is checked so a known password (demo123)
        # gets the same generic error as a wrong one.
        demo_username = (username or "").strip().lower() == "demo" or (
            user is not None and (getattr(user, "username", "") or "").lower() == "demo"
        )
        if demo_username or user is None or not user.check_password(password):
            _record_login_attempt_for_keys(
                attempt_keys,
                success=False,
                ip_address=request.remote_addr,
                user_agent=request.headers.get("User-Agent"),
            )
            # Re-check after recording: this attempt may have just tipped
            # the username into lockout. Surface the imminent-lockout
            # message at the right moment.
            after_remaining = _login_lockout_remaining(attempt_keys)
            if after_remaining > 0:
                mins = max(1, (after_remaining + 59) // 60)
                flash(
                    f"Too many failed attempts for that account. Try again "
                    f"in about {mins} minute{'s' if mins != 1 else ''}, or "
                    f"use 'Forgot password' to reset.",
                    "danger",
                )
            else:
                flash("Invalid username/email or password.", "danger")
            nxt = safe_internal_next(
                request.form.get("next") or request.args.get("next")
            )
            if nxt:
                return redirect(url_for("login", next=nxt))
            return redirect(url_for("login"))

        _record_login_attempt_for_keys(
            attempt_keys,
            success=True,
            ip_address=request.remote_addr,
            user_agent=request.headers.get("User-Agent"),
        )
        login_user(user, remember=remember)

        # Prefer hidden form field (reliable on POST); fall back to query string.
        next_page = safe_internal_next(
            request.form.get("next") or request.args.get("next")
        )
        if not next_page:
            accounts = get_accounts_for_user(user.id)
            if not accounts:
                next_page = url_for("get_started")
            else:
                prof = get_user_profile(user.id) or {}
                next_page = url_for(_landing_endpoint(prof))
        return redirect(next_page)

    next_for_form = safe_internal_next(request.args.get("next"))
    return render_template("login.html", title="Login", next_for_form=next_for_form)


@app.route("/register")
def register_redirect():
    """Common typo/bookmark — redirect to /signup."""
    return redirect(url_for("signup"))


_SIGNUP_ROUTES = {
    "learn": "Lessons, replays, and paper trading are free.",
    "real-pnl": "See your real P&L across every broker you connect. Read-only.",
    "mistakes": "See what an early close cost, on the trades you already made.",
}


def _signup_route(raw) -> str:
    """Ad landings preselect a path. Anything else is blank."""
    text = (raw or "").strip()
    return text if text in _SIGNUP_ROUTES else ""


def _signup_subhead(route: str) -> str:
    return _SIGNUP_ROUTES.get(route) or (
        "Learning and paper trading are free. "
        "Your 30-day trial starts when you connect a real brokerage."
    )


def _render_signup_form(
    invite_required,
    *,
    username="",
    email="",
    invite="",
    route="",
):
    """Re-render signup without reflecting submitted credentials.

    Username, email, and an invite code the visitor just typed come back
    so a mismatched password does not wipe the form. Passwords never do.
    """
    chosen = _signup_route(route)
    site_key, _secret = turnstile_keys()
    return render_template(
        "signup.html",
        title="Sign Up",
        invite_required=invite_required,
        form_username=username,
        form_email=email,
        form_invite=invite,
        form_route=chosen,
        signup_subhead=_signup_subhead(chosen),
        turnstile_site_key=site_key,
    )


@app.route("/signup", methods=["GET", "POST"])
@limiter.limit("10 per minute; 120 per hour", methods=["POST"])
def signup():
    if not app.config.get("SIGNUP_ENABLED", True):
        abort(404)

    if current_user.is_authenticated and not _release_shared_demo_session():
        return redirect(url_for("weekly_review"))

    invite_required = bool(app.config.get("SIGNUP_INVITE_CODE", ""))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        invite = (request.form.get("invite_code", "") or "").strip()
        email_raw = request.form.get("email", "")

        route = _signup_route(request.form.get("route"))

        def _retry(message, category="danger"):
            flash(message, category)
            return _render_signup_form(
                invite_required,
                username=username,
                email=(email_raw or "").strip(),
                invite=invite,
                route=route,
            )

        # Closed-beta gate: when SIGNUP_INVITE_CODE is set in the env, the
        # form value must match exactly. compare_digest avoids leaking the
        # code length via early-return timing.
        if invite_required:
            expected = app.config.get("SIGNUP_INVITE_CODE", "")
            if not invite or not hmac.compare_digest(invite, expected):
                return _retry("That invite code isn't valid.")

        if not username or not password:
            return _retry("Username and password are required.")

        # Email is required for new accounts so testers always have a
        # self-service password recovery path. Existing pre-email rows in
        # Postgres keep working — we only enforce it on signup.
        email, email_err = _validate_email(email_raw)
        if email_err:
            return _retry(email_err)
        if not email:
            return _retry(
                "Please add an email so you can recover your account if you "
                "forget your password.",
            )
        if User.get_by_email(email):
            # Do not confirm that this address already has an account.
            return _retry(
                "We couldn't create that account. If you already have one, "
                "sign in or reset your password.",
            )

        if len(username) < 3:
            return _retry("Username must be at least 3 characters.")
        if username.lower() == "demo":
            return _retry("That username is already taken.")

        valid, err = _validate_password(password)
        if not valid:
            return _retry(err)

        if User.get_by_username(username):
            return _retry("That username is already taken.")

        token = (request.form.get("cf-turnstile-response") or "").strip()
        # Fail open when the widget script never loads or Cloudflare
        # errors. A completed challenge that returns success=false still
        # rejects. The signup rate limit above still applies.
        if not verify_turnstile(token, real_client_ip(), fail_open=True):
            return _retry("Confirm you're a person and try again.")

        User.create(username, password, email=email)
        user = User.get_by_username(username)
        login_user(user, remember=False)
        try:
            from app.campaign import stamp_signup
            from app.funnel import on_signup
            # Funnel first so first-touch is captured before the /start
            # cookie fills any still-empty source columns.
            on_signup(user.id if user else None)
            stamp_signup(user.id if user else None)
        except Exception:
            pass
        try:
            from app.ops_notify import notify_event
            notify_event("signup", username=username)
        except Exception:
            pass
        _send_welcome_verification(user)
        flash("Welcome! You're signed in. Check your inbox to confirm your email.", "success")
        # New accounts have no brokerage yet. /get-started is the
        # broker-first connect screen (CSV is a quiet secondary there).
        accounts = get_accounts_for_user(user.id)
        if not accounts and route == "learn":
            next_page = url_for("learn_index")
        elif not accounts:
            next_page = url_for("get_started")
        else:
            prof = get_user_profile(user.id) or {}
            next_page = url_for(_landing_endpoint(prof))
        return redirect(next_page)

    return _render_signup_form(
        invite_required,
        route=_signup_route(request.args.get("route")),
    )


@app.route("/logout", methods=["GET", "POST"])
def logout():
    """POST is the only path that ends the session.

    A bookmarked or typed GET used to 405. Showing a confirm button keeps
    logout a POST (CSRF-protected) without a dead Method Not Allowed page.
    """
    if request.method != "POST":
        if not current_user.is_authenticated:
            return redirect(url_for("login"))
        return render_template("logout.html", title="Log out")
    logout_user()
    return redirect(url_for("index"))


def _demo_gate_context(next_page):
    site_key, _secret = turnstile_keys()
    return {
        "title": "Try the demo",
        "next_for_form": next_page or "",
        "turnstile_site_key": site_key,
    }


@app.route("/demo/start", methods=["GET", "POST"])
@limiter.limit(
    "10 per day",
    methods=["POST"],
    key_func=real_client_ip,
    exempt_when=demo_start_reuses_visitor,
)
def demo_start():
    """Public demo gate.

    GET renders a landing page (homepage and other CTAs still link here).
    POST creates a short-lived ``demo-session:`` identity after Turnstile
    (when configured) and the per-IP session cap. The shared Postgres
    ``demo`` user is not signed in.

    Accepts an optional ``next`` so inbound deep-links (notably the
    EarningsFollower bridge at /earningsfollower/<symbol>) can drop a
    visitor straight onto a specific page. The target is validated by
    ``safe_internal_next`` — same-origin path and query only.
    """
    import time

    next_page = safe_internal_next(request.values.get("next"))
    ephemeral = is_ephemeral_demo_user(current_user)
    username = (getattr(current_user, "username", "") or "").lower()
    shared_demo = (
        current_user.is_authenticated and not ephemeral and username == "demo"
    )
    real_user = current_user.is_authenticated and not ephemeral and not shared_demo

    if real_user or (ephemeral and request.method == "GET"):
        return redirect(next_page or url_for("weekly_review"))

    if request.method == "GET":
        if shared_demo:
            logout_user()
        return render_template("demo_start.html", **_demo_gate_context(next_page))

    if ephemeral:
        return _enter_demo(
            session.get("_demo_token"),
            session.get("_demo_started_at") or time.time(),
            next_page,
        )
    if shared_demo:
        logout_user()

    ip = real_client_ip()
    token = (request.form.get("cf-turnstile-response") or "").strip()
    if not verify_turnstile(token, ip):
        flash("Confirm you're a person and try the demo again.", "warning")
        return render_template("demo_start.html", **_demo_gate_context(next_page)), 400

    reused = reusable_demo_visitor()
    if reused:
        return _enter_demo(reused[0], reused[1], next_page)

    if not reserve_demo_ip(ip):
        return limited_page(
            f"This network has started {IP_SESSION_CAP} demos today. "
            "Create an account to keep going on your own data."
        )

    session_token = secrets.token_urlsafe(24)
    started = time.time()
    remember_demo_visitor(session_token, started)
    return _enter_demo(session_token, started, next_page)


def _enter_demo(session_token, started_at, next_page):
    """Sign in a demo session and stamp the visitor cookie that resumes it."""
    token = str(session_token or "").strip()
    if not token:
        token = secrets.token_urlsafe(24)
        started_at = time.time()
        remember_demo_visitor(token, started_at)
    started = float(started_at)
    remember_demo_visitor(token, started)
    login_user(DemoSessionUser(token), remember=False)
    session["_demo_token"] = token
    session["_demo_started_at"] = started
    session.permanent = False
    response = redirect(next_page or url_for("weekly_review"))
    return attach_demo_visitor_cookie(response, token, started)


@app.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    if request.method == "GET":
        return redirect(url_for("profile", tab="account"))

    if request.method == "POST":
        blocked = demo_block_writes("changing the demo password")
        if blocked:
            return blocked
        current_pw = request.form.get("current_password", "")
        new_pw = request.form.get("new_password", "")
        confirm_pw = request.form.get("confirm_password", "")

        if not current_user.check_password(current_pw):
            flash("Current password is incorrect.", "danger")
            return redirect(url_for("profile", tab="account"))

        valid, err = _validate_password(new_pw)
        if not valid:
            flash(err, "danger")
            return redirect(url_for("profile", tab="account"))

        if new_pw != confirm_pw:
            flash("New passwords do not match.", "danger")
            return redirect(url_for("profile", tab="account"))

        User.update_password(current_user.id, new_pw)
        flash("Password updated successfully.", "success")
        return redirect(url_for("profile", tab="account"))


# ------------------------------------------------------------------
# Self-serve account deletion
# ------------------------------------------------------------------


@app.route("/profile/delete-account", methods=["POST"])
@login_required
@limiter.limit("10 per hour")
def delete_account():
    """Permanently delete the signed-in user's own account.

    Same machinery as the admin delete (``admin_delete_user``), but
    self-serve, always warehouse-inclusive, and with a SnapTrade
    deregistration step the admin path doesn't have. Order matters:

      1. Verify password + typed confirmation ("DELETE"). Demo blocked.
      2. Cancel any live Stripe subscription while its durable user mapping
         and billing-portal access still exist. Abort on failure so a deleted
         account can never continue renewing.
      3. Purge warehouse rows (``purge_user_id_from_seeds``), resolving
         canonical tenant_ids so NULL/stale informational user_id rows are
         included. Abort the whole delete if this fails — never leave a
         deleted Postgres user whose trade rows silently live on in BigQuery.
      4. Deregister the SnapTrade user (kills broker connections and
         stops per-user aggregator billing). Best-effort: on failure we
         log loudly and continue — the periodic orphan sweep
         (``scripts/admin/deregister_orphan_snaptrade_users.py``) exists
         exactly for registrations left behind without a Postgres row.
      5. ``DELETE FROM users`` — every user-scoped Postgres table
         cascades (broker_tenants, snaptrade_*, profile, insights, …).
      6. Log the session out.

    Admins are refused (an admin deleting the last admin locks everyone
    out of /admin; have another admin do it via the admin panel, which
    already refuses self-deletion).
    """
    blocked = demo_block_writes("deleting the demo account")
    if blocked:
        return blocked

    from app.models import is_admin
    if is_admin(current_user.username):
        flash(
            "Admin accounts can't self-delete (that could lock everyone out "
            "of the admin panel). Have another admin remove you from Admin → Users.",
            "danger",
        )
        return redirect(url_for("profile", tab="security"))

    password = request.form.get("password", "")
    typed = (request.form.get("confirm_text") or "").strip()

    if not current_user.check_password(password):
        flash("Password is incorrect. Nothing was deleted.", "danger")
        return redirect(url_for("profile", tab="security"))
    if typed != "DELETE":
        flash('Type DELETE (all caps) to confirm. Nothing was deleted.', "danger")
        return redirect(url_for("profile", tab="security"))

    uid = current_user.id
    uname = current_user.username

    # 2. Stop external payment FIRST. The user/customer/subscription mapping
    # is destroyed by the Postgres cascade and the portal needs a login, so
    # proceeding after a Stripe failure can leave an unreachable renewal.
    from app.billing import cancel_subscription_for_account_deletion
    stripe_ok, stripe_err = cancel_subscription_for_account_deletion(uid)
    if not stripe_ok:
        app.logger.error(
            "SELF-DELETE: Stripe cancellation failed for user_id=%s: %s",
            uid, stripe_err,
        )
        flash(
            "We couldn't cancel your subscription just now, so your account "
            "was NOT deleted and no data was removed. Please try again in a "
            "few minutes or contact support.",
            "danger",
        )
        return redirect(url_for("profile", tab="security"))

    # 3. Warehouse purge — abort everything else if it fails.
    from app.upload import purge_user_id_from_seeds
    ok, err, rows_removed, _marker = purge_user_id_from_seeds(
        uid,
        commit_message=f"self-serve delete: purge seed rows for user {uname} (id={uid})",
    )
    if not ok:
        app.logger.error("SELF-DELETE: warehouse purge failed for user_id=%s: %s", uid, err)
        flash(
            "We couldn't remove your trading data from our warehouse just now, "
            "so your account was NOT deleted. Please try again in a few minutes "
            "or contact support.",
            "danger",
        )
        return redirect(url_for("profile", tab="security"))

    # 4. SnapTrade deregistration (best-effort, before Postgres cascade
    #    removes the snaptrade_users row we need for the call).
    try:
        from app.models import get_snaptrade_user
        snap = get_snaptrade_user(uid)
        if snap and snap.get("snaptrade_user_id"):
            from app.snaptrade import _get_snaptrade_client
            client = _get_snaptrade_client()
            client.authentication.delete_snap_trade_user(
                user_id=snap["snaptrade_user_id"]
            )
            app.logger.info(
                "SELF-DELETE: deregistered SnapTrade user %s for user_id=%s",
                snap["snaptrade_user_id"], uid,
            )
    except Exception as exc:
        # Orphan registration is swept by the periodic orphan-cleanup
        # script; don't block the user's deletion on an aggregator hiccup.
        app.logger.error(
            "SELF-DELETE: SnapTrade deregistration failed for user_id=%s "
            "(orphan will be swept later): %s", uid, exc,
        )

    # 5. Postgres cascade delete.
    from app.models import delete_user
    if not delete_user(uid):
        app.logger.error("SELF-DELETE: Postgres delete failed for user_id=%s", uid)
        flash(
            "Something went wrong finishing the deletion — your trading data "
            "was already removed from the warehouse, but the login still "
            "exists. Please contact support.",
            "danger",
        )
        return redirect(url_for("profile", tab="security"))

    app.logger.warning(
        "SELF-DELETE: user %s (id=%s) deleted their own account "
        "(warehouse rows removed: %s)",
        uname, uid, sum(rows_removed.values()) if rows_removed else 0,
    )
    logout_user()
    flash("Your account and data have been deleted. Thanks for trying HappyTrader.", "success")
    return redirect(url_for("index"))


# ------------------------------------------------------------------
# Email verification
# ------------------------------------------------------------------


def _send_welcome_verification(user):
    """Mint a verification token and send the welcome+verify email.
    Best-effort: a send failure must never block signup."""
    if user is None or not (user.email or "").strip():
        return
    try:
        token = mint_email_verification_token(user.id)
        verify_url = url_for("verify_email", token=token, _external=True)
        send_welcome_verify_email(to=user.email, username=user.username, verify_url=verify_url)
    except Exception as exc:
        app.logger.exception("welcome/verify email failed for user_id=%s: %s", user.id, exc)


@app.route("/verify-email/<token>")
@limiter.limit("20 per minute; 60 per hour")
def verify_email(token):
    """Consume an email-verification token and mark the address confirmed."""
    user_id = consume_email_verification_token(token)
    if user_id is None:
        flash(
            "That verification link is invalid or expired. Sign in and we'll "
            "send a fresh one.",
            "warning",
        )
        return redirect(url_for("login"))
    flash("Email confirmed — thanks!", "success")
    if current_user.is_authenticated:
        # The next step after verifying is connecting/uploading a
        # brokerage, not Overview (which has nothing to show yet for a
        # brand-new signup). get_started already branches to the
        # first-look profile once data exists, so this is safe for a
        # returning user who verifies later too.
        return redirect(url_for("get_started"))
    return redirect(url_for("login"))


@app.route("/resend-verification", methods=["POST"])
@login_required
@limiter.limit("3 per minute; 10 per hour")
def resend_verification():
    """Re-send the verification email to the signed-in user."""
    if not email_needs_verification(current_user.id):
        flash("Your email is already confirmed.", "info")
        return redirect(request.referrer or url_for("weekly_review"))
    _send_welcome_verification(current_user)
    flash("Verification email sent. Check your inbox (and spam).", "info")
    return redirect(request.referrer or url_for("weekly_review"))


@app.context_processor
def _inject_email_verification_needed():
    """Expose ``email_unverified`` so base.html can show a confirm-your-email
    banner. Anonymous requests get False."""
    try:
        from flask import g as _g
        if getattr(_g, "_ht_skeleton", False):
            return {"email_unverified": False}
        if current_user.is_authenticated:
            from app.request_timing import stage
            with stage("email"):
                unverified = email_needs_verification(current_user.id)
            return {"email_unverified": unverified}
    except Exception:
        pass
    return {"email_unverified": False}


def email_block_writes(action: str = "this action"):
    """Short-circuit data-write POSTs until the signed-in user confirms
    their email. Mirrors ``plan_block_writes`` / ``demo_block_writes``.

    Unverified signups must not be able to open SnapTrade connections
    (aggregator billing) or dispatch warehouse rebuilds. Admins and
    accounts with no email on file are exempt. Fails open on DB errors
    so a stale column cannot freeze a paying customer.
    """
    try:
        if not current_user.is_authenticated:
            return None
        from app.models import is_admin
        if is_admin(getattr(current_user, "username", None)):
            return None
        if not email_needs_verification(current_user.id):
            return None
    except Exception:
        return None

    msg = (
        f"Confirm your email before {action}. Check your inbox "
        "(and spam) for the link, or resend it from the banner."
    )
    wants_json = (
        request.path.startswith("/api/")
        or request.accept_mimetypes.best == "application/json"
        or (request.headers.get("X-Requested-With", "") == "XMLHttpRequest")
    )
    if wants_json:
        return jsonify({"error": "email_unverified", "message": msg}), 403
    flash(msg, "warning")
    return redirect(url_for("profile", tab="security"))


# ------------------------------------------------------------------
# Password recovery (email-based)
# ------------------------------------------------------------------


@app.route("/forgot-password", methods=["GET", "POST"])
@limiter.limit("5 per minute; 20 per hour", methods=["POST"])
def forgot_password():
    """Step 1: user submits email → we mint a one-time token and email it.

    The response is identical whether the email matches a real account or
    not — we never confirm membership to an anonymous requester. That's
    why the same flash + redirect runs in both branches below.

    Per-IP rate limit (anonymous endpoint) keeps this from being a probe
    for which addresses are signed up.
    """
    # Same trap as /login: a demo session used to bounce this page back
    # to Overview, so the visitor could not request a reset for their own
    # account. A real signed-in account still skips the form.
    _release_shared_demo_session()
    if current_user.is_authenticated:
        return redirect(url_for("weekly_review"))

    if request.method == "POST":
        email_raw = request.form.get("email", "")
        email, err = _validate_email(email_raw)
        if err or not email:
            # Don't echo "no email" vs "bad format" differently here either.
            flash(
                "If that email is on file, we sent a password-reset link. "
                "Check your inbox (and spam) within a few minutes.",
                "info",
            )
            return redirect(url_for("forgot_password"))

        user = User.get_by_email(email)
        if user is not None:
            # Demo user is shared; refuse to send anyone a reset link for it.
            if (user.username or "").lower() == "demo":
                app.logger.info(
                    "forgot_password: ignoring reset request for the shared "
                    "demo account (requester_ip=%s).",
                    request.remote_addr,
                )
            else:
                try:
                    token = mint_password_reset_token(
                        user.id, requester_ip=request.remote_addr
                    )
                    reset_url = url_for(
                        "reset_password", token=token, _external=True
                    )
                    send_password_reset_email(
                        to=user.email,
                        username=user.username,
                        reset_url=reset_url,
                        ttl_minutes=int(
                            PASSWORD_RESET_TOKEN_TTL.total_seconds() // 60
                        ),
                    )
                except Exception as exc:
                    # Failed mint or send: log but don't tell the requester
                    # (would leak account existence). They can retry.
                    app.logger.exception(
                        "forgot_password mint/send failed for user_id=%s: %s",
                        user.id, exc,
                    )

        flash(
            "If that email is on file, we sent a password-reset link. "
            "Check your inbox (and spam) within a few minutes.",
            "info",
        )
        return redirect(url_for("forgot_password"))

    return render_template("forgot_password.html", title="Forgot Password")


@app.route("/reset-password/<token>", methods=["GET", "POST"])
@limiter.limit("10 per minute; 30 per hour", methods=["POST"])
def reset_password(token):
    """Step 2: user opens the email link, picks a new password.

    GET peeks (read-only) at the token so we can show 'expired link' UX
    without burning the token. POST consumes the token in a single
    transaction so two parallel clicks can't both succeed.
    """
    # A demo session is not the account the email was sent to. Release it
    # so the link can render. A real signed-in session still bounces:
    # rotating the password from an already-hijacked session is not the
    # point of this page.
    _release_shared_demo_session()
    if current_user.is_authenticated:
        return redirect(url_for("profile", tab="account"))

    target_user_id = peek_password_reset_token(token)
    if target_user_id is None or _user_id_is_demo(target_user_id):
        flash(
            "That reset link is invalid or expired. Request a new one.",
            "danger",
        )
        return redirect(url_for("forgot_password"))

    if request.method == "POST":
        new_pw = request.form.get("new_password", "")
        confirm = request.form.get("confirm_password", "")
        valid, err = _validate_password(new_pw)
        if not valid:
            flash(err, "danger")
            return redirect(url_for("reset_password", token=token))
        if new_pw != confirm:
            flash("New passwords do not match.", "danger")
            return redirect(url_for("reset_password", token=token))

        consumed_user_id = consume_password_reset_token(token)
        if consumed_user_id is None or _user_id_is_demo(consumed_user_id):
            # Race: another tab consumed it between peek and consume.
            # The shared demo account cannot be given a password this way.
            flash("That reset link just expired. Request a new one.", "danger")
            return redirect(url_for("forgot_password"))
        User.update_password(consumed_user_id, new_pw)
        flash("Password updated. Sign in with your new password.", "success")
        return redirect(url_for("login"))

    return render_template("reset_password.html", title="Set a new password", token=token)


# ------------------------------------------------------------------
# Email unsubscribe (one-click, no login)
# ------------------------------------------------------------------


@app.route("/email/unsubscribe/<token>", methods=["GET", "POST"])
@csrf.exempt
@limiter.limit("30 per minute; 200 per hour")
def email_unsubscribe(token):
    """One-click unsubscribe from all lifecycle email.

    CSRF-exempt because RFC 8058 one-click unsubscribe (the
    ``List-Unsubscribe-Post: List-Unsubscribe=One-Click`` header on our
    lifecycle mail) makes mail providers POST here with no cookies or
    token. The token itself is the unguessable capability — a successful
    POST only flips the recipient's own opt-out flags to FALSE, which is
    idempotent and non-destructive, so no CSRF token is required.
    """
    username = unsubscribe_user_by_token(token)
    # POST is the provider's one-click call: a bare 200 is all it wants.
    if request.method == "POST":
        return ("", 200)
    return render_template(
        "unsubscribed.html",
        title="Unsubscribed",
        ok=username is not None,
        username=username,
    )


# ------------------------------------------------------------------
# CLI command:  flask create-user --username <name> --password <pw>
# ------------------------------------------------------------------
def _validate_password(password):
    """Return (is_valid, error_message)."""
    if len(password) < 8:
        return False, "Password must be at least 8 characters."
    if not any(c.isdigit() for c in password):
        return False, "Password must contain at least one number."
    if not any(c.isalpha() for c in password):
        return False, "Password must contain at least one letter."
    return True, None


@app.cli.command("create-user")
@click.option("--username", prompt=True, help="Username for the new account")
@click.option("--password", prompt=True, hide_input=True, confirmation_prompt=True,
              help="Password for the new account (min 8 chars, letter + number)")
def create_user(username, password):
    """Create a new user account."""
    if (username or "").strip().lower() == "demo":
        click.echo("Error: User 'demo' is reserved for the public demo.")
        return
    existing = User.get_by_username(username)
    if existing:
        click.echo(f"Error: User '{username}' already exists.")
        return

    valid, err = _validate_password(password)
    if not valid:
        click.echo(f"Error: {err}")
        return

    User.create(username, password)
    click.echo(f"User '{username}' created successfully.")


@app.cli.command("reset-password")
@click.option("--username", required=True, help="Username whose password to change")
@click.option("--password", prompt=True, hide_input=True, confirmation_prompt=True,
              help="New password (min 8 chars, letter + number)")
def reset_password(username, password):
    """Set a new password for an existing user (e.g. lockout recovery)."""
    if (username or "").strip().lower() == "demo":
        click.echo("Error: The public demo account has no password to reset.")
        return
    user = User.get_by_username(username)
    if user is None:
        click.echo(f"Error: No user named '{username}'.")
        return

    valid, err = _validate_password(password)
    if not valid:
        click.echo(f"Error: {err}")
        return

    User.update_password(user.id, password)
    click.echo(f"Password updated for '{username}'.")
