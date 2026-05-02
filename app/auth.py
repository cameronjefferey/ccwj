import hmac
import os
import re
import click
from flask import render_template, redirect, url_for, request, flash, abort
from flask_login import login_required, login_user, logout_user, current_user
from app import app
from app.email import send_password_reset_email
from app.extensions import limiter
from app.models import (
    PASSWORD_RESET_TOKEN_TTL,
    User,
    consume_password_reset_token,
    get_accounts_for_user,
    get_user_profile,
    mint_password_reset_token,
    peek_password_reset_token,
)
from app.utils import demo_block_writes, safe_internal_next


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

# Profile default "home" after login. Keys must match profile_community _ALLOWED_DEFAULT_ROUTE.
_LANDING = {
    "weekly_review": "weekly_review",
    "positions": "positions",
    "strategies": "strategies",
    "insights": "insights",
    "accounts": "accounts",
    "symbols": "symbols_detail",
}


def _landing_endpoint(prof) -> str:
    dr = ((prof or {}).get("default_route") or "weekly_review").strip()
    if dr == "insights" and not app.config.get("INSIGHTS_ENABLED", True):
        dr = "weekly_review"
    return _LANDING.get(dr, "weekly_review")


@app.route("/login", methods=["GET", "POST"])
@limiter.limit("20 per minute")
def login():
    if current_user.is_authenticated:
        dest = safe_internal_next(request.args.get("next"))
        if dest:
            return redirect(dest)
        return redirect(url_for("weekly_review"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        remember = request.form.get("remember") == "on"

        user = User.get_by_username(username)
        if user is None or not user.check_password(password):
            flash("Invalid username or password.", "danger")
            nxt = safe_internal_next(
                request.form.get("next") or request.args.get("next")
            )
            if nxt:
                return redirect(url_for("login", next=nxt))
            return redirect(url_for("login"))

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


@app.route("/signup", methods=["GET", "POST"])
@limiter.limit("10 per minute; 30 per hour", methods=["POST"])
def signup():
    if not app.config.get("SIGNUP_ENABLED", True):
        abort(404)

    if current_user.is_authenticated:
        return redirect(url_for("weekly_review"))

    invite_required = bool(app.config.get("SIGNUP_INVITE_CODE", ""))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm", "")
        invite = (request.form.get("invite_code", "") or "").strip()
        email_raw = request.form.get("email", "")

        # Closed-beta gate: when SIGNUP_INVITE_CODE is set in the env, the
        # form value must match exactly. compare_digest avoids leaking the
        # code length via early-return timing.
        if invite_required:
            expected = app.config.get("SIGNUP_INVITE_CODE", "")
            if not invite or not hmac.compare_digest(invite, expected):
                flash("That invite code isn't valid.", "danger")
                return redirect(url_for("signup"))

        if not username or not password:
            flash("Username and password are required.", "danger")
            return redirect(url_for("signup"))

        # Email is required for new accounts so testers always have a
        # self-service password recovery path. Existing pre-email rows in
        # Postgres keep working — we only enforce it on signup.
        email, email_err = _validate_email(email_raw)
        if email_err:
            flash(email_err, "danger")
            return redirect(url_for("signup"))
        if not email:
            flash(
                "Please add an email so you can recover your account if you "
                "forget your password.",
                "danger",
            )
            return redirect(url_for("signup"))
        if User.get_by_email(email):
            # Generic message: don't confirm to a stranger which addresses
            # are signed up. They can recover via /forgot-password.
            flash(
                "That email is already in use. If it's yours, sign in or "
                "use 'Forgot password' to recover.",
                "danger",
            )
            return redirect(url_for("signup"))

        if len(username) < 3:
            flash("Username must be at least 3 characters.", "danger")
            return redirect(url_for("signup"))

        valid, err = _validate_password(password)
        if not valid:
            flash(err, "danger")
            return redirect(url_for("signup"))

        if password != confirm:
            flash("Passwords do not match.", "danger")
            return redirect(url_for("signup"))

        if User.get_by_username(username):
            flash("That username is already taken.", "danger")
            return redirect(url_for("signup"))

        User.create(username, password, email=email)
        user = User.get_by_username(username)
        login_user(user, remember=False)
        flash("Welcome! You're signed in.", "success")
        accounts = get_accounts_for_user(user.id)
        if not accounts:
            next_page = url_for("get_started")
        else:
            prof = get_user_profile(user.id) or {}
            next_page = url_for(_landing_endpoint(prof))
        return redirect(next_page)

    return render_template(
        "signup.html",
        title="Sign Up",
        invite_required=invite_required,
    )


@app.route("/logout", methods=["POST"])
def logout():
    logout_user()
    return redirect(url_for("index"))


@app.route("/demo/start")
def demo_start():
    """Log in as the demo user and redirect to the dashboard. No sign-up required."""
    if current_user.is_authenticated:
        return redirect(url_for("weekly_review"))

    demo = User.get_by_username("demo")
    if demo is None:
        flash("Demo is not available. Please create an account to get started.", "warning")
        target = "login" if not app.config.get("SIGNUP_ENABLED", True) else "signup"
        return redirect(url_for(target))

    login_user(demo, remember=False)
    return redirect(url_for("weekly_review"))


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
    if current_user.is_authenticated:
        # Re-using a reset link while signed in is almost never what you
        # want; bounce them to settings instead of letting an attacker
        # who already hijacked a session also rotate the password.
        return redirect(url_for("profile", tab="account"))

    target_user_id = peek_password_reset_token(token)
    if target_user_id is None:
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
        if consumed_user_id is None:
            # Race: another tab consumed it between peek and consume.
            flash("That reset link just expired. Request a new one.", "danger")
            return redirect(url_for("forgot_password"))
        User.update_password(consumed_user_id, new_pw)
        flash("Password updated. Sign in with your new password.", "success")
        return redirect(url_for("login"))

    return render_template("reset_password.html", title="Set a new password", token=token)


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
