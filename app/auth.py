import hmac
import os
import click
from flask import render_template, redirect, url_for, request, flash, abort
from flask_login import login_required, login_user, logout_user, current_user
from app import app
from app.extensions import limiter
from app.models import User, get_accounts_for_user, get_user_profile
from app.utils import safe_internal_next

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

        User.create(username, password)
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
