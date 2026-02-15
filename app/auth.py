import os
import click
from flask import render_template, redirect, url_for, request, flash
from flask_login import login_required, login_user, logout_user, current_user
from app import app
from app.models import User, get_accounts_for_user, get_uploads_for_user, get_schwab_connections


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        remember = request.form.get("remember") == "on"

        user = User.get_by_username(username)
        if user is None or not user.check_password(password):
            flash("Invalid username or password.", "danger")
            return redirect(url_for("login"))

        login_user(user, remember=remember)

        # Redirect to the page the user originally wanted
        next_page = request.args.get("next")
        if not next_page or not next_page.startswith("/"):
            # New users (no accounts) go to onboarding first
            accounts = get_accounts_for_user(user.id)
            next_page = url_for("get_started") if not accounts else url_for("dashboard")
        return redirect(next_page)

    return render_template("login.html", title="Login")


@app.route("/signup", methods=["GET", "POST"])
def signup():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm", "")

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
        flash("Account created! You can now sign in.", "success")
        return redirect(url_for("login"))

    return render_template("signup.html", title="Sign Up")


@app.route("/logout")
def logout():
    logout_user()
    return redirect(url_for("login"))


@app.route("/demo/start")
def demo_start():
    """Log in as the demo user and redirect to the dashboard. No sign-up required."""
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))

    demo = User.get_by_username("demo")
    if demo is None:
        flash("Demo is not available. Please create an account to get started.", "warning")
        return redirect(url_for("signup"))

    login_user(demo, remember=False)
    return redirect(url_for("dashboard"))


@app.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    if request.method == "POST":
        current_pw = request.form.get("current_password", "")
        new_pw = request.form.get("new_password", "")
        confirm_pw = request.form.get("confirm_password", "")

        if not current_user.check_password(current_pw):
            flash("Current password is incorrect.", "danger")
            return redirect(url_for("settings"))

        valid, err = _validate_password(new_pw)
        if not valid:
            flash(err, "danger")
            return redirect(url_for("settings"))

        if new_pw != confirm_pw:
            flash("New passwords do not match.", "danger")
            return redirect(url_for("settings"))

        User.update_password(current_user.id, new_pw)
        flash("Password updated successfully.", "success")
        return redirect(url_for("settings"))

    accounts = get_accounts_for_user(current_user.id)
    recent_uploads = get_uploads_for_user(current_user.id)
    schwab_enabled = bool(os.environ.get("SCHWAB_APP_KEY") and os.environ.get("SCHWAB_APP_SECRET"))
    schwab_connections = get_schwab_connections(current_user.id) if schwab_enabled else []

    return render_template(
        "settings.html",
        title="Settings",
        accounts=accounts,
        recent_uploads=recent_uploads,
        schwab_enabled=schwab_enabled,
        schwab_connections=schwab_connections,
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
