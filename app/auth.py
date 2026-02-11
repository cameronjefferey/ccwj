import click
from flask import render_template, redirect, url_for, request, flash
from flask_login import login_user, logout_user, current_user
from app import app
from app.models import User


@app.route("/auth-debug")
def auth_debug():
    """Temporary debug endpoint -- remove after verifying login works."""
    import os
    from app.models import _get_db, DB_PATH
    env_set = bool(os.environ.get("HAPPYTRADER_USERS", ""))
    conn = _get_db()
    rows = conn.execute("SELECT id, username FROM users").fetchall()
    conn.close()
    users = [{"id": r["id"], "username": r["username"]} for r in rows]
    return {
        "db_path": DB_PATH,
        "db_exists": os.path.exists(DB_PATH),
        "env_var_set": env_set,
        "user_count": len(users),
        "users": users,
    }


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

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
            next_page = url_for("index")
        return redirect(next_page)

    return render_template("login.html", title="Login")


@app.route("/logout")
def logout():
    logout_user()
    return redirect(url_for("login"))


# ------------------------------------------------------------------
# CLI command:  flask create-user --username <name> --password <pw>
# ------------------------------------------------------------------
@app.cli.command("create-user")
@click.option("--username", prompt=True, help="Username for the new account")
@click.option("--password", prompt=True, hide_input=True, confirmation_prompt=True,
              help="Password for the new account")
def create_user(username, password):
    """Create a new user account."""
    existing = User.get_by_username(username)
    if existing:
        click.echo(f"Error: User '{username}' already exists.")
        return

    User.create(username, password)
    click.echo(f"User '{username}' created successfully.")
