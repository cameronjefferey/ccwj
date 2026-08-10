"""
Profile hub (/profile): preferences, account, security, notifications.

Was ``app/profile_community.py`` until the Aug 2026 surface audit removed
the community feature (feed, follows, public profiles, trade publishing) —
it shipped flag-disabled, saw ~no usage, and argued with the product's
"compare traders to themselves, not to others" identity. The community
Postgres tables survive until a later migration drops them; git history
holds the code.
"""
from flask import flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from app import app
from app.utils import demo_block_writes
from app.models import (
    User,
    get_accounts_for_user,
    get_uploads_for_user,
    get_user_profile,
    update_user_profile,
)

_ALLOWED_ACCENTS = frozenset({"violet", "teal", "amber", "rose", "slate"})
_ALLOWED_DEFAULT_ROUTE = frozenset({
    "weekly_review", "positions", "strategies", "insights", "accounts",
})


@app.route("/profile", methods=["GET", "POST"])
@login_required
def profile():
    from app.auth import _validate_password

    tab = request.args.get("tab", "overview")
    if tab not in ("overview", "preferences", "account", "security", "notifications"):
        tab = "overview"

    if request.method == "POST":
        blocked = demo_block_writes("profile and account settings")
        if blocked:
            return blocked
        action = request.form.get("action", "")
        if action == "set_email":
            from app.auth import _validate_email

            email_raw = request.form.get("email", "")
            email, err = _validate_email(email_raw)
            if err:
                flash(err, "danger")
                return redirect(url_for("profile", tab="security"))
            # Allow clearing the email by submitting blank, but warn since
            # losing email means losing self-serve recovery.
            if email is None:
                User.update_email(current_user.id, None)
                flash(
                    "Email removed. You won't be able to reset your password "
                    "without contacting support.",
                    "warning",
                )
                return redirect(url_for("profile", tab="security"))
            existing = User.get_by_email(email)
            if existing is not None and int(existing.id) != int(current_user.id):
                flash(
                    "That email is already in use on another account.",
                    "danger",
                )
                return redirect(url_for("profile", tab="security"))
            User.update_email(current_user.id, email)
            flash("Email updated.", "success")
            return redirect(url_for("profile", tab="security"))

        if action == "change_password":
            current_pw = request.form.get("current_password", "")
            new_pw = request.form.get("new_password", "")
            confirm_pw = request.form.get("confirm_password", "")
            if not current_user.check_password(current_pw):
                flash("Current password is incorrect.", "danger")
                return redirect(url_for("profile", tab="security"))
            valid, err = _validate_password(new_pw)
            if not valid:
                flash(err, "danger")
                return redirect(url_for("profile", tab="security"))
            if new_pw != confirm_pw:
                flash("New passwords do not match.", "danger")
                return redirect(url_for("profile", tab="security"))
            User.update_password(current_user.id, new_pw)
            flash("Password updated successfully.", "success")
            return redirect(url_for("profile", tab="security"))

        if action == "save_profile":
            settings_tab = (request.form.get("settings_tab") or "").strip().lower()
            if settings_tab == "notifications":
                # Email opt-ins only — partial update so we don't touch
                # display_name / timezone / etc. set on the Preferences tab.
                digest_email = request.form.get("digest_email") == "on"
                weekly_preview_email = request.form.get("weekly_preview_email") == "on"
                product_update_email = request.form.get("product_update_email") == "on"
                if not update_user_profile(
                    current_user.id,
                    digest_email=digest_email,
                    weekly_preview_email=weekly_preview_email,
                    product_update_email=product_update_email,
                ):
                    flash("Could not save notification settings. Check server logs.", "danger")
                    return redirect(url_for("profile", tab="notifications"))
                flash("Notification settings saved.", "success")
                return redirect(url_for("profile", tab="notifications"))

            display_name = (request.form.get("display_name") or "").strip() or None
            timezone = (request.form.get("timezone") or "America/New_York").strip() or "America/New_York"
            week_starts_monday = request.form.get("week_starts_monday") == "on"
            default_route = (request.form.get("default_route") or "weekly_review").strip()
            if default_route not in _ALLOWED_DEFAULT_ROUTE:
                default_route = "weekly_review"
            if default_route == "insights" and not app.config.get("INSIGHTS_ENABLED", True):
                default_route = "weekly_review"
            compact_tables = request.form.get("compact_tables") == "on"
            if not update_user_profile(
                current_user.id,
                display_name=display_name,
                timezone=timezone,
                week_starts_monday=week_starts_monday,
                default_route=default_route,
                compact_tables=compact_tables,
            ):
                flash("Could not save profile (database migration may be pending). Check server logs.", "danger")
                return redirect(url_for("profile", tab="preferences"))
            flash("Profile saved.", "success")
            return redirect(url_for("profile", tab="preferences"))

    prof = get_user_profile(current_user.id)
    profile_row = prof
    if prof and (prof.get("default_route") or "") in ("insights", "symbols"):
        # insights may be flag-disabled; symbols was retired (Aug 2026 audit).
        coerced = prof.get("default_route")
        if coerced == "symbols":
            profile_row = {**prof, "default_route": "positions"}
        elif not app.config.get("INSIGHTS_ENABLED", True):
            profile_row = {**prof, "default_route": "weekly_review"}
    accounts = get_accounts_for_user(current_user.id)
    recent_uploads = get_uploads_for_user(current_user.id)

    snaptrade_enabled = False
    snaptrade_accounts = []
    snaptrade_routine_lookback_days = 60
    snaptrade_full_history_lookback_days = 1825
    try:
        from app.snaptrade import (
            snaptrade_enabled as _snaptrade_enabled_fn,
            _routine_lookback_days as _snap_routine_fn,
            SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS as _snap_full_days,
        )
        from app.models import get_snaptrade_accounts as _get_snaptrade_accounts

        snaptrade_enabled = _snaptrade_enabled_fn()
        snaptrade_accounts = _get_snaptrade_accounts(current_user.id) or []
        snaptrade_routine_lookback_days = int(_snap_routine_fn())
        snaptrade_full_history_lookback_days = int(_snap_full_days)
    except Exception:
        snaptrade_enabled = False
        snaptrade_accounts = []

    routes = sorted(_ALLOWED_DEFAULT_ROUTE)
    if not app.config.get("INSIGHTS_ENABLED", True):
        routes = [r for r in routes if r != "insights"]

    return render_template(
        "profile.html",
        title="Settings",
        tab=tab,
        profile_row=profile_row,
        accounts=accounts,
        recent_uploads=recent_uploads,
        snaptrade_enabled=snaptrade_enabled,
        snaptrade_accounts=snaptrade_accounts,
        snaptrade_routine_lookback_days=snaptrade_routine_lookback_days,
        snaptrade_full_history_lookback_days=snaptrade_full_history_lookback_days,
        accent_presets=sorted(_ALLOWED_ACCENTS),
        default_routes=routes,
    )
