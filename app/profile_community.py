"""
Profile hub (preferences, account, security) and community: follows, feed, public profiles.
"""
import os
from urllib.parse import urlparse

from flask import abort, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from app import app
from app.models import (
    User,
    community_feed_for_follower,
    count_published_trades,
    discover_public_traders,
    follow_counts,
    follow_user,
    get_accounts_for_user,
    get_published_trade_fingerprints,
    get_schwab_connection,
    get_schwab_connections,
    get_uploads_for_user,
    get_user_by_username,
    get_user_profile,
    is_admin,
    is_following,
    list_following_ids,
    list_public_published_trades,
    publish_community_trade,
    search_discoverable_traders,
    trade_fingerprint,
    unfollow_user,
    unpublish_community_trade,
    update_user_profile,
)

_ALLOWED_ACCENTS = frozenset({"violet", "teal", "amber", "rose", "slate"})
_ALLOWED_VISIBILITY = frozenset({"private", "followers", "public"})
_ALLOWED_DEFAULT_ROUTE = frozenset({
    "weekly_review", "positions", "strategies", "insights", "accounts", "symbols",
})


def _safe_redirect_target(raw_next):
    if not raw_next or not isinstance(raw_next, str):
        return None
    raw_next = raw_next.strip()
    if not raw_next.startswith("/") or raw_next.startswith("//"):
        return None
    p = urlparse(raw_next)
    if p.netloc:
        return None
    return raw_next


def _account_allowed(user_id, username, account_name):
    if is_admin(username):
        return True
    return account_name in get_accounts_for_user(user_id)


def _viewer_can_see_profile(viewer_id, target_id, visibility, following):
    if viewer_id == target_id:
        return True
    if visibility == "public":
        return True
    if visibility == "followers" and following:
        return True
    return False


@app.route("/profile", methods=["GET", "POST"])
@login_required
def profile():
    from app.auth import _validate_password

    tab = request.args.get("tab", "overview")
    if tab not in ("overview", "preferences", "account", "community", "published"):
        tab = "overview"

    if request.method == "POST":
        action = request.form.get("action", "")
        if action == "change_password":
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

        if action == "save_profile":
            settings_tab = (request.form.get("settings_tab") or "").strip().lower()
            if settings_tab == "community":
                visibility = (request.form.get("profile_visibility") or "private").strip().lower()
                if visibility not in _ALLOWED_VISIBILITY:
                    visibility = "private"
                show_acct = request.form.get("show_account_names_on_published") == "on"
                if not update_user_profile(
                    current_user.id,
                    profile_visibility=visibility,
                    show_account_names_on_published=show_acct,
                ):
                    flash("Could not save (database may need a deploy restart so new tables exist). Check server logs.", "danger")
                    return redirect(url_for("profile", tab="community"))
                flash("The door is set how you want it.", "success")
                return redirect(url_for("profile", tab="community"))

            display_name = (request.form.get("display_name") or "").strip() or None
            headline = (request.form.get("headline") or "").strip() or None
            bio = (request.form.get("bio") or "").strip() or None
            accent = (request.form.get("accent") or "violet").strip().lower()
            if accent not in _ALLOWED_ACCENTS:
                accent = "violet"
            timezone = (request.form.get("timezone") or "America/New_York").strip() or "America/New_York"
            week_starts_monday = request.form.get("week_starts_monday") == "on"
            default_route = (request.form.get("default_route") or "weekly_review").strip()
            if default_route not in _ALLOWED_DEFAULT_ROUTE:
                default_route = "weekly_review"
            digest_email = request.form.get("digest_email") == "on"
            compact_tables = request.form.get("compact_tables") == "on"
            if not update_user_profile(
                current_user.id,
                display_name=display_name,
                headline=headline,
                bio=bio,
                accent=accent,
                timezone=timezone,
                week_starts_monday=week_starts_monday,
                default_route=default_route,
                digest_email=digest_email,
                compact_tables=compact_tables,
            ):
                flash("Could not save profile (database migration may be pending). Check server logs.", "danger")
                return redirect(url_for("profile", tab="preferences"))
            flash("Profile saved.", "success")
            return redirect(url_for("profile", tab="preferences"))

    prof = get_user_profile(current_user.id)
    accounts = get_accounts_for_user(current_user.id)
    recent_uploads = get_uploads_for_user(current_user.id)
    schwab_enabled = bool(os.environ.get("SCHWAB_APP_KEY") and os.environ.get("SCHWAB_APP_SECRET"))
    schwab_connections = get_schwab_connections(current_user.id) if schwab_enabled else []
    schwab_first_sync_completed = False
    schwab_routine_lookback_days = 60
    schwab_full_history_lookback_days = 1825
    if schwab_enabled and schwab_connections:
        from app.schwab import SCHWAB_FULL_HISTORY_LOOKBACK_DAYS, _schwab_transaction_lookback_days

        schwab_full_history_lookback_days = SCHWAB_FULL_HISTORY_LOOKBACK_DAYS
        _c = get_schwab_connection(current_user.id)
        if _c:
            schwab_first_sync_completed = bool(_c.get("schwab_first_sync_completed"))
        schwab_routine_lookback_days = _schwab_transaction_lookback_days()
    fc, fwing = follow_counts(current_user.id)
    my_published = list_public_published_trades(current_user.id, limit=100)
    show_names = bool(prof.get("show_account_names_on_published")) if prof else False
    published_count = count_published_trades(current_user.id)

    return render_template(
        "profile.html",
        title="Who you are",
        tab=tab,
        profile_row=prof,
        accounts=accounts,
        recent_uploads=recent_uploads,
        schwab_enabled=schwab_enabled,
        schwab_connections=schwab_connections,
        schwab_first_sync_completed=schwab_first_sync_completed,
        schwab_routine_lookback_days=schwab_routine_lookback_days,
        schwab_full_history_lookback_days=schwab_full_history_lookback_days,
        follower_count=fc,
        following_count=fwing,
        my_published_trades=my_published,
        published_count=published_count,
        accent_presets=sorted(_ALLOWED_ACCENTS),
        default_routes=sorted(_ALLOWED_DEFAULT_ROUTE),
    )


@app.route("/community")
@login_required
def community():
    feed = community_feed_for_follower(current_user.id, limit=60)
    following_ids = list_following_ids(current_user.id)
    search_query = (request.args.get("q") or "").strip()[:200]
    search_results = []
    if len(search_query) >= 2:
        search_results = search_discoverable_traders(
            current_user.id, search_query, limit=50
        )
    discover = discover_public_traders(limit=30)
    fc, fwing = follow_counts(current_user.id)
    prof = get_user_profile(current_user.id)
    published_count = count_published_trades(current_user.id)
    # Preserve search when follow/unfollow returns here
    community_return = (
        url_for("community", q=search_query)
        if len(search_query) >= 2
        else url_for("community")
    )
    return render_template(
        "community.html",
        title="The Wall",
        feed=feed,
        following_ids=following_ids,
        discover=discover,
        search_query=search_query,
        search_results=search_results,
        community_return_url=community_return,
        follower_count=fc,
        following_count=fwing,
        profile_row=prof,
        published_count=published_count,
    )


@app.route("/u/<username>")
@login_required
def public_trader_profile(username):
    row = get_user_by_username(username)
    if not row:
        abort(404)
    target_id = int(row["id"])
    prof = get_user_profile(target_id)
    if not prof:
        abort(404)
    visibility = (prof.get("profile_visibility") or "private").lower()
    following = is_following(current_user.id, target_id)
    can_see = _viewer_can_see_profile(current_user.id, target_id, visibility, following)
    fc, fwing = follow_counts(target_id)
    published = list_public_published_trades(target_id, limit=80) if can_see else []
    show_names = bool(prof.get("show_account_names_on_published")) if can_see else False
    published_count = count_published_trades(target_id)

    return render_template(
        "user_public.html",
        title=(prof.get("display_name") or row["username"]),
        subject=row,
        profile_row=prof,
        can_see=can_see,
        visibility=visibility,
        is_following=following,
        follower_count=fc,
        following_count=fwing,
        published_trades=published,
        show_account_names=show_names,
        published_count=published_count,
    )


@app.route("/u/<username>/follow", methods=["POST"])
@login_required
def follow_trader(username):
    row = get_user_by_username(username)
    if not row:
        abort(404)
    target_id = int(row["id"])
    if target_id == current_user.id:
        flash("You cannot follow yourself.", "warning")
        return redirect(_safe_redirect_target(request.form.get("next")) or url_for("community"))
    prof = get_user_profile(target_id)
    visibility = (prof.get("profile_visibility") or "private").lower()
    if visibility == "private" and current_user.id != target_id:
        flash("This trader keeps their profile private — follows are disabled.", "warning")
        return redirect(url_for("public_trader_profile", username=username))
    follow_user(current_user.id, target_id)
    flash(f"You’re following @{row['username']} — their receipts will show on your Wall.", "success")
    nxt = _safe_redirect_target(request.form.get("next"))
    return redirect(nxt or url_for("public_trader_profile", username=username))


@app.route("/u/<username>/unfollow", methods=["POST"])
@login_required
def unfollow_trader(username):
    row = get_user_by_username(username)
    if not row:
        abort(404)
    target_id = int(row["id"])
    unfollow_user(current_user.id, target_id)
    flash("Unfollowed.", "info")
    nxt = _safe_redirect_target(request.form.get("next"))
    return redirect(nxt or url_for("community"))


@app.route("/community/publish-trade", methods=["POST"])
@login_required
def community_publish_trade_route():
    """Publish or unpublish a single trade to the community feed (snapshot row in Postgres)."""
    nxt = _safe_redirect_target(request.form.get("next")) or url_for("weekly_review")
    action = (request.form.get("action") or "").strip().lower()
    fingerprint = (request.form.get("trade_fingerprint") or "").strip()

    if not fingerprint:
        flash("Missing trade reference.", "danger")
        return redirect(nxt)

    if action == "unpublish":
        if fingerprint not in get_published_trade_fingerprints(current_user.id):
            flash("That trade was not shared.", "warning")
            return redirect(nxt)
        if not unpublish_community_trade(current_user.id, fingerprint):
            flash("Could not remove that receipt (database error). Try again after redeploy.", "danger")
            return redirect(nxt)
        flash("Taken down from the wall. That trade is yours alone again.", "success")
        return redirect(nxt)

    if action != "publish":
        flash("Unknown action.", "danger")
        return redirect(nxt)

    account = (request.form.get("account") or "").strip()
    symbol = (request.form.get("symbol") or "").strip()
    strategy = (request.form.get("strategy") or "").strip()
    trade_symbol = (request.form.get("trade_symbol") or "").strip()
    open_date = (request.form.get("open_date") or "").strip()
    close_date = (request.form.get("close_date") or "").strip()
    status = (request.form.get("status") or "").strip()
    caption = (request.form.get("caption") or "").strip() or None

    try:
        display_pnl = float(request.form.get("display_pnl") or "")
    except ValueError:
        display_pnl = None

    if not _account_allowed(current_user.id, current_user.username, account):
        flash("That account is not linked to your login.", "danger")
        return redirect(nxt)

    expected = trade_fingerprint(
        current_user.id, account, symbol, trade_symbol, open_date, close_date, strategy,
    )
    if expected != fingerprint:
        flash("Could not verify that trade. Refresh the page and try again.", "danger")
        return redirect(nxt)

    prof = get_user_profile(current_user.id)
    vis = (prof.get("profile_visibility") or "private").lower()
    if vis == "private":
        flash("Open the door first: Profile → The door → set visibility to Followers or Public, then hang your receipt.", "warning")
        return redirect(url_for("profile", tab="community"))

    acct_label = account
    if not prof.get("show_account_names_on_published"):
        acct_label = "Account"

    if not publish_community_trade(
        current_user.id,
        fingerprint,
        acct_label,
        symbol,
        strategy,
        trade_symbol,
        open_date,
        close_date,
        status,
        display_pnl,
        caption=caption,
    ):
        flash("Could not publish (database may be missing community tables). Redeploy or restart the web service.", "danger")
        return redirect(nxt)
    flash("It’s on the wall. Anyone who follows you will see that receipt.", "success")
    return redirect(nxt)
