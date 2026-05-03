"""
Profile hub (preferences, account, security) and community: follows, feed, public profiles.
"""
import os
from urllib.parse import urlparse

from flask import abort, flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from app import app

# Endpoints that make up the community surface. When COMMUNITY_ENABLED is off
# every one of these 404s and any leftover link in a template (or external
# bookmark) becomes a hard "page not found" instead of an empty/broken page.
# The /profile route itself is *not* in here: it stays usable for preferences,
# account, security; only the community/published tabs inside profile.html are
# hidden + redirected (handled below in the GET handler).
_COMMUNITY_ENDPOINTS = frozenset({
    "community",
    "community_post_create",
    "community_my_trades",
    "community_post_delete",
    "community_post_visibility",
    "public_trader_profile",
    "follow_trader",
    "unfollow_trader",
    "community_publish_trade_route",
})


@app.before_request
def _require_community_feature():
    """404 every community endpoint when the feature flag is off.

    Mirrors the pattern used by /insights (`_require_insights_feature` in
    app/insights.py) so behaviour stays consistent between the two
    "behind-a-flag while we iterate" surfaces.
    """
    if app.config.get("COMMUNITY_ENABLED", False):
        return None
    if request.endpoint in _COMMUNITY_ENDPOINTS:
        abort(404)
    return None
from app.bigquery_client import get_bigquery_client
from app.utils import demo_block_writes
from app.models import (
    User,
    community_feed,
    count_published_trades,
    create_post,
    decode_post_attachments,
    delete_post,
    discover_public_traders,
    discover_recent_public_posts,
    follow_counts,
    follow_user,
    get_accounts_for_user,
    get_post,
    get_published_trade_fingerprints,
    get_schwab_connection,
    get_schwab_connections,
    get_uploads_for_user,
    get_user_by_username,
    get_user_profile,
    is_admin,
    is_following,
    list_following_ids,
    list_posts_by_user,
    list_public_published_trades,
    publish_community_trade,
    search_discoverable_traders,
    trade_fingerprint,
    unfollow_user,
    unpublish_community_trade,
    update_post_visibility,
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
    if tab in ("community", "published") and not app.config.get("COMMUNITY_ENABLED", False):
        return redirect(url_for("profile", tab="overview"))

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
                return redirect(url_for("profile", tab="account"))
            # Allow clearing the email by submitting blank, but warn since
            # losing email means losing self-serve recovery.
            if email is None:
                User.update_email(current_user.id, None)
                flash(
                    "Email removed. You won't be able to reset your password "
                    "without contacting support.",
                    "warning",
                )
                return redirect(url_for("profile", tab="account"))
            existing = User.get_by_email(email)
            if existing is not None and int(existing.id) != int(current_user.id):
                flash(
                    "That email is already in use on another account.",
                    "danger",
                )
                return redirect(url_for("profile", tab="account"))
            User.update_email(current_user.id, email)
            flash("Email updated.", "success")
            return redirect(url_for("profile", tab="account"))

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
                if not app.config.get("COMMUNITY_ENABLED", False):
                    abort(404)
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
                flash("Visibility updated.", "success")
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
            if default_route == "insights" and not app.config.get("INSIGHTS_ENABLED", True):
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
    profile_row = prof
    if prof and (prof.get("default_route") or "") == "insights" and not app.config.get("INSIGHTS_ENABLED", True):
        profile_row = {**prof, "default_route": "weekly_review"}
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
        default_routes=routes,
    )


@app.route("/community")
@login_required
def community():
    feed = decode_post_attachments(community_feed(current_user.id, limit=80))
    following_ids = list_following_ids(current_user.id)
    search_query = (request.args.get("q") or "").strip()[:200]
    search_results = []
    if len(search_query) >= 2:
        search_results = search_discoverable_traders(
            current_user.id, search_query, limit=50
        )
    discover_traders = discover_public_traders(limit=12)
    discover_posts = decode_post_attachments(
        discover_recent_public_posts(current_user.id, limit=8)
        if not feed else []
    )
    fc, fwing = follow_counts(current_user.id)
    prof = get_user_profile(current_user.id)
    published_count = count_published_trades(current_user.id)
    community_return = (
        url_for("community", q=search_query)
        if len(search_query) >= 2
        else url_for("community")
    )
    default_visibility = (prof.get("profile_visibility") or "followers").lower()
    if default_visibility == "private":
        default_visibility = "followers"
    return render_template(
        "community.html",
        title="Community",
        feed=feed,
        following_ids=following_ids,
        discover_traders=discover_traders,
        discover_posts=discover_posts,
        search_query=search_query,
        search_results=search_results,
        community_return_url=community_return,
        follower_count=fc,
        following_count=fwing,
        profile_row=prof,
        published_count=published_count,
        default_post_visibility=default_visibility,
    )


@app.route("/community/post", methods=["POST"])
@login_required
def community_post_create():
    blocked = demo_block_writes("posting to the community")
    if blocked:
        return blocked
    body = (request.form.get("body") or "").strip()
    symbol = (request.form.get("symbol") or "").strip()
    strategy = (request.form.get("strategy") or "").strip()
    visibility = (request.form.get("visibility") or "followers").strip().lower()
    next_url = _safe_redirect_target(request.form.get("next")) or url_for("community")

    if not body:
        flash("Write something before posting.", "warning")
        return redirect(next_url)

    # Optional trade attachment — when the user picks one of their own trades
    # from the composer, we publish a snapshot row (so the feed can join it)
    # and wire the post.attached_fingerprint to it.
    attach_fp = (request.form.get("attach_fingerprint") or "").strip() or None
    if attach_fp:
        account = (request.form.get("attach_account") or "").strip()
        att_symbol = (request.form.get("attach_symbol") or "").strip()
        att_strategy = (request.form.get("attach_strategy") or "").strip()
        att_trade_symbol = (request.form.get("attach_trade_symbol") or "").strip()
        att_open = (request.form.get("attach_open_date") or "").strip()
        att_close = (request.form.get("attach_close_date") or "").strip()
        att_status = (request.form.get("attach_status") or "").strip()
        try:
            att_pnl = float(request.form.get("attach_display_pnl") or "")
        except (TypeError, ValueError):
            att_pnl = None

        if not _account_allowed(current_user.id, current_user.username, account):
            flash("That trade belongs to an account you don't own.", "danger")
            return redirect(next_url)
        expected = trade_fingerprint(
            current_user.id, account, att_symbol, att_trade_symbol,
            att_open, att_close, att_strategy,
        )
        if expected != attach_fp:
            flash("Could not verify the attached trade. Refresh and try again.", "danger")
            return redirect(next_url)

        prof = get_user_profile(current_user.id)
        acct_label = account if prof.get("show_account_names_on_published") else "Account"
        publish_community_trade(
            current_user.id, attach_fp, acct_label, att_symbol, att_strategy,
            att_trade_symbol, att_open, att_close, att_status, att_pnl, caption=None,
        )
        if not symbol and att_symbol:
            symbol = att_symbol
        if not strategy and att_strategy:
            strategy = att_strategy

    # Generic attachment payload: supports 'strategy' and 'transaction' pick types
    # (legs still go through attach_fp above). The payload is a small JSON blob
    # we validate, truncate, and store so the feed can render an inline card.
    attachment_kind = (request.form.get("attachment_kind") or "").strip().lower() or None
    attachment_json = None
    if attachment_kind in ("strategy", "transaction") and not attach_fp:
        payload = _build_attachment_payload(attachment_kind, request.form)
        if payload is None:
            attachment_kind = None
        else:
            import json as _json
            attachment_json = _json.dumps(payload, separators=(",", ":"))
            # Auto-fill symbol/strategy tags from the attachment when composer
            # left them blank, so chips still show even if user didn't type.
            if not symbol and payload.get("symbol"):
                symbol = payload["symbol"]
            if not strategy and attachment_kind == "strategy" and payload.get("strategy"):
                strategy = payload["strategy"]
    else:
        attachment_kind = None

    new_id = create_post(
        current_user.id,
        body=body,
        symbol=symbol or None,
        strategy=strategy or None,
        visibility=visibility,
        attached_fingerprint=attach_fp,
        attachment_kind=attachment_kind,
        attachment_json=attachment_json,
    )
    if new_id is None:
        flash("Could not save that post. Try again in a moment.", "danger")
    else:
        flash("Posted.", "success")
    return redirect(next_url)


def _safe_float(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_int(v):
    try:
        if v is None or v == "":
            return None
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _build_attachment_payload(kind, form):
    """
    Turn the composer's hidden 'atk_*' fields into a small, tenant-checked
    payload dict suitable for storing in community_posts.attachment_json.
    Returns None if the payload doesn't belong to the current user.
    """
    account = (form.get("atk_account") or "").strip()
    if not _account_allowed(current_user.id, current_user.username, account):
        return None

    if kind == "strategy":
        sym = (form.get("atk_symbol") or "").strip().upper()
        strat = (form.get("atk_strategy") or "").strip()
        if not sym or not strat:
            return None
        prof = get_user_profile(current_user.id)
        acct_label = account if prof.get("show_account_names_on_published") else "Account"
        return {
            "kind": "strategy",
            "account": acct_label,
            "symbol": sym,
            "strategy": strat,
            "first_open": (form.get("atk_first_open") or "").strip()[:10],
            "last_activity": (form.get("atk_last_activity") or "").strip()[:10],
            "leg_count": _safe_int(form.get("atk_leg_count")),
            "open_legs": _safe_int(form.get("atk_open_legs")),
            "closed_legs": _safe_int(form.get("atk_closed_legs")),
            "total_pnl": _safe_float(form.get("atk_total_pnl")),
        }

    if kind == "transaction":
        sym = (form.get("atk_symbol") or "").strip().upper()
        if not sym:
            return None
        prof = get_user_profile(current_user.id)
        acct_label = account if prof.get("show_account_names_on_published") else "Account"
        return {
            "kind": "transaction",
            "account": acct_label,
            "symbol": sym,
            "trade_symbol": (form.get("atk_trade_symbol") or "").strip()[:64],
            "instrument_type": (form.get("atk_instrument_type") or "").strip()[:32],
            "trade_date": (form.get("atk_trade_date") or "").strip()[:10],
            "action": (form.get("atk_action") or "").strip()[:32],
            "action_label": (form.get("atk_action_label") or "").strip()[:48],
            "quantity": _safe_float(form.get("atk_quantity")),
            "price": _safe_float(form.get("atk_price")),
            "amount": _safe_float(form.get("atk_amount")),
        }

    return None


_TXN_ACTION_LABELS = {
    "equity_buy": "Bought",
    "equity_sell": "Sold",
    "equity_sell_short": "Sold short",
    "option_sell_to_open": "Sold to open",
    "option_buy_to_close": "Bought to close",
    "option_buy_to_open": "Bought to open",
    "option_sell_to_close": "Sold to close",
    "option_expired": "Expired",
    "option_assigned": "Assigned",
    "option_exercised": "Exercised",
    "dividend": "Dividend",
    "margin_interest": "Margin interest",
    "credit_interest": "Credit interest",
    "adr_fee": "ADR fee",
}


def _iso_date(v):
    if v is None:
        return ""
    try:
        import pandas as _pd
        if _pd.isna(v):
            return ""
    except Exception:
        pass
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            return ""
    return str(v)[:10]


def _num_or_none(v):
    """Pandas-safe float cast. Treats NaN/None as None."""
    if v is None:
        return None
    try:
        import pandas as _pd
        if _pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _num_or_zero(v):
    x = _num_or_none(v)
    return 0.0 if x is None else x


@app.route("/community/my-trades")
@login_required
def community_my_trades():
    """
    JSON API backing the composer's 'Attach a trade' picker.

    Returns three buckets of the logged-in user's activity, each newest-first:
      - strategies:   rollups by (account, symbol, strategy)
      - legs:         individual trade groups (contracts / sessions) from
                      mart_weekly_trades — these can attach a trade snapshot
      - transactions: individual rows from stg_history (buys, sells,
                      assignments, expires, etc.)

    SECURITY: this endpoint MUST stay tenant-scoped — see
    .cursor/rules/bigquery-tenant-isolation.mdc. We scope by account both in
    SQL ({account_filter}) and in Python via _filter_df_by_accounts.
    """
    try:
        return _community_my_trades_impl()
    except Exception as exc:
        app.logger.exception("community_my_trades unexpected failure: %s", exc)
        return jsonify({
            "error": "server_error",
            "detail": str(exc),
            "strategies": [], "legs": [], "transactions": [],
        }), 200


def _community_my_trades_impl():
    from app.routes import (
        _user_account_list,
        _account_sql_and,
        _filter_df_by_accounts,
    )

    user_accounts = _user_account_list()
    acct_sql = _account_sql_and(user_accounts)

    # ---- Legs (individual trade groups) --------------------------------
    legs_sql = f"""
        SELECT
          account, symbol, strategy, trade_symbol,
          open_date, close_date, status,
          total_pnl, current_unrealized_pnl, num_trades
        FROM `ccwj-dbt.analytics.mart_weekly_trades`
        WHERE (open_date IS NOT NULL OR close_date IS NOT NULL)
          {acct_sql}
        ORDER BY COALESCE(close_date, open_date) DESC NULLS LAST,
                 open_date DESC
        LIMIT 120
    """

    # ---- Strategies (rollup across legs) -------------------------------
    strategies_sql = f"""
        SELECT
          account, symbol, strategy,
          MIN(open_date) AS first_open,
          MAX(COALESCE(close_date, open_date)) AS last_activity,
          SUM(CASE WHEN status = 'Closed' THEN COALESCE(total_pnl, 0) ELSE 0 END)
            AS closed_pnl,
          SUM(CASE WHEN status = 'Open' THEN COALESCE(current_unrealized_pnl, 0) ELSE 0 END)
            AS open_pnl,
          COUNT(*) AS leg_count,
          COUNTIF(status = 'Open')   AS open_legs,
          COUNTIF(status = 'Closed') AS closed_legs
        FROM `ccwj-dbt.analytics.mart_weekly_trades`
        WHERE account IS NOT NULL
          AND symbol IS NOT NULL
          AND strategy IS NOT NULL
          AND (open_date IS NOT NULL OR close_date IS NOT NULL)
          {acct_sql}
        GROUP BY account, symbol, strategy
        ORDER BY last_activity DESC NULLS LAST
        LIMIT 80
    """

    # ---- Transactions (stg_history rows) -------------------------------
    txn_sql = f"""
        SELECT
          account, trade_date, action, action_raw,
          underlying_symbol, trade_symbol, instrument_type,
          quantity, price, amount
        FROM `ccwj-dbt.analytics.stg_history`
        WHERE trade_date IS NOT NULL
          AND action NOT IN ('dividend', 'margin_interest',
                             'credit_interest', 'adr_fee', 'other')
          {acct_sql}
        ORDER BY trade_date DESC, ABS(COALESCE(amount, 0)) DESC
        LIMIT 250
    """

    client = get_bigquery_client()

    def _run(sql):
        try:
            return client.query(sql).to_dataframe()
        except Exception as exc:
            app.logger.warning("community_my_trades BQ query failed: %s", exc)
            return None

    legs_df = _run(legs_sql)
    strategies_df = _run(strategies_sql)
    txn_df = _run(txn_sql)

    if legs_df is None and strategies_df is None and txn_df is None:
        return jsonify({
            "error": "query_failed",
            "strategies": [], "legs": [], "transactions": [],
        }), 200

    try:
        published_fps = get_published_trade_fingerprints(current_user.id)
    except Exception:
        published_fps = set()

    # -- Legs --
    legs = []
    if legs_df is not None:
        legs_df = _filter_df_by_accounts(legs_df, user_accounts)
        for _, row in legs_df.iterrows():
            status = str(row.get("status") or "")
            num_trades_raw = _num_or_none(row.get("num_trades"))
            num_trades = int(num_trades_raw) if num_trades_raw is not None else 0
            if status.lower() == "open" and num_trades == 0:
                continue
            acct = str(row.get("account") or "")
            sym = str(row.get("symbol") or "")
            strat = str(row.get("strategy") or "")
            tsym = str(row.get("trade_symbol") or "")
            open_d = _iso_date(row.get("open_date"))
            close_d = _iso_date(row.get("close_date"))
            total_pnl = _num_or_none(row.get("total_pnl"))
            unreal = _num_or_none(row.get("current_unrealized_pnl"))
            display_pnl = total_pnl if status == "Closed" else unreal
            fp = trade_fingerprint(
                current_user.id, acct, sym, tsym, open_d, close_d, strat,
            )
            legs.append({
                "fingerprint": fp,
                "account": acct,
                "symbol": sym,
                "strategy": strat,
                "trade_symbol": tsym,
                "open_date": open_d,
                "close_date": close_d,
                "status": status,
                "display_pnl": display_pnl,
                "already_shared": fp in published_fps,
            })

    # -- Strategies --
    strategies = []
    if strategies_df is not None:
        strategies_df = _filter_df_by_accounts(strategies_df, user_accounts)
        for _, row in strategies_df.iterrows():
            acct = str(row.get("account") or "")
            sym = str(row.get("symbol") or "")
            strat = str(row.get("strategy") or "")
            open_legs = int(_num_or_zero(row.get("open_legs")))
            closed_legs = int(_num_or_zero(row.get("closed_legs")))
            total_pnl = _num_or_zero(row.get("closed_pnl")) + _num_or_zero(row.get("open_pnl"))
            strategies.append({
                "account": acct,
                "symbol": sym,
                "strategy": strat,
                "first_open": _iso_date(row.get("first_open")),
                "last_activity": _iso_date(row.get("last_activity")),
                "leg_count": int(_num_or_zero(row.get("leg_count"))),
                "open_legs": open_legs,
                "closed_legs": closed_legs,
                "total_pnl": total_pnl,
            })

    # -- Transactions --
    transactions = []
    if txn_df is not None:
        txn_df = _filter_df_by_accounts(txn_df, user_accounts)
        for _, row in txn_df.iterrows():
            action = str(row.get("action") or "")
            acct = str(row.get("account") or "")
            sym = str(row.get("underlying_symbol") or "")
            tsym = str(row.get("trade_symbol") or "")
            inst = str(row.get("instrument_type") or "")
            qty = _num_or_zero(row.get("quantity"))
            price = _num_or_zero(row.get("price"))
            amount = _num_or_zero(row.get("amount"))
            transactions.append({
                "account": acct,
                "trade_date": _iso_date(row.get("trade_date")),
                "action": action,
                "action_label": _TXN_ACTION_LABELS.get(action, action.replace("_", " ").title() or "—"),
                "symbol": sym,
                "trade_symbol": tsym,
                "instrument_type": inst,
                "quantity": qty,
                "price": price,
                "amount": amount,
            })

    return jsonify({
        "strategies": strategies,
        "legs": legs,
        "transactions": transactions,
    })


@app.route("/community/post/<int:post_id>/delete", methods=["POST"])
@login_required
def community_post_delete(post_id):
    blocked = demo_block_writes("removing community posts")
    if blocked:
        return blocked
    next_url = _safe_redirect_target(request.form.get("next")) or url_for("community")
    row = get_post(post_id)
    if not row or int(row["user_id"]) != int(current_user.id):
        flash("That post is not yours to remove.", "warning")
        return redirect(next_url)
    if not delete_post(current_user.id, post_id):
        flash("Could not remove the post.", "danger")
    else:
        flash("Post deleted.", "info")
    return redirect(next_url)


@app.route("/community/post/<int:post_id>/visibility", methods=["POST"])
@login_required
def community_post_visibility(post_id):
    blocked = demo_block_writes("changing post visibility")
    if blocked:
        return blocked
    next_url = _safe_redirect_target(request.form.get("next")) or url_for("community")
    row = get_post(post_id)
    if not row or int(row["user_id"]) != int(current_user.id):
        flash("That post is not yours.", "warning")
        return redirect(next_url)
    new_vis = (request.form.get("visibility") or "").strip().lower()
    if not update_post_visibility(current_user.id, post_id, new_vis):
        flash("Could not update visibility.", "danger")
    else:
        flash("Visibility updated.", "success")
    return redirect(next_url)


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
    posts = decode_post_attachments(
        list_posts_by_user(target_id, current_user.id, limit=80)
    ) if can_see else []
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
        posts=posts,
        show_account_names=show_names,
        published_count=published_count,
    )


@app.route("/u/<username>/follow", methods=["POST"])
@login_required
def follow_trader(username):
    blocked = demo_block_writes("following other traders")
    if blocked:
        return blocked
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
    flash(f"You’re following @{row['username']}. Their shared trades will appear in your feed.", "success")
    nxt = _safe_redirect_target(request.form.get("next"))
    return redirect(nxt or url_for("public_trader_profile", username=username))


@app.route("/u/<username>/unfollow", methods=["POST"])
@login_required
def unfollow_trader(username):
    blocked = demo_block_writes("changing follow lists")
    if blocked:
        return blocked
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
    blocked = demo_block_writes("publishing trades to the community")
    if blocked:
        return blocked
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
        flash("Shared trade removed.", "success")
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
        flash("Set your visibility to Followers or Public first: Profile → Community.", "warning")
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
    flash("Shared. Followers will see this trade in their feed.", "success")
    return redirect(nxt)
