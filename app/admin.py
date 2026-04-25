"""
Admin tooling — impersonation + diagnostic audit page.

Two distinct features live here:

1. /admin/impersonate/<username>  +  /admin/impersonate/stop
       Lets a user listed in ADMIN_USERS log in as another user for support.
       Original admin id is stashed in flask.session under _impersonator_id
       so the /stop route can switch back without forcing a re-login.

2. /admin/audit
       Side-by-side view of every stg_history row, every leg in
       int_strategy_classification, and the rolled-up positions_summary
       row for a given (account, symbol). Lets the admin verify whether
       the app is showing the truth, without having to first impersonate.

Everything here is gated by app.models.is_admin(). Non-admins get a 404
(not 403) so the existence of these routes is not advertised.
"""
from __future__ import annotations

from functools import wraps

from flask import (
    abort,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_login import current_user, login_required, login_user

from app import app
from app.bigquery_client import get_bigquery_client
from app.models import User, is_admin


_IMPERSONATOR_KEY = "_impersonator_id"


def _fmt_money(v):
    """Format a number as `$1,234.56`. Handles None, NaN, ints, and big numbers
    without lapsing into scientific notation."""
    if v is None:
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "—"
    if f != f:  # NaN
        return "—"
    if f < 0:
        return "-${:,.2f}".format(-f)
    return "${:,.2f}".format(f)


# Make money() available in admin_audit.html (and any other admin template).
app.jinja_env.globals["money"] = _fmt_money


def _admin_only(view):
    """View decorator: require login + admin. 404 for everyone else so we
    don't leak the existence of admin routes to scrapers."""
    @wraps(view)
    @login_required
    def wrapped(*args, **kwargs):
        if not is_admin(current_user.username):
            abort(404)
        return view(*args, **kwargs)
    return wrapped


# ---------------------------------------------------------------------------
# Impersonation
# ---------------------------------------------------------------------------

@app.route("/admin/impersonate/<username>", methods=["POST", "GET"])
@_admin_only
def admin_impersonate(username):
    """
    Switch the current admin's session to act as ``username``.

    GET is allowed (so a support link from /admin/audit can be a regular
    anchor) but we still require an admin session — the gate is the
    ADMIN_USERS env var, not method.
    """
    target = User.get_by_username(username)
    if target is None:
        flash(f"No user named {username!r} found.", "danger")
        return redirect(url_for("admin_audit"))

    if target.id == current_user.id:
        flash("You can't impersonate yourself.", "warning")
        return redirect(url_for("admin_audit"))

    # Don't nest impersonations — always remember the *original* admin.
    if not session.get(_IMPERSONATOR_KEY):
        session[_IMPERSONATOR_KEY] = current_user.id

    admin_username = current_user.username
    login_user(target)
    session.modified = True
    app.logger.warning(
        "ADMIN IMPERSONATE: %s -> %s (target_id=%s)",
        admin_username, target.username, target.id,
    )
    flash(
        f"Impersonating @{target.username}. You see exactly what they see. "
        f"Click 'Stop impersonating' in the banner to switch back.",
        "info",
    )
    return redirect(url_for("weekly_review"))


@app.route("/admin/impersonate/stop", methods=["POST", "GET"])
@login_required
def admin_impersonate_stop():
    """Switch back to the admin session. Available to anyone with an active
    impersonation cookie — that's the whole point. We don't gate on is_admin
    because by the time this fires, current_user is the *target* user."""
    impersonator_id = session.pop(_IMPERSONATOR_KEY, None)
    if impersonator_id is None:
        flash("Not currently impersonating anyone.", "info")
        return redirect(url_for("weekly_review"))

    admin = User.get_by_id(int(impersonator_id))
    if admin is None or not is_admin(admin.username):
        # Defensive: original admin row went away or lost admin rights —
        # log them out entirely rather than silently restoring a stranger.
        from flask_login import logout_user
        logout_user()
        session.clear()
        flash("Stopped impersonating. Please sign back in.", "warning")
        return redirect(url_for("login"))

    impersonated_username = current_user.username
    login_user(admin)
    session.modified = True
    app.logger.warning(
        "ADMIN IMPERSONATE STOP: %s <- %s",
        admin.username, impersonated_username,
    )
    flash(f"Stopped impersonating @{impersonated_username}.", "success")
    return redirect(url_for("admin_audit"))


# ---------------------------------------------------------------------------
# Audit / diagnostic page
# ---------------------------------------------------------------------------

# All BigQuery probes pin location to match dbt/profiles.yml
import os
_BQ_LOC = os.environ.get("BQ_LOCATION", "US")


def _bq_run(sql, params=None):
    """Run a BigQuery query and return a list of dicts. params is a dict of
    {name: value} mapped to @name placeholders. Returns [] on any error so
    one missing model never breaks the whole audit page."""
    from google.cloud.bigquery import (
        QueryJobConfig,
        ScalarQueryParameter,
    )
    try:
        client = get_bigquery_client()
        cfg = None
        if params:
            qparams = []
            for name, value in params.items():
                if isinstance(value, int):
                    qparams.append(ScalarQueryParameter(name, "INT64", value))
                else:
                    qparams.append(ScalarQueryParameter(name, "STRING", value))
            cfg = QueryJobConfig(query_parameters=qparams)
        job = client.query(sql, job_config=cfg, location=_BQ_LOC)
        return [dict(row.items()) for row in job.result()]
    except Exception as exc:
        app.logger.warning("admin_audit BQ query failed: %s", exc)
        return [{"_error": str(exc)}]


@app.route("/admin/audit", methods=["GET"])
@_admin_only
def admin_audit():
    """
    Diagnostic page for one (account, symbol) pair.

    Pulls three slices in parallel-ish (sequential here for simplicity):
      * stg_history        — every transaction row, raw
      * int_strategy_classification — every grouped leg + status + P&L
      * positions_summary  — the rolled-up row the app renders

    Also shows which Flask user(s) have ``account`` linked in user_accounts
    so you can offer an Impersonate link without a username search.
    """
    account = (request.args.get("account") or "").strip()
    symbol  = (request.args.get("symbol") or "").strip().upper()

    transactions = []
    legs = []
    summary_rows = []
    linked_users = []
    by_kind_rows = []

    if account and symbol:
        transactions = _bq_run(
            """
            SELECT trade_date, action, action_raw, underlying_symbol,
                   trade_symbol, instrument_type, quantity, price, amount,
                   fees, description
            FROM `ccwj-dbt.analytics.stg_history`
            WHERE account = @account
              AND underlying_symbol = @symbol
            ORDER BY trade_date, trade_symbol
            """,
            {"account": account, "symbol": symbol},
        )
        legs = _bq_run(
            """
            SELECT account, symbol, strategy, trade_symbol, trade_group_type,
                   direction, status, open_date, close_date, days_in_trade,
                   num_trades, total_pnl, net_cash_flow,
                   premium_received, premium_paid, close_type
            FROM `ccwj-dbt.analytics.int_strategy_classification`
            WHERE account = @account
              AND symbol = @symbol
            ORDER BY open_date, trade_symbol
            """,
            {"account": account, "symbol": symbol},
        )
        summary_rows = _bq_run(
            """
            SELECT *
            FROM `ccwj-dbt.analytics.positions_summary`
            WHERE account = @account
              AND symbol = @symbol
            """,
            {"account": account, "symbol": symbol},
        )
        # Higher-level: equity vs option P&L for this (account, symbol).
        # We pull it directly from int_strategy_classification rather than
        # re-aggregating positions_summary so we can split by trade_group_type.
        by_kind_rows = _bq_run(
            """
            SELECT
                CASE trade_group_type
                    WHEN 'equity_session'  THEN 'Equity'
                    WHEN 'option_contract' THEN 'Options'
                    ELSE trade_group_type
                END AS kind,
                IFNULL(SUM(total_pnl), 0)                                       AS total_pnl,
                IFNULL(SUM(IF(status = 'Closed', total_pnl, 0)), 0)             AS realized_pnl,
                IFNULL(SUM(IF(status = 'Open',   total_pnl, 0)), 0)             AS unrealized_pnl,
                SUM(IFNULL(premium_received, 0))                                AS premium_received,
                SUM(ABS(IFNULL(premium_paid, 0)))                               AS premium_paid,
                SUM(IFNULL(net_cash_flow, 0))                                   AS net_cash_flow,
                COUNT(*)                                                        AS num_legs,
                COUNTIF(status = 'Open')                                        AS num_open,
                COUNTIF(status = 'Closed')                                      AS num_closed,
                COUNTIF(is_winner AND status = 'Closed')                        AS num_winners,
                COUNTIF(NOT is_winner AND status = 'Closed')                    AS num_losers
            FROM `ccwj-dbt.analytics.int_strategy_classification`
            WHERE account = @account
              AND symbol  = @symbol
            GROUP BY kind
            ORDER BY kind
            """,
            {"account": account, "symbol": symbol},
        )

    if account:
        try:
            from app.db import fetch_all
            linked_users = fetch_all(
                """SELECT u.id, u.username
                   FROM user_accounts ua JOIN users u ON u.id = ua.user_id
                   WHERE ua.account_name = %s
                   ORDER BY u.username""",
                (account,),
            )
        except Exception as exc:
            app.logger.warning("admin_audit linked_users lookup failed: %s", exc)

    return render_template(
        "admin_audit.html",
        title="Admin: position audit",
        account=account,
        symbol=symbol,
        transactions=transactions,
        legs=legs,
        summary_rows=summary_rows,
        linked_users=linked_users,
        by_kind_rows=by_kind_rows,
    )
