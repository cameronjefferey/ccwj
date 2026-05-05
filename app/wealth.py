"""
Wealth — daily account value over time, broken into cash / equity / options.

Process-first framing: the page exists so a trader can see how their
balance has moved, not as a "how rich am I" P&L hero. The hero shows
allocation today, the chart shows the time series of components, and a
breakdown panel surfaces dividends, interest, and fees so the user can
see how much of the change was income vs. mark-to-market.

All BigQuery reads go through ``_account_sql_and`` (SQL-level user_id
scoping) and every DataFrame is then passed through
``_filter_df_by_accounts`` for defense-in-depth — see
``.cursor/rules/bigquery-tenant-isolation.mdc`` and
``docs/USER_ID_TENANCY.md``.
"""
import json
from datetime import date, timedelta

from flask import render_template, request
from flask_login import current_user, login_required

from app import app
from app.bigquery_client import get_bigquery_client
from app.models import get_accounts_for_user, is_admin
from app.routes import (
    _account_sql_and,
    _filter_df_by_accounts,
    _redirect_if_no_accounts,
)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

WEALTH_DAILY_QUERY = """
SELECT
    account,
    user_id,
    date,
    account_value,
    cash_value,
    equity_value,
    option_value,
    account_value_delta,
    dividend_today,
    interest_net_today,
    fees_today,
    cumulative_dividends,
    cumulative_interest_net,
    cumulative_fees
FROM `ccwj-dbt.analytics.mart_wealth_daily`
WHERE 1=1 {account_filter}
  AND date >= @start_date
  AND date <= @end_date
ORDER BY date, account
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _user_account_list():
    if is_admin(current_user.username):
        return None
    return get_accounts_for_user(current_user.id)


def _resolve_range(arg_value, default_days):
    """Parse a ?range=... query arg into (start, end) dates.

    Accepts ``"30"``/``"90"``/``"365"``/``"all"`` plus a sane default.
    Anything unparseable falls back to ``default_days``. End date is
    always today so charts include the latest snapshot.
    """
    end = date.today()
    raw = (arg_value or "").strip().lower()
    if raw == "all":
        # mart_wealth_daily covers ~5y of history at most; a 10y window
        # is comfortably larger than the data and avoids a special path.
        return end - timedelta(days=365 * 10), end
    if raw.isdigit():
        n = max(1, min(int(raw), 365 * 10))
        return end - timedelta(days=n), end
    return end - timedelta(days=default_days), end


def _build_chart_payload(df):
    """Aggregate per-account daily rows into the JSON the chart eats.

    Sums across accounts inside the selected scope so multi-account
    users see a single combined wealth curve. The page also exposes
    an account picker if they want to drill in.
    """
    if df is None or df.empty:
        return {"dates": [], "cash": [], "equity": [], "options": [], "account_value": []}

    by_date = df.groupby("date", as_index=False).agg(
        account_value=("account_value", "sum"),
        cash_value=("cash_value", "sum"),
        equity_value=("equity_value", "sum"),
        option_value=("option_value", "sum"),
    ).sort_values("date")

    return {
        "dates": [d.isoformat() if hasattr(d, "isoformat") else str(d) for d in by_date["date"]],
        "cash": [round(float(v), 2) for v in by_date["cash_value"]],
        "equity": [round(float(v), 2) for v in by_date["equity_value"]],
        "options": [round(float(v), 2) for v in by_date["option_value"]],
        "account_value": [round(float(v), 2) for v in by_date["account_value"]],
    }


def _build_summary(df):
    """Hero numbers for the top of the page.

    Latest row is "today" (or last snapshot day). Prior rows are used
    to compute change-over-time. Returns ``None`` when the frame is
    empty so the template can render an empty-state card.
    """
    if df is None or df.empty:
        return None

    by_date = df.groupby("date", as_index=False).agg(
        account_value=("account_value", "sum"),
        cash_value=("cash_value", "sum"),
        equity_value=("equity_value", "sum"),
        option_value=("option_value", "sum"),
    ).sort_values("date")

    if by_date.empty:
        return None

    latest = by_date.iloc[-1]
    first = by_date.iloc[0]

    # Pick a reference row roughly 30/90 days back from the latest
    # snapshot day. We don't index by calendar arithmetic because
    # snapshots are sparse (only days the cron ran), so we anchor on
    # the closest snapshot ON OR BEFORE the target date.
    def _at_or_before(target):
        cutoff = by_date[by_date["date"] <= target]
        return cutoff.iloc[-1] if not cutoff.empty else None

    today = latest["date"]

    def _change(row):
        if row is None:
            return None
        diff = float(latest["account_value"]) - float(row["account_value"])
        base = float(row["account_value"]) or 0.0
        pct = (diff / base * 100) if base else None
        return {"abs": round(diff, 2), "pct": round(pct, 2) if pct is not None else None}

    one_month_ago = today - timedelta(days=30) if hasattr(today, "__sub__") else None
    three_months_ago = today - timedelta(days=90) if hasattr(today, "__sub__") else None

    return {
        "as_of": today.isoformat() if hasattr(today, "isoformat") else str(today),
        "account_value": round(float(latest["account_value"]), 2),
        "cash_value": round(float(latest["cash_value"]), 2),
        "equity_value": round(float(latest["equity_value"]), 2),
        "option_value": round(float(latest["option_value"]), 2),
        "change_in_range": _change(first),
        "change_30d": _change(_at_or_before(one_month_ago)) if one_month_ago else None,
        "change_90d": _change(_at_or_before(three_months_ago)) if three_months_ago else None,
    }


def _build_income_panel(df):
    """Cumulative dividends / interest / fees across the selected
    range. Uses the start-of-range and end-of-range cumulative columns
    on the mart so the totals are exactly the change inside the
    window — no need to re-aggregate stg_history in Python.
    """
    if df is None or df.empty:
        return None

    # Each (account, user_id) carries its own cumulative streak; sum
    # the latest row per (account, user_id) and subtract the first row
    # per same to get range totals.
    if "user_id" in df.columns:
        keys = ["account", "user_id"]
    else:
        keys = ["account"]
    per_key = (
        df.sort_values("date")
        .groupby(keys, as_index=False)
        .agg(
            first_div=("cumulative_dividends", "first"),
            last_div=("cumulative_dividends", "last"),
            first_int=("cumulative_interest_net", "first"),
            last_int=("cumulative_interest_net", "last"),
            first_fee=("cumulative_fees", "first"),
            last_fee=("cumulative_fees", "last"),
        )
    )
    div = float((per_key["last_div"] - per_key["first_div"]).sum())
    interest = float((per_key["last_int"] - per_key["first_int"]).sum())
    fees = float((per_key["last_fee"] - per_key["first_fee"]).sum())

    return {
        "dividends": round(div, 2),
        "interest_net": round(interest, 2),
        "fees": round(fees, 2),
    }


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@app.route("/wealth")
@login_required
def wealth():
    """Daily account value with cash / equity / options breakdown.

    Process-first: the chart and hero exist so a user can see *how*
    their balance moved (composition, income, drawdowns), not as a
    competitive scoreboard. Schwab API sync only emits TRADE rows,
    and the export taxonomy doesn't tag deposits separately, so we
    deliberately avoid claiming a precise "organic vs deposits"
    number — see ``mart_wealth_daily.sql`` for the reasoning.
    """
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce

    user_accounts = _user_account_list()
    selected_account = (request.args.get("account") or "").strip()
    range_arg = request.args.get("range", "")

    effective_accounts = user_accounts
    if selected_account:
        if user_accounts is None:
            effective_accounts = [selected_account]
        else:
            effective_accounts = [a for a in user_accounts if a == selected_account] or user_accounts

    start_date, end_date = _resolve_range(range_arg, default_days=180)
    account_filter = _account_sql_and(effective_accounts)

    context = {
        "title": "Wealth",
        "selected_account": selected_account,
        "selected_range": (range_arg or "180").lower(),
        "accounts": [],
        "summary": None,
        "income_panel": None,
        "chart_json": json.dumps({
            "dates": [], "cash": [], "equity": [], "options": [], "account_value": []
        }),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "error": None,
    }

    try:
        from google.cloud import bigquery
        client = get_bigquery_client()
        sql = WEALTH_DAILY_QUERY.format(account_filter=account_filter)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        )
        df = client.query(sql, job_config=job_config).to_dataframe()
        # Defense-in-depth tenant filter on the DataFrame — even if a
        # SQL change ever drops the account_filter, this strips any
        # row whose ``user_id`` doesn't match the signed-in user.
        df = _filter_df_by_accounts(df, effective_accounts)

        if df.empty:
            return render_template("wealth.html", **context)

        # Account picker source: every account that has any wealth
        # data in the user-scoped result.
        if "account" in df.columns:
            context["accounts"] = sorted(df["account"].dropna().unique().tolist())

        context["summary"] = _build_summary(df)
        context["income_panel"] = _build_income_panel(df)
        context["chart_json"] = json.dumps(_build_chart_payload(df))
    except Exception as exc:
        app.logger.exception("Wealth page query failed: %s", exc)
        context["error"] = "Couldn't load wealth data. Try again in a moment."

    return render_template("wealth.html", **context)
