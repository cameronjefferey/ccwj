"""
Wealth — daily account value over time, broken into cash / equity / options.

Process-first framing: the page exists so a trader can see how their
balance has moved, not as a "how rich am I" P&L hero. The hero shows
allocation today, the chart shows the time series of components, and a
breakdown panel surfaces dividends, interest, and fees so the user can
see how much of the change was income vs. mark-to-market.

All BigQuery reads go through ``_tenant_sql_and`` (SQL-level tenant_id
scoping) and every DataFrame is then passed through
``_filter_df_by_tenant_ids`` for defense-in-depth — see
``.cursor/rules/bigquery-tenant-isolation.mdc`` and
``docs/V2_TENANT_KEY_DESIGN.md``.
"""
import json
from datetime import date, timedelta

import pandas as pd
from flask import redirect, render_template, request, url_for
from flask_login import current_user, login_required

from app import app
from app.bigquery_client import get_bigquery_client
from app.models import is_admin
from app.routes import (
    _norm_account_label,
    _tenants_for_scope,
    _tenant_sql_and,
    _filter_df_by_tenant_ids,
    _redirect_if_no_accounts,
    _user_account_list,
)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

WEALTH_DAILY_QUERY = """
SELECT
    tenant_id,
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
    cumulative_fees,
    net_deposit_today,
    cumulative_net_deposits
FROM `ccwj-dbt.analytics.mart_wealth_daily`
WHERE 1=1 {tenant_filter}
  AND date >= @start_date
  AND date <= @end_date
ORDER BY date, account
"""

# Deploy-order fallback: the /wealth code and the mart_wealth_daily rebuild
# don't necessarily land together (the app can redeploy before the BigQuery
# CI rebuilds the mart). Selecting a not-yet-materialized column 400s the
# whole page, so if the transfer columns are missing we retry without them
# and treat net deposits as 0 (toggle becomes a graceful no-op).
WEALTH_DAILY_QUERY_LEGACY = """
SELECT
    tenant_id,
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
WHERE 1=1 {tenant_filter}
  AND date >= @start_date
  AND date <= @end_date
ORDER BY date, account
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _match_linked_account(user_accounts, requested: str):
    """If ``requested`` identifies one of the user's linked accounts (exact or
    case/whitespace-insensitive after normalization), return the canonical
    Postgres label so SQL IN (...) agrees with Schwab/sync naming."""
    if not user_accounts or not (requested or "").strip():
        return None
    want = _norm_account_label(requested).lower()
    if not want:
        return None
    for a in user_accounts:
        if _norm_account_label(a).lower() == want:
            return a
    return None


def _collapse_wealth_daily_duplicate_grain(df: pd.DataFrame) -> pd.DataFrame:
    """Keep one Wealth mart row per ``(tenant_id, account, date)`` before
    chart/summary sums.

    ``_filter_df_by_accounts`` Stage 0/1 leniency can leave **both** legacy
    ``user_id IS NULL`` and populated-ID rows for the same account/day.
    ``groupby(\"date\").sum()`` would then double cash/equity (visible as a
    ~2× spike mid-range while hero matches the deduped last day).

    The grain key leads with ``tenant_id`` so multiple physical accounts that
    share the same display ``account`` label (e.g. 5 SnapTrade/Schwab accounts
    all labeled "Schwab Account") are NOT collapsed into a single row — doing
    so would silently drop 4 accounts' wealth from every chart/summary.
    """
    if df is None or df.empty:
        return df
    if not {"account", "date"}.issubset(df.columns):
        return df
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    stab = "__r_i__"
    out[stab] = range(len(out))
    has_tenant = "tenant_id" in out.columns and out["tenant_id"].notna().any()
    ks = (["tenant_id"] if has_tenant else []) + ["account", "date"]
    if "user_id" in out.columns:
        uid_col = pd.to_numeric(out["user_id"], errors="coerce")
        out["__prefer_uid__"] = uid_col.notna().astype(int)
        out = out.sort_values(
            by=ks + ["__prefer_uid__", "user_id", stab],
            ascending=([True] * len(ks)) + [False, True, True],
            na_position="last",
        ).drop_duplicates(subset=ks, keep="first").drop(columns=["__prefer_uid__"])
    else:
        out = out.sort_values(by=ks + [stab]).drop_duplicates(subset=ks, keep="last")
    return out.drop(columns=[stab]).reset_index(drop=True)


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


def _net_deposits_by_date(df):
    """Return a date-indexed Series of cumulative net deposits (summed
    across accounts in scope), rebased so the FIRST day in the window is
    0. Rebasing to the window start means a deposit that happened BEFORE
    the window is a constant offset that cancels out (it doesn't distort
    the shape), while a deposit INSIDE the window shows up as the step it
    really is. Returns ``None`` when the mart predates transfer capture
    (column missing) so callers degrade to a no-op.
    """
    if df is None or df.empty or "cumulative_net_deposits" not in df.columns:
        return None
    cum = (
        df.groupby("date", as_index=False)
        .agg(cumulative_net_deposits=("cumulative_net_deposits", "sum"))
        .sort_values("date")
    )
    if cum.empty:
        return None
    base = float(cum["cumulative_net_deposits"].iloc[0])
    cum["net_since_start"] = cum["cumulative_net_deposits"].astype(float) - base
    return cum.set_index("date")["net_since_start"]


def _build_chart_payload(df, exclude_transfers=False):
    """Aggregate per-account daily rows into the JSON the chart eats.

    Sums across accounts inside the selected scope so multi-account
    users see a single combined wealth curve. Also emits a
    deposit-adjusted line (``account_value_ex_transfers``) and the
    cumulative net-deposit series so the page's "exclude deposits &
    withdrawals" toggle can show the balance driven by market moves +
    income only.
    """
    empty = {
        "dates": [], "cash": [], "equity": [], "options": [],
        "account_value": [], "account_value_ex_transfers": [],
        "net_deposits": [], "has_transfers": False,
        "exclude_transfers": bool(exclude_transfers),
    }
    if df is None or df.empty:
        return empty

    by_date = df.groupby("date", as_index=False).agg(
        account_value=("account_value", "sum"),
        cash_value=("cash_value", "sum"),
        equity_value=("equity_value", "sum"),
        option_value=("option_value", "sum"),
    ).sort_values("date")

    net_dep = _net_deposits_by_date(df)
    if net_dep is not None:
        net_series = [float(net_dep.get(d, 0.0) or 0.0) for d in by_date["date"]]
    else:
        net_series = [0.0] * len(by_date)
    has_transfers = any(abs(v) > 0.005 for v in net_series)

    account_value = [float(v) for v in by_date["account_value"]]
    adjusted = [av - nd for av, nd in zip(account_value, net_series)]

    return {
        # .date() first: pandas Timestamps isoformat to "2026-06-09T00:00:00",
        # which rendered raw on the chart's x-axis. Plain date objects have
        # no .date attribute and fall through to isoformat().
        "dates": [
            (d.date().isoformat() if hasattr(d, "date")
             else d.isoformat() if hasattr(d, "isoformat") else str(d))
            for d in by_date["date"]
        ],
        "cash": [round(float(v), 2) for v in by_date["cash_value"]],
        "equity": [round(float(v), 2) for v in by_date["equity_value"]],
        "options": [round(float(v), 2) for v in by_date["option_value"]],
        "account_value": [round(v, 2) for v in account_value],
        "account_value_ex_transfers": [round(v, 2) for v in adjusted],
        "net_deposits": [round(v, 2) for v in net_series],
        "has_transfers": has_transfers,
        "exclude_transfers": bool(exclude_transfers),
    }


def _build_summary(df, exclude_transfers=False):
    """Hero numbers for the top of the page.

    Latest row is "today" (or last snapshot day). Prior rows are used
    to compute change-over-time. Returns ``None`` when the frame is
    empty so the template can render an empty-state card.

    When ``exclude_transfers`` is set, the change-over-time figures are
    computed on a deposit-adjusted value (account_value minus net
    external cash flow since the reference day) so a $10k deposit doesn't
    read as a $10k "gain". The point-in-time allocation numbers
    (account_value / cash / equity / option) are always the real balance.
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

    # Cumulative net deposits per day, rebased to the window start (0 on
    # the first day). Merged onto by_date so each reference row carries the
    # net cash moved between that day and the latest day.
    net_dep = _net_deposits_by_date(df)
    if net_dep is not None:
        by_date["net_dep"] = [float(net_dep.get(d, 0.0) or 0.0) for d in by_date["date"]]
    else:
        by_date["net_dep"] = 0.0

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
        if exclude_transfers:
            # Remove the external cash moved between the reference day and
            # now, so the delta reflects market + income only.
            diff -= float(latest["net_dep"]) - float(row["net_dep"])
        pct = (diff / base * 100) if base else None
        return {"abs": round(diff, 2), "pct": round(pct, 2) if pct is not None else None}

    one_month_ago = today - timedelta(days=30) if hasattr(today, "__sub__") else None
    three_months_ago = today - timedelta(days=90) if hasattr(today, "__sub__") else None

    # Net external cash flow across the whole window (latest − first,
    # both rebased so first = 0 → this is simply the latest value).
    net_deposits_in_range = round(float(latest["net_dep"]) - float(first["net_dep"]), 2)

    return {
        # Human date, not isoformat — pandas Timestamps carry a midnight
        # time component that rendered as "2026-08-08T00:00:00" in the hero.
        "as_of": today.strftime("%b %-d, %Y") if hasattr(today, "strftime") else str(today),
        "account_value": round(float(latest["account_value"]), 2),
        "cash_value": round(float(latest["cash_value"]), 2),
        "equity_value": round(float(latest["equity_value"]), 2),
        "option_value": round(float(latest["option_value"]), 2),
        "change_in_range": _change(first),
        "change_30d": _change(_at_or_before(one_month_ago)) if one_month_ago else None,
        "change_90d": _change(_at_or_before(three_months_ago)) if three_months_ago else None,
        "net_deposits_in_range": net_deposits_in_range,
        "has_transfers": abs(net_deposits_in_range) > 0.005,
        "exclude_transfers": bool(exclude_transfers),
    }


def _build_income_panel(df):
    """Cumulative dividends / interest / fees across the selected
    range. Uses the start-of-range and end-of-range cumulative columns
    on the mart so the totals are exactly the change inside the
    window — no need to re-aggregate stg_history in Python.
    """
    if df is None or df.empty:
        return None

    # Each (tenant_id, account, user_id) carries its own cumulative streak;
    # sum the latest row per key and subtract the first row per same to get
    # range totals. ``tenant_id`` leads so colliding "Schwab Account" labels
    # don't fuse 5 physical accounts' cumulative dividend/interest streaks.
    if "tenant_id" in df.columns and df["tenant_id"].notna().any():
        keys = ["tenant_id", "account", "user_id"] if "user_id" in df.columns else ["tenant_id", "account"]
    elif "user_id" in df.columns:
        keys = ["account", "user_id"]
    else:
        keys = ["account"]
    have_transfers = "cumulative_net_deposits" in df.columns
    agg_kwargs = dict(
        first_div=("cumulative_dividends", "first"),
        last_div=("cumulative_dividends", "last"),
        first_int=("cumulative_interest_net", "first"),
        last_int=("cumulative_interest_net", "last"),
        first_fee=("cumulative_fees", "first"),
        last_fee=("cumulative_fees", "last"),
    )
    if have_transfers:
        agg_kwargs["first_dep"] = ("cumulative_net_deposits", "first")
        agg_kwargs["last_dep"] = ("cumulative_net_deposits", "last")
    per_key = (
        df.sort_values("date")
        .groupby(keys, as_index=False)
        .agg(**agg_kwargs)
    )
    div = float((per_key["last_div"] - per_key["first_div"]).sum())
    interest = float((per_key["last_int"] - per_key["first_int"]).sum())
    fees = float((per_key["last_fee"] - per_key["first_fee"]).sum())
    net_deposits = (
        float((per_key["last_dep"] - per_key["first_dep"]).sum())
        if have_transfers else 0.0
    )

    return {
        "dividends": round(div, 2),
        "interest_net": round(interest, 2),
        "fees": round(fees, 2),
        "net_deposits": round(net_deposits, 2),
        "has_transfers": abs(net_deposits) > 0.005,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
# The standalone /wealth page was merged into /accounts in the Aug 2026
# surface audit: one "Accounts" surface with two views — Performance (the
# accounts_page P&L view) and Value & composition (this module). /wealth
# 301s so bookmarks keep working; render_wealth_view() is called by the
# accounts endpoint when ?view=value.


@app.route("/wealth")
@login_required
def wealth():
    """Legacy URL — permanently moved to /accounts?view=value."""
    args = {"view": "value"}
    for key in ("account", "tenant", "range", "exclude_transfers"):
        val = (request.args.get(key) or "").strip()
        if val:
            args[key] = val
    return redirect(url_for("accounts", **args), code=301)


def render_wealth_view():
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
    selected_raw = (request.args.get("account") or "").strip()
    range_arg = request.args.get("range", "")
    # Toggle: strip the trader's own deposits / withdrawals out of the
    # change-over-time numbers and the chart's headline line so the page
    # shows growth from the market + income, not from money moved in/out.
    exclude_transfers = request.args.get("exclude_transfers") in ("1", "true", "on")

    selected_account = selected_raw
    if selected_raw and user_accounts is not None:
        matched = _match_linked_account(user_accounts, selected_raw)
        if matched is not None:
            selected_account = matched

    tenant_ids = _tenants_for_scope(selected_account or None)
    if selected_raw and user_accounts is not None:
        matched = _match_linked_account(user_accounts, selected_raw)
        wealth_no_match = matched is None
    elif selected_raw and user_accounts is None:
        wealth_no_match = tenant_ids == []
    else:
        wealth_no_match = False

    start_date, end_date = _resolve_range(range_arg, default_days=180)
    tenant_filter = _tenant_sql_and(tenant_ids)

    picker_accounts = sorted(user_accounts) if user_accounts else []

    context = {
        "title": "Accounts — Value & composition",
        "selected_account": selected_account,
        "selected_range": (range_arg or "180").lower(),
        "exclude_transfers": exclude_transfers,
        # Linked labels for the picker; admins also get names seen in BQ
        # once the query succeeds.
        "wealth_account_choices": picker_accounts,
        "accounts": picker_accounts,
        "wealth_no_match": wealth_no_match,
        "summary": None,
        "income_panel": None,
        "chart_json": json.dumps({
            "dates": [], "cash": [], "equity": [], "options": [],
            "account_value": [], "account_value_ex_transfers": [],
            "net_deposits": [], "has_transfers": False,
            "exclude_transfers": exclude_transfers,
        }),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "error": None,
    }

    try:
        df = None
        if not wealth_no_match:
            from google.cloud import bigquery
            client = get_bigquery_client()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                    bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
                ]
            )
            from app.query_cache import cached_query_df
            try:
                sql = WEALTH_DAILY_QUERY.format(tenant_filter=tenant_filter)
                df = cached_query_df(client, sql, job_config=job_config, label="wealth_daily")
            except Exception as col_exc:
                # Deploy-order safety: the transfer columns may not be
                # materialized yet. Fall back to the pre-transfer shape so
                # the page still renders (net deposits = 0, toggle no-op).
                app.logger.warning(
                    "Wealth net-deposit columns unavailable, using legacy query: %s",
                    col_exc,
                )
                sql = WEALTH_DAILY_QUERY_LEGACY.format(tenant_filter=tenant_filter)
                df = cached_query_df(client, sql, job_config=job_config, label="wealth_daily_legacy")
            # Defense-in-depth tenant filter on the DataFrame — even if a
            # SQL change ever drops the account_filter, this strips any
            # row whose ``user_id`` doesn't match the signed-in user.
            df = _filter_df_by_tenant_ids(df, tenant_ids)
            df = _collapse_wealth_daily_duplicate_grain(df)

        if df is None:
            df = pd.DataFrame()

        if df.empty:
            return render_template("wealth.html", **context)

        if (
            user_accounts is None
            and "account" in df.columns
            and df["account"].notna().any()
        ):
            admin_names = sorted(df["account"].dropna().unique().tolist())
            context["wealth_account_choices"] = admin_names
            context["accounts"] = admin_names

        context["summary"] = _build_summary(df, exclude_transfers)
        context["income_panel"] = _build_income_panel(df)
        context["chart_json"] = json.dumps(_build_chart_payload(df, exclude_transfers))
    except Exception as exc:
        app.logger.exception("Wealth page query failed: %s", exc)
        context["error"] = "Couldn't load wealth data. Try again in a moment."

    return render_template("wealth.html", **context)
