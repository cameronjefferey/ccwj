"""
Wealth — high-level "how is my net worth doing" page.

Process-first product, but this is the *context* layer: a wealth dashboard
that answers the boring-but-loved questions:

  - What am I worth right now?
  - How has that changed over time?
  - How does it compare to just buying SPY?
  - Of the dollars I have, how much did I deposit vs. how much did the
    account grow?
  - What's my allocation today vs. a year ago?

Reads from `mart_wealth_daily` and `stg_daily_prices`.  No heavy
aggregation in Python — Flask just shapes data for Chart.js.

Tenancy: every BigQuery read is scoped by user accounts in SQL AND
filtered again client-side via _filter_df_by_accounts before render.
See .cursor/rules/bigquery-tenant-isolation.mdc.
"""
from __future__ import annotations

import json
from datetime import date, timedelta

import pandas as pd
from flask import render_template, request
from flask_login import login_required, current_user

from app import app
from app.bigquery_client import get_bigquery_client
from app.models import get_accounts_for_user, is_admin
from app.routes import _filter_df_by_accounts


# ------------------------------------------------------------------
# SQL (account-scoped via {account_filter})
# ------------------------------------------------------------------

WEALTH_DAILY_QUERY = """
SELECT
    account,
    date,
    account_value,
    equity_value,
    option_value,
    cash_value,
    cumulative_options_pnl,
    cumulative_dividends,
    cumulative_equity_net_flow,
    cumulative_trading_cashflow,
    implied_contributions,
    implied_gains
FROM `ccwj-dbt.analytics.mart_wealth_daily`
WHERE date IS NOT NULL
  {account_filter}
ORDER BY account, date
"""

# SPY/QQQ daily closes for benchmark overlay.  No account scoping needed:
# stg_daily_prices is keyed on (account, symbol, date) where account is
# an artifact of the price-fetch pipeline, not a tenant column.  We
# de-dupe to one close per (symbol, date) in SQL.
BENCHMARK_QUERY = """
SELECT symbol, date, ANY_VALUE(close_price) AS close_price
FROM `ccwj-dbt.analytics.stg_daily_prices`
WHERE symbol IN ('SPY', 'QQQ')
  AND close_price IS NOT NULL AND close_price > 0
  AND date >= @start_date
GROUP BY symbol, date
ORDER BY symbol, date
"""


def _account_sql_and(accounts):
    """AND account IN (...) clause for queries that already have a WHERE.

    accounts=None means admin; emit no filter.
    accounts=[] means a logged-in user with zero accounts; force empty.
    """
    if accounts is None:
        return ""
    if not accounts:
        return "AND 1 = 0"
    quoted = ", ".join(f"'{a.replace(chr(39), chr(39)+chr(39))}'" for a in accounts)
    return f"AND account IN ({quoted})"


def _user_accounts_for_request(selected_account: str):
    """Resolve user accounts for this request, honoring the optional
    single-account focus from the ?account= query string."""
    if is_admin(current_user.username):
        base = None
    else:
        base = get_accounts_for_user(current_user.id)
    if selected_account:
        if base is None:
            return [selected_account]
        return [a for a in base if a == selected_account] or base
    return base


# ------------------------------------------------------------------
# Pure helpers — no BQ, easy to reason about
# ------------------------------------------------------------------

def _delta_pct(now: float, then: float):
    """Percent change as a fraction (0.18 = +18%).  None if base is 0/None."""
    if then is None or now is None:
        return None
    if then == 0:
        return None
    return (now - then) / then


def _value_on_or_before(daily_total: pd.DataFrame, target: date):
    """Pick the most recent (date, total_value) row with date <= target.
    Returns None if no such row.  daily_total has columns
    ('date', 'account_value')."""
    if daily_total.empty:
        return None
    mask = daily_total["date"] <= pd.Timestamp(target).date() \
           if hasattr(daily_total["date"].iloc[0], "year") \
           else daily_total["date"] <= target
    sub = daily_total[mask]
    if sub.empty:
        return None
    return float(sub.iloc[-1]["account_value"])


def _build_hero(daily_total: pd.DataFrame):
    """Compute hero number + period deltas from the all-accounts daily series."""
    if daily_total.empty:
        return None

    today = daily_total.iloc[-1]
    now_value = float(today["account_value"])
    now_date = today["date"]
    if hasattr(now_date, "to_pydatetime"):
        now_date = now_date.to_pydatetime().date()
    elif hasattr(now_date, "date"):
        now_date = now_date.date()

    # Anchors: the closest snapshot on-or-before each target date.
    def _value_at_offset(days: int):
        target = now_date - timedelta(days=days)
        return _value_on_or_before(daily_total, target)

    # YTD anchor = last snapshot of previous year
    ytd_target = date(now_date.year, 1, 1) - timedelta(days=1)
    all_time = float(daily_total.iloc[0]["account_value"])
    first_date = daily_total.iloc[0]["date"]
    if hasattr(first_date, "to_pydatetime"):
        first_date = first_date.to_pydatetime().date()
    elif hasattr(first_date, "date"):
        first_date = first_date.date()

    return {
        "now_value": now_value,
        "as_of": str(now_date),
        "first_snapshot_date": str(first_date),
        "deltas": {
            "today":   _delta_pct(now_value, _value_at_offset(1)),
            "week":    _delta_pct(now_value, _value_at_offset(7)),
            "month":   _delta_pct(now_value, _value_at_offset(30)),
            "ytd":     _delta_pct(now_value, _value_on_or_before(daily_total, ytd_target)),
            "all":     _delta_pct(now_value, all_time),
        },
    }


def _build_networth_series(daily_total: pd.DataFrame):
    """Daily net-worth line: dates + account_value summed across accounts."""
    if daily_total.empty:
        return {"dates": [], "values": []}
    return {
        "dates": [str(d) for d in daily_total["date"].tolist()],
        "values": [round(float(v), 2) for v in daily_total["account_value"].tolist()],
    }


def _build_benchmark_series(daily_total: pd.DataFrame, bench_df: pd.DataFrame, symbol: str):
    """Rebase a benchmark to the trader's first net-worth dollar value
    so they share the same y-axis at t=0.  Returns aligned series at the
    SAME dates as daily_total (forward-filling the benchmark)."""
    if daily_total.empty or bench_df.empty:
        return {"dates": [], "values": []}

    sym = bench_df[bench_df["symbol"] == symbol].copy()
    if sym.empty:
        return {"dates": [], "values": []}
    sym = sym.sort_values("date").drop_duplicates("date", keep="last")

    # Anchor at the first net-worth date with a benchmark close on-or-before.
    nw = daily_total.sort_values("date").reset_index(drop=True)
    start_value = float(nw.iloc[0]["account_value"])
    start_date = nw.iloc[0]["date"]

    bench_anchor = sym[sym["date"] <= start_date]
    if bench_anchor.empty:
        bench_anchor = sym[sym["date"] >= start_date]
        if bench_anchor.empty:
            return {"dates": [], "values": []}
        anchor_close = float(bench_anchor.iloc[0]["close_price"])
    else:
        anchor_close = float(bench_anchor.iloc[-1]["close_price"])

    if anchor_close == 0:
        return {"dates": [], "values": []}

    # Forward-fill: for each net-worth date, use the most recent benchmark close
    # on-or-before that date.  Implemented with merge_asof.
    nw["_d"] = pd.to_datetime(nw["date"])
    sym["_d"] = pd.to_datetime(sym["date"])
    merged = pd.merge_asof(
        nw.sort_values("_d"),
        sym.sort_values("_d")[["_d", "close_price"]].rename(columns={"close_price": "bench_close"}),
        on="_d",
        direction="backward",
    )

    rebased = merged["bench_close"].fillna(anchor_close) / anchor_close * start_value
    return {
        "dates": [str(d) for d in nw["date"].tolist()],
        "values": [round(float(v), 2) for v in rebased.tolist()],
    }


def _build_contrib_vs_gains(daily_total: pd.DataFrame):
    """Stacked-area data: implied contributions vs implied gains over time."""
    if daily_total.empty:
        return {"dates": [], "contributions": [], "gains": []}

    return {
        "dates": [str(d) for d in daily_total["date"].tolist()],
        # Clip to >= 0 for display: gains can be negative on a bad year
        # (account_value < contributions), and stacked area handles that
        # poorly.  We keep the raw sign for the hero summary below.
        "contributions": [round(float(v), 2) for v in daily_total["implied_contributions"].tolist()],
        "gains":         [round(float(v), 2) for v in daily_total["implied_gains"].tolist()],
    }


def _build_allocation(latest_per_account: pd.DataFrame):
    """Today's allocation across all accounts: equity / option / cash."""
    if latest_per_account.empty:
        return {"equity": 0.0, "option": 0.0, "cash": 0.0}
    return {
        "equity": round(float(latest_per_account["equity_value"].sum()), 2),
        "option": round(float(latest_per_account["option_value"].sum()), 2),
        "cash":   round(float(latest_per_account["cash_value"].sum()),   2),
    }


def _build_allocation_year_ago(per_account_long: pd.DataFrame, today: date):
    """Allocation at most-recent snapshot on-or-before today − 1 year, summed
    across the user's accounts."""
    if per_account_long.empty:
        return None
    target = today - timedelta(days=365)
    target_ts = pd.Timestamp(target)

    # Latest row per account on-or-before target
    sub = per_account_long[per_account_long["_d"] <= target_ts]
    if sub.empty:
        return None
    latest = sub.sort_values("_d").groupby("account").tail(1)
    return {
        "equity": round(float(latest["equity_value"].sum()), 2),
        "option": round(float(latest["option_value"].sum()), 2),
        "cash":   round(float(latest["cash_value"].sum()),   2),
        "as_of":  str(target),
    }


# ------------------------------------------------------------------
# Route
# ------------------------------------------------------------------

@app.route("/wealth")
@login_required
def wealth():
    """High-level wealth dashboard."""
    selected_account = (request.args.get("account", "") or "").strip()
    user_accounts = _user_accounts_for_request(selected_account)

    if is_admin(current_user.username):
        accounts = []
    else:
        accounts = get_accounts_for_user(current_user.id) or []

    hero = None
    networth = {"dates": [], "values": []}
    spy_series = {"dates": [], "values": []}
    qqq_series = {"dates": [], "values": []}
    contrib_gains = {"dates": [], "contributions": [], "gains": []}
    alloc_now = {"equity": 0.0, "option": 0.0, "cash": 0.0}
    alloc_year_ago = None
    error = None
    has_data = False

    try:
        client = get_bigquery_client()
        wealth_df = client.query(
            WEALTH_DAILY_QUERY.format(account_filter=_account_sql_and(user_accounts))
        ).to_dataframe()
        wealth_df = _filter_df_by_accounts(wealth_df, user_accounts)

        if not wealth_df.empty:
            has_data = True
            wealth_df["_d"] = pd.to_datetime(wealth_df["date"])

            # Aggregate across the user's accounts to a single per-day
            # net-worth row.  Sums make sense for *_value and the
            # cumulative_* columns.
            daily_total = (
                wealth_df.groupby("date", as_index=False)
                         .agg(account_value=("account_value", "sum"),
                              equity_value=("equity_value", "sum"),
                              option_value=("option_value", "sum"),
                              cash_value=("cash_value", "sum"),
                              implied_contributions=("implied_contributions", "sum"),
                              implied_gains=("implied_gains", "sum"))
                         .sort_values("date")
                         .reset_index(drop=True)
            )

            hero = _build_hero(daily_total[["date", "account_value"]])
            networth = _build_networth_series(daily_total[["date", "account_value"]])
            contrib_gains = _build_contrib_vs_gains(daily_total)

            # Allocation now: latest row per account, summed.
            latest_per_account = (
                wealth_df.sort_values("_d").groupby("account").tail(1)
            )
            alloc_now = _build_allocation(latest_per_account)

            today = daily_total["date"].iloc[-1]
            if hasattr(today, "to_pydatetime"):
                today = today.to_pydatetime().date()
            elif hasattr(today, "date"):
                today = today.date()
            alloc_year_ago = _build_allocation_year_ago(wealth_df, today)

            # Benchmark, anchored at the user's first snapshot date.
            start_date = daily_total["date"].iloc[0]
            cfg = None
            try:
                from google.cloud import bigquery as bq
                cfg = bq.QueryJobConfig(query_parameters=[
                    bq.ScalarQueryParameter("start_date", "DATE", start_date),
                ])
            except Exception:
                cfg = None
            bench_df = client.query(BENCHMARK_QUERY, job_config=cfg).to_dataframe()

            spy_series = _build_benchmark_series(daily_total, bench_df, "SPY")
            qqq_series = _build_benchmark_series(daily_total, bench_df, "QQQ")
    except Exception as exc:
        app.logger.warning("/wealth query failed: %s", exc)
        error = "Could not load wealth data right now. Try again in a minute."

    return render_template(
        "wealth.html",
        title="Wealth",
        accounts=accounts,
        selected_account=selected_account,
        has_data=has_data,
        hero=hero,
        alloc_now=alloc_now,
        alloc_year_ago=alloc_year_ago,
        error=error,
        # JSON payloads consumed by Chart.js inside the template
        networth_json=json.dumps(networth),
        spy_json=json.dumps(spy_series),
        qqq_json=json.dumps(qqq_series),
        contrib_gains_json=json.dumps(contrib_gains),
        alloc_now_json=json.dumps(alloc_now),
        alloc_year_ago_json=json.dumps(alloc_year_ago) if alloc_year_ago else "null",
    )
