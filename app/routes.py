from flask import render_template, request, redirect, url_for, Response, flash, abort
from werkzeug.exceptions import RequestEntityTooLarge
from flask_login import login_required, current_user
from app import app
from app.extensions import limiter
from app.bigquery_client import get_bigquery_client
from app.schwab import (
    SCHWAB_FULL_HISTORY_LOOKBACK_DAYS,
    _schwab_transaction_lookback_days,
)
from app.models import (
    add_account_for_user,
    get_accounts_for_user,
    get_schwab_connections,
    is_admin,
)
from google.cloud import bigquery
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor
import math
import os
import pandas as pd
import json


def _bq_parallel(client, queries):
    """Run multiple BigQuery queries in parallel and return results dict.

    queries: dict of {name: sql_string} or {name: (sql_string, job_config)}
    Returns: dict of {name: DataFrame}
    """
    results = {}

    def _run(name, spec):
        if isinstance(spec, tuple):
            sql, cfg = spec
            return name, client.query(sql, job_config=cfg).to_dataframe()
        return name, client.query(spec).to_dataframe()

    with ThreadPoolExecutor(max_workers=min(len(queries), 8)) as pool:
        futures = [pool.submit(_run, n, s) for n, s in queries.items()]
        for f in futures:
            name, df = f.result()
            results[name] = df

    return results


def _user_account_list():
    """
    Return the list of accounts the current user is allowed to see,
    or None if the user is an admin (meaning: no filter, show everything).

    Includes labels from user_accounts (upload / sync) and from Schwab OAuth
    rows so BigQuery filters match the Account column in seeds. If Schwab is
    connected but user_accounts was never populated, we add the Schwab labels
    here (idempotent) so queries are not forced to AND 1=0.
    """
    if is_admin(current_user.username):
        return None                     # admin → no restriction
    names = list(get_accounts_for_user(current_user.id))
    have = set(names)
    for row in get_schwab_connections(current_user.id):
        label = (row.get("account_name") or "").strip() or str(
            row.get("account_number") or ""
        ).strip()
        if not label:
            continue
        if label not in have:
            add_account_for_user(current_user.id, label)
            have.add(label)
            names.append(label)
    return sorted(names)


def _account_sql_filter(accounts, col="account"):
    """
    Build a SQL WHERE clause fragment for filtering by account.
    accounts: list of account names, or None for no filter (admin).
    Returns a string like "WHERE account IN ('Foo', 'Bar')" or "".
    """
    if accounts is None:
        return ""
    if not accounts:
        return "WHERE 1 = 0"            # user has no accounts → return nothing
    quoted = ", ".join(f"'{a.replace(chr(39), chr(39)+chr(39))}'" for a in accounts)
    expr = f"TRIM(CAST({col} AS STRING))"
    return f"WHERE {expr} IN ({quoted})"


def _account_sql_and(accounts, col="account"):
    """Return AND clause for account filter when already in a WHERE. Empty string if no filter.

    BigQuery may store `account` as INT (e.g. Schwab #) while the app uses string
    labels from user_accounts. Compare as strings so `IN (...)` is not silently empty.
    """
    if accounts is None:
        return ""
    if not accounts:
        return "AND 1 = 0"
    quoted = ", ".join(f"'{a.replace(chr(39), chr(39)+chr(39))}'" for a in accounts)
    expr = f"TRIM(CAST({col} AS STRING))"
    return f"AND {expr} IN ({quoted})"


def _filter_df_by_accounts(df, accounts, col="account"):
    """Filter a DataFrame to only rows matching the user's accounts.
    accounts=None means admin → return unfiltered. Values compared as trimmed
    strings so BQ int/float account ids still match app-side string labels.
    """
    if accounts is None:
        return df
    if not accounts:
        return df.iloc[0:0]             # empty frame
    if col not in df.columns:
        return df
    want = {str(a).strip() for a in accounts if a is not None and str(a).strip() != ""}

    def _norm_acc(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return str(v).strip()

    m = df[col].map(_norm_acc).isin(want)
    return df[m]


def _df_normalize_account_column(df):
    """BigQuery to_dataframe() sometimes returns Account; app filters on account."""
    if df is None or df.empty:
        return df
    if "Account" in df.columns and "account" not in df.columns:
        return df.rename(columns={"Account": "account"})
    return df


def _iter_symbols_for_daily_detail(trades_df, pnl_df, current_df, open_pairs):
    """
    Row keys (account, symbol) for /symbols. dbt can classify open options from
    the current snapshot alone (int_option_contracts.snapshot_only_options) so
    positions_summary has a row with no stg_history rows — the Positions page
    still works. This iterator unions trade-history keys with positions_summary
    and current so Daily Detail matches that catalog.
    """
    seen = set()
    out = []
    if (
        not trades_df.empty
        and "account" in trades_df.columns
        and "symbol" in trades_df.columns
    ):
        for (acc, sym), _ in trades_df.groupby(["account", "symbol"]):
            k = (str(acc), str(sym))
            if open_pairs is not None and k not in open_pairs:
                continue
            if k not in seen:
                seen.add(k)
                out.append((acc, sym))
    for df in (pnl_df, current_df):
        if df is None or df.empty or "account" not in df.columns or "symbol" not in df.columns:
            continue
        for _, row in df.drop_duplicates(["account", "symbol"]).iterrows():
            acc, sym = row["account"], row["symbol"]
            k = (str(acc), str(sym))
            if open_pairs is not None and k not in open_pairs:
                continue
            if k in seen:
                continue
            seen.add(k)
            out.append((acc, sym))
    return out


# ------------------------------------------------------------------
# SQL: date-filtered re-aggregation of positions_summary
# This CANNOT be a dbt model because it requires runtime date parameters
# from the user's filter selection. It re-aggregates int_strategy_classification
# with a WHERE clause on dates — essentially positions_summary with a date window.
# ------------------------------------------------------------------
DATE_FILTERED_QUERY = """
WITH classified AS (
    SELECT *
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE open_date <= @end_date
      AND COALESCE(close_date, CURRENT_DATE()) >= @start_date
      {account_filter}
),

dividends AS (
    SELECT
        account,
        underlying_symbol AS symbol,
        SUM(amount) AS total_dividend_income,
        COUNT(*) AS dividend_count
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE action = 'dividend'
      AND trade_date >= @start_date
      AND trade_date <= @end_date
      {account_filter}
    GROUP BY 1, 2
),

strategy_summary AS (
    SELECT
        account,
        symbol,
        strategy,

        CASE
            WHEN COUNTIF(status = 'Open') > 0 AND COUNTIF(status = 'Closed') > 0 THEN 'Mixed'
            WHEN COUNTIF(status = 'Open') > 0 THEN 'Open'
            ELSE 'Closed'
        END AS status,

        SUM(total_pnl) AS total_pnl,
        SUM(CASE WHEN status = 'Closed' THEN total_pnl ELSE 0 END) AS realized_pnl,
        SUM(CASE WHEN status = 'Open'   THEN total_pnl ELSE 0 END) AS unrealized_pnl,

        SUM(premium_received) AS total_premium_received,
        SUM(ABS(premium_paid)) AS total_premium_paid,

        COUNT(*) AS num_trade_groups,
        SUM(num_trades) AS num_individual_trades,
        COUNTIF(is_winner AND status = 'Closed') AS num_winners,
        COUNTIF(NOT is_winner AND status = 'Closed') AS num_losers,

        SAFE_DIVIDE(
            COUNTIF(is_winner AND status = 'Closed'),
            NULLIF(COUNTIF(status = 'Closed'), 0)
        ) AS win_rate,

        SAFE_DIVIDE(
            SUM(CASE WHEN status = 'Closed' THEN total_pnl ELSE 0 END),
            NULLIF(COUNTIF(status = 'Closed'), 0)
        ) AS avg_pnl_per_trade,

        ROUND(AVG(days_in_trade), 1) AS avg_days_in_trade,
        MIN(open_date) AS first_trade_date,
        MAX(COALESCE(close_date, CURRENT_DATE())) AS last_trade_date

    FROM classified
    GROUP BY 1, 2, 3
),

with_dividend_rank AS (
    SELECT
        ss.*,
        ROW_NUMBER() OVER (
            PARTITION BY ss.account, ss.symbol
            ORDER BY
                CASE ss.strategy
                    WHEN 'Wheel'        THEN 1
                    WHEN 'Covered Call'  THEN 2
                    WHEN 'Buy and Hold'  THEN 3
                    ELSE 99
                END
        ) AS dividend_rank
    FROM strategy_summary ss
),

final AS (
    SELECT
        wdr.account,
        wdr.symbol,
        wdr.strategy,
        wdr.status,
        ROUND(wdr.total_pnl, 2) AS total_pnl,
        ROUND(wdr.realized_pnl, 2) AS realized_pnl,
        ROUND(wdr.unrealized_pnl, 2) AS unrealized_pnl,
        ROUND(wdr.total_premium_received, 2) AS total_premium_received,
        ROUND(wdr.total_premium_paid, 2) AS total_premium_paid,
        wdr.num_trade_groups,
        wdr.num_individual_trades,
        wdr.num_winners,
        wdr.num_losers,
        ROUND(wdr.win_rate, 4) AS win_rate,
        ROUND(wdr.avg_pnl_per_trade, 2) AS avg_pnl_per_trade,
        wdr.avg_days_in_trade,
        wdr.first_trade_date,
        wdr.last_trade_date,
        CASE WHEN wdr.dividend_rank = 1
            THEN ROUND(COALESCE(d.total_dividend_income, 0), 2)
            ELSE 0
        END AS total_dividend_income,
        CASE WHEN wdr.dividend_rank = 1
            THEN COALESCE(d.dividend_count, 0)
            ELSE 0
        END AS dividend_count,
        ROUND(
            wdr.total_pnl
            + CASE WHEN wdr.dividend_rank = 1 THEN COALESCE(d.total_dividend_income, 0) ELSE 0 END
        , 2) AS total_return
    FROM with_dividend_rank wdr
    LEFT JOIN dividends d
        ON wdr.account = d.account
        AND wdr.symbol = d.symbol
)

SELECT * FROM final
ORDER BY account, symbol, strategy
"""

# ------------------------------------------------------------------
# Default (no date filter): use the pre-built mart
# ------------------------------------------------------------------
DEFAULT_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {account_filter}
    ORDER BY account, symbol, strategy
"""

ERROR_DEFAULTS = dict(
    error="",
    rows=[],
    symbol_rows=[],
    kpis={},
    strategy_chart=[],
    accounts=[],
    strategies=[],
    symbols=[],
    selected_account="",
    selected_strategy="",
    selected_status="",
    selected_symbol="",
    selected_start_date="",
    selected_end_date="",
    date_filtered=False,
    page=1,
    total_pages=1,
    total_rows=0,
    per_page=25,
    today=date.today(),
    timedelta=timedelta,
)


def _parse_date(value):
    """Return a date object if value is a valid YYYY-MM-DD string, else None."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None



# ------------------------------------------------------------------
# Feature pages for marketing (logged-out)
# ------------------------------------------------------------------
FEATURES = {
    "strategy-auto-detection": {
        "title": "Strategy auto-detection",
        "subtitle": "Every position classified automatically—no manual tagging.",
        "demo_partial": "features/_demo_strategy.html",
        "value_bullets": [
            "Covered Calls, Cash-Secured Puts, Wheels, spreads, and Buy and Hold—all identified from your trade data.",
            "See exactly which strategies drive your returns and which drain performance.",
            "Stop guessing. Know whether the Wheel is outperforming CSPs for your portfolio.",
        ],
    },
    "ai-trading-insights": {
        "title": "AI trading insights",
        "subtitle": "Personalized analysis of your trading style and performance.",
        "demo_partial": "features/_demo_insights.html",
        "value_bullets": [
            "Get a coach-style overview: what you do well, what needs work, and why.",
            "Actionable suggestions based on your actual data—not generic advice.",
            "The 'wow' moment when the app shows it truly understands your trading.",
        ],
    },
    "performance-charts": {
        "title": "Performance charts",
        "subtitle": "Cumulative P&L over time, broken down by equity, options, and dividends.",
        "demo_partial": "features/_demo_charts.html",
        "value_bullets": [
            "Visualize your progress. See how each strategy contributes over time.",
            "Portfolio-wide and per-account charts so nothing stays hidden.",
            "The full picture—not just today's balance, but the journey.",
        ],
    },
    "position-detail": {
        "title": "Position detail",
        "subtitle": "Drill into any symbol: trades, strategies, and cumulative P&L.",
        "demo_partial": "features/_demo_position.html",
        "value_bullets": [
            "Click any symbol to see its full story: every trade, every strategy, every dollar.",
            "Understand why a position performed the way it did—before your next move.",
            "Trade history, current positions, and charts in one place.",
        ],
    },
    "multi-account": {
        "title": "Multi-account",
        "subtitle": "Track all your Schwab accounts in one place.",
        "demo_partial": "features/_demo_multiaccount.html",
        "value_bullets": [
            "IRA, taxable, joint—see portfolio-wide metrics and per-account breakdowns.",
            "Filter by account on every view: positions, tax center, performance.",
            "One dashboard. All your accounts.",
        ],
    },
}


@app.route("/features/<slug>")
def feature_detail(slug):
    """Feature detail page with demo and value prop."""
    if slug == "ai-trading-insights" and not app.config.get("INSIGHTS_ENABLED", True):
        abort(404)
    feature = FEATURES.get(slug)
    if not feature:
        abort(404)
    return render_template(
        "features/detail.html",
        title=feature["title"],
        feature=feature,
        all_features=FEATURES,
        current_slug=slug,
    )


@app.route("/pricing")
def pricing():
    """Pricing placeholder for marketing."""
    return render_template("pricing.html", title="Pricing")


@app.route("/faq")
def faq():
    """FAQ page for marketing."""
    return render_template("faq.html", title="FAQ")


@app.route("/sitemap.xml")
def sitemap():
    """Simple sitemap for SEO."""
    base = request.url_root.rstrip("/")
    pages = [
        ("", "daily", "1.0"),
        ("/pricing", "monthly", "0.8"),
        ("/faq", "monthly", "0.7"),
    ]
    for slug in FEATURES:
        if slug == "ai-trading-insights" and not app.config.get("INSIGHTS_ENABLED", True):
            continue
        pages.append((f"/features/{slug}", "monthly", "0.7"))
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    for path, freq, prio in pages:
        xml += f"  <url><loc>{base}{path}</loc><changefreq>{freq}</changefreq><priority>{prio}</priority></url>\n"
    xml += "</urlset>"
    return Response(xml, mimetype="application/xml")


@app.route("/robots.txt")
def robots():
    """Basic robots.txt for crawlers."""
    base = request.url_root.rstrip("/")
    return Response(
        f"User-agent: *\nAllow: /\nDisallow: /positions\nDisallow: /upload\nDisallow: /insights\nDisallow: /settings\nDisallow: /accounts\nDisallow: /symbols\nDisallow: /position/\nSitemap: {base}/sitemap.xml\n",
        mimetype="text/plain",
    )


@app.route("/")
@app.route("/index")
def index():
    """Public landing page, or redirect to weekly review (home) if logged in."""
    if current_user.is_authenticated:
        return redirect(url_for("weekly_review"))
    return render_template("landing.html", title="Home")



@app.route("/get-started")
@login_required
def get_started():
    """Onboarding checklist for new users — tracks real progress."""
    user_accounts = get_accounts_for_user(current_user.id)
    has_uploaded = len(user_accounts) > 0

    # Check if data is actually available in BigQuery
    has_data = False
    if has_uploaded:
        try:
            client = get_bigquery_client()
            where = _account_sql_filter(user_accounts)
            check_q = f"SELECT COUNT(*) AS cnt FROM `ccwj-dbt.analytics.positions_summary` {where}"
            result = client.query(check_q).to_dataframe()
            has_data = int(result.iloc[0]["cnt"]) > 0 if not result.empty else False
        except Exception:
            pass

    schwab_enabled = bool(os.environ.get("SCHWAB_APP_KEY") and os.environ.get("SCHWAB_APP_SECRET"))
    schwab_connected = bool(
        schwab_enabled and get_schwab_connections(current_user.id)
    )
    schwab_full_history_days = SCHWAB_FULL_HISTORY_LOOKBACK_DAYS
    schwab_routine_days = _schwab_transaction_lookback_days()

    return render_template(
        "get_started.html",
        title="Get Started",
        has_uploaded=has_uploaded,
        has_data=has_data,
        schwab_enabled=schwab_enabled,
        schwab_connected=schwab_connected,
        schwab_full_history_days=schwab_full_history_days,
        schwab_routine_days=schwab_routine_days,
    )


@app.route("/ping")
@limiter.exempt
def ping():
    return "Flask app is alive"


@app.route("/sentry-debug")
def sentry_debug():
    """Verify Sentry installation. Remove after verification."""
    raise RuntimeError("Sentry test: this error is intentional")


@app.route("/positions")
@login_required
def positions():
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    acct_filter = _account_sql_and(user_accounts)

    # ------------------------------------------------------------------
    # 1. Read filter params
    # ------------------------------------------------------------------
    selected_account = request.args.get("account", "")
    selected_strategy = request.args.get("strategy", "")
    # Allow multi-select status; default to Open when no explicit filter
    selected_statuses = request.args.getlist("status")
    if not selected_statuses:
        selected_statuses = ["Open"]
    selected_symbol = request.args.get("symbol", "")
    selected_start_date = request.args.get("start_date", "")
    selected_end_date = request.args.get("end_date", "")
    page = max(1, int(request.args.get("page", 1)))

    start_date = _parse_date(selected_start_date)
    end_date = _parse_date(selected_end_date)
    date_filtered = start_date is not None or end_date is not None

    # ------------------------------------------------------------------
    # 2. Query BigQuery
    # ------------------------------------------------------------------
    try:
        if date_filtered:
            # Fill open boundaries with wide defaults
            effective_start = start_date or date(2000, 1, 1)
            effective_end = end_date or date.today()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "DATE", effective_start),
                    bigquery.ScalarQueryParameter("end_date", "DATE", effective_end),
                ]
            )
            df = client.query(DATE_FILTERED_QUERY.format(account_filter=acct_filter), job_config=job_config).to_dataframe()
        else:
            df = client.query(DEFAULT_QUERY.format(account_filter=acct_filter)).to_dataframe()
    except Exception as exc:
        ctx = dict(ERROR_DEFAULTS)
        ctx["error"] = str(exc)
        return render_template("positions.html", **ctx)

    # ------------------------------------------------------------------
    # 3. Clean up types
    # ------------------------------------------------------------------
    numeric_cols = [
        "total_pnl", "realized_pnl", "unrealized_pnl",
        "total_premium_received", "total_premium_paid",
        "num_trade_groups", "num_individual_trades",
        "num_winners", "num_losers", "win_rate",
        "avg_pnl_per_trade", "avg_days_in_trade",
        "total_dividend_income", "dividend_count", "total_return",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    for col in ["first_trade_date", "last_trade_date"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("NaT", "")

    # ------------------------------------------------------------------
    # 4. Safety-belt filter (SQL already filtered by account)
    # ------------------------------------------------------------------
    df = _filter_df_by_accounts(df, user_accounts)

    accounts = sorted(df["account"].dropna().unique())
    strategies = sorted(df["strategy"].dropna().unique())
    symbols = sorted(df["symbol"].dropna().unique())

    filtered = df.copy()
    if selected_account:
        filtered = filtered[filtered["account"] == selected_account]
    if selected_strategy:
        filtered = filtered[filtered["strategy"] == selected_strategy]
    if selected_statuses:
        filtered = filtered[filtered["status"].isin(selected_statuses)]
    if selected_symbol:
        filtered = filtered[filtered["symbol"] == selected_symbol]

    # ------------------------------------------------------------------
    # 5. KPIs
    # ------------------------------------------------------------------
    total_winners = int(filtered["num_winners"].sum())
    total_losers = int(filtered["num_losers"].sum())
    total_closed = total_winners + total_losers

    kpis = {
        "total_return": float(filtered["total_return"].sum()),
        "realized_pnl": float(filtered["realized_pnl"].sum()),
        "unrealized_pnl": float(filtered["unrealized_pnl"].sum()),
        "premium_collected": float(filtered["total_premium_received"].sum()),
        "win_rate": total_winners / total_closed if total_closed else 0,
        "num_positions": len(filtered),
        "total_trades": int(filtered["num_individual_trades"].sum()),
    }

    # ------------------------------------------------------------------
    # 6. Chart data: total P&L by strategy
    # ------------------------------------------------------------------
    strategy_chart = (
        filtered.groupby("strategy")["total_pnl"]
        .sum()
        .sort_values(ascending=True)
        .reset_index()
        .rename(columns={"total_pnl": "pnl"})
        .to_dict(orient="records")
    )

    # ------------------------------------------------------------------
    # 7. Symbol-level summary (grouped by account + symbol)
    # ------------------------------------------------------------------
    if not filtered.empty:
        symbol_agg = (
            filtered.groupby(["account", "symbol"])
            .agg(
                total_pnl=("total_pnl", "sum"),
                realized_pnl=("realized_pnl", "sum"),
                unrealized_pnl=("unrealized_pnl", "sum"),
                total_premium_received=("total_premium_received", "sum"),
                total_dividend_income=("total_dividend_income", "sum"),
                total_return=("total_return", "sum"),
                num_individual_trades=("num_individual_trades", "sum"),
                num_winners=("num_winners", "sum"),
                num_losers=("num_losers", "sum"),
                num_strategies=("strategy", "nunique"),
                strategies=("strategy", lambda x: ", ".join(sorted(x.unique()))),
            )
            .reset_index()
        )
        closed = symbol_agg["num_winners"] + symbol_agg["num_losers"]
        symbol_agg["win_rate"] = symbol_agg["num_winners"] / closed.replace(0, pd.NA)
        symbol_agg["win_rate"] = symbol_agg["win_rate"].fillna(0)
        symbol_agg = symbol_agg.sort_values("total_return", ascending=False)
        symbol_rows = symbol_agg.to_dict(orient="records")
    else:
        symbol_rows = []

    # ------------------------------------------------------------------
    # 8. Strategy detail rows (aggregated by account × strategy, paginated)
    # ------------------------------------------------------------------
    if not filtered.empty:
        strat_agg = (
            filtered.groupby(["account", "strategy"])
            .agg(
                status=("status", lambda xs: "Open" if (xs == "Open").any() else "Closed"),
                total_pnl=("total_pnl", "sum"),
                realized_pnl=("realized_pnl", "sum"),
                unrealized_pnl=("unrealized_pnl", "sum"),
                total_premium_received=("total_premium_received", "sum"),
                total_dividend_income=("total_dividend_income", "sum"),
                total_return=("total_return", "sum"),
                num_individual_trades=("num_individual_trades", "sum"),
                num_winners=("num_winners", "sum"),
                num_losers=("num_losers", "sum"),
                avg_pnl_per_trade=("avg_pnl_per_trade", "mean"),
                avg_days_in_trade=("avg_days_in_trade", "mean"),
            )
            .reset_index()
        )
        closed_ct = strat_agg["num_winners"] + strat_agg["num_losers"]
        strat_agg["win_rate"] = strat_agg["num_winners"] / closed_ct.replace(0, pd.NA)
        strat_agg["win_rate"] = strat_agg["win_rate"].fillna(0)
        strat_agg = strat_agg.sort_values("total_return", ascending=False)
        all_rows = strat_agg.to_dict(orient="records")
    else:
        all_rows = []

    per_page = 25
    total_rows = len(all_rows)
    total_pages = max(1, (total_rows + per_page - 1) // per_page)
    page = min(page, total_pages)
    start_idx = (page - 1) * per_page
    rows = all_rows[start_idx : start_idx + per_page]

    return render_template(
        "positions.html",
        rows=rows,
        symbol_rows=symbol_rows,
        kpis=kpis,
        strategy_chart=strategy_chart,
        accounts=accounts,
        strategies=strategies,
        symbols=symbols,
        selected_account=selected_account,
        selected_strategy=selected_strategy,
        selected_statuses=selected_statuses,
        selected_symbol=selected_symbol,
        selected_start_date=selected_start_date,
        selected_end_date=selected_end_date,
        date_filtered=date_filtered,
        page=page,
        total_pages=total_pages,
        total_rows=total_rows,
        per_page=per_page,
        today=date.today(),
        timedelta=timedelta,
    )


# ======================================================================
# Position Detail  (/position/<symbol>)
# ======================================================================

POSITION_SUMMARY_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE symbol = '{symbol}'
    {account_filter}
    ORDER BY account, strategy
"""

POSITION_TRADES_QUERY = """
    SELECT
        account,
        underlying_symbol AS symbol,
        trade_date,
        action,
        action_raw,
        trade_symbol,
        instrument_type,
        description,
        quantity,
        price,
        fees,
        amount
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE trade_date IS NOT NULL
      AND (
        UPPER(TRIM(COALESCE(underlying_symbol, ''))) = UPPER(TRIM('{symbol}'))
        OR UPPER(TRIM(SPLIT(COALESCE(trade_symbol, ''), ' ')[SAFE_OFFSET(0)])) = UPPER(TRIM('{symbol}'))
      )
    {account_filter}
    ORDER BY trade_date DESC
"""

POSITION_CURRENT_QUERY = """
    SELECT
        account,
        underlying_symbol AS symbol,
        instrument_type,
        trade_symbol,
        description,
        quantity,
        current_price,
        market_value,
        cost_basis,
        unrealized_pnl,
        unrealized_pnl_pct
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE underlying_symbol = '{symbol}'
    {account_filter}
"""

POSITION_CLOSED_LEGS_QUERY = """
    SELECT
        sc.account,
        sc.symbol,
        sc.strategy,
        sc.trade_symbol,
        sc.open_date,
        sc.close_date,
        sc.total_pnl,
        sc.status,
        oc.contracts_sold_to_open + oc.contracts_bought_to_open AS quantity,
        oc.premium_received,
        oc.premium_paid,
        oc.cost_to_close,
        oc.proceeds_from_close,
        oc.direction,
        oc.close_type,
        oc.days_in_trade
    FROM `ccwj-dbt.analytics.int_strategy_classification` sc
    JOIN `ccwj-dbt.analytics.int_option_contracts` oc
      ON sc.account = oc.account AND sc.trade_symbol = oc.trade_symbol
    WHERE sc.status = 'Closed'
      AND sc.trade_group_type = 'option_contract'
      AND sc.symbol = '{symbol}'
    {sc_account_filter}
"""

POSITION_CLOSED_EQUITY_QUERY = """
    SELECT
        account,
        symbol,
        trade_symbol,
        session_id,
        open_date,
        close_date,
        quantity,
        sale_price_per_share,
        sell_proceeds,
        cost_basis,
        realized_pnl,
        description
    FROM `ccwj-dbt.analytics.int_closed_equity_legs`
    WHERE symbol = '{symbol}'
    {account_filter}
"""

POSITION_SESSIONS_QUERY = """
    SELECT
        account,
        symbol,
        session_id,
        open_date,
        last_trade_date,
        status,
        total_pnl,
        days_held,
        max_quantity_held,
        num_trades
    FROM `ccwj-dbt.analytics.int_equity_sessions`
    WHERE symbol = '{symbol}'
    {account_filter}
    ORDER BY account, session_id
"""

POSITION_MATRIX_QUERY = """
    SELECT
        account,
        underlying_symbol,
        strategy,
        trade_symbol,
        dte_at_open,
        dte_bucket,
        strike_distance,
        pnl_pct,
        total_pnl,
        direction,
        option_type,
        outcome
    FROM `ccwj-dbt.analytics.int_option_trade_kinds`
    WHERE status = 'Closed'
      AND strike_distance IS NOT NULL
      AND underlying_symbol = '{symbol}'
    {account_filter}
"""

def _merge_position_strategy_breakdown(
    symbol: str,
    summary_df: pd.DataFrame,
    closed_legs_df: pd.DataFrame,
    closed_equity_df: pd.DataFrame,
) -> pd.DataFrame:
    """Return a strategy table that includes any (account, strategy) in closed legs/equity
    missing from positions_summary, so the breakdown matches the Position Legs / history.

    positions_summary is one row per (account, symbol, strategy); in edge cases a closed
    strategy can be absent from the mart while int_strategy_classification still has legs.
    """
    existing = set()
    if summary_df is not None and not summary_df.empty and "account" in summary_df.columns:
        for _, r in summary_df.iterrows():
            a = r.get("account")
            s = r.get("strategy")
            if a is None or (isinstance(s, float) and pd.isna(s)) or s is None:
                continue
            st = str(s).strip()
            if not st:
                continue
            existing.add((str(a).strip(), st))

    def _row_from_option_group(acct: str, strat: str, sub: pd.DataFrame) -> dict:
        total = float(sub["total_pnl"].sum()) if "total_pnl" in sub.columns else 0.0
        prem_r = float(sub["premium_received"].sum()) if "premium_received" in sub.columns else 0.0
        prem_p = float(sub["premium_paid"].sum()) if "premium_paid" in sub.columns else 0.0
        n = len(sub)
        wins = int((sub["total_pnl"] > 0).sum()) if "total_pnl" in sub.columns else 0
        losses = n - wins
        wr = wins / n if n else 0.0
        days_mean = 0.0
        if "days_in_trade" in sub.columns:
            days_mean = float(sub["days_in_trade"].fillna(0).mean() or 0.0)
        od = (
            sub["open_date"].dropna().min() if "open_date" in sub.columns else None
        )
        cd = (
            sub["close_date"].dropna().max() if "close_date" in sub.columns else None
        )
        avg_pnl = total / n if n else 0.0
        return {
            "account": acct,
            "symbol": symbol,
            "strategy": strat,
            "status": "Closed",
            "total_pnl": round(total, 2),
            "realized_pnl": round(total, 2),
            "unrealized_pnl": 0.0,
            "total_premium_received": round(prem_r, 2),
            "total_premium_paid": round(prem_p, 2),
            "num_trade_groups": n,
            "num_individual_trades": n,
            "num_winners": wins,
            "num_losers": losses,
            "win_rate": wr,
            "avg_pnl_per_trade": round(avg_pnl, 2),
            "avg_days_in_trade": round(days_mean, 1),
            "first_trade_date": od,
            "last_trade_date": cd,
            "total_dividend_income": 0.0,
            "dividend_count": 0,
            "total_return": round(total, 2),
        }

    def _row_from_equity_group(acct: str, lbl: str, sub: pd.DataFrame) -> dict:
        real = float(sub["realized_pnl"].sum()) if "realized_pnl" in sub.columns else 0.0
        n = len(sub)
        wins = int((sub["realized_pnl"] > 0).sum()) if "realized_pnl" in sub.columns else 0
        losses = n - wins
        wr = wins / n if n else 0.0
        od = sub["open_date"].dropna().min() if "open_date" in sub.columns else None
        cd = sub["close_date"].dropna().max() if "close_date" in sub.columns else None
        days_mean = 0.0
        for _, er in sub.iterrows():
            o = er.get("open_date")
            c = er.get("close_date")
            if pd.notna(o) and pd.notna(c):
                try:
                    days_mean += (pd.to_datetime(c) - pd.to_datetime(o)).days
                except Exception:
                    pass
        if n:
            days_mean = round(days_mean / n, 1)
        avg_pnl = real / n if n else 0.0
        return {
            "account": acct,
            "symbol": symbol,
            "strategy": lbl,
            "status": "Closed",
            "total_pnl": round(real, 2),
            "realized_pnl": round(real, 2),
            "unrealized_pnl": 0.0,
            "total_premium_received": 0.0,
            "total_premium_paid": 0.0,
            "num_trade_groups": n,
            "num_individual_trades": n,
            "num_winners": wins,
            "num_losers": losses,
            "win_rate": wr,
            "avg_pnl_per_trade": round(avg_pnl, 2),
            "avg_days_in_trade": days_mean,
            "first_trade_date": od,
            "last_trade_date": cd,
            "total_dividend_income": 0.0,
            "dividend_count": 0,
            "total_return": round(real, 2),
        }

    extra: list[dict] = []

    if closed_legs_df is not None and not closed_legs_df.empty and "strategy" in closed_legs_df.columns:
        g = closed_legs_df.copy()
        g = g[g["strategy"].notna() & (g["strategy"].astype(str).str.strip() != "")]
        for (acct, strat), sub in g.groupby(
            [g["account"].astype(str), g["strategy"].astype(str)]
        ):
            acct, strat = str(acct).strip(), str(strat).strip()
            if (acct, strat) in existing:
                continue
            extra.append(_row_from_option_group(acct, strat, sub))
            existing.add((acct, strat))

    if closed_equity_df is not None and not closed_equity_df.empty and "account" in closed_equity_df.columns:
        g = closed_equity_df.copy()
        lbl_col = "description" if "description" in g.columns else None
        if lbl_col:
            g["_strat_lbl"] = g[lbl_col].fillna("Equity").astype(str).str.strip()
            g.loc[g["_strat_lbl"] == "", "_strat_lbl"] = "Equity"
        else:
            g["_strat_lbl"] = "Equity"
        for (acct, slbl), sub in g.groupby(
            [g["account"].astype(str), g["_strat_lbl"]]
        ):
            acct, slbl = str(acct).strip(), str(slbl).strip() or "Equity"
            if (acct, slbl) in existing:
                continue
            sub = sub.drop(columns=["_strat_lbl"], errors="ignore")
            extra.append(_row_from_equity_group(acct, slbl, sub))
            existing.add((acct, slbl))

    if not extra:
        return summary_df if summary_df is not None else pd.DataFrame()

    extra_df = pd.DataFrame(extra)
    if summary_df is None or summary_df.empty:
        out = extra_df
    else:
        extra_df = extra_df.reindex(columns=list(summary_df.columns))
        out = pd.concat([summary_df, extra_df], ignore_index=True)

    if "status" in out.columns:
        _open = out["status"].astype(str).str.lower().eq("open")
        out = out.assign(_o=_open)
        if "total_return" in out.columns:
            out = out.sort_values(["_o", "total_return"], ascending=[False, False])
        else:
            out = out.sort_values("_o", ascending=False)
        out = out.drop(columns=["_o"])
    return out


def _realized_pnl_from_closed_frames(
    closed_legs_df: pd.DataFrame, closed_equity_df: pd.DataFrame
) -> float:
    """Sum realized P&L from closed option contract legs and closed equity lots."""
    r = 0.0
    if (
        closed_legs_df is not None
        and not closed_legs_df.empty
        and "total_pnl" in closed_legs_df.columns
    ):
        r += float(closed_legs_df["total_pnl"].sum())
    if (
        closed_equity_df is not None
        and not closed_equity_df.empty
        and "realized_pnl" in closed_equity_df.columns
    ):
        r += float(closed_equity_df["realized_pnl"].sum())
    return r


def _premium_totals_from_closed_options(closed_legs_df: pd.DataFrame) -> tuple:
    if closed_legs_df is None or closed_legs_df.empty:
        return 0.0, 0.0
    pr = (
        float(closed_legs_df["premium_received"].sum())
        if "premium_received" in closed_legs_df.columns
        else 0.0
    )
    pp = (
        float(closed_legs_df["premium_paid"].sum())
        if "premium_paid" in closed_legs_df.columns
        else 0.0
    )
    return pr, pp


# Pre-aggregated daily P&L data for chart rendering (single symbol)
CHART_DATA_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.mart_daily_pnl`
    WHERE symbol = '{symbol}'
      {account_filter}
    ORDER BY date
"""

# Pre-aggregated daily P&L data for all symbols (account-level charts)
CHART_DATA_ALL_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.mart_daily_pnl`
    WHERE 1=1 {account_filter}
    ORDER BY symbol, date
"""


@app.route("/position/<symbol>")
@login_required
def position_detail(symbol):
    client = get_bigquery_client()
    user_accounts = _user_account_list()

    # Escape symbol for SQL (prevent injection)
    safe_symbol = symbol.replace("'", "''")

    try:
        _pos_acct = _account_sql_and(user_accounts, col="account")
        _pos_sc_acct = _account_sql_and(user_accounts, col="sc.account")
        dfs = _bq_parallel(client, {
            "summary": POSITION_SUMMARY_QUERY.format(
                symbol=safe_symbol, account_filter=_pos_acct
            ),
            "trades": POSITION_TRADES_QUERY.format(
                symbol=safe_symbol, account_filter=_pos_acct
            ),
            "current": POSITION_CURRENT_QUERY.format(
                symbol=safe_symbol, account_filter=_pos_acct
            ),
            "closed_legs": POSITION_CLOSED_LEGS_QUERY.format(
                symbol=safe_symbol, sc_account_filter=_pos_sc_acct
            ),
            "closed_equity": POSITION_CLOSED_EQUITY_QUERY.format(
                symbol=safe_symbol, account_filter=_pos_acct
            ),
            "matrix": POSITION_MATRIX_QUERY.format(
                symbol=safe_symbol, account_filter=_pos_acct
            ),
            "sessions": POSITION_SESSIONS_QUERY.format(
                symbol=safe_symbol, account_filter=_pos_acct
            ),
        })
        summary_df = dfs["summary"]
        trades_df = dfs["trades"]
        current_df = dfs["current"]
        closed_legs_df = dfs["closed_legs"]
        closed_equity_df = dfs["closed_equity"]
        matrix_df = dfs["matrix"]
        sessions_df = dfs["sessions"]
        summary_df = _df_normalize_account_column(summary_df)
        trades_df = _df_normalize_account_column(trades_df)
        current_df = _df_normalize_account_column(current_df)
        closed_legs_df = _df_normalize_account_column(closed_legs_df)
        closed_equity_df = _df_normalize_account_column(closed_equity_df)
        matrix_df = _df_normalize_account_column(matrix_df)
        sessions_df = _df_normalize_account_column(sessions_df)
    except Exception as exc:
        return render_template(
            "position_detail.html",
            symbol=symbol,
            error=str(exc),
            kpis={},
            strategy_rows=[],
            trades=[],
            trade_outcomes=[],
            current_positions=[],
            option_matrices=[],
            sessions=[],
            selected_legs=[],
            leg_param="",
            chart_data_json="{}",
            has_underlying_price=False,
        )

    # Clean numeric types for summary
    num_cols = [
        "total_pnl", "realized_pnl", "unrealized_pnl",
        "total_premium_received", "total_premium_paid",
        "num_trade_groups", "num_individual_trades",
        "num_winners", "num_losers", "win_rate",
        "avg_pnl_per_trade", "avg_days_in_trade",
        "total_dividend_income", "dividend_count", "total_return",
    ]
    for col in num_cols:
        if col in summary_df.columns:
            summary_df[col] = pd.to_numeric(summary_df[col], errors="coerce").fillna(0)

    # Clean trades
    for col in ["amount", "quantity", "price", "fees"]:
        if col in trades_df.columns:
            trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
    if "trade_date" in trades_df.columns:
        trades_df["trade_date"] = pd.to_datetime(trades_df["trade_date"]).dt.date

    # Clean current positions
    for col in ["unrealized_pnl", "market_value", "quantity", "current_price", "cost_basis"]:
        if col in current_df.columns:
            current_df[col] = pd.to_numeric(current_df[col], errors="coerce").fillna(0)
    if "unrealized_pnl_pct" in current_df.columns:
        current_df["unrealized_pnl_pct"] = pd.to_numeric(
            current_df["unrealized_pnl_pct"], errors="coerce"
        ).fillna(0)

    # Filter to user's accounts (must run on every BQ frame — queries are by symbol
    # only, so unfiltered closed_legs/closed_equity/matrix would include all tenants.)
    summary_df = _filter_df_by_accounts(summary_df, user_accounts)
    trades_df = _filter_df_by_accounts(trades_df, user_accounts)
    current_df = _filter_df_by_accounts(current_df, user_accounts)
    closed_legs_df = _filter_df_by_accounts(closed_legs_df, user_accounts)
    closed_equity_df = _filter_df_by_accounts(closed_equity_df, user_accounts)
    matrix_df = _filter_df_by_accounts(matrix_df, user_accounts)

    # Optional filters carried from Positions page
    selected_account = request.args.get("account", "")
    selected_strategy = request.args.get("strategy", "")
    selected_statuses = request.args.getlist("status")
    selected_start_date = request.args.get("start_date", "")
    selected_end_date = request.args.get("end_date", "")

    start_date = _parse_date(selected_start_date)
    end_date = _parse_date(selected_end_date)

    if selected_account:
        summary_df = summary_df[summary_df["account"] == selected_account]
        trades_df = trades_df[trades_df["account"] == selected_account]
        current_df = current_df[current_df["account"] == selected_account]
    if selected_strategy:
        if "strategy" in summary_df.columns:
            summary_df = summary_df[summary_df["strategy"] == selected_strategy]
        if "strategy" in trades_df.columns:
            trades_df = trades_df[trades_df["strategy"] == selected_strategy]
    if selected_statuses and "status" in summary_df.columns:
        summary_df = summary_df[summary_df["status"].isin(selected_statuses)]
    if start_date is not None and "trade_date" in trades_df.columns:
        trades_df = trades_df[trades_df["trade_date"] >= start_date]
    if end_date is not None and "trade_date" in trades_df.columns:
        trades_df = trades_df[trades_df["trade_date"] <= end_date]

    # ── Session / leg filtering ──
    sessions_df = _filter_df_by_accounts(sessions_df, user_accounts)
    if selected_account and not sessions_df.empty:
        sessions_df = sessions_df[sessions_df["account"] == selected_account]
    for col in ["total_pnl"]:
        if col in sessions_df.columns:
            sessions_df[col] = pd.to_numeric(sessions_df[col], errors="coerce").fillna(0)
    if "open_date" in sessions_df.columns:
        sessions_df["open_date"] = pd.to_datetime(sessions_df["open_date"]).dt.date
    if "last_trade_date" in sessions_df.columns:
        sessions_df["last_trade_date"] = pd.to_datetime(sessions_df["last_trade_date"]).dt.date

    sessions_list = sessions_df.to_dict(orient="records") if not sessions_df.empty else []
    for s in sessions_list:
        s["open_date"] = str(s["open_date"]) if s.get("open_date") else ""
        s["last_trade_date"] = str(s["last_trade_date"]) if s.get("last_trade_date") else ""

    # Enrich sessions with option P&L that overlaps each session's date range
    if sessions_list and not closed_legs_df.empty and "open_date" in closed_legs_df.columns and "total_pnl" in closed_legs_df.columns:
        _cl = closed_legs_df.copy()
        if selected_account:
            _cl = _cl[_cl["account"] == selected_account] if "account" in _cl.columns else _cl
        _cl["_opt_od"] = pd.to_datetime(_cl["open_date"]).dt.date
        _cl["_opt_pnl"] = pd.to_numeric(_cl["total_pnl"], errors="coerce").fillna(0)

        _assigned_option_indices = set()
        for s in sessions_list:
            s_od = pd.to_datetime(s["open_date"]).date() if s["open_date"] else None
            s_ltd = pd.to_datetime(s["last_trade_date"]).date() if s["last_trade_date"] else None
            s_open = str(s.get("status", "")).strip().lower() == "open"
            s_end = s_ltd if not s_open else date.today()
            if s_od and s_end:
                mask = (_cl["_opt_od"] >= s_od) & (_cl["_opt_od"] <= s_end)
                matching = _cl[mask]
                s["options_pnl"] = round(float(matching["_opt_pnl"].sum()), 2)
                s["options_count"] = len(matching)
                _assigned_option_indices.update(matching.index.tolist())
            else:
                s["options_pnl"] = 0.0
                s["options_count"] = 0
            s["equity_pnl"] = round(float(s.get("total_pnl") or 0), 2)
            s["combined_pnl"] = round(s["equity_pnl"] + s["options_pnl"], 2)

        # Orphan options: split into non-overlapping groups around equity sessions
        orphan_mask = ~_cl.index.isin(_assigned_option_indices)
        orphan_df = _cl[orphan_mask]
        if not orphan_df.empty:
            # Build sorted list of equity session boundaries to split orphans around
            eq_boundaries = []
            for s in sessions_list:
                s_od = pd.to_datetime(s["open_date"]).date() if s.get("open_date") else None
                s_ltd = pd.to_datetime(s["last_trade_date"]).date() if s.get("last_trade_date") else None
                s_open = str(s.get("status", "")).strip().lower() == "open"
                s_end = s_ltd if not s_open else date.today()
                if s_od and s_end:
                    eq_boundaries.append((s_od, s_end))
            eq_boundaries.sort()

            # Group orphan options into clusters separated by equity sessions
            orphan_dates_series = orphan_df["_opt_od"].dropna().sort_values()
            orphan_groups = []
            current_group = []
            _orphan_sid_counter = 0

            for idx in orphan_dates_series.index:
                od = orphan_df.loc[idx, "_opt_od"]
                # Check which gap this orphan falls in (before, between, or after sessions)
                gap_id = 0
                for i, (eq_start, eq_end) in enumerate(eq_boundaries):
                    if od >= eq_start:
                        gap_id = i + 1
                if not current_group or current_group[0]["gap"] == gap_id:
                    current_group.append({"idx": idx, "gap": gap_id})
                else:
                    orphan_groups.append(current_group)
                    current_group = [{"idx": idx, "gap": gap_id}]
            if current_group:
                orphan_groups.append(current_group)

            # Deduplicate groups by gap_id
            gap_groups = {}
            for grp in orphan_groups:
                gid = grp[0]["gap"]
                gap_groups.setdefault(gid, []).extend(grp)

            for gid, grp in sorted(gap_groups.items()):
                indices = [g["idx"] for g in grp]
                grp_df = orphan_df.loc[indices]
                grp_pnl = round(float(grp_df["_opt_pnl"].sum()), 2)
                grp_dates = grp_df["_opt_od"].dropna()
                _orphan_sid_counter -= 1
                sessions_list.append({
                    "session_id": _orphan_sid_counter,
                    "open_date": str(grp_dates.min()) if not grp_dates.empty else "",
                    "last_trade_date": str(grp_dates.max()) if not grp_dates.empty else "",
                    "status": "Closed",
                    "total_pnl": grp_pnl,
                    "equity_pnl": 0.0,
                    "options_pnl": grp_pnl,
                    "options_count": len(grp),
                    "combined_pnl": grp_pnl,
                    "days_held": 0,
                    "max_quantity_held": 0,
                    "num_trades": len(grp),
                    "options_only": True,
                })
            sessions_list.sort(key=lambda x: x.get("open_date") or "")
    else:
        for s in sessions_list:
            s["equity_pnl"] = round(float(s.get("total_pnl") or 0), 2)
            s["options_pnl"] = 0.0
            s["options_count"] = 0
            s["combined_pnl"] = s["equity_pnl"]

    # Assign sequential display leg numbers (1, 2, 3...) by chronological order
    for i, s in enumerate(sessions_list, start=1):
        s["display_leg"] = i

    leg_param = request.args.get("leg", "")
    if leg_param:
        selected_legs = []
        for x in leg_param.split(","):
            x = x.strip()
            try:
                selected_legs.append(int(x))
            except ValueError:
                pass
    else:
        selected_legs = [s["session_id"] for s in sessions_list]

    # Build date ranges for selected sessions
    _leg_ranges = []
    _has_open_leg = False
    for s in sessions_list:
        if s["session_id"] in selected_legs:
            od = pd.to_datetime(s["open_date"]).date() if s["open_date"] else None
            ltd = pd.to_datetime(s["last_trade_date"]).date() if s["last_trade_date"] else None
            is_open = str(s.get("status", "")).strip().lower() == "open"
            if is_open:
                _has_open_leg = True
            _leg_ranges.append((od, ltd if not is_open else date.today()))

    def _in_leg_range(d):
        """Return True if date d falls within any selected leg's date range."""
        if not _leg_ranges:
            return True
        for lo, hi in _leg_ranges:
            if lo and hi and lo <= d <= hi:
                return True
            if lo and not hi and d >= lo:
                return True
        return False

    # Snapshot before leg filter so hero + chart can use full symbol history
    # when the selected leg has no trade rows in-range (common for new option legs).
    trades_pre_leg = trades_df.copy()

    # Apply leg filter to trades
    if leg_param and "trade_date" in trades_df.columns and _leg_ranges:
        trades_df = trades_df[trades_df["trade_date"].apply(_in_leg_range)]

    # Apply leg filter to current positions (only show if an open leg is selected)
    if leg_param and not _has_open_leg:
        current_df = current_df.iloc[0:0]

    # For open equity positions: if cost_basis is missing/zero, derive from trade history
    # so unrealized P&L = market_value - cost_basis (true P/L for open positions)
    if not current_df.empty and not trades_df.empty and "action" in trades_df.columns:
        for idx, row in current_df.iterrows():
            if row.get("instrument_type") != "Equity":
                continue
            cost_basis = float(row.get("cost_basis") or 0)
            market_value = float(row.get("market_value") or 0)
            if market_value <= 0:
                continue
            if cost_basis is None or cost_basis == 0:
                acct, sym = row.get("account"), row.get("symbol")
                buys = trades_df[
                    (trades_df["account"] == acct)
                    & (trades_df["symbol"] == sym)
                    & (trades_df["action"].astype(str).str.lower().str.strip() == "buy")
                ]
                if not buys.empty:
                    cost_basis = abs(float(buys["amount"].sum()))
                    current_df.at[idx, "cost_basis"] = cost_basis
                    current_df.at[idx, "unrealized_pnl"] = market_value - cost_basis
                    if cost_basis:
                        current_df.at[idx, "unrealized_pnl_pct"] = 100.0 * (market_value - cost_basis) / cost_basis

    # ── Filter closed legs early so KPIs can use them ──
    if selected_account:
        if not closed_legs_df.empty:
            closed_legs_df = closed_legs_df[closed_legs_df["account"] == selected_account]
        if not closed_equity_df.empty:
            closed_equity_df = closed_equity_df[closed_equity_df["account"] == selected_account]
    if selected_strategy and not closed_legs_df.empty and "strategy" in closed_legs_df.columns:
        closed_legs_df = closed_legs_df[closed_legs_df["strategy"] == selected_strategy]
    # Before leg scoping, keep copies for "first/last activity" on the page
    closed_legs_pre_leg = closed_legs_df.copy()
    closed_equity_pre_leg = closed_equity_df.copy()
    if leg_param and _leg_ranges:
        if not closed_legs_df.empty and "open_date" in closed_legs_df.columns:
            closed_legs_df["_od"] = pd.to_datetime(closed_legs_df["open_date"]).dt.date
            closed_legs_df = closed_legs_df[closed_legs_df["_od"].apply(_in_leg_range)]
            closed_legs_df = closed_legs_df.drop(columns=["_od"])
        if not closed_equity_df.empty and "session_id" in closed_equity_df.columns:
            closed_equity_df = closed_equity_df[closed_equity_df["session_id"].isin(selected_legs)]

    # Min/max activity for hero + chart when summary/leg filter hides dates (e.g. open option leg).
    _activity_all_dates = _collect_activity_candidate_dates(
        trades_pre_leg, closed_legs_pre_leg, closed_equity_pre_leg, sessions_list
    )
    _activity_date_min = min(_activity_all_dates) if _activity_all_dates else None
    _activity_date_max = max(_activity_all_dates) if _activity_all_dates else None

    # Status (needed for open-only realized logic)
    status_col = None
    for c in ("status", "Status", "STATUS"):
        if c in (summary_df.columns if not summary_df.empty else []):
            status_col = c
            break
    statuses = summary_df[status_col].unique().tolist() if status_col and not summary_df.empty else []
    _has_open = any(str(s).strip().lower() == "open" for s in statuses if s is not None)
    _has_closed = any(str(s).strip().lower() == "closed" for s in statuses if s is not None)
    # Open equity/options from snapshots have no positions_summary row until trades exist in stg_history.
    if not _has_open and not current_df.empty:
        _has_open = True
    if _has_open:
        overall_status = "Open"
    else:
        overall_status = "Closed"

    # When leg filter is active, override overall_status based on selected sessions
    if leg_param:
        overall_status = "Open" if _has_open_leg else "Closed"

    # ── KPIs and Strategy Rows ──
    # When leg filter is active, recompute from filtered trade data instead of summary_df
    if leg_param and _leg_ranges:
        # Filter summary_df by date overlap with selected leg ranges
        if not summary_df.empty and "first_trade_date" in summary_df.columns:
            summary_df["_ftd"] = pd.to_datetime(summary_df["first_trade_date"]).dt.date
            summary_df = summary_df[summary_df["_ftd"].apply(_in_leg_range)]
            summary_df = summary_df.drop(columns=["_ftd"])

    total_winners = int(summary_df["num_winners"].sum()) if not summary_df.empty else 0
    total_losers = int(summary_df["num_losers"].sum()) if not summary_df.empty else 0
    total_closed = total_winners + total_losers

    _sell_actions = ("equity_sell", "option_sell_to_close", "option_buy_to_close")
    has_sell_trades = (
        not trades_df.empty
        and "action" in trades_df.columns
        and trades_df["action"].astype(str).str.strip().isin(_sell_actions).any()
    )
    # True only for snapshot-only / no-history edge cases (not "any open position").
    is_open_only = (total_closed == 0 and not current_df.empty) or (
        not has_sell_trades and not current_df.empty
    )

    if leg_param and _leg_ranges:
        realized_for_display = _realized_pnl_from_closed_frames(
            closed_legs_df, closed_equity_df
        )
    else:
        has_closed_frame = (not closed_legs_pre_leg.empty) or (
            not closed_equity_pre_leg.empty
        )
        if has_closed_frame:
            realized_for_display = _realized_pnl_from_closed_frames(
                closed_legs_pre_leg, closed_equity_pre_leg
            )
        else:
            realized_for_display = (
                float(summary_df["realized_pnl"].sum()) if not summary_df.empty else 0.0
            )

    if app.debug and symbol == "ATZAF":
        app.logger.warning(
            "position_detail ATZAF: status_col=%s overall_status=%s total_closed=%s is_open_only=%s realized_for_display=%s",
            status_col, overall_status, total_closed, is_open_only, realized_for_display,
        )

    kpis = {}
    # positions_summary is trade-derived; open lots synced without matching history have current_df only.
    _show_position_kpis = (
        leg_param
        or not summary_df.empty
        or not current_df.empty
        or not trades_df.empty
    )
    if _show_position_kpis:
        unrealized_from_summary = float(summary_df["unrealized_pnl"].sum()) if not summary_df.empty else 0.0
        if not current_df.empty and "unrealized_pnl" in current_df.columns:
            unrealized_from_summary = float(current_df["unrealized_pnl"].sum())

        # When leg-filtered, premium = filtered closed options only (never full-history
        # legs when the filtered frame is empty for that range).
        if leg_param and _leg_ranges:
            if not closed_legs_df.empty:
                pr, pp = _premium_totals_from_closed_options(closed_legs_df)
            else:
                pr, pp = 0.0, 0.0
            premium_collected, premium_paid = pr, pp
        else:
            pr, pp = _premium_totals_from_closed_options(closed_legs_pre_leg)
            premium_collected, premium_paid = pr, pp
            if (premium_collected == 0.0 and premium_paid == 0.0) and not summary_df.empty:
                premium_collected = float(summary_df["total_premium_received"].sum())
                premium_paid = float(summary_df["total_premium_paid"].sum())

        # Trade count: use row count when summary is empty (e.g. Schwab positions-only path)
        if leg_param or summary_df.empty:
            trade_count = len(trades_df)
            if trade_count == 0 and not trades_pre_leg.empty:
                trade_count = len(trades_pre_leg)
        else:
            trade_count = int(
                summary_df["num_individual_trades"].sum()
            ) if "num_individual_trades" in summary_df.columns else 0
            if trade_count == 0 and not trades_pre_leg.empty:
                trade_count = len(trades_pre_leg)

        # Date range: prefer stg (trades_pre_leg) when present — positions_summary can lag
        # and show 0 trades + a bogus same-day "first" and "last" as-of stamp.
        if leg_param and not trades_df.empty and "trade_date" in trades_df.columns:
            first_trade = str(trades_df["trade_date"].min())[:10]
            last_trade = str(trades_df["trade_date"].max())[:10]
        elif (not leg_param) and (not trades_pre_leg.empty) and "trade_date" in trades_pre_leg.columns:
            first_trade = str(trades_pre_leg["trade_date"].min())[:10]
            last_trade = str(trades_pre_leg["trade_date"].max())[:10]
        elif not summary_df.empty and "first_trade_date" in summary_df.columns:
            first_trade = str(pd.to_datetime(summary_df["first_trade_date"].min()).date())
            last_trade = str(pd.to_datetime(summary_df["last_trade_date"].max()).date())
        elif not trades_df.empty and "trade_date" in trades_df.columns:
            first_trade = str(trades_df["trade_date"].min())[:10]
            last_trade = str(trades_df["trade_date"].max())[:10]
        else:
            first_trade = ""
            last_trade = ""
        if (not first_trade) and _activity_date_min is not None:
            first_trade = str(_activity_date_min)
        if (not last_trade) and _activity_date_max is not None:
            last_trade = str(_activity_date_max)

        # Open, still no real range (e.g. only summary as-of) — session open + through today
        if (
            not leg_param
            and overall_status == "Open"
            and sessions_list
            and (not first_trade or (first_trade == last_trade and trade_count == 0))
        ):
            ods = []
            for s in sessions_list:
                if str(s.get("status", "")).strip().lower() == "open" and s.get("open_date"):
                    try:
                        ods.append(pd.to_datetime(s["open_date"]).date())
                    except Exception:
                        pass
            if ods:
                d0 = min(ods)
                first_trade = str(d0)[:10]
                last_trade = str(date.today())

        # Stg row count for hero; if summary says 0 trades but legs exist, show leg count.
        _fills = len(trades_pre_leg) if not trades_pre_leg.empty else 0
        _n_legs = (
            (len(closed_legs_pre_leg) if not closed_legs_pre_leg.empty else 0)
            + (len(closed_equity_pre_leg) if not closed_equity_pre_leg.empty else 0)
            + (len(current_df) if not current_df.empty else 0)
        )
        if _fills > 0:
            trade_count = _fills
        elif trade_count == 0 and _n_legs > 0:
            trade_count = _n_legs

        # Win/loss: from filtered closed legs when leg-filtered; otherwise from all
        # symbol closed legs (positions_summary is wrong when open rows mask closed stats).
        if leg_param and _leg_ranges:
            opt_wins = int((closed_legs_df["total_pnl"] > 0).sum()) if not closed_legs_df.empty and "total_pnl" in closed_legs_df.columns else 0
            opt_losses = int((closed_legs_df["total_pnl"] <= 0).sum()) if not closed_legs_df.empty and "total_pnl" in closed_legs_df.columns else 0
            eq_wins = int((closed_equity_df["realized_pnl"] > 0).sum()) if not closed_equity_df.empty and "realized_pnl" in closed_equity_df.columns else 0
            eq_losses = int((closed_equity_df["realized_pnl"] <= 0).sum()) if not closed_equity_df.empty and "realized_pnl" in closed_equity_df.columns else 0
            total_winners = opt_wins + eq_wins
            total_losers = opt_losses + eq_losses
            total_closed = total_winners + total_losers
        elif (not closed_legs_pre_leg.empty) or (not closed_equity_pre_leg.empty):
            opt_wins = int((closed_legs_pre_leg["total_pnl"] > 0).sum()) if not closed_legs_pre_leg.empty and "total_pnl" in closed_legs_pre_leg.columns else 0
            opt_losses = int((closed_legs_pre_leg["total_pnl"] <= 0).sum()) if not closed_legs_pre_leg.empty and "total_pnl" in closed_legs_pre_leg.columns else 0
            eq_wins = int((closed_equity_pre_leg["realized_pnl"] > 0).sum()) if not closed_equity_pre_leg.empty and "realized_pnl" in closed_equity_pre_leg.columns else 0
            eq_losses = int((closed_equity_pre_leg["realized_pnl"] <= 0).sum()) if not closed_equity_pre_leg.empty and "realized_pnl" in closed_equity_pre_leg.columns else 0
            total_winners = opt_wins + eq_wins
            total_losers = opt_losses + eq_losses
            total_closed = total_winners + total_losers

        avg_days_val = float(summary_df["avg_days_in_trade"].mean()) if not summary_df.empty else 0.0
        if pd.isna(avg_days_val):
            avg_days_val = 0.0
        if (not closed_legs_pre_leg.empty) and "days_in_trade" in closed_legs_pre_leg.columns:
            d_alt = float(closed_legs_pre_leg["days_in_trade"].fillna(0).mean() or 0.0)
            if d_alt > 0 and avg_days_val == 0.0:
                avg_days_val = d_alt

        div_income = (
            float(summary_df["total_dividend_income"].sum()) if not summary_df.empty else 0.0
        )

        kpis = {
            "total_return": realized_for_display + unrealized_from_summary + div_income,
            "realized_pnl": realized_for_display,
            "unrealized_pnl": unrealized_from_summary,
            "premium_collected": premium_collected,
            "premium_paid": premium_paid,
            "dividend_income": div_income,
            "win_rate": total_winners / total_closed if total_closed else 0,
            "avg_days": avg_days_val,
            "total_trades": trade_count,
            "num_winners": total_winners,
            "num_losers": total_losers,
            "first_trade": first_trade,
            "last_trade": last_trade,
        }

    # Strategy rows: positions_summary plus any (account, strategy) seen in closed legs
    # but missing from the mart, so open + closed strategies all appear.
    _cl_for_strat = closed_legs_pre_leg if not leg_param else closed_legs_df
    _eq_for_strat = closed_equity_pre_leg if not leg_param else closed_equity_df
    merged_strategy_df = _merge_position_strategy_breakdown(
        safe_symbol, summary_df, _cl_for_strat, _eq_for_strat
    )
    strategy_rows = (
        merged_strategy_df.to_dict(orient="records")
        if not merged_strategy_df.empty
        else []
    )

    # Build chart data from pre-aggregated mart_daily_pnl
    chart_data = {"dates": [], "equity": [], "options": [], "dividends": [], "total": [], "underlying_price": [], "has_underlying_price": False}
    prices_through_date = None
    try:
        acct_filter = _account_sql_and([selected_account] if selected_account else user_accounts)
        chart_df = client.query(
            CHART_DATA_QUERY.format(symbol=safe_symbol, account_filter=acct_filter)
        ).to_dataframe()
        # Filter chart data by selected session date ranges and re-zero cumulative columns
        if leg_param and _leg_ranges and not chart_df.empty and "date" in chart_df.columns:
            chart_df["_d"] = pd.to_datetime(chart_df["date"]).dt.date
            chart_df = chart_df[chart_df["_d"].apply(_in_leg_range)].copy()
            chart_df = chart_df.drop(columns=["_d"])
            if not chart_df.empty:
                for cum_col in ("cumulative_options_pnl", "cumulative_dividends_pnl", "cumulative_other_pnl"):
                    if cum_col in chart_df.columns:
                        baseline = float(chart_df[cum_col].iloc[0] or 0)
                        chart_df[cum_col] = chart_df[cum_col].astype(float) - baseline
                # Snapshot market value / cost basis covers ALL open options for the
                # symbol, not just those in the selected leg. Null them out so the
                # chart uses only the re-zeroed cash flows for this leg's P&L.
                for snap_col in ("option_market_value", "option_cost_basis"):
                    if snap_col in chart_df.columns:
                        chart_df[snap_col] = None
        if not chart_df.empty:
            chart_data = _build_chart_from_daily_pnl(chart_df, current_df)
            # Latest date we have close_price for (from pipeline); user can run current_position_stock_price.py to refresh
            if "date" in chart_df.columns:
                prices_through_date = str(chart_df["date"].max())[:10]
    except Exception as exc:
        app.logger.exception(
            "position_detail chart query or build failed for %s: %s", safe_symbol, exc
        )

    # Prefer stg/leg when they have more calendar coverage than mart (e.g. mart has 2–3
    # recent days while closed legs span years). Compare both; tie-break -> leg. stg can
    # still be 2 days if only recent fills — do not pick stg before comparing n_leg vs n_stg.
    _chart_dates = chart_data.get("dates") or []
    n_m = len(_chart_dates)
    ch_stg = _cumulative_pnl_from_stg_trades(trades_pre_leg, current_df) if not trades_pre_leg.empty else None
    n_stg = len(ch_stg["dates"]) if ch_stg and ch_stg.get("dates") else 0
    ch_leg = _cumulative_pnl_from_leg_closes(closed_legs_pre_leg, closed_equity_pre_leg)
    n_leg = len(ch_leg["dates"]) if ch_leg and ch_leg.get("dates") else 0
    cands = []
    if ch_leg and n_leg >= 2:
        cands.append((n_leg, "leg", ch_leg))
    if ch_stg and n_stg >= 2:
        cands.append((n_stg, "stg", ch_stg))
    if cands:
        cands.sort(key=lambda t: (-t[0], 0 if t[1] == "leg" else 1))
        best_n = cands[0][0]
        if best_n > n_m or (n_m <= 2 and best_n >= 2):
            chart_data = cands[0][2]

    # Chart.js needs at least two x values to draw a line; a single mart day
    # (e.g. new option leg) would otherwise show only a blank chart.
    _chart_dates = chart_data.get("dates") or []
    if kpis and (not _chart_dates or len(_chart_dates) < 2):
        chart_data = _synthetic_cumulative_pnl_for_position(
            kpis, sessions_list, leg_param, selected_legs, current_df
        )

    if kpis:
        _align_position_pnl_chart_with_kpi(chart_data, kpis)

    # Trade history rows
    trades_for_table = trades_df.copy()
    if "trade_date" in trades_for_table.columns:
        trades_for_table["trade_date"] = trades_for_table["trade_date"].astype(str)
    trades = trades_for_table.to_dict(orient="records") if not trades_for_table.empty else []

    # Current positions
    current_positions = current_df.to_dict(orient="records") if not current_df.empty else []

    # ── Closed option legs (with cost/proceeds) ──
    closed_legs_list = []
    if not closed_legs_df.empty:
        closed_legs_list = closed_legs_df.sort_values("close_date").to_dict(orient="records")
        for r in closed_legs_list:
            r["open_date"] = str(r["open_date"]) if pd.notna(r.get("open_date")) else ""
            r["close_date"] = str(r["close_date"]) if pd.notna(r.get("close_date")) else ""
            r["total_pnl"] = round(float(r.get("total_pnl") or 0), 2)

    # ── Closed equity legs ──
    closed_equity_list = []
    if not closed_equity_df.empty:
        closed_equity_list = closed_equity_df.sort_values("close_date").to_dict(orient="records")
        for r in closed_equity_list:
            r["open_date"] = str(r["open_date"]) if pd.notna(r.get("open_date")) else ""
            r["close_date"] = str(r["close_date"]) if pd.notna(r.get("close_date")) else ""
            r["realized_pnl"] = round(float(r.get("realized_pnl") or 0), 2)

    # ── Trade Outcomes ──
    trade_outcomes = []
    for leg in closed_legs_list:
        direction = str(leg.get("direction") or "")
        prem_recv = float(leg.get("premium_received") or 0)
        prem_paid = float(leg.get("premium_paid") or 0)
        cost_close = float(leg.get("cost_to_close") or 0)
        proceeds_close = float(leg.get("proceeds_from_close") or 0)
        if direction == "Sold":
            o_cost = abs(cost_close)
            o_proceeds = abs(prem_recv)
        else:
            o_cost = abs(prem_paid)
            o_proceeds = abs(proceeds_close)
        o_pnl = float(leg.get("total_pnl") or 0)
        o_return = round(o_pnl / o_cost * 100, 1) if o_cost else None
        trade_outcomes.append({
            "trade_symbol": leg.get("trade_symbol"),
            "strategy": leg.get("strategy") or "",
            "direction": direction,
            "close_type": str(leg.get("close_type") or ""),
            "open_date": leg.get("open_date") or "",
            "close_date": leg.get("close_date") or "",
            "days_held": leg.get("days_in_trade"),
            "quantity": leg.get("quantity"),
            "cost": round(o_cost, 2),
            "proceeds": round(o_proceeds, 2),
            "pnl": round(o_pnl, 2),
            "return_pct": o_return,
            "is_winner": o_pnl > 0,
            "type": "option",
        })
    for leg in closed_equity_list:
        eq_proceeds = float(leg.get("sell_proceeds") or 0)
        eq_cost = float(leg.get("cost_basis") or 0)
        eq_pnl = float(leg.get("realized_pnl") or 0)
        eq_return = round(eq_pnl / eq_cost * 100, 1) if eq_cost else None
        od = leg.get("open_date") or ""
        cd = leg.get("close_date") or ""
        try:
            days = (pd.to_datetime(cd) - pd.to_datetime(od)).days if od and cd else None
        except Exception:
            days = None
        trade_outcomes.append({
            "trade_symbol": leg.get("trade_symbol") or symbol,
            "strategy": leg.get("description") or "Equity Sold",
            "direction": "Sold",
            "close_type": "Sold",
            "open_date": od,
            "close_date": cd,
            "days_held": days,
            "quantity": leg.get("quantity"),
            "cost": round(eq_cost, 2),
            "proceeds": round(eq_proceeds, 2),
            "pnl": round(eq_pnl, 2),
            "return_pct": eq_return,
            "is_winner": eq_pnl > 0,
            "type": "equity",
            "session_id": leg.get("session_id"),
        })
    trade_outcomes.sort(key=lambda x: x.get("close_date") or "", reverse=True)

    # Attach raw transactions to each outcome for drill-down
    # Build session date range lookup for scoping equity trades
    _session_ranges = {}
    for s in sessions_list:
        sid = s.get("session_id")
        if sid is not None:
            s_od = pd.to_datetime(s["open_date"]).date() if s.get("open_date") else None
            s_ltd = pd.to_datetime(s["last_trade_date"]).date() if s.get("last_trade_date") else None
            s_open = str(s.get("status", "")).strip().lower() == "open"
            _session_ranges[sid] = (s_od, s_ltd if not s_open else date.today())

    trades_by_symbol = {}
    for t in trades:
        ts = str(t.get("trade_symbol") or "")
        trades_by_symbol.setdefault(ts, []).append(t)
    for o in trade_outcomes:
        ts = str(o.get("trade_symbol") or "")
        if o["type"] == "option":
            matching = trades_by_symbol.get(ts, [])
        else:
            # Scope equity raw trades to this outcome's session date range
            sid = o.get("session_id")
            s_range = _session_ranges.get(sid)
            eq_trades = [
                t for t in trades
                if str(t.get("instrument_type") or "") == "Equity"
                and str(t.get("trade_symbol") or "") == ts
            ]
            if s_range and s_range[0]:
                matching = []
                for t in eq_trades:
                    try:
                        td = pd.to_datetime(t.get("trade_date")).date()
                    except Exception:
                        continue
                    if s_range[0] <= td <= (s_range[1] or date.today()):
                        matching.append(t)
            else:
                matching = eq_trades
        o["raw_trades"] = sorted(matching, key=lambda t: str(t.get("trade_date") or ""))

    # Assign leg numbers to trade outcomes and open positions
    def _date_to_leg(d_str):
        """Return display_leg number for a date string, or None.
        Prefers equity sessions over orphan (options-only) sessions to avoid
        the orphan's wide date range swallowing trades that belong to a real session."""
        if not d_str or not sessions_list:
            return None
        try:
            d = pd.to_datetime(d_str).date()
        except Exception:
            return None
        # First pass: check equity sessions (non-orphan)
        for s in sessions_list:
            if s.get("options_only"):
                continue
            s_od = pd.to_datetime(s["open_date"]).date() if s.get("open_date") else None
            s_ltd = pd.to_datetime(s["last_trade_date"]).date() if s.get("last_trade_date") else None
            s_open = str(s.get("status", "")).strip().lower() == "open"
            s_end = s_ltd if not s_open else date.today()
            if s_od and s_end and s_od <= d <= s_end:
                return s["display_leg"]
        # Second pass: fall back to orphan (options-only) sessions
        for s in sessions_list:
            if not s.get("options_only"):
                continue
            s_od = pd.to_datetime(s["open_date"]).date() if s.get("open_date") else None
            s_ltd = pd.to_datetime(s["last_trade_date"]).date() if s.get("last_trade_date") else None
            s_end = s_ltd or date.today()
            if s_od and s_end and s_od <= d <= s_end:
                return s["display_leg"]
        return None

    for o in trade_outcomes:
        o["leg_num"] = _date_to_leg(o.get("open_date") or o.get("close_date"))
    for p in current_positions:
        # Open positions belong to the latest open session
        open_sessions = [s for s in sessions_list if str(s.get("status", "")).strip().lower() == "open"]
        p["leg_num"] = open_sessions[-1]["display_leg"] if open_sessions else (sessions_list[-1]["display_leg"] if sessions_list else None)

    # ── Option matrices (DTE × Strike Distance heatmap) ──
    if selected_account:
        matrix_df = matrix_df[matrix_df["account"] == selected_account] if not matrix_df.empty else matrix_df
    # Filter matrix by selected legs (date range overlap via trade_symbol matching closed legs)
    if leg_param and _leg_ranges and not matrix_df.empty:
        filtered_trade_syms = set(r.get("trade_symbol") for r in closed_legs_list)
        if "trade_symbol" in matrix_df.columns:
            matrix_df = matrix_df[matrix_df["trade_symbol"].isin(filtered_trade_syms)]
    option_matrices = _build_option_matrices(
        matrix_df, selected_account or (user_accounts[0] if len(user_accounts) == 1 else ""), symbol
    ) if not matrix_df.empty else []
    if not option_matrices and not matrix_df.empty:
        all_mats = []
        for acct in matrix_df["account"].unique():
            all_mats.extend(_build_option_matrices(matrix_df, acct, symbol))
        seen = set()
        for m in all_mats:
            if m["strategy"] not in seen:
                seen.add(m["strategy"])
                option_matrices.append(m)

    # Available accounts for filter (summary may be empty for open-only Schwab lots)
    if not summary_df.empty and "account" in summary_df.columns:
        all_accounts = sorted(summary_df["account"].dropna().unique())
    elif not current_df.empty and "account" in current_df.columns:
        all_accounts = sorted(current_df["account"].dropna().unique())
    else:
        all_accounts = []

    return render_template(
        "position_detail.html",
        symbol=symbol,
        kpis=kpis,
        overall_status=overall_status,
        strategy_rows=strategy_rows,
        trades=trades,
        trade_outcomes=trade_outcomes,
        current_positions=current_positions,
        option_matrices=option_matrices,
        sessions=sessions_list,
        selected_legs=selected_legs,
        leg_param=leg_param,
        chart_data_json=json.dumps(_chart_data_for_json(chart_data)),
        has_underlying_price=chart_data.get("has_underlying_price", False),
        prices_through_date=prices_through_date,
        accounts=all_accounts,
        selected_account=selected_account,
    )


# ======================================================================
# Daily Position Detail  (/symbols)
# ======================================================================

TRADES_QUERY = """
    SELECT
        account,
        underlying_symbol AS symbol,
        trade_date,
        action,
        action_raw,
        trade_symbol,
        instrument_type,
        description,
        quantity,
        price,
        fees,
        amount
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE underlying_symbol IS NOT NULL
      AND trade_date IS NOT NULL
      {account_filter}
    ORDER BY underlying_symbol, trade_date
"""

OPEN_SESSION_START_QUERY = """
    SELECT
        account,
        symbol,
        MIN(open_date) AS open_start
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE status = 'Open'
      {account_filter}
    GROUP BY account, symbol
"""


CLOSED_LEGS_QUERY = """
    SELECT
        sc.account,
        sc.symbol,
        sc.strategy,
        sc.trade_symbol,
        sc.open_date,
        sc.close_date,
        sc.total_pnl,
        sc.status,
        oc.contracts_sold_to_open + oc.contracts_bought_to_open AS quantity,
        oc.premium_received,
        oc.premium_paid,
        oc.cost_to_close,
        oc.proceeds_from_close,
        oc.direction,
        oc.close_type,
        oc.days_in_trade
    FROM `ccwj-dbt.analytics.int_strategy_classification` sc
    JOIN `ccwj-dbt.analytics.int_option_contracts` oc
      ON sc.account = oc.account AND sc.trade_symbol = oc.trade_symbol
    WHERE sc.status = 'Closed'
      AND sc.trade_group_type = 'option_contract'
      {closed_legs_account_filter}
"""

CLOSED_EQUITY_LEGS_QUERY = """
    SELECT
        account,
        symbol,
        trade_symbol,
        open_date,
        close_date,
        quantity,
        sale_price_per_share,
        sell_proceeds,
        cost_basis,
        realized_pnl,
        description
    FROM `ccwj-dbt.analytics.int_closed_equity_legs`
    WHERE 1=1 {account_filter}
"""

CURRENT_POSITIONS_QUERY = """
    SELECT
        account,
        underlying_symbol AS symbol,
        instrument_type,
        trade_symbol,
        description,
        quantity,
        current_price,
        market_value,
        cost_basis,
        unrealized_pnl,
        unrealized_pnl_pct
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE 1=1 {account_filter}
"""

STRATEGIES_MAP_QUERY = """
    SELECT account, symbol, strategy
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {account_filter}
"""

SYMBOLS_PNL_QUERY = """
    SELECT account, symbol, status, realized_pnl, unrealized_pnl
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {account_filter}
"""

def _build_option_matrices(matrix_df, account, symbol):
    """Build per-strategy DTE × Strike-Distance heatmap matrices for one position."""
    import math

    df = matrix_df[
        (matrix_df["account"] == account)
        & (matrix_df["underlying_symbol"] == symbol)
    ].copy()
    if df.empty:
        return []

    DTE_BINS = [
        (0, 7, "0–7"),
        (8, 14, "8–14"),
        (15, 30, "15–30"),
        (31, 60, "31–60"),
        (61, 999, "61+"),
    ]

    def dte_label(dte):
        for lo, hi, lbl in DTE_BINS:
            if lo <= dte <= hi:
                return lbl
        return "61+"

    def strike_bucket(dist):
        """Round strike distance to nearest integer for column headers."""
        return int(round(dist))

    matrices = []
    for strategy, grp in df.groupby("strategy"):
        grp = grp.copy()
        grp["dte_label"] = grp["dte_at_open"].apply(dte_label)
        grp["strike_col"] = grp["strike_distance"].apply(strike_bucket)

        min_col = grp["strike_col"].min()
        max_col = grp["strike_col"].max()
        col_range = list(range(min_col, max_col + 1))

        dte_order = [lbl for _, _, lbl in DTE_BINS if lbl in grp["dte_label"].values]

        rows = []
        for dte_lbl in dte_order:
            cells = []
            for col_val in col_range:
                bucket = grp[
                    (grp["dte_label"] == dte_lbl) & (grp["strike_col"] == col_val)
                ]
                if bucket.empty:
                    cells.append({"count": 0, "avg_pnl": None, "win_rate": None})
                else:
                    wins = int((bucket["total_pnl"] > 0).sum())
                    total = len(bucket)
                    avg_pnl_dollar = bucket["total_pnl"].mean()
                    cells.append({
                        "count": total,
                        "avg_pnl": round(float(avg_pnl_dollar), 0) if not math.isnan(avg_pnl_dollar) else None,
                        "win_rate": round(wins / total * 100, 0),
                        "wins": wins,
                    })
            rows.append({"dte_label": dte_lbl, "cells": cells})

        matrices.append({
            "strategy": strategy,
            "trade_count": len(grp),
            "col_headers": col_range,
            "rows": rows,
        })

    return matrices


def _chart_data_for_json(obj):
    """Recursively make chart data JSON/JS-safe (NaN/Inf break Chart.js parsing)."""
    if isinstance(obj, dict):
        return {k: _chart_data_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_chart_data_for_json(x) for x in obj]
    if isinstance(obj, bool) or obj is None:
        return obj
    if isinstance(obj, int):
        return obj
    try:
        f = float(obj)
    except (TypeError, ValueError):
        return obj
    if not math.isfinite(f):
        return None
    return f


def _collect_activity_candidate_dates(
    trades_pre_leg, closed_legs_pre_leg, closed_equity_pre_leg, sessions_list
):
    """
    Dates that represent when the user first/last touched this symbol, using
    trades and strategy metadata before leg scoping. Used when summary/stg rows
    are missing or leg-filtered to empty.
    """
    out = []
    if (
        trades_pre_leg is not None
        and not trades_pre_leg.empty
        and "trade_date" in trades_pre_leg.columns
    ):
        td = pd.to_datetime(trades_pre_leg["trade_date"], errors="coerce")
        out.extend([x for x in td.dropna().dt.date.tolist() if x is not None])
    for df, cols in (
        (closed_legs_pre_leg, ("open_date", "close_date")),
        (closed_equity_pre_leg, ("open_date", "close_date")),
    ):
        if df is None or df.empty:
            continue
        for c in cols:
            if c not in df.columns:
                continue
            for v in df[c].dropna():
                ts = pd.to_datetime(v, errors="coerce")
                if pd.isna(ts):
                    continue
                try:
                    out.append(ts.date())
                except Exception:
                    pass
    for s in sessions_list or []:
        for key in ("open_date", "last_trade_date"):
            v = s.get(key)
            if not v:
                continue
            ts = pd.to_datetime(v, errors="coerce")
            if pd.isna(ts):
                continue
            try:
                out.append(ts.date())
            except Exception:
                pass
    return [d for d in out if d is not None]


def _synthetic_cumulative_pnl_for_position(kpis, sessions_list, leg_param, selected_legs, current_df):
    """
    When mart_daily_pnl has no rows in-range (new leg, pipeline lag, leg filter) or
    the chart query failed, draw a 2-point cumulative P&L line consistent with KPIs.
    """
    empty = {
        "dates": [], "equity": [], "options": [], "dividends": [],
        "total": [], "underlying_price": [], "has_underlying_price": False,
    }
    if not kpis:
        return empty

    realized = float(kpis.get("realized_pnl") or 0)
    unreal = float(kpis.get("unrealized_pnl") or 0)
    div_d = float(kpis.get("dividend_income") or 0)
    tot_end = round(float(kpis.get("total_return") or 0), 2)

    eq_unreal = 0.0
    opt_unreal = 0.0
    if (
        not current_df.empty
        and "instrument_type" in current_df.columns
        and "unrealized_pnl" in current_df.columns
    ):
        eq_df = current_df[current_df["instrument_type"] == "Equity"]
        op_df = current_df[current_df["instrument_type"].isin(["Call", "Put"])]
        if not eq_df.empty:
            eq_unreal = float(eq_df["unrealized_pnl"].sum())
        if not op_df.empty:
            opt_unreal = float(op_df["unrealized_pnl"].sum())
    if abs(eq_unreal + opt_unreal - unreal) > 0.02:
        eq_unreal, opt_unreal = unreal, 0.0

    eq_end = round(realized + eq_unreal, 2)
    opt_end = round(opt_unreal, 2)

    start_d = None
    if leg_param and sessions_list and selected_legs:
        ods = []
        for s in sessions_list:
            if s.get("session_id") in selected_legs and s.get("open_date"):
                try:
                    ods.append(pd.to_datetime(s["open_date"]).date())
                except Exception:
                    pass
        if ods:
            start_d = min(ods)
    if start_d is None and kpis.get("first_trade"):
        try:
            start_d = pd.to_datetime(kpis["first_trade"]).date()
        except Exception:
            start_d = None

    end_d = date.today()
    if start_d is None:
        start_d = end_d - timedelta(days=1) if end_d > date(2000, 1, 2) else end_d
    if start_d > end_d:
        start_d = end_d
    if start_d == end_d:
        start_d = end_d - timedelta(days=1) if end_d > date(2000, 1, 2) else end_d

    d0, d1 = str(start_d), str(end_d)

    p0, p1 = None, None
    if not current_df.empty and "instrument_type" in current_df.columns and "current_price" in current_df.columns:
        eqp = current_df[current_df["instrument_type"] == "Equity"]
        if not eqp.empty:
            c = float(eqp["current_price"].iloc[0] or 0)
            if c > 0:
                p1 = round(c, 2)

    return {
        "dates": [d0, d1],
        "equity": [0.0, eq_end],
        "options": [0.0, opt_end],
        "dividends": [0.0, div_d],
        "total": [0.0, tot_end],
        "underlying_price": [p0, p1],
        "has_underlying_price": p1 is not None,
    }


def _align_position_pnl_chart_with_kpi(chart_data, kpis):
    """
    `mart_daily_pnl` is full cumulative history for account + symbol. The page KPIs
    can be scoped with status=Open, strategy, or a leg, so the chart can show a
    much larger total (e.g. all closed + open) than the hero row. When the
    series end disagrees, scale equity/options/dividend components to match
    `kpis['total_return']` and re-sum `total` (leave underlying_price alone).
    """
    if not chart_data or not kpis or not chart_data.get("total"):
        return
    n = len(chart_data["total"])
    if n < 1:
        return
    t_end = float(chart_data["total"][-1] or 0.0)
    k = float(kpis.get("total_return", 0) or 0.0)
    if abs(t_end - k) <= 0.02:
        return
    if abs(t_end) < 1e-9:
        # e.g. leg staircase is realized closed only, hero is open-only unreal — can't scale
        if abs(k) > 0.02 and n >= 1:
            tlist = [0.0] * (n - 1) + [round(k, 2)]
            chart_data["total"] = tlist
            e_abs = sum(
                abs(float(x or 0)) for x in (chart_data.get("equity") or [0.0] * n)[:n]
            )
            o_abs = sum(
                abs(float(x or 0)) for x in (chart_data.get("options") or [0.0] * n)[:n]
            )
            d_abs = sum(
                abs(float(x or 0)) for x in (chart_data.get("dividends") or [0.0] * n)[:n]
            )
            for key in ("equity", "options", "dividends"):
                if key in chart_data and len(chart_data.get(key) or []) == n:
                    chart_data[key] = [0.0] * n
            mx = max(d_abs, e_abs, o_abs)
            if mx < 1e-9:
                # All-flat leg/stg series: put open P&L on one stream so stacked sum matches `total`
                if "options" in chart_data and len(chart_data["options"]) == n:
                    chart_data["options"][-1] = round(k, 2)
                elif "equity" in chart_data and len(chart_data["equity"]) == n:
                    chart_data["equity"][-1] = round(k, 2)
                elif "dividends" in chart_data and len(chart_data["dividends"]) == n:
                    chart_data["dividends"][-1] = round(k, 2)
            else:
                _tie = {"options": 0, "equity": 1, "dividends": 2}
                streams = [
                    (d_abs, "dividends"),
                    (e_abs, "equity"),
                    (o_abs, "options"),
                ]
                streams.sort(key=lambda t: (-t[0], _tie.get(t[1], 9)))
                for _score, sname in streams:
                    if sname in chart_data and len(chart_data[sname]) == n:
                        chart_data[sname][-1] = round(k, 2)
                        break
        return
    f = k / t_end
    if not all(
        len(chart_data.get(skey) or []) == n
        for skey in ("equity", "options", "dividends")
    ):
        chart_data["total"] = [round(float(x) * f, 2) for x in chart_data["total"]]
        return
    for key in ("equity", "options", "dividends"):
        arr = chart_data.get(key) or []
        chart_data[key] = [round(float(x) * f, 2) for x in arr]
    chart_data["total"] = [
        round(
            float(chart_data["equity"][i] or 0)
            + float(chart_data["options"][i] or 0)
            + float(chart_data["dividends"][i] or 0),
            2,
        )
        for i in range(n)
    ]


def _cumulative_pnl_from_stg_trades(trades_df, current_df):
    """
    Cumulative P&L by calendar day from stg_history (cash flow per row). Used when
    mart_daily_pnl is sparse but stg has years of RDDT fills (symbol match quirks).
    """
    empty = {
        "dates": [],
        "equity": [],
        "options": [],
        "dividends": [],
        "total": [],
        "underlying_price": [],
        "has_underlying_price": False,
    }
    if trades_df is None or trades_df.empty or "amount" not in trades_df.columns:
        return None
    t = trades_df.copy()
    if "trade_date" not in t.columns or "instrument_type" not in t.columns:
        return None
    t["td"] = pd.to_datetime(t["trade_date"], errors="coerce").dt.normalize()
    t = t[pd.notna(t["td"])]
    if t.empty:
        return None
    t["amount"] = pd.to_numeric(t["amount"], errors="coerce").fillna(0.0)
    it = t["instrument_type"].fillna("").str.strip()
    a = t["amount"]
    t["_div"] = a.where(
        (it == "Dividend") | it.str.contains("ividend", case=False, na=False), 0.0
    )
    t["_eq"] = a.where(it == "Equity", 0.0)
    t["_op"] = a.where(it.isin(["Call", "Put"]), 0.0)
    t["_oth"] = a - t["_div"] - t["_eq"] - t["_op"]
    g = t.groupby("td", as_index=False).agg(
        {"_eq": "sum", "_op": "sum", "_div": "sum", "_oth": "sum"}
    )
    g = g.sort_values("td")
    g["c_eq"] = g["_eq"].cumsum()
    g["c_op"] = (g["_op"] + g["_oth"]).cumsum()  # fees/margin in with options line for chart
    g["c_div"] = g["_div"].cumsum()
    g["tot"] = g["c_eq"] + g["c_op"] + g["c_div"]
    dates = [str(pd.Timestamp(x).date()) for x in g["td"].tolist()]
    return {
        "dates": dates,
        "equity": [round(x, 2) for x in g["c_eq"]],
        "options": [round(x, 2) for x in g["c_op"]],
        "dividends": [round(x, 2) for x in g["c_div"]],
        "total": [round(x, 2) for x in g["tot"]],
        "underlying_price": [None] * len(dates),
        "has_underlying_price": False,
    }


def _cumulative_pnl_from_leg_closes(closed_legs_pre_leg, closed_equity_pre_leg):
    """
    Step cumulative P&L from closed option legs and closed equity by close_date.
    Fallback when stg is empty but int_* legs exist.
    """
    events = []  # (date, d_eq, d_op, d_div)
    if closed_legs_pre_leg is not None and not closed_legs_pre_leg.empty and "close_date" in closed_legs_pre_leg.columns:
        for _, r in closed_legs_pre_leg.iterrows():
            d = r.get("close_date")
            if pd.isna(d):
                continue
            pnl = float(r.get("total_pnl") or 0)
            d0 = pd.to_datetime(d).date()
            events.append((d0, 0.0, pnl, 0.0))
    if closed_equity_pre_leg is not None and not closed_equity_pre_leg.empty and "close_date" in closed_equity_pre_leg.columns:
        for _, r in closed_equity_pre_leg.iterrows():
            d = r.get("close_date")
            if pd.isna(d):
                continue
            pnl = float(r.get("realized_pnl") or 0)
            d0 = pd.to_datetime(d).date()
            events.append((d0, pnl, 0.0, 0.0))
    if not events:
        return None
    events.sort(key=lambda x: x[0])
    byd = {}
    for d0, e, o, di in events:
        byd.setdefault(d0, [0.0, 0.0, 0.0])
        byd[d0][0] += e
        byd[d0][1] += o
        byd[d0][2] += di
    d_sorted = sorted(byd)
    c_eq, c_op, c_div = 0.0, 0.0, 0.0
    dates, eq, op, div, tot = [], [], [], [], []
    for d0 in d_sorted:
        c_eq += byd[d0][0]
        c_op += byd[d0][1]
        c_div += byd[d0][2]
        dates.append(str(d0))
        eq.append(round(c_eq, 2))
        op.append(round(c_op, 2))
        div.append(round(c_div, 2))
        tot.append(round(c_eq + c_op + c_div, 2))
    return {
        "dates": dates,
        "equity": eq,
        "options": op,
        "dividends": div,
        "total": tot,
        "underlying_price": [None] * len(dates),
        "has_underlying_price": False,
    }


def _build_chart_from_daily_pnl(daily_df, current_df):
    """
    Build cumulative P&L chart from pre-aggregated mart_daily_pnl data.

    Options, dividends, and other: read pre-computed cumulative sums.
    Equity: compute running average-cost P&L (stateful — buy/sell events
    from the mart, with daily mark-to-market via close_price).
    """
    empty = {
        "dates": [], "equity": [], "options": [], "dividends": [],
        "total": [], "underlying_price": [], "has_underlying_price": False,
    }
    if daily_df.empty:
        return empty

    daily_df = daily_df.sort_values("date")

    shares_held = 0.0
    total_cost = 0.0
    cum_realized = 0.0
    short_shares = 0.0
    short_cost_basis = 0.0
    position_is_closed = current_df.empty
    last_trade_date = None

    dates, equity_s, options_s, dividends_s, total_s, price_s = (
        [], [], [], [], [], [],
    )
    last_cumulative_options_pnl = 0.0

    for _, row in daily_df.iterrows():
        buy_qty = float(row.get("equity_buy_qty") or 0)
        buy_cost = float(row.get("equity_buy_cost") or 0)
        sell_qty = float(row.get("equity_sell_qty") or 0)
        sell_proceeds = float(row.get("equity_sell_proceeds") or 0)
        has_trade = bool(row.get("has_trade"))

        if has_trade:
            last_trade_date = row["date"]

        if position_is_closed and shares_held == 0 and short_shares == 0 and not has_trade:
            continue

        # Process sells first (may create short position)
        if sell_qty > 0:
            remaining_sell = sell_qty
            remaining_proceeds = sell_proceeds
            if shares_held > 0:
                sold_long = min(remaining_sell, shares_held)
                avg = total_cost / shares_held if shares_held > 0 else 0
                frac = sold_long / sell_qty if sell_qty > 0 else 1
                sold_long_proceeds = sell_proceeds * frac
                cum_realized += sold_long_proceeds - avg * sold_long
                total_cost = max(0, total_cost - avg * sold_long)
                shares_held = max(0, shares_held - sold_long)
                remaining_sell -= sold_long
                remaining_proceeds -= sold_long_proceeds
            if remaining_sell > 0:
                short_shares += remaining_sell
                short_cost_basis += remaining_proceeds

        # Process buys (may cover short position)
        if buy_qty > 0:
            remaining_buy = buy_qty
            remaining_cost = buy_cost
            if short_shares > 0:
                covered = min(remaining_buy, short_shares)
                frac = covered / buy_qty if buy_qty > 0 else 1
                cover_cost = buy_cost * frac
                avg_short = short_cost_basis / short_shares if short_shares > 0 else 0
                cum_realized += avg_short * covered - cover_cost
                short_cost_basis = max(0, short_cost_basis - avg_short * covered)
                short_shares = max(0, short_shares - covered)
                remaining_buy -= covered
                remaining_cost -= cover_cost
            if remaining_buy > 0:
                shares_held += remaining_buy
                total_cost += remaining_cost

        close = float(row.get("close_price") or 0)
        # If no close price on a buy day, use avg cost so open position doesn't show full cost as "loss"
        if close <= 0 and buy_qty > 0 and buy_cost > 0 and shares_held > 0:
            close = buy_cost / buy_qty
        unrealized = 0
        if close > 0:
            if shares_held > 0:
                unrealized = shares_held * close - total_cost
            if short_shares > 0:
                unrealized -= (short_shares * close - short_cost_basis)
        eq_pnl = cum_realized + unrealized

        # Options P&L = cumulative cash flows + current option market value.
        # After the first snapshot, the mart carries forward option_market_value
        # via LAST_VALUE IGNORE NULLS, so it's only NULL pre-first-snapshot.
        # Pre-snapshot days show cash flows only (realized premiums / costs).
        cum_opt = float(row.get("cumulative_options_pnl") or 0)
        opt_mv = row.get("option_market_value")
        if opt_mv is not None and not pd.isna(opt_mv):
            opt_pnl = cum_opt + float(opt_mv)
        else:
            opt_pnl = cum_opt
        div_pnl = float(row.get("cumulative_dividends_pnl") or 0)
        oth_pnl = float(row.get("cumulative_other_pnl") or 0)
        last_cumulative_options_pnl = cum_opt

        dates.append(str(row["date"])[:10])
        equity_s.append(round(eq_pnl, 2))
        options_s.append(round(opt_pnl, 2))
        dividends_s.append(round(div_pnl, 2))
        total_s.append(round(eq_pnl + opt_pnl + div_pnl + oth_pnl, 2))
        # Underlying close for the chart: use whenever the mart has a price.
        # Do not require shares_held > 0 here — that failed when the chart date range
        # starts after the equity open (leg filter) or carry-forward is missing rows.
        price_s.append(round(close, 2) if close > 0 else None)

    if not dates:
        return empty

    today_str = str(date.today())
    if not current_df.empty and dates[-1] != today_str:
        # Today's option P&L = cumulative cash flows + current open option market value.
        # Using market_value (not unrealized_pnl) keeps this consistent with the
        # cum + mv formula used for all historical points.
        opt_mv_today = float(current_df.loc[
            current_df["instrument_type"].isin(["Call", "Put"]), "market_value"
        ].sum()) if "market_value" in current_df.columns else 0.0
        today_option_pnl = last_cumulative_options_pnl + opt_mv_today
        eq_row = current_df[current_df["instrument_type"] == "Equity"]
        today_eq = equity_s[-1]
        if not eq_row.empty and (shares_held > 0 or short_shares > 0):
            p = float(eq_row["current_price"].iloc[0] or 0)
            if p:
                unreal = 0
                if shares_held > 0:
                    unreal = shares_held * p - total_cost
                if short_shares > 0:
                    unreal -= (short_shares * p - short_cost_basis)
                today_eq = cum_realized + unreal
        today_price = None
        if not eq_row.empty:
            today_price = float(eq_row["current_price"].iloc[0] or 0) or None

        dates.append(today_str)
        equity_s.append(round(today_eq, 2))
        options_s.append(round(today_option_pnl, 2))
        dividends_s.append(dividends_s[-1])
        price_s.append(round(today_price, 2) if today_price else None)
        total_s.append(round(today_eq + today_option_pnl + dividends_s[-1], 2))

    return {
        "dates": dates,
        "equity": equity_s,
        "options": options_s,
        "dividends": dividends_s,
        "total": total_s,
        "underlying_price": price_s,
        "has_underlying_price": any(p is not None for p in price_s),
    }


@app.route("/symbols")
@login_required
def symbols_detail():
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    acct_filter = _account_sql_and(user_accounts)

    try:
        dfs = _bq_parallel(client, {
            "trades": TRADES_QUERY.format(account_filter=acct_filter),
            "current": CURRENT_POSITIONS_QUERY.format(account_filter=acct_filter),
            "strat": STRATEGIES_MAP_QUERY.format(account_filter=acct_filter),
            "pnl": SYMBOLS_PNL_QUERY.format(account_filter=acct_filter),
            "open_start": OPEN_SESSION_START_QUERY.format(account_filter=acct_filter),
            "closed_legs": CLOSED_LEGS_QUERY.format(
                closed_legs_account_filter=_account_sql_and(user_accounts, col="sc.account")),
            "closed_equity": CLOSED_EQUITY_LEGS_QUERY.format(account_filter=acct_filter),
        })
        trades_df = dfs["trades"]
        current_df = dfs["current"]
        strat_df = dfs["strat"]
        pnl_df = dfs["pnl"]
        open_start_df = dfs["open_start"]
        closed_legs_df = dfs["closed_legs"]
        closed_equity_df = dfs["closed_equity"]
    except Exception as exc:
        return render_template(
            "symbols.html",
            error=str(exc),
            symbol_data=[],
            chart_data_json="[]",
            accounts=[],
            selected_account="",
            open_only=False,
            linked_brokerage_accounts=(user_accounts or []),
            viewer_is_admin=is_admin(current_user.username),
        )

    trades_df = _df_normalize_account_column(trades_df)
    current_df = _df_normalize_account_column(current_df)
    strat_df = _df_normalize_account_column(strat_df)
    pnl_df = _df_normalize_account_column(pnl_df)
    open_start_df = _df_normalize_account_column(open_start_df)
    closed_legs_df = _df_normalize_account_column(closed_legs_df)
    closed_equity_df = _df_normalize_account_column(closed_equity_df)

    # ------------------------------------------------------------------
    # Clean types
    # ------------------------------------------------------------------
    if not trades_df.empty:
        for col in ["amount", "quantity", "price", "fees"]:
            if col in trades_df.columns:
                trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
        if "trade_date" in trades_df.columns:
            trades_df["trade_date"] = pd.to_datetime(trades_df["trade_date"]).dt.date

    for col in ["unrealized_pnl", "market_value", "quantity", "current_price", "cost_basis"]:
        if col in current_df.columns:
            current_df[col] = pd.to_numeric(current_df[col], errors="coerce").fillna(0)
    if "unrealized_pnl_pct" in current_df.columns:
        current_df["unrealized_pnl_pct"] = pd.to_numeric(current_df["unrealized_pnl_pct"], errors="coerce").fillna(0)

    # Strategy map: (account, symbol) → sorted list of strategies
    strat_map = (
        strat_df.groupby(["account", "symbol"])["strategy"]
        .apply(lambda x: sorted(x.unique().tolist()))
        .to_dict()
    )

    # Open session start map: (account, symbol) → open_start date
    open_start_map = {}
    if "open_start_df" in locals() and not open_start_df.empty:
        for _, row in open_start_df.iterrows():
            key = (str(row["account"]), str(row["symbol"]))
            open_start_map[key] = row["open_start"]

    # Normalize closed_legs_df for date filtering
    if "closed_legs_df" in locals() and not closed_legs_df.empty:
        closed_legs_df["close_date"] = pd.to_datetime(closed_legs_df["close_date"], errors="coerce").dt.date
    else:
        closed_legs_df = pd.DataFrame()

    # ------------------------------------------------------------------
    # Safety-belt: re-filter in Python (SQL already filtered by account)
    # ------------------------------------------------------------------
    trades_df = _filter_df_by_accounts(trades_df, user_accounts)
    current_df = _filter_df_by_accounts(current_df, user_accounts)
    strat_df = _filter_df_by_accounts(strat_df, user_accounts)
    pnl_df = _filter_df_by_accounts(pnl_df, user_accounts)
    if not open_start_df.empty:
        open_start_df = _filter_df_by_accounts(open_start_df, user_accounts)
    if not closed_legs_df.empty:
        closed_legs_df = _filter_df_by_accounts(closed_legs_df, user_accounts)
    if not closed_equity_df.empty:
        closed_equity_df = _filter_df_by_accounts(closed_equity_df, user_accounts)

    def _unique_accounts(*frames):
        s = set()
        for f in frames:
            if f is not None and not f.empty and "account" in f.columns:
                for v in f["account"].dropna().unique():
                    t = str(v).strip()
                    if t:
                        s.add(t)
        return sorted(s)

    accounts = _unique_accounts(trades_df, pnl_df, current_df, strat_df)
    if not accounts and user_accounts:
        accounts = sorted(
            {str(a).strip() for a in user_accounts if a and str(a).strip()}
        )
    selected_account = request.args.get("account", "")
    # Use getlist so duplicate params (e.g. open_only=1&open_only=0 from checkbox+hidden) don't break the filter
    open_only = "1" in request.args.getlist("open_only")
    positions_only = "1" in request.args.getlist("positions_only")

    # Redirect to canonical URL if duplicate params present (cleans bookmarks/cached URLs)
    open_list = request.args.getlist("open_only")
    pos_list = request.args.getlist("positions_only")
    if len(open_list) > 1 or len(pos_list) > 1:
        q = {"account": selected_account} if selected_account else {}
        if open_only:
            q["open_only"] = "1"
        if positions_only:
            q["positions_only"] = "1"
        return redirect(url_for("symbols_detail", **q))

    # "Open positions only" implies "symbols with open positions" filter.
    # If the user checks the second box but not the first, we still want
    # to restrict the symbol list to those with an open position.
    if positions_only and not open_only:
        open_only = True

    if selected_account:
        trades_df = trades_df[trades_df["account"] == selected_account]
        current_df = current_df[current_df["account"] == selected_account]
        strat_df = strat_df[strat_df["account"] == selected_account]
        pnl_df = pnl_df[pnl_df["account"] == selected_account]
        if not open_start_df.empty:
            open_start_df = open_start_df[open_start_df["account"] == selected_account]
        if not closed_legs_df.empty:
            closed_legs_df = closed_legs_df[closed_legs_df["account"] == selected_account]
        if not closed_equity_df.empty:
            closed_equity_df = closed_equity_df[closed_equity_df["account"] == selected_account]

    # Restrict to symbols that have a current open position (match current_positions / int_enriched_current)
    if open_only:
        open_pairs = set(zip(current_df["account"].astype(str), current_df["symbol"].astype(str))) if not current_df.empty else set()
    else:
        open_pairs = None

    # Fetch pre-aggregated chart data from mart
    try:
        acct_filter = _account_sql_and([selected_account] if selected_account else user_accounts)
        all_chart_df = client.query(
            CHART_DATA_ALL_QUERY.format(account_filter=acct_filter)
        ).to_dataframe()
        all_chart_df = _filter_df_by_accounts(all_chart_df, user_accounts)
        if selected_account and not all_chart_df.empty:
            all_chart_df = all_chart_df[all_chart_df["account"] == selected_account]
    except Exception:
        all_chart_df = pd.DataFrame()

    # ------------------------------------------------------------------
    # Build per-symbol data
    # ------------------------------------------------------------------
    symbol_data = []
    chart_data_list = []

    for account, symbol in _iter_symbols_for_daily_detail(
        trades_df, pnl_df, current_df, open_pairs
    ):
        group = trades_df[
            (trades_df["account"] == account) & (trades_df["symbol"] == symbol)
        ]
        if not group.empty and "trade_date" in group.columns:
            group = group.sort_values("trade_date")

        sym_current = current_df[
            (current_df["account"] == account) & (current_df["symbol"] == symbol)
        ]

        # Realized P&L: use positions_summary when available so mixed
        # open/closed symbols (e.g. RKLB) show historical realized plus
        # current unrealized. For symbols that are purely open (only
        # Open status and no closed trades), treat realized as 0.
        sym_pnl = pnl_df[
            (pnl_df["account"] == account) & (pnl_df["symbol"] == symbol)
        ]
        if not sym_pnl.empty:
            statuses = (
                sym_pnl["status"]
                .dropna()
                .astype(str)
                .str.lower()
                .str.strip()
                .unique()
                .tolist()
            )
            has_open = any(s == "open" for s in statuses)
            has_closed = any(s == "closed" for s in statuses)
            realized_val = float(sym_pnl["realized_pnl"].sum() or 0.0)
            # Purely open symbol (no closed legs): no realized yet, even
            # if the mart currently reports a negative net cash flow.
            if has_open and not has_closed:
                realized_val = 0.0
            total_realized = round(realized_val, 2)
        else:
            # Fallback: net cash flow from trades if mart row missing.
            total_realized = (
                round(float(group["amount"].sum()), 2)
                if not group.empty and "amount" in group.columns
                else 0.0
            )

        # Unrealized from current open positions (matches current positions table)
        unrealized = round(float(sym_current["unrealized_pnl"].sum()), 2) if not sym_current.empty else 0.0
        equity_open_pnl = round(
            float(sym_current.loc[sym_current["instrument_type"] == "Equity", "unrealized_pnl"].sum()), 2
        ) if not sym_current.empty else 0.0
        options_open_pnl = round(
            float(sym_current.loc[sym_current["instrument_type"].isin(["Call", "Put"]), "unrealized_pnl"].sum()), 2
        ) if not sym_current.empty else 0.0

        # Closed legs that belong to this position (closed on or after open_start).
        # For \"open positions only\", prefer the precomputed open_start_map; if it's
        # missing, fall back to the first trade date so we anchor to the current run.
        open_key = (str(account), str(symbol))
        open_start_val = open_start_map.get(open_key) if positions_only else None
        if positions_only and open_start_val is None and not group.empty:
            open_start_val = group["trade_date"].min()

        strategies = strat_map.get((account, symbol), [])

        if not closed_legs_df.empty and open_start_val is not None:
            open_start_date = pd.to_datetime(open_start_val).date()
            # The date range (open_start_date to present) is already anchored to the
            # current position's equity session start date (from int_strategy_classification).
            # That is the correct and sufficient filter — any option that closed on or
            # after the position opened belongs to this position.  Strategy-label
            # filtering is removed because it excluded legs whose classification differed
            # slightly from the live open strategy (e.g. expired-worthless covered calls
            # inferred as Closed via option_expiry, or PMCC short legs labelled differently
            # from the open long-call anchor).
            legs = closed_legs_df[
                (closed_legs_df["account"] == account)
                & (closed_legs_df["symbol"] == symbol)
                & (closed_legs_df["close_date"] >= open_start_date)
            ]
            closed_legs_list = legs.sort_values("close_date").to_dict(orient="records")
            for r in closed_legs_list:
                r["open_date"] = str(r["open_date"]) if pd.notna(r.get("open_date")) else ""
                r["close_date"] = str(r["close_date"]) if pd.notna(r.get("close_date")) else ""
                r["total_pnl"] = round(float(r.get("total_pnl") or 0), 2)
        else:
            closed_legs_list = []

        # Closed equity legs (shares sold / called away) within this position.
        closed_equity_list = []
        if not closed_equity_df.empty and open_start_val is not None:
            open_start_date = pd.to_datetime(open_start_val).date()
            eq_legs = closed_equity_df[
                (closed_equity_df["account"] == account)
                & (closed_equity_df["symbol"] == symbol)
                & (closed_equity_df["close_date"] >= open_start_date)
            ]
            closed_equity_list = eq_legs.sort_values("close_date").to_dict(orient="records")
            for r in closed_equity_list:
                r["open_date"] = str(r["open_date"]) if pd.notna(r.get("open_date")) else ""
                r["close_date"] = str(r["close_date"]) if pd.notna(r.get("close_date")) else ""
                r["realized_pnl"] = round(float(r.get("realized_pnl") or 0), 2)

        # Total closed P&L = option legs + equity legs
        closed_options_pnl = round(sum(float(r.get("total_pnl") or 0) for r in closed_legs_list), 2)
        closed_equity_pnl = round(sum(float(r.get("realized_pnl") or 0) for r in closed_equity_list), 2)
        closed_legs_pnl = round(closed_options_pnl + closed_equity_pnl, 2)

        # Display semantics:
        # - Default view: total_return = realized (history) + unrealized (current)
        # - "Open positions only" view: show this position's closed legs + current open P&L.
        display_realized = total_realized
        display_total = round(total_realized + unrealized, 2)
        if positions_only:
            display_realized = closed_legs_pnl
            display_total = round(closed_legs_pnl + unrealized, 2)

        if not group.empty and "trade_date" in group.columns:
            num_trades = len(group)
            first_date = str(group["trade_date"].min())
            last_date = str(group["trade_date"].max())
        else:
            num_trades = 0
            first_date = ""
            last_date = ""

        sym_chart_df = all_chart_df[
            (all_chart_df["account"] == account) & (all_chart_df["symbol"] == symbol)
        ] if not all_chart_df.empty else pd.DataFrame()

        # For "Open positions only", clip the daily P&L series to the open
        # session start so the chart focuses on the live leg while still using
        # true end-of-day prices from mart_daily_pnl.
        if positions_only and open_start_val is not None and not sym_chart_df.empty and "date" in sym_chart_df.columns:
            sym_chart_df = sym_chart_df[sym_chart_df["date"] >= pd.to_datetime(open_start_val)]
            if not sym_chart_df.empty and not group.empty and "trade_date" in group.columns:
                first_date = str(
                    min(group["trade_date"].max(), sym_chart_df["date"].min())
                )

        chart = _build_chart_from_daily_pnl(sym_chart_df, sym_current)

        # When viewing "this position only", rebase chart so it starts at 0
        # (first point = start of position, not cumulative from prior history)
        if positions_only and chart.get("dates") and len(chart["dates"]) > 0:
            base_equity = chart["equity"][0] if chart["equity"] else 0
            base_options = chart["options"][0] if chart["options"] else 0
            base_dividends = chart["dividends"][0] if chart["dividends"] else 0
            base_total = chart["total"][0] if chart["total"] else 0
            chart["equity"] = [round(x - base_equity, 2) for x in chart["equity"]]
            chart["options"] = [round(x - base_options, 2) for x in chart["options"]]
            chart["dividends"] = [round(x - base_dividends, 2) for x in chart["dividends"]]
            chart["total"] = [round(x - base_total, 2) for x in chart["total"]]
            # If this position has no open equity (options-only), strip equity from the
            # chart so we don't show phantom spikes from past equity trades in the mart.
            has_open_equity = not sym_current.empty and (
                (sym_current["instrument_type"] == "Equity").any()
            )
            if not has_open_equity:
                n = len(chart["dates"])
                for i in range(n):
                    chart["total"][i] = round(chart["total"][i] - chart["equity"][i], 2)
                    chart["equity"][i] = 0
            # Anchor the last options point to closed OPTION legs + current open
            # option unrealized only.  Equity realized P&L (shares sold/called away)
            # is already captured by the natural avg-cost equity calculation and must
            # not be added to the options series — doing so double-counts it and
            # causes a spurious drop to -$3k on the final data point.
            chart["options"][-1] = round(closed_options_pnl + options_open_pnl, 2)
            chart["total"][-1] = round(
                chart["equity"][-1] + chart["options"][-1] + chart["dividends"][-1], 2
            )

        chart_data_list.append(chart)

        # Trade table rows (convert dates to str for Jinja)
        trades_table = group.copy()
        trades_table["trade_date"] = trades_table["trade_date"].astype(str)
        trades_list = trades_table.to_dict(orient="records")

        # Positions table rows: combine open positions from current snapshot
        # with closed legs for this position, and add a status column.
        current_list = sym_current.to_dict(orient="records") if not sym_current.empty else []
        combined_positions = []
        # Position-level open date (for equity / fallback) — reuse open_start_val
        open_start_str = None
        if open_start_val is not None:
            try:
                open_start_str = str(pd.to_datetime(open_start_val).date())
            except Exception:
                open_start_str = None

        # Per-option open date from transaction history (sell_to_open / buy_to_open).
        # The current snapshot doesn't carry open dates, so we look up each
        # option's trade_symbol in the trade history to find its opening trade.
        option_open_date_map: dict = {}
        if not group.empty and "action" in group.columns and "trade_symbol" in group.columns:
            open_actions = {"option_sell_to_open", "option_buy_to_open"}
            opt_opens = group[
                group["action"].astype(str).str.lower().str.strip().isin(open_actions)
            ]
            for _, trade_row in opt_opens.iterrows():
                ts = str(trade_row.get("trade_symbol", "")).strip()
                td = trade_row.get("trade_date")
                if ts and td is not None:
                    td_str = str(td)
                    if ts not in option_open_date_map or td_str < option_open_date_map[ts]:
                        option_open_date_map[ts] = td_str

        for row in current_list:
            r = dict(row)
            r["status"] = "Open"
            ts = str(r.get("trade_symbol", "")).strip()
            if r.get("instrument_type") in ("Call", "Put") and ts in option_open_date_map:
                r["open_date"] = option_open_date_map[ts]
            else:
                r["open_date"] = open_start_str
            r["close_date"] = ""
            combined_positions.append(r)

        # Closed legs within the current open session always show in the
        # Positions table so you can see the full story of the live position.
        for leg in closed_legs_list:
            direction = str(leg.get("direction") or "")
            prem_recv = float(leg.get("premium_received") or 0)
            prem_paid = float(leg.get("premium_paid") or 0)
            cost_close = float(leg.get("cost_to_close") or 0)
            proceeds_close = float(leg.get("proceeds_from_close") or 0)
            if direction == "Sold":
                leg_cost = abs(cost_close)
                leg_proceeds = abs(prem_recv)
            else:
                leg_cost = abs(prem_paid)
                leg_proceeds = abs(proceeds_close)
            opt_pnl = float(leg.get("total_pnl") or 0)
            opt_return_pct = round(opt_pnl / leg_cost * 100, 2) if leg_cost else None
            combined_positions.append({
                "status": "Closed",
                "trade_symbol": leg.get("trade_symbol"),
                "description": leg.get("strategy") or "",
                "quantity": leg.get("quantity"),
                "current_price": None,
                "market_value": round(leg_proceeds, 2) if leg_proceeds else None,
                "cost_basis": round(leg_cost, 2) if leg_cost else None,
                "unrealized_pnl": opt_pnl,
                "unrealized_pnl_pct": opt_return_pct,
                "open_date": leg.get("open_date") or "",
                "close_date": leg.get("close_date") or "",
            })

        # Closed equity legs (shares sold / called away).
        for leg in closed_equity_list:
            eq_proceeds = float(leg.get("sell_proceeds") or 0)
            eq_cost = float(leg.get("cost_basis") or 0)
            eq_pnl = float(leg.get("realized_pnl") or 0)
            eq_return_pct = round(eq_pnl / eq_cost * 100, 2) if eq_cost else None
            combined_positions.append({
                "status": "Closed",
                "trade_symbol": leg.get("trade_symbol") or symbol,
                "description": leg.get("description") or "Equity Sold",
                "quantity": leg.get("quantity"),
                "current_price": leg.get("sale_price_per_share"),
                "market_value": round(eq_proceeds, 2) if eq_proceeds else None,
                "cost_basis": round(eq_cost, 2) if eq_cost else None,
                "unrealized_pnl": eq_pnl,
                "unrealized_pnl_pct": eq_return_pct,
                "open_date": leg.get("open_date") or "",
                "close_date": leg.get("close_date") or "",
            })

        # Quick story stats for this symbol/position (across option + equity legs)
        all_closed_for_stats = [
            *closed_legs_list,
            *[{
                "trade_symbol": r.get("trade_symbol") or symbol,
                "strategy": r.get("description") or "Equity Sold",
                "close_date": r.get("close_date") or "",
                "total_pnl": r.get("realized_pnl", 0),
            } for r in closed_equity_list],
        ]
        best_leg = None
        worst_leg = None
        if all_closed_for_stats:
            best_leg = max(all_closed_for_stats, key=lambda r: r.get("total_pnl", 0))
            worst_leg = min(all_closed_for_stats, key=lambda r: r.get("total_pnl", 0))

        open_start_val = open_start_map.get((str(account), str(symbol)))
        days_in_position = None
        if open_start_val is not None:
            try:
                days_in_position = (date.today() - pd.to_datetime(open_start_val).date()).days
            except Exception:
                days_in_position = None

        open_legs_count = sum(1 for r in combined_positions if r.get("status") == "Open")
        closed_legs_count = sum(1 for r in combined_positions if r.get("status") == "Closed")

        symbol_data.append({
            "account": account,
            "symbol": symbol,
            "total_realized": display_realized,
            "unrealized": unrealized,
            "total_return": display_total,
            "num_trades": num_trades,
            "first_date": first_date,
            "last_date": last_date,
            "strategies": strategies,
            "trades": trades_list,
            "current_positions": combined_positions,
            "story_days_in_position": days_in_position,
            "story_open_legs": open_legs_count,
            "story_closed_legs": closed_legs_count,
            "story_best_leg": best_leg,
            "story_worst_leg": worst_leg,
            "_chart_idx": len(chart_data_list) - 1,
        })

    # Sort by total return descending; rebuild chart list in matching order
    symbol_data.sort(key=lambda x: x["total_return"], reverse=True)
    sorted_charts = [chart_data_list[item["_chart_idx"]] for item in symbol_data]
    for item in symbol_data:
        del item["_chart_idx"]

    return render_template(
        "symbols.html",
        symbol_data=symbol_data,
        chart_data_json=json.dumps(sorted_charts),
        accounts=accounts,
        selected_account=selected_account,
        open_only=open_only,
        positions_only=positions_only,
        linked_brokerage_accounts=(user_accounts or []),
        viewer_is_admin=is_admin(current_user.username),
    )


# ======================================================================
# Account Performance  (/accounts)
# ======================================================================

ACCOUNT_BALANCES_QUERY = """
    SELECT account, row_type, market_value, cost_basis,
           unrealized_pnl, unrealized_pnl_pct, percent_of_account
    FROM `ccwj-dbt.analytics.stg_account_balances`
    WHERE 1=1 {account_filter}
"""

STRATEGY_CLASSIFICATION_QUERY = """
    SELECT account, symbol, strategy, status, open_date, close_date,
           total_pnl, num_trades
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE 1=1 {account_filter}
"""

ACCOUNT_POSITIONS_SUMMARY_QUERY = """
    SELECT account, strategy,
           SUM(total_pnl) AS total_pnl,
           SUM(realized_pnl) AS realized_pnl,
           SUM(unrealized_pnl) AS unrealized_pnl,
           SUM(total_premium_received) AS premium_received,
           SUM(total_premium_paid) AS premium_paid,
           SUM(num_individual_trades) AS num_trades,
           SUM(num_winners) AS num_winners,
           SUM(num_losers) AS num_losers,
           SUM(total_dividend_income) AS dividend_income,
           SUM(total_return) AS total_return
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {account_filter}
    GROUP BY account, strategy
    ORDER BY account, strategy
"""


def _build_account_chart_from_daily_pnl(daily_df, current_df):
    """
    Build account-level cumulative P&L chart from mart_daily_pnl.

    Aggregates across all symbols.  Options/dividends/other use running
    sums of daily amounts.  Equity requires per-symbol average-cost tracking.
    """
    empty = {"dates": [], "equity": [], "options": [], "dividends": [], "total": []}
    if daily_df.empty:
        return empty

    daily_df = daily_df.sort_values("date")
    all_dates = sorted(daily_df["date"].dropna().unique())

    eq_state = {}
    cum_opt = cum_div = cum_oth = 0.0
    dates_out, equity_s, options_s, dividends_s, total_s = [], [], [], [], []

    for d in all_dates:
        day = daily_df[daily_df["date"] == d]

        cum_opt += float(day["options_amount"].sum())
        cum_div += float(day["dividends_amount"].sum())
        cum_oth += float(day["other_amount"].sum())

        for _, row in day.iterrows():
            key = (row["account"], row["symbol"])
            if key not in eq_state:
                eq_state[key] = {"shares": 0.0, "cost": 0.0, "realized": 0.0}
            s = eq_state[key]
            bq = float(row.get("equity_buy_qty") or 0)
            bc = float(row.get("equity_buy_cost") or 0)
            sq = float(row.get("equity_sell_qty") or 0)
            sp = float(row.get("equity_sell_proceeds") or 0)

            if bq > 0:
                s["shares"] += bq
                s["cost"] += bc
            if sq > 0 and s["shares"] > 0:
                avg = s["cost"] / s["shares"]
                sold = min(sq, s["shares"])
                s["realized"] += sp - avg * sold
                s["cost"] = max(0, s["cost"] - avg * sold)
                s["shares"] = max(0, s["shares"] - sold)
            elif sq > 0:
                s["realized"] += sp

        eq_total = sum(s["realized"] for s in eq_state.values())
        for _, row in day.iterrows():
            key = (row["account"], row["symbol"])
            s = eq_state[key]
            close = float(row.get("close_price") or 0)
            if close > 0 and s["shares"] > 0:
                eq_total += s["shares"] * close - s["cost"]

        dates_out.append(str(d)[:10])
        equity_s.append(round(eq_total, 2))
        options_s.append(round(cum_opt, 2))
        dividends_s.append(round(cum_div, 2))
        total_s.append(round(eq_total + cum_opt + cum_div + cum_oth, 2))

    today = date.today()
    today_str = str(today)
    if not current_df.empty and dates_out and dates_out[-1] != today_str:
        eq_unreal = float(current_df.loc[current_df["instrument_type"] == "Equity", "unrealized_pnl"].sum())
        opt_unreal = float(current_df.loc[current_df["instrument_type"].isin(["Call", "Put"]), "unrealized_pnl"].sum())
        total_unreal = eq_unreal + opt_unreal
        if total_unreal != 0:
            dates_out.append(today_str)
            equity_s.append(round(equity_s[-1] + eq_unreal, 2))
            options_s.append(round(options_s[-1] + opt_unreal, 2))
            dividends_s.append(dividends_s[-1])
            total_s.append(round(total_s[-1] + total_unreal, 2))

    return {
        "dates": dates_out,
        "equity": equity_s,
        "options": options_s,
        "dividends": dividends_s,
        "total": total_s,
    }


def _build_strategy_time_chart(strat_df):
    """
    Build cumulative P&L over time per strategy from trade-group data.
    Closed groups → P&L attributed to close_date.
    Open groups   → P&L attributed to today.
    """
    if strat_df.empty:
        return {"dates": [], "series": {}}

    today = date.today()
    rows = []
    for _, r in strat_df.iterrows():
        pnl_date = r["close_date"] if r["status"] == "Closed" and pd.notna(r["close_date"]) else today
        rows.append({"strategy": r["strategy"], "pnl_date": pnl_date, "pnl": float(r["total_pnl"])})

    events = pd.DataFrame(rows)
    events["pnl_date"] = pd.to_datetime(events["pnl_date"]).dt.date

    # Sum P&L per (strategy, date)
    grouped = events.groupby(["strategy", "pnl_date"])["pnl"].sum().reset_index()
    strategies = sorted(grouped["strategy"].unique())
    all_dates = sorted(grouped["pnl_date"].unique())

    series = {}
    for strat in strategies:
        strat_data = grouped[grouped["strategy"] == strat].set_index("pnl_date")["pnl"]
        cum = 0.0
        vals = []
        for d in all_dates:
            cum += float(strat_data.get(d, 0))
            vals.append(round(cum, 2))
        series[strat] = vals

    return {
        "dates": [str(d) for d in all_dates],
        "series": series,
    }


@app.route("/accounts")
@login_required
def accounts():
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    acct_filter = _account_sql_and(user_accounts)

    try:
        dfs = _bq_parallel(client, {
            "balances": ACCOUNT_BALANCES_QUERY.format(account_filter=acct_filter),
            "trades": TRADES_QUERY.format(account_filter=acct_filter),
            "current": CURRENT_POSITIONS_QUERY.format(account_filter=acct_filter),
            "strat_class": STRATEGY_CLASSIFICATION_QUERY.format(account_filter=acct_filter),
            "strat_summary": ACCOUNT_POSITIONS_SUMMARY_QUERY.format(account_filter=acct_filter),
        })
        balances_df = dfs["balances"]
        trades_df = dfs["trades"]
        current_df = dfs["current"]
        strat_class_df = dfs["strat_class"]
        strat_summary_df = dfs["strat_summary"]
    except Exception as exc:
        return render_template(
            "accounts.html",
            error=str(exc),
            kpis={},
            summary_chart_json="{}",
            strategy_chart_json="{}",
            strategy_rows=[],
            accounts=[],
            selected_account="",
        )

    # ------------------------------------------------------------------
    # Clean types
    # ------------------------------------------------------------------
    for col in ["market_value", "cost_basis", "unrealized_pnl", "unrealized_pnl_pct", "percent_of_account"]:
        if col in balances_df.columns:
            balances_df[col] = pd.to_numeric(balances_df[col], errors="coerce").fillna(0)

    for col in ["amount", "quantity", "price", "fees"]:
        trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
    trades_df["trade_date"] = pd.to_datetime(trades_df["trade_date"]).dt.date

    for col in ["unrealized_pnl", "market_value", "quantity", "current_price", "cost_basis"]:
        if col in current_df.columns:
            current_df[col] = pd.to_numeric(current_df[col], errors="coerce").fillna(0)

    for col in ["total_pnl", "num_trades"]:
        if col in strat_class_df.columns:
            strat_class_df[col] = pd.to_numeric(strat_class_df[col], errors="coerce").fillna(0)
    for col in ["open_date", "close_date"]:
        if col in strat_class_df.columns:
            strat_class_df[col] = pd.to_datetime(strat_class_df[col], errors="coerce").dt.date

    num_cols = ["total_pnl", "realized_pnl", "unrealized_pnl", "premium_received",
                "premium_paid", "num_trades", "num_winners", "num_losers",
                "dividend_income", "total_return"]
    for col in num_cols:
        if col in strat_summary_df.columns:
            strat_summary_df[col] = pd.to_numeric(strat_summary_df[col], errors="coerce").fillna(0)

    # ------------------------------------------------------------------
    # Safety-belt: re-filter in Python (SQL already filtered by account)
    # ------------------------------------------------------------------
    balances_df = _filter_df_by_accounts(balances_df, user_accounts)
    trades_df = _filter_df_by_accounts(trades_df, user_accounts)
    current_df = _filter_df_by_accounts(current_df, user_accounts)
    strat_class_df = _filter_df_by_accounts(strat_class_df, user_accounts)
    strat_summary_df = _filter_df_by_accounts(strat_summary_df, user_accounts)

    all_accounts = sorted(trades_df["account"].dropna().unique())
    selected_account = request.args.get("account", "")

    if selected_account:
        balances_df = balances_df[balances_df["account"] == selected_account]
        trades_df = trades_df[trades_df["account"] == selected_account]
        current_df = current_df[current_df["account"] == selected_account]
        strat_class_df = strat_class_df[strat_class_df["account"] == selected_account]
        strat_summary_df = strat_summary_df[strat_summary_df["account"] == selected_account]

    # ------------------------------------------------------------------
    # KPIs from balances
    # ------------------------------------------------------------------
    cash_rows = balances_df[balances_df["row_type"] == "cash"]
    total_rows = balances_df[balances_df["row_type"] == "account_total"]

    cash_balance = float(cash_rows["market_value"].sum())
    account_value = float(total_rows["market_value"].sum())
    invested_value = account_value - cash_balance
    acct_unrealized = float(total_rows["unrealized_pnl"].sum())
    acct_cost_basis = float(total_rows["cost_basis"].sum())

    # Realized P&L from positions_summary
    realized_pnl = float(strat_summary_df["realized_pnl"].sum())
    total_return = float(strat_summary_df["total_return"].sum())

    kpis = {
        "account_value": account_value,
        "cash_balance": cash_balance,
        "invested_value": invested_value,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": acct_unrealized,
        "total_return": total_return,
    }

    # ------------------------------------------------------------------
    # Chart 1: Cumulative P&L over time (summary) — from mart_daily_pnl
    # ------------------------------------------------------------------
    try:
        acct_filter_sql = _account_sql_and([selected_account] if selected_account else user_accounts)
        chart_df = client.query(
            CHART_DATA_ALL_QUERY.format(account_filter=acct_filter_sql)
        ).to_dataframe()
        chart_df = _filter_df_by_accounts(chart_df, user_accounts)
        if selected_account and not chart_df.empty:
            chart_df = chart_df[chart_df["account"] == selected_account]
        summary_chart = _build_account_chart_from_daily_pnl(chart_df, current_df)
    except Exception:
        summary_chart = {"dates": [], "equity": [], "options": [], "dividends": [], "total": []}

    # ------------------------------------------------------------------
    # Chart 2: Strategy P&L over time
    # ------------------------------------------------------------------
    strategy_chart = _build_strategy_time_chart(strat_class_df)

    # ------------------------------------------------------------------
    # Strategy summary table
    # ------------------------------------------------------------------
    if not strat_summary_df.empty:
        strat_summary_df["win_rate"] = strat_summary_df.apply(
            lambda r: r["num_winners"] / (r["num_winners"] + r["num_losers"])
            if (r["num_winners"] + r["num_losers"]) > 0 else 0,
            axis=1,
        )
        strategy_rows = strat_summary_df.to_dict(orient="records")
    else:
        strategy_rows = []

    return render_template(
        "accounts.html",
        kpis=kpis,
        summary_chart_json=json.dumps(summary_chart),
        strategy_chart_json=json.dumps(strategy_chart),
        strategy_rows=strategy_rows,
        accounts=all_accounts,
        selected_account=selected_account,
    )


@app.errorhandler(RequestEntityTooLarge)
def request_entity_too_large(_e):
    """CSV uploads exceed MAX_CONTENT_LENGTH (see config MAX_UPLOAD_MB)."""
    flash(
        "Upload too large. Try a shorter date range in your export, or raise MAX_UPLOAD_MB.",
        "danger",
    )
    if current_user.is_authenticated:
        return redirect(url_for("upload"))
    return redirect(url_for("index"))
