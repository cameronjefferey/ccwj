from flask import render_template, request, redirect, url_for, Response
from flask_login import login_required, current_user
from app import app
from app.bigquery_client import get_bigquery_client
from app.models import (
    get_accounts_for_user, is_admin, get_insight_for_user,
    get_mirror_score_history, get_journal_stats, list_journal_entries,
)
from google.cloud import bigquery
from datetime import datetime, date, timedelta
import pandas as pd
import json


def _user_account_list():
    """
    Return the list of accounts the current user is allowed to see,
    or None if the user is an admin (meaning: no filter, show everything).
    """
    if is_admin(current_user.username):
        return None                     # admin → no restriction
    return get_accounts_for_user(current_user.id)


def _account_sql_filter(accounts):
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
    return f"WHERE account IN ({quoted})"


def _account_sql_and(accounts):
    """Return AND clause for account filter when already in a WHERE. Empty string if no filter."""
    if accounts is None:
        return ""
    if not accounts:
        return "AND 1 = 0"
    quoted = ", ".join(f"'{a.replace(chr(39), chr(39)+chr(39))}'" for a in accounts)
    return f"AND account IN ({quoted})"


def _filter_df_by_accounts(df, accounts, col="account"):
    """Filter a DataFrame to only rows matching the user's accounts.
    accounts=None means admin → return unfiltered."""
    if accounts is None:
        return df
    if not accounts:
        return df.iloc[0:0]             # empty frame
    return df[df[col].isin(accounts)]


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


HOMEPAGE_STATS_QUERY_TPL = """
    SELECT
        COUNT(DISTINCT account) AS num_accounts,
        COUNT(DISTINCT symbol) AS num_symbols,
        COUNT(DISTINCT strategy) AS num_strategies,
        SUM(total_return) AS total_return,
        SUM(realized_pnl) AS realized_pnl,
        SUM(unrealized_pnl) AS unrealized_pnl,
        SUM(total_premium_received) AS premium_collected,
        SUM(total_dividend_income) AS dividend_income,
        SUM(num_individual_trades) AS total_trades,
        SUM(num_winners) AS total_winners,
        SUM(num_losers) AS total_losers
    FROM `ccwj-dbt.analytics.positions_summary`
    {where}
"""

HOMEPAGE_TOP_SYMBOLS_QUERY_TPL = """
    SELECT
        symbol,
        SUM(total_return) AS total_return,
        STRING_AGG(DISTINCT strategy, ', ' ORDER BY strategy) AS strategies
    FROM `ccwj-dbt.analytics.positions_summary`
    {where}
    GROUP BY symbol
    ORDER BY SUM(total_return) DESC
    LIMIT 5
"""

HOMEPAGE_STRATEGY_BREAKDOWN_QUERY_TPL = """
    SELECT
        strategy,
        SUM(total_return) AS total_return,
        COUNT(DISTINCT symbol) AS num_symbols
    FROM `ccwj-dbt.analytics.positions_summary`
    {where}
    GROUP BY strategy
    ORDER BY SUM(total_return) DESC
"""

DASHBOARD_WEEK_SUMMARY_QUERY = """
    SELECT
        SUM(trades_closed) AS trades_closed,
        SUM(total_pnl) AS total_pnl,
        SUM(num_winners) AS num_winners,
        SUM(num_losers) AS num_losers,
        SUM(trades_opened) AS trades_opened,
        MAX(best_pnl) AS max_best_pnl
    FROM `ccwj-dbt.analytics.mart_weekly_summary`
    WHERE week_start = @week_start
      {account_filter}
"""

DASHBOARD_BEST_TRADE_QUERY = """
    SELECT best_symbol, best_strategy, best_pnl
    FROM `ccwj-dbt.analytics.mart_weekly_summary`
    WHERE week_start = @week_start
      AND best_pnl IS NOT NULL
      {account_filter}
    ORDER BY best_pnl DESC
    LIMIT 1
"""

LATEST_WEEK_QUERY = """
    SELECT MAX(week_start) AS latest_week
    FROM `ccwj-dbt.analytics.mart_weekly_summary`
    WHERE (trades_closed > 0 OR trades_opened > 0)
      {account_filter}
"""

TRADER_PROFILE_QUERY = """
    SELECT
        STRING_AGG(DISTINCT strategy, ', ' ORDER BY strategy) AS strategies,
        SUM(num_individual_trades) AS total_trades,
        COUNT(DISTINCT symbol) AS num_symbols,
        SUM(num_winners) AS total_winners,
        SUM(num_losers) AS total_losers,
        ROUND(AVG(avg_days_in_trade), 0) AS avg_days,
        MIN(first_trade_date) AS first_trade
    FROM `ccwj-dbt.analytics.positions_summary`
    {where}
"""


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
    "tax-center": {
        "title": "Tax Center",
        "subtitle": "Short-term vs long-term, wash sales, and estimated tax impact.",
        "demo_partial": "features/_demo_tax.html",
        "value_bullets": [
            "Most traders ignore taxes until April. See your exposure year-round.",
            "Wash sale flags help you avoid unpleasant surprises at tax time.",
            "Choose your bracket and see estimated federal tax on gains—no surprises.",
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
    feature = FEATURES.get(slug)
    if not feature:
        from flask import abort
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
        f"User-agent: *\nAllow: /\nDisallow: /dashboard\nDisallow: /positions\nDisallow: /upload\nDisallow: /insights\nDisallow: /taxes\nDisallow: /settings\nDisallow: /accounts\nDisallow: /symbols\nDisallow: /position/\nSitemap: {base}/sitemap.xml\n",
        mimetype="text/plain",
    )


@app.route("/")
@app.route("/index")
def index():
    """Public landing page, or redirect to weekly review (home) if logged in."""
    if current_user.is_authenticated:
        return redirect(url_for("weekly_review"))
    return render_template("landing.html", title="Home")


@app.route("/dashboard")
@login_required
def dashboard():
    stats = {}
    top_symbols = []
    strategy_breakdown = []
    user_accounts = _user_account_list()
    week_summary = None
    mirror_history = []
    journal_stats = {"total_entries": 0, "last_entry_at": None}

    try:
        client = get_bigquery_client()
        where = _account_sql_filter(user_accounts)
        acct_and = _account_sql_and(user_accounts) if user_accounts else ""

        stats_df = client.query(HOMEPAGE_STATS_QUERY_TPL.format(where=where)).to_dataframe()
        if not stats_df.empty:
            row = stats_df.iloc[0]
            total_winners = int(row.get("total_winners", 0))
            total_losers = int(row.get("total_losers", 0))
            total_closed = total_winners + total_losers
            stats = {
                "num_accounts": int(row.get("num_accounts", 0)),
                "num_symbols": int(row.get("num_symbols", 0)),
                "num_strategies": int(row.get("num_strategies", 0)),
                "total_return": float(row.get("total_return", 0)),
                "realized_pnl": float(row.get("realized_pnl", 0)),
                "unrealized_pnl": float(row.get("unrealized_pnl", 0)),
                "premium_collected": float(row.get("premium_collected", 0)),
                "dividend_income": float(row.get("dividend_income", 0)),
                "total_trades": int(row.get("total_trades", 0)),
                "win_rate": total_winners / total_closed if total_closed else 0,
            }

        top_df = client.query(HOMEPAGE_TOP_SYMBOLS_QUERY_TPL.format(where=where)).to_dataframe()
        if not top_df.empty:
            for col in ["total_return"]:
                top_df[col] = pd.to_numeric(top_df[col], errors="coerce").fillna(0)
            top_symbols = top_df.to_dict(orient="records")

        strat_df = client.query(HOMEPAGE_STRATEGY_BREAKDOWN_QUERY_TPL.format(where=where)).to_dataframe()
        if not strat_df.empty:
            for col in ["total_return", "num_symbols"]:
                strat_df[col] = pd.to_numeric(strat_df[col], errors="coerce").fillna(0)
            strategy_breakdown = strat_df.to_dict(orient="records")

        # Weekly summary: try this week, fall back to most recent active week
        this_week_start = date.today() - timedelta(days=date.today().weekday())
        target_week = this_week_start
        try:
            week_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("week_start", "DATE", target_week),
                ]
            )
            week_df = client.query(
                DASHBOARD_WEEK_SUMMARY_QUERY.format(account_filter=acct_and),
                job_config=week_config,
            ).to_dataframe()
            has_data = not week_df.empty and int(week_df.iloc[0].get("trades_closed", 0) or 0) + int(week_df.iloc[0].get("trades_opened", 0) or 0) > 0
            if not has_data:
                latest_df = client.query(
                    LATEST_WEEK_QUERY.format(account_filter=acct_and)
                ).to_dataframe()
                if not latest_df.empty and latest_df.iloc[0]["latest_week"] is not None:
                    lw = latest_df.iloc[0]["latest_week"]
                    if hasattr(lw, "date"):
                        lw = lw.date()
                    elif not isinstance(lw, date):
                        lw = date.fromisoformat(str(lw)[:10])
                    target_week = lw
                    week_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("week_start", "DATE", target_week),
                        ]
                    )
                    week_df = client.query(
                        DASHBOARD_WEEK_SUMMARY_QUERY.format(account_filter=acct_and),
                        job_config=week_config,
                    ).to_dataframe()

            if not week_df.empty:
                wr = week_df.iloc[0]
                week_summary = {
                    "trades_closed": int(wr.get("trades_closed", 0) or 0),
                    "total_pnl": float(wr.get("total_pnl", 0) or 0),
                    "num_winners": int(wr.get("num_winners", 0) or 0),
                    "trades_opened": int(wr.get("trades_opened", 0) or 0),
                    "week_start": target_week.isoformat(),
                    "is_backfilled": target_week != this_week_start,
                }
                best_df = client.query(
                    DASHBOARD_BEST_TRADE_QUERY.format(account_filter=acct_and),
                    job_config=week_config,
                ).to_dataframe()
                if not best_df.empty:
                    br = best_df.iloc[0]
                    week_summary["best_symbol"] = br.get("best_symbol", "")
                    week_summary["best_pnl"] = float(br.get("best_pnl", 0) or 0)
        except Exception as e:
            # Week summary comes from mart_weekly_summary; if missing, pipeline may not have run yet
            if app.debug:
                app.logger.warning("Dashboard week summary failed: %s", e)

    except Exception as e:
        if app.debug:
            app.logger.warning("Dashboard stats/week summary failed: %s", e)

    has_accounts = user_accounts is None or len(user_accounts) > 0

    # Portfolio cumulative P&L chart
    portfolio_chart = {"dates": [], "equity": [], "options": [], "dividends": [], "total": []}
    if has_accounts:
        try:
            acct_filter = _account_sql_and(user_accounts) if user_accounts else ""
            chart_df = client.query(
                CHART_DATA_ALL_QUERY.format(account_filter=acct_filter)
            ).to_dataframe()
            chart_df = _filter_df_by_accounts(chart_df, user_accounts)
            current_df = client.query(CURRENT_POSITIONS_QUERY).to_dataframe()
            for col in ["unrealized_pnl", "market_value", "quantity", "current_price", "cost_basis"]:
                if col in current_df.columns:
                    current_df[col] = pd.to_numeric(current_df[col], errors="coerce").fillna(0)
            current_df = _filter_df_by_accounts(current_df, user_accounts)
            portfolio_chart = _build_account_chart_from_daily_pnl(chart_df, current_df)
        except Exception:
            pass

    # Process-first data: Mirror Score trend + journal nudge
    try:
        mirror_history = get_mirror_score_history(current_user.id, limit=8)
    except Exception:
        pass
    try:
        journal_stats = get_journal_stats(current_user.id)
    except Exception:
        pass

    # Unjournaled open positions — find positions without a journal entry
    unjournaled = []
    if has_accounts:
        try:
            acct_filter2 = _account_sql_and(user_accounts) if user_accounts else ""
            uj_df = client.query(f"""
                SELECT account, underlying_symbol AS symbol,
                       SUM(ABS(CAST(market_value AS FLOAT64))) AS exposure
                FROM `ccwj-dbt.analytics.int_enriched_current`
                WHERE quantity IS NOT NULL AND quantity != 0 {acct_filter2}
                GROUP BY 1, 2
                ORDER BY exposure DESC
            """).to_dataframe()
            if not uj_df.empty:
                journal_entries = list_journal_entries(current_user.id, limit=500)
                journaled_symbols = {
                    (e.get("account", ""), e.get("symbol", "").upper())
                    for e in journal_entries
                }
                for _, row in uj_df.iterrows():
                    key = (row["account"], str(row["symbol"]).upper())
                    if key not in journaled_symbols:
                        unjournaled.append({
                            "account": row["account"],
                            "symbol": row["symbol"],
                            "exposure": float(row.get("exposure", 0) or 0),
                        })
                    if len(unjournaled) >= 5:
                        break
        except Exception:
            pass

    insight = get_insight_for_user(current_user.id)

    # Trader profile for the hero section — used when no mirror history
    trader_profile = None
    if has_accounts and not mirror_history:
        try:
            where = _account_sql_filter(user_accounts)
            tp_df = client.query(TRADER_PROFILE_QUERY.format(where=where)).to_dataframe()
            if not tp_df.empty:
                tpr = tp_df.iloc[0]
                tw = int(tpr.get("total_winners", 0) or 0)
                tl = int(tpr.get("total_losers", 0) or 0)
                tc = tw + tl
                strats = str(tpr.get("strategies", "")).split(", ")
                top_strat = strats[0] if strats else ""
                trader_profile = {
                    "top_strategy": top_strat,
                    "num_strategies": len(strats),
                    "total_trades": int(tpr.get("total_trades", 0) or 0),
                    "num_symbols": int(tpr.get("num_symbols", 0) or 0),
                    "win_rate": tw / tc if tc else 0,
                    "avg_days": float(tpr.get("avg_days", 0) or 0),
                    "first_trade": str(tpr.get("first_trade", ""))[:10],
                }
        except Exception:
            pass

    is_first_visit = has_accounts and not mirror_history and journal_stats["total_entries"] == 0

    return render_template(
        "index.html",
        title="Home",
        stats=stats,
        top_symbols=top_symbols,
        strategy_breakdown=strategy_breakdown,
        has_accounts=has_accounts,
        insight=insight,
        portfolio_chart_json=json.dumps(portfolio_chart),
        week_summary=week_summary,
        mirror_history=mirror_history,
        journal_stats=journal_stats,
        unjournaled_positions=unjournaled,
        trader_profile=trader_profile,
        is_first_visit=is_first_visit,
    )


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

    return render_template(
        "get_started.html",
        title="Get Started",
        has_uploaded=has_uploaded,
        has_data=has_data,
    )


@app.route("/ping")
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

    # ------------------------------------------------------------------
    # 1. Read filter params
    # ------------------------------------------------------------------
    selected_account = request.args.get("account", "")
    selected_strategy = request.args.get("strategy", "")
    selected_status = request.args.get("status", "")
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
            df = client.query(DATE_FILTERED_QUERY, job_config=job_config).to_dataframe()
        else:
            df = client.query(DEFAULT_QUERY).to_dataframe()
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
    # 4. Filter to user's accounts, then build filter options
    # ------------------------------------------------------------------
    user_accounts = _user_account_list()
    df = _filter_df_by_accounts(df, user_accounts)

    accounts = sorted(df["account"].dropna().unique())
    strategies = sorted(df["strategy"].dropna().unique())
    symbols = sorted(df["symbol"].dropna().unique())

    filtered = df.copy()
    if selected_account:
        filtered = filtered[filtered["account"] == selected_account]
    if selected_strategy:
        filtered = filtered[filtered["strategy"] == selected_strategy]
    if selected_status:
        filtered = filtered[filtered["status"] == selected_status]
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
    # 8. Strategy detail rows (paginated)
    # ------------------------------------------------------------------
    all_rows = filtered.to_dict(orient="records")
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
        selected_status=selected_status,
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
    WHERE underlying_symbol = '{symbol}'
      AND trade_date IS NOT NULL
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
"""

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
        # Strategy summary from positions_summary
        summary_df = client.query(
            POSITION_SUMMARY_QUERY.format(symbol=safe_symbol)
        ).to_dataframe()

        # Trade history
        trades_df = client.query(
            POSITION_TRADES_QUERY.format(symbol=safe_symbol)
        ).to_dataframe()

        # Current positions
        current_df = client.query(
            POSITION_CURRENT_QUERY.format(symbol=safe_symbol)
        ).to_dataframe()
    except Exception as exc:
        return render_template(
            "position_detail.html",
            symbol=symbol,
            error=str(exc),
            kpis={},
            strategy_rows=[],
            trades=[],
            current_positions=[],
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

    # Filter to user's accounts
    summary_df = _filter_df_by_accounts(summary_df, user_accounts)
    trades_df = _filter_df_by_accounts(trades_df, user_accounts)
    current_df = _filter_df_by_accounts(current_df, user_accounts)

    # Optional account filter
    selected_account = request.args.get("account", "")
    if selected_account:
        summary_df = summary_df[summary_df["account"] == selected_account]
        trades_df = trades_df[trades_df["account"] == selected_account]
        current_df = current_df[current_df["account"] == selected_account]

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

    # Status (needed for open-only realized logic)
    status_col = None
    for c in ("status", "Status", "STATUS"):
        if c in (summary_df.columns if not summary_df.empty else []):
            status_col = c
            break
    statuses = summary_df[status_col].unique().tolist() if status_col and not summary_df.empty else []
    _has_open = any(str(s).strip().lower() == "open" for s in statuses if s is not None)
    _has_closed = any(str(s).strip().lower() == "closed" for s in statuses if s is not None)
    if _has_open and _has_closed:
        overall_status = "Mixed"
    elif _has_open:
        overall_status = "Open"
    else:
        overall_status = "Closed"

    # KPIs (aggregated across strategies)
    total_winners = int(summary_df["num_winners"].sum())
    total_losers = int(summary_df["num_losers"].sum())
    total_closed = total_winners + total_losers

    # Realized P/L only from closed trades. For open positions there is no realized (cost of buy is not a "realized loss").
    _sell_actions = ("equity_sell", "option_sell_to_close", "option_buy_to_close")
    has_sell_trades = (
        not trades_df.empty
        and "action" in trades_df.columns
        and trades_df["action"].astype(str).str.strip().isin(_sell_actions).any()
    )
    is_open_only = (
        overall_status == "Open"
        or (total_closed == 0 and not current_df.empty)
        or (not has_sell_trades and not current_df.empty)
    )
    if is_open_only:
        realized_for_display = 0.0
    else:
        realized_for_display = float(summary_df["realized_pnl"].sum()) if not summary_df.empty else 0.0

    if app.debug and symbol == "ATZAF":
        app.logger.warning(
            "position_detail ATZAF: status_col=%s overall_status=%s total_closed=%s is_open_only=%s realized_for_display=%s",
            status_col, overall_status, total_closed, is_open_only, realized_for_display,
        )

    kpis = {}
    if not summary_df.empty:
        unrealized_from_summary = float(summary_df["unrealized_pnl"].sum())
        if not current_df.empty and "unrealized_pnl" in current_df.columns:
            unrealized_from_summary = float(current_df["unrealized_pnl"].sum())
        kpis = {
            "total_return": realized_for_display + unrealized_from_summary,
            "realized_pnl": realized_for_display,
            "unrealized_pnl": unrealized_from_summary,
            "premium_collected": float(summary_df["total_premium_received"].sum()),
            "premium_paid": float(summary_df["total_premium_paid"].sum()),
            "dividend_income": float(summary_df["total_dividend_income"].sum()),
            "win_rate": total_winners / total_closed if total_closed else 0,
            "avg_days": float(summary_df["avg_days_in_trade"].mean()),
            "total_trades": int(summary_df["num_individual_trades"].sum()),
            "num_winners": total_winners,
            "num_losers": total_losers,
            "first_trade": str(summary_df["first_trade_date"].min()) if "first_trade_date" in summary_df.columns else "",
            "last_trade": str(summary_df["last_trade_date"].max()) if "last_trade_date" in summary_df.columns else "",
        }

    # Strategy rows
    strategy_rows = summary_df.to_dict(orient="records") if not summary_df.empty else []

    # Build chart data from pre-aggregated mart_daily_pnl
    chart_data = {"dates": [], "equity": [], "options": [], "dividends": [], "total": [], "underlying_price": [], "has_underlying_price": False}
    try:
        acct_filter = _account_sql_and([selected_account] if selected_account else user_accounts) if (selected_account or user_accounts) else ""
        chart_df = client.query(
            CHART_DATA_QUERY.format(symbol=safe_symbol, account_filter=acct_filter)
        ).to_dataframe()
        if not chart_df.empty:
            chart_data = _build_chart_from_daily_pnl(chart_df, current_df)
    except Exception:
        pass

    # Trade history rows
    trades_for_table = trades_df.copy()
    if "trade_date" in trades_for_table.columns:
        trades_for_table["trade_date"] = trades_for_table["trade_date"].astype(str)
    trades = trades_for_table.to_dict(orient="records") if not trades_for_table.empty else []

    # Current positions
    current_positions = current_df.to_dict(orient="records") if not current_df.empty else []

    # Available accounts for filter
    all_accounts = sorted(summary_df["account"].dropna().unique()) if not summary_df.empty else []

    return render_template(
        "position_detail.html",
        symbol=symbol,
        kpis=kpis,
        overall_status=overall_status,
        strategy_rows=strategy_rows,
        trades=trades,
        current_positions=current_positions,
        chart_data_json=json.dumps(chart_data),
        has_underlying_price=chart_data.get("has_underlying_price", False),
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
    ORDER BY underlying_symbol, trade_date
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
"""

STRATEGIES_MAP_QUERY = """
    SELECT account, symbol, strategy
    FROM `ccwj-dbt.analytics.positions_summary`
"""

SYMBOLS_PNL_QUERY = """
    SELECT account, symbol, status, realized_pnl, unrealized_pnl
    FROM `ccwj-dbt.analytics.positions_summary`
"""

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
    position_is_closed = current_df.empty
    last_trade_date = None

    dates, equity_s, options_s, dividends_s, total_s, price_s = (
        [], [], [], [], [], [],
    )

    for _, row in daily_df.iterrows():
        buy_qty = float(row.get("equity_buy_qty") or 0)
        buy_cost = float(row.get("equity_buy_cost") or 0)
        sell_qty = float(row.get("equity_sell_qty") or 0)
        sell_proceeds = float(row.get("equity_sell_proceeds") or 0)
        has_trade = bool(row.get("has_trade"))

        if has_trade:
            last_trade_date = row["date"]

        if position_is_closed and shares_held == 0 and not has_trade:
            continue

        if buy_qty > 0:
            shares_held += buy_qty
            total_cost += buy_cost

        if sell_qty > 0 and shares_held > 0:
            avg = total_cost / shares_held
            sold = min(sell_qty, shares_held)
            cum_realized += sell_proceeds - avg * sold
            total_cost = max(0, total_cost - avg * sold)
            shares_held = max(0, shares_held - sold)
        elif sell_qty > 0:
            cum_realized += sell_proceeds

        close = float(row.get("close_price") or 0)
        # If no close price on a buy day, use avg cost so open position doesn't show full cost as "loss"
        if close <= 0 and buy_qty > 0 and buy_cost > 0 and shares_held > 0:
            close = buy_cost / buy_qty
        unrealized = (shares_held * close - total_cost) if close > 0 and shares_held > 0 else 0
        eq_pnl = cum_realized + unrealized

        opt_pnl = float(row.get("cumulative_options_pnl") or 0)
        div_pnl = float(row.get("cumulative_dividends_pnl") or 0)
        oth_pnl = float(row.get("cumulative_other_pnl") or 0)

        dates.append(str(row["date"])[:10])
        equity_s.append(round(eq_pnl, 2))
        options_s.append(round(opt_pnl, 2))
        dividends_s.append(round(div_pnl, 2))
        total_s.append(round(eq_pnl + opt_pnl + div_pnl + oth_pnl, 2))
        price_s.append(round(close, 2) if close > 0 and shares_held > 0 else None)

    if not dates:
        return empty

    today_str = str(date.today())
    if not current_df.empty and dates[-1] != today_str:
        opt_unreal = float(current_df.loc[
            current_df["instrument_type"].isin(["Call", "Put"]), "unrealized_pnl"
        ].sum())
        eq_row = current_df[current_df["instrument_type"] == "Equity"]
        today_eq = equity_s[-1]
        if not eq_row.empty and shares_held > 0:
            p = float(eq_row["current_price"].iloc[0] or 0)
            if p:
                today_eq = cum_realized + (shares_held * p - total_cost)
        today_price = None
        if not eq_row.empty:
            today_price = float(eq_row["current_price"].iloc[0] or 0) or None

        dates.append(today_str)
        equity_s.append(round(today_eq, 2))
        options_s.append(round(options_s[-1] + opt_unreal, 2))
        dividends_s.append(dividends_s[-1])
        price_s.append(round(today_price, 2) if today_price else None)
        total_s.append(round(today_eq + options_s[-1] + dividends_s[-1], 2))

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

    try:
        trades_df = client.query(TRADES_QUERY).to_dataframe()
        current_df = client.query(CURRENT_POSITIONS_QUERY).to_dataframe()
        strat_df = client.query(STRATEGIES_MAP_QUERY).to_dataframe()
        pnl_df = client.query(SYMBOLS_PNL_QUERY).to_dataframe()
    except Exception as exc:
        return render_template(
            "symbols.html",
            error=str(exc),
            symbol_data=[],
            chart_data_json="[]",
            accounts=[],
            selected_account="",
            open_only=False,
        )

    # ------------------------------------------------------------------
    # Clean types
    # ------------------------------------------------------------------
    for col in ["amount", "quantity", "price", "fees"]:
        trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
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

    # ------------------------------------------------------------------
    # Restrict to user's accounts, then apply account filter
    # ------------------------------------------------------------------
    user_accounts = _user_account_list()
    trades_df = _filter_df_by_accounts(trades_df, user_accounts)
    current_df = _filter_df_by_accounts(current_df, user_accounts)
    pnl_df = _filter_df_by_accounts(pnl_df, user_accounts)

    accounts = sorted(trades_df["account"].dropna().unique())
    selected_account = request.args.get("account", "")
    open_only = request.args.get("open_only") == "1"
    positions_only = request.args.get("positions_only") == "1"

    if selected_account:
        trades_df = trades_df[trades_df["account"] == selected_account]
        current_df = current_df[current_df["account"] == selected_account]
        pnl_df = pnl_df[pnl_df["account"] == selected_account]

    # Restrict to symbols that have a current open position (match current_positions / int_enriched_current)
    if open_only:
        open_pairs = set(zip(current_df["account"].astype(str), current_df["symbol"].astype(str))) if not current_df.empty else set()
    else:
        open_pairs = None

    # Fetch pre-aggregated chart data from mart
    try:
        acct_filter = _account_sql_and([selected_account] if selected_account else user_accounts) if (selected_account or user_accounts) else ""
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

    for (account, symbol), group in trades_df.groupby(["account", "symbol"]):
        if open_pairs is not None and (str(account), str(symbol)) not in open_pairs:
            continue
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
            total_realized = round(float(group["amount"].sum()), 2)

        # Unrealized from current open positions (matches current positions table)
        unrealized = round(float(sym_current["unrealized_pnl"].sum()), 2) if not sym_current.empty else 0.0

        # Display semantics:
        # - Default view: total_return = realized (history) + unrealized (current)
        # - "Open positions only" view: focus on the live leg only — realized = 0,
        #   total_return = unrealized.
        display_realized = total_realized
        display_total = round(total_realized + unrealized, 2)
        if positions_only:
            display_realized = 0.0
            display_total = unrealized
        num_trades = len(group)
        first_date = str(group["trade_date"].min())
        last_date = str(group["trade_date"].max())
        strategies = strat_map.get((account, symbol), [])

        sym_chart_df = all_chart_df[
            (all_chart_df["account"] == account) & (all_chart_df["symbol"] == symbol)
        ] if not all_chart_df.empty else pd.DataFrame()
        chart = _build_chart_from_daily_pnl(sym_chart_df, sym_current)
        chart_data_list.append(chart)

        # Trade table rows (convert dates to str for Jinja)
        trades_table = group.copy()
        trades_table["trade_date"] = trades_table["trade_date"].astype(str)
        trades_list = trades_table.to_dict(orient="records")

        # Current positions table
        current_list = sym_current.to_dict(orient="records") if not sym_current.empty else []

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
            "current_positions": current_list,
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
    )


# ======================================================================
# Account Performance  (/accounts)
# ======================================================================

ACCOUNT_BALANCES_QUERY = """
    SELECT account, row_type, market_value, cost_basis,
           unrealized_pnl, unrealized_pnl_pct, percent_of_account
    FROM `ccwj-dbt.analytics.stg_account_balances`
"""

STRATEGY_CLASSIFICATION_QUERY = """
    SELECT account, symbol, strategy, status, open_date, close_date,
           total_pnl, num_trades
    FROM `ccwj-dbt.analytics.int_strategy_classification`
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

    try:
        balances_df = client.query(ACCOUNT_BALANCES_QUERY).to_dataframe()
        trades_df = client.query(TRADES_QUERY).to_dataframe()
        current_df = client.query(CURRENT_POSITIONS_QUERY).to_dataframe()
        strat_class_df = client.query(STRATEGY_CLASSIFICATION_QUERY).to_dataframe()
        strat_summary_df = client.query(ACCOUNT_POSITIONS_SUMMARY_QUERY).to_dataframe()
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
    # Restrict to user's accounts, then apply account filter
    # ------------------------------------------------------------------
    user_accounts = _user_account_list()
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
        acct_filter_sql = _account_sql_and([selected_account] if selected_account else user_accounts) if (selected_account or user_accounts) else ""
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
