"""
Daily Review — single-mode end-of-day pulse page.

The page used to fork into Friday Review / Monday Check / Mid-Week Check.
That made it three different products glued together: behavior-baseline
narrative on Friday, exposure tables on Monday, "today" framing in the
middle. Users actually want the same answer EVERY day at the close:

    1. What happened today.
    2. What's coming that I need to watch (earnings, expiries, ex-div).
    3. How are my positions doing in total (G/L stock vs option vs div).
    4. Same breakdown rolled up by strategy.
    5. Same breakdown rolled up by sector / subsector.

So we collapsed to one mode. The old mode-pills, behavioral baseline,
coaching-take, and Mon-Fri "diary" sections were removed from render. The
helper functions that produced them are kept in this module — tests pin
them and we don't want to thrash CI — but they aren't called from the
view. Future cleanup may delete them entirely once the daily shape settles.

Tenancy: every BQ read passes through `_tenant_sql_and` (SQL-level) and
every DataFrame is filtered via `_filter_df_by_tenant_ids` BEFORE any
merge / re-aggregation. See `.cursor/rules/bigquery-tenant-isolation.mdc`.
"""
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from flask import render_template, request
from flask_login import login_required, current_user
from app import app
from app.bigquery_client import get_bigquery_client
from app.models import (
    is_admin,
    get_mirror_score_for_user, get_mirror_score_history,
    get_insight_for_user,
    get_published_trade_fingerprints,
    get_user_profile,
    trade_fingerprint,
    get_review_visit,
    bump_review_visit,
)
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor
import pandas as pd


def _bq_parallel(client, queries):
    """Run multiple BigQuery queries in parallel. Returns dict of {name: DataFrame}.

    Resilience contract: a failure in ONE query must not blank the entire
    page. Pre-fix, a SQL typo in the "attribution" query crashed the
    whole batch — the caller's outer `except` swallowed it, set
    ``batch = {}``, and EVERY downstream section (snapshots, positions,
    movers, breakdowns) rendered em-dashes. Per-key isolation means one
    bad query produces one empty DataFrame, logged loudly, while the
    other eight sections still render real data. Mirrors the contract
    on ``app.routes._bq_parallel``.
    """
    results = {}

    def _run(name, spec):
        try:
            if isinstance(spec, tuple):
                sql, cfg = spec
                return name, client.query(sql, job_config=cfg).to_dataframe(), None
            return name, client.query(spec).to_dataframe(), None
        except Exception as exc:
            return name, pd.DataFrame(), exc

    with ThreadPoolExecutor(max_workers=min(len(queries), 8)) as pool:
        futures = [pool.submit(_run, n, s) for n, s in queries.items()]
        for f in futures:
            name, df, exc = f.result()
            results[name] = df
            if exc is not None:
                try:
                    app.logger.error(
                        "_bq_parallel: query %r failed: %s", name, exc,
                    )
                except Exception:
                    pass

    return results


def _user_account_list():
    from app.routes import _user_account_list as _routes_user_account_list
    return _routes_user_account_list()


# Tenant-scoped query helpers live in app.routes (v2 tenant_id cutover).
from app.routes import (
    _tenants_for_scope,
    _tenant_sql_and,  # noqa: E402,F401
    _tenant_sql_filter as _tenant_sql_where,  # noqa: E402,F401
    _filter_df_by_tenant_ids,  # noqa: E402,F401
    _tenant_label_map_for_user,  # noqa: E402,F401
)


def _classify_expiring_moneyness(*, instrument_type, option_type, stock_price, strike):
    """Return (itm: bool|None, distance: float|None) for an option vs the
    current stock price.

    Conventions:
        - Call ITM when stock >= strike (right to BUY at strike < market).
        - Put  ITM when stock <= strike (right to SELL at strike > market).
        - `distance` is signed so the magnitude reads as "$X away from the
          ITM/OTM boundary"; the template renders abs(distance).

    Robust against either input string for the option side because the
    pipeline carries two columns: the canonical full label
    (`instrument_type` = 'Call' / 'Put', from stg_current's case statement)
    and the OSI single-char (`option_type` = 'C' / 'P'). The original
    implementation only checked one against the wrong literal and
    silently inverted ITM/OTM for every call expiring soon — exactly
    the bug a user spotted on a PLTR 141 Call with stock @ $137.92
    showing as "ITM $3.08" instead of "OTM $3.08".
    """
    try:
        sp = float(stock_price or 0)
        k = float(strike or 0)
    except (TypeError, ValueError):
        return None, None
    if sp <= 0 or k <= 0:
        return None, None

    instr = str(instrument_type or "").strip()
    osi = str(option_type or "").strip().upper()
    is_call = (instr == "Call") or osi.startswith("C")
    is_put = (instr == "Put") or osi.startswith("P")
    if not (is_call or is_put):
        return None, None

    if is_call:
        return sp >= k, round(sp - k, 2)
    return sp <= k, round(k - sp, 2)


MARKET_PERF_QUERY = """
WITH prices AS (
    SELECT symbol, date, close_price
    FROM `ccwj-dbt.analytics.stg_daily_prices`
    WHERE symbol IN ('SPY', 'QQQ')
      AND date >= @ytd_start
      AND close_price IS NOT NULL AND close_price > 0
),
week_bounds AS (
    SELECT symbol,
           MIN(CASE WHEN date >= @week_start THEN close_price END) AS week_open,
           MAX(CASE WHEN date >= @week_start THEN date END) AS week_last_date
    FROM prices
    WHERE date >= @week_start
    GROUP BY symbol
),
ytd_bounds AS (
    SELECT symbol,
           MIN(CASE WHEN date = (SELECT MIN(date) FROM prices p2 WHERE p2.symbol = p.symbol) THEN close_price END) AS ytd_open,
           MAX(date) AS ytd_last_date
    FROM prices p
    GROUP BY symbol
),
latest AS (
    SELECT p.symbol, p.close_price AS latest_close
    FROM prices p
    INNER JOIN (
        SELECT symbol, MAX(date) AS max_date FROM prices GROUP BY symbol
    ) m ON p.symbol = m.symbol AND p.date = m.max_date
)
SELECT
    w.symbol,
    ROUND(SAFE_DIVIDE(l.latest_close - w.week_open, w.week_open) * 100, 2) AS week_pct,
    ROUND(SAFE_DIVIDE(l.latest_close - y.ytd_open, y.ytd_open) * 100, 2) AS ytd_pct
FROM week_bounds w
JOIN ytd_bounds y USING (symbol)
JOIN latest l USING (symbol)
"""


def _get_market_performance(week_start, today):
    """SPY/QQQ returns from pre-loaded daily prices in BigQuery."""
    out = {"spy_week_pct": None, "qqq_week_pct": None, "spy_ytd_pct": None, "qqq_ytd_pct": None}
    try:
        client = get_bigquery_client()
        ytd_start = date(today.year, 1, 1)
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("week_start", "DATE", week_start),
            bigquery.ScalarQueryParameter("ytd_start", "DATE", ytd_start),
        ])
        df = client.query(MARKET_PERF_QUERY, job_config=cfg).to_dataframe()
        for _, row in df.iterrows():
            sym = str(row["symbol"]).upper()
            if sym == "SPY":
                out["spy_week_pct"] = float(row["week_pct"]) if row["week_pct"] is not None else None
                out["spy_ytd_pct"] = float(row["ytd_pct"]) if row["ytd_pct"] is not None else None
            elif sym == "QQQ":
                out["qqq_week_pct"] = float(row["week_pct"]) if row["week_pct"] is not None else None
                out["qqq_ytd_pct"] = float(row["ytd_pct"]) if row["ytd_pct"] is not None else None
    except Exception as e:
        if app.debug:
            app.logger.warning("Market performance query failed: %s", e)
        return None
    return out


# Index total return between a start date and the latest available close.
# Powers the "would you have beaten the index?" benchmark rows under the
# Performance-by-Account scorecard. SPY (S&P 500) / QQQ (Nasdaq 100) daily
# closes are pre-loaded into stg_daily_prices (no tenant data — public
# market prices, nothing to leak).
BENCHMARK_RETURN_QUERY = """
WITH prices AS (
    SELECT symbol, date, close_price
    FROM `ccwj-dbt.analytics.stg_daily_prices`
    WHERE symbol IN ('SPY', 'QQQ')
      AND date >= @start_date
      AND close_price IS NOT NULL AND close_price > 0
),
start_close AS (
    SELECT p.symbol, p.close_price AS start_price
    FROM prices p
    INNER JOIN (SELECT symbol, MIN(date) AS d FROM prices GROUP BY symbol) m
        ON p.symbol = m.symbol AND p.date = m.d
),
latest_close AS (
    SELECT p.symbol, p.close_price AS latest_price
    FROM prices p
    INNER JOIN (SELECT symbol, MAX(date) AS d FROM prices GROUP BY symbol) m
        ON p.symbol = m.symbol AND p.date = m.d
)
SELECT s.symbol,
       ROUND(SAFE_DIVIDE(l.latest_price - s.start_price, s.start_price) * 100, 2) AS return_pct
FROM start_close s
JOIN latest_close l USING (symbol)
"""

# (warehouse symbol, display label) for the benchmark comparison rows.
BENCHMARK_INDEXES = [("SPY", "S&P 500"), ("QQQ", "Nasdaq 100")]


def _get_benchmark_returns(start_date):
    """Total % return of each benchmark index from ``start_date`` to the
    latest available close. Returns ``{"SPY": pct, "QQQ": pct}`` (percent,
    not fraction); empty dict on failure (caller renders no benchmark)."""
    out = {}
    try:
        client = get_bigquery_client()
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        ])
        df = client.query(BENCHMARK_RETURN_QUERY, job_config=cfg).to_dataframe()
        for _, row in df.iterrows():
            sym = str(row["symbol"]).upper()
            out[sym] = float(row["return_pct"]) if row["return_pct"] is not None else None
    except Exception as e:
        if app.debug:
            app.logger.warning("Benchmark return query failed: %s", e)
        return {}
    return out


def _build_benchmark_rows(basis, returns):
    """"If your capital had been in the index instead" rows for the
    scorecard. Uses the totals-line capital + holding window so the $,
    G/L %, and annualized columns line up directly under the user's
    Total row.

    ``basis`` = {"capital_at_risk": float, "days": int} (from
    ``_build_account_breakdown``). ``returns`` = output of
    ``_get_benchmark_returns``. Annualized mirrors the portfolio math
    (× 365 / max(days, 30)) so it's apples-to-apples with the user's
    Annualized column.
    """
    if not basis or not returns:
        return []
    cap = float(basis.get("capital_at_risk") or 0)
    days = int(basis.get("days") or 0)
    rows = []
    for sym, label in BENCHMARK_INDEXES:
        pct = returns.get(sym)
        if pct is None:
            continue
        dollar = round(cap * pct / 100.0, 2)
        ann = (
            round(pct * 365.0 / max(days, ANNUALIZED_MIN_DAYS), 1)
            if days > 0 else None
        )
        rows.append({
            "symbol": sym,
            "label": label,
            "total_pnl": dollar,
            "pct_return": round(pct, 1),
            "annualized_pct": ann,
        })
    return rows


# Index % change over the account-snapshot periods (1 day / 1 week /
# 1 month) so a benchmark row can sit under the snapshot table's Total.
# Each window's base is the most recent close on/before (latest_date −
# window) — mirrors how the account snapshot picks the prior trading-day /
# week / month base. Pure market data (no tenant scope).
BENCHMARK_SNAPSHOT_QUERY = """
WITH prices AS (
    SELECT symbol, date, close_price
    FROM `ccwj-dbt.analytics.stg_daily_prices`
    WHERE symbol IN ('SPY', 'QQQ')
      AND close_price IS NOT NULL AND close_price > 0
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 70 DAY)
),
latest AS (
    SELECT symbol, MAX(date) AS latest_date FROM prices GROUP BY symbol
),
px AS (
    SELECT p.symbol, p.date, p.close_price, l.latest_date
    FROM prices p JOIN latest l USING (symbol)
)
SELECT
    symbol,
    ANY_VALUE(IF(date = latest_date, close_price, NULL)) AS latest_close,
    ARRAY_AGG(IF(date < latest_date, close_price, NULL)
              IGNORE NULLS ORDER BY date DESC LIMIT 1)[SAFE_OFFSET(0)] AS day_close,
    ARRAY_AGG(IF(date <= DATE_SUB(latest_date, INTERVAL 7 DAY), close_price, NULL)
              IGNORE NULLS ORDER BY date DESC LIMIT 1)[SAFE_OFFSET(0)] AS week_close,
    ARRAY_AGG(IF(date <= DATE_SUB(latest_date, INTERVAL 30 DAY), close_price, NULL)
              IGNORE NULLS ORDER BY date DESC LIMIT 1)[SAFE_OFFSET(0)] AS month_close
FROM px
GROUP BY symbol
"""

# Compact labels for the snapshot benchmark rows.
BENCHMARK_SHORT_LABELS = {"SPY": "S&P 500", "QQQ": "Nasdaq 100"}


def _build_benchmark_snapshot(bench_df):
    """Index % change over the snapshot periods (1d / 1w / 1m), one entry
    per benchmark index, ordered SPY then QQQ. Renders as a benchmark row
    beneath the account-snapshot Total so the trader can compare each
    period's account move to the market. Returns [] on missing data.
    """
    if bench_df is None or bench_df.empty:
        return []

    def _pct(latest, base):
        try:
            lt = float(latest)
            bs = float(base)
        except (TypeError, ValueError):
            return None
        if bs <= 0:
            return None
        return round((lt - bs) / bs * 100.0, 2)

    by_symbol = {}
    for _, r in bench_df.iterrows():
        sym = str(r.get("symbol") or "").upper()
        latest = r.get("latest_close")
        by_symbol[sym] = {
            "symbol": sym,
            "label": BENCHMARK_SHORT_LABELS.get(sym, sym),
            "day_pct": _pct(latest, r.get("day_close")),
            "week_pct": _pct(latest, r.get("week_close")),
            "month_pct": _pct(latest, r.get("month_close")),
        }
    return [by_symbol[s] for s, _ in BENCHMARK_INDEXES if s in by_symbol]


WEEKLY_SUMMARY_COMBINED_QUERY = """
SELECT *
FROM `ccwj-dbt.analytics.mart_weekly_summary`
WHERE week_start IN UNNEST(@week_starts)
  {tenant_filter}
"""

LATEST_ACTIVE_WEEK_QUERY = """
SELECT MAX(week_start) AS latest_week
FROM `ccwj-dbt.analytics.mart_weekly_summary`
WHERE (trades_closed > 0 OR trades_opened > 0)
  {tenant_filter}
"""

# Live account value per tenant (from the latest balances snapshot) for
# context, return % vs account, AND a fallback for the Daily Review
# "Account Value" hero when the daily-snapshot mart hasn't captured a
# brand-new account yet (mart_account_snapshots_enriched lags a build
# behind a just-connected account's first balance row). Per-tenant grain
# so Flask can both SUM for the aggregate equity_snapshot and map each
# account's live total onto its placeholder snapshot row.
ACCOUNT_VALUE_QUERY = """
SELECT
  tenant_id,
  ANY_VALUE(account) AS account,
  COALESCE(SUM(CASE WHEN row_type = 'account_total' THEN market_value ELSE 0 END), 0) AS account_value,
  COALESCE(SUM(CASE WHEN row_type = 'cash' THEN market_value ELSE 0 END), 0) AS cash_balance
FROM `ccwj-dbt.analytics.stg_account_balances`
WHERE 1=1 {tenant_filter}
GROUP BY tenant_id
"""

# Weekly account return from dbt mart (replaces inline WEEKLY_ACCOUNT_CHANGE_QUERY)
WEEKLY_RETURNS_QUERY = """
SELECT account, start_value, end_value, weekly_return_pct
FROM `ccwj-dbt.analytics.mart_account_weekly_returns`
WHERE week_start = @week_start
  {tenant_filter}
"""

# Today's snapshot: per-account enriched rows; Flask aggregates by date for user's accounts
TODAY_SNAPSHOT_ENRICHED_QUERY = """
SELECT account, tenant_id, date, account_value,
  base_1d_date, base_1d_value, delta_1d, delta_1d_pct,
  base_1w_date, base_1w_value, delta_1w, delta_1w_pct,
  base_1m_date, base_1m_value, delta_1m, delta_1m_pct
FROM `ccwj-dbt.analytics.mart_account_snapshots_enriched`
WHERE 1=1 {tenant_filter}
ORDER BY date DESC
"""

# Trades this week from dbt mart (replaces TRADES_THIS_WEEK_QUERY + Python cost/value calc)
WEEKLY_TRADES_MART_QUERY = """
SELECT
  account,
  tenant_id,
  symbol,
  strategy,
  trade_symbol,
  open_date,
  close_date,
  status,
  trade_cost,
  current_market_value,
  current_unrealized_pnl,
  total_pnl,
  num_trades
FROM `ccwj-dbt.analytics.mart_weekly_trades`
WHERE week_start = @week_start
  {tenant_filter}
ORDER BY close_date DESC NULLS LAST, open_date DESC
"""

# Weekly behavior baseline — compare this week to recent 8-week norm
WEEKLY_BEHAVIOR_QUERY = """
SELECT
  account,
  week_start,
  trades_closed,
  total_pnl,
  num_winners,
  num_losers,
  win_rate_week,
  avg_trades_closed_8w,
  avg_total_pnl_8w,
  avg_win_rate_8w,
  baseline_weeks_8w
FROM `ccwj-dbt.analytics.mart_weekly_behavior_enriched`
WHERE week_start = @week_start
  {tenant_filter}
"""

# Open positions exposure for Monday Risk Check
EXPOSURE_QUERY = """
SELECT
    account,
    underlying_symbol AS symbol,
    instrument_type,
    SUM(ABS(CAST(market_value AS FLOAT64))) AS exposure
FROM `ccwj-dbt.analytics.int_enriched_current`
WHERE quantity IS NOT NULL AND quantity != 0
  {tenant_filter}
GROUP BY 1, 2, 3
ORDER BY exposure DESC
"""

# Upcoming earnings for currently-held holdings (next 14 days).
#
# Source: stg_earnings_calendar (per-symbol next_earnings_date from
# yfinance, populated by scripts/refresh_earnings_calendar.py). Joined to
# distinct underlyings currently held via int_enriched_current — TENANT
# SCOPE applied inside the holdings CTE via {tenant_filter}, mirroring
# the same predicate used by OPEN_POSITIONS_QUERY below. The resulting
# rows are symbol-grain (no account column); the symbol set is already
# narrowed to the user's holdings, so no Python-side _filter_df_by_accounts
# is needed (and would be a no-op anyway).
#
# Why open holdings (vs everything ever traded): "earnings this week" is
# only useful for positions the trader currently has exposure to. A
# closed-out position from six months ago reporting earnings is noise.
EARNINGS_UPCOMING_QUERY = """
WITH holdings AS (
    SELECT DISTINCT UPPER(TRIM(underlying_symbol)) AS symbol
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE quantity IS NOT NULL AND quantity != 0
      {tenant_filter}
)
SELECT
    e.symbol,
    e.next_earnings_date,
    e.earnings_window_start,
    e.earnings_window_end,
    DATE_DIFF(e.next_earnings_date, CURRENT_DATE(), DAY) AS days_until,
    m.long_name,
    m.sector,
    m.subsector
FROM `ccwj-dbt.analytics.stg_earnings_calendar` e
JOIN holdings h USING (symbol)
LEFT JOIN `ccwj-dbt.analytics.stg_symbol_metadata` m USING (symbol)
WHERE e.next_earnings_date BETWEEN CURRENT_DATE()
                              AND DATE_ADD(CURRENT_DATE(), INTERVAL 14 DAY)
ORDER BY e.next_earnings_date, e.symbol
"""

# Position-level P&L attribution: one row per (tenant_id, account, user_id,
# symbol) with equity P&L, option P&L, dividend income, capital deployed,
# status, and sector context. The grain leads with ``tenant_id`` (and every
# join keys on it) because the ``account`` display label is NOT unique — a
# user can hold the same symbol in several physical accounts that all surface
# as "Schwab Account". Without the tenant_id join key, the per-tenant CTEs
# (int_dividends, int_strategy_classification, …) fan out the LEFT JOINs and
# multiply equity P&L / capital by the number of tenants (QTUM 5-tenant case:
# rendered 4× the true $68k). The Python layer groups by ``symbol`` to produce
# the combined breakdown, so emitting one clean row per tenant sums correctly.
# Mirrors the per-position spreadsheet the user
# tracks in Excel ("CC Trading Summary": G/L Stock | G/L Option | Dividend
# | Net | Annualized). Powers the position / strategy / sector breakdown
# tables on the Daily Review.
#
# Capital deployed (the denominator of the annualized return) is the sum
# of:
#   • equity buy cash      — abs(amount) on stg_history.action='equity_buy'
#   • option buy cash      — abs(amount) on stg_history.action='option_buy'
#   • current equity cost  — broker snapshot cost_basis on Equity rows
#                            still held (covers transferred-in lots that
#                            don't have a buy row in stg_history)
# It's a PROXY for peak-capital-deployed, not a financial-accountant's
# average-cost-base. Good enough for "what was at risk" annualized math.
# Falls back to abs(net_pnl) when nothing else is available so the
# annualized column doesn't go ±infinity on a free-money dividend lot.
#
# Tenancy: every CTE that hits a user-data table carries {tenant_filter}.
# Sentinel passed as ``week_start`` when a caller wants the LIFETIME view
# (every closed group qualifies, so the scoped sums collapse back to lifetime).
# Used by the /accounts detail page; the Daily Review scorecard passes the real
# Monday so old closed groups drop out of the per-asset-class columns.
ATTRIBUTION_LIFETIME_SENTINEL = "1900-01-01"

POSITION_ATTRIBUTION_QUERY = """
WITH classification AS (
    SELECT tenant_id, account, user_id, symbol, trade_group_type, total_pnl,
           status, open_date, close_date
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE 1=1 {tenant_filter}
),
per_sym_pnl AS (
    -- equity_pnl / option_pnl are scoped to trade groups that are CURRENTLY
    -- OPEN or were CLOSED on/after {week_start}. This keeps each asset-class
    -- column honest under the Daily Review "open + closed this week" lens: a
    -- symbol that's only in scope because it has an open EQUITY leg must NOT
    -- drag in option P&L from contracts that closed months ago (the "stale
    -- option line" bug — Cameron 401k showed -$3,757 of options that all
    -- closed in Jul/Aug 2025). Callers wanting the LIFETIME view (e.g. the
    -- /accounts detail page) pass a far-past sentinel date so every closed
    -- group qualifies and the sums collapse back to lifetime. The COUNTIF /
    -- first_open / last_activity fields below stay LIFETIME on purpose — they
    -- drive "is this symbol open / when did it last move" row selection.
    SELECT
        tenant_id, account, user_id, symbol,
        SUM(CASE WHEN trade_group_type='equity_session'
                  AND (status='Open' OR close_date >= DATE '{week_start}')
                 THEN total_pnl ELSE 0 END) AS equity_pnl,
        SUM(CASE WHEN trade_group_type='option_contract'
                  AND (status='Open' OR close_date >= DATE '{week_start}')
                 THEN total_pnl ELSE 0 END) AS option_pnl,
        COUNTIF(trade_group_type='equity_session'  AND status='Open') AS num_equity_open,
        COUNTIF(trade_group_type='option_contract' AND status='Open') AS num_option_open,
        COUNTIF(trade_group_type='option_contract' AND status='Closed') AS num_option_closed,
        COUNTIF(trade_group_type='equity_session'  AND status='Closed') AS num_equity_closed,
        MIN(open_date) AS first_open_date,
        MAX(COALESCE(close_date, CURRENT_DATE())) AS last_activity_date
    FROM classification
    GROUP BY 1, 2, 3, 4
),
per_sym_div AS (
    SELECT tenant_id, account, user_id, symbol,
           total_dividend_income AS dividend_income,
           dividend_count, last_dividend_date
    FROM `ccwj-dbt.analytics.int_dividends`
    WHERE 1=1 {tenant_filter}
),
per_sym_capital AS (
    -- stg_history exposes ``trade_symbol`` (raw broker fill symbol) and
    -- ``underlying_symbol`` (canonical per-position label that matches
    -- int_strategy_classification.symbol). We attribute capital to the
    -- underlying so option fills roll up with their equity siblings.
    SELECT
        tenant_id, account, user_id, UPPER(TRIM(underlying_symbol)) AS symbol,
        SUM(CASE WHEN action='equity_buy' THEN ABS(amount) ELSE 0 END) AS equity_capital,
        SUM(CASE WHEN action='option_buy' THEN ABS(amount) ELSE 0 END) AS option_capital_paid,
        SUM(CASE WHEN action='option_sell' THEN ABS(amount) ELSE 0 END) AS option_premium_collected
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE underlying_symbol IS NOT NULL
      {tenant_filter}
    GROUP BY 1, 2, 3, 4
),
per_sym_holdings AS (
    SELECT
        tenant_id, account, user_id, underlying_symbol AS symbol,
        SUM(CASE WHEN instrument_type='Equity' THEN COALESCE(cost_basis, 0) ELSE 0 END) AS current_equity_cost,
        SUM(CASE WHEN instrument_type='Equity' THEN COALESCE(market_value, 0) ELSE 0 END) AS current_equity_value,
        SUM(CASE WHEN instrument_type IN ('Call','Put')
                 THEN COALESCE(market_value, 0) ELSE 0 END) AS current_option_value,
        SUM(CASE WHEN instrument_type IN ('Call','Put')
                 THEN COALESCE(unrealized_pnl, 0) ELSE 0 END) AS current_option_unrealized,
        SUM(CASE WHEN instrument_type='Equity' THEN COALESCE(unrealized_pnl, 0) ELSE 0 END) AS current_equity_unrealized,
        SUM(CASE WHEN instrument_type='Equity' THEN ABS(COALESCE(quantity, 0)) ELSE 0 END) AS current_equity_shares,
        COUNTIF(instrument_type='Equity') AS num_equity_legs,
        COUNTIF(instrument_type IN ('Call','Put')) AS num_option_legs,
        MAX(current_price) AS current_price
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE quantity IS NOT NULL AND quantity != 0
      {tenant_filter}
    GROUP BY 1, 2, 3, 4
),
sym_meta AS (
    -- stg_symbol_metadata exposes symbol/sector/subsector/long_name/market_cap;
    -- dividend_yield is not (yet) materialized at this layer — see int_dividends
    -- for the historical per-symbol dividend stream consumed by per_sym_div.
    SELECT symbol, sector, subsector, long_name, market_cap
    FROM `ccwj-dbt.analytics.stg_symbol_metadata`
)
SELECT
    p.tenant_id,
    p.account,
    p.user_id,
    p.symbol,
    ROUND(COALESCE(p.equity_pnl, 0), 2) AS equity_pnl,
    ROUND(COALESCE(p.option_pnl, 0), 2) AS option_pnl,
    ROUND(COALESCE(d.dividend_income, 0), 2) AS dividend_income,
    ROUND(COALESCE(p.equity_pnl, 0) + COALESCE(p.option_pnl, 0)
          + COALESCE(d.dividend_income, 0), 2) AS net_pnl,
    ROUND(COALESCE(c.equity_capital, 0), 2)            AS equity_capital,
    ROUND(COALESCE(c.option_capital_paid, 0), 2)       AS option_capital_paid,
    ROUND(COALESCE(c.option_premium_collected, 0), 2)  AS option_premium_collected,
    ROUND(COALESCE(h.current_equity_cost, 0), 2)       AS current_equity_cost,
    ROUND(COALESCE(h.current_equity_value, 0), 2)      AS current_equity_value,
    ROUND(COALESCE(h.current_option_value, 0), 2)      AS current_option_value,
    ROUND(COALESCE(h.current_option_unrealized, 0), 2) AS current_option_unrealized,
    ROUND(COALESCE(h.current_equity_unrealized, 0), 2) AS current_equity_unrealized,
    COALESCE(h.current_equity_shares, 0)               AS current_equity_shares,
    COALESCE(h.num_equity_legs, 0)                     AS num_equity_legs,
    COALESCE(h.num_option_legs, 0)                     AS num_option_legs,
    COALESCE(p.num_equity_open + p.num_option_open, 0) AS num_open_groups,
    COALESCE(p.num_equity_closed + p.num_option_closed, 0) AS num_closed_groups,
    h.current_price,
    p.first_open_date,
    p.last_activity_date,
    DATE_DIFF(p.last_activity_date, p.first_open_date, DAY) AS days_held,
    CASE
        WHEN (COALESCE(p.num_equity_open, 0) + COALESCE(p.num_option_open, 0)
              + COALESCE(h.num_equity_legs, 0) + COALESCE(h.num_option_legs, 0)) > 0
        THEN 'Open'
        ELSE 'Closed'
    END AS status,
    COALESCE(m.sector, 'Unknown')    AS sector,
    COALESCE(m.subsector, 'Unknown') AS subsector,
    m.long_name                      AS company_name,
    d.last_dividend_date,
    COALESCE(d.dividend_count, 0)    AS dividend_count
FROM per_sym_pnl p
LEFT JOIN per_sym_div d
    ON (p.tenant_id IS NOT DISTINCT FROM d.tenant_id)
    AND p.account = d.account
    AND (p.user_id IS NOT DISTINCT FROM d.user_id)
    AND p.symbol = d.symbol
LEFT JOIN per_sym_capital c
    ON (p.tenant_id IS NOT DISTINCT FROM c.tenant_id)
    AND p.account = c.account
    AND (p.user_id IS NOT DISTINCT FROM c.user_id)
    AND p.symbol = c.symbol
LEFT JOIN per_sym_holdings h
    ON (p.tenant_id IS NOT DISTINCT FROM h.tenant_id)
    AND p.account = h.account
    AND (p.user_id IS NOT DISTINCT FROM h.user_id)
    AND p.symbol = h.symbol
LEFT JOIN sym_meta m
    ON UPPER(TRIM(p.symbol)) = m.symbol
"""

# Today's price moves for currently-held symbols. Joins the user's
# current equity holdings to two-day-window daily prices so we can
# render "biggest movers right now" with a $ impact per position.
TODAY_MOVES_QUERY = """
WITH holdings AS (
    SELECT
        underlying_symbol AS symbol,
        SUM(CASE WHEN instrument_type='Equity' THEN ABS(COALESCE(quantity, 0)) ELSE 0 END) AS shares,
        SUM(CASE WHEN instrument_type='Equity' THEN COALESCE(market_value, 0) ELSE 0 END) AS market_value
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE quantity IS NOT NULL AND quantity != 0
      {tenant_filter}
    GROUP BY 1
),
recent_prices AS (
    SELECT
        symbol,
        date,
        close_price,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
    FROM `ccwj-dbt.analytics.stg_daily_prices`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)
      AND close_price IS NOT NULL AND close_price > 0
),
pair AS (
    SELECT
        cur.symbol,
        cur.date AS today_date,
        cur.close_price AS today_close,
        prev.date AS prev_date,
        prev.close_price AS prev_close
    FROM recent_prices cur
    JOIN recent_prices prev
      ON cur.symbol = prev.symbol AND cur.rn = 1 AND prev.rn = 2
)
SELECT
    h.symbol,
    h.shares,
    h.market_value AS current_value,
    p.today_close,
    p.prev_close,
    p.today_date,
    p.prev_date,
    ROUND(p.today_close - p.prev_close, 4) AS price_change,
    ROUND(SAFE_DIVIDE(p.today_close - p.prev_close, p.prev_close) * 100, 2) AS price_change_pct,
    ROUND(h.shares * (p.today_close - p.prev_close), 2) AS dollar_impact
FROM holdings h
JOIN pair p USING (symbol)
WHERE h.shares > 0
"""

# After-hours movers (CLOSE-BASED REPORTING, June 2026).
#
# Core reporting now anchors equities on the official close (see AGENTS.md
# "Pricing Precedence" + int_enriched_current). The broker's after-hours
# mark — captured whenever the connection last synced — is no longer allowed
# to drive any "current value" number, but it is still useful signal, so we
# surface it HERE explicitly, clearly labeled "as of last broker sync".
#
# after-hours move per share = broker mark (stg_current.market_value/quantity,
# the raw broker snapshot) - today's official yfinance close. The join to
# today's close gates this to AFTER the bell (yfinance only publishes the
# close once the regular session ends) — intraday there is no close row, so
# the section is naturally empty until the close lands. Read from stg_current
# directly (NOT int_enriched_current, which is now close-priced and would
# show ~zero after-hours move by construction).
AFTER_HOURS_MOVERS_QUERY = """
WITH holdings AS (
    SELECT
        underlying_symbol AS symbol,
        SUM(ABS(COALESCE(quantity, 0)))   AS shares,
        SUM(COALESCE(market_value, 0))    AS broker_mv,
        MAX(snapshot_date)                AS snapshot_date
    FROM `ccwj-dbt.analytics.stg_current`
    WHERE instrument_type = 'Equity'
      AND quantity IS NOT NULL AND quantity != 0
      AND market_value IS NOT NULL AND market_value != 0
      {tenant_filter}
    GROUP BY 1
),
today_close AS (
    -- stg_daily_prices is keyed (account, symbol, date); the close is
    -- market-wide so collapse to one row per symbol to avoid fanning the
    -- holdings join when a symbol is held in multiple accounts.
    --
    -- Trading day is ET, NOT UTC: BigQuery CURRENT_DATE() defaults to UTC,
    -- which rolls over to "tomorrow" at 8pm ET (midnight UTC). A bare
    -- CURRENT_DATE() therefore looks for tomorrow's (nonexistent) close all
    -- evening ET and returns nothing — exactly when the after-hours section
    -- is most relevant. Anchor on the America/New_York calendar date.
    SELECT symbol, MAX(close_price) AS close_price
    FROM `ccwj-dbt.analytics.stg_daily_prices`
    WHERE date = CURRENT_DATE('America/New_York')
      AND close_price IS NOT NULL AND close_price > 0
    GROUP BY symbol
)
SELECT
    h.symbol,
    h.shares,
    h.snapshot_date,
    ROUND(SAFE_DIVIDE(h.broker_mv, h.shares), 4)            AS broker_mark,
    tc.close_price                                          AS today_close,
    ROUND(SAFE_DIVIDE(h.broker_mv, h.shares) - tc.close_price, 4) AS price_change,
    ROUND(SAFE_DIVIDE(
        SAFE_DIVIDE(h.broker_mv, h.shares) - tc.close_price,
        tc.close_price) * 100, 2)                           AS price_change_pct,
    ROUND(h.shares * (SAFE_DIVIDE(h.broker_mv, h.shares) - tc.close_price), 2) AS dollar_impact
FROM holdings h
JOIN today_close tc USING (symbol)
WHERE h.shares > 0
"""

# Projected next ex-dividend dates for holdings, inferred from yfinance
# historical cadence (stg_daily_prices.dividend > 0). Reads the last 6
# distinct ex-div dates per symbol, computes median spacing in days, and
# projects last_ex_div + median_spacing. This is a HEURISTIC — corporate
# actions can move ex-div dates — but quarterly issuers stay tight to
# pattern and the column is labeled "projected" in the UI.
#
# We need real future ex-div dates (yfinance Calendar) for full
# accuracy; that's a refresher-script TODO. In the meantime this gets
# JEPI / JEPQ / SCHD / VYM / DELL etc. surfaced for the day-before-ex
# dividend reminder.
UPCOMING_DIVIDENDS_QUERY = """
WITH holdings AS (
    SELECT DISTINCT UPPER(TRIM(underlying_symbol)) AS symbol
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE quantity IS NOT NULL AND quantity != 0
      AND instrument_type = 'Equity'
      {tenant_filter}
),
ex_divs AS (
    SELECT
        UPPER(TRIM(symbol)) AS symbol,
        date AS ex_div_date,
        dividend AS amount_per_share,
        ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(symbol)) ORDER BY date DESC) AS rn
    FROM `ccwj-dbt.analytics.stg_daily_prices`
    WHERE dividend IS NOT NULL AND dividend > 0
),
recent AS (
    SELECT
        symbol,
        ex_div_date,
        amount_per_share,
        LAG(ex_div_date) OVER (PARTITION BY symbol ORDER BY ex_div_date) AS prev_ex_div_date
    FROM ex_divs
    WHERE rn <= 6
),
spacings AS (
    SELECT symbol, DATE_DIFF(ex_div_date, prev_ex_div_date, DAY) AS spacing_days
    FROM recent
    WHERE prev_ex_div_date IS NOT NULL
),
cadence AS (
    SELECT
        symbol,
        APPROX_QUANTILES(spacing_days, 2)[OFFSET(1)] AS median_spacing_days
    FROM spacings
    GROUP BY symbol
),
last_event AS (
    SELECT symbol, ex_div_date AS last_ex_div_date,
           amount_per_share AS last_amount_per_share
    FROM ex_divs
    WHERE rn = 1
),
projected AS (
    SELECT
        le.symbol,
        le.last_ex_div_date,
        le.last_amount_per_share,
        c.median_spacing_days,
        DATE_ADD(le.last_ex_div_date,
                 INTERVAL COALESCE(c.median_spacing_days, 91) DAY) AS projected_next_ex_div_date
    FROM last_event le
    LEFT JOIN cadence c USING (symbol)
)
SELECT
    h.symbol,
    p.last_ex_div_date,
    p.last_amount_per_share,
    p.median_spacing_days,
    p.projected_next_ex_div_date,
    DATE_DIFF(p.projected_next_ex_div_date, CURRENT_DATE(), DAY) AS days_until_projected,
    m.sector,
    m.subsector,
    m.long_name
FROM holdings h
JOIN projected p USING (symbol)
LEFT JOIN `ccwj-dbt.analytics.stg_symbol_metadata` m USING (symbol)
WHERE p.projected_next_ex_div_date BETWEEN CURRENT_DATE()
                                      AND DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY p.projected_next_ex_div_date
"""

OPEN_POSITIONS_QUERY = """
WITH latest_prices AS (
    SELECT symbol, close_price
    FROM (
        SELECT symbol, close_price,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
        FROM `ccwj-dbt.analytics.stg_daily_prices`
        WHERE close_price IS NOT NULL AND close_price > 0
    )
    WHERE rn = 1
)
SELECT
    e.account,
    e.underlying_symbol AS symbol,
    e.instrument_type,
    e.trade_symbol,
    e.description,
    e.quantity,
    e.current_price,
    e.market_value,
    e.cost_basis,
    e.unrealized_pnl,
    e.unrealized_pnl_pct,
    e.option_expiry,
    e.option_strike,
    e.option_type,
    lp.close_price AS latest_stock_price
FROM `ccwj-dbt.analytics.int_enriched_current` e
LEFT JOIN latest_prices lp ON e.underlying_symbol = lp.symbol
WHERE e.quantity IS NOT NULL AND e.quantity != 0
  {tenant_filter}
ORDER BY e.underlying_symbol, e.instrument_type
"""

WEEKLY_STOCK_MOVEMENT_QUERY = """
WITH boundary AS (
    SELECT account, symbol,
        FIRST_VALUE(close_price) OVER (PARTITION BY account, symbol ORDER BY date) AS start_price,
        FIRST_VALUE(date)        OVER (PARTITION BY account, symbol ORDER BY date) AS start_date,
        FIRST_VALUE(close_price) OVER (PARTITION BY account, symbol ORDER BY date DESC) AS end_price,
        FIRST_VALUE(date)        OVER (PARTITION BY account, symbol ORDER BY date DESC) AS end_date
    FROM `ccwj-dbt.analytics.stg_daily_prices`
    WHERE date BETWEEN @start_date AND @end_date
      AND close_price IS NOT NULL AND close_price > 0
      {tenant_filter}
)
SELECT DISTINCT account, symbol, start_price, start_date, end_price, end_date
FROM boundary
"""

TRADING_DAYS_QUERY = """
SELECT
    COUNT(DISTINCT date) AS trading_days,
    MAX(date) AS last_trading_date
FROM `ccwj-dbt.analytics.stg_daily_prices`
WHERE date BETWEEN @start_date AND @end_date
  AND close_price IS NOT NULL
  AND close_price > 0
"""

# Daily Account Δ calendar: day-over-day change in BROKERAGE ACCOUNT VALUE.
#
# What this measures: "how much did my account total swing today" — the raw
# end-of-day broker-reported account value, differenced against the prior
# trading day. Not trade-event P&L. Not realized closures. The trader's
# net worth motion as the market sees it.
#
# Source: `mart_account_snapshots_enriched.delta_1d`.
#   account_value comes from snapshot_account_balances_daily account_total
#   rows (Schwab's reported "Account Total" at end-of-day) deduped to the
#   latest snapshot per (account, user_id, date).
#   delta_1d = account_value(d) - account_value(prior_trading_day)
#   The enriched mart restricts its series to weekdays (Mon-Fri), so the
#   "prior" date skips weekends — the first day a new account reports has
#   NULL delta, and Monday's delta is against the previous Friday's close
#   (never against Sat/Sun, which would always read $0).
#
# Multi-account scope: we SUM delta_1d across all accounts in the user's
# filter (mirrors mart_account_equity_daily which is per-account). For a
# single-account filter this collapses to the trivial pass-through.
#
# Caveats — surface but don't filter out (matches product intent):
#   1) DEPOSITS / WITHDRAWALS land in account_value, so a paycheck transfer
#      shows as a positive delta — that's literally "my account got bigger
#      today" from the trader's perspective. Surface, don't subtract.
#   2) JOURNAL TRANSFERS between two of the trader's own accounts net to
#      zero at the user level but each leg shows on its own account row.
#      Multi-account view nets correctly; single-account filter sees one leg.
#   3) Pre-snapshot history is blank. Account-balance snapshots only
#      accumulate going forward from when broker sync started capturing
#      end-of-day. New users or pre-sync history → empty cells.
#
# Pre-fix history:
#   - First implementation: `int_strategy_classification` realized closures
#     summed on close_date + `int_dividend_events`. Showed cells only on
#     closure / dividend days → calendar was mostly blank for an options
#     trader who opened Mon and closed Fri.
#   - Second iteration: `mart_daily_pnl` book-P&L delta (cumulative_options
#     + open_options_unrealized). Captured option MTM motion on still-open
#     contracts but missed equity daily moves entirely (no running
#     equity-held qty in the mart).
#   - Current: account-value delta — strictly more general than either. The
#     broker already integrates equity MTM, option MTM, dividends,
#     deposits, journal transfers into one number every day. We just
#     report the day-over-day change of that number.
DAILY_CALENDAR_QUERY = """
WITH daily AS (
    SELECT
        date,
        -- SUM across accounts so multi-account scopes net correctly.
        -- Single-account scope is a trivial pass-through.
        SUM(account_value)             AS account_value,
        SUM(COALESCE(delta_1d, 0))     AS delta_1d
    FROM `ccwj-dbt.analytics.mart_account_snapshots_enriched`
    WHERE date >= @start_date
      AND date <= @end_date
      {tenant_filter}
    GROUP BY date
)
SELECT
    date,
    account_value,
    ROUND(delta_1d, 2) AS daily_change
FROM daily
ORDER BY date
"""

# Trades opened this week for Mid-Week Check
# "Opens" = trade-backed position groups. Snapshot-only rows (stg_current with
# no stg_history yet) have num_trades = 0 and open_date = snapshot day — they
# must not count as new activity or Pace Check will list every open position.
OPENS_THIS_WEEK_QUERY = """
SELECT
    c.account, c.symbol, c.strategy, c.open_date
FROM `ccwj-dbt.analytics.int_strategy_classification` c
WHERE c.open_date >= @start_date
  AND c.open_date <= @end_date
  AND c.num_trades > 0
  {tenant_filter}
"""

# This week's P&L and counts by strategy (for "What works for you" section)
WEEKLY_STRATEGY_BREAKDOWN_QUERY = """
SELECT
  strategy,
  SUM(total_pnl) AS total_pnl,
  COUNT(*)       AS trades,
  SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) AS winners,
  SUM(CASE WHEN NOT is_winner THEN 1 ELSE 0 END) AS losers
FROM `ccwj-dbt.analytics.int_strategy_classification`
WHERE status = 'Closed'
  AND close_date >= @start_date
  AND close_date <= @end_date
  {tenant_filter}
GROUP BY strategy
ORDER BY total_pnl DESC
"""

PATTERNS_COMBINED_QUERY = """
WITH streak AS (
    SELECT streak_type, streak_length, week_pnl
    FROM `ccwj-dbt.analytics.mart_weekly_streaks`
    WHERE week_start = @week_start
      {tenant_filter}
    ORDER BY streak_length DESC
    LIMIT 1
),
loss_cluster AS (
    SELECT
        COUNTIF(is_post_loss) AS post_loss_trades,
        COUNTIF(is_post_loss AND outcome = 'Winner') AS post_loss_winners,
        COUNTIF(is_post_loss AND outcome = 'Loser') AS post_loss_losers,
        COUNT(*) AS total_trades,
        COUNTIF(outcome = 'Winner') AS total_winners
    FROM `ccwj-dbt.analytics.int_trade_sequence`
    WHERE 1=1 {tenant_filter}
),
loss_cluster_week AS (
    SELECT
        COUNTIF(is_post_loss) AS week_post_loss_count,
        COUNT(*) AS week_trades
    FROM `ccwj-dbt.analytics.int_trade_sequence`
    WHERE close_date >= @week_start
      AND close_date <= @week_end
      {tenant_filter}
),
dte_sensitivity AS (
    SELECT
        dte_bucket,
        SUM(num_trades) AS num_trades,
        SUM(CASE WHEN outcome = 'Winner' THEN num_trades ELSE 0 END) AS winners,
        SUM(CASE WHEN outcome = 'Loser' THEN num_trades ELSE 0 END) AS losers,
        SUM(total_pnl) AS total_pnl
    FROM `ccwj-dbt.analytics.mart_option_trades_by_kind`
    WHERE 1=1 {tenant_filter}
    GROUP BY dte_bucket
)
SELECT 'streak' AS _section, streak_type, CAST(streak_length AS STRING) AS val1, CAST(week_pnl AS STRING) AS val2, NULL AS val3, NULL AS val4, NULL AS val5 FROM streak
UNION ALL
SELECT 'loss_cluster', NULL, CAST(post_loss_trades AS STRING), CAST(post_loss_winners AS STRING), CAST(total_trades AS STRING), CAST(total_winners AS STRING), NULL FROM loss_cluster
UNION ALL
SELECT 'loss_cluster_week', NULL, CAST(week_post_loss_count AS STRING), NULL, NULL, NULL, NULL FROM loss_cluster_week
UNION ALL
SELECT 'dte', dte_bucket, CAST(num_trades AS STRING), CAST(winners AS STRING), CAST(losers AS STRING), CAST(total_pnl AS STRING), NULL FROM dte_sensitivity
"""

WEEKLY_EXIT_ANALYSIS_QUERY = """
SELECT
    trade_symbol, underlying_symbol, strategy, direction,
    close_date, actual_pnl, peak_unrealized_pnl, peak_date,
    days_held_past_peak, pnl_given_back, giveback_pct,
    pct_of_premium_captured, optimal_exit,
    snapshot_count, snapshot_density, data_reliable
FROM `ccwj-dbt.analytics.int_option_exit_analysis`
WHERE close_date >= @week_start
  AND close_date <= @week_end
  AND data_reliable = true
  {tenant_filter}
ORDER BY pnl_given_back DESC
"""

COACHING_SIGNALS_WEEKLY_QUERY = """
SELECT
    strategy,
    total_closed, reliable_contracts, pct_contracts_reliable,
    avg_giveback_pct, avg_days_held_past_peak,
    total_pnl_given_back, optimal_exit_rate,
    num_rolls, avg_dte_at_roll,
    rolls_after_losing_leg, pct_rolls_after_losing_leg,
    rolls_at_0_or_1_dte, pct_rolls_at_0_or_1_dte,
    rolls_with_spot_for_itm, pct_rolls_sold_short_itm_when_known
FROM `ccwj-dbt.analytics.mart_coaching_signals`
{where}
ORDER BY total_pnl_given_back DESC
"""


def _detect_patterns(client, tenant_filter, week_start, week_end):
    """Detect behavioral patterns from pre-computed dbt models.

    Returns a list of pattern dicts:
      { type, headline, detail, severity: positive|neutral|warning }

    Uses a single combined query instead of 3-4 separate round-trips.
    """
    patterns = []

    try:
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("week_start", "DATE", week_start),
            bigquery.ScalarQueryParameter("week_end", "DATE", week_end),
        ])
        combined_df = client.query(
            PATTERNS_COMBINED_QUERY.format(tenant_filter=tenant_filter),
            job_config=cfg,
        ).to_dataframe()

        if combined_df.empty:
            return patterns

        # ── Pattern 1: Week streak ──
        streak_rows = combined_df[combined_df["_section"] == "streak"]
        if not streak_rows.empty:
            row = streak_rows.iloc[0]
            slen = int(float(row["val1"]))
            stype = str(row["streak_type"] or "")
            if slen >= 2:
                if stype == "winning":
                    patterns.append({
                        "type": "week_streak",
                        "headline": f"{slen}-week winning streak",
                        "detail": f"This is your {slen}{'th' if slen > 3 else ['', 'st', 'nd', 'rd'][min(slen, 3)]} consecutive positive week. Momentum is real — but so is reversion.",
                        "severity": "positive",
                    })
                else:
                    patterns.append({
                        "type": "week_streak",
                        "headline": f"{slen}-week losing streak",
                        "detail": f"This is your {slen}{'th' if slen > 3 else ['', 'st', 'nd', 'rd'][min(slen, 3)]} consecutive negative week. Consider whether conditions changed or if execution is drifting.",
                        "severity": "warning",
                    })

        # ── Pattern 2: Loss clustering ──
        lc_rows = combined_df[combined_df["_section"] == "loss_cluster"]
        if not lc_rows.empty:
            row = lc_rows.iloc[0]
            post_loss_total = int(float(row.get("val1") or 0))
            post_loss_winners = int(float(row.get("val2") or 0))
            total_trades = int(float(row.get("val3") or 0))
            total_winners = int(float(row.get("val4") or 0))

            if post_loss_total >= 5 and total_trades >= 10:
                post_loss_wr = round(post_loss_winners / post_loss_total * 100, 0)
                overall_wr = round(total_winners / total_trades * 100, 0)
                wr_drop = overall_wr - post_loss_wr

                if wr_drop >= 10:
                    week_detail = ""
                    lc_week_rows = combined_df[combined_df["_section"] == "loss_cluster_week"]
                    if not lc_week_rows.empty:
                        pl_count = int(float(lc_week_rows.iloc[0].get("val1") or 0))
                        if pl_count > 0:
                            week_detail = f" This week, {pl_count} of your trades came right after a loss."

                    patterns.append({
                        "type": "loss_cluster",
                        "headline": "Loss clustering detected",
                        "detail": (
                            f"After a losing trade, your win rate drops to {post_loss_wr:.0f}% "
                            f"(vs {overall_wr:.0f}% overall). "
                            f"Trades placed right after a loss underperform.{week_detail}"
                        ),
                        "severity": "warning",
                    })

        # ── Pattern 3: DTE sensitivity ──
        dte_rows = combined_df[combined_df["_section"] == "dte"].copy()
        if not dte_rows.empty:
            dte_rows["num_trades"] = pd.to_numeric(dte_rows["val1"], errors="coerce").fillna(0)
            dte_rows["winners"] = pd.to_numeric(dte_rows["val2"], errors="coerce").fillna(0)

            total_option_trades = int(dte_rows["num_trades"].sum())
            total_option_winners = int(dte_rows["winners"].sum())
            overall_wr = round(total_option_winners / total_option_trades * 100, 0) if total_option_trades > 0 else None

            if overall_wr is not None:
                for _, row in dte_rows.iterrows():
                    bucket_trades = int(row["num_trades"])
                    bucket_winners = int(row["winners"])
                    if bucket_trades >= 5:
                        bucket_wr = round(bucket_winners / bucket_trades * 100, 0)
                        wr_gap = overall_wr - bucket_wr
                        if wr_gap >= 15:
                            bucket = str(row["streak_type"] or "")
                            patterns.append({
                                "type": "dte_sensitivity",
                                "headline": f"Weak spot: {bucket} trades",
                                "detail": (
                                    f"Your win rate on {bucket} trades is {bucket_wr:.0f}% "
                                    f"({wr_gap:.0f} points below your {overall_wr:.0f}% overall). "
                                    f"Based on {bucket_trades} trades."
                                ),
                                "severity": "warning",
                            })
                            break
                        elif wr_gap <= -15:
                            bucket = str(row["streak_type"] or "")
                            patterns.append({
                                "type": "dte_sensitivity",
                                "headline": f"Sweet spot: {bucket} trades",
                                "detail": (
                                    f"Your win rate on {bucket} trades is {bucket_wr:.0f}% "
                                    f"({abs(wr_gap):.0f} points above your {overall_wr:.0f}% overall). "
                                    f"This DTE range works for you."
                                ),
                                "severity": "positive",
                            })
                            break

    except Exception:
        pass

    return patterns


# ──────────────────────────────────────────────────────────────────
# "Since you last looked" — daily pull hook
# ──────────────────────────────────────────────────────────────────
#
# The page is Today-first. We diff the current world against a snapshot of
# the world at the user's previous visit (or yesterday if this is their first
# real visit). Output is ALWAYS in the user's accounts only — every query
# below is account-scoped at the SQL level (per BigQuery tenant-isolation
# rules) and any DataFrame is filtered before merge.

# Prior closes (one row per symbol) at the most recent close strictly BEFORE
# the cutoff date. Used to compute "since you last looked" stock moves.
PRIOR_CLOSE_QUERY = """
WITH ranked AS (
    SELECT symbol, date, close_price,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
    FROM `ccwj-dbt.analytics.stg_daily_prices`
    WHERE symbol IN UNNEST(@symbols)
      AND date <= @cutoff_date
      AND close_price IS NOT NULL AND close_price > 0
)
SELECT symbol, date AS prior_date, close_price AS prior_close
FROM ranked
WHERE rn = 1
"""

# Trades that closed since the prior visit (date-grain — within the user's
# account scope). Small list intended for the "since" strip.
TRADES_CLOSED_SINCE_QUERY = """
SELECT account, symbol, strategy, trade_symbol, close_date, total_pnl
FROM `ccwj-dbt.analytics.int_strategy_classification`
WHERE status = 'Closed'
  AND close_date >= @since_date
  AND close_date <= @today_date
  AND num_trades > 0
  {tenant_filter}
ORDER BY close_date DESC, ABS(total_pnl) DESC
LIMIT 5
"""

# Trades that opened since the prior visit.
TRADES_OPENED_SINCE_QUERY = """
SELECT account, symbol, strategy, open_date
FROM `ccwj-dbt.analytics.int_strategy_classification`
WHERE open_date >= @since_date
  AND open_date <= @today_date
  AND num_trades > 0
  {tenant_filter}
ORDER BY open_date DESC
LIMIT 10
"""


def _humanize_gap(gap):
    """Render a timedelta as 'N hours/days/weeks ago' for a friendly label."""
    if gap is None:
        return "your last visit"
    total_seconds = int(gap.total_seconds())
    if total_seconds < 60:
        return "moments ago"
    minutes = total_seconds // 60
    if minutes < 60:
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = hours // 24
    if days < 7:
        return f"{days} day{'s' if days != 1 else ''} ago"
    weeks = days // 7
    if weeks < 5:
        return f"{weeks} week{'s' if weeks != 1 else ''} ago"
    return "a while ago"


def _since_last_looked(client, tenant_filter, prev_visit_dt, today, today_strip,
                       expiring_options, user_tz, force_show=False):
    """
    Build the "Since you last looked" diff card.

    Strategy:
      • Anchor date = the calendar date of prev_visit_dt in the user's TZ
        (or today - 1 day if there's no prior visit yet — first real visit
        still gets a useful "since yesterday" view).
      • Stock moves: per open-position symbol, compare today's latest close
        vs the close on/before anchor_date (PRIOR_CLOSE_QUERY).
      • Trades closed / opened since anchor (account-scoped).
      • Newly-near expirations: options now ≤ 7 days out that were > 7 days
        out at anchor_date (cheap heuristic: expiry - anchor_date > 7).

    Returns a dict consumable by the template, or None if there's nothing
    interesting to show.

    Visibility gate (the user's intra-day reload was showing yesterday's
    diff over and over): the section only renders when at least one of
      • first visit ever (prev_visit_dt is None), or
      • full 24 hours have passed since prev_visit_dt, or
      • the user's local calendar date has rolled over (daily sync /
        new daily snapshots), or
      • the user just hit the page from an explicit sync/upload flow
        (`?from_sync=1` / `?from_upload=1`, surfaced as force_show)
    is true. Otherwise we return None up front and skip the BQ queries
    entirely.
    """
    try:
        if prev_visit_dt is not None:
            tz = ZoneInfo(user_tz) if user_tz else ZoneInfo("America/New_York")
            prev_local = prev_visit_dt.astimezone(tz)
            now_local = datetime.now(tz)
            anchor_date = prev_local.date()
            gap = now_local - prev_local
            time_ago = _humanize_gap(gap)
            is_first_visit = False

            # Gate: skip the section entirely when nothing meaningful has
            # changed since the user's last visit. Same calendar day in the
            # user's TZ + less than a full day elapsed + no explicit sync
            # signal = same content they already saw, so don't re-surface it.
            if not force_show:
                full_day_passed = gap >= timedelta(hours=24)
                crossed_calendar_day = now_local.date() > anchor_date
                if not (full_day_passed or crossed_calendar_day):
                    return None
        else:
            anchor_date = today - timedelta(days=1)
            time_ago = "yesterday"
            is_first_visit = True

        # Don't diff against today itself — nothing changes.
        if anchor_date >= today:
            anchor_date = today - timedelta(days=1)

        symbols = sorted({
            (s.get("symbol") or "").upper()
            for s in (today_strip or [])
            if s.get("symbol")
        })

        moves = []
        if symbols:
            try:
                cfg = bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ArrayQueryParameter("symbols", "STRING", symbols),
                    bigquery.ScalarQueryParameter("cutoff_date", "DATE", anchor_date),
                ])
                df = client.query(PRIOR_CLOSE_QUERY, job_config=cfg).to_dataframe()
                # Map: symbol → prior_close  (PRIOR_CLOSE_QUERY is symbol-only,
                # not user-data; account scoping doesn't apply.)
                prior_map = {}
                for _, row in df.iterrows():
                    sym = str(row["symbol"]).upper()
                    prior_map[sym] = float(row["prior_close"])

                for s in today_strip:
                    sym = (s.get("symbol") or "").upper()
                    cur_price = s.get("price")
                    prior = prior_map.get(sym)
                    if not sym or cur_price is None or prior is None or prior <= 0:
                        continue
                    delta = float(cur_price) - prior
                    pct = (delta / prior) * 100.0
                    if abs(pct) < 0.5:
                        continue  # too small to bother surfacing
                    moves.append({
                        "symbol": sym,
                        "prior_price": round(prior, 2),
                        "current_price": round(float(cur_price), 2),
                        "delta": round(delta, 2),
                        "delta_pct": round(pct, 2),
                        "positive": delta >= 0,
                    })
                moves.sort(key=lambda m: abs(m["delta_pct"]), reverse=True)
                moves = moves[:5]
            except Exception as exc:
                if app.debug:
                    app.logger.warning("Since-last-looked prior closes failed: %s", exc)

        # Trades opened / closed since anchor — strictly account-scoped.
        opened = []
        closed = []
        try:
            cfg = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("since_date", "DATE", anchor_date),
                bigquery.ScalarQueryParameter("today_date", "DATE", today),
            ])
            closed_df = client.query(
                TRADES_CLOSED_SINCE_QUERY.format(tenant_filter=tenant_filter),
                job_config=cfg,
            ).to_dataframe()
            for _, row in closed_df.iterrows():
                pnl = row.get("total_pnl")
                pnl_v = float(pnl) if pnl is not None else None
                cd = row.get("close_date")
                cd_s = cd.isoformat() if hasattr(cd, "isoformat") else str(cd)[:10]
                closed.append({
                    "symbol": str(row.get("symbol") or ""),
                    "strategy": str(row.get("strategy") or ""),
                    "trade_symbol": str(row.get("trade_symbol") or ""),
                    "close_date": cd_s,
                    "total_pnl": pnl_v,
                    "positive": (pnl_v is not None and pnl_v >= 0),
                })

            opened_df = client.query(
                TRADES_OPENED_SINCE_QUERY.format(tenant_filter=tenant_filter),
                job_config=cfg,
            ).to_dataframe()
            for _, row in opened_df.iterrows():
                od = row.get("open_date")
                od_s = od.isoformat() if hasattr(od, "isoformat") else str(od)[:10]
                opened.append({
                    "symbol": str(row.get("symbol") or ""),
                    "strategy": str(row.get("strategy") or ""),
                    "open_date": od_s,
                })
        except Exception as exc:
            if app.debug:
                app.logger.warning("Since-last-looked trades query failed: %s", exc)

        # Newly-near expirations: now in the 7-day window AND were further out
        # at anchor (expiry - anchor > 7 days). Uses already-computed list.
        newly_near_expiry = []
        for opt in (expiring_options or []):
            try:
                exp_str = opt.get("expiry") or ""
                if not exp_str:
                    continue
                exp_d = date.fromisoformat(str(exp_str)[:10])
            except Exception:
                continue
            days_now = (exp_d - today).days
            days_at_anchor = (exp_d - anchor_date).days
            if 0 <= days_now <= 7 and days_at_anchor > 7:
                newly_near_expiry.append({
                    "symbol": opt.get("symbol"),
                    "trade_symbol": opt.get("trade_symbol"),
                    "option_type": opt.get("option_type"),
                    "strike": opt.get("strike"),
                    "expiry": exp_str,
                    "days_to_exp": days_now,
                    "itm": opt.get("itm"),
                })

        # Newly-ITM options: ITM now AND would have been OTM/ATM at anchor
        # (cheap proxy: stock_price moved across strike since anchor based on
        # the same prior_map we built for today_strip, falling back to
        # stock_price on the option row if symbol isn't in the strip).
        prior_map_for_opts = {m["symbol"]: m["prior_price"] for m in moves}
        newly_itm = []
        for opt in (expiring_options or []):
            sym = (opt.get("symbol") or "").upper()
            strike = opt.get("strike") or 0
            opt_type = opt.get("option_type") or ""
            cur_stock = opt.get("stock_price") or 0
            if not opt.get("itm") or not strike or not cur_stock:
                continue
            prior_stock = prior_map_for_opts.get(sym)
            if prior_stock is None:
                continue
            was_itm_then = (
                (opt_type == "Call" and prior_stock >= strike) or
                (opt_type == "Put" and prior_stock <= strike)
            )
            if not was_itm_then:
                newly_itm.append({
                    "symbol": sym,
                    "trade_symbol": opt.get("trade_symbol"),
                    "option_type": opt_type,
                    "strike": strike,
                    "expiry": opt.get("expiry"),
                    "days_to_exp": opt.get("days_to_exp"),
                    "prior_stock": prior_stock,
                    "current_stock": cur_stock,
                })

        has_anything = bool(moves or opened or closed or newly_near_expiry or newly_itm)
        if not has_anything:
            return None

        return {
            "time_ago": time_ago,
            "anchor_date": anchor_date.isoformat(),
            "is_first_visit": is_first_visit,
            "moves": moves,
            "opened_trades": opened,
            "closed_trades": closed,
            "newly_near_expiry": newly_near_expiry,
            "newly_itm": newly_itm,
        }
    except Exception as exc:
        if app.debug:
            app.logger.warning("_since_last_looked failed: %s", exc)
        return None


def _build_narrative(mode, review, prev_review, behavior_mirror, market,
                     today, week_start, trading_days=0, market_session=None):
    """Generate a dynamic hero headline + subtitle from actual data."""
    review = review or {}
    bm = behavior_mirror or {}

    total_pnl = float(review.get("total_pnl", 0) or 0)
    trades_closed = int(review.get("trades_closed", 0) or 0)
    num_winners = int(review.get("num_winners", 0) or 0)

    bm_has_baseline = bm.get("has_baseline", False)
    pnl_baseline = bm.get("pnl", {}).get("baseline")

    if mode == "friday":
        if trades_closed == 0:
            return {
                "headline": "No closed trades this week.",
                "subtitle": "Your open positions are still in play. Come back when something closes.",
            }
        sign = "+" if total_pnl >= 0 else ""
        headline = (
            f"{trades_closed} trade{'s' if trades_closed != 1 else ''} closed \u00b7 "
            f"{sign}${abs(total_pnl):,.0f} realized"
        )
        parts = []
        if num_winners == trades_closed:
            parts.append("Every closed trade finished as a winner.")
        elif num_winners == 0:
            parts.append("No winners in closed trades — all positions went against you.")
        else:
            losers = trades_closed - num_winners
            parts.append(
                f"{num_winners} winner{'s' if num_winners != 1 else ''}, "
                f"{losers} loser{'s' if losers != 1 else ''}."
            )
        if bm_has_baseline and pnl_baseline is not None:
            diff = total_pnl - pnl_baseline
            if diff > 200:
                parts.append(f"Better than your ${abs(pnl_baseline):,.0f}/week average.")
            elif diff < -200:
                parts.append(f"Below your ${abs(pnl_baseline):,.0f}/week average.")
            else:
                parts.append("Right in line with your recent average.")
        if market:
            spy = market.get("spy_week_pct")
            if spy is not None:
                if total_pnl > 0 and spy < -1:
                    parts.append(f"SPY was down {abs(spy):.1f}% — you went against the market.")
                elif total_pnl < 0 and spy > 1:
                    parts.append(f"SPY was up {spy:.1f}% — a tough week relative to conditions.")

        # Build richer story sections for Friday narrative
        story_parts = {}

        # Opening: frame the week
        opening_parts = []
        if trades_closed >= 5:
            opening_parts.append(f"An active week with {trades_closed} closed trades.")
        elif trades_closed <= 2:
            opening_parts.append(f"A quiet week — just {trades_closed} trade{'s' if trades_closed != 1 else ''} closed.")
        else:
            opening_parts.append(f"A typical week with {trades_closed} closed trades.")
        if trading_days and trading_days < 5:
            opening_parts.append(f"({trading_days} trading day{'s' if trading_days != 1 else ''} this week.)")
        if market:
            spy_pct = market.get("spy_week_pct")
            qqq_pct = market.get("qqq_week_pct")
            if spy_pct is not None:
                direction = "up" if spy_pct >= 0 else "down"
                qqq_part = ""
                if qqq_pct is not None:
                    qqq_dir = "up" if qqq_pct >= 0 else "down"
                    qqq_part = f" and QQQ {qqq_dir} {abs(qqq_pct):.1f}%"
                opening_parts.append(f"SPY was {direction} {abs(spy_pct):.1f}%{qqq_part}.")
        story_parts["opening"] = " ".join(opening_parts)

        # Middle: behavior context
        middle_parts = []
        if bm_has_baseline:
            vol = bm.get("volume", {})
            vol_base = float(vol.get("baseline") or 0)
            vol_val = float(vol.get("value") or 0)
            if vol_base > 0:
                ratio = vol_val / vol_base
                if ratio >= 1.5:
                    middle_parts.append(f"You traded {ratio:.1f}x your normal volume.")
                elif ratio <= 0.6:
                    middle_parts.append("You were more selective than usual.")

            wr_data = bm.get("win_rate", {})
            wr_val = wr_data.get("value")
            wr_base = wr_data.get("baseline")
            if wr_val is not None and wr_base is not None:
                wr_diff = wr_val - wr_base
                if wr_diff >= 10:
                    middle_parts.append(f"Win rate of {wr_val:.0f}% — {wr_diff:.0f} points above your baseline.")
                elif wr_diff <= -10:
                    middle_parts.append(f"Win rate of {wr_val:.0f}% — {abs(wr_diff):.0f} points below your baseline.")
        story_parts["middle"] = " ".join(middle_parts) if middle_parts else None

        return {
            "headline": headline,
            "subtitle": " ".join(parts),
            "story": story_parts,
        }

    elif mode == "monday":
        prev = prev_review or {}
        prev_pnl = float(prev.get("total_pnl", 0) or 0)
        prev_trades = int(prev.get("trades_closed", 0) or 0)
        if prev_trades > 0:
            sign = "+" if prev_pnl >= 0 else ""
            headline = (
                f"Last week: {prev_trades} trade{'s' if prev_trades != 1 else ''} \u00b7 "
                f"{sign}${abs(prev_pnl):,.0f}"
            )
            subtitle = "Before you trade anything this week, check what you're carrying and whether last week's behavior is worth repeating."
        else:
            headline = "New week, clean slate."
            subtitle = "Set your intention before the market sets it for you."
        return {"headline": headline, "subtitle": subtitle}

    elif mode == "midweek":
        # "Today" framing — forward-looking, not a recap. The weekly stats are
        # context underneath; the headline answers "what's live right now".
        try:
            day_name = today.strftime("%A")
        except Exception:
            day_name = "Today"
        if trades_closed > 0:
            sign = "+" if total_pnl >= 0 else ""
            headline = (
                f"{day_name} \u00b7 {trades_closed} closed this week \u00b7 "
                f"{sign}${abs(total_pnl):,.0f} so far"
            )
        else:
            headline = f"{day_name} \u00b7 Nothing closed this week yet"
        prefix = ""
        if market_session:
            st = market_session.get("state")
            if st == "open":
                prefix = "Markets are open. "
            elif st == "weekend":
                prefix = "Markets are closed (weekend). "
            elif st == "pre_market":
                prefix = "Before the U.S. open. "
            elif st == "after_hours":
                prefix = "After the U.S. close. "
        subtitle = prefix + "Here's what's live, what just changed, and what's coming up."
        return {
            "headline": headline,
            "subtitle": subtitle,
        }

    return {"headline": "Weekly Review", "subtitle": ""}


def _key_observation(review, behavior_mirror, strategy_breakdown):
    """Return the single most notable behavioral signal for the week."""
    review = review or {}
    bm = behavior_mirror or {}

    trades_closed = int(review.get("trades_closed", 0) or 0)
    num_winners = int(review.get("num_winners", 0) or 0)
    total_pnl = float(review.get("total_pnl", 0) or 0)

    if trades_closed == 0:
        return None

    vol = bm.get("volume", {})
    baseline_vol = float(vol.get("baseline") or 0)
    actual_vol = float(vol.get("value") or 0)
    if baseline_vol > 0 and actual_vol > 0:
        ratio = actual_vol / baseline_vol
        if ratio >= 2.0:
            return {
                "type": "warning",
                "icon": "⚡",
                "text": (
                    f"You traded {ratio:.1f}\u00d7 your normal volume "
                    f"({int(actual_vol)} trades vs your {baseline_vol:.1f}/week average). "
                    "Higher activity doesn't always mean better results — worth checking."
                ),
            }
        if ratio <= 0.4:
            return {
                "type": "neutral",
                "icon": "\u2014",
                "text": (
                    f"Quieter than usual — {int(actual_vol)} trade{'s' if actual_vol != 1 else ''} "
                    f"vs your {baseline_vol:.1f}/week average. Patient, or cautious?"
                ),
            }

    wr = bm.get("win_rate", {})
    wr_val = wr.get("value")
    wr_base = wr.get("baseline")
    wr_diff = wr.get("diff")
    if wr_val is not None and wr_base is not None and wr_diff is not None:
        if wr_diff >= 20:
            return {
                "type": "positive",
                "icon": "\u2191",
                "text": (
                    f"Win rate: {wr_val:.0f}% \u2014 {wr_diff:.0f} points above your "
                    f"{wr_base:.0f}% baseline. You were selective, and it paid off."
                ),
            }
        if wr_diff <= -20:
            return {
                "type": "negative",
                "icon": "\u2193",
                "text": (
                    f"Win rate: {wr_val:.0f}% \u2014 {abs(wr_diff):.0f} points below your "
                    f"{wr_base:.0f}% average. More losers than usual. Worth reviewing the pattern."
                ),
            }

    if strategy_breakdown and len(strategy_breakdown) >= 2:
        total_abs = sum(abs(s.get("total_pnl", 0)) for s in strategy_breakdown)
        if total_abs > 100:
            top = max(strategy_breakdown, key=lambda s: abs(s.get("total_pnl", 0)))
            top_share = abs(top.get("total_pnl", 0)) / total_abs
            if top_share > 0.80:
                direction = "profit" if top.get("total_pnl", 0) >= 0 else "loss"
                return {
                    "type": "neutral",
                    "icon": "\u2192",
                    "text": (
                        f"This week\u2019s {direction} was {top_share:.0%} driven by one strategy: "
                        f"{top['strategy']}. Everything else barely moved the needle."
                    ),
                }

    if trades_closed >= 2 and num_winners == trades_closed:
        return {
            "type": "positive",
            "icon": "\u2713",
            "text": f"Swept the week \u2014 all {trades_closed} closed trades were winners. Note what you did differently.",
        }

    pnl_data = bm.get("pnl", {})
    pnl_diff = pnl_data.get("diff")
    pnl_baseline = pnl_data.get("baseline")
    if pnl_diff is not None and pnl_baseline and abs(pnl_diff) > max(200, abs(pnl_baseline) * 0.4):
        if pnl_diff > 0:
            return {
                "type": "positive",
                "icon": "\u2191",
                "text": f"${pnl_diff:,.0f} above your typical week. Solid execution relative to your own baseline.",
            }
        return {
            "type": "negative",
            "icon": "\u2193",
            "text": f"${abs(pnl_diff):,.0f} below your typical week. What was different this week?",
        }

    return None


def _today_pulse(today_snapshots_by_account):
    """Distill today's account movement into a single number."""
    if not today_snapshots_by_account:
        return None
    total_delta = 0.0
    has_data = False
    date_label = None
    for snap in today_snapshots_by_account:
        day_comp = snap.get("comparisons", {}).get("day", {})
        if day_comp.get("has_data") and day_comp.get("delta") is not None:
            total_delta += float(day_comp["delta"])
            has_data = True
            if date_label is None and snap.get("today_date"):
                date_label = str(snap["today_date"])
    if not has_data:
        return None
    return {"delta": round(total_delta, 0), "positive": total_delta >= 0, "date": date_label}


def _today_totals(today_snapshots_by_account):
    """Aggregate per-account snapshot rows into one consolidated "All Accounts" row.

    Mirrors the shape of a single entry in today_snapshots_by_account so the
    template can render it with the same tile component.

    Percentages are computed off the *contributing* accounts' base value (today
    minus delta), not off the full multi-account today total — otherwise an
    account missing a 1-month baseline would silently dilute the % for the
    other accounts.
    """
    if not today_snapshots_by_account or len(today_snapshots_by_account) < 2:
        return None

    total_today = 0.0
    today_seen = False
    latest_date = None
    accounts_with_value = 0
    for snap in today_snapshots_by_account:
        tv = snap.get("today_value")
        if tv is not None:
            total_today += float(tv)
            today_seen = True
            accounts_with_value += 1
            td = snap.get("today_date")
            if td and (latest_date is None or str(td) > str(latest_date)):
                latest_date = td

    if not today_seen:
        return None

    def _agg_period(key):
        sum_delta = 0.0
        sum_base = 0.0
        any_data = False
        for snap in today_snapshots_by_account:
            comp = (snap.get("comparisons") or {}).get(key) or {}
            if not comp.get("has_data"):
                continue
            d = comp.get("delta")
            tv = snap.get("today_value")
            if d is None or tv is None:
                continue
            sum_delta += float(d)
            sum_base += float(tv) - float(d)
            any_data = True
        if not any_data:
            return {"label": None, "base_date": None, "delta": None, "delta_pct": None, "has_data": False}
        pct = round(sum_delta / sum_base * 100, 2) if sum_base > 0 else None
        return {
            "label": None,
            "base_date": None,
            "delta": round(sum_delta, 2),
            "delta_pct": pct,
            "has_data": True,
        }

    def _agg_week_start():
        sum_delta = 0.0
        sum_base = 0.0
        any_data = False
        base_date = None
        for snap in today_snapshots_by_account:
            sw = snap.get("vs_week_start") or {}
            if not sw.get("has_data"):
                continue
            d = sw.get("delta")
            tv = snap.get("today_value")
            if d is None or tv is None:
                continue
            sum_delta += float(d)
            sum_base += float(tv) - float(d)
            any_data = True
            if base_date is None:
                base_date = sw.get("base_date")
        if not any_data:
            return {"delta": None, "delta_pct": None, "has_data": False, "base_date": base_date}
        pct = round(sum_delta / sum_base * 100, 2) if sum_base > 0 else None
        return {
            "delta": round(sum_delta, 2),
            "delta_pct": pct,
            "has_data": True,
            "base_date": base_date,
        }

    return {
        "today_value": round(total_today, 2),
        "today_date": latest_date,
        "accounts_count": accounts_with_value,
        "comparisons": {
            "day": _agg_period("day"),
            "week": _agg_period("week"),
            "month": _agg_period("month"),
        },
        "vs_week_start": _agg_week_start(),
    }


def _iso_week_start(d):
    """Return Monday of the ISO week containing d."""
    return d - timedelta(days=d.weekday())


def _date_in_user_tz(tz_name: str) -> date:
    """Today’s calendar date in the user’s profile timezone (defaults to New York)."""
    raw = (tz_name or "").strip() or "America/New_York"
    try:
        z = ZoneInfo(raw)
    except Exception:
        z = ZoneInfo("America/New_York")
    return datetime.now(z).date()


def _us_market_session():
    """
    U.S. cash equity regular session, America/New_York. Mon–Fri 9:30–16:00.
    No NYSE holiday calendar in this pass; holidays may still show 'pre_market' before open.
    """
    et = ZoneInfo("America/New_York")
    now = datetime.now(et)
    w = now.weekday()
    if w >= 5:
        return {
            "state": "weekend",
            "label": "U.S. markets closed (weekend) · times in ET",
            "badge": "weekend",
        }
    t = now.time()
    o, c = time(9, 30), time(16, 0)
    time_h = now.strftime("%a %I:%M %p %Z")
    if t < o:
        return {
            "state": "pre_market",
            "label": f"Before U.S. open (9:30 ET) · {time_h}",
            "badge": "pre_market",
        }
    if t > c:
        return {
            "state": "after_hours",
            "label": f"After U.S. close (4:00 ET) · {time_h}",
            "badge": "after_hours",
        }
    return {
        "state": "open",
        "label": f"U.S. regular session (9:30–4:00 ET) · {time_h}",
        "badge": "open",
    }


def _auto_mode(today: date) -> str:
    """Auto-detect mode from day of week in the user’s local calendar (profile TZ)."""
    dow = today.weekday()
    if dow == 0:
        return "monday"
    if dow >= 4:
        return "friday"
    return "midweek"


_MODE_LABELS = {
    "friday": "Friday Review",
    "monday": "Monday Check",
    "midweek": "Mid-Week",
}


def _build_behavior_sentence(review, behavior_mirror, mode):
    """One-line behavior summary for the dual hero. Process-first, no money.

    Falls back gracefully when there is no baseline yet.
    """
    review = review or {}
    bm = behavior_mirror or {}
    trades_closed = int(review.get("trades_closed", 0) or 0)

    if mode == "midweek" and trades_closed == 0:
        return "Mid-week check-in. No closed trades yet — your week is still being written."
    if mode == "monday" and trades_closed == 0:
        return "New week, clean slate. Look at what you carried in before you trade."
    if trades_closed == 0:
        return "Nothing closed this week — your open positions are still in play."

    # Volume baseline shapes the strongest signal: are you trading more / less than usual?
    has_baseline = bool(bm.get("has_baseline"))
    if has_baseline:
        vol = bm.get("volume", {})
        vol_val = float(vol.get("value") or 0)
        vol_base = float(vol.get("baseline") or 0)
        wr = bm.get("win_rate", {})
        wr_val = wr.get("value")
        wr_base = wr.get("baseline")
        wr_diff = wr.get("diff")

        # Volume deviation comes first (it changes the shape of the week).
        if vol_base > 0 and vol_val > 0:
            ratio = vol_val / vol_base
            if ratio >= 1.6:
                return (
                    f"More active than usual — {int(vol_val)} closes vs your "
                    f"typical {vol_base:.1f}/week."
                )
            if ratio <= 0.5:
                return (
                    f"More selective than usual — {int(vol_val)} closes vs your "
                    f"typical {vol_base:.1f}/week."
                )

        # Win-rate deviation second.
        if wr_val is not None and wr_base is not None and wr_diff is not None:
            if wr_diff >= 15:
                return f"Sharper than usual — {wr_val:.0f}% wins vs your {wr_base:.0f}% baseline."
            if wr_diff <= -15:
                return f"Tougher week — {wr_val:.0f}% wins vs your {wr_base:.0f}% baseline."

        return "You traded like you usually do this week."

    # No baseline yet (early account / sparse history).
    if trades_closed == 1:
        return "One trade closed — building the baseline you'll be compared to."
    return f"{trades_closed} trades closed — building the baseline you'll be compared to."


def _neutral_market_line(market):
    """Neutral one-liner about market context. No 'beating'/'trailing' judgment."""
    if not market:
        return None
    spy = market.get("spy_week_pct")
    qqq = market.get("qqq_week_pct")
    parts = []
    if spy is not None:
        parts.append(f"SPY {'+' if spy >= 0 else ''}{spy:.1f}%")
    if qqq is not None:
        parts.append(f"QQQ {'+' if qqq >= 0 else ''}{qqq:.1f}%")
    if not parts:
        return None
    return f"Market context this week: {' · '.join(parts)}."


def _build_week_diary(*, week_start, today, trades, daily_changes, expiring_options):
    """Build a Mon→Fri diary of activity for the current week.

    A "trading mirror" should read like a diary, not a dashboard. One row per
    weekday with a one-line natural-language summary plus structured event
    chips so the template can render expandable details if needed.

    Args:
        week_start: date — Monday of the week being reviewed.
        today: date — user-local today.
        trades: list of dicts (the same `trades_this_week` shape we already build).
        daily_changes: dict[date → float] — account value daily change from the
            calendar query, used as a per-day P&L glance.
        expiring_options: list of dicts — used to surface weekday expiries.

    Returns:
        list[dict] of length 5 (Mon..Fri). Weekend is skipped — markets are closed.
    """
    diary = []
    # Bucket trades by their relevant date for each weekday.
    opens_by_date = {}
    closes_by_date = {}
    for t in trades or []:
        od = t.get("open_date") or ""
        cd = t.get("close_date") or ""
        try:
            if od:
                d = date.fromisoformat(od[:10])
                if week_start <= d <= week_start + timedelta(days=6):
                    opens_by_date.setdefault(d, []).append(t)
        except (TypeError, ValueError):
            pass
        try:
            if cd and cd != od:
                d = date.fromisoformat(cd[:10])
                if week_start <= d <= week_start + timedelta(days=6):
                    closes_by_date.setdefault(d, []).append(t)
        except (TypeError, ValueError):
            pass

    # Bucket expirations by their expiry date.
    exps_by_date = {}
    for e in expiring_options or []:
        try:
            d = date.fromisoformat((e.get("expiry") or "")[:10])
            if week_start <= d <= week_start + timedelta(days=6):
                exps_by_date.setdefault(d, []).append(e)
        except (TypeError, ValueError):
            pass

    for i in range(5):
        d = week_start + timedelta(days=i)
        opens = opens_by_date.get(d, [])
        closes = closes_by_date.get(d, [])
        exps = exps_by_date.get(d, [])

        # Build natural-language summary.
        bits = []
        if closes:
            wins = sum(1 for t in closes if (t.get("current_pnl") or 0) > 0)
            losses = sum(1 for t in closes if (t.get("current_pnl") or 0) < 0)
            if len(closes) == 1:
                t = closes[0]
                pnl = float(t.get("current_pnl") or 0)
                sign = "+" if pnl >= 0 else "-"
                bits.append(
                    f"Closed {t.get('strategy') or 'trade'} on "
                    f"{t.get('symbol') or ''} ({sign}${abs(pnl):,.0f})"
                )
            else:
                if wins == len(closes):
                    bits.append(f"Closed {len(closes)} trades (all winners)")
                elif losses == len(closes):
                    bits.append(f"Closed {len(closes)} trades (all losers)")
                else:
                    bits.append(f"Closed {len(closes)} trades ({wins}W / {losses}L)")
        if opens:
            if len(opens) == 1:
                o = opens[0]
                bits.append(f"Opened {o.get('strategy') or 'trade'} on {o.get('symbol') or ''}")
            else:
                # Group by strategy for compactness.
                strats = {}
                for o in opens:
                    s = o.get("strategy") or "trade"
                    strats[s] = strats.get(s, 0) + 1
                strat_bits = ", ".join(
                    f"{n} {s}" + ("s" if n != 1 else "") for s, n in strats.items()
                )
                bits.append(f"Opened {strat_bits}")
        if exps and not closes and not opens:
            sym_count = len(exps)
            bits.append(f"{sym_count} option{'s' if sym_count != 1 else ''} expiring")

        is_today = (d == today)
        is_future = (d > today)
        if not bits:
            if is_future:
                summary = ""
            elif is_today:
                summary = "Today — nothing closed yet."
            else:
                summary = "Quiet day."
        else:
            summary = " · ".join(bits)

        diary.append({
            "date": d,
            "weekday": d.weekday(),
            "label": d.strftime("%a"),
            "long_label": d.strftime("%a %b %-d"),
            "is_today": is_today,
            "is_future": is_future,
            "daily_change": daily_changes.get(d) if daily_changes else None,
            "num_opens": len(opens),
            "num_closes": len(closes),
            "num_expirations": len(exps),
            "summary": summary,
        })
    return diary


def _build_noticed(key_observation, patterns, coaching_take):
    """Merge Key Observation + Patterns + Coach's Take into a single 'What we
    noticed' panel of up to 3 cards. Process-first ordering.

    Returns: list of dicts {severity, headline, detail, icon?}
    """
    cards = []

    # 1) Key observation gets the top slot when present (it's the strongest single signal).
    if key_observation:
        cards.append({
            "severity": key_observation.get("type") or "neutral",
            "icon": key_observation.get("icon") or "",
            "headline": "This week",
            "detail": key_observation.get("text") or "",
        })

    # 2) Patterns from history (streaks, post-loss clusters, DTE sensitivity).
    for p in patterns or []:
        cards.append({
            "severity": p.get("severity") or "neutral",
            "icon": "",
            "headline": p.get("headline") or "",
            "detail": p.get("detail") or "",
        })

    # 3) Coach's Take — exit timing — only if there's a real story.
    if coaching_take and coaching_take.get("coaching_signals"):
        for sig in coaching_take["coaching_signals"][:1]:
            cards.append({
                "severity": "warning",
                "icon": "",
                "headline": "Exit timing",
                "detail": sig,
            })
    elif coaching_take and coaching_take.get("total_given_back", 0) > 50:
        cards.append({
            "severity": "warning",
            "icon": "",
            "headline": "Exit timing",
            "detail": (
                f"Left ${coaching_take['total_given_back']:,.0f} on the table this week "
                f"by holding past peak profit."
            ),
        })

    # Cap at 3. The page should not become a wall of cards.
    return cards[:3]


# Calendar window:
#   - We fetch and render up to DAILY_CALENDAR_WEEKS weeks (~3 months) so the
#     full extra history is right there in the DOM.
#   - We *display* DAILY_CALENDAR_DEFAULT_WEEKS by default. The remainder is
#     marked with ``is_extra=True`` and hidden behind a CSS-toggle "Show
#     earlier weeks" button rendered by the template. No extra round-trip
#     when the user expands.
#
# The data source (closed-trade P&L + dividends) extends as far back as the
# user has trade history; bumping these constants is the only knob.
DAILY_CALENDAR_WEEKS = 12
DAILY_CALENDAR_DEFAULT_WEEKS = 4


def _build_calendar_grid(
    daily_changes,
    today,
    weeks_back=DAILY_CALENDAR_WEEKS,
    default_weeks=DAILY_CALENDAR_DEFAULT_WEEKS,
):
    """Rolling N-weeks-ending-today calendar (N ISO weeks × Mon-Fri = 5·N cells).

    Always populated regardless of where we are in the calendar month — a much
    better default than "current month so far" (which was empty on the 1st).

    Args:
        daily_changes: dict[date, float] of P&L per date.
        today:        anchor date; the current week is the bottom row.
        weeks_back:   total rows fetched (default ~3 months).
        default_weeks: how many of the most-recent rows to mark as visible by
                      default; rows older than this carry ``is_extra=True``
                      so the template can hide-and-toggle them. The remainder
                      is still in the rendered DOM, just behind a button.

    Earlier versions hard-coded 4 weeks for both fetch and display; we
    extended fetch to ~3 months once the data source switched from sparse
    account snapshots to closed-trade P&L + dividends, then split fetch
    from display so the calendar still leads with a tight 4-week view.
    """
    weeks_back = max(1, int(weeks_back))
    default_weeks = max(1, min(int(default_weeks), weeks_back))
    extra_weeks = weeks_back - default_weeks
    # Anchor on the Monday of the current week and walk back (weeks_back - 1) weeks.
    week_mon = today - timedelta(days=today.weekday())
    rows = []
    for idx, w in enumerate(range(weeks_back - 1, -1, -1)):
        row_start = week_mon - timedelta(days=w * 7)
        row_cells = []
        for i in range(5):
            d = row_start + timedelta(days=i)
            change = daily_changes.get(d) if daily_changes else None
            row_cells.append({
                "date": d,
                "day": d.day,
                "month_short": d.strftime("%b"),
                "is_today": d == today,
                "is_future": d > today,
                "weekday": d.weekday(),
                "daily_change": change,
                "has_data": change is not None,
            })
        rows.append({
            "week_label": row_start.strftime("%b %-d"),
            "week_start": row_start,
            "cells": row_cells,
            # First `extra_weeks` rows are older history hidden by default.
            "is_extra": idx < extra_weeks,
        })
    return rows


def _aggregate_weekly_rows(rows):
    """Aggregate multiple account rows from mart_weekly_summary into one summary.

    Dividends-as-first-class:
        total_pnl       — closed-trade P&L (preserved as-is for behavioral baselines).
        dividends_amount — cash dividends received during the week.
        total_return    — total_pnl + dividends_amount, the "what did this
                          week make me" headline number.
    """
    if not rows:
        return None
    total_pnl = sum(r.get("total_pnl", 0) for r in rows)
    dividends_amount = sum(r.get("dividends_amount", 0) for r in rows)
    summary = {
        "trades_closed": sum(r.get("trades_closed", 0) for r in rows),
        "total_pnl": total_pnl,
        "dividends_amount": dividends_amount,
        "total_return": total_pnl + dividends_amount,
        "num_winners": sum(r.get("num_winners", 0) for r in rows),
        "num_losers": sum(r.get("num_losers", 0) for r in rows),
        "premium_received": sum(r.get("premium_received", 0) for r in rows),
        "premium_paid": sum(r.get("premium_paid", 0) for r in rows),
        "trades_opened": sum(r.get("trades_opened", 0) for r in rows),
    }

    # Best trade: highest PnL across accounts. Skip rows where the symbol is
    # missing — without a symbol we can't link to /position/<symbol> and
    # url_for() will raise BuildError at render time.
    best_candidates = [
        r for r in rows
        if r.get("best_pnl") is not None and r.get("best_symbol")
    ]
    if best_candidates:
        best = max(best_candidates, key=lambda r: float(r.get("best_pnl", 0)))
        summary["best_trade"] = {
            "symbol": best.get("best_symbol") or "",
            "strategy": best.get("best_strategy") or "",
            "trade_symbol": best.get("best_trade_symbol") or "",
            "total_pnl": float(best.get("best_pnl", 0)),
            "close_date": str(best.get("best_close_date") or ""),
            "account": best.get("account") or "",
        }
    else:
        summary["best_trade"] = None

    # Worst trade: lowest PnL across accounts. Same symbol-required guard.
    worst_candidates = [
        r for r in rows
        if r.get("worst_pnl") is not None and r.get("worst_symbol")
    ]
    if worst_candidates:
        worst = min(worst_candidates, key=lambda r: float(r.get("worst_pnl", 0)))
        summary["worst_trade"] = {
            "symbol": worst.get("worst_symbol") or "",
            "strategy": worst.get("worst_strategy") or "",
            "trade_symbol": worst.get("worst_trade_symbol") or "",
            "total_pnl": float(worst.get("worst_pnl", 0)),
            "close_date": str(worst.get("worst_close_date") or ""),
            "account": worst.get("account") or "",
        }
    else:
        summary["worst_trade"] = None

    # Largest mistake (worst loser)
    if summary["worst_trade"] and summary["worst_trade"]["total_pnl"] < 0:
        summary["largest_mistake"] = summary["worst_trade"]
    else:
        summary["largest_mistake"] = None

    # Top strategy: pick the one with highest win rate across all accounts
    strat_candidates = [r for r in rows if r.get("top_strategy")]
    if strat_candidates:
        top = max(strat_candidates, key=lambda r: float(r.get("top_strategy_win_rate", 0)))
        summary["most_consistent_strategy"] = {
            "strategy": top.get("top_strategy", ""),
            "win_rate": float(top.get("top_strategy_win_rate", 0)),
            "trades": int(top.get("top_strategy_trades", 0)),
            "total_pnl": float(top.get("top_strategy_pnl", 0)),
        }
    else:
        summary["most_consistent_strategy"] = None

    return summary


# ──────────────────────────────────────────────────────────────────
# Per-symbol attribution + rollups
# ──────────────────────────────────────────────────────────────────
#
# Powers the three breakdown tables on the Daily Review:
#
#   • Positions ── one row per (symbol). The "CC Trading Summary" the
#                   user already maintains in Excel — G/L Stock | G/L
#                   Option | Dividend | Net | Annualized.
#   • Strategies ── same shape grouped by strategy label.
#   • Sectors    ── same shape grouped by yfinance sector (Tech /
#                   Energy / Financials / …) and subsector.
#
# All three pull from the same `POSITION_ATTRIBUTION_QUERY` result so
# the totals reconcile.

# Floor on the annualized-return denominator. Reading $5 of dividend
# income from a $1 dust-lot must not extrapolate to "20,000%/yr". A
# $200 floor caps annualized to a reasonable upper bound on tiny
# positions while leaving real cost basis untouched.
ANNUALIZED_DENOMINATOR_FLOOR = 200.0
# And a minimum holding window — if you opened a position yesterday and
# made $40, 365/1 = 365x scaling is dishonest math. Anchor the
# annualized window at 30 days minimum.
ANNUALIZED_MIN_DAYS = 30


def _annualized_pct(net_pnl, capital_at_risk, days_held):
    """Simple holding-period annualized return %.

    annualized = (net / capital) * (365 / max(days_held, 30)) * 100

    Returns None when capital is too small to be meaningful (see
    ANNUALIZED_DENOMINATOR_FLOOR — protects against $0.50 cost basis
    producing 10,000%).
    """
    try:
        cap = float(capital_at_risk or 0)
    except (TypeError, ValueError):
        return None
    if cap < ANNUALIZED_DENOMINATOR_FLOOR:
        return None
    try:
        net = float(net_pnl or 0)
    except (TypeError, ValueError):
        return None
    days = max(int(days_held or 0), ANNUALIZED_MIN_DAYS)
    return round(net / cap * (365.0 / days) * 100.0, 1)


def _strategy_for_symbol(symbol, strategy_breakdown_lookup):
    """Best-effort: when a symbol has more than one strategy attached,
    pick the one with the largest absolute P&L (matches the headline
    "primary strategy" we already use on /positions). Returns None when
    no classification exists for the symbol — caller should fall back to
    'Buy and Hold' or 'Unclassified'."""
    rows = strategy_breakdown_lookup.get(symbol)
    if not rows:
        return None
    top = max(rows, key=lambda r: abs(float(r.get("total_pnl") or 0)))
    return top.get("strategy") or None


def _build_position_breakdown(attribution_df, strategy_by_symbol, *, week_start=None):
    """Per-symbol breakdown rows for the Positions table.

    Input is the raw `POSITION_ATTRIBUTION_QUERY` DataFrame already
    filtered to the current tenant. Aggregates across accounts so the
    "All Accounts" view collapses to one row per symbol (single-account
    view is the same shape, trivially).

    Scope filter (Daily Review semantic): when ``week_start`` is
    provided, only symbols that are CURRENTLY OPEN or whose most recent
    activity is on/after ``week_start`` are returned. Without this
    filter the table runs to dozens of historically-closed symbols
    (76 rows in May 2026 testing) which buries the few positions the
    user actually needs to look at end-of-day. The strategy / sector
    / subsector rollups inherit this filter for free because they
    aggregate over the returned rows.
    """
    if attribution_df is None or attribution_df.empty:
        return []

    grouped = (
        attribution_df.groupby("symbol", dropna=False)
        .agg(
            equity_pnl=("equity_pnl", "sum"),
            option_pnl=("option_pnl", "sum"),
            dividend_income=("dividend_income", "sum"),
            net_pnl=("net_pnl", "sum"),
            equity_capital=("equity_capital", "sum"),
            option_capital_paid=("option_capital_paid", "sum"),
            option_premium_collected=("option_premium_collected", "sum"),
            current_equity_cost=("current_equity_cost", "sum"),
            current_equity_value=("current_equity_value", "sum"),
            current_option_value=("current_option_value", "sum"),
            current_equity_shares=("current_equity_shares", "sum"),
            num_equity_legs=("num_equity_legs", "sum"),
            num_option_legs=("num_option_legs", "sum"),
            num_open_groups=("num_open_groups", "sum"),
            num_closed_groups=("num_closed_groups", "sum"),
            first_open_date=("first_open_date", "min"),
            last_activity_date=("last_activity_date", "max"),
            dividend_count=("dividend_count", "sum"),
            sector=("sector", "first"),
            subsector=("subsector", "first"),
            company_name=("company_name", "first"),
        )
        .reset_index()
    )

    rows = []
    for _, r in grouped.iterrows():
        sym = str(r.get("symbol") or "")
        if not sym:
            continue
        equity_cap = float(r.get("equity_capital") or 0)
        opt_cap_paid = float(r.get("option_capital_paid") or 0)
        opt_cap_coll = float(r.get("option_premium_collected") or 0)
        cur_eq_cost = float(r.get("current_equity_cost") or 0)
        # Capital at risk: trade-history buys cover most cases; for
        # transferred-in lots (no buy row) we add the current snapshot's
        # cost basis so the denominator isn't $0.
        capital_at_risk = max(equity_cap + opt_cap_paid + opt_cap_coll, cur_eq_cost)
        # Days held: from first trade to last activity (close date or
        # today for still-open positions).
        try:
            first_d = r.get("first_open_date")
            last_d = r.get("last_activity_date")
            if hasattr(first_d, "date"):
                first_d = first_d.date()
            if hasattr(last_d, "date"):
                last_d = last_d.date()
            days_held = (last_d - first_d).days if first_d and last_d else 0
        except Exception:
            days_held = 0

        net_pnl = float(r.get("net_pnl") or 0)
        pct_return = None
        if capital_at_risk >= ANNUALIZED_DENOMINATOR_FLOOR:
            pct_return = round(net_pnl / capital_at_risk * 100.0, 1)
        annualized = _annualized_pct(net_pnl, capital_at_risk, days_held)

        is_open = (
            int(r.get("num_open_groups") or 0) > 0
            or int(r.get("num_equity_legs") or 0) > 0
            or int(r.get("num_option_legs") or 0) > 0
        )
        rows.append({
            "symbol": sym,
            "company_name": str(r.get("company_name") or "") or None,
            "equity_pnl": round(float(r.get("equity_pnl") or 0), 2),
            "option_pnl": round(float(r.get("option_pnl") or 0), 2),
            "dividend_income": round(float(r.get("dividend_income") or 0), 2),
            "net_pnl": round(net_pnl, 2),
            "capital_at_risk": round(capital_at_risk, 2),
            "current_equity_cost": round(cur_eq_cost, 2),
            "current_equity_value": round(float(r.get("current_equity_value") or 0), 2),
            "current_option_value": round(float(r.get("current_option_value") or 0), 2),
            "current_equity_shares": float(r.get("current_equity_shares") or 0),
            "num_equity_legs": int(r.get("num_equity_legs") or 0),
            "num_option_legs": int(r.get("num_option_legs") or 0),
            "num_open_groups": int(r.get("num_open_groups") or 0),
            "num_closed_groups": int(r.get("num_closed_groups") or 0),
            "dividend_count": int(r.get("dividend_count") or 0),
            "first_open_date": (first_d.isoformat() if first_d else None),
            "last_activity_date": (last_d.isoformat() if last_d else None),
            "days_held": days_held,
            "pct_return": pct_return,
            "annualized_pct": annualized,
            "status": "Open" if is_open else "Closed",
            "strategy": strategy_by_symbol.get(sym),
            "sector": str(r.get("sector") or "Unknown") or "Unknown",
            "subsector": str(r.get("subsector") or "Unknown") or "Unknown",
        })

    # Daily Review scope: open positions + positions closed this week.
    # ``last_activity_date`` is the actual close_date for Closed symbols
    # and today for Open ones (see POSITION_ATTRIBUTION_QUERY) so this
    # filter is symmetric across both branches.
    if week_start is not None:
        def _is_in_scope(row):
            if row["status"] == "Open":
                return True
            last = row.get("last_activity_date")
            if not last:
                return False
            try:
                last_d = date.fromisoformat(last) if isinstance(last, str) else last
            except (TypeError, ValueError):
                return False
            return last_d >= week_start
        rows = [r for r in rows if _is_in_scope(r)]

    rows.sort(key=lambda x: x["net_pnl"], reverse=True)
    return rows


def _aggregate_breakdown_by(rows, key, label_name="bucket"):
    """Group per-symbol breakdown rows by a categorical key (strategy /
    sector / subsector). Returns rows in the same shape so the template
    renders all three tables with one macro.

    Annualized for the bucket uses summed capital_at_risk and the MAX
    days_held in the bucket (longest-running position anchors the
    window — picking max instead of mean prevents brand-new positions
    in the bucket from dragging the bucket's annualized math to
    "everything's a moonshot").
    """
    by = {}
    for r in rows:
        k = r.get(key) or "Unclassified"
        slot = by.setdefault(k, {
            label_name: k,
            "equity_pnl": 0.0,
            "option_pnl": 0.0,
            "dividend_income": 0.0,
            "net_pnl": 0.0,
            "capital_at_risk": 0.0,
            "current_equity_value": 0.0,
            "current_option_value": 0.0,
            "max_days_held": 0,
            "symbols": [],
            "num_open": 0,
            "num_closed": 0,
            "num_winners": 0,
            "num_losers": 0,
        })
        slot["equity_pnl"] += float(r.get("equity_pnl") or 0)
        slot["option_pnl"] += float(r.get("option_pnl") or 0)
        slot["dividend_income"] += float(r.get("dividend_income") or 0)
        slot["net_pnl"] += float(r.get("net_pnl") or 0)
        slot["capital_at_risk"] += float(r.get("capital_at_risk") or 0)
        slot["current_equity_value"] += float(r.get("current_equity_value") or 0)
        slot["current_option_value"] += float(r.get("current_option_value") or 0)
        if int(r.get("days_held") or 0) > slot["max_days_held"]:
            slot["max_days_held"] = int(r.get("days_held") or 0)
        slot["symbols"].append(r.get("symbol"))
        if r.get("status") == "Open":
            slot["num_open"] += 1
        else:
            slot["num_closed"] += 1
        net = float(r.get("net_pnl") or 0)
        if net > 0:
            slot["num_winners"] += 1
        elif net < 0:
            slot["num_losers"] += 1

    out = []
    for k, s in by.items():
        cap = s["capital_at_risk"]
        net = s["net_pnl"]
        pct = round(net / cap * 100.0, 1) if cap >= ANNUALIZED_DENOMINATOR_FLOOR else None
        ann = _annualized_pct(net, cap, s["max_days_held"])
        s_total = {
            label_name: k,
            "equity_pnl": round(s["equity_pnl"], 2),
            "option_pnl": round(s["option_pnl"], 2),
            "dividend_income": round(s["dividend_income"], 2),
            "net_pnl": round(net, 2),
            "capital_at_risk": round(cap, 2),
            "current_equity_value": round(s["current_equity_value"], 2),
            "current_option_value": round(s["current_option_value"], 2),
            "current_value": round(s["current_equity_value"] + s["current_option_value"], 2),
            "max_days_held": s["max_days_held"],
            "pct_return": pct,
            "annualized_pct": ann,
            "num_symbols": len(s["symbols"]),
            "symbols": s["symbols"],
            "num_open": s["num_open"],
            "num_closed": s["num_closed"],
            "num_winners": s["num_winners"],
            "num_losers": s["num_losers"],
        }
        out.append(s_total)
    out.sort(key=lambda x: x["net_pnl"], reverse=True)
    return out


def _build_breakdown_totals(rows):
    """Footer-row totals for any of the breakdown tables."""
    if not rows:
        return None
    equity = sum(float(r.get("equity_pnl") or 0) for r in rows)
    option = sum(float(r.get("option_pnl") or 0) for r in rows)
    div = sum(float(r.get("dividend_income") or 0) for r in rows)
    net = sum(float(r.get("net_pnl") or 0) for r in rows)
    cap = sum(float(r.get("capital_at_risk") or 0) for r in rows)
    eq_profitable = sum(1 for r in rows if float(r.get("equity_pnl") or 0) > 0)
    eq_losing = sum(1 for r in rows if float(r.get("equity_pnl") or 0) < 0)
    opt_profitable = sum(1 for r in rows if float(r.get("option_pnl") or 0) > 0)
    opt_losing = sum(1 for r in rows if float(r.get("option_pnl") or 0) < 0)
    net_profitable = sum(1 for r in rows if float(r.get("net_pnl") or 0) > 0)
    net_losing = sum(1 for r in rows if float(r.get("net_pnl") or 0) < 0)
    # For "Profitability scorecard" footer (matches the user's
    # spreadsheet ratio at the bottom): only count symbols that had
    # exposure in that asset class.
    eq_with_exposure = sum(1 for r in rows if float(r.get("equity_pnl") or 0) != 0)
    opt_with_exposure = sum(1 for r in rows if float(r.get("option_pnl") or 0) != 0)
    return {
        "equity_pnl": round(equity, 2),
        "option_pnl": round(option, 2),
        "dividend_income": round(div, 2),
        "net_pnl": round(net, 2),
        "capital_at_risk": round(cap, 2),
        "pct_return": round(net / cap * 100.0, 1) if cap >= ANNUALIZED_DENOMINATOR_FLOOR else None,
        "num_symbols": len(rows),
        "equity_profitable": eq_profitable,
        "equity_losing": eq_losing,
        "equity_with_exposure": eq_with_exposure,
        "option_profitable": opt_profitable,
        "option_losing": opt_losing,
        "option_with_exposure": opt_with_exposure,
        "net_profitable": net_profitable,
        "net_losing": net_losing,
        "equity_win_pct": round(eq_profitable / eq_with_exposure * 100.0, 1) if eq_with_exposure else None,
        "option_win_pct": round(opt_profitable / opt_with_exposure * 100.0, 1) if opt_with_exposure else None,
        "net_win_pct": round(net_profitable / len(rows) * 100.0, 1) if rows else None,
    }


def _build_account_breakdown(attribution_df, label_map=None, *, week_start=None):
    """One summarized row per ACCOUNT (tenant) for the Daily Review.

    Rolls the per-(tenant, symbol) attribution up to one line per
    physical account so the trader sees, at a glance, how each account
    is doing split by asset type:

        Account | Stock (equity P&L) | Options (option P&L) | Dividend
                | Total (net) | G/L % | Annualized G/L %

    Scope (Daily Review semantic): when ``week_start`` is provided, only
    positions that are CURRENTLY OPEN or were CLOSED on/after
    ``week_start`` contribute to each account's row — same filter the
    per-symbol breakdown uses, so the scorecard reads as a "what's live
    or just closed" pulse, not a lifetime ledger. Pass ``week_start=None``
    for a full lifetime view. The per-symbol number themselves are always
    lifetime (a symbol's G/L doesn't reset weekly); the filter only
    decides which symbols are IN the account total.

    The detailed per-symbol / strategy / sector breakdown lives on
    /accounts; each row carries ``tenant_id`` so the template can
    deep-link there via ``?tenant=<tenant_id>``.

    Annualized math matches the per-symbol / strategy rollups:
      • denominator = summed capital-at-risk (buy cash + current cost),
        floored by ANNUALIZED_DENOMINATOR_FLOOR so dust lots don't
        extrapolate to four-digit %.
      • window anchors on the LONGEST-held position in the account
        (max days_held) — a brand-new lot can't shrink the window and
        inflate the rate.

    Returns ``{"rows": [...], "totals": {...} | None}``.
    """
    label_map = label_map or {}
    if attribution_df is None or attribution_df.empty:
        return {"rows": [], "totals": None}

    # Per (tenant, symbol) first so capital sums cleanly and we can take
    # the MAX days_held within each account.
    grouped = (
        attribution_df.groupby(["tenant_id", "symbol"], dropna=False)
        .agg(
            equity_pnl=("equity_pnl", "sum"),
            option_pnl=("option_pnl", "sum"),
            dividend_income=("dividend_income", "sum"),
            net_pnl=("net_pnl", "sum"),
            equity_capital=("equity_capital", "sum"),
            option_capital_paid=("option_capital_paid", "sum"),
            option_premium_collected=("option_premium_collected", "sum"),
            current_equity_cost=("current_equity_cost", "sum"),
            num_open_groups=("num_open_groups", "sum"),
            num_equity_legs=("num_equity_legs", "sum"),
            num_option_legs=("num_option_legs", "sum"),
            first_open_date=("first_open_date", "min"),
            last_activity_date=("last_activity_date", "max"),
            account=("account", "first"),
        )
        .reset_index()
    )

    by = {}
    for _, r in grouped.iterrows():
        tid = str(r.get("tenant_id") or "")
        equity_cap = float(r.get("equity_capital") or 0)
        opt_cap_paid = float(r.get("option_capital_paid") or 0)
        opt_cap_coll = float(r.get("option_premium_collected") or 0)
        cur_eq_cost = float(r.get("current_equity_cost") or 0)
        capital_at_risk = max(equity_cap + opt_cap_paid + opt_cap_coll, cur_eq_cost)
        try:
            first_d = r.get("first_open_date")
            last_d = r.get("last_activity_date")
            if hasattr(first_d, "date"):
                first_d = first_d.date()
            if hasattr(last_d, "date"):
                last_d = last_d.date()
            days_held = (last_d - first_d).days if first_d and last_d else 0
        except Exception:
            last_d = None
            days_held = 0

        # Daily Review scope: keep currently-open positions plus anything
        # closed on/after week_start. ``last_activity_date`` is today for
        # open positions and the close date for closed ones (see
        # POSITION_ATTRIBUTION_QUERY), so this filter is symmetric.
        is_open = (
            int(r.get("num_open_groups") or 0) > 0
            or int(r.get("num_equity_legs") or 0) > 0
            or int(r.get("num_option_legs") or 0) > 0
        )
        if week_start is not None and not is_open:
            if last_d is None or last_d < week_start:
                continue

        slot = by.get(tid)
        if slot is None:
            slot = {
                "tenant_id": tid,
                "account": str(r.get("account") or ""),
                "equity_pnl": 0.0,
                "option_pnl": 0.0,
                "dividend_income": 0.0,
                "net_pnl": 0.0,
                "capital_at_risk": 0.0,
                "max_days_held": 0,
            }
            by[tid] = slot
        slot["equity_pnl"] += float(r.get("equity_pnl") or 0)
        slot["option_pnl"] += float(r.get("option_pnl") or 0)
        slot["dividend_income"] += float(r.get("dividend_income") or 0)
        slot["net_pnl"] += float(r.get("net_pnl") or 0)
        slot["capital_at_risk"] += capital_at_risk
        if days_held > slot["max_days_held"]:
            slot["max_days_held"] = days_held

    rows = []
    for tid, s in by.items():
        cap = s["capital_at_risk"]
        net = s["net_pnl"]
        pct = round(net / cap * 100.0, 1) if cap >= ANNUALIZED_DENOMINATOR_FLOOR else None
        ann = _annualized_pct(net, cap, s["max_days_held"])
        rows.append({
            "tenant_id": tid,
            "account_display": label_map.get(tid) or s["account"] or tid,
            "equity_pnl": round(s["equity_pnl"], 2),
            "option_pnl": round(s["option_pnl"], 2),
            "dividend_income": round(s["dividend_income"], 2),
            "net_pnl": round(net, 2),
            "capital_at_risk": round(cap, 2),
            "pct_return": pct,
            "annualized_pct": ann,
            "max_days_held": s["max_days_held"],
        })
    rows.sort(key=lambda x: x["net_pnl"], reverse=True)

    totals = None
    if len(rows) > 1:
        t_eq = sum(r["equity_pnl"] for r in rows)
        t_opt = sum(r["option_pnl"] for r in rows)
        t_div = sum(r["dividend_income"] for r in rows)
        t_net = sum(r["net_pnl"] for r in rows)
        t_cap = sum(r["capital_at_risk"] for r in rows)
        t_days = max((r["max_days_held"] for r in rows), default=0)
        totals = {
            "equity_pnl": round(t_eq, 2),
            "option_pnl": round(t_opt, 2),
            "dividend_income": round(t_div, 2),
            "net_pnl": round(t_net, 2),
            "capital_at_risk": round(t_cap, 2),
            "pct_return": round(t_net / t_cap * 100.0, 1) if t_cap >= ANNUALIZED_DENOMINATOR_FLOOR else None,
            "annualized_pct": _annualized_pct(t_net, t_cap, t_days),
            "num_accounts": len(rows),
        }

    # Benchmark basis: capital + holding window of the "total" line, so a
    # caller can compute "what the index returned over the SAME window on
    # the SAME capital" and render it directly beneath the totals row.
    # Single-account → the one row IS the total.
    basis = None
    if totals:
        basis = {
            "capital_at_risk": totals["capital_at_risk"],
            "days": t_days,
        }
    elif rows:
        basis = {
            "capital_at_risk": rows[0]["capital_at_risk"],
            "days": rows[0]["max_days_held"],
        }

    return {"rows": rows, "totals": totals, "basis": basis}


def _build_today_movers(today_moves_df, account_total_value=None):
    """Today's biggest stock moves on currently-held symbols.

    Returns at most 8 winners and 8 losers, sorted by absolute $ impact.
    """
    if today_moves_df is None or today_moves_df.empty:
        return {"winners": [], "losers": [], "total_impact": 0.0, "as_of": None}
    df = today_moves_df.copy()
    for col in ["shares", "today_close", "prev_close", "price_change",
                "price_change_pct", "dollar_impact", "current_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    total_impact = float(df["dollar_impact"].sum()) if "dollar_impact" in df.columns else 0.0
    items = []
    as_of = None
    for _, r in df.iterrows():
        td = r.get("today_date")
        if as_of is None and td is not None:
            as_of = td.isoformat() if hasattr(td, "isoformat") else str(td)[:10]
        items.append({
            "symbol": str(r.get("symbol") or ""),
            "shares": float(r.get("shares") or 0),
            "current_value": round(float(r.get("current_value") or 0), 2),
            "price_change": round(float(r.get("price_change") or 0), 2),
            "price_change_pct": round(float(r.get("price_change_pct") or 0), 2),
            "dollar_impact": round(float(r.get("dollar_impact") or 0), 2),
            "today_close": round(float(r.get("today_close") or 0), 2),
        })

    winners = sorted([i for i in items if i["dollar_impact"] > 0],
                     key=lambda x: x["dollar_impact"], reverse=True)[:8]
    losers = sorted([i for i in items if i["dollar_impact"] < 0],
                    key=lambda x: x["dollar_impact"])[:8]
    return {
        "winners": winners,
        "losers": losers,
        "total_impact": round(total_impact, 2),
        "as_of": as_of,
    }


def _build_after_hours_movers(ah_df):
    """After-hours moves: broker mark (as of last sync) vs today's close.

    Reporting is close-based (the close is the price the trader decided on
    during the day); this section surfaces the after-hours drift separately
    so it informs without polluting the core numbers. Returns at most 8
    winners and 8 losers by absolute $ impact, plus the broker snapshot date
    ("as of last broker sync"). Empty until today's official close publishes.
    """
    empty = {"winners": [], "losers": [], "total_impact": 0.0, "as_of": None}
    if ah_df is None or ah_df.empty:
        return empty
    df = ah_df.copy()
    for col in ["shares", "broker_mark", "today_close", "price_change",
                "price_change_pct", "dollar_impact"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    total_impact = float(df["dollar_impact"].sum()) if "dollar_impact" in df.columns else 0.0
    items = []
    as_of = None
    for _, r in df.iterrows():
        sd = r.get("snapshot_date")
        if as_of is None and sd is not None:
            as_of = sd.isoformat() if hasattr(sd, "isoformat") else str(sd)[:10]
        items.append({
            "symbol": str(r.get("symbol") or ""),
            "shares": float(r.get("shares") or 0),
            "broker_mark": round(float(r.get("broker_mark") or 0), 2),
            "today_close": round(float(r.get("today_close") or 0), 2),
            "price_change": round(float(r.get("price_change") or 0), 2),
            "price_change_pct": round(float(r.get("price_change_pct") or 0), 2),
            "dollar_impact": round(float(r.get("dollar_impact") or 0), 2),
        })

    # Only surface meaningful drift (broker mark genuinely differs from close).
    items = [i for i in items if abs(i["dollar_impact"]) >= 0.01]
    winners = sorted([i for i in items if i["dollar_impact"] > 0],
                     key=lambda x: x["dollar_impact"], reverse=True)[:8]
    losers = sorted([i for i in items if i["dollar_impact"] < 0],
                    key=lambda x: x["dollar_impact"])[:8]
    if not winners and not losers:
        return {**empty, "as_of": as_of}
    return {
        "winners": winners,
        "losers": losers,
        "total_impact": round(total_impact, 2),
        "as_of": as_of,
    }


def _build_upcoming_dividends(div_df):
    """Projected next ex-div dates for held dividend-paying symbols."""
    if div_df is None or div_df.empty:
        return []
    out = []
    for _, r in div_df.iterrows():
        proj = r.get("projected_next_ex_div_date")
        last = r.get("last_ex_div_date")
        try:
            d_until = int(r.get("days_until_projected")) if r.get("days_until_projected") is not None else None
        except (TypeError, ValueError):
            d_until = None
        proj_s = (
            proj.isoformat() if hasattr(proj, "isoformat") and not isinstance(proj, str)
            else str(proj)[:10] if proj is not None else None
        )
        last_s = (
            last.isoformat() if hasattr(last, "isoformat") and not isinstance(last, str)
            else str(last)[:10] if last is not None else None
        )
        out.append({
            "symbol": str(r.get("symbol") or ""),
            "company": str(r.get("long_name") or "") or None,
            "sector": str(r.get("sector") or "") if r.get("sector") not in (None, "Unknown") else "",
            "subsector": str(r.get("subsector") or "") if r.get("subsector") not in (None, "Unknown") else "",
            "projected_date": proj_s,
            "last_ex_div_date": last_s,
            "last_amount_per_share": float(r.get("last_amount_per_share") or 0),
            "days_until": d_until,
            "median_spacing_days": int(r.get("median_spacing_days") or 0) or None,
        })
    out.sort(key=lambda x: x.get("days_until") if x.get("days_until") is not None else 999)
    return out


_OSI_RE = __import__("re").compile(r"^(?P<root>[A-Z]+)\s+(?P<y>\d{2})(?P<m>\d{2})(?P<d>\d{2})(?P<cp>[CP])(?P<strike>\d{8})$")


def _format_trade_contract(trade_symbol, symbol):
    """Human label for a trade group's ``trade_symbol``.

    Option contracts arrive as OSI-ish strings ("ASTS  260605C00102000")
    → "ASTS Jun 5 $102 Call". Equity sessions arrive as "<SYM>_session_N"
    → just the underlying symbol. Anything we can't parse falls back to
    the raw trade_symbol so we never render an empty cell.
    """
    ts = (str(trade_symbol or "")).strip()
    if not ts:
        return str(symbol or "")
    compact = " ".join(ts.split())
    m = _OSI_RE.match(compact)
    if m:
        import datetime as _dt
        try:
            exp = _dt.date(2000 + int(m.group("y")), int(m.group("m")), int(m.group("d")))
            exp_s = exp.strftime("%b %-d")
        except ValueError:
            exp_s = ""
        strike = int(m.group("strike")) / 1000.0
        strike_s = f"${strike:,.2f}".rstrip("0").rstrip(".")
        kind = "Call" if m.group("cp") == "C" else "Put"
        parts = [m.group("root")]
        if exp_s:
            parts.append(exp_s)
        parts.append(strike_s)
        parts.append(kind)
        return " ".join(parts)
    if ts.endswith(tuple(f"_session_{i}" for i in range(0, 10))) or "_session_" in ts:
        return str(symbol or ts.split("_session_")[0])
    return compact


def _build_trades_this_week(trades_df, week_start, week_end, label_map=None):
    """Build ONE unified list of trade groups touched this week for the
    Daily Review "Trades this week" section.

    Source: ``mart_weekly_trades`` already filtered to ``week_start`` and
    tenant-scoped. Each df row is one trade group (one row per group in
    ``int_strategy_classification``); the mart keys ``week_start`` on
    ``coalesce(close_date, open_date)``. A group is shown when:

      - it CLOSED this week  (status Closed AND close_date in the ISO week), OR
      - it OPENED this week   (open_date in the ISO week AND num_trades > 0;
        num_trades == 0 is a snapshot-only synthetic open the mart says to hide)

    A group that opened AND closed in the same week is a SINGLE row (it's
    one trade group), tagged ``is_closed`` so the table shows its realized
    P&L. This replaces the old Opened/Closed two-table split, which
    rendered the same same-week round trip in both tables. Account display
    uses the disambiguated tenant label map so several "Schwab Account"s
    read as their nicknames.

    Rows are grouped to ONE line per ``(tenant_id, symbol)`` within the
    account. Traders write a fresh weekly covered call on each underlying,
    so per-contract rows read as duplicates ("ASTS again?"). We net every
    contract/session on the same symbol this week into a single row:

      - ``realized_pnl``   = Σ ``total_pnl`` of legs that CLOSED this week
      - ``unrealized_pnl`` = Σ ``current_unrealized_pnl`` of legs still OPEN
      - ``result_pnl``     = realized + unrealized (the one number shown)
      - ``status``         = "Open" if ANY leg is still open, else "Closed"
      - ``result_kind``    = "realized" (all closed) / "unrealized" (all
        open) / "net" (a mix of closed + open legs this week)

    ``contract`` shows the single contract name when the symbol has one leg
    this week, otherwise an "N contracts" summary. Mixed strategies across a
    symbol's legs render as "Mixed".

    Returns a dict with a unified ``trades`` list plus summary counters
    (``closed_count`` / ``opened_count`` / ``realized_pnl`` /
    ``unrealized_pnl``) for the header.
    """
    empty = {"trades": [], "count": 0, "opened_count": 0, "closed_count": 0,
             "realized_pnl": 0.0, "unrealized_pnl": 0.0, "has_any": False}
    if trades_df is None or trades_df.empty:
        return empty
    label_map = label_map or {}

    def _as_date(v):
        if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
            return None
        if hasattr(v, "isoformat") and not isinstance(v, str):
            try:
                return v.date() if hasattr(v, "date") else v
            except Exception:
                return None
        try:
            return pd.to_datetime(v).date()
        except Exception:
            return None

    # Aggregate every qualifying leg (option contract or equity session
    # that opened or closed this week) into one bucket per (tenant, symbol).
    groups = {}
    order = []
    for _, r in trades_df.iterrows():
        tid = str(r.get("tenant_id") or "")
        status = str(r.get("status") or "")
        od = _as_date(r.get("open_date"))
        cd = _as_date(r.get("close_date"))
        num_trades = int(float(r.get("num_trades") or 0))

        closed_this_week = (
            status == "Closed" and cd is not None and week_start <= cd <= week_end
        )
        opened_this_week = (
            od is not None and week_start <= od <= week_end and num_trades > 0
        )
        if not (closed_this_week or opened_this_week):
            continue

        symbol = str(r.get("symbol") or "")
        total_pnl = float(r.get("total_pnl") or 0)
        unrealized = float(r.get("current_unrealized_pnl") or 0)
        leg_closed = bool(closed_this_week)

        key = (tid, symbol)
        g = groups.get(key)
        if g is None:
            g = {
                "symbol": symbol,
                "tenant_id": tid,
                "account_display": label_map.get(tid) or str(r.get("account") or ""),
                "strategies": set(),
                "contracts": [],
                "realized": 0.0,
                "unrealized": 0.0,
                "num_legs": 0,
                "has_open": False,
                "has_closed": False,
                "opened_this_week": False,
                "open_dates": [],
                "close_dates": [],
            }
            groups[key] = g
            order.append(key)

        g["num_legs"] += 1
        strat = str(r.get("strategy") or "").strip()
        if strat:
            g["strategies"].add(strat)
        g["contracts"].append(_format_trade_contract(r.get("trade_symbol"), r.get("symbol")))
        if od is not None:
            g["open_dates"].append(od)
        if opened_this_week:
            g["opened_this_week"] = True
        if leg_closed:
            g["has_closed"] = True
            g["realized"] += total_pnl
            if cd is not None:
                g["close_dates"].append(cd)
        else:
            g["has_open"] = True
            g["unrealized"] += unrealized

    trades = []
    for key in order:
        g = groups[key]
        is_closed = not g["has_open"]
        strategies = sorted(g["strategies"])
        if len(strategies) == 1:
            strategy = strategies[0]
        elif strategies:
            strategy = "Mixed"
        else:
            strategy = "Uncategorized"
        realized = round(g["realized"], 2)
        unrealized = round(g["unrealized"], 2)
        if is_closed:
            result_kind = "realized"
        elif g["has_closed"]:
            result_kind = "net"
        else:
            result_kind = "unrealized"
        trades.append({
            "symbol": g["symbol"],
            "tenant_id": g["tenant_id"],
            "account_display": g["account_display"],
            "strategy": strategy,
            "num_legs": g["num_legs"],
            "contract": (
                g["contracts"][0] if g["num_legs"] == 1
                else f"{g['num_legs']} contracts"
            ),
            "status": "Closed" if is_closed else "Open",
            "is_closed": is_closed,
            "opened_this_week": g["opened_this_week"],
            "open_date": min(g["open_dates"]) if g["open_dates"] else None,
            "close_date": max(g["close_dates"]) if (is_closed and g["close_dates"]) else None,
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "result_pnl": round(realized + unrealized, 2),
            "result_kind": result_kind,
        })

    realized_pnl = sum(r["realized_pnl"] for r in trades)
    unrealized_pnl = sum(r["unrealized_pnl"] for r in trades)
    closed_count = sum(1 for r in trades if r["is_closed"])
    opened_count = sum(1 for r in trades if not r["is_closed"])

    # Group by account (alphabetical, case-insensitive) so the table reads
    # account-by-account; within an account keep most-recent activity first
    # (close date for closed symbols, open date for still-open symbols).
    # Python's sort is stable, so sort by the secondary key first.
    trades.sort(
        key=lambda x: (x["close_date"] if x["is_closed"] else x["open_date"]) or week_start,
        reverse=True,
    )
    trades.sort(key=lambda x: (x["account_display"] or "").lower())
    return {
        "trades": trades,
        "count": len(trades),
        "opened_count": opened_count,
        "closed_count": closed_count,
        "realized_pnl": round(realized_pnl, 2),
        "unrealized_pnl": round(unrealized_pnl, 2),
        "has_any": bool(trades),
    }


def _today_headline(today_pulse, today_movers, equity_snapshot):
    """One-liner that anchors the page: '"Today: -$2,134 (-0.11%)"`.

    Falls back gracefully when the broker snapshot isn't there yet
    (cold-start, mid-day, weekend)."""
    if not today_pulse:
        return None
    delta = float(today_pulse.get("delta") or 0)
    sign = "+" if delta >= 0 else "-"
    pct = None
    if equity_snapshot and equity_snapshot.get("account_value"):
        base = float(equity_snapshot["account_value"]) - delta
        if base > 0:
            pct = round(delta / base * 100, 2)
    pct_str = f" ({'+' if pct is not None and pct >= 0 else ''}{pct:.2f}%)" if pct is not None else ""
    return f"Today: {sign}${abs(delta):,.0f}{pct_str}"


# Decorator order is intentional: ``/daily-review`` is the inner (applied
# first) so Flask registers it first in the url_map, and ``url_for(
# 'weekly_review')`` returns ``/daily-review``. ``/weekly-review`` stays
# attached as a legacy alias so external bookmarks keep working.
@app.route("/weekly-review")
@app.route("/daily-review")
@login_required
def weekly_review():
    """Daily Review — end-of-day pulse: today, watch list, position /
    strategy / sector breakdowns. Single mode (the old friday / monday /
    midweek toggle was removed in favor of a consistent daily shape).

    Endpoint name kept as ``weekly_review`` so the 30+ ``url_for()``
    callsites across templates, auth, profile, upload, admin etc.
    continue to work without a coordinated cross-cut rename. The URL
    is exposed under both ``/daily-review`` (canonical) and
    ``/weekly-review`` (legacy alias for bookmarks).
    """
    user_accounts = _user_account_list()

    # Account focus (multi-account support). Regular users can only
    # restrict to accounts they own; admins can ad-hoc any label.
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

    prof = get_user_profile(current_user.id) or {}
    user_tz = (prof.get("timezone") or "America/New_York").strip() or "America/New_York"
    today = _date_in_user_tz(user_tz)
    this_week = _iso_week_start(today)
    week_end = this_week + timedelta(days=6)

    from_upload = request.args.get("from_upload") == "1"
    from_sync = request.args.get("from_sync") == "1"

    # Brand-new accounts with zero linked broker accounts → bounce to
    # /get-started so they don't sit on a "still calculating" banner
    # forever. _redirect_if_no_accounts also short-circuits on the
    # post-upload / post-sync flows so the processing screen still works.
    from app.routes import _redirect_if_no_accounts
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce

    # ── Visit anchor for "Since you last looked" ──
    prior_visit = bump_review_visit(current_user.id, datetime.now(ZoneInfo("UTC")))
    since_anchor_dt = prior_visit.get("last_visit_at") if prior_visit else None

    context = {
        "title": "Daily Review",
        # `mode` stays in the context so legacy templates that reference
        # it during the rebuild don't KeyError. Always "daily" now.
        "mode": "daily",
        "week_start": this_week,
        "week_end": week_end,
        "user_timezone": user_tz,
        "today": today,
        "accounts": user_accounts or [],
        "selected_account": selected_account,
        "error": None,
        "equity_snapshot": None,
        "today_snapshots_by_account": [],
        "today_strip": [],
        "expiring_options": [],
        "upcoming_earnings_this_week": [],
        "upcoming_earnings_next_week": [],
        "upcoming_ex_dividends": [],
        "today_movers": None,
        "after_hours_movers": None,
        "today_pulse": None,
        "today_snapshots_total": None,
        "today_headline": None,
        "from_upload": from_upload,
        "market": None,
        "market_session": None,
        "market_neutral_line": None,
        "since_last_looked": None,
        "calendar_grid": [],
        "calendar_weeks_back": DAILY_CALENDAR_WEEKS,
        "calendar_default_weeks": DAILY_CALENDAR_DEFAULT_WEEKS,
        "calendar_extra_weeks": max(0, DAILY_CALENDAR_WEEKS - DAILY_CALENDAR_DEFAULT_WEEKS),
        "daily_calendar_no_query_rows": True,
        "trades_this_week": {"trades": [], "count": 0, "opened_count": 0,
                             "closed_count": 0, "realized_pnl": 0.0,
                             "unrealized_pnl": 0.0, "has_any": False},
        "account_breakdown": {"rows": [], "totals": None, "benchmarks": []},
        "benchmark_snapshot": [],
        "community_profile_visibility": "private",
        "community_publish_ready": False,
    }

    daily_changes_map = {}

    try:
        client = get_bigquery_client()

        cal_start = this_week - timedelta(days=(DAILY_CALENDAR_WEEKS - 1) * 7)
        cal_end = this_week + timedelta(days=4)

        cal_cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", cal_start),
            bigquery.ScalarQueryParameter("end_date", "DATE", cal_end),
        ])

        week_cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("week_start", "DATE", this_week),
        ])

        # Compute the market session up front so we can skip queries whose
        # results are only meaningful once the regular session has closed.
        market_session = _us_market_session()

        batch_queries = {
            "account_value": ACCOUNT_VALUE_QUERY.format(tenant_filter=tenant_filter),
            "snapshots": TODAY_SNAPSHOT_ENRICHED_QUERY.format(tenant_filter=tenant_filter),
            "positions": OPEN_POSITIONS_QUERY.format(tenant_filter=tenant_filter),
            "calendar": (DAILY_CALENDAR_QUERY.format(tenant_filter=tenant_filter), cal_cfg),
            "earnings": EARNINGS_UPCOMING_QUERY.format(tenant_filter=tenant_filter),
            "today_moves": TODAY_MOVES_QUERY.format(tenant_filter=tenant_filter),
            "upcoming_divs": UPCOMING_DIVIDENDS_QUERY.format(tenant_filter=tenant_filter),
            "weekly_trades": (WEEKLY_TRADES_MART_QUERY.format(tenant_filter=tenant_filter), week_cfg),
            "attribution": POSITION_ATTRIBUTION_QUERY.format(
                tenant_filter=tenant_filter, week_start=this_week.isoformat()),
            "benchmark_snapshot": BENCHMARK_SNAPSHOT_QUERY,
        }
        # After-hours drift compares the broker mark to today's *official*
        # close. Two conditions must hold or the reading is noise/wrong:
        #   1) the bell has rung (state == after_hours) so the close exists;
        #   2) the broker mark itself was captured AFTER the close — else we'd
        #      compare a mid-session mark to the close and show the intraday
        #      move backwards. We have no per-row capture time in the warehouse,
        #      so we ask SnapTrade's holdings_last_successful_sync WHICH accounts
        #      are post-close and scope the query to exactly those tenants. An
        #      account that hasn't re-synced since the close (or is broken) is
        #      dropped rather than hiding the whole section for the others.
        from app.snaptrade import post_close_broker_tenant_ids
        ah_tenants = post_close_broker_tenant_ids(current_user.id)
        if tenant_ids is not None:
            # Respect the active account filter (?account= / ?tenant=).
            ah_tenants = {t for t in ah_tenants if t in set(tenant_ids)}
        after_hours_ready = (
            market_session.get("state") == "after_hours"
            and bool(ah_tenants)
        )
        if after_hours_ready:
            batch_queries["after_hours"] = AFTER_HOURS_MOVERS_QUERY.format(
                tenant_filter=_tenant_sql_and(sorted(ah_tenants)))

        try:
            batch = _bq_parallel(client, batch_queries)
        except Exception as e:
            if app.debug:
                app.logger.warning("Daily review parallel batch failed: %s", e)
            batch = {}

        # Defense in depth: every BQ DataFrame goes through the tenant
        # filter before we touch it. The SQL also carries the predicate,
        # but the rule (and 2026 incident history) says "both layers".
        for k in ("account_value", "snapshots", "positions", "calendar",
                 "today_moves", "weekly_trades", "attribution"):
            df = batch.get(k)
            if df is not None and not df.empty and "account" in df.columns:
                batch[k] = _filter_df_by_tenant_ids(df, tenant_ids)

        # Market context — neutral framing line ("SPY +1.2% · QQQ +0.8%"),
        # NOT a "you outperformed" badge (manifesto: framing, not scoring).
        context["market"] = _get_market_performance(this_week, today)
        context["market_session"] = market_session
        context["market_open_today"] = context["market_session"]["state"] == "open"
        context["market_neutral_line"] = _neutral_market_line(context.get("market"))

        # Benchmark snapshot (index 1d / 1w / 1m %) — sits under the account
        # snapshot Total so each period's account move has a market baseline.
        try:
            context["benchmark_snapshot"] = _build_benchmark_snapshot(
                batch.get("benchmark_snapshot", pd.DataFrame())
            )
        except Exception as e:
            if app.debug:
                app.logger.warning("Benchmark snapshot processing failed: %s", e)

        # ── Account value (cash / invested split) ─────────────────────
        # ``live_av_by_label`` is the per-account live total used as a
        # fallback in the snapshot placeholder fill below, so a freshly
        # connected account shows its broker balance immediately instead
        # of "—" until the daily-snapshot mart catches up. Keyed by the
        # SAME disambiguated label the placeholder rows use (tenant_label_map)
        # so the lookup matches.
        live_av_by_label = {}
        try:
            from app.routes import _tenant_label_map_for_user
            _tenant_label_map = _tenant_label_map_for_user(current_user.id)
        except Exception:
            _tenant_label_map = {}
        try:
            av_df = batch.get("account_value", pd.DataFrame())
            if not av_df.empty:
                total_account_value = 0.0
                total_cash = 0.0
                for _, row in av_df.iterrows():
                    av = float(row.get("account_value", 0) or 0)
                    cb = float(row.get("cash_balance", 0) or 0)
                    total_account_value += av
                    total_cash += cb
                    tid = row.get("tenant_id")
                    label = _tenant_label_map.get(tid) or row.get("account") or tid
                    if label is not None:
                        live_av_by_label[label] = live_av_by_label.get(label, 0.0) + av
                invested_value = total_account_value - total_cash
                pct_invested = round(invested_value / total_account_value * 100, 1) if total_account_value > 0 else None
                context["equity_snapshot"] = {
                    "account_value": total_account_value,
                    "cash_balance": total_cash,
                    "invested_value": invested_value,
                    "pct_invested": pct_invested,
                }
        except Exception as e:
            if app.debug:
                app.logger.warning("Equity snapshot failed: %s", e)

        # ── Account snapshots per account (day / week / month deltas) ─
        try:
            snap_df = batch.get("snapshots", pd.DataFrame())
            seen_accounts = set()
            if not snap_df.empty and "date" in snap_df.columns and "tenant_id" in snap_df.columns:
                if hasattr(snap_df["date"].iloc[0], "date"):
                    snap_df["date"] = snap_df["date"].dt.date
                elif snap_df["date"].dtype == object:
                    snap_df["date"] = pd.to_datetime(snap_df["date"]).dt.date

                # v2: group by the broker-stable ``tenant_id`` (not the
                # display ``account`` label) so several physical accounts
                # that share a label — e.g. multiple "Schwab Account"s —
                # render as distinct rows. The display label is resolved
                # per tenant via the disambiguating label map (nickname >
                # broker label, with a stable suffix when labels collide).
                tenant_label_map = _tenant_label_map

                def _round_opt(val):
                    if val is None or (hasattr(val, "__float__") and pd.isna(val)):
                        return None
                    try:
                        return round(float(val), 2)
                    except (TypeError, ValueError):
                        return None

                latest_per_account = (
                    snap_df.sort_values("date", ascending=False)
                    .groupby("tenant_id").first().reset_index()
                )
                for _, row in latest_per_account.iterrows():
                    tid = row["tenant_id"]
                    acct = tenant_label_map.get(tid) or row.get("account") or tid
                    seen_accounts.add(acct)
                    today_date = row["date"]
                    today_value = float(row.get("account_value") or 0)
                    comps = {
                        "day": {
                            "label": "vs 1 day ago",
                            "base_date": row.get("base_1d_date"),
                            "delta": _round_opt(row.get("delta_1d")),
                            "delta_pct": _round_opt(row.get("delta_1d_pct")),
                            "has_data": row.get("base_1d_value") is not None and pd.notna(row.get("base_1d_value")),
                        },
                        "week": {
                            "label": "vs 1 week ago",
                            "base_date": row.get("base_1w_date"),
                            "delta": _round_opt(row.get("delta_1w")),
                            "delta_pct": _round_opt(row.get("delta_1w_pct")),
                            "has_data": row.get("base_1w_value") is not None and pd.notna(row.get("base_1w_value")),
                        },
                        "month": {
                            "label": "vs 1 month ago",
                            "base_date": row.get("base_1m_date"),
                            "delta": _round_opt(row.get("delta_1m")),
                            "delta_pct": _round_opt(row.get("delta_1m_pct")),
                            "has_data": row.get("base_1m_value") is not None and pd.notna(row.get("base_1m_value")),
                        },
                    }
                    # vs week start: derived from week comparison's base_date matching this Monday
                    vs_week_start = {"delta": None, "delta_pct": None, "has_data": False, "base_date": this_week}
                    context["today_snapshots_by_account"].append({
                        "account": acct,
                        # Carry tenant_id so the Account snapshot table can
                        # deep-link the account name to /accounts?tenant=<id>,
                        # mirroring the Performance by Account rows.
                        "tenant_id": tid,
                        "today_value": today_value,
                        "today_date": today_date,
                        "today_is_live": False,
                        "comparisons": comps,
                        "vs_week_start": vs_week_start,
                    })

            # Fill placeholder rows for any account with no snapshot row yet.
            # ``seen_accounts`` holds display labels from the mart's ``account``
            # column ("Alpaca Paper Account", "Interactive Brokers ••••7930"),
            # so the placeholder fill must iterate display labels too.
            # Iterating raw ``tenant_ids`` ("snaptrade:<uuid>") would never
            # match and would produce phantom rows beside the real ones —
            # the bug visible on Daily Review when two accounts rendered
            # as four rows (two raw tenant_id labels + two real labels).
            display_labels = user_accounts or []
            # Reverse map (disambiguated label -> tenant_id) for deep-linking
            # placeholder rows. _tenant_label_map values are unique by
            # construction (colliding broker labels get a ••<uuid tail>
            # suffix), so this inversion is safe.
            label_to_tid = {v: k for k, v in (_tenant_label_map or {}).items()}
            if display_labels:
                for label in display_labels:
                    if label not in seen_accounts:
                        # Fallback to the live broker balance so a freshly
                        # connected account shows its value immediately
                        # instead of "—" until the snapshot mart captures
                        # its first daily row. Deltas stay blank (no history
                        # yet); today_is_live flags the UI to label it.
                        live_val = live_av_by_label.get(label)
                        context["today_snapshots_by_account"].append({
                            "account": label,
                            "tenant_id": label_to_tid.get(label),
                            "today_value": live_val,
                            "today_date": None,
                            "today_is_live": live_val is not None,
                            "comparisons": {
                                "day": {"base_date": None, "delta": None, "delta_pct": None, "has_data": False},
                                "week": {"base_date": None, "delta": None, "delta_pct": None, "has_data": False},
                                "month": {"base_date": None, "delta": None, "delta_pct": None, "has_data": False},
                            },
                            "vs_week_start": {"delta": None, "delta_pct": None, "has_data": False, "base_date": this_week},
                        })
                acct_order = {a: i for i, a in enumerate(display_labels)}
                context["today_snapshots_by_account"].sort(
                    key=lambda s: acct_order.get(s["account"], 999)
                )
        except Exception as e:
            if app.debug:
                app.logger.warning("Snapshots processing failed: %s", e)

        # ── Open positions: today strip + expiring options ────────────
        try:
            all_pos_df = batch.get("positions", pd.DataFrame())
            if not all_pos_df.empty:
                for col in ["market_value", "cost_basis", "unrealized_pnl", "unrealized_pnl_pct",
                             "current_price", "quantity", "option_strike", "latest_stock_price"]:
                    if col in all_pos_df.columns:
                        all_pos_df[col] = pd.to_numeric(all_pos_df[col], errors="coerce").fillna(0)

                symbols_agg = all_pos_df.groupby("symbol").agg(
                    total_mv=("market_value", "sum"),
                    total_cost=("cost_basis", "sum"),
                    total_upnl=("unrealized_pnl", "sum"),
                    num_legs=("trade_symbol", "count"),
                ).reset_index()

                eq_rows = all_pos_df[all_pos_df["instrument_type"] == "Equity"]
                eq_prices = dict(zip(eq_rows["symbol"], eq_rows["current_price"])) if not eq_rows.empty else {}
                stock_prices = dict(zip(all_pos_df["symbol"], all_pos_df["latest_stock_price"]))
                for sym in stock_prices:
                    if sym not in eq_prices and stock_prices[sym]:
                        eq_prices[sym] = stock_prices[sym]

                for _, row in symbols_agg.iterrows():
                    sym = row["symbol"]
                    mv = float(row["total_mv"])
                    cost = float(row["total_cost"])
                    upnl = float(row["total_upnl"])
                    upnl_pct = round(upnl / cost * 100, 1) if cost else None
                    context["today_strip"].append({
                        "symbol": sym,
                        "market_value": round(mv, 2),
                        "unrealized_pnl": round(upnl, 2),
                        "unrealized_pnl_pct": upnl_pct,
                        "price": round(eq_prices.get(sym, 0), 2) if eq_prices.get(sym) else None,
                        "num_legs": int(row["num_legs"]),
                    })
                context["today_strip"].sort(key=lambda x: abs(x["market_value"]), reverse=True)

                opts = all_pos_df[all_pos_df["instrument_type"].isin(["Call", "Put"])].copy()
                if not opts.empty and "option_expiry" in opts.columns:
                    opts["option_expiry"] = pd.to_datetime(opts["option_expiry"])
                    # 14 day window for the daily review's "expiring soon" — a
                    # weekly review's 7d hides the next-Mon expirations from a
                    # trader checking on a Wednesday.
                    expiry_cutoff = pd.Timestamp(today + timedelta(days=14))
                    expiring = opts[opts["option_expiry"].notna()
                                    & (opts["option_expiry"] <= expiry_cutoff)]

                    for _, row in expiring.iterrows():
                        sym = str(row.get("symbol", ""))
                        strike = float(row.get("option_strike") or 0)
                        opt_type = str(row.get("option_type") or "")
                        stock_price = float(eq_prices.get(sym, 0)) or float(row.get("latest_stock_price") or 0)
                        expiry = row.get("option_expiry")
                        expiry_str = expiry.strftime("%Y-%m-%d") if hasattr(expiry, "strftime") else str(expiry)[:10]
                        days_to_exp = (expiry.date() - today).days if hasattr(expiry, "date") else None

                        itm, distance = _classify_expiring_moneyness(
                            instrument_type=row.get("instrument_type"),
                            option_type=opt_type,
                            stock_price=stock_price,
                            strike=strike,
                        )
                        context["expiring_options"].append({
                            "symbol": sym,
                            "trade_symbol": str(row.get("trade_symbol", "")),
                            "instrument_type": row.get("instrument_type"),
                            "option_type": opt_type,
                            "strike": strike,
                            "expiry": expiry_str,
                            "days_to_exp": days_to_exp,
                            "quantity": int(row.get("quantity") or 0),
                            "market_value": round(float(row.get("market_value") or 0), 2),
                            "unrealized_pnl": round(float(row.get("unrealized_pnl") or 0), 2),
                            "stock_price": round(stock_price, 2),
                            "itm": itm,
                            "distance": distance,
                        })
                    context["expiring_options"].sort(
                        key=lambda x: (x.get("days_to_exp") if x.get("days_to_exp") is not None else 999)
                    )
        except Exception as e:
            if app.debug:
                app.logger.warning("Open positions processing failed: %s", e)

        # ── Upcoming earnings ─────────────────────────────────────────
        try:
            earn_df = batch.get("earnings", pd.DataFrame())
            if not earn_df.empty:
                for _, row in earn_df.iterrows():
                    ed = row.get("next_earnings_date")
                    if ed is None or (hasattr(ed, "__float__") and pd.isna(ed)):
                        continue
                    ed_date = ed.date() if hasattr(ed, "date") and not isinstance(ed, date) else ed
                    try:
                        days_until = int(row.get("days_until")) if row.get("days_until") is not None else None
                    except (TypeError, ValueError):
                        days_until = None
                    item = {
                        "symbol": str(row["symbol"]),
                        "company": str(row.get("long_name") or ""),
                        "sector": str(row.get("sector") or "") if row.get("sector") not in (None, "Unknown") else "",
                        "subsector": str(row.get("subsector") or "") if row.get("subsector") not in (None, "Unknown") else "",
                        "earnings_date": ed_date.strftime("%Y-%m-%d") if hasattr(ed_date, "strftime") else str(ed_date)[:10],
                        "earnings_date_display": ed_date.strftime("%a %b %-d") if hasattr(ed_date, "strftime") else str(ed_date)[:10],
                        "days_until": days_until,
                    }
                    if days_until is not None and days_until <= 7:
                        context["upcoming_earnings_this_week"].append(item)
                    else:
                        context["upcoming_earnings_next_week"].append(item)
        except Exception as e:
            if app.debug:
                app.logger.warning("Earnings processing failed: %s", e)

        # ── Today's stock movers (held symbols) ───────────────────────
        try:
            tm_df = batch.get("today_moves", pd.DataFrame())
            context["today_movers"] = _build_today_movers(tm_df, account_total_value=
                (context.get("equity_snapshot") or {}).get("account_value"))
        except Exception as e:
            if app.debug:
                app.logger.warning("Today movers processing failed: %s", e)

        # ── After-hours movers (broker mark vs official close) ─────────
        # Only built when after_hours_ready (bell has rung AND the broker mark
        # is post-close per SnapTrade's holdings_last_successful_sync). Before
        # the close, and whenever the last broker sync predates the close, the
        # "drift vs close" is noise or the intraday move shown backwards, so
        # the query wasn't run and we leave the section hidden.
        try:
            if after_hours_ready:
                ah_df = batch.get("after_hours", pd.DataFrame())
                context["after_hours_movers"] = _build_after_hours_movers(ah_df)
            else:
                context["after_hours_movers"] = None
        except Exception as e:
            if app.debug:
                app.logger.warning("After-hours movers processing failed: %s", e)

        # ── Projected ex-dividend dates ───────────────────────────────
        try:
            ud_df = batch.get("upcoming_divs", pd.DataFrame())
            context["upcoming_ex_dividends"] = _build_upcoming_dividends(ud_df)
        except Exception as e:
            if app.debug:
                app.logger.warning("Upcoming ex-div processing failed: %s", e)

        # ── Trades opened / closed this week ──────────────────────────
        try:
            wt_df = batch.get("weekly_trades", pd.DataFrame())
            label_map = _tenant_label_map_for_user(current_user.id)
            context["trades_this_week"] = _build_trades_this_week(
                wt_df, this_week, week_end, label_map=label_map,
            )
        except Exception as e:
            if app.debug:
                app.logger.warning("Trades-this-week processing failed: %s", e)

        # ── Performance by account (summarized scorecard) ─────────────
        try:
            attr_df = batch.get("attribution", pd.DataFrame())
            label_map = _tenant_label_map_for_user(current_user.id)
            ab = _build_account_breakdown(
                attr_df, label_map=label_map, week_start=this_week,
            )
            # Benchmark "did I beat the index?" rows: index return over the
            # SAME holding window on the SAME capital, rendered under the
            # totals line. One extra small market-data query (window depends
            # on the attribution result, so it can't join the parallel batch).
            basis = ab.get("basis")
            if basis and basis.get("days"):
                bench_start = today - timedelta(days=int(basis["days"]))
                ab["benchmarks"] = _build_benchmark_rows(
                    basis, _get_benchmark_returns(bench_start)
                )
                ab["benchmark_start"] = bench_start
            context["account_breakdown"] = ab
        except Exception as e:
            if app.debug:
                app.logger.warning("Account breakdown processing failed: %s", e)

        # ── Since you last looked (daily-pull diff) ───────────────────
        try:
            context["since_last_looked"] = _since_last_looked(
                client=client,
                tenant_filter=tenant_filter,
                prev_visit_dt=since_anchor_dt,
                today=today,
                today_strip=context.get("today_strip", []),
                expiring_options=context.get("expiring_options", []),
                user_tz=user_tz,
                force_show=from_upload or from_sync,
            )
        except Exception as e:
            if app.debug:
                app.logger.warning("Since-last-looked failed: %s", e)

        # ── Daily account Δ calendar grid ─────────────────────────────
        try:
            cal_df = batch.get("calendar", pd.DataFrame())
            if not cal_df.empty:
                for col in ["account_value", "daily_change"]:
                    if col in cal_df.columns:
                        cal_df[col] = pd.to_numeric(cal_df[col], errors="coerce").fillna(0)
                if "date" in cal_df.columns:
                    cal_df["date"] = pd.to_datetime(cal_df["date"]).dt.date
                for _, row in cal_df.iterrows():
                    daily_changes_map[row["date"]] = round(float(row.get("daily_change") or 0), 2)

            context["daily_calendar_no_query_rows"] = cal_df.empty
            context["calendar_grid"] = _build_calendar_grid(daily_changes_map, today)
        except Exception as e:
            if app.debug:
                app.logger.warning("Calendar grid failed: %s", e)
            context["daily_calendar_no_query_rows"] = True
            context["calendar_grid"] = _build_calendar_grid({}, today)

    except Exception as e:
        context["error"] = str(e)

    if context.get("market_session") is None:
        context["market_session"] = _us_market_session()
        context["market_open_today"] = context["market_session"]["state"] == "open"

    context["today_pulse"] = _today_pulse(context.get("today_snapshots_by_account", []))
    context["today_snapshots_total"] = _today_totals(context.get("today_snapshots_by_account", []))

    # Hero "total" must anchor on the SAME close-based account value the
    # Account snapshot table totals to. Otherwise the page shows two
    # different totals: equity_snapshot.account_value is the LIVE broker
    # balance (latest sync — an after-hours mark when the page is opened
    # after the bell), while the snapshot table sums the close-based daily
    # snapshot. Close-based wins for the core number (pricing-precedence
    # rule: after-hours drift is surfaced only in After-hours movers, never
    # in core numbers). Falls back to the live equity_snapshot when no daily
    # snapshot exists yet (cold-start / freshly connected account).
    _snaps = context.get("today_snapshots_by_account") or []
    _total_row = context.get("today_snapshots_total")
    _hero_av = None
    if _total_row and _total_row.get("today_value") is not None:
        _hero_av = _total_row["today_value"]
    elif len(_snaps) == 1 and _snaps[0].get("today_value") is not None:
        _hero_av = _snaps[0]["today_value"]
    if _hero_av is None:
        _hero_av = (context.get("equity_snapshot") or {}).get("account_value")
    context["hero_account_value"] = _hero_av

    context["today_headline"] = _today_headline(
        context.get("today_pulse"),
        context.get("today_movers"),
        context.get("equity_snapshot"),
    )

    _prof = get_user_profile(current_user.id)
    _vis = (_prof.get("profile_visibility") or "private").lower()
    context["community_profile_visibility"] = _vis
    context["community_publish_ready"] = _vis in ("followers", "public")

    # NB: "Broker data as of" is now a GLOBAL freshness strip (base.html, fed
    # by `_inject_broker_data_freshness` in app/snaptrade.py) so it shows on
    # every page — no per-page context needed here.

    return render_template("weekly_review.html", **context)
