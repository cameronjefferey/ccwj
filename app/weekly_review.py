"""
Weekly Review — temporal hub with three modes:

  Friday Review   → "What happened this week?"
  Monday Check    → "How am I showing up?"
  Mid-Week Check  → "Am I deviating?"

Reads pre-aggregated data from mart_weekly_summary where possible.
"""
from datetime import date, timedelta
from flask import render_template, request
from flask_login import login_required, current_user
from app import app
from app.bigquery_client import get_bigquery_client
from app.models import (
    get_accounts_for_user, is_admin,
    get_mirror_score_for_user, get_mirror_score_history,
    get_insight_for_user,
)
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor
import pandas as pd


def _bq_parallel(client, queries):
    """Run multiple BigQuery queries in parallel. Returns dict of {name: DataFrame}."""
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
    if is_admin(current_user.username):
        return None
    return get_accounts_for_user(current_user.id)


def _account_sql_and(accounts):
    if accounts is None:
        return ""
    if not accounts:
        return "AND 1 = 0"
    quoted = ", ".join(f"'{a.replace(chr(39), chr(39)+chr(39))}'" for a in accounts)
    return f"AND account IN ({quoted})"


def _account_sql_where(accounts):
    """Build a SQL WHERE clause for queries that have no prior WHERE."""
    if accounts is None:
        return ""
    if not accounts:
        return "WHERE 1 = 0"
    quoted = ", ".join(f"'{a.replace(chr(39), chr(39)+chr(39))}'" for a in accounts)
    return f"WHERE account IN ({quoted})"


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


WEEKLY_SUMMARY_COMBINED_QUERY = """
SELECT *
FROM `ccwj-dbt.analytics.mart_weekly_summary`
WHERE week_start IN UNNEST(@week_starts)
  {account_filter}
"""

LATEST_ACTIVE_WEEK_QUERY = """
SELECT MAX(week_start) AS latest_week
FROM `ccwj-dbt.analytics.mart_weekly_summary`
WHERE (trades_closed > 0 OR trades_opened > 0)
  {account_filter}
"""

# Total account value (from current positions snapshot) for context and return % vs account
ACCOUNT_VALUE_QUERY = """
SELECT
  COALESCE(SUM(CASE WHEN row_type = 'account_total' THEN market_value ELSE 0 END), 0) AS account_value,
  COALESCE(SUM(CASE WHEN row_type = 'cash' THEN market_value ELSE 0 END), 0) AS cash_balance
FROM `ccwj-dbt.analytics.stg_account_balances`
WHERE 1=1 {account_filter}
"""

# Weekly account return from dbt mart (replaces inline WEEKLY_ACCOUNT_CHANGE_QUERY)
WEEKLY_RETURNS_QUERY = """
SELECT account, start_value, end_value, weekly_return_pct
FROM `ccwj-dbt.analytics.mart_account_weekly_returns`
WHERE week_start = @week_start
  {account_filter}
"""

# Today's snapshot: per-account enriched rows; Flask aggregates by date for user's accounts
TODAY_SNAPSHOT_ENRICHED_QUERY = """
SELECT account, date, account_value,
  base_1d_date, base_1d_value, delta_1d, delta_1d_pct,
  base_1w_date, base_1w_value, delta_1w, delta_1w_pct,
  base_1m_date, base_1m_value, delta_1m, delta_1m_pct
FROM `ccwj-dbt.analytics.mart_account_snapshots_enriched`
WHERE 1=1 {account_filter}
ORDER BY date DESC
"""

# Trades this week from dbt mart (replaces TRADES_THIS_WEEK_QUERY + Python cost/value calc)
WEEKLY_TRADES_MART_QUERY = """
SELECT
  account,
  symbol,
  strategy,
  trade_symbol,
  open_date,
  close_date,
  status,
  trade_cost,
  current_market_value,
  current_unrealized_pnl,
  total_pnl
FROM `ccwj-dbt.analytics.mart_weekly_trades`
WHERE week_start = @week_start
  {account_filter}
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
  {account_filter}
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
  {account_filter}
GROUP BY 1, 2, 3
ORDER BY exposure DESC
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
  {account_filter}
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
      {account_filter}
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

# Daily P&L calendar: account value changes for the current month
DAILY_CALENDAR_QUERY = """
SELECT
    date,
    SUM(account_value) AS account_value,
    SUM(delta_1d) AS daily_change
FROM `ccwj-dbt.analytics.mart_account_snapshots_enriched`
WHERE date >= @month_start
  AND date <= @month_end
  {account_filter}
GROUP BY date
ORDER BY date
"""

# Trades opened this week for Mid-Week Check
OPENS_THIS_WEEK_QUERY = """
SELECT
    c.account, c.symbol, c.strategy, c.open_date
FROM `ccwj-dbt.analytics.int_strategy_classification` c
WHERE c.open_date >= @start_date
  AND c.open_date <= @end_date
  {account_filter}
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
  {account_filter}
GROUP BY strategy
ORDER BY total_pnl DESC
"""

PATTERNS_COMBINED_QUERY = """
WITH streak AS (
    SELECT streak_type, streak_length, week_pnl
    FROM `ccwj-dbt.analytics.mart_weekly_streaks`
    WHERE week_start = @week_start
      {account_filter}
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
    WHERE 1=1 {account_filter}
),
loss_cluster_week AS (
    SELECT
        COUNTIF(is_post_loss) AS week_post_loss_count,
        COUNT(*) AS week_trades
    FROM `ccwj-dbt.analytics.int_trade_sequence`
    WHERE close_date >= @week_start
      AND close_date <= @week_end
      {account_filter}
),
dte_sensitivity AS (
    SELECT
        dte_bucket,
        SUM(num_trades) AS num_trades,
        SUM(CASE WHEN outcome = 'Winner' THEN num_trades ELSE 0 END) AS winners,
        SUM(CASE WHEN outcome = 'Loser' THEN num_trades ELSE 0 END) AS losers,
        SUM(total_pnl) AS total_pnl
    FROM `ccwj-dbt.analytics.mart_option_trades_by_kind`
    WHERE 1=1 {account_filter}
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
  {account_filter}
ORDER BY pnl_given_back DESC
"""

COACHING_SIGNALS_WEEKLY_QUERY = """
SELECT
    strategy,
    total_closed, reliable_contracts, pct_contracts_reliable,
    avg_giveback_pct, avg_days_held_past_peak,
    total_pnl_given_back, optimal_exit_rate,
    num_rolls, avg_dte_at_roll, roll_success_rate,
    early_roll_success_rate, late_roll_success_rate,
    rolls_early, rolls_late
FROM `ccwj-dbt.analytics.mart_coaching_signals`
{where}
ORDER BY total_pnl_given_back DESC
"""


def _detect_patterns(client, account_filter, week_start, week_end):
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
            PATTERNS_COMBINED_QUERY.format(account_filter=account_filter),
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


def _build_narrative(mode, review, prev_review, behavior_mirror, market,
                     today, week_start, trading_days=0, market_open_today=True):
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
        try:
            days_in = max(1, (today - week_start).days + 1)
        except TypeError:
            days_in = 1
        td_label = f"{trading_days} trading day{'s' if trading_days != 1 else ''}" if trading_days else f"Day {days_in}"
        if trades_closed > 0:
            sign = "+" if total_pnl >= 0 else ""
            headline = (
                f"{td_label} \u00b7 {trades_closed} closed \u00b7 "
                f"{sign}${abs(total_pnl):,.0f} so far"
            )
        else:
            headline = f"{td_label} \u00b7 No closed trades yet"
        subtitle = "Mid-week check. Are you trading your plan, or reacting to the market?"
        if not market_open_today:
            subtitle = "Market closed today. " + subtitle
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


def _iso_week_start(d):
    """Return Monday of the ISO week containing d."""
    return d - timedelta(days=d.weekday())


def _auto_mode():
    """Auto-detect mode from day of week."""
    dow = date.today().weekday()
    if dow == 0:
        return "monday"
    if dow >= 4:
        return "friday"
    return "midweek"


def _aggregate_weekly_rows(rows):
    """Aggregate multiple account rows from mart_weekly_summary into one summary."""
    if not rows:
        return None
    summary = {
        "trades_closed": sum(r.get("trades_closed", 0) for r in rows),
        "total_pnl": sum(r.get("total_pnl", 0) for r in rows),
        "num_winners": sum(r.get("num_winners", 0) for r in rows),
        "num_losers": sum(r.get("num_losers", 0) for r in rows),
        "premium_received": sum(r.get("premium_received", 0) for r in rows),
        "premium_paid": sum(r.get("premium_paid", 0) for r in rows),
        "trades_opened": sum(r.get("trades_opened", 0) for r in rows),
    }

    # Best trade: highest PnL across accounts
    best_candidates = [r for r in rows if r.get("best_pnl") is not None]
    if best_candidates:
        best = max(best_candidates, key=lambda r: float(r.get("best_pnl", 0)))
        summary["best_trade"] = {
            "symbol": best.get("best_symbol", ""),
            "strategy": best.get("best_strategy", ""),
            "trade_symbol": best.get("best_trade_symbol", ""),
            "total_pnl": float(best.get("best_pnl", 0)),
            "close_date": str(best.get("best_close_date", "")),
            "account": best.get("account", ""),
        }
    else:
        summary["best_trade"] = None

    # Worst trade: lowest PnL across accounts
    worst_candidates = [r for r in rows if r.get("worst_pnl") is not None]
    if worst_candidates:
        worst = min(worst_candidates, key=lambda r: float(r.get("worst_pnl", 0)))
        summary["worst_trade"] = {
            "symbol": worst.get("worst_symbol", ""),
            "strategy": worst.get("worst_strategy", ""),
            "trade_symbol": worst.get("worst_trade_symbol", ""),
            "total_pnl": float(worst.get("worst_pnl", 0)),
            "close_date": str(worst.get("worst_close_date", "")),
            "account": worst.get("account", ""),
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


@app.route("/weekly-review")
@login_required
def weekly_review():
    """Temporal hub: Friday Review / Monday Risk Check / Mid-Week Check-In."""
    user_accounts = _user_account_list()

    # Optional: focus on a single account (multi-account support)
    selected_account = request.args.get("account", "")
    effective_accounts = user_accounts
    if selected_account:
        if user_accounts is None:
            # Admin: allow ad-hoc focus on a single account
            effective_accounts = [selected_account]
        else:
            # Regular user: only allow accounts they actually own
            effective_accounts = [a for a in user_accounts if a == selected_account] or user_accounts

    account_filter = _account_sql_and(effective_accounts)

    mode = request.args.get("mode") or _auto_mode()
    if mode not in ("friday", "monday", "midweek"):
        mode = _auto_mode()

    from_upload = request.args.get("from_upload") == "1"

    today = date.today()
    this_week = _iso_week_start(today)

    context = {
        "title": "Weekly Review",
        "mode": mode,
        "week_start": this_week,
        "week_end": this_week + timedelta(days=6),
        "accounts": user_accounts or [],
        "selected_account": selected_account,
        "error": None,
        "review": None,
        "prev_review": None,
        "exposure": None,
        "mirror_score": None,
        "mirror_history": None,
        "opens_this_week": None,
        "ai_insight": None,
        "is_backfilled": False,
        "market": None,
        "equity_snapshot": None,
        "trades_this_week": [],
        "from_upload": from_upload,
        "strategy_breakdown_week": [],
    }

    try:
        client = get_bigquery_client()

        prev_week = this_week - timedelta(days=7)

        # Fetch this week + prev week from mart_weekly_summary in a single query
        combined_cfg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("week_starts", "DATE", [this_week, prev_week]),
            ]
        )
        combined_df = client.query(
            WEEKLY_SUMMARY_COMBINED_QUERY.format(account_filter=account_filter),
            job_config=combined_cfg,
        ).to_dataframe()

        if not combined_df.empty and "week_start" in combined_df.columns:
            combined_df["week_start"] = pd.to_datetime(combined_df["week_start"]).dt.date
            this_rows = combined_df[combined_df["week_start"] == this_week]
            has_current = not this_rows.empty and (
                int(this_rows.iloc[0].get("trades_closed", 0) or 0)
                + int(this_rows.iloc[0].get("trades_opened", 0) or 0) > 0
            )
        else:
            this_rows = pd.DataFrame()
            has_current = False

        if not has_current:
            latest_df = client.query(
                LATEST_ACTIVE_WEEK_QUERY.format(account_filter=account_filter)
            ).to_dataframe()
            if not latest_df.empty and pd.notna(latest_df.iloc[0]["latest_week"]):
                lw = latest_df.iloc[0]["latest_week"]
                if hasattr(lw, "date"):
                    lw = lw.date()
                elif not isinstance(lw, date):
                    lw = date.fromisoformat(str(lw)[:10])
                this_week = lw
                prev_week = this_week - timedelta(days=7)
                context["week_start"] = this_week
                context["week_end"] = this_week + timedelta(days=6)
                context["is_backfilled"] = True

                # Re-fetch for the corrected week pair
                combined_cfg2 = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter("week_starts", "DATE", [this_week, prev_week]),
                    ]
                )
                combined_df = client.query(
                    WEEKLY_SUMMARY_COMBINED_QUERY.format(account_filter=account_filter),
                    job_config=combined_cfg2,
                ).to_dataframe()
                if not combined_df.empty and "week_start" in combined_df.columns:
                    combined_df["week_start"] = pd.to_datetime(combined_df["week_start"]).dt.date

        # Split combined result into this_week and prev_week
        if not combined_df.empty and "week_start" in combined_df.columns:
            for target_week, key in [(this_week, "review"), (prev_week, "prev_review")]:
                subset = combined_df[combined_df["week_start"] == target_week]
                if not subset.empty:
                    context[key] = _aggregate_weekly_rows(subset.to_dict(orient="records"))

        week_end = this_week + timedelta(days=6)

        # ── Parallel batch: all independent queries after week is determined ──
        month_start = today.replace(day=1)
        if today.month == 12:
            month_end_date = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            month_end_date = today.replace(month=today.month + 1, day=1) - timedelta(days=1)

        week_cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("week_start", "DATE", this_week)]
        )
        week_range_cfg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", this_week),
                bigquery.ScalarQueryParameter("end_date", "DATE", week_end),
            ]
        )
        cal_cfg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("month_start", "DATE", month_start),
                bigquery.ScalarQueryParameter("month_end", "DATE", month_end_date),
            ]
        )

        try:
            batch = _bq_parallel(client, {
                "account_value": ACCOUNT_VALUE_QUERY.format(account_filter=account_filter),
                "returns": (WEEKLY_RETURNS_QUERY.format(account_filter=account_filter), week_cfg),
                "snapshots": TODAY_SNAPSHOT_ENRICHED_QUERY.format(account_filter=account_filter),
                "behavior": (WEEKLY_BEHAVIOR_QUERY.format(account_filter=account_filter),
                             bigquery.QueryJobConfig(query_parameters=[
                                 bigquery.ScalarQueryParameter("week_start", "DATE", this_week)])),
                "strategy": (WEEKLY_STRATEGY_BREAKDOWN_QUERY.format(account_filter=account_filter), week_range_cfg),
                "trades": (WEEKLY_TRADES_MART_QUERY.format(account_filter=account_filter),
                           bigquery.QueryJobConfig(query_parameters=[
                               bigquery.ScalarQueryParameter("week_start", "DATE", this_week)])),
                "positions": OPEN_POSITIONS_QUERY.format(account_filter=account_filter),
                "stock_moves": (WEEKLY_STOCK_MOVEMENT_QUERY.format(account_filter=account_filter), week_range_cfg),
                "calendar": (DAILY_CALENDAR_QUERY.format(account_filter=account_filter), cal_cfg),
                "trading_days": (TRADING_DAYS_QUERY, week_range_cfg),
            })
        except Exception as e:
            if app.debug:
                app.logger.warning("Weekly review parallel batch failed: %s", e)
            batch = {}

        # Market performance (SPY, QQQ) for comparison
        context["market"] = _get_market_performance(this_week, today)

        # ── Process: Trading days & market status ──
        try:
            td_df = batch.get("trading_days", pd.DataFrame())
            if not td_df.empty:
                row = td_df.iloc[0]
                context["trading_days"] = int(row.get("trading_days", 0) or 0)
                last_td = row.get("last_trading_date")
                if last_td is not None and not isinstance(last_td, date):
                    last_td = date.fromisoformat(str(last_td)[:10])
                context["market_open_today"] = (last_td == today) if last_td else False
            else:
                context["trading_days"] = 0
                context["market_open_today"] = False
        except Exception:
            context["trading_days"] = 0
            context["market_open_today"] = True

        # ── Process: Account value ──
        try:
            av_df = batch.get("account_value", pd.DataFrame())
            if not av_df.empty:
                row = av_df.iloc[0]
                account_value = float(row.get("account_value", 0) or 0)
                cash_balance = float(row.get("cash_balance", 0) or 0)
                invested_value = account_value - cash_balance
                pct_invested = None
                if account_value > 0:
                    pct_invested = round(invested_value / account_value * 100, 1)
                context["equity_snapshot"] = {
                    "account_value": account_value,
                    "cash_balance": cash_balance,
                    "invested_value": invested_value,
                    "pct_invested": pct_invested,
                }
        except Exception as e:
            if app.debug:
                app.logger.warning("Equity snapshot processing failed: %s", e)

        # ── Process: Weekly returns ──
        start_value_by_account = {}
        ret_df = batch.get("returns", pd.DataFrame())
        try:
            if not ret_df.empty and "account" in ret_df.columns and "start_value" in ret_df.columns:
                for _, r in ret_df.iterrows():
                    start_value_by_account[str(r["account"])] = float(r.get("start_value") or 0)
        except Exception as e:
            if app.debug:
                app.logger.warning("Weekly returns processing failed: %s", e)

        # ── Process: Today's snapshot ──
        context["today_snapshots_by_account"] = []
        try:
            snap_df = batch.get("snapshots", pd.DataFrame())
            if not snap_df.empty and "date" in snap_df.columns and "account" in snap_df.columns:
                if hasattr(snap_df["date"].iloc[0], "date"):
                    snap_df["date"] = snap_df["date"].dt.date
                elif snap_df["date"].dtype == object:
                    snap_df["date"] = pd.to_datetime(snap_df["date"]).dt.date

                def _round_opt(val):
                    if val is None or (hasattr(val, "__float__") and pd.isna(val)):
                        return None
                    try:
                        return round(float(val), 2)
                    except (TypeError, ValueError):
                        return None

                # Latest row per account (query is ORDER BY date DESC, so first row per account is latest)
                latest_per_account = snap_df.sort_values("date", ascending=False).groupby("account").first().reset_index()
                seen_accounts = set()
                for _, row in latest_per_account.iterrows():
                    acct = row["account"]
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
                    # Compared to start of week (same week as mart_account_weekly_returns; $ and %)
                    vs_week_start = None
                    start_val = start_value_by_account.get(str(acct))
                    if start_val is not None and start_val > 0 and today_value is not None:
                        delta_week = round(today_value - start_val, 2)
                        delta_week_pct = round((today_value - start_val) / start_val * 100, 2)
                        vs_week_start = {
                            "delta": delta_week,
                            "delta_pct": delta_week_pct,
                            "has_data": True,
                            "base_date": this_week,
                        }
                    elif start_val is not None and start_val == 0 and today_value is not None:
                        vs_week_start = {
                            "delta": round(today_value, 2),
                            "delta_pct": None,
                            "has_data": True,
                            "base_date": this_week,
                        }
                    if vs_week_start is None:
                        vs_week_start = {"delta": None, "delta_pct": None, "has_data": False, "base_date": this_week}

                    context["today_snapshots_by_account"].append({
                        "account": acct,
                        "today_value": today_value,
                        "today_date": today_date,
                        "comparisons": comps,
                        "vs_week_start": vs_week_start,
                    })

                # One row per account: add placeholder rows for any account with no snapshot yet
                if effective_accounts:
                    for acct in effective_accounts:
                        if acct not in seen_accounts:
                            context["today_snapshots_by_account"].append({
                                "account": acct,
                                "today_value": None,
                                "today_date": None,
                                "comparisons": {
                                    "day": {"base_date": None, "delta": None, "delta_pct": None, "has_data": False},
                                    "week": {"base_date": None, "delta": None, "delta_pct": None, "has_data": False},
                                    "month": {"base_date": None, "delta": None, "delta_pct": None, "has_data": False},
                                },
                                "vs_week_start": {"delta": None, "delta_pct": None, "has_data": False, "base_date": this_week},
                            })
                    # Keep row order consistent with account list
                    acct_order = {a: i for i, a in enumerate(effective_accounts)}
                    context["today_snapshots_by_account"].sort(
                        key=lambda s: acct_order.get(s["account"], 999)
                    )
        except Exception as e:
            if app.debug:
                app.logger.warning("Today snapshot (enriched) query failed: %s", e)
            if effective_accounts:
                for acct in effective_accounts:
                    context["today_snapshots_by_account"].append({
                        "account": acct,
                        "today_value": None,
                        "today_date": None,
                        "comparisons": {
                            "day": {"base_date": None, "delta": None, "delta_pct": None, "has_data": False},
                            "week": {"base_date": None, "delta": None, "delta_pct": None, "has_data": False},
                            "month": {"base_date": None, "delta": None, "delta_pct": None, "has_data": False},
                        },
                        "vs_week_start": {"delta": None, "delta_pct": None, "has_data": False, "base_date": this_week},
                    })

        # Your week: closed P&L plus % account change this week (reuse weekly returns if we have it)
        context["your_week"] = None
        review_ctx = context.get("review") or {}
        total_pnl = float(review_ctx.get("total_pnl", 0) or 0)
        trades_closed = int(review_ctx.get("trades_closed", 0) or 0)

        acct_pct = None
        if not ret_df.empty:
            total_start = float(ret_df["start_value"].sum() or 0)
            total_end = float(ret_df["end_value"].sum() or 0)
            if total_start > 0:
                acct_pct = round((total_end - total_start) / total_start * 100, 2)

        context["your_week"] = {
            "dollars": total_pnl,
            "acct_pct": acct_pct,
            "trades_closed": trades_closed,
        }

        # ── Process: Behavior baseline ──
        context["behavior_mirror"] = None
        try:
            beh_df = batch.get("behavior", pd.DataFrame())
            if not beh_df.empty:
                # Aggregate across accounts for this user scope
                for col in [
                    "trades_closed",
                    "total_pnl",
                    "num_winners",
                    "num_losers",
                    "avg_trades_closed_8w",
                    "avg_total_pnl_8w",
                    "avg_win_rate_8w",
                    "baseline_weeks_8w",
                ]:
                    if col in beh_df.columns:
                        beh_df[col] = pd.to_numeric(beh_df[col], errors="coerce").fillna(0)

                total_trades = int(beh_df["trades_closed"].sum() or 0)
                total_winners = int(beh_df["num_winners"].sum() or 0)
                total_losers = int(beh_df["num_losers"].sum() or 0)
                closed_trades = total_winners + total_losers
                win_rate_week = None
                if closed_trades > 0:
                    win_rate_week = round(total_winners / closed_trades * 100, 1)

                # Baseline: simple average of per-account 8-week baselines
                baseline_trades = float(beh_df["avg_trades_closed_8w"].mean() or 0)
                baseline_pnl = float(beh_df["avg_total_pnl_8w"].mean() or 0)
                baseline_win_rate = None
                if "avg_win_rate_8w" in beh_df.columns:
                    baseline_win_rate = float(beh_df["avg_win_rate_8w"].mean() or 0) * 100

                context["behavior_mirror"] = {
                    "has_baseline": bool(beh_df["baseline_weeks_8w"].sum() > 0),
                    "volume": {
                        "value": total_trades,
                        "baseline": baseline_trades,
                        "diff": total_trades - baseline_trades if baseline_trades else None,
                    },
                    "win_rate": {
                        "value": win_rate_week,
                        "baseline": baseline_win_rate,
                        "diff": (win_rate_week - baseline_win_rate) if (win_rate_week is not None and baseline_win_rate is not None) else None,
                    },
                    "pnl": {
                        "value": float(beh_df["total_pnl"].sum() or 0),
                        "baseline": baseline_pnl,
                        "diff": float(beh_df["total_pnl"].sum() or 0) - baseline_pnl if baseline_pnl else None,
                    },
                }
        except Exception as e:
            if app.debug:
                app.logger.warning("Weekly behavior baseline query failed: %s", e)

        # ── Process: Strategy breakdown ──
        try:
            strat_df = batch.get("strategy", pd.DataFrame())
            if not strat_df.empty:
                w_total = lambda r: int(r.get("winners", 0) or 0) + int(r.get("losers", 0) or 0)
                context["strategy_breakdown_week"] = [
                    {
                        "strategy": row.get("strategy") or "Unclassified",
                        "total_pnl": round(float(row.get("total_pnl") or 0), 2),
                        "trades": int(row.get("trades") or 0),
                        "winners": int(row.get("winners") or 0),
                        "losers": int(row.get("losers") or 0),
                        "win_rate_pct": round(
                            int(row.get("winners", 0) or 0) / w_total(row) * 100,
                            1,
                        ) if w_total(row) > 0 else None,
                    }
                    for _, row in strat_df.iterrows()
                ]
        except Exception as e:
            if app.debug:
                app.logger.warning("Weekly strategy breakdown query failed: %s", e)

        # ── Process: Trades this week ──
        try:
            trades_df = batch.get("trades", pd.DataFrame())
            if not trades_df.empty:
                trades_list = []
                for _, row in trades_df.iterrows():
                    open_d = row.get("open_date")
                    close_d = row.get("close_date")
                    open_d = open_d.isoformat() if hasattr(open_d, "isoformat") else (str(open_d)[:10] if open_d is not None else "")
                    close_d = close_d.isoformat() if hasattr(close_d, "isoformat") else (str(close_d)[:10] if close_d is not None else "")
                    total_pnl = row.get("total_pnl")
                    total_pnl = float(total_pnl) if total_pnl is not None else None
                    status = str(row.get("status", ""))
                    # Open: show unrealized P/L; Closed: show realized P/L (total_pnl)
                    is_closed = status == "Closed"
                    if is_closed:
                        display_pnl = total_pnl
                    else:
                        display_pnl = float(row["current_unrealized_pnl"]) if row.get("current_unrealized_pnl") is not None else None
                    trades_list.append({
                        "account": str(row.get("account", "")),
                        "symbol": str(row.get("symbol", "")),
                        "strategy": str(row.get("strategy", "")),
                        "trade_symbol": str(row.get("trade_symbol", "")) if row.get("trade_symbol") else "",
                        "open_date": open_d,
                        "close_date": close_d,
                        "total_pnl": total_pnl,
                        "status": status,
                        "cost_basis": float(row["trade_cost"]) if row.get("trade_cost") is not None else None,
                        "market_value": float(row["current_market_value"]) if row.get("current_market_value") is not None else None,
                        "current_pnl": display_pnl,
                    })
                context["trades_this_week"] = trades_list
        except Exception as e:
            if app.debug:
                app.logger.warning("Weekly trades mart query failed: %s", e)

        # Risk drift (from mart data)
        this_opens = context["review"]["trades_opened"] if context["review"] else 0
        prev_opens = context["prev_review"]["trades_opened"] if context["prev_review"] else 0
        if context["review"] is None:
            context["review"] = {
                "trades_closed": 0, "total_pnl": 0, "num_winners": 0, "num_losers": 0,
                "premium_received": 0, "premium_paid": 0, "trades_opened": 0,
                "best_trade": None, "worst_trade": None, "largest_mistake": None,
                "most_consistent_strategy": None,
            }
        context["review"]["risk_drift"] = {
            "this_week_opens": this_opens,
            "prev_week_opens": prev_opens,
            "diff": this_opens - prev_opens,
        }

        # ── Process: Open positions (today strip + expiring options) ──
        context["today_strip"] = []
        context["expiring_options"] = []
        try:
            all_pos_df = batch.get("positions", pd.DataFrame())
            if not all_pos_df.empty:
                for col in ["market_value", "cost_basis", "unrealized_pnl", "unrealized_pnl_pct",
                             "current_price", "quantity", "option_strike", "latest_stock_price"]:
                    if col in all_pos_df.columns:
                        all_pos_df[col] = pd.to_numeric(all_pos_df[col], errors="coerce").fillna(0)

                # --- Today strip: group by symbol ---
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
                    upnl_pct = round(upnl / cost * 100, 1) if cost and cost != 0 else None
                    context["today_strip"].append({
                        "symbol": sym,
                        "market_value": round(mv, 2),
                        "unrealized_pnl": round(upnl, 2),
                        "unrealized_pnl_pct": upnl_pct,
                        "price": round(eq_prices.get(sym, 0), 2) if eq_prices.get(sym) else None,
                        "num_legs": int(row["num_legs"]),
                    })
                context["today_strip"].sort(key=lambda x: abs(x["market_value"]), reverse=True)

                # --- Expiring options: filter from same dataframe ---
                opts = all_pos_df[all_pos_df["instrument_type"].isin(["Call", "Put"])].copy()
                if not opts.empty and "option_expiry" in opts.columns:
                    opts["option_expiry"] = pd.to_datetime(opts["option_expiry"])
                    expiry_cutoff = pd.Timestamp(today + timedelta(days=7))
                    expiring = opts[opts["option_expiry"].notna() & (opts["option_expiry"] <= expiry_cutoff)]

                    for _, row in expiring.iterrows():
                        sym = str(row.get("symbol", ""))
                        strike = float(row.get("option_strike") or 0)
                        opt_type = str(row.get("option_type") or "")
                        stock_price = float(eq_prices.get(sym, 0)) or float(row.get("latest_stock_price") or 0)
                        expiry = row.get("option_expiry")
                        expiry_str = expiry.strftime("%Y-%m-%d") if hasattr(expiry, "strftime") else str(expiry)[:10]
                        days_to_exp = (expiry.date() - today).days if hasattr(expiry, "date") else None

                        if stock_price > 0 and strike > 0:
                            if opt_type == "Call":
                                itm = stock_price >= strike
                                distance = round(stock_price - strike, 2)
                            else:
                                itm = stock_price <= strike
                                distance = round(strike - stock_price, 2)
                        else:
                            itm = None
                            distance = None

                        context["expiring_options"].append({
                            "symbol": sym,
                            "trade_symbol": str(row.get("trade_symbol", "")),
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
        except Exception as e:
            if app.debug:
                app.logger.warning("Open positions query failed: %s", e)

        # ── Process: Position Impact (stock movement vs option P&L) ──
        context["position_impact"] = []
        context["position_impact_summary"] = None
        try:
            stock_df = batch.get("stock_moves", pd.DataFrame())
            trades_list = context.get("trades_this_week", [])
            all_pos_df_impact = batch.get("positions", pd.DataFrame())

            if not stock_df.empty and trades_list:
                for col in ["start_price", "end_price"]:
                    stock_df[col] = pd.to_numeric(stock_df[col], errors="coerce")

                # Aggregate stock prices across accounts (weighted avg not needed — use first account's prices as proxy)
                stock_by_sym = (
                    stock_df.groupby("symbol")
                    .agg(start_price=("start_price", "first"), end_price=("end_price", "first"))
                    .reset_index()
                )
                price_map = {
                    row["symbol"]: {"start": float(row["start_price"]), "end": float(row["end_price"])}
                    for _, row in stock_by_sym.iterrows()
                    if pd.notna(row["start_price"]) and row["start_price"] > 0
                }

                # Equity share counts from current positions
                shares_map = {}
                if not all_pos_df_impact.empty and "instrument_type" in all_pos_df_impact.columns:
                    eq = all_pos_df_impact[all_pos_df_impact["instrument_type"] == "Equity"].copy()
                    if not eq.empty:
                        eq["quantity"] = pd.to_numeric(eq["quantity"], errors="coerce").fillna(0)
                        shares_map = dict(eq.groupby("symbol")["quantity"].sum())

                # Option P&L by symbol from closed trades
                option_pnl_map = {}
                for t in trades_list:
                    if t.get("status") == "Closed" and t.get("current_pnl") is not None:
                        sym = t["symbol"]
                        option_pnl_map[sym] = option_pnl_map.get(sym, 0) + float(t["current_pnl"])

                # Symbols that had option trades this week
                traded_symbols = set(option_pnl_map.keys())
                total_option_pnl = 0.0
                total_equity_impact = 0.0
                impacts = []

                for sym in sorted(traded_symbols):
                    prices = price_map.get(sym)
                    if not prices:
                        continue
                    start_p = prices["start"]
                    end_p = prices["end"]
                    price_change = end_p - start_p
                    price_change_pct = (price_change / start_p) * 100 if start_p else 0

                    shares = shares_map.get(sym, 0)
                    equity_impact = shares * price_change if shares else None
                    opt_pnl = option_pnl_map.get(sym, 0)
                    net = (equity_impact or 0) + opt_pnl

                    total_option_pnl += opt_pnl
                    if equity_impact is not None:
                        total_equity_impact += equity_impact

                    impacts.append({
                        "symbol": sym,
                        "start_price": round(start_p, 2),
                        "end_price": round(end_p, 2),
                        "price_change": round(price_change, 2),
                        "price_change_pct": round(price_change_pct, 1),
                        "shares": int(shares) if shares else 0,
                        "equity_impact": round(equity_impact, 2) if equity_impact is not None else None,
                        "option_pnl": round(opt_pnl, 2),
                        "net_impact": round(net, 2),
                    })

                impacts.sort(key=lambda x: abs(x["net_impact"]), reverse=True)
                context["position_impact"] = impacts

                has_equity = any(i["equity_impact"] is not None and i["shares"] > 0 for i in impacts)
                if impacts:
                    context["position_impact_summary"] = {
                        "total_option_pnl": round(total_option_pnl, 2),
                        "total_equity_impact": round(total_equity_impact, 2) if has_equity else None,
                        "total_net": round(total_equity_impact + total_option_pnl, 2) if has_equity else None,
                        "num_symbols": len(impacts),
                        "has_equity": has_equity,
                    }
        except Exception as e:
            if app.debug:
                app.logger.warning("Position impact processing failed: %s", e)

        # ── Process: Daily P&L Calendar Heatmap ──
        context["daily_calendar"] = []
        context["calendar_month_label"] = today.strftime("%B %Y")
        try:
            cal_df = batch.get("calendar", pd.DataFrame())
            # Build lookup of data by date
            data_by_date = {}
            if not cal_df.empty:
                for col in ["account_value", "daily_change"]:
                    if col in cal_df.columns:
                        cal_df[col] = pd.to_numeric(cal_df[col], errors="coerce").fillna(0)
                if "date" in cal_df.columns:
                    cal_df["date"] = pd.to_datetime(cal_df["date"]).dt.date
                for _, row in cal_df.iterrows():
                    data_by_date[row["date"]] = round(float(row.get("daily_change") or 0), 2)

            # Generate all days of the month up to today
            d = month_start
            end_day = min(month_end_date, today)
            while d <= end_day:
                context["daily_calendar"].append({
                    "date": str(d),
                    "day": d.day,
                    "weekday": d.weekday(),
                    "daily_change": data_by_date.get(d, 0),
                    "has_data": d in data_by_date,
                    "is_today": d == today,
                })
                d += timedelta(days=1)
        except Exception as e:
            if app.debug:
                app.logger.warning("Daily calendar query failed: %s", e)

        # Mode-specific data
        # AI Insight summary (for Friday review)
        context["ai_insight"] = get_insight_for_user(current_user.id)

        if mode == "monday":
            # Open positions exposure
            exposure_df = client.query(
                EXPOSURE_QUERY.format(account_filter=account_filter)
            ).to_dataframe()
            if not exposure_df.empty:
                for col in ["exposure"]:
                    exposure_df[col] = pd.to_numeric(exposure_df[col], errors="coerce").fillna(0)
                context["exposure"] = {
                    "total": float(exposure_df["exposure"].sum()),
                    "by_symbol": exposure_df.head(10).to_dict(orient="records"),
                    "num_positions": len(exposure_df),
                }

            # Mirror Score
            context["mirror_score"] = get_mirror_score_for_user(current_user.id)
            context["mirror_history"] = get_mirror_score_history(current_user.id, limit=8)

        elif mode == "midweek":
            # Trades opened this week so far
            opens_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "DATE", this_week),
                    bigquery.ScalarQueryParameter("end_date", "DATE", today),
                ]
            )
            opens_df = client.query(
                OPENS_THIS_WEEK_QUERY.format(account_filter=account_filter),
                job_config=opens_config,
            ).to_dataframe()
            if not opens_df.empty:
                context["opens_this_week"] = {
                    "count": len(opens_df),
                    "symbols": opens_df["symbol"].unique().tolist()[:15],
                }

    except Exception as e:
        context["error"] = str(e)

    context["narrative"] = _build_narrative(
        mode=mode,
        review=context.get("review"),
        prev_review=context.get("prev_review"),
        behavior_mirror=context.get("behavior_mirror"),
        market=context.get("market"),
        today=today,
        week_start=this_week,
        trading_days=context.get("trading_days", 0),
        market_open_today=context.get("market_open_today", True),
    )
    context["key_observation"] = _key_observation(
        review=context.get("review"),
        behavior_mirror=context.get("behavior_mirror"),
        strategy_breakdown=context.get("strategy_breakdown_week", []),
    )
    context["today_pulse"] = _today_pulse(context.get("today_snapshots_by_account", []))

    # ── Pattern detection (all modes) ──
    context["patterns"] = []
    try:
        client = get_bigquery_client()
        week_end = this_week + timedelta(days=6)
        context["patterns"] = _detect_patterns(client, account_filter, this_week, week_end)
    except Exception:
        pass

    # ── Coach's Take: exit timing for this week's closed trades (Friday mode) ──
    context["coaching_take"] = None
    if mode == "friday":
        try:
            week_end_date = this_week + timedelta(days=6)
            exit_cfg = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("week_start", "DATE", this_week),
                bigquery.ScalarQueryParameter("week_end", "DATE", week_end_date),
            ])
            exit_df = client.query(
                WEEKLY_EXIT_ANALYSIS_QUERY.format(account_filter=account_filter),
                job_config=exit_cfg,
            ).to_dataframe()

            coaching_take = {
                "exits": [],
                "total_given_back": 0,
                "total_exits": 0,
                "avg_giveback_pct": 0,
                "coaching_signals": [],
            }

            if not exit_df.empty:
                for col in ["pnl_given_back", "giveback_pct", "actual_pnl",
                             "peak_unrealized_pnl", "days_held_past_peak"]:
                    if col in exit_df.columns:
                        exit_df[col] = pd.to_numeric(exit_df[col], errors="coerce").fillna(0)

                coaching_take["total_exits"] = len(exit_df)
                coaching_take["total_given_back"] = round(float(exit_df["pnl_given_back"].sum()), 2)
                gb_nonzero = exit_df[exit_df["pnl_given_back"] > 0]
                if not gb_nonzero.empty:
                    coaching_take["avg_giveback_pct"] = round(float(gb_nonzero["giveback_pct"].mean()), 0)

                for _, r in exit_df.head(5).iterrows():
                    coaching_take["exits"].append({
                        "symbol": str(r.get("underlying_symbol", "")),
                        "strategy": str(r.get("strategy", "")),
                        "actual_pnl": round(float(r.get("actual_pnl", 0)), 2),
                        "peak_pnl": round(float(r.get("peak_unrealized_pnl", 0)), 2),
                        "given_back": round(float(r.get("pnl_given_back", 0)), 2),
                        "days_past_peak": int(r.get("days_held_past_peak", 0) or 0),
                        "pct_premium_captured": float(r.get("pct_of_premium_captured") or 0),
                        "optimal": bool(r.get("optimal_exit")),
                    })

            # Aggregate coaching signals
            try:
                where_clause = _account_sql_where(effective_accounts)
                signals_df = client.query(
                    COACHING_SIGNALS_WEEKLY_QUERY.format(where=where_clause)
                ).to_dataframe()
                if not signals_df.empty:
                    for col in ["avg_giveback_pct", "avg_days_held_past_peak",
                                 "total_pnl_given_back", "optimal_exit_rate",
                                 "num_rolls", "roll_success_rate"]:
                        if col in signals_df.columns:
                            signals_df[col] = pd.to_numeric(signals_df[col], errors="coerce").fillna(0)

                    total_given_back_all = float(signals_df["total_pnl_given_back"].sum())
                    avg_days_past = float(signals_df["avg_days_held_past_peak"].mean())
                    num_rolls = int(signals_df.iloc[0].get("num_rolls", 0))

                    if total_given_back_all > 50:
                        coaching_take["coaching_signals"].append(
                            f"Across all your trading, you've left ${total_given_back_all:,.0f} "
                            f"on the table by holding past peak profit (avg {avg_days_past:.0f} days past peak)."
                        )
                    if num_rolls >= 3:
                        roll_wr = float(signals_df.iloc[0].get("roll_success_rate", 0))
                        avg_dte = float(signals_df.iloc[0].get("avg_dte_at_roll", 0))
                        coaching_take["coaching_signals"].append(
                            f"You've rolled {num_rolls} times, typically at {avg_dte:.0f} DTE "
                            f"with a {roll_wr:.0f}% success rate."
                        )
            except Exception:
                pass

            if coaching_take["total_exits"] > 0 or coaching_take["coaching_signals"]:
                context["coaching_take"] = coaching_take
        except Exception as e:
            if app.debug:
                app.logger.warning("Coaching take query failed: %s", e)

    # ── Watch Next Week (Friday mode) ──
    context["watch_next_week"] = None
    if mode == "friday":
        watch_items = []
        expiring_count = len(context.get("expiring_options", []))
        if expiring_count > 0:
            watch_items.append(f"{expiring_count} option{'s' if expiring_count != 1 else ''} expiring within 7 days")
        open_count = len(context.get("today_strip", []))
        if open_count > 0:
            watch_items.append(f"{open_count} open position{'s' if open_count != 1 else ''} to manage")
        # Streak awareness
        for p in context["patterns"]:
            if p["type"] == "week_streak":
                streak_note = f"Currently on a {p['headline'].lower()}"
                watch_items.append(streak_note)
                break
        if watch_items:
            context["watch_next_week"] = watch_items

    return render_template("weekly_review.html", **context)
