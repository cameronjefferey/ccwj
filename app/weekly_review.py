"""
Weekly Review — temporal hub with three modes:

  Friday Review   → "What happened this week?"
  Monday Check    → "How am I showing up?"
  Mid-Week Check  → "Am I deviating?"

Reads pre-aggregated data from mart_weekly_summary where possible.
Journal-based metrics (emotional drift, behavioral anomaly) still come from SQLite.
"""
from datetime import date, timedelta
from flask import render_template, request
from flask_login import login_required, current_user
from app import app
from app.bigquery_client import get_bigquery_client
from app.models import (
    get_accounts_for_user, is_admin,
    list_journal_entries, get_mirror_score_for_user, get_mirror_score_history,
    get_insight_for_user,
)
from google.cloud import bigquery
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None


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


def _get_market_performance(week_start, today):
    """Fetch SPY and QQQ returns for the week and YTD via yfinance. Returns None on failure."""
    if yf is None:
        return None
    out = {"spy_week_pct": None, "qqq_week_pct": None, "spy_ytd_pct": None, "qqq_ytd_pct": None}
    try:
        end = today + timedelta(days=1)
        ytd_start = date(today.year, 1, 1)
        for ticker, week_key, ytd_key in [
            ("SPY", "spy_week_pct", "spy_ytd_pct"),
            ("QQQ", "qqq_week_pct", "qqq_ytd_pct"),
        ]:
            t = yf.Ticker(ticker)
            hist = t.history(start=week_start, end=end, auto_adjust=True)
            if hist is not None and len(hist) >= 2:
                start_p = hist["Close"].iloc[0]
                end_p = hist["Close"].iloc[-1]
                if start_p and start_p > 0:
                    out[week_key] = round((end_p - start_p) / start_p * 100, 2)
            ytd_hist = t.history(start=ytd_start, end=end, auto_adjust=True)
            if ytd_hist is not None and len(ytd_hist) >= 2:
                ytd_start_p = ytd_hist["Close"].iloc[0]
                ytd_end_p = ytd_hist["Close"].iloc[-1]
                if ytd_start_p and ytd_start_p > 0:
                    out[ytd_key] = round((ytd_end_p - ytd_start_p) / ytd_start_p * 100, 2)
    except Exception as e:
        if app.debug:
            app.logger.warning("Market performance fetch failed: %s", e)
        return None
    return out


# Pre-aggregated weekly summary from dbt
WEEKLY_SUMMARY_QUERY = """
SELECT *
FROM `ccwj-dbt.analytics.mart_weekly_summary`
WHERE week_start = @week_start
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

# For behavioral anomaly: need individual losing trades joined with journal
LOSERS_QUERY = """
SELECT
    c.account, c.symbol, c.strategy, c.trade_symbol,
    c.open_date, c.close_date, c.total_pnl
FROM `ccwj-dbt.analytics.int_strategy_classification` c
WHERE c.status = 'Closed'
  AND c.total_pnl < 0
  AND c.close_date >= @start_date
  AND c.close_date <= @end_date
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


def _mood_counts(entries):
    from collections import Counter
    moods = [e.get("mood") for e in entries if e.get("mood")]
    return dict(Counter(moods))


def _compute_behavioral_anomaly(losers_df, journal_entries):
    """Match losing trades with risky journal tags (fomo, revenge, boredom)."""
    if losers_df is None or losers_df.empty:
        return None

    risky_tags = {"fomo", "revenge_trade", "boredom_trade"}
    journal_by_key = {}
    for e in journal_entries:
        key = (
            str(e.get("account", "")),
            str(e.get("symbol", "")).upper(),
            str(e.get("strategy", "")),
            str(e.get("trade_open_date", ""))[:10],
        )
        journal_by_key[key] = e

    anomalies = []
    for _, row in losers_df.iterrows():
        key = (
            str(row.get("account", "")),
            str(row.get("symbol", "")).upper(),
            str(row.get("strategy", "")),
            str(row.get("open_date", ""))[:10] if row.get("open_date") else "",
        )
        j = journal_by_key.get(key)
        if not j:
            continue
        tags = set(j.get("tags") or [])
        overlap = tags & risky_tags
        if overlap:
            anomalies.append({
                "symbol": row.get("symbol", ""),
                "strategy": row.get("strategy", ""),
                "tags": list(overlap),
                "total_pnl": float(row["total_pnl"]),
                "reflection": (j.get("reflection") or "")[:200],
            })
    return anomalies[:5] if anomalies else None


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
        "journal_entries": [],
        "emotional_drift": None,
        "behavioral_anomaly": None,
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

        # Check if the current week has data; if not, fall back to most recent active week
        job_config_check = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("week_start", "DATE", this_week),
            ]
        )
        check_df = client.query(
            WEEKLY_SUMMARY_QUERY.format(account_filter=account_filter),
            job_config=job_config_check,
        ).to_dataframe()
        has_current = not check_df.empty and (
            int(check_df.iloc[0].get("trades_closed", 0) or 0) + int(check_df.iloc[0].get("trades_opened", 0) or 0) > 0
        )

        if not has_current:
            latest_df = client.query(
                LATEST_ACTIVE_WEEK_QUERY.format(account_filter=account_filter)
            ).to_dataframe()
            if not latest_df.empty and latest_df.iloc[0]["latest_week"] is not None:
                lw = latest_df.iloc[0]["latest_week"]
                if hasattr(lw, "date"):
                    lw = lw.date()
                elif not isinstance(lw, date):
                    lw = date.fromisoformat(str(lw)[:10])
                this_week = lw
                context["week_start"] = this_week
                context["week_end"] = this_week + timedelta(days=6)
                context["is_backfilled"] = True

        prev_week = this_week - timedelta(days=7)

        # Market performance (SPY, QQQ) for comparison
        context["market"] = _get_market_performance(this_week, today)

        # Always fetch this week + prev week from mart_weekly_summary
        for target_week, key in [(this_week, "review"), (prev_week, "prev_review")]:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("week_start", "DATE", target_week),
                ]
            )
            df = client.query(
                WEEKLY_SUMMARY_QUERY.format(account_filter=account_filter),
                job_config=job_config,
            ).to_dataframe()
            if not df.empty:
                rows = df.to_dict(orient="records")
                context[key] = _aggregate_weekly_rows(rows)

        week_end = this_week + timedelta(days=6)

        # Current account snapshot (equity context)
        try:
            av_df = client.query(
                ACCOUNT_VALUE_QUERY.format(account_filter=account_filter)
            ).to_dataframe()
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
                app.logger.warning("Equity snapshot query failed: %s", e)

        # Weekly returns: start/end value per account (for "Compared to Start of Week" tile and "Your week" %)
        start_value_by_account = {}
        ret_df = pd.DataFrame()
        try:
            ret_cfg = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("week_start", "DATE", this_week)]
            )
            ret_df = client.query(
                WEEKLY_RETURNS_QUERY.format(account_filter=account_filter),
                job_config=ret_cfg,
            ).to_dataframe()
            if not ret_df.empty and "account" in ret_df.columns and "start_value" in ret_df.columns:
                for _, r in ret_df.iterrows():
                    start_value_by_account[str(r["account"])] = float(r.get("start_value") or 0)
        except Exception as e:
            if app.debug:
                app.logger.warning("Weekly returns (start value) query failed: %s", e)

        # Today's snapshot: one row per account (latest snapshot per account from mart)
        context["today_snapshots_by_account"] = []
        try:
            snap_df = client.query(
                TODAY_SNAPSHOT_ENRICHED_QUERY.format(account_filter=account_filter)
            ).to_dataframe()
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
        total_pnl = float(context.get("review", {}).get("total_pnl", 0) or 0)
        trades_closed = int(context.get("review", {}).get("trades_closed", 0) or 0)

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

        # Behavior baseline: how this week compares to recent 8-week norm
        context["behavior_mirror"] = None
        try:
            beh_cfg = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("week_start", "DATE", this_week)]
            )
            beh_df = client.query(
                WEEKLY_BEHAVIOR_QUERY.format(account_filter=account_filter),
                job_config=beh_cfg,
            ).to_dataframe()
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

        # This week's P&L by strategy (for "What works for you" section)
        try:
            strat_cfg = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "DATE", this_week),
                    bigquery.ScalarQueryParameter("end_date", "DATE", week_end),
                ]
            )
            strat_df = client.query(
                WEEKLY_STRATEGY_BREAKDOWN_QUERY.format(account_filter=account_filter),
                job_config=strat_cfg,
            ).to_dataframe()
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

        # Detailed list of trades this week from mart_weekly_trades
        try:
            trades_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("week_start", "DATE", this_week)]
            )
            trades_df = client.query(
                WEEKLY_TRADES_MART_QUERY.format(account_filter=account_filter),
                job_config=trades_config,
            ).to_dataframe()
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

        # Journal data for emotional drift + behavioral anomaly
        journal_entries = list_journal_entries(
            current_user.id,
            start_date=str(this_week),
            end_date=str(week_end),
            limit=500,
        )
        context["journal_entries"] = journal_entries

        prev_journal = list_journal_entries(
            current_user.id,
            start_date=str(prev_week),
            end_date=str(prev_week + timedelta(days=6)),
            limit=200,
        )
        context["emotional_drift"] = {
            "this_week": _mood_counts(journal_entries),
            "prev_week": _mood_counts(prev_journal),
            "entries_this_week": len(journal_entries),
            "entries_prev_week": len(prev_journal),
        }

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

        # Mode-specific data
        # AI Insight summary (for Friday review)
        context["ai_insight"] = get_insight_for_user(current_user.id)

        if mode == "friday":
            # Behavioral anomaly: need individual losers + journal cross-ref
            losers_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "DATE", this_week),
                    bigquery.ScalarQueryParameter("end_date", "DATE", week_end),
                ]
            )
            losers_df = client.query(
                LOSERS_QUERY.format(account_filter=account_filter),
                job_config=losers_config,
            ).to_dataframe()
            context["behavioral_anomaly"] = _compute_behavioral_anomaly(losers_df, journal_entries)

        elif mode == "monday":
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

    return render_template("weekly_review.html", **context)
