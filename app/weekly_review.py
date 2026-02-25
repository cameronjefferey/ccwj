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
    account_filter = _account_sql_and(user_accounts)

    mode = request.args.get("mode") or _auto_mode()
    if mode not in ("friday", "monday", "midweek"):
        mode = _auto_mode()

    today = date.today()
    this_week = _iso_week_start(today)

    context = {
        "title": "Weekly Review",
        "mode": mode,
        "week_start": this_week,
        "week_end": this_week + timedelta(days=6),
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

        # Journal data for emotional drift + behavioral anomaly
        week_end = this_week + timedelta(days=6)
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
