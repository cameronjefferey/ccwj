"""Weekly Review — auto-generated Sunday summary: best/worst trade, drift, anomalies."""
from datetime import date, timedelta
from flask import render_template
from flask_login import login_required, current_user
from app import app
from app.bigquery_client import get_bigquery_client
from app.models import get_accounts_for_user, is_admin, list_journal_entries
from google.cloud import bigquery
import pandas as pd


def _user_account_list():
    if is_admin(current_user.username):
        return None
    return get_accounts_for_user(current_user.id)


def _account_sql_filter(accounts):
    if accounts is None:
        return ""
    if not accounts:
        return "AND 1 = 0"
    quoted = ", ".join(f"'{a.replace(chr(39), chr(39)+chr(39))}'" for a in accounts)
    return f"AND c.account IN ({quoted})"


# Trades that closed in the date range (from int_strategy_classification)
WEEKLY_TRADES_QUERY = """
SELECT
    c.account,
    c.symbol,
    c.strategy,
    c.trade_symbol,
    c.open_date,
    c.close_date,
    c.total_pnl,
    c.is_winner,
    c.status,
    c.num_trades,
    c.premium_received,
    c.premium_paid
FROM `ccwj-dbt.analytics.int_strategy_classification` c
WHERE c.close_date IS NOT NULL
  AND c.close_date >= @start_date
  AND c.close_date <= @end_date
  {account_filter}
ORDER BY c.close_date DESC
"""

# Same for previous week (for drift)
PREV_WEEK_TRADES_QUERY = """
SELECT
    c.account,
    c.symbol,
    c.strategy,
    c.trade_symbol,
    c.open_date,
    c.close_date,
    c.total_pnl,
    c.is_winner,
    c.status
FROM `ccwj-dbt.analytics.int_strategy_classification` c
WHERE c.close_date IS NOT NULL
  AND c.close_date >= @prev_start
  AND c.close_date <= @prev_end
  {account_filter}
"""

# Trades opened in the date range (for risk drift - new position count)
OPENS_QUERY = """
SELECT
    c.account,
    c.symbol,
    c.strategy,
    c.open_date
FROM `ccwj-dbt.analytics.int_strategy_classification` c
WHERE c.open_date >= @start_date
  AND c.open_date <= @end_date
  {account_filter}
"""

PREV_OPENS_QUERY = """
SELECT
    c.account,
    c.symbol,
    c.strategy,
    c.open_date
FROM `ccwj-dbt.analytics.int_strategy_classification` c
WHERE c.open_date >= @prev_start
  AND c.open_date <= @prev_end
  {account_filter}
"""


def _get_week_bounds():
    """Return (start, end) for last 7 days, and (prev_start, prev_end) for the 7 days before that."""
    end = date.today()
    start = end - timedelta(days=6)  # 7 days inclusive
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=6)
    return start, end, prev_start, prev_end


def _compute_review(user_accounts, trades_df, prev_trades_df, opens_df, prev_opens_df, journal_entries):
    """Build the weekly review dict from BQ and journal data."""
    review = {
        "best_trade": None,
        "worst_trade": None,
        "largest_mistake": None,
        "most_consistent_strategy": None,
        "risk_drift": None,
        "emotional_drift": None,
        "behavioral_anomaly": None,
        "week_start": None,
        "week_end": None,
        "total_pnl": 0,
        "num_trades": 0,
        "num_winners": 0,
    }

    start, end, prev_start, prev_end = _get_week_bounds()
    review["week_start"] = start
    review["week_end"] = end

    # Risk drift & emotional drift — always compute (don't require closed trades)
    num_opens = len(opens_df) if opens_df is not None and not opens_df.empty else 0
    num_prev_opens = len(prev_opens_df) if prev_opens_df is not None and not prev_opens_df.empty else 0
    review["risk_drift"] = {
        "this_week_opens": num_opens,
        "prev_week_opens": num_prev_opens,
        "diff": num_opens - num_prev_opens,
    }
    this_week_j = [e for e in journal_entries if _date_in_range(e.get("trade_open_date"), start, end)]
    prev_week_j = list_journal_entries(
        current_user.id,
        start_date=str(prev_start),
        end_date=str(prev_end),
        limit=200,
    )
    review["emotional_drift"] = {
        "this_week": _mood_counts(this_week_j),
        "prev_week": _mood_counts(prev_week_j),
        "entries_this_week": len(this_week_j),
        "entries_prev_week": len(prev_week_j),
    }

    if trades_df.empty:
        return review

    closed = trades_df[trades_df["status"] == "Closed"]
    if closed.empty:
        return review

    closed = closed.copy()
    closed["total_pnl"] = pd.to_numeric(closed["total_pnl"], errors="coerce").fillna(0)

    review["total_pnl"] = float(closed["total_pnl"].sum())
    review["num_trades"] = len(closed)
    review["num_winners"] = int((closed["total_pnl"] > 0).sum())

    # Best trade
    best_row = closed.loc[closed["total_pnl"].idxmax()]
    review["best_trade"] = {
        "account": best_row.get("account", ""),
        "symbol": best_row.get("symbol", ""),
        "strategy": best_row.get("strategy", ""),
        "trade_symbol": best_row.get("trade_symbol", ""),
        "close_date": str(best_row.get("close_date", "")),
        "total_pnl": float(best_row["total_pnl"]),
    }

    # Worst trade
    worst_row = closed.loc[closed["total_pnl"].idxmin()]
    review["worst_trade"] = {
        "account": worst_row.get("account", ""),
        "symbol": worst_row.get("symbol", ""),
        "strategy": worst_row.get("strategy", ""),
        "trade_symbol": worst_row.get("trade_symbol", ""),
        "close_date": str(worst_row.get("close_date", "")),
        "total_pnl": float(worst_row["total_pnl"]),
    }

    # Largest mistake = worst loser (or worst trade if all winners)
    losers = closed[closed["total_pnl"] < 0]
    if not losers.empty:
        mistake_row = losers.loc[losers["total_pnl"].idxmin()]
        review["largest_mistake"] = {
            "account": mistake_row.get("account", ""),
            "symbol": mistake_row.get("symbol", ""),
            "strategy": mistake_row.get("strategy", ""),
            "trade_symbol": mistake_row.get("trade_symbol", ""),
            "close_date": str(mistake_row.get("close_date", "")),
            "total_pnl": float(mistake_row["total_pnl"]),
        }
    else:
        review["largest_mistake"] = None

    # Most consistent strategy (highest win rate, min 2 trades)
    strat_agg = closed.groupby("strategy").agg(
        trades=("total_pnl", "count"),
        winners=("total_pnl", lambda x: (x > 0).sum()),
        total_pnl=("total_pnl", "sum"),
    ).reset_index()
    strat_agg["win_rate"] = strat_agg["winners"] / strat_agg["trades"]
    strat_agg = strat_agg[strat_agg["trades"] >= 2].sort_values("win_rate", ascending=False)
    if not strat_agg.empty:
        top = strat_agg.iloc[0]
        review["most_consistent_strategy"] = {
            "strategy": str(top["strategy"]),
            "win_rate": float(top["win_rate"]),
            "trades": int(top["trades"]),
            "total_pnl": float(top["total_pnl"]),
        }

    # Behavioral anomaly: journal entries with risky tags (fomo, revenge_trade, boredom_trade) that lost
    risky_tags = {"fomo", "revenge_trade", "boredom_trade"}
    journal_by_key = {
        _journal_match_key(e): e
        for e in journal_entries
    }
    anomalies = []
    for _, row in losers.iterrows():
        key = _trade_match_key(row)
        j = journal_by_key.get(key)
        if not j:
            continue
        tags = set((j.get("tags") or []))
        overlap = tags & risky_tags
        if overlap:
            anomalies.append({
                "symbol": row.get("symbol", ""),
                "strategy": row.get("strategy", ""),
                "tags": list(overlap),
                "total_pnl": float(row["total_pnl"]),
                "reflection": (j.get("reflection") or "")[:200],
            })
    review["behavioral_anomaly"] = anomalies[:5] if anomalies else None

    return review


def _date_in_range(d, start, end):
    if not d:
        return False
    try:
        if isinstance(d, str):
            from datetime import datetime
            parsed = datetime.strptime(d[:10], "%Y-%m-%d").date()
        else:
            parsed = d
        return start <= parsed <= end
    except Exception:
        return False


def _mood_counts(entries):
    from collections import Counter
    moods = [e.get("mood") for e in entries if e.get("mood")]
    return dict(Counter(moods))


def _journal_match_key(e):
    return (
        str(e.get("account", "")),
        str(e.get("symbol", "")).upper(),
        str(e.get("strategy", "")),
        str(e.get("trade_open_date", ""))[:10],
    )


def _trade_match_key(row):
    return (
        str(row.get("account", "")),
        str(row.get("symbol", "")).upper(),
        str(row.get("strategy", "")),
        str(row.get("open_date", ""))[:10] if row.get("open_date") else "",
    )


@app.route("/weekly-review")
@login_required
def weekly_review():
    """Auto-generated weekly review: best/worst trade, drift, anomalies."""
    user_accounts = _user_account_list()
    account_filter = _account_sql_filter(user_accounts)

    start, end, prev_start, prev_end = _get_week_bounds()

    try:
        client = get_bigquery_client()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start),
                bigquery.ScalarQueryParameter("end_date", "DATE", end),
                bigquery.ScalarQueryParameter("prev_start", "DATE", prev_start),
                bigquery.ScalarQueryParameter("prev_end", "DATE", prev_end),
            ]
        )

        trades_df = client.query(
            WEEKLY_TRADES_QUERY.format(account_filter=account_filter),
            job_config=job_config,
        ).to_dataframe()

        prev_trades_df = client.query(
            PREV_WEEK_TRADES_QUERY.format(account_filter=account_filter),
            job_config=job_config,
        ).to_dataframe()

        opens_df = client.query(
            OPENS_QUERY.format(account_filter=account_filter),
            job_config=job_config,
        ).to_dataframe()

        prev_opens_df = client.query(
            PREV_OPENS_QUERY.format(account_filter=account_filter),
            job_config=job_config,
        ).to_dataframe()

    except Exception as e:
        return render_template(
            "weekly_review.html",
            title="Weekly Review",
            error=str(e),
            review=None,
            week_start=start,
            week_end=end,
        )

    # Filter BQ results by account if needed (query param filter may not cover all cases)
    if user_accounts is not None and not user_accounts:
        trades_df = pd.DataFrame()
        prev_trades_df = pd.DataFrame()
        opens_df = pd.DataFrame()
        prev_opens_df = pd.DataFrame()
    elif user_accounts:
        trades_df = trades_df[trades_df["account"].isin(user_accounts)]
        prev_trades_df = prev_trades_df[prev_trades_df["account"].isin(user_accounts)]
        opens_df = opens_df[opens_df["account"].isin(user_accounts)]
        prev_opens_df = prev_opens_df[prev_opens_df["account"].isin(user_accounts)]

    journal_entries = list_journal_entries(
        current_user.id,
        start_date=str(start),
        end_date=str(end),
        limit=500,
    )

    review = _compute_review(user_accounts, trades_df, prev_trades_df, opens_df, prev_opens_df, journal_entries)

    return render_template(
        "weekly_review.html",
        title="Weekly Review",
        review=review,
        error=None,
        week_start=start,
        week_end=end,
    )
