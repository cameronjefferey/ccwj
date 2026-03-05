"""
Strategies — what works for you.

Strategy as a centerpiece: which strategies you use and how they perform.
Process-focused, no gamification. Reads from mart_strategy_performance (dbt).
"""
from flask import render_template, request
from flask_login import login_required, current_user
from google.cloud import bigquery

from app import app
from app.bigquery_client import get_bigquery_client
from app.models import get_accounts_for_user, is_admin


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


STRATEGY_PERFORMANCE_QUERY = """
SELECT
  account,
  strategy,
  total_pnl,
  realized_pnl,
  unrealized_pnl,
  premium_received,
  premium_paid,
  num_trades,
  num_winners,
  num_losers,
  win_rate,
  dividend_income,
  total_return,
  num_symbols,
  first_trade_date,
  last_trade_date,
  avg_days_in_trade
FROM `ccwj-dbt.analytics.mart_strategy_performance`
WHERE 1=1 {account_filter}
ORDER BY total_return DESC
"""

STRATEGY_POSITIONS_QUERY = """
SELECT
  account,
  symbol,
  status,
  total_pnl,
  realized_pnl,
  unrealized_pnl,
  total_premium_received,
  total_dividend_income,
  total_return,
  num_individual_trades,
  num_winners,
  num_losers,
  win_rate,
  avg_pnl_per_trade,
  avg_days_in_trade
FROM `ccwj-dbt.analytics.positions_summary`
WHERE strategy = @strategy
  {account_filter}
ORDER BY total_return DESC
"""


@app.route("/strategies")
@login_required
def strategies():
    """Strategy performance — what works for you. No judgment, just evidence."""
    user_accounts = _user_account_list()

    # Optional filters (from Positions page)
    selected_account = request.args.get("account", "")
    selected_strategy = request.args.get("strategy", "")

    # Narrow account scope if a specific account was selected
    effective_accounts = user_accounts
    if selected_account:
        if user_accounts is None:
            effective_accounts = [selected_account]
        else:
            effective_accounts = [a for a in user_accounts if a == selected_account] or user_accounts

    account_filter = _account_sql_and(effective_accounts)

    context = {
        "title": "Strategies",
        "strategies": [],
        "error": None,
        "totals": None,
        "summary": None,
        "focus_strategy": None,
        "focus_accounts": None,
        "focus_symbols": None,
        "accounts": [],
        "selected_account": selected_account,
        "selected_strategy": selected_strategy,
    }

    try:
        client = get_bigquery_client()
        df = client.query(
            STRATEGY_PERFORMANCE_QUERY.format(account_filter=account_filter)
        ).to_dataframe()

        if df.empty:
            return render_template("strategies.html", **context)

        # Available accounts for filter UI (after applying user account scope)
        if "account" in df.columns:
            context["accounts"] = sorted(df["account"].dropna().unique().tolist())

        # Aggregate across accounts for display (optional: also show per-account)
        for col in [
            "total_pnl", "realized_pnl", "unrealized_pnl", "premium_received", "premium_paid",
            "num_trades", "num_winners", "num_losers", "dividend_income", "total_return",
        ]:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        # Per-strategy rollup (sum across accounts for same strategy)
        by_strategy = df.groupby("strategy").agg({
            "total_pnl": "sum",
            "realized_pnl": "sum",
            "unrealized_pnl": "sum",
            "premium_received": "sum",
            "premium_paid": "sum",
            "num_trades": "sum",
            "num_winners": "sum",
            "num_losers": "sum",
            "dividend_income": "sum",
            "total_return": "sum",
            "num_symbols": "sum",
        }).reset_index()

        # Optionally focus on a single strategy; fall back to all if filter is empty
        if selected_strategy:
            filtered = by_strategy[by_strategy["strategy"] == selected_strategy]
            if not filtered.empty:
                by_strategy = filtered

        # Win rate for closed trades (percentage of winners among closed trades)
        by_strategy["win_rate"] = by_strategy.apply(
            lambda r: (
                r["num_winners"] / (r["num_winners"] + r["num_losers"]) * 100
            ) if (r["num_winners"] + r["num_losers"]) > 0 else None,
            axis=1,
        )

        # High-level summary across strategies (after any account/strategy filter)
        total_trades = int(by_strategy["num_trades"].sum() or 0)
        total_winners = int(by_strategy["num_winners"].sum() or 0)
        total_losers = int(by_strategy["num_losers"].sum() or 0)
        closed_trades = total_winners + total_losers
        overall_win_rate = (total_winners / closed_trades * 100) if closed_trades else None
        net_premium = float(by_strategy["premium_received"].sum() - by_strategy["premium_paid"].sum())

        best_row = by_strategy.sort_values("total_return", ascending=False).iloc[0]
        worst_row = by_strategy.sort_values("total_return", ascending=True).iloc[0]
        most_used_row = by_strategy.sort_values("num_trades", ascending=False).iloc[0]

        context["summary"] = {
            "strategies_count": int(len(by_strategy)),
            "total_trades": total_trades,
            "overall_win_rate": overall_win_rate,
            "net_premium": net_premium,
            "best": {
                "strategy": best_row["strategy"],
                "total_return": float(best_row["total_return"] or 0),
                "num_trades": int(best_row["num_trades"] or 0),
            },
            "worst": {
                "strategy": worst_row["strategy"],
                "total_return": float(worst_row["total_return"] or 0),
                "num_trades": int(worst_row["num_trades"] or 0),
            },
            "most_used": {
                "strategy": most_used_row["strategy"],
                "num_trades": int(most_used_row["num_trades"] or 0),
            },
        }

        # Build per-strategy list for cards
        strategies_list = []
        for _, row in by_strategy.iterrows():
            strategies_list.append({
                "strategy": row["strategy"],
                "total_return": round(float(row["total_return"] or 0), 2),
                "realized_pnl": round(float(row["realized_pnl"] or 0), 2),
                "unrealized_pnl": round(float(row["unrealized_pnl"] or 0), 2),
                "num_trades": int(row["num_trades"] or 0),
                "num_winners": int(row["num_winners"] or 0),
                "num_losers": int(row["num_losers"] or 0),
                "win_rate": round(row["win_rate"], 1) if row["win_rate"] is not None else None,
                "premium_received": round(float(row["premium_received"] or 0), 2),
                "premium_paid": round(float(row["premium_paid"] or 0), 2),
                "num_symbols": int(row["num_symbols"] or 0),
                "is_selected": bool(selected_strategy and row["strategy"] == selected_strategy),
            })
        context["strategies"] = strategies_list

        context["totals"] = {
            "total_return": round(float(by_strategy["total_return"].sum()), 2),
            "num_trades": int(by_strategy["num_trades"].sum()),
        }

        # Focus card data for the selected strategy, if any
        if selected_strategy:
            focus_rows = [s for s in strategies_list if s["strategy"] == selected_strategy]
            if focus_rows:
                context["focus_strategy"] = focus_rows[0]

                # Per-account breakdown for this strategy (from mart_strategy_performance)
                acct_df = df[df["strategy"] == selected_strategy].copy()
                if not acct_df.empty:
                    acct_rows = []
                    for _, r in acct_df.iterrows():
                        acct_rows.append({
                            "account": r["account"],
                            "total_return": float(r.get("total_return") or 0),
                            "realized_pnl": float(r.get("realized_pnl") or 0),
                            "unrealized_pnl": float(r.get("unrealized_pnl") or 0),
                            "num_trades": int(r.get("num_trades") or 0),
                            "win_rate": float(r.get("win_rate") or 0) * 100 if r.get("win_rate") is not None else None,
                        })
                    context["focus_accounts"] = acct_rows

                # Per-symbol breakdown for this strategy (from positions_summary)
                try:
                    acct_clause = _account_sql_and(effective_accounts)
                    pos_query = STRATEGY_POSITIONS_QUERY.format(account_filter=acct_clause)
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("strategy", "STRING", selected_strategy),
                        ]
                    )
                    pos_df = client.query(pos_query, job_config=job_config).to_dataframe()
                    if not pos_df.empty:
                        sym_rows = []
                        for _, r in pos_df.iterrows():
                            sym_rows.append({
                                "account": r.get("account"),
                                "symbol": r.get("symbol"),
                                "status": r.get("status"),
                                "total_return": float(r.get("total_return") or 0),
                                "realized_pnl": float(r.get("realized_pnl") or 0),
                                "unrealized_pnl": float(r.get("unrealized_pnl") or 0),
                                "num_trades": int(r.get("num_individual_trades") or 0),
                                "win_rate": float(r.get("win_rate") or 0),
                                "avg_pnl": float(r.get("avg_pnl_per_trade") or 0),
                                "avg_days": float(r.get("avg_days_in_trade") or 0),
                                "premium": float(r.get("total_premium_received") or 0),
                                "dividends": float(r.get("total_dividend_income") or 0),
                            })
                        context["focus_symbols"] = sym_rows
                except Exception:
                    # If symbol-level breakdown fails, don't block the page
                    pass

    except Exception as e:
        context["error"] = str(e)

    return render_template("strategies.html", **context)
