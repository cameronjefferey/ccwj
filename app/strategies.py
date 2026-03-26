"""
Strategies — are you getting better or worse?

Process-focused view: trend over time, DTE/moneyness sweet spots, execution
consistency. Reads from dbt marts; Flask does light assembly, not computation.
"""
from flask import render_template, request
from flask_login import login_required, current_user
from google.cloud import bigquery
import pandas as pd

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

STRATEGY_TREND_QUERY = """
SELECT
  account,
  strategy,
  month_start,
  trades_closed,
  num_winners,
  num_losers,
  win_rate_pct,
  total_pnl,
  avg_pnl_per_trade,
  avg_days_in_trade,
  premium_collected,
  premium_paid,
  win_rate_3m_pct,
  avg_pnl_3m,
  baseline_months,
  trend_signal
FROM `ccwj-dbt.analytics.mart_strategy_trend`
WHERE 1=1 {account_filter}
ORDER BY strategy, month_start
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

DTE_MONEYNESS_QUERY = """
SELECT
  dte_bucket,
  moneyness_at_open,
  outcome,
  num_trades,
  total_pnl,
  win_rate_pct
FROM `ccwj-dbt.analytics.mart_option_trades_by_kind`
WHERE strategy = @strategy
  {account_filter}
ORDER BY dte_bucket, moneyness_at_open
"""


def _strategy_narrative(summary, strategies_list, trend_data):
    """Process-focused narrative: trend-aware, not just lifetime scoreboard."""
    if not summary or not strategies_list:
        return None
    count = summary["strategies_count"]
    parts = []
    if count == 1:
        parts.append("You have one strategy in your history.")
    else:
        parts.append(f"You've used {count} strategies.")

    # Find strategies with clear trends
    improving = [s for s in strategies_list if s.get("trend_signal") == "improving"]
    declining = [s for s in strategies_list if s.get("trend_signal") == "declining"]

    if improving:
        best = max(improving, key=lambda s: s.get("num_trades", 0))
        parts.append(f"{best['strategy']} is improving — win rate trending up.")
    elif declining:
        worst = max(declining, key=lambda s: s.get("num_trades", 0))
        parts.append(f"{worst['strategy']} is declining — worth reviewing.")
    else:
        best = summary.get("best")
        if best and best["total_return"] > 0 and count > 1:
            parts.append(f"Your most profitable strategy is {best['strategy']}.")

    return " ".join(parts)


def _focus_insights(focus_strategy, overall_win_rate, trend_months, dte_data):
    """Generate multiple insights for the drill-down view."""
    insights = []
    if not focus_strategy:
        return insights

    wr = focus_strategy.get("win_rate")
    trades = focus_strategy.get("num_trades", 0)
    wr_diff = focus_strategy.get("wr_vs_overall")

    if trades < 3:
        insights.append({
            "type": "neutral",
            "text": f"Only {trades} trade{'s' if trades != 1 else ''} so far — not enough history to read patterns.",
        })
        return insights

    # Trend insight
    trend_signal = focus_strategy.get("trend_signal")
    if trend_signal == "improving":
        insights.append({
            "type": "positive",
            "text": "Win rate is trending up over the last 3 months. Your execution is sharpening.",
        })
    elif trend_signal == "declining":
        insights.append({
            "type": "negative",
            "text": "Win rate is trending down over the last 3 months. Conditions or execution may be shifting.",
        })

    # Win rate vs overall
    if wr_diff is not None:
        if wr_diff >= 15:
            insights.append({
                "type": "positive",
                "text": f"Win rate of {wr:.0f}% — {wr_diff:.0f} points above your overall average.",
            })
        elif wr_diff <= -15:
            insights.append({
                "type": "negative",
                "text": f"Win rate of {wr:.0f}% — {abs(wr_diff):.0f} points below your overall average.",
            })

    # DTE sweet spot / weak spot
    if dte_data:
        for bucket in dte_data:
            if bucket["num_trades"] >= 3:
                bucket_wr = bucket.get("win_rate_pct")
                if bucket_wr is not None and wr is not None:
                    gap = bucket_wr - wr
                    if gap >= 20:
                        insights.append({
                            "type": "positive",
                            "text": (
                                f"Sweet spot: {bucket['dte_bucket']} trades have a "
                                f"{bucket_wr:.0f}% win rate ({bucket['num_trades']} trades)."
                            ),
                        })
                        break
                    elif gap <= -20:
                        insights.append({
                            "type": "negative",
                            "text": (
                                f"Weak spot: {bucket['dte_bucket']} trades have a "
                                f"{bucket_wr:.0f}% win rate ({bucket['num_trades']} trades)."
                            ),
                        })
                        break

    return insights[:3]


@app.route("/strategies")
@login_required
def strategies():
    """Strategy performance — process-focused, trend-aware."""
    user_accounts = _user_account_list()

    selected_account = request.args.get("account", "")
    selected_strategy = request.args.get("strategy", "")

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
        "summary": None,
        "narrative": None,
        "overall_win_rate": None,
        "focus_strategy": None,
        "focus_insights": [],
        "focus_accounts": None,
        "focus_symbols": None,
        "focus_trend_months": [],
        "focus_dte_breakdown": [],
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

        if "account" in df.columns:
            context["accounts"] = sorted(df["account"].dropna().unique().tolist())

        for col in [
            "total_pnl", "realized_pnl", "unrealized_pnl", "premium_received", "premium_paid",
            "num_trades", "num_winners", "num_losers", "dividend_income", "total_return",
        ]:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        all_by_strategy = df.groupby("strategy").agg({
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
            "first_trade_date": "min",
            "last_trade_date": "max",
            "avg_days_in_trade": "mean",
        }).reset_index()

        all_by_strategy["win_rate"] = all_by_strategy.apply(
            lambda r: (
                r["num_winners"] / (r["num_winners"] + r["num_losers"]) * 100
            ) if (r["num_winners"] + r["num_losers"]) > 0 else None,
            axis=1,
        )

        total_winners = int(all_by_strategy["num_winners"].sum() or 0)
        total_losers = int(all_by_strategy["num_losers"].sum() or 0)
        closed_trades = total_winners + total_losers
        overall_win_rate = (total_winners / closed_trades * 100) if closed_trades else None
        net_premium = float(
            all_by_strategy["premium_received"].sum() - all_by_strategy["premium_paid"].sum()
        )

        best_row = all_by_strategy.sort_values("total_return", ascending=False).iloc[0]
        worst_row = all_by_strategy.sort_values("total_return", ascending=True).iloc[0]
        most_used_row = all_by_strategy.sort_values("num_trades", ascending=False).iloc[0]

        context["overall_win_rate"] = round(overall_win_rate, 1) if overall_win_rate is not None else None
        context["summary"] = {
            "strategies_count": int(len(all_by_strategy)),
            "total_trades": int(all_by_strategy["num_trades"].sum() or 0),
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

        # ── Trend data: monthly performance per strategy ──
        trend_df = pd.DataFrame()
        try:
            trend_df = client.query(
                STRATEGY_TREND_QUERY.format(account_filter=account_filter)
            ).to_dataframe()
        except Exception:
            pass

        # Build latest trend signal per strategy (from most recent month)
        latest_trend = {}
        recent_wr_3m = {}
        if not trend_df.empty and "month_start" in trend_df.columns:
            trend_df["month_start"] = pd.to_datetime(trend_df["month_start"])
            for col in ["trades_closed", "win_rate_pct", "total_pnl", "win_rate_3m_pct", "avg_pnl_per_trade"]:
                if col in trend_df.columns:
                    trend_df[col] = pd.to_numeric(trend_df[col], errors="coerce").fillna(0)

            # Aggregate across accounts per (strategy, month)
            agg_trend = trend_df.groupby(["strategy", "month_start"]).agg({
                "trades_closed": "sum",
                "win_rate_pct": "mean",
                "total_pnl": "sum",
                "avg_pnl_per_trade": "mean",
                "trend_signal": "first",
                "win_rate_3m_pct": "mean",
            }).reset_index()

            for strat in agg_trend["strategy"].unique():
                strat_rows = agg_trend[agg_trend["strategy"] == strat].sort_values("month_start")
                if not strat_rows.empty:
                    latest = strat_rows.iloc[-1]
                    latest_trend[strat] = str(latest.get("trend_signal", "stable"))
                    wr3 = latest.get("win_rate_3m_pct")
                    if wr3 and float(wr3) > 0:
                        recent_wr_3m[strat] = round(float(wr3), 1)

        strategies_list = []
        for _, row in all_by_strategy.sort_values("total_return", ascending=False).iterrows():
            wr = row["win_rate"]
            wr_vs_overall = (
                round(wr - overall_win_rate, 1)
                if wr is not None and overall_win_rate is not None
                else None
            )
            if wr_vs_overall is not None:
                if wr_vs_overall >= 10:
                    wr_signal = "above"
                elif wr_vs_overall <= -10:
                    wr_signal = "below"
                else:
                    wr_signal = "average"
            else:
                wr_signal = None

            strat_name = row["strategy"]
            signal = latest_trend.get(strat_name, "stable")

            # Build sparkline data: last 6 months of win rate
            sparkline = []
            if not trend_df.empty:
                strat_trend = trend_df[trend_df["strategy"] == strat_name].copy()
                if not strat_trend.empty:
                    agg_monthly = strat_trend.groupby("month_start").agg(
                        wr=("win_rate_pct", "mean"),
                        pnl=("total_pnl", "sum"),
                        trades=("trades_closed", "sum"),
                    ).reset_index().sort_values("month_start")
                    for _, m in agg_monthly.tail(6).iterrows():
                        sparkline.append({
                            "month": str(m["month_start"])[:7],
                            "win_rate": round(float(m["wr"]), 1),
                            "pnl": round(float(m["pnl"]), 2),
                            "trades": int(m["trades"]),
                        })

            strategies_list.append({
                "strategy": strat_name,
                "total_return": round(float(row["total_return"] or 0), 2),
                "realized_pnl": round(float(row["realized_pnl"] or 0), 2),
                "unrealized_pnl": round(float(row["unrealized_pnl"] or 0), 2),
                "num_trades": int(row["num_trades"] or 0),
                "num_winners": int(row["num_winners"] or 0),
                "num_losers": int(row["num_losers"] or 0),
                "win_rate": round(wr, 1) if wr is not None else None,
                "wr_vs_overall": wr_vs_overall,
                "wr_signal": wr_signal,
                "premium_received": round(float(row["premium_received"] or 0), 2),
                "premium_paid": round(float(row["premium_paid"] or 0), 2),
                "num_symbols": int(row["num_symbols"] or 0),
                "is_selected": bool(selected_strategy and strat_name == selected_strategy),
                "trend_signal": signal,
                "recent_wr_3m": recent_wr_3m.get(strat_name),
                "avg_days": round(float(row.get("avg_days_in_trade") or 0), 1),
                "sparkline": sparkline,
                "first_trade_date": str(row.get("first_trade_date", ""))[:10] if row.get("first_trade_date") is not None else None,
                "last_trade_date": str(row.get("last_trade_date", ""))[:10] if row.get("last_trade_date") is not None else None,
            })

        context["strategies"] = strategies_list
        context["narrative"] = _strategy_narrative(context["summary"], strategies_list, trend_df)

        # ── Focus detail for the selected strategy ──
        if selected_strategy:
            focus_rows = [s for s in strategies_list if s["strategy"] == selected_strategy]
            if focus_rows:
                context["focus_strategy"] = focus_rows[0]

                # Monthly trend data for chart
                if not trend_df.empty:
                    strat_trend = trend_df[trend_df["strategy"] == selected_strategy].copy()
                    if not strat_trend.empty:
                        agg = strat_trend.groupby("month_start").agg(
                            trades=("trades_closed", "sum"),
                            winners=("win_rate_pct", "mean"),
                            pnl=("total_pnl", "sum"),
                            avg_pnl=("avg_pnl_per_trade", "mean"),
                        ).reset_index().sort_values("month_start")
                        context["focus_trend_months"] = [
                            {
                                "month": str(m["month_start"])[:7],
                                "month_label": pd.to_datetime(m["month_start"]).strftime("%b %Y"),
                                "trades": int(m["trades"]),
                                "win_rate": round(float(m["winners"]), 1),
                                "pnl": round(float(m["pnl"]), 2),
                                "avg_pnl": round(float(m["avg_pnl"]), 2),
                            }
                            for _, m in agg.iterrows()
                        ]

                # DTE / moneyness breakdown
                try:
                    dte_cfg = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("strategy", "STRING", selected_strategy),
                        ]
                    )
                    dte_df = client.query(
                        DTE_MONEYNESS_QUERY.format(account_filter=account_filter),
                        job_config=dte_cfg,
                    ).to_dataframe()
                    if not dte_df.empty:
                        for col in ["num_trades", "total_pnl"]:
                            dte_df[col] = pd.to_numeric(dte_df[col], errors="coerce").fillna(0)
                        # Aggregate by DTE bucket
                        dte_agg = dte_df.groupby("dte_bucket").agg(
                            num_trades=("num_trades", "sum"),
                            total_pnl=("total_pnl", "sum"),
                        ).reset_index()
                        # Compute win rate per bucket from winner/loser rows
                        for bucket in dte_agg["dte_bucket"].unique():
                            bucket_rows = dte_df[dte_df["dte_bucket"] == bucket]
                            w = bucket_rows[bucket_rows["outcome"] == "Winner"]["num_trades"].sum()
                            l = bucket_rows[bucket_rows["outcome"] == "Loser"]["num_trades"].sum()
                            total = w + l
                            dte_agg.loc[dte_agg["dte_bucket"] == bucket, "win_rate_pct"] = (
                                round(w / total * 100, 1) if total > 0 else None
                            )

                        dte_list = []
                        for _, r in dte_agg.sort_values("num_trades", ascending=False).iterrows():
                            dte_list.append({
                                "dte_bucket": str(r["dte_bucket"]),
                                "num_trades": int(r["num_trades"]),
                                "total_pnl": round(float(r["total_pnl"]), 2),
                                "win_rate_pct": float(r["win_rate_pct"]) if r.get("win_rate_pct") is not None and not pd.isna(r.get("win_rate_pct")) else None,
                            })
                        context["focus_dte_breakdown"] = dte_list
                except Exception:
                    pass

                # Generate insights
                context["focus_insights"] = _focus_insights(
                    focus_rows[0],
                    overall_win_rate,
                    context.get("focus_trend_months", []),
                    context.get("focus_dte_breakdown", []),
                )

                # Per-account breakdown
                acct_df = df[df["strategy"] == selected_strategy].copy()
                if not acct_df.empty and len(acct_df) > 1:
                    acct_rows = []
                    for _, r in acct_df.iterrows():
                        raw_wr = r.get("win_rate")
                        acct_rows.append({
                            "account": r["account"],
                            "total_return": float(r.get("total_return") or 0),
                            "realized_pnl": float(r.get("realized_pnl") or 0),
                            "num_trades": int(r.get("num_trades") or 0),
                            "win_rate": float(raw_wr) * 100 if raw_wr is not None else None,
                        })
                    context["focus_accounts"] = acct_rows

                # Per-symbol breakdown
                try:
                    pos_query = STRATEGY_POSITIONS_QUERY.format(account_filter=account_filter)
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
                            })
                        context["focus_symbols"] = sym_rows
                except Exception:
                    pass

    except Exception as e:
        context["error"] = str(e)

    return render_template("strategies.html", **context)
