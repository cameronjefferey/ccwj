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
from app.models import is_admin


def _user_account_list():
    from app.routes import _user_account_list as _routes_user_account_list
    return _routes_user_account_list()


from app.routes import (  # noqa: E402
    _tenants_for_scope,
    _tenant_sql_and,
    _filter_df_by_tenant_ids,
)


STRATEGY_PERFORMANCE_QUERY = """
SELECT
  account,
  tenant_id,
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
WHERE 1=1 {tenant_filter}
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
WHERE 1=1 {tenant_filter}
ORDER BY strategy, month_start
"""

STRATEGY_POSITIONS_QUERY = """
SELECT
  account,
  tenant_id,
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
  {tenant_filter}
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
  {tenant_filter}
ORDER BY dte_bucket, moneyness_at_open
"""


STRATEGY_TYPE_BREAKDOWN_QUERY = """
SELECT
  trade_group_type,
  ROUND(SUM(realized_pnl), 2) AS realized_sum,
  ROUND(SUM(unrealized_pnl), 2) AS unrealized_sum,
  COUNT(*) AS num_groups,
  COUNTIF(status = 'Open') AS num_open_groups
FROM `ccwj-dbt.analytics.int_strategy_classification`
WHERE strategy = @strategy
  {tenant_filter}
GROUP BY 1
"""

STRATEGY_DIVIDEND_ROLLUP_QUERY = """
SELECT
  ROUND(SUM(IFNULL(total_dividend_income, 0)), 2) AS dividend_total,
  SUM(IFNULL(dividend_count, 0)) AS dividend_events
FROM `ccwj-dbt.analytics.positions_summary`
WHERE strategy = @strategy
  {tenant_filter}
"""


TYPE_LABEL_FOR_GROUP = {
    "equity_session": "Equity",
    "option_contract": "Options",
}


def _focus_breakdown_rows(breakdown_df: pd.DataFrame, dividend_total: float, dividend_events: int):
    """Build Breakdown-by-Type rows for the focused strategy drill-in.
    Inputs are tenant-scoped DataFrames / scalars."""
    buckets = {}

    def _accum(label: str, rsum, usum, ng, og):
        d = buckets.setdefault(
            label,
            {"realized": 0.0, "unrealized": 0.0, "groups": 0, "open_groups": 0},
        )
        rs = pd.to_numeric(rsum, errors="coerce")
        us = pd.to_numeric(usum, errors="coerce")
        d["realized"] += float(0 if rs is None or pd.isna(rs) else rs)
        d["unrealized"] += float(0 if us is None or pd.isna(us) else us)
        d["groups"] += int(ng or 0)
        d["open_groups"] += int(og or 0)

    if breakdown_df is not None and not breakdown_df.empty:
        for _, crow in breakdown_df.iterrows():
            tg = str(crow.get("trade_group_type") or "").strip()
            lbl = TYPE_LABEL_FOR_GROUP.get(tg, "Other")
            _accum(
                lbl,
                crow.get("realized_sum"),
                crow.get("unrealized_sum"),
                crow.get("num_groups"),
                crow.get("num_open_groups"),
            )

    xdiv = pd.to_numeric(dividend_total, errors="coerce")
    div_tot = round(float(0 if xdiv is None or pd.isna(xdiv) else xdiv), 2)
    div_ev_raw = pd.to_numeric(dividend_events if dividend_events is not None else 0, errors="coerce")
    div_ev = int(0 if div_ev_raw is None or pd.isna(div_ev_raw) else div_ev_raw)

    if not buckets and div_tot == 0.0:
        return []

    out_rows = []
    preferred = ["Equity", "Options", "Other"]

    for lbl in preferred:
        st = buckets.get(lbl)
        if not st:
            continue
        grp = int(st["groups"])
        open_g = int(st["open_groups"])
        suf_parts = []
        if grp > 0:
            suf_parts.append(f"{grp} {'group' if grp == 1 else 'groups'}")
        if open_g > 0:
            suf_parts.append(f"{open_g} open")

        total = round(st["realized"] + st["unrealized"], 2)

        out_rows.append(
            {
                "type": lbl,
                "total": total,
                "realized": round(float(st["realized"]), 2),
                "unrealized": round(float(st["unrealized"]), 2),
                "suffix": "(" + "; ".join(suf_parts) + ")" if suf_parts else "",
            }
        )

    if div_tot != 0.0 or div_ev > 0:
        ev_sfx = ""
        if div_ev > 0:
            ev_sfx = f"({div_ev} event{'s' if div_ev != 1 else ''})"
        out_rows.append(
            {
                "type": "Dividends",
                "total": div_tot,
                "realized": div_tot,
                "unrealized": None,
                "suffix": ev_sfx,
            }
        )

    # Equity / Options first, Dividends always last after Other
    return out_rows


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
    from app.routes import _redirect_if_no_accounts
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    user_accounts = _user_account_list()

    selected_account = request.args.get("account", "")
    selected_strategy = request.args.get("strategy", "")

    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

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
        "focus_breakdown_rows": [],
        "focus_trend_months": [],
        "focus_dte_breakdown": [],
        "accounts": [],
        "auth_accounts": sorted(user_accounts) if user_accounts else [],
        "selected_account": selected_account,
        "selected_strategy": selected_strategy,
    }

    try:
        client = get_bigquery_client()
        df = client.query(
            STRATEGY_PERFORMANCE_QUERY.format(tenant_filter=tenant_filter)
        ).to_dataframe()

        df = _filter_df_by_tenant_ids(df, tenant_ids)

        if df.empty:
            return render_template("strategies.html", **context)

        # Disambiguating label map so several physical accounts sharing a
        # base label (e.g. multiple "Schwab Account"s) read distinctly.
        from app.routes import _tenant_label_map_for_user
        try:
            _tlabel = _tenant_label_map_for_user(current_user.id)
        except Exception:
            _tlabel = {}

        def _acct_label(row):
            tid = row.get("tenant_id") if hasattr(row, "get") else None
            return (_tlabel.get(tid) if tid else None) or row.get("account")

        if "account" in df.columns:
            # Picker uses the user's full disambiguated account set when
            # available so each physical account is independently selectable.
            context["accounts"] = (
                sorted(user_accounts)
                if user_accounts
                else sorted(df["account"].dropna().unique().tolist())
            )

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
                STRATEGY_TREND_QUERY.format(tenant_filter=tenant_filter)
            ).to_dataframe()
            trend_df = _filter_df_by_tenant_ids(trend_df, tenant_ids)
        except Exception:
            app.logger.exception("mart_strategy_trend lookup failed")

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
            # pandas/numpy can produce NaN for groups with no closed trades;
            # treat those the same as None so we render an em-dash, not "nan%".
            if wr is None or pd.isna(wr):
                wr = None
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
                        m_wr = m["wr"]
                        sparkline.append({
                            "month": str(m["month_start"])[:7],
                            "win_rate": round(float(m_wr), 1) if pd.notna(m_wr) else None,
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

                # Breakdown by type (equity / options / dividends) for drill-in
                try:
                    strat_job = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("strategy", "STRING", selected_strategy),
                        ]
                    )
                    bdf = client.query(
                        STRATEGY_TYPE_BREAKDOWN_QUERY.format(tenant_filter=tenant_filter),
                        job_config=strat_job,
                    ).to_dataframe()
                    bdf = _filter_df_by_tenant_ids(bdf, tenant_ids)
                    div_df = client.query(
                        STRATEGY_DIVIDEND_ROLLUP_QUERY.format(tenant_filter=tenant_filter),
                        job_config=strat_job,
                    ).to_dataframe()
                    div_tot, div_ev = 0.0, 0
                    if not div_df.empty:
                        div_tot = float(div_df.iloc[0].get("dividend_total") or 0)
                        div_ev = int(div_df.iloc[0].get("dividend_events") or 0)
                    context["focus_breakdown_rows"] = _focus_breakdown_rows(bdf, div_tot, div_ev)
                except Exception:
                    app.logger.exception("strategy focus type breakdown failed")

                # DTE / moneyness breakdown
                try:
                    dte_cfg = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("strategy", "STRING", selected_strategy),
                        ]
                    )
                    dte_df = client.query(
                        DTE_MONEYNESS_QUERY.format(tenant_filter=tenant_filter),
                        job_config=dte_cfg,
                    ).to_dataframe()
                    dte_df = _filter_df_by_tenant_ids(dte_df, tenant_ids)
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
                    app.logger.exception("strategy DTE breakdown query failed")

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
                            "account": _acct_label(r),
                            "total_return": float(r.get("total_return") or 0),
                            "realized_pnl": float(r.get("realized_pnl") or 0),
                            "num_trades": int(r.get("num_trades") or 0),
                            "win_rate": float(raw_wr) * 100 if raw_wr is not None else None,
                        })
                    context["focus_accounts"] = acct_rows

                # Per-symbol breakdown
                try:
                    pos_query = STRATEGY_POSITIONS_QUERY.format(tenant_filter=tenant_filter)
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("strategy", "STRING", selected_strategy),
                        ]
                    )
                    pos_df = client.query(pos_query, job_config=job_config).to_dataframe()
                    pos_df = _filter_df_by_tenant_ids(pos_df, tenant_ids)
                    if not pos_df.empty:
                        sym_rows = []
                        for _, r in pos_df.iterrows():
                            sym_rows.append({
                                "account": _acct_label(r),
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
                    app.logger.exception("strategy positions_summary drill-in failed")

    except Exception as e:
        context["error"] = str(e)

    return render_template("strategies.html", **context)
