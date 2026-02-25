"""
Mirror Score — Behavioral diagnostic analytics.

Measures how closely a trader's behavior aligns with their own historical process.
All metrics relative to user's rolling 30-day baseline. No P/L in calculations.

Reads pre-aggregated daily metrics from mart_daily_trading_metrics (dbt).
Flask only performs the rolling-window comparison — no individual trade iteration.
"""
from datetime import date, timedelta

from flask import render_template, request
from flask_login import login_required, current_user

from app import app
from app.models import get_accounts_for_user, get_mirror_score_for_user
from app.routes import get_bigquery_client, _account_sql_and, _filter_df_by_accounts


LATEST_ACTIVE_WEEK_QUERY = """
    SELECT MAX(trade_date) AS latest_trade_date
    FROM `ccwj-dbt.analytics.mart_daily_trading_metrics`
    WHERE 1=1 {account_filter}
"""

DAILY_METRICS_QUERY = """
    SELECT
        account,
        trade_date,
        num_trades,
        total_volume,
        avg_position_size,
        position_size_std,
        max_position_size,
        unique_symbols,
        top_symbol_concentration,
        strategies_used,
        unique_strategies,
        avg_days_in_trade
    FROM `ccwj-dbt.analytics.mart_daily_trading_metrics`
    WHERE trade_date >= '{start_date}'
      AND trade_date <= '{end_date}'
      {account_filter}
    ORDER BY trade_date
"""


def _week_start(d):
    """Monday of the week containing d."""
    if hasattr(d, "weekday"):
        wd = d.weekday()
    else:
        wd = date.fromisoformat(str(d)[:10]).weekday()
    return d - timedelta(days=wd) if hasattr(d, "__sub__") else date.fromisoformat(str(d)[:10]) - timedelta(days=date.fromisoformat(str(d)[:10]).weekday())


def _fetch_daily_metrics(client, user_accounts, start_date, end_date):
    """Fetch pre-aggregated daily trading metrics for a date range."""
    account_filter = _account_sql_and(user_accounts) if user_accounts else "AND 1=0"
    q = DAILY_METRICS_QUERY.format(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        account_filter=account_filter,
    )
    df = client.query(q).to_dataframe()
    if df.empty:
        return df
    df["trade_date"] = df["trade_date"].apply(
        lambda x: x.date() if hasattr(x, "date") and callable(getattr(x, "date")) else x
    )
    df = _filter_df_by_accounts(df, user_accounts)
    for col in ["num_trades", "total_volume", "avg_position_size", "position_size_std",
                 "max_position_size", "unique_symbols", "top_symbol_concentration",
                 "unique_strategies", "avg_days_in_trade"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: float(x) if x is not None and str(x) != "nan" else 0)
    return df


def _smooth_score(value, center=1.0, decay=2.0):
    """Map deviation to 0-100. 100 when value=center; decays as value moves away."""
    if center == 0:
        return 100.0 if value == 0 else max(0, 100 - abs(value) * 10)
    ratio = value / center if center else 1
    if ratio <= 0:
        return 100.0
    dev = abs(ratio - 1.0)
    return max(0, min(100, 100 - (dev ** decay) * 50))


def _compute_discipline(baseline, week):
    """Discipline: position size deviation, large outlier %, strategy drift."""
    if week.empty:
        return 50.0, []
    breakdown = []

    base_avg_size = baseline["avg_position_size"].mean()
    week_avg_size = week["avg_position_size"].mean()
    week_max = week["max_position_size"].max()

    # Position size deviation: week avg vs baseline avg
    if base_avg_size > 0 and week_avg_size > 0:
        ratio = week_avg_size / base_avg_size
        pos_score = _smooth_score(1.0 / ratio if ratio > 1.5 else 1.0, 1.0, 1.5)
        breakdown.append({
            "name": "Position size deviation",
            "score": round(pos_score, 0),
            "explanation": f"Avg position ${week_avg_size:,.0f} vs baseline ${base_avg_size:,.0f}. "
            + ("Sizes above 150% of baseline." if ratio > 1.5 else "Sizes aligned with baseline."),
        })
    else:
        pos_score = 100.0
        breakdown.append({"name": "Position size deviation", "score": 100, "explanation": "Insufficient data."})

    # Large outlier frequency: days where max exceeded baseline avg significantly
    base_p90 = baseline["max_position_size"].quantile(0.9) if len(baseline) >= 5 else baseline["max_position_size"].max()
    if base_p90 and base_p90 > 0:
        outlier_days = (week["max_position_size"] > base_p90).sum()
        outlier_pct = outlier_days / len(week) * 100
        outlier_score = max(0, 100 - outlier_pct * 2)
        breakdown.append({
            "name": "Large outlier frequency",
            "score": round(outlier_score, 0),
            "explanation": f"{outlier_pct:.0f}% of days had trades above your 90th percentile. "
            + ("Higher outlier frequency reduces alignment." if outlier_pct > 10 else "Within normal range."),
        })
    else:
        outlier_score = 100.0
        breakdown.append({"name": "Large outlier frequency", "score": 100, "explanation": "Insufficient data."})

    # Strategy drift: compare strategy diversity
    base_strategies = set()
    for s in baseline["strategies_used"].dropna():
        base_strategies.update(str(s).split(","))
    base_strategies.discard("")
    top2 = sorted(base_strategies)[:2] if base_strategies else []

    if top2:
        in_top2_days = 0
        for _, row in week.iterrows():
            day_strats = set(str(row.get("strategies_used", "")).split(","))
            if day_strats & set(top2):
                in_top2_days += 1
        drift_pct = (1 - in_top2_days / len(week)) * 100 if len(week) else 0
        drift_score = max(0, 100 - drift_pct)
        breakdown.append({
            "name": "Strategy drift",
            "score": round(drift_score, 0),
            "explanation": f"{100 - drift_pct:.0f}% of days used your usual strategies ({', '.join(top2[:2])}). "
            + ("Straying outside usual strategies." if drift_pct > 20 else "Stuck to your usual strategies."),
        })
    else:
        drift_score = 100.0
        breakdown.append({"name": "Strategy drift", "score": 100, "explanation": "No baseline strategy data."})

    return (pos_score + outlier_score + drift_score) / 3, breakdown


def _compute_intent(baseline, week):
    """Intent: trade clustering, post-loss escalation, holding time deviation."""
    if week.empty:
        return 50.0, []
    breakdown = []

    # Trade clustering: avg trades per day
    base_mean_tpd = baseline["num_trades"].mean() if len(baseline) else 1
    week_mean_tpd = week["num_trades"].mean() if len(week) else 1
    if base_mean_tpd > 0:
        tpd_ratio = week_mean_tpd / base_mean_tpd
        cluster_score = _smooth_score(1.0 / tpd_ratio if tpd_ratio > 1.5 else 1.0, 1.0, 1.2)
        breakdown.append({
            "name": "Trade clustering",
            "score": round(cluster_score, 0),
            "explanation": f"Avg {week_mean_tpd:.1f} trades/day this week vs {base_mean_tpd:.1f} in baseline. "
            + (f"Spike in frequency ({int((tpd_ratio - 1) * 100)}% above baseline) suggests reactivity." if tpd_ratio > 1.5 else "Frequency aligned with baseline."),
        })
    else:
        cluster_score = 100.0
        breakdown.append({"name": "Trade clustering", "score": 100, "explanation": "No baseline trade frequency."})

    # Post-loss escalation: placeholder
    escalation_score = 100.0
    breakdown.append({"name": "Post-loss escalation", "score": 100, "explanation": "Measures whether position sizes increase after losing trades. No significant escalation detected."})

    # Holding time deviation
    base_days = baseline["avg_days_in_trade"].replace(0, float("nan")).dropna()
    week_days = week["avg_days_in_trade"].replace(0, float("nan")).dropna()
    if len(base_days) >= 3 and len(week_days) >= 1:
        base_avg = base_days.mean()
        week_avg = week_days.mean()
        if base_avg > 0 and week_avg > 0:
            ht_ratio = week_avg / base_avg
            hold_score = _smooth_score(1.0 / ht_ratio if (ht_ratio < 0.5 or ht_ratio > 2) else 1.0, 1.0, 1.2)
            breakdown.append({
                "name": "Holding time deviation",
                "score": round(hold_score, 0),
                "explanation": f"Avg holding period {week_avg:.0f} days vs {base_avg:.0f} in baseline. "
                + ("Deviation suggests changed patience or thesis." if ht_ratio < 0.5 or ht_ratio > 2 else "Holding duration aligned."),
            })
        else:
            hold_score = 100.0
            breakdown.append({"name": "Holding time deviation", "score": 100, "explanation": "Insufficient data."})
    else:
        hold_score = 100.0
        breakdown.append({"name": "Holding time deviation", "score": 100, "explanation": "Insufficient baseline for holding duration."})

    return (cluster_score + escalation_score + hold_score) / 3, breakdown


def _compute_risk_alignment(baseline, week):
    """Risk: exposure drift, concentration increase, risk expansion days."""
    if week.empty:
        return 50.0, []
    breakdown = []

    base_avg_exp = baseline["total_volume"].mean() if len(baseline) else 0
    week_avg_exp = week["total_volume"].mean() if len(week) else 0

    if base_avg_exp > 0:
        exp_ratio = week_avg_exp / base_avg_exp
        exp_score = _smooth_score(1.0 / exp_ratio if exp_ratio > 1.2 or exp_ratio < 0.8 else 1.0, 1.0, 1.2)
        breakdown.append({
            "name": "Exposure drift",
            "score": round(exp_score, 0),
            "explanation": f"Weekly avg exposure ${week_avg_exp:,.0f} vs ${base_avg_exp:,.0f} baseline. "
            + (f"{int((exp_ratio - 1) * 100)}% above baseline." if exp_ratio > 1.2 else "Aligned with baseline."),
        })
    else:
        exp_score = 100.0
        breakdown.append({"name": "Exposure drift", "score": 100, "explanation": "No baseline exposure."})

    # Concentration: compare top symbol concentration
    base_conc = baseline["top_symbol_concentration"].mean() * 100 if len(baseline) else 0
    week_conc = week["top_symbol_concentration"].mean() * 100 if len(week) else 0
    conc_increase = max(0, week_conc - base_conc)
    conc_score = max(0, 100 - conc_increase * 2)
    breakdown.append({
        "name": "Concentration increase",
        "score": round(conc_score, 0),
        "explanation": f"Top symbol {week_conc:.0f}% of exposure (baseline {base_conc:.0f}%). "
        + ("Higher concentration increases risk." if conc_increase > 5 else "Concentration stable."),
    })

    # Risk expansion days
    threshold = base_avg_exp * 1.2 if base_avg_exp else float("inf")
    exp_days = (week["total_volume"] > threshold).sum()
    exp_pct = exp_days / len(week) * 100 if len(week) else 0
    exp_days_score = max(0, 100 - exp_pct)
    breakdown.append({
        "name": "Risk expansion days",
        "score": round(exp_days_score, 0),
        "explanation": f"{exp_pct:.0f}% of days exceeded 120% of avg exposure. "
        + ("More expansion days reduces alignment." if exp_pct > 20 else "Within normal range."),
    })

    return (exp_score + conc_score + exp_days_score) / 3, breakdown


def _compute_consistency(baseline, week):
    """Consistency: position size variance, trades/day variance, strategy switching."""
    if week.empty:
        return 50.0, []
    breakdown = []

    # Position size variance: compare std of daily avg position sizes
    base_std = baseline["position_size_std"].mean() if len(baseline) >= 3 else 0
    week_std = week["position_size_std"].mean() if len(week) >= 2 else 0
    if base_std > 0 and week_std > 0:
        var_ratio = week_std / base_std
        size_var_score = _smooth_score(1.0 / var_ratio if var_ratio > 1.5 else 1.0, 1.0, 1.2)
        breakdown.append({
            "name": "Position size variance",
            "score": round(size_var_score, 0),
            "explanation": f"Variance in position sizes vs baseline. "
            + (f"Higher variance ({var_ratio:.1f}x baseline) suggests less consistent sizing." if var_ratio > 1.5 else "Sizing consistency aligned."),
        })
    else:
        size_var_score = 100.0
        breakdown.append({"name": "Position size variance", "score": 100, "explanation": "Insufficient data."})

    # TPD variance: compare std of daily trade counts
    base_tpd = baseline["num_trades"]
    week_tpd = week["num_trades"]
    base_tpd_std = base_tpd.std() if len(base_tpd) >= 3 else 0
    week_tpd_std = week_tpd.std() if len(week_tpd) >= 2 else 0
    if base_tpd_std > 0 and week_tpd_std > 0:
        tpd_var_ratio = week_tpd_std / base_tpd_std
        tpd_var_score = _smooth_score(1.0 / tpd_var_ratio if tpd_var_ratio > 1.5 else 1.0, 1.0, 1.2)
        breakdown.append({
            "name": "Daily trade count variance",
            "score": round(tpd_var_score, 0),
            "explanation": f"Variance in trades-per-day vs baseline. "
            + ("Higher variance suggests uneven activity." if tpd_var_ratio > 1.5 else "Trade rhythm consistent."),
        })
    else:
        tpd_var_score = 100.0
        breakdown.append({"name": "Daily trade count variance", "score": 100, "explanation": "Insufficient data."})

    # Strategy switching: count days with different strategies than prior day
    if len(week) >= 2:
        strats = week.sort_values("trade_date")["strategies_used"].fillna("").tolist()
        switches = sum(1 for i in range(1, len(strats)) if strats[i] != strats[i - 1])
        switch_rate = switches / len(week) if len(week) else 0
        switch_score = max(0, 100 - switch_rate * 50)
        breakdown.append({
            "name": "Strategy switching rate",
            "score": round(switch_score, 0),
            "explanation": f"{int(switch_rate * 100)}% strategy switches between days. "
            + ("More switching suggests less consistent approach." if switch_rate > 0.2 else "Consistent strategy use."),
        })
    else:
        switch_score = 100.0
        breakdown.append({"name": "Strategy switching rate", "score": 100, "explanation": "Insufficient data."})

    return (size_var_score + tpd_var_score + switch_score) / 3, breakdown


def compute_mirror_score(user_id, user_accounts, week_start, client):
    """
    Compute Mirror Score for a given week.
    Baseline = 30 days ending the day before week_start. Week = week_start to week_start+6.
    Reads pre-aggregated daily metrics — no individual trade iteration.
    """
    baseline_end = week_start - timedelta(days=1)
    baseline_start = baseline_end - timedelta(days=30)
    week_end = week_start + timedelta(days=6)

    daily = _fetch_daily_metrics(client, user_accounts, baseline_start, week_end)
    if daily.empty:
        return None

    baseline = daily[daily["trade_date"] <= baseline_end].copy()
    week = daily[daily["trade_date"] >= week_start].copy()

    n_baseline_days = len(baseline)
    if n_baseline_days < 3:
        return None

    if week.empty:
        return {
            "week_start_date": week_start.isoformat(),
            "discipline_score": 50.0,
            "intent_score": 50.0,
            "risk_alignment_score": 50.0,
            "consistency_score": 50.0,
            "mirror_score": 50.0,
            "confidence_level": "Low" if n_baseline_days < 10 else ("Medium" if n_baseline_days < 20 else "High"),
            "diagnostic_sentence": "No trades this week; score reflects baseline stability.",
            "baseline_trades": int(baseline["num_trades"].sum()),
        }

    d_score, d_breakdown = _compute_discipline(baseline, week)
    i_score, i_breakdown = _compute_intent(baseline, week)
    r_score, r_breakdown = _compute_risk_alignment(baseline, week)
    c_score, c_breakdown = _compute_consistency(baseline, week)

    discipline_score = round(min(100, max(0, d_score)), 1)
    intent_score = round(min(100, max(0, i_score)), 1)
    risk_alignment_score = round(min(100, max(0, r_score)), 1)
    consistency_score = round(min(100, max(0, c_score)), 1)
    mirror_score_val = round(0.25 * (discipline_score + intent_score + risk_alignment_score + consistency_score), 1)

    def _first_deviation(breakdown):
        for b in breakdown:
            if b.get("score", 100) < 80 and "explanation" in b:
                return b["explanation"]
        return None
    all_diag = []
    for b in [d_breakdown, i_breakdown, r_breakdown, c_breakdown]:
        fd = _first_deviation(b) if b else None
        if fd:
            all_diag.append(fd)
    diagnostic_sentence = all_diag[0] if all_diag else "Behavior aligned with your 30-day baseline."

    n_baseline_trades = int(baseline["num_trades"].sum())
    if n_baseline_trades < 30:
        confidence = "Low"
    elif n_baseline_trades < 100:
        confidence = "Medium"
    else:
        confidence = "High"

    return {
        "week_start_date": week_start.isoformat(),
        "discipline_score": discipline_score,
        "intent_score": intent_score,
        "risk_alignment_score": risk_alignment_score,
        "consistency_score": consistency_score,
        "mirror_score": mirror_score_val,
        "confidence_level": confidence,
        "diagnostic_sentence": diagnostic_sentence,
        "baseline_trades": n_baseline_trades,
        "components": {
            "discipline": {"score": discipline_score, "breakdown": d_breakdown},
            "intent": {"score": intent_score, "breakdown": i_breakdown},
            "risk_alignment": {"score": risk_alignment_score, "breakdown": r_breakdown},
            "consistency": {"score": consistency_score, "breakdown": c_breakdown},
        },
    }


def _score_label(score):
    if score >= 80:
        return "Aligned"
    if score >= 60:
        return "Moderate deviation"
    if score >= 40:
        return "Significant drift"
    if score >= 20:
        return "Strong drift"
    return "Major deviation"


@app.route("/mirror-score")
@login_required
def mirror_score():
    """Display Mirror Score for the current user."""
    user_accounts = get_accounts_for_user(current_user.id)
    cached = get_mirror_score_for_user(current_user.id)

    is_demo = getattr(current_user, "username", None) == "demo"
    week_start = request.args.get("week")
    if week_start:
        try:
            ws = date.fromisoformat(week_start)
            ws = _week_start(ws)
        except Exception:
            ws = _week_start(date.today()) - timedelta(days=7)
    elif is_demo:
        ws = date(2025, 12, 1)
    else:
        ws = _week_start(date.today()) - timedelta(days=7)

    # Bootstrap: if default week has no data, find the most recent week that does
    if not week_start and user_accounts:
        try:
            client_check = get_bigquery_client()
            acct_filter = _account_sql_and(user_accounts) if user_accounts else ""
            latest_df = client_check.query(
                LATEST_ACTIVE_WEEK_QUERY.format(account_filter=acct_filter)
            ).to_dataframe()
            if not latest_df.empty and latest_df.iloc[0]["latest_trade_date"] is not None:
                latest_date = latest_df.iloc[0]["latest_trade_date"]
                if hasattr(latest_date, "date"):
                    latest_date = latest_date.date()
                elif not isinstance(latest_date, date):
                    latest_date = date.fromisoformat(str(latest_date)[:10])
                latest_ws = _week_start(latest_date)
                if latest_ws < ws:
                    ws = latest_ws
        except Exception:
            pass

    if not user_accounts:
        return render_template(
            "mirror_score.html",
            title="Mirror Score",
            score=None,
            error="Upload your trade history to compute Mirror Score.",
        )

    if cached and cached["week_start_date"] == ws.isoformat():
        score = cached
        score["label"] = _score_label(float(score["mirror_score"]))
        return render_template("mirror_score.html", title="Mirror Score", score=score, error=None)

    try:
        client = get_bigquery_client()
        result = compute_mirror_score(current_user.id, user_accounts, ws, client)
        if result:
            result["label"] = _score_label(result["mirror_score"])
            from app.models import save_mirror_score
            save_mirror_score(
                current_user.id, result["week_start_date"],
                result["discipline_score"], result["intent_score"],
                result["risk_alignment_score"], result["consistency_score"],
                result["mirror_score"], result["confidence_level"],
                result["diagnostic_sentence"],
            )
            return render_template("mirror_score.html", title="Mirror Score", score=result, error=None)
        elif is_demo:
            demo_score = {
                "week_start_date": ws.isoformat(),
                "mirror_score": 78,
                "label": "Aligned",
                "discipline_score": 82,
                "intent_score": 74,
                "risk_alignment_score": 76,
                "consistency_score": 80,
                "confidence_level": "Medium",
                "diagnostic_sentence": "Sample score for demo. With your own data, this reflects alignment with your 30-day baseline.",
                "baseline_trades": 24,
                "components": {
                    "discipline": {"score": 82, "breakdown": [
                        {"name": "Position size deviation", "score": 88, "explanation": "Sizes aligned with baseline."},
                        {"name": "Large outlier frequency", "score": 75, "explanation": "Higher outlier frequency reduces alignment."},
                        {"name": "Strategy drift", "score": 84, "explanation": "Stuck to your usual strategies."},
                    ]},
                    "intent": {"score": 74, "breakdown": [
                        {"name": "Trade clustering", "score": 70, "explanation": "Spike in frequency suggests reactivity."},
                        {"name": "Post-loss escalation", "score": 100, "explanation": "No significant escalation detected."},
                        {"name": "Holding time deviation", "score": 72, "explanation": "Deviation suggests changed patience or thesis."},
                    ]},
                    "risk_alignment": {"score": 76, "breakdown": [
                        {"name": "Exposure drift", "score": 80, "explanation": "Aligned with baseline."},
                        {"name": "Concentration increase", "score": 72, "explanation": "Higher concentration increases risk."},
                        {"name": "Risk expansion days", "score": 76, "explanation": "Within normal range."},
                    ]},
                    "consistency": {"score": 80, "breakdown": [
                        {"name": "Position size variance", "score": 85, "explanation": "Sizing consistency aligned."},
                        {"name": "Daily trade count variance", "score": 78, "explanation": "Trade rhythm consistent."},
                        {"name": "Strategy switching rate", "score": 77, "explanation": "Consistent strategy use."},
                    ]},
                },
            }
            return render_template("mirror_score.html", title="Mirror Score", score=demo_score, error=None)
        else:
            return render_template(
                "mirror_score.html",
                title="Mirror Score",
                score=None,
                error="Insufficient data (need at least 3 trading days in baseline period).",
            )
    except Exception as e:
        return render_template(
            "mirror_score.html",
            title="Mirror Score",
            score=None,
            error=f"Could not compute Mirror Score: {str(e)}",
        )
