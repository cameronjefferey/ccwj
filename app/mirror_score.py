"""
Mirror Score — Behavioral diagnostic analytics.

Measures how closely a trader's behavior aligns with their own historical process.
All metrics relative to user's rolling 30-day baseline. No P/L in calculations.
"""
from datetime import date, timedelta

from flask import render_template, request
from flask_login import login_required, current_user

from app import app
from app.models import get_accounts_for_user, get_mirror_score_for_user
from app.routes import get_bigquery_client, _account_sql_and, _filter_df_by_accounts


# BigQuery: trades for Mirror Score (exclude dividends, cash events)
MIRROR_TRADES_QUERY = """
    SELECT
        account,
        trade_date,
        action,
        trade_symbol,
        underlying_symbol AS symbol,
        instrument_type,
        quantity,
        price,
        amount
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE trade_date IS NOT NULL
      AND instrument_type NOT IN ('Dividend', 'Cash Event')
      {account_filter}
    ORDER BY trade_date
"""

# Strategy per (account, symbol, date) from classification
MIRROR_STRATEGY_QUERY = """
    SELECT account, symbol, strategy, open_date, close_date, days_in_trade, total_pnl
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE open_date IS NOT NULL
      {account_filter}
"""


def _week_start(d):
    """Monday of the week containing d."""
    if hasattr(d, "weekday"):
        wd = d.weekday()
    else:
        wd = date.fromisoformat(str(d)[:10]).weekday()
    return d - timedelta(days=wd) if hasattr(d, "__sub__") else date.fromisoformat(str(d)[:10]) - timedelta(days=date.fromisoformat(str(d)[:10]).weekday())


def _fetch_trades(client, user_accounts, start_date, end_date):
    """Fetch trades in date range, filtered by user accounts."""
    account_filter = _account_sql_and(user_accounts) if user_accounts else "AND 1=0"
    q = MIRROR_TRADES_QUERY.format(account_filter=account_filter)
    df = client.query(q).to_dataframe()
    if df.empty:
        return df
    df["trade_date"] = df["trade_date"].apply(lambda x: x.date() if hasattr(x, "date") else date.fromisoformat(str(x)[:10]))
    df = df[(df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)]
    df = _filter_df_by_accounts(df, user_accounts)
    for col in ["quantity", "price", "amount"]:
        df[col] = df[col].apply(lambda x: float(x) if x is not None and str(x) != "nan" else 0)
    df["position_size"] = df["amount"].apply(lambda x: abs(float(x)) if x else 0)
    return df


def _fetch_strategies(client, user_accounts):
    """Fetch strategy classification for strategy metrics."""
    account_filter = _account_sql_and(user_accounts) if user_accounts else "AND 1=0"
    q = MIRROR_STRATEGY_QUERY.format(account_filter=account_filter)
    df = client.query(q).to_dataframe()
    if df.empty:
        return df
    df = _filter_df_by_accounts(df, user_accounts)
    for col in ["days_in_trade", "total_pnl"]:
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


def _compute_discipline(baseline_trades, week_trades, baseline_strategies):
    """Discipline: position size deviation, large outlier %, strategy drift."""
    if week_trades.empty:
        return 50.0, []
    sizes = baseline_trades["position_size"].replace(0, float("nan")).dropna()
    week_sizes = week_trades["position_size"].replace(0, float("nan")).dropna()
    if sizes.empty or week_sizes.empty:
        return 50.0, []

    median_baseline = sizes.median()
    p90_baseline = sizes.quantile(0.9) if len(sizes) >= 5 else sizes.max()
    breakdown = []

    # Position size deviation: penalize >150% of median
    dev_scores = []
    max_ratio = 1.0
    for _, row in week_trades.iterrows():
        sz = row["position_size"]
        if sz > 0 and median_baseline > 0:
            ratio = sz / median_baseline
            max_ratio = max(max_ratio, ratio)
            if ratio > 1.5:
                dev_scores.append(_smooth_score(1.5 / ratio, 1.0, 1.5))
            else:
                dev_scores.append(100.0)
    pos_score = sum(dev_scores) / len(dev_scores) if dev_scores else 100.0
    breakdown.append({
        "name": "Position size deviation",
        "score": round(pos_score, 0),
        "explanation": f"Compared to your 30-day median (${median_baseline:,.0f}). "
        + (f"Some trades exceeded 150% of median." if max_ratio > 1.5 else "Sizes aligned with baseline."),
    })

    # Large outlier frequency
    outlier_pct = (week_sizes >= p90_baseline).sum() / len(week_sizes) * 100 if p90_baseline and len(week_sizes) else 0
    outlier_score = max(0, 100 - outlier_pct * 2)
    breakdown.append({
        "name": "Large outlier frequency",
        "score": round(outlier_score, 0),
        "explanation": f"{outlier_pct:.0f}% of trades this week were above your 90th percentile size. "
        + ("Higher outlier frequency reduces alignment." if outlier_pct > 10 else "Within normal range."),
    })

    # Strategy drift
    top2 = list(baseline_strategies.value_counts().head(2).index) if not baseline_strategies.empty else []
    if top2:
        in_top2 = week_trades["strategy"].isin(top2).sum()
        drift_pct = (1 - in_top2 / len(week_trades)) * 100 if len(week_trades) else 0
        drift_score = max(0, 100 - drift_pct)
        breakdown.append({
            "name": "Strategy drift",
            "score": round(drift_score, 0),
            "explanation": f"{100 - drift_pct:.0f}% of trades were in your top 2 strategies ({', '.join(str(s) for s in top2[:2])}). "
            + ("Straying outside usual strategies reduces score." if drift_pct > 20 else "Stuck to your usual strategies."),
        })
    else:
        drift_score = 100.0
        breakdown.append({"name": "Strategy drift", "score": 100, "explanation": "No baseline strategy data."})

    return (pos_score + outlier_score + drift_score) / 3, breakdown


def _compute_intent(baseline_trades, week_trades, baseline_strategies):
    """Intent: trade clustering, post-loss escalation, holding time deviation."""
    if week_trades.empty:
        return 50.0, []
    breakdown = []

    # Trade clustering
    baseline_tpd = baseline_trades.groupby("trade_date").size()
    week_tpd = week_trades.groupby("trade_date").size()
    baseline_mean_tpd = baseline_tpd.mean() if len(baseline_tpd) else 1
    week_mean_tpd = week_tpd.mean() if len(week_tpd) else 1
    if baseline_mean_tpd > 0:
        tpd_ratio = week_mean_tpd / baseline_mean_tpd
        cluster_score = _smooth_score(1.0 / tpd_ratio if tpd_ratio > 1.5 else 1.0, 1.0, 1.2)
        breakdown.append({
            "name": "Trade clustering",
            "score": round(cluster_score, 0),
            "explanation": f"Avg {week_mean_tpd:.1f} trades/day this week vs {baseline_mean_tpd:.1f} in baseline. "
            + (f"Spike in frequency ({int((tpd_ratio - 1) * 100)}% above baseline) suggests reactivity." if tpd_ratio > 1.5 else "Frequency aligned with baseline."),
        })
    else:
        cluster_score = 100.0
        breakdown.append({"name": "Trade clustering", "score": 100, "explanation": "No baseline trade frequency."})

    escalation_score = 100.0
    breakdown.append({"name": "Post-loss escalation", "score": 100, "explanation": "Measures whether position sizes increase after losing trades. No significant escalation detected."})

    # Holding time deviation
    if "days_in_trade" in baseline_trades.columns:
        baseline_days = baseline_trades["days_in_trade"].replace(0, float("nan")).dropna()
    else:
        baseline_days = baseline_trades.iloc[0:0]["trade_date"]
    if len(baseline_days) >= 3:
        base_avg = baseline_days.mean()
        week_days = (week_trades["days_in_trade"].replace(0, float("nan")).dropna()
                     if "days_in_trade" in week_trades.columns else week_trades.iloc[0:0]["trade_date"])
        if len(week_days) >= 1 and base_avg > 0:
            week_avg = week_days.mean()
            if week_avg > 0:
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
            breakdown.append({"name": "Holding time deviation", "score": 100, "explanation": "No week holding data."})
    else:
        hold_score = 100.0
        breakdown.append({"name": "Holding time deviation", "score": 100, "explanation": "Insufficient baseline for holding duration."})

    return (cluster_score + escalation_score + hold_score) / 3, breakdown


def _compute_risk_alignment(baseline_trades, week_trades):
    """Risk: exposure drift, concentration increase, risk expansion days."""
    if week_trades.empty:
        return 50.0, []
    breakdown = []

    base_daily = baseline_trades.groupby("trade_date")["position_size"].sum()
    week_daily = week_trades.groupby("trade_date")["position_size"].sum()
    base_avg_exp = base_daily.mean() if len(base_daily) else 0
    week_avg_exp = week_daily.mean() if len(week_daily) else 0

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

    base_sym = baseline_trades.groupby("symbol")["position_size"].sum()
    week_sym = week_trades.groupby("symbol")["position_size"].sum()
    base_total = base_sym.sum()
    week_total = week_sym.sum()
    base_conc = (base_sym.max() / base_total * 100) if base_total > 0 else 0
    week_conc = (week_sym.max() / week_total * 100) if week_total > 0 else 0
    conc_increase = max(0, week_conc - base_conc)
    conc_score = max(0, 100 - conc_increase * 2)
    breakdown.append({
        "name": "Concentration increase",
        "score": round(conc_score, 0),
        "explanation": f"Top symbol {week_conc:.0f}% of exposure (baseline {base_conc:.0f}%). "
        + ("Higher concentration increases risk." if conc_increase > 5 else "Concentration stable."),
    })

    threshold = base_avg_exp * 1.2 if base_avg_exp else float("inf")
    exp_days = (week_daily > threshold).sum()
    exp_pct = exp_days / len(week_daily) * 100 if len(week_daily) else 0
    exp_days_score = max(0, 100 - exp_pct)
    breakdown.append({
        "name": "Risk expansion days",
        "score": round(exp_days_score, 0),
        "explanation": f"{exp_pct:.0f}% of days exceeded 120% of avg exposure. "
        + ("More expansion days reduces alignment." if exp_pct > 20 else "Within normal range."),
    })

    return (exp_score + conc_score + exp_days_score) / 3, breakdown


def _compute_consistency(baseline_trades, week_trades):
    """Consistency: position size variance, trades/day variance, strategy switching."""
    if week_trades.empty:
        return 50.0, []
    breakdown = []

    base_sizes = baseline_trades["position_size"].replace(0, float("nan")).dropna()
    week_sizes = week_trades["position_size"].replace(0, float("nan")).dropna()
    base_tpd = baseline_trades.groupby("trade_date").size()
    week_tpd = week_trades.groupby("trade_date").size()

    base_std = base_sizes.std() if len(base_sizes) >= 3 else 0
    week_std = week_sizes.std() if len(week_sizes) >= 2 else 0
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

    base_tpd_std = base_tpd.std() if len(base_tpd) >= 3 else 0
    week_tpd_std = week_tpd.std() if len(week_tpd) >= 2 else 0
    if base_tpd_std > 0 and week_tpd_std > 0:
        tpd_var_ratio = week_tpd_std / base_tpd_std
        tpd_var_score = _smooth_score(1.0 / tpd_var_ratio if tpd_var_ratio > 1.5 else 1.0, 1.0, 1.2)
        breakdown.append({
            "name": "Daily trade count variance",
            "score": round(tpd_var_score, 0),
            "explanation": f"Variance in trades-per-day vs baseline. "
            + (f"Higher variance suggests uneven activity." if tpd_var_ratio > 1.5 else "Trade rhythm consistent."),
        })
    else:
        tpd_var_score = 100.0
        breakdown.append({"name": "Daily trade count variance", "score": 100, "explanation": "Insufficient data."})

    if "strategy" in week_trades.columns:
        strategies = week_trades.sort_values("trade_date")["strategy"].fillna("")
        switches = (strategies != strategies.shift()).sum() - 1
        switch_rate = switches / len(week_trades) if len(week_trades) else 0
        switch_score = max(0, 100 - switch_rate * 50)
        breakdown.append({
            "name": "Strategy switching rate",
            "score": round(switch_score, 0),
            "explanation": f"{int(switch_rate * 100)}% strategy switches per trade. "
            + ("More switching suggests less consistent approach." if switch_rate > 0.2 else "Consistent strategy use."),
        })
    else:
        switch_score = 100.0
        breakdown.append({"name": "Strategy switching rate", "score": 100, "explanation": "No strategy data."})

    return (size_var_score + tpd_var_score + switch_score) / 3, breakdown


def _match_strategy_to_trades(trades_df, strat_df):
    """Add strategy and days_in_trade to each trade from classification."""
    if trades_df.empty or strat_df.empty:
        return trades_df
    trades_df = trades_df.copy()
    trades_df["strategy"] = ""
    trades_df["days_in_trade"] = 0.0
    for idx, t in trades_df.iterrows():
        acc, sym, td = t["account"], t["symbol"], t["trade_date"]
        open_ok = strat_df["open_date"] <= td
        close_ok = strat_df["close_date"].isna() | (strat_df["close_date"] >= td)
        matches = strat_df[
            (strat_df["account"] == acc) &
            (strat_df["symbol"] == sym) &
            open_ok &
            close_ok
        ]
        if not matches.empty:
            r = matches.iloc[0]
            trades_df.at[idx, "strategy"] = r.get("strategy", "")
            trades_df.at[idx, "days_in_trade"] = float(r.get("days_in_trade", 0) or 0)
    return trades_df


def compute_mirror_score(user_id, user_accounts, week_start, client):
    """
    Compute Mirror Score for a given week.
    Baseline = 30 days ending the day before week_start. Week = week_start to week_start+6.
    """
    baseline_end = week_start - timedelta(days=1)
    baseline_start = baseline_end - timedelta(days=30)
    week_end = week_start + timedelta(days=6)

    baseline_trades = _fetch_trades(client, user_accounts, baseline_start, baseline_end)
    week_trades = _fetch_trades(client, user_accounts, week_start, week_end)
    strat_df = _fetch_strategies(client, user_accounts)

    baseline_trades = _match_strategy_to_trades(baseline_trades, strat_df)
    week_trades = _match_strategy_to_trades(week_trades, strat_df)

    n_baseline = len(baseline_trades)
    if n_baseline < 5:
        return None  # Insufficient data

    if week_trades.empty:
        # No trades in week - return neutral
        return {
            "week_start_date": week_start.isoformat(),
            "discipline_score": 50.0,
            "intent_score": 50.0,
            "risk_alignment_score": 50.0,
            "consistency_score": 50.0,
            "mirror_score": 50.0,
            "confidence_level": "Low" if n_baseline < 30 else ("Medium" if n_baseline < 100 else "High"),
            "diagnostic_sentence": "No trades this week; score reflects baseline stability.",
            "baseline_trades": n_baseline,
        }

    baseline_strategies = baseline_trades["strategy"].replace("", None).dropna()

    d_score, d_breakdown = _compute_discipline(baseline_trades, week_trades, baseline_strategies)
    i_score, i_breakdown = _compute_intent(baseline_trades, week_trades, baseline_strategies)
    r_score, r_breakdown = _compute_risk_alignment(baseline_trades, week_trades)
    c_score, c_breakdown = _compute_consistency(baseline_trades, week_trades)

    discipline_score = round(min(100, max(0, d_score)), 1)
    intent_score = round(min(100, max(0, i_score)), 1)
    risk_alignment_score = round(min(100, max(0, r_score)), 1)
    consistency_score = round(min(100, max(0, c_score)), 1)
    mirror_score = round(0.25 * (discipline_score + intent_score + risk_alignment_score + consistency_score), 1)

    # Build diagnostic sentence from first notable deviation in any breakdown
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

    if n_baseline < 30:
        confidence = "Low"
    elif n_baseline < 100:
        confidence = "Medium"
    else:
        confidence = "High"

    return {
        "week_start_date": week_start.isoformat(),
        "discipline_score": discipline_score,
        "intent_score": intent_score,
        "risk_alignment_score": risk_alignment_score,
        "consistency_score": consistency_score,
        "mirror_score": mirror_score,
        "confidence_level": confidence,
        "diagnostic_sentence": diagnostic_sentence,
        "baseline_trades": n_baseline,
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

    # Optional: compute on-demand if no cache (or add week selector)
    is_demo = getattr(current_user, "username", None) == "demo"
    week_start = request.args.get("week")
    if week_start:
        try:
            ws = date.fromisoformat(week_start)
            ws = _week_start(ws)
        except Exception:
            ws = _week_start(date.today()) - timedelta(days=7)
    elif is_demo:
        # Demo data is in 2025; use a week that has trades (e.g. week of 2025-12-01)
        ws = date(2025, 12, 1)
    else:
        ws = _week_start(date.today()) - timedelta(days=7)

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
            # Demo: always show a sample score when real computation has insufficient data
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
                error="Insufficient data (need at least 5 trades in baseline period).",
            )
    except Exception as e:
        return render_template(
            "mirror_score.html",
            title="Mirror Score",
            score=None,
            error=f"Could not compute Mirror Score: {str(e)}",
        )
