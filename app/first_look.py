"""
First Look — the "Here's what we found" page shown after first upload.

Queries positions_summary and int_strategy_classification to build a
narrative profile of the trader. Designed to deliver at least one
genuine insight on day one.
"""
from flask import render_template, redirect, url_for
from flask_login import login_required, current_user
from app import app
from app.bigquery_client import get_bigquery_client
from app.routes import _account_sql_filter, _account_sql_and, _user_account_list
import pandas as pd


PROFILE_QUERY = """
    SELECT
        COUNT(DISTINCT symbol) AS num_symbols,
        COUNT(DISTINCT strategy) AS num_strategies,
        SUM(num_individual_trades) AS total_trades,
        SUM(total_return) AS total_return,
        SUM(realized_pnl) AS realized_pnl,
        SUM(unrealized_pnl) AS unrealized_pnl,
        SUM(total_premium_received) AS premium_collected,
        SUM(total_dividend_income) AS dividend_income,
        SUM(num_winners) AS total_winners,
        SUM(num_losers) AS total_losers,
        MIN(first_trade_date) AS first_trade,
        MAX(last_trade_date) AS last_trade,
        ROUND(AVG(avg_days_in_trade), 1) AS avg_holding_days
    FROM `ccwj-dbt.analytics.positions_summary`
    {where}
"""

STRATEGY_QUERY = """
    SELECT
        strategy,
        SUM(total_return) AS total_return,
        SUM(num_winners) AS winners,
        SUM(num_losers) AS losers,
        COUNT(DISTINCT symbol) AS num_symbols,
        SUM(num_individual_trades) AS total_trades,
        ROUND(AVG(avg_days_in_trade), 1) AS avg_days
    FROM `ccwj-dbt.analytics.positions_summary`
    {where}
    GROUP BY strategy
    ORDER BY SUM(total_return) DESC
"""

SYMBOL_QUERY = """
    SELECT
        symbol,
        SUM(total_return) AS total_return,
        STRING_AGG(DISTINCT strategy, ', ' ORDER BY strategy) AS strategies
    FROM `ccwj-dbt.analytics.positions_summary`
    {where}
    GROUP BY symbol
    ORDER BY SUM(total_return) DESC
"""

WIN_LOSS_QUERY = """
    SELECT
        AVG(CASE WHEN is_winner AND status = 'Closed' THEN total_pnl END) AS avg_win,
        AVG(CASE WHEN NOT is_winner AND status = 'Closed' THEN total_pnl END) AS avg_loss
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    {where}
"""

BUSIEST_MONTH_QUERY = """
    SELECT
        FORMAT_DATE('%%Y-%%m', open_date) AS month,
        COUNT(*) AS trades_opened
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE open_date IS NOT NULL {account_filter}
    GROUP BY 1
    ORDER BY trades_opened DESC
    LIMIT 1
"""


def _safe_float(val, default=0.0):
    try:
        f = float(val)
        return f if str(f) != "nan" else default
    except (TypeError, ValueError):
        return default


def _safe_int(val, default=0):
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return default


def _build_profile(client, where, acct_and):
    """Build the trader profile dict from BigQuery data."""
    profile = {}

    # Overall stats
    df = client.query(PROFILE_QUERY.format(where=where)).to_dataframe()
    if df.empty:
        return None
    r = df.iloc[0]
    total_winners = _safe_int(r.get("total_winners"))
    total_losers = _safe_int(r.get("total_losers"))
    total_closed = total_winners + total_losers
    profile["num_symbols"] = _safe_int(r.get("num_symbols"))
    profile["num_strategies"] = _safe_int(r.get("num_strategies"))
    profile["total_trades"] = _safe_int(r.get("total_trades"))
    profile["total_return"] = _safe_float(r.get("total_return"))
    profile["realized_pnl"] = _safe_float(r.get("realized_pnl"))
    profile["unrealized_pnl"] = _safe_float(r.get("unrealized_pnl"))
    profile["premium_collected"] = _safe_float(r.get("premium_collected"))
    profile["dividend_income"] = _safe_float(r.get("dividend_income"))
    profile["total_winners"] = total_winners
    profile["total_losers"] = total_losers
    profile["win_rate"] = total_winners / total_closed if total_closed else 0
    profile["first_trade"] = str(r.get("first_trade", ""))[:10]
    profile["last_trade"] = str(r.get("last_trade", ""))[:10]
    profile["avg_holding_days"] = _safe_float(r.get("avg_holding_days"))

    if profile["total_trades"] == 0:
        return None

    # Date span in months
    try:
        from datetime import datetime
        ft = datetime.strptime(profile["first_trade"][:10], "%Y-%m-%d")
        lt = datetime.strptime(profile["last_trade"][:10], "%Y-%m-%d")
        profile["months_active"] = max(1, round((lt - ft).days / 30))
    except Exception:
        profile["months_active"] = 0

    # Strategies
    strat_df = client.query(STRATEGY_QUERY.format(where=where)).to_dataframe()
    strategies = []
    if not strat_df.empty:
        for col in ["total_return", "winners", "losers", "num_symbols", "total_trades", "avg_days"]:
            strat_df[col] = pd.to_numeric(strat_df[col], errors="coerce").fillna(0)
        for _, sr in strat_df.iterrows():
            closed = int(sr["winners"] + sr["losers"])
            strategies.append({
                "strategy": sr["strategy"],
                "total_return": float(sr["total_return"]),
                "win_rate": float(sr["winners"]) / closed if closed else 0,
                "num_symbols": int(sr["num_symbols"]),
                "total_trades": int(sr["total_trades"]),
                "avg_days": float(sr["avg_days"]),
            })
    profile["strategies"] = strategies
    if strategies:
        profile["top_strategy"] = strategies[0]

    # Symbols — best and worst
    sym_df = client.query(SYMBOL_QUERY.format(where=where)).to_dataframe()
    if not sym_df.empty:
        sym_df["total_return"] = pd.to_numeric(sym_df["total_return"], errors="coerce").fillna(0)
        best = sym_df.iloc[0]
        worst = sym_df.iloc[-1] if len(sym_df) > 1 else None
        profile["best_symbol"] = {
            "symbol": best["symbol"],
            "total_return": float(best["total_return"]),
            "strategies": best.get("strategies", ""),
        }
        if worst is not None and float(worst["total_return"]) < 0:
            profile["worst_symbol"] = {
                "symbol": worst["symbol"],
                "total_return": float(worst["total_return"]),
                "strategies": worst.get("strategies", ""),
            }
        else:
            profile["worst_symbol"] = None

        # Concentration: top 3 symbols as % of total absolute return
        total_abs = sym_df["total_return"].abs().sum()
        top3_abs = sym_df.head(3)["total_return"].abs().sum()
        profile["top3_concentration"] = (top3_abs / total_abs * 100) if total_abs > 0 else 0
        profile["top3_symbols"] = sym_df.head(3)["symbol"].tolist()
    else:
        profile["best_symbol"] = None
        profile["worst_symbol"] = None
        profile["top3_concentration"] = 0
        profile["top3_symbols"] = []

    # Win/loss asymmetry — the core insight
    wl_df = client.query(WIN_LOSS_QUERY.format(where=where)).to_dataframe()
    if not wl_df.empty:
        avg_win = _safe_float(wl_df.iloc[0].get("avg_win"))
        avg_loss = _safe_float(wl_df.iloc[0].get("avg_loss"))
        profile["avg_win"] = avg_win
        profile["avg_loss"] = avg_loss
        # Profit factor: do your wins outweigh your losses?
        if avg_loss != 0 and avg_win != 0:
            profile["profit_factor"] = abs(avg_win / avg_loss)
        else:
            profile["profit_factor"] = None
    else:
        profile["avg_win"] = 0
        profile["avg_loss"] = 0
        profile["profit_factor"] = None

    # Busiest month
    try:
        bm_df = client.query(
            BUSIEST_MONTH_QUERY.format(account_filter=acct_and)
        ).to_dataframe()
        if not bm_df.empty:
            profile["busiest_month"] = str(bm_df.iloc[0]["month"])
            profile["busiest_month_trades"] = int(bm_df.iloc[0]["trades_opened"])
        else:
            profile["busiest_month"] = None
    except Exception:
        profile["busiest_month"] = None

    # Build the "one thing to think about" — pick the most interesting insight
    profile["key_insight"] = _pick_key_insight(profile)

    return profile


def _pick_key_insight(p):
    """Pick the single most interesting insight from the profile."""
    insights = []

    # Win/loss asymmetry
    if p.get("profit_factor") is not None and p["win_rate"] > 0:
        pf = p["profit_factor"]
        wr = p["win_rate"]
        if pf < 1.0 and wr > 0.5:
            insights.append({
                "priority": 10,
                "title": "Your wins are smaller than your losses",
                "body": (
                    f"You win {wr:.0%} of the time — that's solid. But your average win "
                    f"(${abs(p['avg_win']):,.0f}) is smaller than your average loss "
                    f"(${abs(p['avg_loss']):,.0f}). That means a single bad trade can erase "
                    f"multiple winners. This is the most common pattern in options trading "
                    f"— and the most important one to watch."
                ),
                "action": "Look at your biggest loss this month. Was it a thesis you held too long, or a stop you didn't set?",
            })
        elif pf > 2.0:
            insights.append({
                "priority": 5,
                "title": "Your edge is real",
                "body": (
                    f"Your average win (${abs(p['avg_win']):,.0f}) is {pf:.1f}x your average "
                    f"loss (${abs(p['avg_loss']):,.0f}). That's a strong profit factor. "
                    f"The question isn't whether your strategy works — it's whether you can "
                    f"stay disciplined when it doesn't."
                ),
                "action": "The Mirror Score is designed to track exactly this. Check it weekly.",
            })

    # Concentration risk
    if p.get("top3_concentration", 0) > 70 and p.get("num_symbols", 0) > 5:
        syms = ", ".join(p.get("top3_symbols", [])[:3])
        insights.append({
            "priority": 8,
            "title": f"You're concentrated in {syms}",
            "body": (
                f"Your top 3 symbols account for {p['top3_concentration']:.0f}% of your total returns. "
                f"That's a lot of exposure to a small number of names. If one of them moves against you "
                f"significantly, it affects your whole portfolio."
            ),
            "action": "Ask yourself: if your biggest position dropped 20% tomorrow, would you be okay?",
        })

    # Short holding period + high frequency
    if p.get("avg_holding_days", 0) < 10 and p.get("total_trades", 0) > 50:
        insights.append({
            "priority": 7,
            "title": "You trade frequently with short holds",
            "body": (
                f"Your average holding period is {p['avg_holding_days']:.0f} days across "
                f"{p['total_trades']} trades. Fast turnover can work, but it also means more "
                f"decisions, more opportunities for emotion to creep in, and more friction costs."
            ),
            "action": "Track how you feel before your next trade. If you're not calm, wait.",
        })

    # Low win rate but still profitable
    if p.get("win_rate", 0) < 0.5 and p.get("total_return", 0) > 0:
        insights.append({
            "priority": 6,
            "title": "You're profitable despite losing more often than winning",
            "body": (
                f"Your win rate is {p['win_rate']:.0%}, but your total return is "
                f"${p['total_return']:,.0f}. That means when you win, you win big. "
                f"This is a valid approach — but it requires patience and confidence in "
                f"your thesis during losing streaks."
            ),
            "action": "Journal your biggest winners. What did they have in common?",
        })

    # Only one strategy
    if p.get("num_strategies", 0) == 1 and p.get("strategies"):
        strat = p["strategies"][0]["strategy"]
        insights.append({
            "priority": 4,
            "title": f"You're all-in on {strat}",
            "body": (
                f"Every position uses {strat}. That's focus — which is good. But it also means "
                f"you're exposed to environments where {strat} underperforms. Understanding "
                f"when your strategy works best is as important as the strategy itself."
            ),
            "action": "Think about what market conditions would make this strategy struggle.",
        })

    # Default insight
    if not insights:
        insights.append({
            "priority": 1,
            "title": "You've been trading — now start reflecting",
            "body": (
                f"You have {p.get('total_trades', 0)} trades across "
                f"{p.get('num_symbols', 0)} symbols. The data is here. "
                f"The question is: what patterns are hiding in it?"
            ),
            "action": "Start a journal entry for your most recent trade. Just write the thesis — one sentence is enough.",
        })

    return max(insights, key=lambda i: i["priority"])


@app.route("/first-look")
@login_required
def first_look():
    """Post-upload 'Here's what we found' page."""
    user_accounts = _user_account_list()
    has_accounts = user_accounts is None or len(user_accounts) > 0

    if not has_accounts:
        return redirect(url_for("upload"))

    try:
        client = get_bigquery_client()
        where = _account_sql_filter(user_accounts)
        acct_and = _account_sql_and(user_accounts) if user_accounts else ""
        profile = _build_profile(client, where, acct_and)

        if not profile:
            return redirect(url_for("weekly_review"))

        return render_template(
            "first_look.html",
            title="Your Trading Profile",
            profile=profile,
        )
    except Exception as e:
        return redirect(url_for("weekly_review"))
