import os
import re
from google import genai
from google.genai import types
import pandas as pd
import markupsafe
from flask import render_template, request, redirect, url_for, flash, jsonify, abort
from flask_login import login_required, current_user
from google.cloud import bigquery as bq

from app import app
from app.bigquery_client import get_bigquery_client
from app.models import (
    get_accounts_for_user, is_admin,
    save_insight, get_insight_for_user,
)


def _account_sql_filter(accounts):
    """Build a SQL WHERE clause for filtering by account."""
    if accounts is None:
        return ""
    if not accounts:
        return "WHERE 1 = 0"
    quoted = ", ".join(f"'{a.replace(chr(39), chr(39)+chr(39))}'" for a in accounts)
    return f"WHERE account IN ({quoted})"


def _account_sql_and(accounts):
    """Build a SQL AND clause for queries that already have a WHERE."""
    if accounts is None:
        return ""
    if not accounts:
        return "AND 1 = 0"
    quoted = ", ".join(f"'{a.replace(chr(39), chr(39)+chr(39))}'" for a in accounts)
    return f"AND account IN ({quoted})"


# ------------------------------------------------------------------
# Queries — coaching signals from unique data
# ------------------------------------------------------------------

COACHING_SIGNALS_QUERY = """
SELECT
    account, strategy,
    total_closed, reliable_contracts, pct_contracts_reliable,
    avg_giveback_pct, avg_pnl_given_back, avg_days_held_past_peak,
    optimal_exit_rate, avg_pct_premium_captured, avg_actual_pnl,
    total_pnl_given_back,
    num_rolls, avg_dte_at_roll, roll_success_rate, avg_roll_credit,
    rolls_early, rolls_late, early_roll_success_rate, late_roll_success_rate,
    best_dte_bucket, best_dte_win_rate, best_dte_trades,
    worst_dte_bucket, worst_dte_win_rate, worst_dte_trades
FROM `ccwj-dbt.analytics.mart_coaching_signals`
{where}
ORDER BY total_pnl_given_back DESC
"""

RECENT_EXITS_QUERY = """
SELECT
    trade_symbol, underlying_symbol, strategy, direction,
    open_date, close_date, close_type, days_in_trade,
    actual_pnl, peak_unrealized_pnl, peak_date,
    days_held_past_peak, pnl_given_back, giveback_pct,
    pct_of_premium_captured, optimal_exit,
    snapshot_count, snapshot_density, data_reliable
FROM `ccwj-dbt.analytics.int_option_exit_analysis`
WHERE close_date >= @since_date
  AND data_reliable = true
  {account_filter}
ORDER BY pnl_given_back DESC
LIMIT 20
"""

ROLLS_QUERY = """
SELECT
    underlying_symbol, option_type,
    old_trade_symbol, old_expiry, old_strike, old_close_date, old_pnl,
    dte_at_roll,
    new_trade_symbol, new_expiry, new_strike, new_open_date,
    new_contract_status, new_contract_pnl, new_contract_outcome,
    strike_change, net_roll_credit
FROM `ccwj-dbt.analytics.int_option_rolls`
{where}
ORDER BY old_close_date DESC
"""

INSIGHTS_DATA_QUERY = """
SELECT
    account, symbol, strategy, status,
    total_pnl, realized_pnl, unrealized_pnl,
    total_premium_received, total_premium_paid,
    num_trade_groups, num_individual_trades,
    num_winners, num_losers, win_rate,
    avg_pnl_per_trade, avg_days_in_trade,
    first_trade_date, last_trade_date,
    total_dividend_income, total_return
FROM `ccwj-dbt.analytics.positions_summary`
{where}
ORDER BY account, symbol, strategy
"""

WEEKLY_QA_QUERY = """
SELECT
  week_start,
  SUM(trades_closed) AS trades_closed,
  SUM(trades_opened) AS trades_opened,
  SUM(total_pnl)     AS total_pnl,
  SUM(num_winners)   AS num_winners,
  SUM(num_losers)    AS num_losers,
  SUM(premium_received) AS premium_received,
  SUM(premium_paid)     AS premium_paid,
  ANY_VALUE(best_symbol)      AS best_symbol,
  ANY_VALUE(best_strategy)    AS best_strategy,
  ANY_VALUE(best_pnl)         AS best_pnl,
  ANY_VALUE(worst_symbol)     AS worst_symbol,
  ANY_VALUE(worst_strategy)   AS worst_strategy,
  ANY_VALUE(worst_pnl)        AS worst_pnl
FROM `ccwj-dbt.analytics.mart_weekly_summary`
{where}
GROUP BY week_start
ORDER BY week_start DESC
LIMIT 1
"""


# ------------------------------------------------------------------
# Coaching brief builder — the core differentiator
# ------------------------------------------------------------------

def _build_coaching_brief(client, user_accounts):
    """Build a structured coaching brief from pre-computed dbt signals.

    Returns (brief_text, coaching_data_dict) where coaching_data_dict
    contains the raw data for deterministic rendering in the template.
    """
    where = _account_sql_filter(user_accounts)
    acct_and = _account_sql_and(user_accounts)
    sections = []
    coaching_data = {
        "signals": [],
        "recent_exits": [],
        "rolls": [],
        "has_data": False,
        "total_closed": 0,
        "reliable_contracts": 0,
        "pct_reliable": 0,
    }

    # 1. Coaching signals per strategy
    try:
        signals_df = client.query(
            COACHING_SIGNALS_QUERY.format(where=where)
        ).to_dataframe()
        if not signals_df.empty:
            coaching_data["has_data"] = True
            for col in ["avg_giveback_pct", "avg_pnl_given_back", "avg_days_held_past_peak",
                         "optimal_exit_rate", "avg_pct_premium_captured", "total_pnl_given_back",
                         "total_closed", "reliable_contracts", "pct_contracts_reliable",
                         "num_rolls", "avg_dte_at_roll", "roll_success_rate", "avg_roll_credit",
                         "best_dte_win_rate", "worst_dte_win_rate"]:
                if col in signals_df.columns:
                    signals_df[col] = pd.to_numeric(signals_df[col], errors="coerce").fillna(0)

            total_given_back = float(signals_df["total_pnl_given_back"].sum())
            total_closed = int(signals_df["total_closed"].sum())
            reliable_contracts = int(signals_df["reliable_contracts"].sum())
            pct_reliable = round(reliable_contracts / total_closed * 100, 0) if total_closed > 0 else 0

            coaching_data["total_closed"] = total_closed
            coaching_data["reliable_contracts"] = reliable_contracts
            coaching_data["pct_reliable"] = pct_reliable

            # Exit timing section
            exit_lines = []
            if reliable_contracts > 0:
                avg_gb = float(signals_df["avg_giveback_pct"].mean())
                avg_days = float(signals_df["avg_days_held_past_peak"].mean())
                exit_lines.append(
                    f"- Based on {reliable_contracts} closed options with sufficient daily snapshot data "
                    f"({pct_reliable:.0f}% of {total_closed} total closed), "
                    f"you give back an average of {avg_gb:.0f}% of peak profit."
                )
                exit_lines.append(
                    f"- Total profit left on the table: ${total_given_back:,.0f}.")
                exit_lines.append(
                    f"- Average days held past peak: {avg_days:.1f}.")

                strat_rows = []
                for _, r in signals_df.iterrows():
                    if int(r.get("reliable_contracts", 0)) >= 3:
                        strat_rows.append({
                            "strategy": r["strategy"],
                            "giveback_pct": float(r["avg_giveback_pct"]),
                            "days_past_peak": float(r["avg_days_held_past_peak"]),
                            "pnl_given_back": float(r["total_pnl_given_back"]),
                            "trades": int(r["reliable_contracts"]),
                            "total_closed": int(r["total_closed"]),
                            "pct_reliable": float(r["pct_contracts_reliable"]),
                            "pct_premium_captured": float(r.get("avg_pct_premium_captured") or 0),
                        })
                        coaching_data["signals"].append(strat_rows[-1])

                strat_rows.sort(key=lambda x: x["giveback_pct"], reverse=True)
                for s in strat_rows[:3]:
                    exit_lines.append(
                        f"  - {s['strategy']}: {s['giveback_pct']:.0f}% giveback, "
                        f"{s['days_past_peak']:.0f} days past peak, "
                        f"${s['pnl_given_back']:,.0f} left on table "
                        f"({s['trades']} reliable trades)."
                    )

            if exit_lines:
                sections.append("EXIT TIMING PROFILE\n" + "\n".join(exit_lines))

            # Roll section (account-level)
            first_row = signals_df.iloc[0]
            num_rolls = int(first_row.get("num_rolls", 0))
            if num_rolls >= 2:
                roll_lines = [f"- {num_rolls} rolls detected."]
                avg_dte = float(first_row.get("avg_dte_at_roll", 0))
                roll_wr = float(first_row.get("roll_success_rate", 0))
                roll_lines.append(f"- Average roll happens at {avg_dte:.0f} DTE with {roll_wr:.0f}% success rate.")

                early_wr = first_row.get("early_roll_success_rate")
                late_wr = first_row.get("late_roll_success_rate")
                early_n = int(first_row.get("rolls_early", 0))
                late_n = int(first_row.get("rolls_late", 0))
                if early_n >= 2 and late_n >= 2 and early_wr is not None and late_wr is not None:
                    roll_lines.append(
                        f"- Rolls at 7+ DTE: {float(early_wr):.0f}% success ({early_n} rolls). "
                        f"Rolls at <7 DTE: {float(late_wr):.0f}% success ({late_n} rolls)."
                    )
                sections.append("ROLL BEHAVIOR\n" + "\n".join(roll_lines))

            # DTE sweet spots
            dte_lines = []
            for _, r in signals_df.iterrows():
                best_b = r.get("best_dte_bucket")
                worst_b = r.get("worst_dte_bucket")
                strat = r.get("strategy", "")
                if best_b and worst_b and best_b != worst_b:
                    bwr = float(r.get("best_dte_win_rate", 0))
                    wwr = float(r.get("worst_dte_win_rate", 0))
                    if bwr - wwr >= 15:
                        dte_lines.append(
                            f"- {strat}: best at {best_b} ({bwr:.0f}% WR), "
                            f"worst at {worst_b} ({wwr:.0f}% WR)."
                        )
            if dte_lines:
                sections.append("DTE SWEET SPOTS\n" + "\n".join(dte_lines[:5]))

    except Exception:
        pass

    # 2. Recent exits (last 90 days, for weekly context)
    try:
        from datetime import date, timedelta
        since = date.today() - timedelta(days=90)
        cfg = bq.QueryJobConfig(query_parameters=[
            bq.ScalarQueryParameter("since_date", "DATE", since),
        ])
        exits_df = client.query(
            RECENT_EXITS_QUERY.format(account_filter=acct_and),
            job_config=cfg,
        ).to_dataframe()
        if not exits_df.empty:
            recent_lines = []
            for _, r in exits_df.head(10).iterrows():
                gb = float(r.get("pnl_given_back", 0) or 0)
                sym = r.get("underlying_symbol", "")
                strat = r.get("strategy", "")
                pnl = float(r.get("actual_pnl", 0) or 0)
                peak = float(r.get("peak_unrealized_pnl", 0) or 0)
                days_past = int(r.get("days_held_past_peak", 0) or 0)

                coaching_data["recent_exits"].append({
                    "symbol": sym,
                    "strategy": strat,
                    "actual_pnl": pnl,
                    "peak_pnl": peak,
                    "given_back": gb,
                    "days_past_peak": days_past,
                    "close_date": str(r.get("close_date", ""))[:10],
                    "pct_premium_captured": float(r.get("pct_of_premium_captured") or 0),
                })

                if gb > 10:
                    recent_lines.append(
                        f"  - {sym} ({strat}): peaked at +${peak:,.0f}, "
                        f"closed at +${pnl:,.0f}, gave back ${gb:,.0f} "
                        f"({days_past}d past peak)"
                    )
            if recent_lines:
                sections.append("RECENT EXIT EXAMPLES (last 90 days)\n" + "\n".join(recent_lines))
    except Exception:
        pass

    # 3. Rolls detail
    try:
        rolls_df = client.query(
            ROLLS_QUERY.format(where=where)
        ).to_dataframe()
        if not rolls_df.empty:
            for _, r in rolls_df.head(10).iterrows():
                coaching_data["rolls"].append({
                    "symbol": str(r.get("underlying_symbol", "")),
                    "type": str(r.get("option_type", "")),
                    "old_strike": float(r.get("old_strike", 0) or 0),
                    "new_strike": float(r.get("new_strike", 0) or 0),
                    "dte_at_roll": int(r.get("dte_at_roll", 0) or 0),
                    "old_pnl": float(r.get("old_pnl", 0) or 0),
                    "outcome": str(r.get("new_contract_outcome", "")),
                    "date": str(r.get("old_close_date", ""))[:10],
                })
    except Exception:
        pass

    brief_text = "\n\n".join(sections) if sections else None
    return brief_text, coaching_data


def _build_prompt_data(df):
    """Fallback: flat portfolio summary when coaching signals aren't available."""
    if df.empty:
        return None

    num_cols = [
        "total_pnl", "realized_pnl", "unrealized_pnl",
        "total_premium_received", "total_premium_paid",
        "num_trade_groups", "num_individual_trades",
        "num_winners", "num_losers", "win_rate",
        "avg_pnl_per_trade", "avg_days_in_trade",
        "total_dividend_income", "total_return",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    total_return = float(df["total_return"].sum())
    realized = float(df["realized_pnl"].sum())
    unrealized = float(df["unrealized_pnl"].sum())
    premium_received = float(df["total_premium_received"].sum())
    premium_paid = float(df["total_premium_paid"].sum())
    total_trades = int(df["num_individual_trades"].sum())
    total_winners = int(df["num_winners"].sum())
    total_losers = int(df["num_losers"].sum())
    total_closed = total_winners + total_losers
    overall_win_rate = total_winners / total_closed if total_closed else 0
    num_symbols = df["symbol"].nunique()

    first_date = str(df["first_trade_date"].min())
    last_date = str(df["last_trade_date"].max())

    strat_agg = df.groupby("strategy").agg(
        total_return=("total_return", "sum"),
        num_trades=("num_individual_trades", "sum"),
        num_winners=("num_winners", "sum"),
        num_losers=("num_losers", "sum"),
        avg_days=("avg_days_in_trade", "mean"),
        premium_received=("total_premium_received", "sum"),
    ).reset_index()

    strategy_lines = []
    for _, r in strat_agg.iterrows():
        closed = int(r["num_winners"] + r["num_losers"])
        wr = r["num_winners"] / closed if closed else 0
        strategy_lines.append(
            f"  - {r['strategy']}: return=${r['total_return']:,.2f}, "
            f"WR={wr:.1%}, trades={int(r['num_trades'])}, avg_days={r['avg_days']:.1f}"
        )

    return f"""PORTFOLIO OVERVIEW
- Symbols: {num_symbols}, Trades: {total_trades}, Range: {first_date} to {last_date}
- Return: ${total_return:,.2f} (realized ${realized:,.2f}, unrealized ${unrealized:,.2f})
- Win rate: {overall_win_rate:.1%} ({total_winners}W / {total_losers}L)
- Net premium: ${premium_received - premium_paid:,.2f}

STRATEGY BREAKDOWN
{chr(10).join(strategy_lines)}"""


# ------------------------------------------------------------------
# AI prompts — the AI narrates pre-computed signals
# ------------------------------------------------------------------

SYSTEM_PROMPT = """You are narrating a trader's behavioral coaching report. The data below
contains PRE-COMPUTED signals about their option trading behavior — exit timing,
roll patterns, and DTE performance. These signals come from daily option
mark-to-market data that no other retail tool tracks.

IMPORTANT — DATA COVERAGE: The signals are computed only from contracts with
sufficient daily snapshot data (at least 40% of hold days covered, minimum 3
snapshots). The data will tell you how many contracts qualified. If coverage
is low (e.g., "15 of 40 contracts"), acknowledge that the patterns are based
on a subset and may become clearer as more daily data accumulates. Do NOT
present partial-coverage findings as definitive.

Your job:
1. Lead with the MOST ACTIONABLE finding — the behavior change that would
   save the most money if corrected.
2. Use specific numbers from the signals. Never generalize when you have data.
3. Frame everything as process, not outcome. Say "You held 8 days past peak"
   not "you lost money." Say "Your rolls at 7+ DTE succeed 80% of the time"
   not "you should roll earlier."
4. Write 3-4 concise paragraphs. No section headings. No bullet lists.
   Write like a coach talking after a game — direct, specific, constructive.
5. End with ONE concrete thing to watch next week.

Rules:
- Do NOT give financial advice or recommend specific trades.
- Do NOT recommend securities, strikes, expirations, or position sizes.
- Do NOT make price predictions.
- Focus only on behavioral patterns visible in the data.
- Write in second person ("You...").

IMPORTANT: Start with a 2-sentence summary under "## Summary" that captures
the single most important behavioral insight. Then write the full analysis."""


QA_SYSTEM_PROMPT = """You are a trading coach with access to detailed behavioral data about
this trader's option trading — including daily mark-to-market curves, exit
timing analysis, roll patterns, and DTE performance breakdowns.

You will receive:
- COACHING SIGNALS: Pre-computed behavioral metrics (exit timing, roll behavior, DTE patterns)
- PORTFOLIO OVERVIEW: Lifetime strategy performance
- Optionally: RECENT EXITS showing specific trades where profit was left on the table
- Optionally: LAST WEEK performance summary

The coaching signals only include contracts with reliable daily data (40%+
snapshot density). If the data mentions "X of Y contracts," the remaining
contracts lacked sufficient daily data. Do not extrapolate beyond what the
data covers.

Answer the user's question in 3-6 short paragraphs. Be specific — use exact
numbers, trade symbols, and dates from the data. If the question asks about
exit timing, rolls, or holding behavior, lean heavily on the coaching signals.

Rules:
- Do NOT give financial advice or trade recommendations.
- If data isn't available to answer, say so honestly.
- Focus on behavioral patterns, not market predictions.
- Write in second person ("You...")."""


def _call_gemini(data_text):
    """Call Gemini with coaching brief and return (summary, full_analysis)."""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return None, "GEMINI_API_KEY not set. Add it to your .env file."

    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=SYSTEM_PROMPT + "\n\nHere is the trader's behavioral data:\n\n" + data_text,
            config=types.GenerateContentConfig(
                temperature=0.7,
                max_output_tokens=2000,
            ),
        )
        full_text = response.text.strip()

        summary = ""
        if "## Summary" in full_text:
            after_summary = full_text.split("## Summary", 1)[1]
            next_heading = after_summary.find("\n## ")
            if next_heading != -1:
                summary = after_summary[:next_heading].strip()
            else:
                summary = after_summary.strip()
        else:
            summary = full_text[:200].strip()

        return (summary, full_text), None
    except Exception as exc:
        return None, f"Gemini API error: {exc}"


def _call_gemini_question(coaching_text, portfolio_text, weekly_text, question):
    """Call Gemini for Q&A, grounded in coaching + portfolio + weekly data."""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return None, "GEMINI_API_KEY not set. Add it to your .env file."

    try:
        client = genai.Client(api_key=api_key)
        parts = [QA_SYSTEM_PROMPT]
        if coaching_text:
            parts.append("COACHING SIGNALS:\n" + coaching_text)
        if weekly_text:
            parts.append("LAST WEEK DATA:\n" + weekly_text)
        if portfolio_text:
            parts.append("PORTFOLIO OVERVIEW:\n" + portfolio_text)
        parts.append(
            "\nAnswer the user's question below. Be concise and specific, "
            "grounded strictly in the data above.\n"
            f"User question: {question}\n"
        )
        prompt = "\n\n".join(parts)

        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.6,
                max_output_tokens=800,
            ),
        )
        return response.text.strip(), None
    except Exception as exc:
        return None, f"Gemini API error: {exc}"


def _md_to_html(md_text):
    """Simple markdown-to-HTML for Gemini output."""
    lines = md_text.split("\n")
    html_lines = []
    in_list = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("## "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h2>{markupsafe.escape(stripped[3:])}</h2>")
            continue
        if stripped.startswith("- ") or stripped.startswith("* "):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            item = stripped[2:]
            item = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', item)
            html_lines.append(f"<li>{item}</li>")
            continue
        if in_list:
            html_lines.append("</ul>")
            in_list = False
        if not stripped:
            continue
        text = markupsafe.escape(stripped)
        text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', str(text))
        html_lines.append(f"<p>{text}</p>")

    if in_list:
        html_lines.append("</ul>")
    return markupsafe.Markup("\n".join(html_lines))


def _get_user_accounts(selected_account=""):
    """Resolve user accounts with optional single-account focus."""
    if is_admin(current_user.username):
        base_accounts = None
    else:
        base_accounts = get_accounts_for_user(current_user.id)

    if selected_account:
        if base_accounts is None:
            return [selected_account]
        return [a for a in base_accounts if a == selected_account] or base_accounts
    return base_accounts


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------

_INSIGHTS_ENDPOINTS = frozenset({"insights", "generate_insights", "insights_ask"})


@app.before_request
def _require_insights_feature():
    if app.config.get("INSIGHTS_ENABLED", True):
        return None
    if request.endpoint in _INSIGHTS_ENDPOINTS:
        abort(404)
    return None


@app.route("/insights")
@login_required
def insights():
    """Show coaching data + cached AI analysis."""
    selected_account = request.args.get("account", "")
    user_accounts = _get_user_accounts(selected_account)

    if is_admin(current_user.username):
        accounts = []
    else:
        accounts = get_accounts_for_user(current_user.id) or []

    cached = get_insight_for_user(current_user.id)
    gemini_available = bool(os.environ.get("GEMINI_API_KEY"))

    if cached:
        cached["full_analysis_html"] = _md_to_html(cached["full_analysis"])

    # Load deterministic coaching data for the template
    coaching_data = {"has_data": False, "signals": [], "recent_exits": [], "rolls": []}
    try:
        client = get_bigquery_client()
        _, coaching_data = _build_coaching_brief(client, user_accounts)
    except Exception:
        pass

    return render_template(
        "insights.html",
        title="AI Coach",
        insight=cached,
        gemini_available=gemini_available,
        accounts=accounts,
        selected_account=selected_account,
        coaching=coaching_data,
    )


@app.route("/insights/generate", methods=["POST"])
@login_required
def generate_insights():
    """Build coaching brief, call Gemini, cache the result."""
    selected_account = request.args.get("account", "")
    user_accounts = _get_user_accounts(selected_account)
    redir = url_for("insights", account=selected_account) if selected_account else url_for("insights")

    try:
        client = get_bigquery_client()

        # Try coaching brief first (the unique data)
        coaching_text, _ = _build_coaching_brief(client, user_accounts)

        # Fallback to portfolio summary if no coaching data
        if not coaching_text:
            where = _account_sql_filter(user_accounts)
            df = client.query(INSIGHTS_DATA_QUERY.format(where=where)).to_dataframe()
            if df.empty:
                flash("No portfolio data found. Upload your trading data first.", "warning")
                return redirect(redir)
            coaching_text = _build_prompt_data(df)

        if not coaching_text:
            flash("Not enough data to generate insights.", "warning")
            return redirect(redir)

        result, error = _call_gemini(coaching_text)
        if error:
            flash(error, "danger")
            return redirect(redir)

        summary, full_analysis = result
        save_insight(current_user.id, summary, full_analysis)
        flash("AI coaching analysis generated.", "success")

    except Exception as exc:
        flash(f"Could not generate insights: {exc}", "danger")

    return redirect(redir)


@app.route("/insights/ask", methods=["POST"])
@login_required
def insights_ask():
    """Q&A endpoint grounded in coaching signals + portfolio data."""
    payload = request.get_json(silent=True) or {}
    question = str(payload.get("question", "")).strip()
    if not question:
        return jsonify({"error": "Question is required."}), 400
    if len(question) > 800:
        question = question[:800]

    selected_account = request.args.get("account", "")
    user_accounts = _get_user_accounts(selected_account)

    try:
        client = get_bigquery_client()

        # Coaching signals (the unique data)
        coaching_text, _ = _build_coaching_brief(client, user_accounts)

        # Portfolio fallback
        where = _account_sql_filter(user_accounts)
        df = client.query(INSIGHTS_DATA_QUERY.format(where=where)).to_dataframe()
        portfolio_text = _build_prompt_data(df) if not df.empty else None

        # Weekly context
        weekly_text = None
        try:
            wdf = client.query(WEEKLY_QA_QUERY.format(where=where)).to_dataframe()
            if not wdf.empty:
                row = wdf.iloc[0]
                tc = int(row.get("trades_closed", 0) or 0)
                to = int(row.get("trades_opened", 0) or 0)
                tp = float(row.get("total_pnl", 0) or 0)
                nw = int(row.get("num_winners", 0) or 0)
                nl = int(row.get("num_losers", 0) or 0)
                total_c = nw + nl
                wr = nw / total_c if total_c else 0
                ws = str(row.get("week_start", ""))
                weekly_text = (
                    f"WEEK {ws}: {tc} closed ({nw}W/{nl}L, {wr:.0%}), "
                    f"{to} opened, P&L ${tp:,.2f}"
                )
        except Exception:
            pass

        if not coaching_text and not portfolio_text:
            return jsonify({"error": "No data available to answer questions."}), 400

        answer_md, error = _call_gemini_question(
            coaching_text, portfolio_text, weekly_text, question
        )
        if error:
            return jsonify({"error": error}), 500

        return jsonify({"answer_html": str(_md_to_html(answer_md)), "error": None})

    except Exception as exc:
        return jsonify({"error": f"Could not process question: {exc}"}), 500
