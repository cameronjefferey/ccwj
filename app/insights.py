import os
import re
from google import genai
from google.genai import types
import pandas as pd
import markupsafe
from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from app import app
from app.bigquery_client import get_bigquery_client
from app.models import (
    get_accounts_for_user, is_admin,
    save_insight, get_insight_for_user,
)


# ------------------------------------------------------------------
# SQL to gather trading data for the AI prompt
# ------------------------------------------------------------------

INSIGHTS_DATA_QUERY = """
    SELECT
        account,
        symbol,
        strategy,
        status,
        total_pnl,
        realized_pnl,
        unrealized_pnl,
        total_premium_received,
        total_premium_paid,
        num_trade_groups,
        num_individual_trades,
        num_winners,
        num_losers,
        win_rate,
        avg_pnl_per_trade,
        avg_days_in_trade,
        first_trade_date,
        last_trade_date,
        total_dividend_income,
        total_return
    FROM `ccwj-dbt.analytics.positions_summary`
    {where}
    ORDER BY account, symbol, strategy
"""


def _account_sql_filter(accounts):
    """Build a SQL WHERE clause for filtering by account."""
    if accounts is None:
        return ""
    if not accounts:
        return "WHERE 1 = 0"
    quoted = ", ".join(f"'{a.replace(chr(39), chr(39)+chr(39))}'" for a in accounts)
    return f"WHERE account IN ({quoted})"


def _build_prompt_data(df):
    """
    Build a structured text summary of the user's trading data
    to send to Gemini. Keeps token usage low by pre-aggregating.
    """
    if df.empty:
        return None

    # Ensure numeric types
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

    # Overall stats
    total_return = float(df["total_return"].sum())
    realized = float(df["realized_pnl"].sum())
    unrealized = float(df["unrealized_pnl"].sum())
    premium_received = float(df["total_premium_received"].sum())
    premium_paid = float(df["total_premium_paid"].sum())
    dividend_income = float(df["total_dividend_income"].sum())
    total_trades = int(df["num_individual_trades"].sum())
    total_winners = int(df["num_winners"].sum())
    total_losers = int(df["num_losers"].sum())
    total_closed = total_winners + total_losers
    overall_win_rate = total_winners / total_closed if total_closed else 0
    num_symbols = df["symbol"].nunique()
    num_accounts = df["account"].nunique()

    # Date range
    first_date = str(df["first_trade_date"].min())
    last_date = str(df["last_trade_date"].max())

    # Per-strategy breakdown
    strat_agg = df.groupby("strategy").agg(
        total_return=("total_return", "sum"),
        realized_pnl=("realized_pnl", "sum"),
        num_trades=("num_individual_trades", "sum"),
        num_winners=("num_winners", "sum"),
        num_losers=("num_losers", "sum"),
        avg_days=("avg_days_in_trade", "mean"),
        num_symbols=("symbol", "nunique"),
        premium_received=("total_premium_received", "sum"),
    ).reset_index()

    strategy_lines = []
    for _, r in strat_agg.iterrows():
        closed = int(r["num_winners"] + r["num_losers"])
        wr = r["num_winners"] / closed if closed else 0
        strategy_lines.append(
            f"  - {r['strategy']}: total_return=${r['total_return']:,.2f}, "
            f"win_rate={wr:.1%}, trades={int(r['num_trades'])}, "
            f"symbols={int(r['num_symbols'])}, avg_days={r['avg_days']:.1f}, "
            f"premium_received=${r['premium_received']:,.2f}"
        )

    # Top 5 and bottom 5 symbols
    sym_agg = df.groupby("symbol").agg(
        total_return=("total_return", "sum"),
        strategies=("strategy", lambda x: ", ".join(sorted(x.unique()))),
    ).reset_index().sort_values("total_return", ascending=False)

    top5 = sym_agg.head(5)
    bottom5 = sym_agg.tail(5).sort_values("total_return")

    top_lines = [f"  - {r['symbol']}: ${r['total_return']:,.2f} ({r['strategies']})"
                 for _, r in top5.iterrows()]
    bottom_lines = [f"  - {r['symbol']}: ${r['total_return']:,.2f} ({r['strategies']})"
                    for _, r in bottom5.iterrows()]

    data_text = f"""PORTFOLIO OVERVIEW
- Accounts: {num_accounts}
- Symbols traded: {num_symbols}
- Total trades: {total_trades}
- Date range: {first_date} to {last_date}

PERFORMANCE
- Total return: ${total_return:,.2f}
- Realized P&L: ${realized:,.2f}
- Unrealized P&L: ${unrealized:,.2f}
- Overall win rate: {overall_win_rate:.1%} ({total_winners}W / {total_losers}L)
- Premium received: ${premium_received:,.2f}
- Premium paid: ${premium_paid:,.2f}
- Net premium: ${premium_received - premium_paid:,.2f}
- Dividend income: ${dividend_income:,.2f}

STRATEGY BREAKDOWN
{chr(10).join(strategy_lines)}

TOP 5 SYMBOLS (by total return)
{chr(10).join(top_lines)}

BOTTOM 5 SYMBOLS (by total return)
{chr(10).join(bottom_lines)}"""

    return data_text


SYSTEM_PROMPT = """You are a trading coach analyzing a retail options trader's portfolio data.
You specialize in options strategies like covered calls, cash-secured puts, wheels, and spreads.

Based on the data provided, write a personalized analysis with these sections:

1. **Trading Style Overview** - Describe how this trader operates (strategies used, frequency, holding periods, etc.)
2. **What's Working** - Highlight strengths: profitable strategies, good win rates, smart positions
3. **What Needs Attention** - Identify weaknesses: losing strategies, poor win rates, risky positions
4. **Actionable Suggestions** - Give 2-3 specific, practical recommendations to improve

Keep the tone encouraging but honest. Use specific numbers from the data.
Write in second person ("You...").

IMPORTANT: Also provide a 2-3 sentence executive summary at the very top, before the sections,
under a heading called "## Summary". This summary should capture the key takeaway.

Use markdown formatting with ## headings for each section."""


def _call_gemini(data_text):
    """
    Call Gemini API with the trading data and return (summary, full_analysis).
    Returns (None, error_message) on failure.
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return None, "GEMINI_API_KEY not set. Add it to your .env file."

    try:
        client = genai.Client(api_key=api_key)

        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=SYSTEM_PROMPT + "\n\nHere is the trader's portfolio data:\n\n" + data_text,
            config=types.GenerateContentConfig(
                temperature=0.7,
                max_output_tokens=2000,
            ),
        )

        full_text = response.text.strip()

        # Extract the summary (text between "## Summary" and the next "##")
        summary = ""
        if "## Summary" in full_text:
            after_summary = full_text.split("## Summary", 1)[1]
            # Find the next heading
            next_heading = after_summary.find("\n## ")
            if next_heading != -1:
                summary = after_summary[:next_heading].strip()
            else:
                summary = after_summary.strip()
        else:
            # Fallback: use first 200 chars
            summary = full_text[:200].strip()

        return (summary, full_text), None

    except Exception as exc:
        return None, f"Gemini API error: {exc}"


def _md_to_html(md_text):
    """
    Simple markdown-to-HTML for Gemini output.
    Handles headings, bold, lists, and paragraphs.
    """
    lines = md_text.split("\n")
    html_lines = []
    in_list = False

    for line in lines:
        stripped = line.strip()

        # Headings
        if stripped.startswith("## "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h2>{markupsafe.escape(stripped[3:])}</h2>")
            continue

        # List items
        if stripped.startswith("- ") or stripped.startswith("* "):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            item = stripped[2:]
            # Bold
            item = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', item)
            html_lines.append(f"<li>{item}</li>")
            continue

        # Close list if we're no longer in one
        if in_list and not stripped.startswith("- ") and not stripped.startswith("* "):
            html_lines.append("</ul>")
            in_list = False

        # Empty line
        if not stripped:
            continue

        # Regular paragraph with bold support
        text = markupsafe.escape(stripped)
        text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', str(text))
        html_lines.append(f"<p>{text}</p>")

    if in_list:
        html_lines.append("</ul>")

    return markupsafe.Markup("\n".join(html_lines))


@app.route("/insights")
@login_required
def insights():
    """Show cached AI analysis, or prompt to generate one."""
    cached = get_insight_for_user(current_user.id)
    gemini_available = bool(os.environ.get("GEMINI_API_KEY"))

    # Convert markdown to HTML for rendering
    if cached:
        cached["full_analysis_html"] = _md_to_html(cached["full_analysis"])

    return render_template(
        "insights.html",
        title="AI Insights",
        insight=cached,
        gemini_available=gemini_available,
    )


@app.route("/insights/generate", methods=["POST"])
@login_required
def generate_insights():
    """Query BigQuery data, call Gemini, cache the result."""
    # Get user's accounts
    if is_admin(current_user.username):
        user_accounts = None
    else:
        user_accounts = get_accounts_for_user(current_user.id)

    # Query positions data
    try:
        client = get_bigquery_client()
        where = _account_sql_filter(user_accounts)
        df = client.query(INSIGHTS_DATA_QUERY.format(where=where)).to_dataframe()
    except Exception as exc:
        flash(f"Could not load portfolio data: {exc}", "danger")
        return redirect(url_for("insights"))

    if df.empty:
        flash("No portfolio data found. Upload your trading data first.", "warning")
        return redirect(url_for("insights"))

    # Build prompt data
    data_text = _build_prompt_data(df)
    if not data_text:
        flash("Not enough data to generate insights.", "warning")
        return redirect(url_for("insights"))

    # Call Gemini
    result, error = _call_gemini(data_text)
    if error:
        flash(error, "danger")
        return redirect(url_for("insights"))

    summary, full_analysis = result

    # Cache in SQLite
    save_insight(current_user.id, summary, full_analysis)

    flash("AI insights generated successfully!", "success")
    return redirect(url_for("insights"))
