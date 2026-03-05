"""
Option trades by kind: strategy, DTE bucket, moneyness, outcome.

Groups option trades by strategy (from classification), DTE at open,
moneyness at open (ITM/ATM/OTM), and outcome (Winner/Loser). Reads from
mart_option_trades_by_kind and int_option_trade_kinds (for detail).
"""
import os
from flask import render_template, request
from flask_login import login_required, current_user
from google import genai
from google.genai import types

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


KINDS_QUERY = """
SELECT
  account,
  strategy,
  dte_bucket,
  moneyness_at_open,
  outcome,
  num_trades,
  total_pnl,
  net_cash_flow,
  win_rate_pct,
  strategy_num_trades,
  strategy_total_pnl,
  strategy_win_rate_pct
FROM `ccwj-dbt.analytics.mart_option_trades_by_kind`
WHERE 1=1 {account_filter}
ORDER BY strategy_total_pnl DESC NULLS LAST, strategy, dte_bucket, moneyness_at_open, outcome
"""

DETAIL_QUERY = """
SELECT
  account,
  trade_symbol,
  underlying_symbol,
  strategy,
  option_type,
  option_strike,
  option_expiry,
  open_date,
  close_date,
  status,
  dte_at_open,
  dte_bucket,
  moneyness_at_open,
  outcome,
  total_pnl,
  num_trades
FROM `ccwj-dbt.analytics.int_option_trade_kinds`
WHERE 1=1 {account_filter}
ORDER BY open_date DESC
LIMIT 500
"""

TRADE_KINDS_SUMMARY_PROMPT = """You are summarizing an options trader's "Option Trades by Kind" report.

You will receive structured data: overall totals, breakdown by DTE at open, by moneyness, by DTE and moneyness, by strategy and moneyness, and the full strategy×DTE×moneyness grid.

Your job: Write one short paragraph (2–4 sentences) that states the main takeaway from this page. Answer: What does this data show? Where do they win or lose? Which DTE or moneyness or strategy stands out?

Rules:
- Use specific numbers from the data (trades, win rate %, P&L).
- Do NOT give financial advice: no recommendations, no buy/sell, no predictions.
- Write in second person ("Your...") or neutral ("The data shows...").
- Be direct and clear. No filler."""


def _build_trade_kinds_data_text(summary, by_dte, by_moneyness, by_dte_moneyness, by_strategy_moneyness, rows):
    """Build a compact text snapshot of the trade-kinds data for the AI."""
    lines = []
    lines.append("OVERALL: total_trades=%s, total_pnl=$%.2f, win_rate=%s%%"
                 % (summary["total_trades"], summary["total_pnl"],
                    summary["overall_win_rate"] if summary.get("overall_win_rate") is not None else "—"))
    lines.append("")
    lines.append("BY DTE AT OPEN:")
    for r in (by_dte or []):
        lines.append("  %s: trades=%s, W=%s L=%s, win%%=%s, pnl=$%.2f"
                     % (r.get("dte_bucket"), r.get("total_trades"), r.get("winners"), r.get("losers"),
                        r.get("win_rate_pct") if r.get("win_rate_pct") is not None else "—", r.get("total_pnl", 0)))
    lines.append("")
    lines.append("BY MONEYNESS AT OPEN:")
    for r in (by_moneyness or []):
        lines.append("  %s: trades=%s, W=%s L=%s, win%%=%s, pnl=$%.2f"
                     % (r.get("moneyness_at_open"), r.get("total_trades"), r.get("winners"), r.get("losers"),
                        r.get("win_rate_pct") if r.get("win_rate_pct") is not None else "—", r.get("total_pnl", 0)))
    lines.append("")
    lines.append("BY DTE AND MONEYNESS:")
    for r in (by_dte_moneyness or [])[:15]:
        lines.append("  %s | %s: trades=%s, pnl=$%.2f"
                     % (r.get("dte_bucket"), r.get("moneyness_at_open"), r.get("total_trades"), r.get("total_pnl", 0)))
    if (by_dte_moneyness or []) and len(by_dte_moneyness) > 15:
        lines.append("  ... and %s more cells" % (len(by_dte_moneyness) - 15))
    lines.append("")
    lines.append("BY STRATEGY AND MONEYNESS:")
    for r in (by_strategy_moneyness or [])[:15]:
        lines.append("  %s | %s: trades=%s, pnl=$%.2f"
                     % (r.get("strategy"), r.get("moneyness_at_open"), r.get("total_trades"), r.get("total_pnl", 0)))
    if (by_strategy_moneyness or []) and len(by_strategy_moneyness) > 15:
        lines.append("  ... and %s more cells" % (len(by_strategy_moneyness) - 15))
    lines.append("")
    lines.append("FULL GRID (strategy × DTE × moneyness), top 20 by P&L:")
    for r in (rows or [])[:20]:
        lines.append("  %s | %s | %s: trades=%s, pnl=$%.2f"
                     % (r.get("strategy"), r.get("dte_bucket"), r.get("moneyness_at_open"),
                        r.get("total_trades"), r.get("total_pnl", 0)))
    return "\n".join(lines)


def _call_gemini_trade_kinds_summary(data_text):
    """Call Gemini to get a 2–4 sentence takeaway from the trade-kinds data. Returns (summary_text, error)."""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return None, None  # No key: don't show error, just no summary
    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=TRADE_KINDS_SUMMARY_PROMPT + "\n\nData:\n\n" + data_text,
            config=types.GenerateContentConfig(
                temperature=0.4,
                max_output_tokens=400,
            ),
        )
        return response.text.strip(), None
    except Exception as exc:
        return None, str(exc)


@app.route("/trade-kinds")
@login_required
def trade_kinds():
    """Option trades grouped by kind: strategy, DTE, moneyness, outcome."""
    user_accounts = _user_account_list()
    selected_account = request.args.get("account", "")

    effective_accounts = user_accounts
    if selected_account:
        if user_accounts is None:
            effective_accounts = [selected_account]
        else:
            effective_accounts = [a for a in user_accounts if a == selected_account] or user_accounts

    account_filter = _account_sql_and(effective_accounts)

    context = {
        "title": "Option Trades by Kind",
        "rows": [],
        "detail": [],
        "accounts": [],
        "selected_account": selected_account,
        "error": None,
        "by_dte": [],
        "by_moneyness": [],
        "by_dte_moneyness": [],
        "by_strategy_moneyness": [],
        "ai_summary": None,
        "ai_summary_error": None,
    }

    try:
        client = get_bigquery_client()
        df = client.query(KINDS_QUERY.format(account_filter=account_filter)).to_dataframe()
        detail_df = client.query(DETAIL_QUERY.format(account_filter=account_filter)).to_dataframe()

        if not df.empty and "account" in df.columns:
            context["accounts"] = sorted(df["account"].dropna().unique().tolist())

        # Fill NaN for display
        for col in ["num_trades", "total_pnl", "net_cash_flow", "win_rate_pct"]:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        # Build list of unique (strategy, dte_bucket, moneyness) with outcome breakdown
        by_cell = {}
        for _, r in df.iterrows():
            key = (r["strategy"], r["dte_bucket"], r["moneyness_at_open"])
            if key not in by_cell:
                by_cell[key] = {
                    "strategy": r["strategy"],
                    "dte_bucket": r["dte_bucket"],
                    "moneyness_at_open": r["moneyness_at_open"],
                    "strategy_num_trades": int(r.get("strategy_num_trades") or 0),
                    "strategy_total_pnl": float(r.get("strategy_total_pnl") or 0),
                    "strategy_win_rate_pct": r.get("strategy_win_rate_pct"),
                    "winners": 0,
                    "losers": 0,
                    "total_trades": 0,
                    "total_pnl": 0,
                    "win_rate_pct": r.get("win_rate_pct"),
                }
            cell = by_cell[key]
            cell["total_trades"] += int(r["num_trades"] or 0)
            cell["total_pnl"] += float(r["total_pnl"] or 0)
            if r["outcome"] == "Winner":
                cell["winners"] += int(r["num_trades"] or 0)
            else:
                cell["losers"] += int(r["num_trades"] or 0)

        # Sort by strategy P&L then by cell
        strategy_order = {}
        for key in by_cell:
            s = key[0]
            if s not in strategy_order:
                strategy_order[s] = by_cell[key]["strategy_total_pnl"]
        sorted_keys = sorted(
            by_cell.keys(),
            key=lambda k: (strategy_order.get(k[0], 0) or 0, k[0], k[1], k[2]),
            reverse=True,
        )
        context["rows"] = [by_cell[k] for k in sorted_keys]

        # Summary tables: by DTE, by Moneyness, by DTE×Moneyness, by Strategy×Moneyness
        def agg_from_df(df_in, group_cols):
            """Group by group_cols, aggregate num_trades/total_pnl by outcome."""
            out = []
            cols = [group_cols] if isinstance(group_cols, str) else group_cols
            g = df_in.groupby(cols)
            for name, grp in g:
                key_vals = (name,) if not isinstance(name, tuple) else name
                total_trades = int(grp["num_trades"].sum())
                winners = int(grp[grp["outcome"] == "Winner"]["num_trades"].sum())
                losers = int(grp[grp["outcome"] == "Loser"]["num_trades"].sum())
                total_pnl = float(grp["total_pnl"].sum())
                win_rate = round(100.0 * winners / (winners + losers), 0) if (winners + losers) > 0 else None
                row = {col: key_vals[i] for i, col in enumerate(cols)}
                row["total_trades"] = total_trades
                row["winners"] = winners
                row["losers"] = losers
                row["total_pnl"] = total_pnl
                row["win_rate_pct"] = win_rate
                out.append(row)
            return out

        dte_order = ["0-7 DTE", "8-30 DTE", "31-60 DTE", "61-90 DTE", "91+ DTE"]
        moneyness_order = ["ITM", "ATM", "OTM", "Unknown"]

        by_dte = agg_from_df(df, "dte_bucket")
        by_dte.sort(key=lambda r: (dte_order.index(r["dte_bucket"]) if r["dte_bucket"] in dte_order else 99))

        by_moneyness = agg_from_df(df, "moneyness_at_open")
        by_moneyness.sort(key=lambda r: (moneyness_order.index(r["moneyness_at_open"]) if r["moneyness_at_open"] in moneyness_order else 99))

        by_dte_moneyness = agg_from_df(df, ["dte_bucket", "moneyness_at_open"])
        by_dte_moneyness.sort(key=lambda r: (
            dte_order.index(r["dte_bucket"]) if r["dte_bucket"] in dte_order else 99,
            moneyness_order.index(r["moneyness_at_open"]) if r["moneyness_at_open"] in moneyness_order else 99,
        ))

        by_strategy_moneyness = agg_from_df(df, ["strategy", "moneyness_at_open"])
        by_strategy_moneyness.sort(key=lambda r: (-(r["total_pnl"] or 0), r["strategy"], moneyness_order.index(r["moneyness_at_open"]) if r["moneyness_at_open"] in moneyness_order else 99))

        context["by_dte"] = by_dte
        context["by_moneyness"] = by_moneyness
        context["by_dte_moneyness"] = by_dte_moneyness
        context["by_strategy_moneyness"] = by_strategy_moneyness

        # Summary totals for KPIs (each cell counted once; total_trades summed across cells)
        total_trades = sum(r["total_trades"] for r in context["rows"])
        total_winners = sum(r["winners"] for r in context["rows"])
        total_losers = sum(r["losers"] for r in context["rows"])
        closed = total_winners + total_losers
        context["summary"] = {
            "total_trades": total_trades,
            "total_winners": total_winners,
            "total_losers": total_losers,
            "overall_win_rate": round(100.0 * total_winners / closed, 0) if closed else None,
            "total_pnl": sum(r["total_pnl"] for r in context["rows"]),
        }

        # AI summary: one short takeaway from the full page data
        data_text = _build_trade_kinds_data_text(
            context["summary"],
            context["by_dte"],
            context["by_moneyness"],
            context["by_dte_moneyness"],
            context["by_strategy_moneyness"],
            context["rows"],
        )
        ai_text, ai_err = _call_gemini_trade_kinds_summary(data_text)
        context["ai_summary"] = ai_text
        context["ai_summary_error"] = ai_err

        # Detail: list of recent option trades with kind attributes
        if not detail_df.empty:
            context["detail"] = detail_df.to_dict("records")
            for d in context["detail"]:
                for k, v in list(d.items()):
                    if hasattr(v, "isoformat"):
                        d[k] = v.isoformat()[:10] if v else ""
                    elif v is None:
                        d[k] = ""
                    elif k == "total_pnl" and v is not None:
                        d[k] = float(v)

    except Exception as e:
        context["error"] = str(e)

    return render_template("trade_kinds.html", **context)
