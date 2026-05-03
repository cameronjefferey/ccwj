"""AI-narrated insights for the Strategy fit matrix.

Two-layer architecture (mirrors `app/insights.py`):

1. _build_strategy_fit_brief() — DETERMINISTIC. Computes structured
   observations from positions_summary: sweet/soft spots (sample-size
   guarded), strategy concentration, sector edge, top symbols carrying
   each cell, and coverage caveats. No LLM, no hallucination risk —
   just stats.

2. _call_gemini_strategy_fit_brief() — LLM. Hands the brief to Gemini
   with a tight system prompt: 3-5 short bullets, observational voice,
   specific numbers, no trade advice. Cached per (user, account scope).

The split means we can fall back to deterministic bullets if the API
key is missing, and we can audit/test the facts independently of the
narration."""

from __future__ import annotations

import os

import pandas as pd
from flask import flash, redirect, request, url_for
from flask_login import current_user, login_required
from google import genai
from google.genai import types

from app import app
from app.bigquery_client import get_bigquery_client
from app.extensions import limiter
from app.models import (
    get_accounts_for_user,
    is_admin,
    save_strategy_fit_insight,
)
from app.routes import _account_sql_and, _filter_df_by_accounts
from app.utils import demo_block_writes


STRATEGY_FIT_QUERY = """
    SELECT
        account,
        symbol,
        strategy,
        status,
        total_pnl,
        realized_pnl,
        unrealized_pnl,
        num_individual_trades,
        num_winners,
        num_losers,
        sector,
        subsector
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1
    {account_filter}
"""


# Sample-size thresholds. Below MIN_TRADES_FOR_CALLOUT we don't celebrate
# a cell as a sweet spot (or condemn it as soft) — too noisy. Below
# MIN_TRADES_FOR_VISIBILITY we hide it from the brief entirely so we
# don't waste prompt tokens on coin flips.
MIN_TRADES_FOR_CALLOUT = 5
MIN_TRADES_FOR_VISIBILITY = 3
MIN_WIN_RATE_FOR_SWEET = 0.45  # don't crown 30%-WR + R:R as "edge"


def _user_accounts(selected_account: str = ""):
    """Resolve which accounts to scope to.

    Mirrors `_get_user_accounts` in app/insights.py — admins see all,
    non-admins see their linked accounts, and a single-account selection
    further narrows the view."""
    if is_admin(current_user.username):
        base = None
    else:
        base = get_accounts_for_user(current_user.id)
    if selected_account:
        if base is None:
            return [selected_account]
        return [a for a in base if a == selected_account] or base
    return base


# --------------------------------------------------------------------
# Deterministic brief
# --------------------------------------------------------------------


def _build_strategy_fit_brief(client, user_accounts):
    """Build a structured fact sheet for the Strategy x Sector matrix.

    Returns (brief_text, brief_dict) where brief_text is a plain-text
    summary suitable for an LLM prompt and brief_dict is the same data
    in a form that's easy to render deterministically.
    Returns (None, {...}) when there isn't enough data.
    """
    acct_filter = _account_sql_and(user_accounts)
    df = client.query(
        STRATEGY_FIT_QUERY.format(account_filter=acct_filter)
    ).to_dataframe()
    df = _filter_df_by_accounts(df, user_accounts)
    if df.empty:
        return None, {"has_data": False}

    # Numeric coercion — defend against BigQuery returning Decimal/None.
    for c in ("total_pnl", "realized_pnl", "unrealized_pnl",
              "num_individual_trades", "num_winners", "num_losers"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    for c in ("sector", "subsector", "symbol", "strategy"):
        if c in df.columns:
            df[c] = df[c].fillna("Unknown").astype(str)

    # Baselines — these are the user's overall numbers. Every claim in
    # the brief should reference these so "edge" is meaningful.
    total_trades = int(df["num_individual_trades"].sum())
    total_winners = int(df["num_winners"].sum())
    total_losers = int(df["num_losers"].sum())
    total_closed = total_winners + total_losers
    total_pnl = float(df["total_pnl"].sum())
    baseline_expectancy = (total_pnl / total_trades) if total_trades else 0.0
    baseline_win_rate = (total_winners / total_closed) if total_closed else 0.0

    if total_trades == 0:
        return None, {"has_data": False}

    # Aggregate to (strategy, sector). One cell per intersection.
    cell_agg = (
        df.groupby(["strategy", "sector"], dropna=False)
        .agg(
            total_pnl=("total_pnl", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            unrealized_pnl=("unrealized_pnl", "sum"),
            num_trades=("num_individual_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
            num_symbols=("symbol", "nunique"),
        )
        .reset_index()
    )
    cell_closed = cell_agg["num_winners"] + cell_agg["num_losers"]
    cell_agg["win_rate"] = (cell_agg["num_winners"] / cell_closed.replace(0, pd.NA)).fillna(0)
    cell_agg["expectancy"] = (
        cell_agg["total_pnl"] / cell_agg["num_trades"].replace(0, pd.NA)
    ).fillna(0)
    cell_agg["edge_expectancy"] = cell_agg["expectancy"] - baseline_expectancy
    cell_agg["edge_win_rate"]   = cell_agg["win_rate"]   - baseline_win_rate

    # Sample-size guarded sweet/soft picks. Cells in the "Unknown"
    # sector bucket are excluded from the narrative — naming "Unknown"
    # as a sweet or soft spot isn't actionable for the user, and the
    # symbols in there are typically delisted/post-corp-action tickers
    # that yfinance can't classify rather than a coherent group.
    qualified = cell_agg[
        (cell_agg["num_trades"] >= MIN_TRADES_FOR_CALLOUT)
        & (cell_agg["sector"].astype(str) != "Unknown")
    ].copy()

    sweet_df = qualified[
        (qualified["expectancy"] > 0)
        & (qualified["win_rate"] >= MIN_WIN_RATE_FOR_SWEET)
    ].sort_values("expectancy", ascending=False).head(3)

    soft_df = qualified[qualified["expectancy"] < 0].sort_values(
        "expectancy", ascending=True
    ).head(2)

    # Top symbols carrying each sweet/soft cell — gives the LLM
    # concrete tickers to cite without us paraphrasing.
    sym_agg = (
        df.groupby(["strategy", "sector", "symbol"], dropna=False)
        .agg(
            total_pnl=("total_pnl", "sum"),
            num_trades=("num_individual_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
        )
        .reset_index()
    )

    def _top_symbols_for(strategy, sector, n=3, by="abs"):
        sub = sym_agg[
            (sym_agg["strategy"] == strategy) & (sym_agg["sector"] == sector)
        ].copy()
        if sub.empty:
            return []
        if by == "abs":
            sub["sort_key"] = sub["total_pnl"].abs()
        else:
            sub["sort_key"] = sub["total_pnl"]
        sub = sub.sort_values("sort_key", ascending=(by != "abs")).head(n)
        return [
            {
                "symbol": r["symbol"],
                "total_pnl": float(r["total_pnl"]),
                "num_trades": int(r["num_trades"]),
            }
            for _, r in sub.iterrows()
        ]

    def _enrich(records, sort_dir):
        enriched = []
        for r in records.to_dict(orient="records"):
            r["top_symbols"] = _top_symbols_for(r["strategy"], r["sector"], n=3,
                                                by="abs" if sort_dir == "desc" else "asc")
            enriched.append(r)
        return enriched

    sweet_spots = _enrich(sweet_df, "desc")
    soft_spots  = _enrich(soft_df, "asc")

    # Sector-level top symbols (across all strategies). Used so the LLM
    # can name actual tickers when it mentions a sector — most users
    # don't memorize sector / subsector classifications, but they know
    # their symbols.
    sec_sym_agg = (
        df.groupby(["sector", "symbol"], dropna=False)
        .agg(
            total_pnl=("total_pnl", "sum"),
            num_trades=("num_individual_trades", "sum"),
        )
        .reset_index()
    )

    def _top_symbols_for_sector(sector, n=3, sort="abs"):
        sub = sec_sym_agg[sec_sym_agg["sector"] == sector].copy()
        if sub.empty:
            return []
        if sort == "abs":
            sub["sort_key"] = sub["total_pnl"].abs()
        elif sort == "neg":
            sub["sort_key"] = sub["total_pnl"]
            sub = sub[sub["total_pnl"] < 0]
        else:  # "pos"
            sub["sort_key"] = sub["total_pnl"]
            sub = sub[sub["total_pnl"] > 0]
        sub = sub.sort_values("sort_key", ascending=(sort == "neg")).head(n)
        return [
            {
                "symbol": r["symbol"],
                "total_pnl": float(r["total_pnl"]),
                "num_trades": int(r["num_trades"]),
            }
            for _, r in sub.iterrows()
        ]

    # Strategy-level rollup — answers "is this strategy concentrated or
    # broad?" If a strategy only works in 1 sector, that's narrower edge
    # than one that works in 4. Exclude "Unknown" from the sector count
    # so a strategy with trades in 2 real sectors + 1 Unknown bucket
    # doesn't get falsely promoted to "works across 3 sectors" in the
    # narrative.
    cell_agg_named = cell_agg[cell_agg["sector"].astype(str) != "Unknown"].copy()
    strat_agg = (
        cell_agg_named.groupby("strategy")
        .agg(
            total_pnl=("total_pnl", "sum"),
            num_trades=("num_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
            sectors_with_trades=("sector", "nunique"),
            profitable_sectors=(
                "total_pnl",
                lambda s: int((s > 0).sum()),
            ),
            losing_sectors=(
                "total_pnl",
                lambda s: int((s < 0).sum()),
            ),
        )
        .reset_index()
    )
    strat_closed = strat_agg["num_winners"] + strat_agg["num_losers"]
    strat_agg["win_rate"]   = (strat_agg["num_winners"] / strat_closed.replace(0, pd.NA)).fillna(0)
    strat_agg["expectancy"] = (
        strat_agg["total_pnl"] / strat_agg["num_trades"].replace(0, pd.NA)
    ).fillna(0)
    strat_agg = strat_agg.sort_values("total_pnl", ascending=False)

    # Sector punch-above-weight — share of P&L vs share of trades.
    # If you're 12% of trades but 28% of P&L, that's where you're
    # generating outsized returns relative to attention.
    sec_agg = (
        cell_agg.groupby("sector")
        .agg(
            total_pnl=("total_pnl", "sum"),
            num_trades=("num_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
        )
        .reset_index()
    )
    if total_trades > 0 and total_pnl != 0:
        sec_agg["pct_trades"] = sec_agg["num_trades"] / total_trades
        sec_agg["pct_pnl"]    = sec_agg["total_pnl"] / total_pnl
        # Punch ratio = share of P&L / share of trades. >1 means efficient.
        # Only meaningful if pct_pnl is positive — negative ratios are
        # confusing because both can be negative.
        sec_agg["punch_ratio"] = sec_agg.apply(
            lambda r: (r["pct_pnl"] / r["pct_trades"])
            if r["pct_trades"] > 0 and r["total_pnl"] > 0 else 0,
            axis=1,
        )
    else:
        sec_agg["pct_trades"] = 0
        sec_agg["pct_pnl"] = 0
        sec_agg["punch_ratio"] = 0

    # Surface sectors that are both *positive* AND punching above weight.
    overweight = sec_agg[
        (sec_agg["total_pnl"] > 0) & (sec_agg["sector"] != "Unknown") & (sec_agg["num_trades"] >= MIN_TRADES_FOR_CALLOUT)
    ].sort_values("punch_ratio", ascending=False).head(2).to_dict(orient="records")
    for r in overweight:
        r["top_symbols"] = _top_symbols_for_sector(r["sector"], n=3, sort="pos")

    underweight = sec_agg[
        (sec_agg["total_pnl"] < 0) & (sec_agg["sector"] != "Unknown") & (sec_agg["num_trades"] >= MIN_TRADES_FOR_CALLOUT)
    ].sort_values("total_pnl", ascending=True).head(2).to_dict(orient="records")
    for r in underweight:
        r["top_symbols"] = _top_symbols_for_sector(r["sector"], n=3, sort="neg")

    brief = {
        "has_data": True,
        "baseline": {
            "expectancy": baseline_expectancy,
            "win_rate": baseline_win_rate,
            "total_pnl": total_pnl,
            "total_trades": total_trades,
            "winners": total_winners,
            "losers": total_losers,
        },
        "sweet_spots": sweet_spots,
        "soft_spots": soft_spots,
        "strategies": strat_agg.to_dict(orient="records"),
        "overweight_sectors": overweight,
        "underweight_sectors": underweight,
        "num_strategies": int(strat_agg.shape[0]),
        "num_sectors": int(sec_agg[sec_agg["sector"] != "Unknown"].shape[0]),
        "unknown_share": float(
            (sec_agg.loc[sec_agg["sector"] == "Unknown", "num_trades"].sum() / total_trades)
            if total_trades else 0
        ),
    }

    # Render to a compact text form for the LLM. Keep it dense — token
    # cost matters and we want every fact to anchor a claim.
    lines = []
    lines.append(
        f"BASELINE: ${baseline_expectancy:,.2f}/trade · "
        f"{baseline_win_rate:.0%} win rate · "
        f"${total_pnl:,.0f} total over {total_trades} trades "
        f"({total_winners}W/{total_losers}L closed)."
    )
    lines.append(f"COVERAGE: {brief['num_strategies']} strategies, {brief['num_sectors']} sectors with data.")
    if brief["unknown_share"] > 0.1:
        lines.append(
            f"  NOTE: {brief['unknown_share']:.0%} of trades are in symbols without sector metadata yet — "
            f"those sit in an 'Unknown' bucket and aren't included in sector observations."
        )

    if sweet_spots:
        lines.append("\nSWEET SPOTS (positive expectancy, ≥5 trades, ≥45% win rate):")
        for s in sweet_spots:
            edge_x = (s["expectancy"] / baseline_expectancy) if baseline_expectancy > 0 else None
            lines.append(
                f"- {s['strategy']} × {s['sector']}: ${s['expectancy']:,.2f}/trade "
                f"vs ${baseline_expectancy:,.2f}/trade baseline"
                f"{f' ({edge_x:.1f}× edge)' if edge_x else ''}, "
                f"{s['win_rate']:.0%} win rate vs {baseline_win_rate:.0%} baseline. "
                f"${s['total_pnl']:,.0f} total over {int(s['num_trades'])} trades, "
                f"{int(s['num_symbols'])} symbol(s)."
            )
            if s.get("top_symbols"):
                tops = ", ".join(
                    f"{t['symbol']} (${t['total_pnl']:,.0f})" for t in s["top_symbols"]
                )
                lines.append(f"  Top symbols: {tops}.")

    if soft_spots:
        lines.append("\nSOFT SPOTS (negative expectancy, ≥5 trades):")
        for s in soft_spots:
            lines.append(
                f"- {s['strategy']} × {s['sector']}: ${s['expectancy']:,.2f}/trade, "
                f"{s['win_rate']:.0%} win rate. "
                f"${s['total_pnl']:,.0f} total over {int(s['num_trades'])} trades."
            )
            if s.get("top_symbols"):
                worst = ", ".join(
                    f"{t['symbol']} (${t['total_pnl']:,.0f})" for t in s["top_symbols"]
                )
                lines.append(f"  Biggest contributors: {worst}.")

    if overweight:
        lines.append("\nSECTORS WHERE YOU PUNCH ABOVE YOUR WEIGHT (positive P&L, share of P&L > share of trades):")
        for r in overweight:
            lines.append(
                f"- {r['sector']}: {r['pct_trades']:.0%} of trades but {r['pct_pnl']:.0%} of P&L "
                f"({r['punch_ratio']:.1f}× ratio). ${r['total_pnl']:,.0f} over {int(r['num_trades'])} trades."
            )
            if r.get("top_symbols"):
                tops = ", ".join(
                    f"{t['symbol']} (${t['total_pnl']:,.0f})" for t in r["top_symbols"]
                )
                lines.append(f"  Top symbols in {r['sector']}: {tops}.")

    if underweight:
        lines.append("\nSECTORS DRAGGING YOUR PERFORMANCE (negative P&L):")
        for r in underweight:
            lines.append(
                f"- {r['sector']}: ${r['total_pnl']:,.0f} over {int(r['num_trades'])} trades "
                f"({r['pct_trades']:.0%} of total trade volume)."
            )
            if r.get("top_symbols"):
                worst = ", ".join(
                    f"{t['symbol']} (${t['total_pnl']:,.0f})" for t in r["top_symbols"]
                )
                lines.append(f"  Biggest losers in {r['sector']}: {worst}.")

    # Strategy concentration — flag strategies that work in only 1 sector
    # vs strategies that generalize. The LLM can pick this up if it's
    # interesting; we just expose it.
    multi_sector = strat_agg[strat_agg["sectors_with_trades"] >= 3].head(3)
    if not multi_sector.empty:
        lines.append("\nSTRATEGIES THAT WORK ACROSS SECTORS (≥3 sectors with trades):")
        for _, r in multi_sector.iterrows():
            # Top contributing symbols across all sectors for this strategy.
            strat_syms = (
                df[df["strategy"] == r["strategy"]]
                .groupby("symbol", dropna=False)["total_pnl"].sum()
                .reset_index()
            )
            strat_syms["sort_key"] = strat_syms["total_pnl"].abs()
            top = strat_syms.sort_values("sort_key", ascending=False).head(3)
            tops_str = ""
            if not top.empty:
                tops_str = " Top symbols: " + ", ".join(
                    f"{row['symbol']} (${row['total_pnl']:,.0f})"
                    for _, row in top.iterrows()
                ) + "."
            lines.append(
                f"- {r['strategy']}: profitable in {int(r['profitable_sectors'])}/{int(r['sectors_with_trades'])} sectors "
                f"({r['win_rate']:.0%} WR, ${r['expectancy']:,.2f}/trade across {int(r['num_trades'])} trades).{tops_str}"
            )

    brief_text = "\n".join(lines)
    return brief_text, brief


# --------------------------------------------------------------------
# Gemini call
# --------------------------------------------------------------------


SYSTEM_PROMPT = """You are a quantitative trading coach. You will receive a structured brief
of a trader's strategy-by-sector performance. Your job is to surface the
3-5 most decision-relevant observations as short, evidence-grounded bullets.

Voice and structure:
- Open with a single 1-sentence summary under "## Summary" — the most
  important pattern in the data.
- Then 3-5 markdown bullets. Each bullet must reference a specific
  number from the brief (P&L, win rate, expectancy, edge, or trade count).
- Frame observations as "You..." in second person.
- Where edge is meaningful, contrast with the user's baseline
  (e.g. "vs your baseline of $X/trade").
- If multiple cells have small samples, acknowledge it once briefly
  ("These are still small samples — keep watching.").

CRITICAL — symbol attribution:
- The trader does NOT memorize sector / subsector classifications, but
  they DO remember every ticker they trade. Whenever you name a sector
  (e.g. "Industrials", "Technology", "Healthcare"), you MUST cite the
  specific ticker symbols from the brief in parentheses immediately
  after the sector name.
- Examples of the right format:
    - "Your Industrials sector (RKLB, X, BA) accounts for 67%..."
    - "Technology (NVDA, AAPL, GOOG) is dragging you down..."
    - "Long Calls in Communication Services (RDDT, PINS, NFLX) cost..."
- Use the "Top symbols" line under each sector/cell in the brief.
  Pick the 2-3 most relevant tickers — winners when celebrating, losers
  when flagging a drag.
- If a sector's symbols aren't in the brief (because the brief didn't
  list them), don't make them up — leave the parenthetical off.

Hard rules — do not violate:
- Do NOT recommend trades, securities, strikes, sizes, or directions.
- Do NOT make price or market predictions.
- Do NOT speculate about emotional state ("revenge", "tilt", "FOMO").
- Do NOT add severity labels ("ALERT", "WARNING", "HIGH").
- Do NOT invent numbers or symbols. If it isn't in the brief, don't claim it.
- Do NOT pad with generic trading advice — only narrate what's IN the brief.

Format: markdown. Use "## Summary" once at the top, then "## Observations"
followed by bulleted "- " items. Keep total length under ~280 words."""


def _call_gemini_strategy_fit(brief_text):
    """Call Gemini with the strategy-fit brief.

    Returns ((summary, full_markdown), None) on success or (None, error_msg).
    """
    import time as _time
    from app.cost_tracking import log_cost_event
    from app.insights import _gemini_usage_fields

    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        return None, "GEMINI_API_KEY not set."

    try:
        client = genai.Client(api_key=api_key)
        t0 = _time.monotonic()
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=SYSTEM_PROMPT + "\n\nBRIEF:\n\n" + brief_text,
            config=types.GenerateContentConfig(
                temperature=0.4,  # lower than the coach — we want fewer flourishes
                max_output_tokens=1400,  # slight bump to fit symbol parentheticals
            ),
        )
        duration_ms = int((_time.monotonic() - t0) * 1000)
        log_cost_event(
            "gemini",
            "strategy_fit.generate",
            model="gemini-2.0-flash",
            duration_ms=duration_ms,
            **_gemini_usage_fields(response),
        )
        full = (response.text or "").strip()
        if not full:
            return None, "Empty response from Gemini."

        # Pull the summary section if present, otherwise first paragraph.
        summary = ""
        if "## Summary" in full:
            after = full.split("## Summary", 1)[1]
            nxt = after.find("\n## ")
            summary = (after[:nxt] if nxt != -1 else after).strip()
        else:
            summary = full.split("\n\n", 1)[0].strip()

        return (summary, full), None
    except Exception as exc:
        return None, f"Gemini API error: {exc}"


# --------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------


@app.route("/strategy-fit/insights/generate", methods=["POST"])
@login_required
@limiter.limit("3 per minute; 10 per hour; 30 per day")
def generate_strategy_fit_insights():
    """Build the brief, call Gemini, cache. Redirects back to /strategy-fit."""
    blocked = demo_block_writes("regenerating Strategy Fit insights")
    if blocked:
        return blocked
    selected_account = request.args.get("account", "")
    drill_sector     = request.args.get("sector", "")
    redir_kwargs = {}
    if selected_account:
        redir_kwargs["account"] = selected_account
    if drill_sector:
        redir_kwargs["sector"] = drill_sector
    redir = url_for("strategy_fit", **redir_kwargs)

    if not app.config.get("INSIGHTS_ENABLED", True):
        flash("AI Coach is currently disabled.", "warning")
        return redirect(redir)
    if not os.environ.get("GEMINI_API_KEY", "").strip():
        flash("AI Coach is warming up — try again in a few minutes.", "info")
        return redirect(redir)

    try:
        client = get_bigquery_client()
        accounts = _user_accounts(selected_account)
        brief_text, _brief = _build_strategy_fit_brief(client, accounts)
        if not brief_text:
            flash("Not enough data yet to summarize. Add a few more trades and try again.", "warning")
            return redirect(redir)

        result, err = _call_gemini_strategy_fit(brief_text)
        if err:
            app.logger.error("Strategy-fit insight generation failed: %s", err)
            flash("Couldn't generate insights right now. Try again in a moment.", "danger")
            return redirect(redir)

        summary, full = result
        save_strategy_fit_insight(
            current_user.id,
            account_filter=selected_account or "",
            summary=summary,
            full_analysis=full,
            brief_text=brief_text,
        )
        flash("Insights refreshed.", "success")
    except Exception as exc:
        app.logger.exception("Strategy-fit insight generation failed: %s", exc)
        flash("Couldn't generate insights right now. Try again in a moment.", "danger")

    return redirect(redir)
