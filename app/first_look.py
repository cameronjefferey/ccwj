"""
First Look — the "Here's what we found" page shown after first upload.

Queries positions_summary and int_strategy_classification to build a
narrative profile of the trader. Designed to deliver at least one
genuine insight on day one.
"""
from flask import render_template, redirect, request, url_for
from flask_login import current_user, login_required
from app import app
from app.bigquery_client import get_bigquery_client
from app.money import fmt_money
from app.outcome_units import SPREAD_STRATEGIES
from app.routes import (
    _tenants_for_scope,
    _tenant_sql_filter,
    _tenant_sql_and,
    _user_account_list,
)
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
        -- A one-fill name must not weigh the same as a fifty-fill name.
        ROUND(SAFE_DIVIDE(
            SUM(IF(avg_days_in_trade IS NULL, 0,
                   avg_days_in_trade * GREATEST(num_individual_trades, 0))),
            NULLIF(SUM(IF(avg_days_in_trade IS NULL, 0,
                          GREATEST(num_individual_trades, 0))), 0)
        ), 1) AS avg_holding_days
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
        ROUND(SAFE_DIVIDE(
            SUM(IF(avg_days_in_trade IS NULL, 0,
                   avg_days_in_trade * GREATEST(num_individual_trades, 0))),
            NULLIF(SUM(IF(avg_days_in_trade IS NULL, 0,
                          GREATEST(num_individual_trades, 0))), 0)
        ), 1) AS avg_days
    FROM `ccwj-dbt.analytics.positions_summary`
    {where}
    GROUP BY strategy
    -- ``total_return`` is the SELECT alias here. Wrapping it in SUM()
    -- again makes BigQuery reject this as an aggregation of aggregations.
    ORDER BY total_return DESC
"""

SYMBOL_QUERY = """
    SELECT
        symbol,
        SUM(total_return) AS total_return,
        STRING_AGG(DISTINCT strategy, ', ' ORDER BY strategy) AS strategies
    FROM `ccwj-dbt.analytics.positions_summary`
    {where}
    GROUP BY symbol
    -- NB: must be the output alias, not SUM(total_return) — BigQuery
    -- resolves total_return in ORDER BY to the SELECT alias, so wrapping
    -- it in SUM() again is an "aggregation of aggregations" error that
    -- silently killed this page for months (the old route swallowed it).
    ORDER BY total_return DESC
"""

def _spread_list_sql():
    """Same spread labels as positions_summary outcome units."""
    return ", ".join(f"'{name}'" for name in sorted(SPREAD_STRATEGIES))


# One row per closed result. A same-day spread is one unit, matching
# positions_summary, so the average win is not a single leg of a spread
# next to a grouped win rate.
WIN_LOSS_QUERY = """
    WITH keyed AS (
        SELECT
            tenant_id,
            account,
            user_id,
            symbol,
            strategy,
            status,
            total_pnl,
            CASE
                WHEN strategy IN ({spread_list})
                THEN CONCAT(
                    COALESCE(CAST(open_date AS STRING), ''),
                    '|',
                    COALESCE(CAST(option_expiry AS STRING), '')
                )
                ELSE COALESCE(trade_symbol, '')
            END AS unit_key
        FROM `ccwj-dbt.analytics.int_strategy_classification`
        {where}
    ),
    units AS (
        SELECT
            unit_key,
            LOGICAL_AND(status = 'Closed') AS unit_closed,
            SUM(total_pnl) AS unit_pnl
        FROM keyed
        GROUP BY unit_key, tenant_id, account, user_id, symbol, strategy
    )
    SELECT
        AVG(IF(unit_closed AND ROUND(unit_pnl, 2) > 0, unit_pnl, NULL)) AS avg_win,
        AVG(IF(unit_closed AND ROUND(unit_pnl, 2) < 0, unit_pnl, NULL)) AS avg_loss,
        SUM(IF(unit_closed AND ROUND(unit_pnl, 2) > 0, unit_pnl, 0)) AS gross_wins,
        SUM(IF(unit_closed AND ROUND(unit_pnl, 2) < 0, unit_pnl, 0)) AS gross_losses
    FROM units
"""

BUSIEST_MONTH_QUERY = """
    WITH keyed AS (
        SELECT
            open_date,
            tenant_id,
            account,
            user_id,
            symbol,
            strategy,
            CASE
                WHEN strategy IN ({spread_list})
                THEN CONCAT(
                    COALESCE(CAST(open_date AS STRING), ''),
                    '|',
                    COALESCE(CAST(option_expiry AS STRING), '')
                )
                ELSE COALESCE(trade_symbol, '')
            END AS unit_key
        FROM `ccwj-dbt.analytics.int_strategy_classification`
        WHERE open_date IS NOT NULL {tenant_filter}
    ),
    units AS (
        SELECT MIN(open_date) AS open_date
        FROM keyed
        GROUP BY unit_key, tenant_id, account, user_id, symbol, strategy
    )
    SELECT
        -- Single %: this string goes through str.format() (which ignores %),
        -- NOT %-formatting. %% here would render a literal "%Y-%m" on the page.
        FORMAT_DATE('%B %Y', open_date) AS month,
        COUNT(*) AS trades_opened
    FROM units
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


def _build_profile(client, where, tenant_and):
    """Build the trader profile dict from BigQuery data."""
    from app.routes import _bq_parallel

    profile = {}

    # All five stats queries are independent — one parallel cached wave
    # instead of five serial round trips (this page clocked up to 5.9s in
    # production on the serial path).
    batch = _bq_parallel(client, {
        "fl_profile": PROFILE_QUERY.format(where=where),
        "fl_strategies": STRATEGY_QUERY.format(where=where),
        "fl_symbols": SYMBOL_QUERY.format(where=where),
        "fl_win_loss": WIN_LOSS_QUERY.format(
            where=where, spread_list=_spread_list_sql(),
        ),
        "fl_busiest_month": BUSIEST_MONTH_QUERY.format(
            tenant_filter=tenant_and, spread_list=_spread_list_sql(),
        ),
    })

    # Overall stats
    df = batch.get("fl_profile", pd.DataFrame())
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
    strat_df = batch.get("fl_strategies", pd.DataFrame())
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
                "winners": int(sr["winners"]),
                "losers": int(sr["losers"]),
                "num_symbols": int(sr["num_symbols"]),
                "total_trades": int(sr["total_trades"]),
                "avg_days": float(sr["avg_days"]),
            })
    profile["strategies"] = strategies
    if strategies:
        profile["top_strategy"] = strategies[0]

    # Symbols — best and worst
    sym_df = batch.get("fl_symbols", pd.DataFrame())
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
    wl_df = batch.get("fl_win_loss", pd.DataFrame())
    if not wl_df.empty:
        avg_win = _safe_float(wl_df.iloc[0].get("avg_win"))
        avg_loss = _safe_float(wl_df.iloc[0].get("avg_loss"))
        gross_wins = _safe_float(wl_df.iloc[0].get("gross_wins"))
        gross_losses = _safe_float(wl_df.iloc[0].get("gross_losses"))
        profile["avg_win"] = avg_win
        profile["avg_loss"] = avg_loss
        profile["gross_wins"] = gross_wins
        profile["gross_losses"] = gross_losses
        # Profit factor is the sum of wins divided by the sum of losses.
        # The ratio of the two averages is a different number whenever
        # the win count and the loss count differ.
        if gross_losses < 0 and gross_wins > 0:
            profile["profit_factor"] = gross_wins / abs(gross_losses)
        else:
            profile["profit_factor"] = None
    else:
        profile["avg_win"] = 0
        profile["avg_loss"] = 0
        profile["gross_wins"] = 0
        profile["gross_losses"] = 0
        profile["profit_factor"] = None

    # Busiest month
    try:
        bm_df = batch.get("fl_busiest_month", pd.DataFrame())
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
    """Pick the single most interesting fact from the profile.

    Evidence only. The ratio of average win to average loss is not the
    profit factor, and this page does not name a score that was removed.
    """
    insights = []
    wr = p.get("win_rate") or 0
    pf = p.get("profit_factor")
    avg_win = abs(p.get("avg_win") or 0)
    avg_loss = abs(p.get("avg_loss") or 0)
    gross_wins = p.get("gross_wins") or 0
    gross_losses = abs(p.get("gross_losses") or 0)

    if pf is not None and wr > 0.5 and pf < 1.0:
        insights.append({
            "priority": 10,
            "title": "Losses add up to more than wins",
            "body": (
                f"Closed results finished ahead {wr:.0%} of the time. "
                f"The average win is {fmt_money(avg_win, 0)} and the average loss is "
                f"{fmt_money(avg_loss, 0)}. Added together, wins are "
                f"{fmt_money(gross_wins, 0)} and losses are {fmt_money(gross_losses, 0)}."
            ),
            "action": "Each closed result is on Positions.",
        })
    elif pf is not None and pf > 2.0:
        insights.append({
            "priority": 5,
            "title": "Wins add up to more than losses",
            "body": (
                f"Added together, wins are {fmt_money(gross_wins, 0)} and losses are "
                f"{fmt_money(gross_losses, 0)}. Wins are {pf:.1f} times losses. "
                f"The average win is {fmt_money(avg_win, 0)} and the average loss is "
                f"{fmt_money(avg_loss, 0)}."
            ),
            "action": "The closed results are on Positions.",
        })

    if p.get("top3_concentration", 0) > 70 and p.get("num_symbols", 0) > 5:
        syms = ", ".join(p.get("top3_symbols", [])[:3])
        insights.append({
            "priority": 8,
            "title": f"{syms} carry most of the absolute results",
            "body": (
                f"Those symbols are {p['top3_concentration']:.0f}% of the absolute results. "
                f"A large gain and a large loss both count."
            ),
            "action": "Those symbols open from Positions.",
        })

    if p.get("avg_holding_days", 0) < 10 and p.get("total_trades", 0) > 50:
        insights.append({
            "priority": 7,
            "title": "Holding periods are short",
            "body": (
                f"The average hold, weighted by fills, is {p['avg_holding_days']:.0f} days "
                f"across {p['total_trades']} fills."
            ),
            "action": "Position Detail shows how long each one was open.",
        })

    if wr < 0.5 and p.get("total_return", 0) > 0 and (p.get("total_winners", 0) + p.get("total_losers", 0)) > 0:
        insights.append({
            "priority": 6,
            "title": "Fewer than half of closed results finished ahead",
            "body": (
                f"The win rate is {wr:.0%}, and total return is "
                f"{fmt_money(p.get('total_return') or 0, 0)}."
            ),
            "action": "The largest results are on Positions.",
        })

    if p.get("num_strategies", 0) == 1 and p.get("strategies"):
        strat = p["strategies"][0]["strategy"]
        insights.append({
            "priority": 4,
            "title": f"Every position uses {strat}",
            "body": f"{strat} is the only strategy label in this record.",
            "action": "Strategies shows that label across the book.",
        })

    if not insights:
        insights.append({
            "priority": 1,
            "title": "The record is in",
            "body": (
                f"{p.get('total_trades', 0)} fills across "
                f"{p.get('num_symbols', 0)} symbols."
            ),
            "action": "Overview shows the latest session. Positions shows the whole book.",
        })

    return max(insights, key=lambda i: i["priority"])


# The standalone /first-look page was folded into /get-started in the
# Aug 2026 surface audit: one onboarding surface that shows the checklist
# until data lands, then flips to the "here's what we found" profile.
# /first-look 301s so the post-upload/post-sync processing redirects and
# any bookmarks keep working.


@app.route("/first-look")
@login_required
def first_look():
    """Legacy URL — permanently moved to /get-started."""
    args = {}
    for key in ("from_upload", "from_sync"):
        val = (request.args.get(key) or "").strip()
        if val:
            args[key] = val
    return redirect(url_for("get_started", **args), code=301)


def render_first_look_view():
    """Render the post-upload "Here's what we found" profile, or None.

    Returns None when the profile can't be built (no accounts, no rows
    yet, or a transient BQ failure) so the caller (get_started) can fall
    back to the onboarding checklist.
    """
    user_accounts = _user_account_list()
    has_accounts = user_accounts is None or len(user_accounts) > 0
    if not has_accounts:
        return None

    try:
        client = get_bigquery_client()
        tenant_ids = _tenants_for_scope()
        where = _tenant_sql_filter(tenant_ids)
        tenant_and = _tenant_sql_and(tenant_ids)
        profile = _build_profile(client, where, tenant_and)
        if not profile:
            return None
        from app.early_broker import early_broker_notice_for_user
        connect_another = False
        try:
            from app.snaptrade import snaptrade_enabled as _snaptrade_enabled_fn
            connect_another = bool(_snaptrade_enabled_fn())
        except Exception:
            connect_another = False
        return render_template(
            "first_look.html",
            title="Your Trading Profile",
            profile=profile,
            connect_another=connect_another,
            early_broker=early_broker_notice_for_user(
                getattr(current_user, "id", None)
            ),
        )
    except Exception as exc:
        app.logger.warning("first-look profile build failed: %s", exc)
        return None
