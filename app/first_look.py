"""
First Look — the "Here's what we found" page shown after first upload.

Queries positions_summary plus the canonical grouped-trade inputs to build
a narrative profile of the trader. Designed to deliver at least one genuine
insight on day one.
"""
from flask import render_template, redirect, request, url_for
from flask_login import current_user, login_required
from app import app
from app.bigquery_client import get_bigquery_client
from app.grouped_trades import (
    CLASSIFICATION_GROUPS_QUERY,
    OPTION_FILLS_QUERY,
    group_book_trades,
)
from app.money import fmt_money
from app.routes import (
    _filter_df_by_tenant_ids,
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


def _grouped_result_metrics(units):
    """Canonical grouped-trade counts and dollars for First Look."""
    counted = [unit for unit in (units or []) if unit.get("counts_as_trade")]
    closed = [
        unit for unit in counted
        if str(unit.get("status") or "") == "Closed"
        and abs(round(_safe_float(unit.get("pnl")), 2)) >= 0.01
    ]
    wins = [_safe_float(unit.get("pnl")) for unit in closed
            if round(_safe_float(unit.get("pnl")), 2) > 0]
    losses = [_safe_float(unit.get("pnl")) for unit in closed
              if round(_safe_float(unit.get("pnl")), 2) < 0]

    by_strategy = {}
    months = {}
    for unit in counted:
        strategy = str(unit.get("strategy") or "")
        slot = by_strategy.setdefault(
            strategy, {"num_trades": 0, "num_winners": 0, "num_losers": 0},
        )
        slot["num_trades"] += 1
        if str(unit.get("status") or "") == "Closed":
            pnl = round(_safe_float(unit.get("pnl")), 2)
            slot["num_winners"] += int(pnl > 0)
            slot["num_losers"] += int(pnl < 0)
        opened = str(unit.get("open_date") or "")[:7]
        if len(opened) == 7:
            months[opened] = months.get(opened, 0) + 1

    busiest_month = None
    busiest_count = 0
    if months:
        month, busiest_count = min(months.items(), key=lambda item: (-item[1], item[0]))
        try:
            busiest_month = pd.Timestamp(f"{month}-01").strftime("%B %Y")
        except (TypeError, ValueError):
            busiest_month = month

    return {
        "total_winners": len(wins),
        "total_losers": len(losses),
        "avg_win": (sum(wins) / len(wins)) if wins else 0.0,
        "avg_loss": (sum(losses) / len(losses)) if losses else 0.0,
        "gross_wins": sum(wins),
        "gross_losses": sum(losses),
        "busiest_month": busiest_month,
        "busiest_month_trades": busiest_count,
        "by_strategy": by_strategy,
    }


def _build_profile(client, where, tenant_and, tenant_ids):
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
        "fl_trade_groups": CLASSIFICATION_GROUPS_QUERY.format(
            tenant_filter=tenant_and,
        ),
        "fl_trade_fills": OPTION_FILLS_QUERY.format(
            tenant_filter=tenant_and,
        ),
    })

    group_df = _filter_df_by_tenant_ids(
        batch.get("fl_trade_groups", pd.DataFrame()), tenant_ids,
    )
    fill_df = _filter_df_by_tenant_ids(
        batch.get("fl_trade_fills", pd.DataFrame()), tenant_ids,
    )
    grouped_metrics = None
    if group_df is not None and not group_df.empty:
        fills = [] if fill_df is None or fill_df.empty else fill_df.to_dict("records")
        grouped_metrics = _grouped_result_metrics(
            group_book_trades(group_df.to_dict("records"), fills),
        )

    # Overall stats
    df = batch.get("fl_profile", pd.DataFrame())
    if df.empty:
        return None
    r = df.iloc[0]
    total_winners = _safe_int(r.get("total_winners"))
    total_losers = _safe_int(r.get("total_losers"))
    if grouped_metrics is not None:
        total_winners = grouped_metrics["total_winners"]
        total_losers = grouped_metrics["total_losers"]
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
            winners = int(sr["winners"])
            losers = int(sr["losers"])
            if grouped_metrics is not None:
                grouped = grouped_metrics["by_strategy"].get(str(sr["strategy"]))
                if grouped is not None:
                    winners = grouped["num_winners"]
                    losers = grouped["num_losers"]
            closed = winners + losers
            strategies.append({
                "strategy": sr["strategy"],
                "total_return": float(sr["total_return"]),
                "win_rate": winners / closed if closed else 0,
                "winners": winners,
                "losers": losers,
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

    # Win/loss asymmetry — the core insight. The same canonical grouping
    # powers Strategies and Trader Profile, including independent spreads
    # opened on the same date and expiry.
    if grouped_metrics is not None:
        avg_win = grouped_metrics["avg_win"]
        avg_loss = grouped_metrics["avg_loss"]
        gross_wins = grouped_metrics["gross_wins"]
        gross_losses = grouped_metrics["gross_losses"]
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

    # Busiest month uses those same grouped opens; a spread counts once.
    profile["busiest_month"] = (
        grouped_metrics["busiest_month"] if grouped_metrics is not None else None
    )
    profile["busiest_month_trades"] = (
        grouped_metrics["busiest_month_trades"] if grouped_metrics is not None else 0
    )

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
        profile = _build_profile(client, where, tenant_and, tenant_ids)
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
