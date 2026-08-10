"""Accounts page (/accounts, /accounts/breakdown) — account performance.

Extracted verbatim from app/routes.py (routes.py refactor, Aug 2026).
Endpoint names unchanged (`accounts`, `accounts_breakdown_fragment`).
ACCOUNT_LEGS_QUERY is also consumed by the positions page tag scoping
(re-exported through app.routes).
"""

from datetime import datetime, date, timedelta  # noqa: F401

import json
import pandas as pd
from flask import render_template, request
from flask_login import login_required, current_user
from urllib.parse import quote_plus

from app import app
from app.bigquery_client import get_bigquery_client
from app.query_cache import cached_query_df, cached_payload, frame_fingerprint, timed
from app.skeleton import skeleton_page
from app.tenant_scope import (
    filter_df_by_tenant_ids as _filter_df_by_tenant_ids,
    tenant_sql_and as _tenant_sql_and,
)
from app.pnl_charts import CHART_DATA_ALL_QUERY, _build_account_chart_from_daily_pnl
from app.symbols_page import CURRENT_POSITIONS_QUERY, TRADES_QUERY
from app.routes import (
    _bq_parallel,
    _tags_for_leg_range,
    _tenants_for_scope,
    _user_account_list,
)


# ======================================================================
# Account Performance  (/accounts)
# ======================================================================

ACCOUNT_BALANCES_QUERY = """
    SELECT account, tenant_id, row_type, market_value, cost_basis,
           unrealized_pnl, unrealized_pnl_pct, percent_of_account
    FROM `ccwj-dbt.analytics.stg_account_balances`
    WHERE 1=1 {tenant_filter}
"""

STRATEGY_CLASSIFICATION_QUERY = """
    SELECT account, tenant_id, symbol, strategy, status, open_date, close_date,
           total_pnl, num_trades
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE 1=1 {tenant_filter}
"""

ACCOUNT_POSITIONS_SUMMARY_QUERY = """
    SELECT account, tenant_id, strategy,
           SUM(total_pnl) AS total_pnl,
           SUM(realized_pnl) AS realized_pnl,
           SUM(unrealized_pnl) AS unrealized_pnl,
           SUM(total_premium_received) AS premium_received,
           SUM(total_premium_paid) AS premium_paid,
           SUM(num_individual_trades) AS num_trades,
           SUM(num_winners) AS num_winners,
           SUM(num_losers) AS num_losers,
           SUM(total_dividend_income) AS dividend_income,
           SUM(total_return) AS total_return
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {tenant_filter}
    GROUP BY account, tenant_id, strategy
    ORDER BY account, strategy
"""

# Per-day external cash flow (deposits +, withdrawals −) for the /accounts
# "Net deposits" card. net_deposit_today already carries the signed amount.
# Fetched in the same _bq_parallel wave as everything else; if the mart
# predates transfer capture the query fails, the frame comes back
# column-less, and the card simply hides itself.
NET_DEPOSITS_QUERY = """
    SELECT tenant_id, account, user_id, date, net_deposit_today
    FROM `ccwj-dbt.analytics.mart_wealth_daily`
    WHERE 1=1 {tenant_filter}
    ORDER BY date
"""

# Per-leg rollup powering the /accounts Tag Breakdown. Leg grain (chapter of a
# (tenant_id, symbol)) so mixed tagged/untagged legs of one symbol don't
# overstate — the breakdown sums combined_pnl of ONLY the tagged legs, matched
# to stored tags by date-containment in Python (_build_tag_breakdown).
ACCOUNT_LEGS_QUERY = """
    SELECT tenant_id, symbol, open_date, last_activity_date,
           equity_pnl, closed_options_pnl, open_options_pnl,
           combined_pnl, status
    FROM `ccwj-dbt.analytics.int_position_legs`
    WHERE 1=1 {tenant_filter}
"""


def _build_tag_breakdown(legs_df, tags_rows):
    """Roll up leg-level P&L by user-defined tag for the /accounts page.

    Leg grain: each ``int_position_legs`` row is matched to stored tags by
    (tenant_id) + date-containment (``_tags_for_leg_range``). A leg carrying N
    tags contributes to N buckets; net_pnl sums ONLY the tagged legs'
    ``combined_pnl`` so mixed tagged/untagged legs of one symbol don't
    overstate. Returns rows sorted by net_pnl desc.
    """
    if legs_df is None or legs_df.empty or not tags_rows:
        return []

    df = legs_df.copy()
    for col in ("equity_pnl", "closed_options_pnl", "open_options_pnl", "combined_pnl"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            df[col] = 0.0

    buckets = {}
    for _, r in df.iterrows():
        tenant_id = str(r.get("tenant_id") or "")
        symbol = str(r.get("symbol") or "").upper()
        matched = _tags_for_leg_range(
            tags_rows, tenant_id, r.get("open_date"), r.get("last_activity_date"),
            symbol=symbol,
        )
        if not matched:
            continue
        equity = float(r.get("equity_pnl") or 0)
        option = float(r.get("closed_options_pnl") or 0) + float(r.get("open_options_pnl") or 0)
        combined = float(r.get("combined_pnl") or (equity + option))
        for tag in matched:
            b = buckets.setdefault(tag, {
                "tag": tag, "num_legs": 0, "symbols": set(),
                "equity_pnl": 0.0, "option_pnl": 0.0, "net_pnl": 0.0,
                "wins": 0, "losses": 0,
            })
            b["num_legs"] += 1
            if symbol:
                b["symbols"].add(symbol)
            b["equity_pnl"] += equity
            b["option_pnl"] += option
            b["net_pnl"] += combined
            if combined > 0:
                b["wins"] += 1
            elif combined < 0:
                b["losses"] += 1

    out = []
    for b in buckets.values():
        decided = b["wins"] + b["losses"]
        out.append({
            "tag": b["tag"],
            "num_legs": b["num_legs"],
            "num_symbols": len(b["symbols"]),
            "equity_pnl": round(b["equity_pnl"], 2),
            "option_pnl": round(b["option_pnl"], 2),
            "net_pnl": round(b["net_pnl"], 2),
            "wins": b["wins"],
            "losses": b["losses"],
            "win_rate": round(100.0 * b["wins"] / decided, 1) if decided else 0.0,
        })
    out.sort(key=lambda x: x["net_pnl"], reverse=True)
    return out



def _build_strategy_time_chart(strat_df):
    """
    Build cumulative P&L over time per strategy from trade-group data.
    Closed groups → P&L attributed to close_date.
    Open groups   → P&L attributed to today.
    """
    if strat_df.empty:
        return {"dates": [], "series": {}}

    today = date.today()
    rows = []
    for _, r in strat_df.iterrows():
        pnl_date = r["close_date"] if r["status"] == "Closed" and pd.notna(r["close_date"]) else today
        rows.append({"strategy": r["strategy"], "pnl_date": pnl_date, "pnl": float(r["total_pnl"])})

    events = pd.DataFrame(rows)
    events["pnl_date"] = pd.to_datetime(events["pnl_date"]).dt.date

    # Sum P&L per (strategy, date)
    grouped = events.groupby(["strategy", "pnl_date"])["pnl"].sum().reset_index()
    strategies = sorted(grouped["strategy"].unique())
    all_dates = sorted(grouped["pnl_date"].unique())

    series = {}
    for strat in strategies:
        strat_data = grouped[grouped["strategy"] == strat].set_index("pnl_date")["pnl"]
        cum = 0.0
        vals = []
        for d in all_dates:
            cum += float(strat_data.get(d, 0))
            vals.append(round(cum, 2))
        series[strat] = vals

    dates_out = [str(d) for d in all_dates]

    # Anchor every strategy line at $0 the day before the first event so the
    # un-rebased All-time view starts at zero (mirrors the summary chart's
    # anchor in _build_account_chart_from_daily_pnl). Terminals are untouched.
    if dates_out:
        anchor = (all_dates[0] - timedelta(days=1)).isoformat()
        dates_out.insert(0, anchor)
        for strat in series:
            series[strat].insert(0, 0.0)

    return {
        "dates": dates_out,
        "series": series,
    }



def _validate_accounts_financial_frames(dfs, *, needs_trade_accounts=False):
    """Reject failed /accounts query frames before they become plausible $0s.

    BigQuery preserves the selected columns for a legitimate zero-row result.
    ``_bq_parallel`` returns a column-less frame only when that query failed.
    The balances and summary frames drive the headline financial KPIs, so
    treating their failures as real zero balances silently misstates the
    account. Admins also need the trades frame's account column for the picker.
    """
    required = {
        "balances": {"row_type", "market_value", "cost_basis"},
        "strat_summary": {
            "realized_pnl",
            "unrealized_pnl",
            "dividend_income",
            "total_return",
            "num_winners",
            "num_losers",
        },
    }
    if needs_trade_accounts:
        required["trades"] = {"account"}

    failures = []
    for name, expected in required.items():
        frame = dfs.get(name)
        columns = set(frame.columns) if isinstance(frame, pd.DataFrame) else set()
        missing = sorted(expected - columns)
        if missing:
            failures.append(f"{name} ({', '.join(missing)})")

    if failures:
        raise RuntimeError(
            "Required account data unavailable: " + "; ".join(failures)
        )


ACCOUNTS_RANGE_DAYS = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365}
ACCOUNTS_VALID_RANGES = {"1M", "3M", "6M", "YTD", "1Y", "ALL"}


def _build_account_breakdowns(attribution_df, strat_class_df, range_start):
    """Build the four windowed breakdown tables (position / strategy / sector /
    subsector) + totals from the attribution frame. Shared by the /accounts
    page and the /accounts/breakdown fragment endpoint (the latter powers the
    instant client-side time-frame switch — see accounts.html). ``range_start``
    windows _build_position_breakdown to open + closed-in-window positions;
    None = lifetime."""
    from app.weekly_review import (
        _build_position_breakdown,
        _aggregate_breakdown_by,
        _build_breakdown_totals,
        _strategy_for_symbol,
    )
    out = {
        "position_breakdown": [],
        "position_breakdown_totals": None,
        "strategy_breakdown": [],
        "strategy_breakdown_totals": None,
        "sector_breakdown": [],
        "subsector_breakdown": [],
    }
    try:
        # strategy_by_symbol: largest abs-P&L strategy label per symbol
        # (matches the "primary strategy" lens on /positions).
        strategy_by_symbol = {}
        if strat_class_df is not None and not strat_class_df.empty:
            sb = (
                strat_class_df.groupby(["symbol", "strategy"], dropna=False)["total_pnl"]
                .sum()
                .reset_index()
            )
            lookup = {}
            for _, r in sb.iterrows():
                sym = str(r.get("symbol") or "")
                lookup.setdefault(sym, []).append(
                    {"strategy": r.get("strategy"), "total_pnl": r.get("total_pnl")}
                )
            for sym, classes in lookup.items():
                strategy_by_symbol[sym] = _strategy_for_symbol(sym, {sym: classes})

        pb = _build_position_breakdown(
            attribution_df, strategy_by_symbol, week_start=range_start,
        )
        out["position_breakdown"] = pb
        out["position_breakdown_totals"] = _build_breakdown_totals(pb)
        out["strategy_breakdown"] = _aggregate_breakdown_by(
            pb, "strategy", label_name="strategy"
        )
        out["strategy_breakdown_totals"] = _build_breakdown_totals(
            out["strategy_breakdown"]
        )
        out["sector_breakdown"] = _aggregate_breakdown_by(
            pb, "sector", label_name="sector"
        )
        out["subsector_breakdown"] = _aggregate_breakdown_by(
            pb, "subsector", label_name="subsector"
        )
    except Exception as exc:
        app.logger.warning("Account breakdown tables failed: %s", exc)
    return out


def _accounts_range_start(range_key, today=None):
    """Cutoff date for the /accounts time filter (None for ALL). Mirrors the
    client-side ``RANGE_DAYS`` / YTD logic in accounts.html so the server-side
    windowed KPIs + breakdown tables agree with the client-sliced charts."""
    today = today or date.today()
    rk = (range_key or "ALL").upper()
    if rk == "YTD":
        return date(today.year, 1, 1)
    days = ACCOUNTS_RANGE_DAYS.get(rk)
    return (today - timedelta(days=days)) if days else None


def _accounts_scope_query(args):
    """Encode the effective /accounts scope for range-switch URLs.

    Keep the same precedence as ``_tenants_for_scope``.  In particular, a
    direct ``?tenant=`` deep-link identifies one physical account even when
    several accounts share the same display label.
    """
    for key in ("tenant", "tenants", "account"):
        value = (args.get(key) or "").strip()
        if value:
            return f"{key}={quote_plus(value)}"
    return ""


@app.route("/accounts")
@login_required
@skeleton_page
def accounts():
    # One "Accounts" surface, two views (Aug 2026 surface audit):
    # Performance (this function, default) and Value & composition (the
    # former /wealth page — daily balance, cash/equity/options split,
    # income, deposits toggle). Deferred import: wealth.py imports from
    # app.routes at module load.
    if (request.args.get("view") or "").strip().lower() == "value":
        from app.wealth import render_wealth_view
        return render_wealth_view()

    client = get_bigquery_client()
    user_accounts = _user_account_list()
    selected_account = request.args.get("account", "")
    account_scope_query = _accounts_scope_query(request.args)
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

    # Time frame filter (applies to the whole page). The P&L-earned KPI cards
    # and the four breakdown tables are windowed server-side to this range;
    # the point-in-time cards (Account Value / Cash / Invested / current
    # Unrealized) always reflect "now". ALL = lifetime (default).
    selected_range = (request.args.get("range", "ALL") or "ALL").upper()
    if selected_range not in ACCOUNTS_VALID_RANGES:
        selected_range = "ALL"
    range_start = _accounts_range_start(selected_range)
    # Window the attribution breakdowns by feeding the range cutoff as the
    # attribution ``week_start`` (open positions + groups closed on/after the
    # cutoff). ALL passes the far-past sentinel = lifetime.
    attribution_week_start = (
        range_start.isoformat() if range_start else None  # set below
    )

    # Attribution query lives in app.weekly_review. Deferred import:
    # weekly_review imports from app.routes at module load, so a top-level
    # import here would be circular. The breakdown builders are invoked via
    # _build_account_breakdowns (shared with the fragment endpoint).
    from app.weekly_review import (
        POSITION_ATTRIBUTION_QUERY,
        ATTRIBUTION_LIFETIME_SENTINEL,
    )
    if attribution_week_start is None:
        attribution_week_start = ATTRIBUTION_LIFETIME_SENTINEL

    try:
        dfs = _bq_parallel(client, {
            "balances": ACCOUNT_BALANCES_QUERY.format(tenant_filter=tenant_filter),
            "trades": TRADES_QUERY.format(tenant_filter=tenant_filter),
            "current": CURRENT_POSITIONS_QUERY.format(tenant_filter=tenant_filter),
            "strat_class": STRATEGY_CLASSIFICATION_QUERY.format(tenant_filter=tenant_filter),
            "strat_summary": ACCOUNT_POSITIONS_SUMMARY_QUERY.format(tenant_filter=tenant_filter),
            # Windowed by the selected time frame (ALL = far-past sentinel so
            # every closed group counts; a range cutoff scopes each
            # per-asset-class P&L column to open + closed-in-window groups).
            "attribution": POSITION_ATTRIBUTION_QUERY.format(
                tenant_filter=tenant_filter, week_start=attribution_week_start),
            # Per-leg rollup for the Tag Breakdown card (leg grain).
            "legs": ACCOUNT_LEGS_QUERY.format(tenant_filter=tenant_filter),
            # External cash flow for the "Net deposits" KPI card.
            "net_deposits": NET_DEPOSITS_QUERY.format(tenant_filter=tenant_filter),
        })
        _validate_accounts_financial_frames(
            dfs, needs_trade_accounts=not bool(user_accounts)
        )
        balances_df = dfs["balances"]
        trades_df = dfs["trades"]
        current_df = dfs["current"]
        strat_class_df = dfs["strat_class"]
        strat_summary_df = dfs["strat_summary"]
        attribution_df = dfs["attribution"]
        legs_df = dfs["legs"]
        net_deposits_df = dfs.get("net_deposits")
    except Exception as exc:
        return render_template(
            "accounts.html",
            error=str(exc),
            kpis={},
            summary_chart_json="{}",
            strategy_chart_json="{}",
            realized_events_json="[]",
            net_deposit_events_json="[]",
            strategy_rows=[],
            position_breakdown=[],
            position_breakdown_totals=None,
            strategy_breakdown=[],
            strategy_breakdown_totals=None,
            sector_breakdown=[],
            subsector_breakdown=[],
            tag_breakdown=[],
            accounts=[],
            selected_account="",
            account_scope_query=account_scope_query,
            selected_range="ALL",
            period_kpis=None,
        )

    # ------------------------------------------------------------------
    # Clean types
    # ------------------------------------------------------------------
    for col in ["market_value", "cost_basis", "unrealized_pnl", "unrealized_pnl_pct", "percent_of_account"]:
        if col in balances_df.columns:
            balances_df[col] = pd.to_numeric(balances_df[col], errors="coerce").fillna(0)

    for col in ["amount", "quantity", "price", "fees"]:
        if col in trades_df.columns:
            trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
    if "trade_date" in trades_df.columns:
        trades_df["trade_date"] = pd.to_datetime(trades_df["trade_date"]).dt.date

    for col in ["unrealized_pnl", "market_value", "quantity", "current_price", "cost_basis"]:
        if col in current_df.columns:
            current_df[col] = pd.to_numeric(current_df[col], errors="coerce").fillna(0)

    for col in ["total_pnl", "num_trades"]:
        if col in strat_class_df.columns:
            strat_class_df[col] = pd.to_numeric(strat_class_df[col], errors="coerce").fillna(0)
    for col in ["open_date", "close_date"]:
        if col in strat_class_df.columns:
            strat_class_df[col] = pd.to_datetime(strat_class_df[col], errors="coerce").dt.date

    num_cols = ["total_pnl", "realized_pnl", "unrealized_pnl", "premium_received",
                "premium_paid", "num_trades", "num_winners", "num_losers",
                "dividend_income", "total_return"]
    for col in num_cols:
        if col in strat_summary_df.columns:
            strat_summary_df[col] = pd.to_numeric(strat_summary_df[col], errors="coerce").fillna(0)

    # ------------------------------------------------------------------
    # Safety-belt: re-filter in Python (SQL already filtered by account)
    # ------------------------------------------------------------------
    balances_df = _filter_df_by_tenant_ids(balances_df, tenant_ids)
    trades_df = _filter_df_by_tenant_ids(trades_df, tenant_ids)
    current_df = _filter_df_by_tenant_ids(current_df, tenant_ids)
    strat_class_df = _filter_df_by_tenant_ids(strat_class_df, tenant_ids)
    strat_summary_df = _filter_df_by_tenant_ids(strat_summary_df, tenant_ids)
    attribution_df = _filter_df_by_tenant_ids(attribution_df, tenant_ids)
    legs_df = _filter_df_by_tenant_ids(legs_df, tenant_ids)

    # Picker lists the full disambiguated account set (non-admin) so every
    # physical account is selectable after tenant scope narrows the data.
    all_accounts = (
        sorted(user_accounts)
        if user_accounts
        else sorted(trades_df["account"].dropna().unique())
    )
    selected_account = request.args.get("account", "")
    # tenant scope (resolved from selected_account → tenant_ids above) already
    # narrowed every frame; no secondary label-equality narrowing needed
    # (which would break for disambiguated colliding labels).

    # ------------------------------------------------------------------
    # KPIs from balances
    # ------------------------------------------------------------------
    # Query failures were rejected above. A zero-row frame that still has the
    # selected schema is legitimate (for example, an account with no balance
    # snapshots yet) and should render as zero.
    if not balances_df.empty and "row_type" in balances_df.columns:
        cash_rows = balances_df[balances_df["row_type"] == "cash"]
        total_rows = balances_df[balances_df["row_type"] == "account_total"]
        cash_balance = (
            float(cash_rows["market_value"].sum())
            if "market_value" in cash_rows.columns else 0.0
        )
        account_value = (
            float(total_rows["market_value"].sum())
            if "market_value" in total_rows.columns else 0.0
        )
        acct_cost_basis = (
            float(total_rows["cost_basis"].sum())
            if "cost_basis" in total_rows.columns else 0.0
        )
    else:
        cash_balance = account_value = acct_cost_basis = 0.0
    invested_value = account_value - cash_balance

    # Realized + unrealized + total_return all come from the same source
    # (positions_summary) so the three KPIs reconcile: total_return =
    # realized + unrealized + dividends. Mixing the snapshot's unrealized
    # with positions_summary's realized has shipped a $300+ discrepancy.
    def _sum_col(df, col):
        return float(df[col].sum()) if col in df.columns else 0.0

    realized_pnl = _sum_col(strat_summary_df, "realized_pnl")
    acct_unrealized = _sum_col(strat_summary_df, "unrealized_pnl")
    total_return = _sum_col(strat_summary_df, "total_return")
    # Surfacing dividends as its own KPI so the math reconciles for the
    # reader: realized + unrealized + dividends = total return. Without
    # this card the row silently failed by ~$200-300 (the missing piece
    # was always dividends), and investors / power users noticed.
    dividend_income = (
        float(strat_summary_df["dividend_income"].sum())
        if "dividend_income" in strat_summary_df.columns else 0.0
    )

    # ------------------------------------------------------------------
    # Net deposits / withdrawals (external cash the trader moved in/out).
    # The P&L cards + chart are already deposit-free (they read realized /
    # unrealized trading P&L, not balance), but Account Value on its own
    # can't tell "I'm up" from "I added money". Surfacing net deposits next
    # to it — and shipping per-day events so the card re-windows client-side
    # on a time-frame switch (mirrors REALIZED_EVENTS) — closes that gap.
    # Defensive: mart_wealth_daily may predate transfer capture, so a
    # missing column just yields 0 / [] and the card hides itself.
    # ------------------------------------------------------------------
    net_deposits_lifetime = 0.0
    net_deposit_events = []
    try:
        nd_df = _filter_df_by_tenant_ids(net_deposits_df, tenant_ids)
        if nd_df is not None and not nd_df.empty and "net_deposit_today" in nd_df.columns:
            nd_df = nd_df.copy()
            nd_df["net_deposit_today"] = pd.to_numeric(
                nd_df["net_deposit_today"], errors="coerce"
            ).fillna(0)
            by_day = (
                nd_df.groupby("date", as_index=False)["net_deposit_today"].sum()
                .sort_values("date")
            )
            net_deposits_lifetime = float(by_day["net_deposit_today"].sum())
            for d_, a_ in zip(by_day["date"], by_day["net_deposit_today"]):
                amt = float(a_ or 0)
                if abs(amt) < 0.005:
                    continue
                d_iso = d_.isoformat() if hasattr(d_, "isoformat") else str(d_)
                net_deposit_events.append([d_iso, round(amt, 2)])
    except Exception as exc:
        app.logger.warning("Account net-deposits rollup failed: %s", exc)

    kpis = {
        "account_value": account_value,
        "cash_balance": cash_balance,
        "invested_value": invested_value,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": acct_unrealized,
        "dividend_income": dividend_income,
        "total_return": total_return,
        "net_deposits": round(net_deposits_lifetime, 2),
        "has_transfers": abs(net_deposits_lifetime) > 0.005,
    }

    # ------------------------------------------------------------------
    # Chart 1: Cumulative P&L over time (summary) — from mart_daily_pnl
    # ------------------------------------------------------------------
    try:
        chart_tenant_ids = _tenants_for_scope(selected_account)
        chart_tenant_filter = _tenant_sql_and(chart_tenant_ids)
        chart_df = cached_query_df(
            client,
            CHART_DATA_ALL_QUERY.format(tenant_filter=chart_tenant_filter)
        )
        chart_df = _filter_df_by_tenant_ids(chart_df, chart_tenant_ids)
        # tenant scope already narrowed chart_df to the selected account's tenant
        with timed("acct_chart"):
            summary_chart = cached_payload(
                ("acct_chart", str(date.today()), frame_fingerprint(chart_df, current_df)),
                lambda: _build_account_chart_from_daily_pnl(chart_df, current_df),
            )
    except Exception as exc:
        app.logger.exception(
            "accounts chart query or build failed for account=%r: %s",
            selected_account, exc,
        )
        summary_chart = {"dates": [], "equity": [], "options": [], "dividends": [], "total": []}

    # ------------------------------------------------------------------
    # Windowed P&L cards (only when a time frame other than ALL is active).
    # These REPLACE the lifetime Realized / Dividends / Total Return cards
    # with their "this period" equivalents so the KPI row agrees with the
    # windowed charts + breakdown tables. The point-in-time cards
    # (Account Value / Cash / Invested / current Unrealized) always stay
    # "as of now". Net P&L this period and Dividends this period are the
    # windowed deltas of the cumulative chart series (so the top-line card
    # equals the chart's terminal for the same window); Realized this period
    # is summed straight from groups CLOSED within the window.
    # ------------------------------------------------------------------
    period_kpis = None
    if range_start is not None and summary_chart.get("dates"):
        dts = summary_chart["dates"]
        # Anchor the window on the LAST chart date (not today) so the card
        # deltas match the client-side chart rebase exactly — accounts.html
        # computes its cutoff from ``dates[dates.length-1]``. On a weekend
        # this differs from a today-anchored cutoff by a couple of days.
        card_cutoff = _accounts_range_start(
            selected_range, date.fromisoformat(dts[-1])
        ) or range_start
        cutoff_iso = card_cutoff.isoformat()
        start_idx = next((i for i, d in enumerate(dts) if d >= cutoff_iso),
                         max(0, len(dts) - 1))
        tot = summary_chart.get("total") or []
        divs = summary_chart.get("dividends") or []
        net_period = round(tot[-1] - tot[start_idx], 2) if tot else 0.0
        div_period = round(divs[-1] - divs[start_idx], 2) if divs else 0.0
        realized_period = 0.0
        if (not strat_class_df.empty and "close_date" in strat_class_df.columns
                and "total_pnl" in strat_class_df.columns):
            cd = pd.to_datetime(strat_class_df["close_date"], errors="coerce").dt.date
            mask = (
                (strat_class_df.get("status") == "Closed")
                & cd.notna()
                & (cd >= card_cutoff)
            )
            realized_period = float(strat_class_df.loc[mask, "total_pnl"].sum())
        period_kpis = {
            "net": net_period,
            "dividends": div_period,
            "realized": round(realized_period, 2),
            "start": dts[start_idx] if dts else None,
        }

    # ------------------------------------------------------------------
    # Chart 2: Strategy P&L over time
    # ------------------------------------------------------------------
    strategy_chart = _build_strategy_time_chart(strat_class_df)

    # ------------------------------------------------------------------
    # Strategy summary table
    # ------------------------------------------------------------------
    # The SQL is tenant-grained (so the tenant filter above can apply);
    # collapse back to the (account, strategy) display grain here so a user
    # with colliding account labels doesn't see duplicate strategy rows.
    if not strat_summary_df.empty:
        _sum_cols = [c for c in num_cols if c in strat_summary_df.columns]
        strat_summary_df = (
            strat_summary_df.groupby(["account", "strategy"], as_index=False)[_sum_cols]
            .sum()
        )
        strat_summary_df["win_rate"] = strat_summary_df.apply(
            lambda r: r["num_winners"] / (r["num_winners"] + r["num_losers"])
            if (r["num_winners"] + r["num_losers"]) > 0 else 0,
            axis=1,
        )
        strategy_rows = strat_summary_df.to_dict(orient="records")
    else:
        strategy_rows = []

    # ------------------------------------------------------------------
    # Detailed breakdown tables (the per-symbol / strategy / sector /
    # subsector "CC Trading Summary" the Daily Review account scorecard
    # drills into). Same Stock | Options | Dividend | Net | % | Annualized
    # shape, windowed to the selected time frame (range_start; ALL = None =
    # the SENTINEL passed to the SQL above, so every closed group counts).
    # All four pull from POSITION_ATTRIBUTION_QUERY. On a time-frame switch
    # the browser re-fetches these four tables from /accounts/breakdown
    # (accounts_breakdown_fragment) via the same _build_account_breakdowns
    # helper — no full page reload.
    # ------------------------------------------------------------------
    tag_breakdown = []
    # Tag Breakdown (leg-grained): match the user's Postgres leg tags to the
    # scoped legs by date-containment. Scoped to in-scope tenant_ids so a
    # narrowed account view only rolls up that account's tagged legs.
    # (Lifetime — not windowed by the time filter; it stays fixed while the
    # four windowed breakdown tables swap via the fragment endpoint.)
    try:
        from app.models import get_all_leg_tags_for_user as _get_all_leg_tags_for_user
        _tag_rows = _get_all_leg_tags_for_user(current_user.id, tenant_ids)
        tag_breakdown = _build_tag_breakdown(legs_df, _tag_rows)
    except Exception as exc:
        app.logger.warning("Account tag breakdown failed: %s", exc)

    breakdowns = _build_account_breakdowns(attribution_df, strat_class_df, range_start)

    # Closed-group (close_date, total_pnl) pairs so the browser can recompute
    # "Realized (period)" instantly on a time-frame switch without a round trip
    # (see accounts.html realizedInWindow()).
    realized_events = []
    if (not strat_class_df.empty and "close_date" in strat_class_df.columns
            and "total_pnl" in strat_class_df.columns):
        _cd = pd.to_datetime(strat_class_df["close_date"], errors="coerce").dt.date
        _closed = strat_class_df[(strat_class_df.get("status") == "Closed") & _cd.notna()]
        _cd2 = pd.to_datetime(_closed["close_date"], errors="coerce").dt.date
        for d_, p_ in zip(_cd2, _closed["total_pnl"]):
            realized_events.append([d_.isoformat(), float(p_ or 0)])

    return render_template(
        "accounts.html",
        kpis=kpis,
        summary_chart_json=json.dumps(summary_chart),
        strategy_chart_json=json.dumps(strategy_chart),
        realized_events_json=json.dumps(realized_events),
        net_deposit_events_json=json.dumps(net_deposit_events),
        strategy_rows=strategy_rows,
        position_breakdown=breakdowns["position_breakdown"],
        position_breakdown_totals=breakdowns["position_breakdown_totals"],
        strategy_breakdown=breakdowns["strategy_breakdown"],
        strategy_breakdown_totals=breakdowns["strategy_breakdown_totals"],
        sector_breakdown=breakdowns["sector_breakdown"],
        subsector_breakdown=breakdowns["subsector_breakdown"],
        tag_breakdown=tag_breakdown,
        accounts=all_accounts,
        selected_account=selected_account,
        account_scope_query=account_scope_query,
        selected_range=selected_range,
        period_kpis=period_kpis,
    )


# ----------------------------------------------------------------------
# /accounts/breakdown — HTML fragment for the 4 windowed breakdown tables.
# Powers the INSTANT client-side time-frame switch on /accounts: the buttons
# slice the charts + recompute the KPI cards in the browser (no round trip)
# and fetch just this fragment to refresh the tables, instead of reloading the
# whole page (which re-ran the full BQ wave + re-initialized both charts).
# Tenant-scoped exactly like /accounts; renders only the tables partial.
# ----------------------------------------------------------------------
@app.route("/accounts/breakdown")
@login_required
def accounts_breakdown_fragment():
    client = get_bigquery_client()
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

    selected_range = (request.args.get("range", "ALL") or "ALL").upper()
    if selected_range not in ACCOUNTS_VALID_RANGES:
        selected_range = "ALL"
    range_start = _accounts_range_start(selected_range)

    from app.weekly_review import (
        POSITION_ATTRIBUTION_QUERY,
        ATTRIBUTION_LIFETIME_SENTINEL,
    )
    attribution_week_start = (
        range_start.isoformat() if range_start else ATTRIBUTION_LIFETIME_SENTINEL
    )
    try:
        dfs = _bq_parallel(client, {
            "attribution": POSITION_ATTRIBUTION_QUERY.format(
                tenant_filter=tenant_filter, week_start=attribution_week_start),
            "strat_class": STRATEGY_CLASSIFICATION_QUERY.format(tenant_filter=tenant_filter),
        })
        attribution_df = dfs["attribution"]
        strat_class_df = dfs["strat_class"]
    except Exception as exc:
        app.logger.warning("Account breakdown fragment query failed: %s", exc)
        # Empty frames render an empty (but valid) fragment.
        import pandas as _pd
        attribution_df = _pd.DataFrame()
        strat_class_df = _pd.DataFrame()

    breakdowns = _build_account_breakdowns(attribution_df, strat_class_df, range_start)
    return render_template(
        "_accounts_breakdowns.html",
        position_breakdown=breakdowns["position_breakdown"],
        position_breakdown_totals=breakdowns["position_breakdown_totals"],
        strategy_breakdown=breakdowns["strategy_breakdown"],
        strategy_breakdown_totals=breakdowns["strategy_breakdown_totals"],
        sector_breakdown=breakdowns["sector_breakdown"],
        subsector_breakdown=breakdowns["subsector_breakdown"],
        # So row-click drill-downs in the fragment carry the account scope
        # (the inline include on /accounts inherits this from the page).
        selected_account=selected_account,
    )



