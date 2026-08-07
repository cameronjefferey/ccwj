"""Symbols page (/symbols) — per-symbol daily detail cards.

Extracted verbatim from app/routes.py (routes.py refactor, Aug 2026).
Endpoint name unchanged (`symbols_detail`). Mostly superseded by
Position Detail per AGENTS.md, but still the navigation step between
the dashboard and per-symbol drill-down.

TRADES_QUERY / CURRENT_POSITIONS_QUERY are also consumed by the
/accounts page (imported from here).
"""

from datetime import date

import json
import pandas as pd
from flask import render_template, request, redirect, url_for
from flask_login import login_required, current_user
from urllib.parse import quote_plus

from app import app
from app.bigquery_client import get_bigquery_client
from app.query_cache import cached_query_df, cached_payload, frame_fingerprint, timed
from app.models import is_admin
from app.tenant_scope import (
    filter_df_by_tenant_ids as _filter_df_by_tenant_ids,
    tenant_sql_and as _tenant_sql_and,
)
from app.pnl_charts import (
    CHART_DATA_ALL_QUERY,
    _align_position_pnl_chart_with_kpi,
    _build_chart_from_daily_pnl,
    _chart_data_for_json,
)
from app.routes import (
    _bq_parallel,
    _df_normalize_account_column,
    _iter_symbols_for_daily_detail,
    _redirect_if_no_accounts,
    _tenants_for_scope,
    _user_account_list,
)


# ======================================================================
# Daily Position Detail  (/symbols)
# ======================================================================

TRADES_QUERY = """
    SELECT
        account,
        underlying_symbol AS symbol,
        trade_date,
        action,
        action_raw,
        trade_symbol,
        instrument_type,
        description,
        quantity,
        price,
        fees,
        amount
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE underlying_symbol IS NOT NULL
      AND trade_date IS NOT NULL
      {tenant_filter}
    ORDER BY underlying_symbol, trade_date
"""

OPEN_SESSION_START_QUERY = """
    SELECT
        account,
        symbol,
        MIN(open_date) AS open_start
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE status = 'Open'
      {tenant_filter}
    GROUP BY account, symbol
"""


CLOSED_LEGS_QUERY = """
    SELECT
        sc.account,
        sc.symbol,
        sc.strategy,
        sc.trade_symbol,
        sc.open_date,
        sc.close_date,
        sc.total_pnl,
        sc.status,
        oc.contracts_sold_to_open + oc.contracts_bought_to_open AS quantity,
        oc.premium_received,
        oc.premium_paid,
        oc.cost_to_close,
        oc.proceeds_from_close,
        oc.direction,
        oc.close_type,
        oc.days_in_trade
    FROM `ccwj-dbt.analytics.int_strategy_classification` sc
    JOIN `ccwj-dbt.analytics.int_option_contracts` oc
      ON (sc.tenant_id IS NOT DISTINCT FROM oc.tenant_id)
     AND sc.account = oc.account
     AND sc.trade_symbol = oc.trade_symbol
     AND sc.user_id IS NOT DISTINCT FROM oc.user_id
    WHERE sc.status = 'Closed'
      AND sc.trade_group_type = 'option_contract'
      {closed_legs_tenant_filter}
"""

CLOSED_EQUITY_LEGS_QUERY = """
    SELECT
        account,
        symbol,
        trade_symbol,
        open_date,
        close_date,
        quantity,
        sale_price_per_share,
        sell_proceeds,
        cost_basis,
        realized_pnl,
        description
    FROM `ccwj-dbt.analytics.int_closed_equity_legs`
    WHERE 1=1 {tenant_filter}
"""

CURRENT_POSITIONS_QUERY = """
    SELECT
        account,
        underlying_symbol AS symbol,
        instrument_type,
        trade_symbol,
        description,
        quantity,
        current_price,
        market_value,
        cost_basis,
        unrealized_pnl,
        unrealized_pnl_pct
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE 1=1 {tenant_filter}
"""

STRATEGIES_MAP_QUERY = """
    SELECT account, symbol, strategy
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {tenant_filter}
"""

SYMBOLS_PNL_QUERY = """
    SELECT account, symbol, status, realized_pnl, unrealized_pnl
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {tenant_filter}
"""


def _finish_symbol_chart(sym_chart_df, sym_current, positions_only,
                         closed_options_pnl, options_open_pnl):
    """Build ONE symbol's cumulative-P&L chart payload (heavy) and rebase it.

    This wraps the expensive stateful ``_build_chart_from_daily_pnl`` walk —
    the /symbols cold-load hotspot. It is the SINGLE builder for a symbol's
    chart: the page calls it only for the active symbol, and every other
    symbol re-enters the same ``/symbols`` handler via the navigate-mode tab
    strip (``?symbol=``), so an eager build and an on-demand build are always
    byte-identical (no second code path to drift).

    ``sym_chart_df`` must already be sliced to the (account, symbol) and, for
    ``positions_only``, clipped to the open-session start — exactly as the
    per-symbol loop prepares it.
    """
    with timed("symbol_charts"):
        chart = cached_payload(
            ("sym_chart", str(date.today()), frame_fingerprint(sym_chart_df, sym_current)),
            lambda sdf=sym_chart_df, scur=sym_current: _build_chart_from_daily_pnl(sdf, scur),
        )

    # When viewing "this position only", rebase chart so it starts at 0
    # (first point = start of position, not cumulative from prior history)
    if positions_only and chart.get("dates") and len(chart["dates"]) > 0:
        base_equity = chart["equity"][0] if chart["equity"] else 0
        base_options = chart["options"][0] if chart["options"] else 0
        base_dividends = chart["dividends"][0] if chart["dividends"] else 0
        base_total = chart["total"][0] if chart["total"] else 0
        chart["equity"] = [round(x - base_equity, 2) for x in chart["equity"]]
        chart["options"] = [round(x - base_options, 2) for x in chart["options"]]
        chart["dividends"] = [round(x - base_dividends, 2) for x in chart["dividends"]]
        chart["total"] = [round(x - base_total, 2) for x in chart["total"]]
        # If this position has no open equity (options-only), strip equity from the
        # chart so we don't show phantom spikes from past equity trades in the mart.
        has_open_equity = not sym_current.empty and (
            (sym_current["instrument_type"] == "Equity").any()
        )
        if not has_open_equity:
            n = len(chart["dates"])
            for i in range(n):
                chart["total"][i] = round(chart["total"][i] - chart["equity"][i], 2)
                chart["equity"][i] = 0
        # Anchor the last options point to closed OPTION legs + current open
        # option unrealized only.  Equity realized P&L (shares sold/called away)
        # is already captured by the natural avg-cost equity calculation and must
        # not be added to the options series — doing so double-counts it and
        # causes a spurious drop to -$3k on the final data point.
        chart["options"][-1] = round(closed_options_pnl + options_open_pnl, 2)
        chart["total"][-1] = round(
            chart["equity"][-1] + chart["options"][-1] + chart["dividends"][-1], 2
        )

    return chart


@app.route("/symbols")
@login_required
def symbols_detail():
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

    try:
        dfs = _bq_parallel(client, {
            "trades": TRADES_QUERY.format(tenant_filter=tenant_filter),
            "current": CURRENT_POSITIONS_QUERY.format(tenant_filter=tenant_filter),
            "strat": STRATEGIES_MAP_QUERY.format(tenant_filter=tenant_filter),
            "pnl": SYMBOLS_PNL_QUERY.format(tenant_filter=tenant_filter),
            "open_start": OPEN_SESSION_START_QUERY.format(tenant_filter=tenant_filter),
            "closed_legs": CLOSED_LEGS_QUERY.format(
                closed_legs_tenant_filter=_tenant_sql_and(tenant_ids, col="sc.tenant_id")),
            "closed_equity": CLOSED_EQUITY_LEGS_QUERY.format(tenant_filter=tenant_filter),
        })
        trades_df = dfs["trades"]
        current_df = dfs["current"]
        strat_df = dfs["strat"]
        pnl_df = dfs["pnl"]
        open_start_df = dfs["open_start"]
        closed_legs_df = dfs["closed_legs"]
        closed_equity_df = dfs["closed_equity"]
    except Exception as exc:
        app.logger.exception("Daily P&L load failed: %s", exc)
        return render_template(
            "symbols.html",
            title="Daily P&L",
            error=str(exc),
            symbol_data=[],
            chart_data_json="[]",
            accounts=[],
            selected_account="",
            open_only=False,
            linked_brokerage_accounts=(user_accounts or []),
            viewer_is_admin=is_admin(current_user.username),
        )

    trades_df = _df_normalize_account_column(trades_df)
    current_df = _df_normalize_account_column(current_df)
    strat_df = _df_normalize_account_column(strat_df)
    pnl_df = _df_normalize_account_column(pnl_df)
    open_start_df = _df_normalize_account_column(open_start_df)
    closed_legs_df = _df_normalize_account_column(closed_legs_df)
    closed_equity_df = _df_normalize_account_column(closed_equity_df)

    # ------------------------------------------------------------------
    # Clean types
    # ------------------------------------------------------------------
    if not trades_df.empty:
        for col in ["amount", "quantity", "price", "fees"]:
            if col in trades_df.columns:
                trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
        if "trade_date" in trades_df.columns:
            trades_df["trade_date"] = pd.to_datetime(trades_df["trade_date"]).dt.date

    for col in ["unrealized_pnl", "market_value", "quantity", "current_price", "cost_basis"]:
        if col in current_df.columns:
            current_df[col] = pd.to_numeric(current_df[col], errors="coerce").fillna(0)
    if "unrealized_pnl_pct" in current_df.columns:
        current_df["unrealized_pnl_pct"] = pd.to_numeric(current_df["unrealized_pnl_pct"], errors="coerce").fillna(0)

    # Strategy map: (account, symbol) → sorted list of strategies
    strat_map = (
        strat_df.groupby(["account", "symbol"])["strategy"]
        .apply(lambda x: sorted(x.unique().tolist()))
        .to_dict()
    )

    # Open session start map: (account, symbol) → open_start date
    open_start_map = {}
    if "open_start_df" in locals() and not open_start_df.empty:
        for _, row in open_start_df.iterrows():
            key = (str(row["account"]), str(row["symbol"]))
            open_start_map[key] = row["open_start"]

    # Normalize closed_legs_df for date filtering
    if "closed_legs_df" in locals() and not closed_legs_df.empty:
        closed_legs_df["close_date"] = pd.to_datetime(closed_legs_df["close_date"], errors="coerce").dt.date
    else:
        closed_legs_df = pd.DataFrame()

    # ------------------------------------------------------------------
    # Safety-belt: re-filter in Python (SQL already filtered by account)
    # ------------------------------------------------------------------
    trades_df = _filter_df_by_tenant_ids(trades_df, tenant_ids)
    current_df = _filter_df_by_tenant_ids(current_df, tenant_ids)
    strat_df = _filter_df_by_tenant_ids(strat_df, tenant_ids)
    pnl_df = _filter_df_by_tenant_ids(pnl_df, tenant_ids)
    if not open_start_df.empty:
        open_start_df = _filter_df_by_tenant_ids(open_start_df, tenant_ids)
    if not closed_legs_df.empty:
        closed_legs_df = _filter_df_by_tenant_ids(closed_legs_df, tenant_ids)
    if not closed_equity_df.empty:
        closed_equity_df = _filter_df_by_tenant_ids(closed_equity_df, tenant_ids)

    def _unique_accounts(*frames):
        s = set()
        for f in frames:
            if f is not None and not f.empty and "account" in f.columns:
                for v in f["account"].dropna().unique():
                    t = str(v).strip()
                    if t:
                        s.add(t)
        return sorted(s)

    accounts = _unique_accounts(trades_df, pnl_df, current_df, strat_df)
    # Picker lists the full disambiguated account set (non-admin) so every
    # physical account stays selectable after tenant scope narrows the data.
    if user_accounts:
        accounts = sorted(
            {str(a).strip() for a in user_accounts if a and str(a).strip()}
        )
    selected_account = request.args.get("account", "")
    # Use getlist so duplicate params (e.g. open_only=1&open_only=0 from checkbox+hidden) don't break the filter
    open_only = "1" in request.args.getlist("open_only")
    positions_only = "1" in request.args.getlist("positions_only")

    # Redirect to canonical URL if duplicate params present (cleans bookmarks/cached URLs)
    open_list = request.args.getlist("open_only")
    pos_list = request.args.getlist("positions_only")
    if len(open_list) > 1 or len(pos_list) > 1:
        q = {"account": selected_account} if selected_account else {}
        if open_only:
            q["open_only"] = "1"
        if positions_only:
            q["positions_only"] = "1"
        return redirect(url_for("symbols_detail", **q))

    # "Open positions only" implies "symbols with open positions" filter.
    # If the user checks the second box but not the first, we still want
    # to restrict the symbol list to those with an open position.
    if positions_only and not open_only:
        open_only = True

    # No secondary ``account == selected_account`` narrowing: tenant scope
    # already filtered every frame to the selected account's tenant_id
    # (handles disambiguated colliding labels too).

    # Restrict to symbols that have a current open position (match current_positions / int_enriched_current)
    if open_only:
        open_pairs = set(zip(current_df["account"].astype(str), current_df["symbol"].astype(str))) if not current_df.empty else set()
    else:
        open_pairs = None

    # Fetch pre-aggregated chart data from mart
    try:
        tenant_filter = _tenant_sql_and(_tenants_for_scope(selected_account))
        all_chart_df = cached_query_df(
            client,
            CHART_DATA_ALL_QUERY.format(tenant_filter=tenant_filter)
        )
        all_chart_df = _filter_df_by_tenant_ids(all_chart_df, tenant_ids)
        # tenant scope already narrowed to the selected account's tenant
    except Exception:
        all_chart_df = pd.DataFrame()

    # ------------------------------------------------------------------
    # Build per-symbol data
    # ------------------------------------------------------------------
    symbol_data = []
    chart_data_list = []

    for account, symbol in _iter_symbols_for_daily_detail(
        trades_df, pnl_df, current_df, open_pairs
    ):
        group = trades_df[
            (trades_df["account"] == account) & (trades_df["symbol"] == symbol)
        ]
        if not group.empty and "trade_date" in group.columns:
            group = group.sort_values("trade_date")

        sym_current = current_df[
            (current_df["account"] == account) & (current_df["symbol"] == symbol)
        ]

        # Realized P&L: use positions_summary when available so mixed
        # open/closed symbols (e.g. RKLB) show historical realized plus
        # current unrealized. For symbols that are purely open (only
        # Open status and no closed trades), treat realized as 0.
        sym_pnl = pnl_df[
            (pnl_df["account"] == account) & (pnl_df["symbol"] == symbol)
        ]
        if not sym_pnl.empty:
            statuses = (
                sym_pnl["status"]
                .dropna()
                .astype(str)
                .str.lower()
                .str.strip()
                .unique()
                .tolist()
            )
            has_open = any(s == "open" for s in statuses)
            has_closed = any(s == "closed" for s in statuses)
            realized_val = float(sym_pnl["realized_pnl"].sum() or 0.0)
            # Purely open symbol (no closed legs): no realized yet, even
            # if the mart currently reports a negative net cash flow.
            if has_open and not has_closed:
                realized_val = 0.0
            total_realized = round(realized_val, 2)
        else:
            # Fallback: net cash flow from trades if mart row missing.
            total_realized = (
                round(float(group["amount"].sum()), 2)
                if not group.empty and "amount" in group.columns
                else 0.0
            )

        # Unrealized from current open positions (matches current positions table)
        unrealized = round(float(sym_current["unrealized_pnl"].sum()), 2) if not sym_current.empty else 0.0
        equity_open_pnl = round(
            float(sym_current.loc[sym_current["instrument_type"] == "Equity", "unrealized_pnl"].sum()), 2
        ) if not sym_current.empty else 0.0
        options_open_pnl = round(
            float(sym_current.loc[sym_current["instrument_type"].isin(["Call", "Put"]), "unrealized_pnl"].sum()), 2
        ) if not sym_current.empty else 0.0

        # Closed legs that belong to this position (closed on or after open_start).
        # For \"open positions only\", prefer the precomputed open_start_map; if it's
        # missing, fall back to the first trade date so we anchor to the current run.
        open_key = (str(account), str(symbol))
        open_start_val = open_start_map.get(open_key) if positions_only else None
        if positions_only and open_start_val is None and not group.empty:
            open_start_val = group["trade_date"].min()

        strategies = strat_map.get((account, symbol), [])

        if not closed_legs_df.empty and open_start_val is not None:
            open_start_date = pd.to_datetime(open_start_val).date()
            # The date range (open_start_date to present) is already anchored to the
            # current position's equity session start date (from int_strategy_classification).
            # That is the correct and sufficient filter — any option that closed on or
            # after the position opened belongs to this position.  Strategy-label
            # filtering is removed because it excluded legs whose classification differed
            # slightly from the live open strategy (e.g. expired-worthless covered calls
            # inferred as Closed via option_expiry, or PMCC short legs labelled differently
            # from the open long-call anchor).
            legs = closed_legs_df[
                (closed_legs_df["account"] == account)
                & (closed_legs_df["symbol"] == symbol)
                & (closed_legs_df["close_date"] >= open_start_date)
            ]
            closed_legs_list = legs.sort_values("close_date").to_dict(orient="records")
            for r in closed_legs_list:
                r["open_date"] = str(r["open_date"]) if pd.notna(r.get("open_date")) else ""
                r["close_date"] = str(r["close_date"]) if pd.notna(r.get("close_date")) else ""
                r["total_pnl"] = round(float(r.get("total_pnl") or 0), 2)
        else:
            closed_legs_list = []

        # Closed equity legs (shares sold / called away) within this position.
        closed_equity_list = []
        if not closed_equity_df.empty and open_start_val is not None:
            open_start_date = pd.to_datetime(open_start_val).date()
            eq_legs = closed_equity_df[
                (closed_equity_df["account"] == account)
                & (closed_equity_df["symbol"] == symbol)
                & (closed_equity_df["close_date"] >= open_start_date)
            ]
            closed_equity_list = eq_legs.sort_values("close_date").to_dict(orient="records")
            for r in closed_equity_list:
                r["open_date"] = str(r["open_date"]) if pd.notna(r.get("open_date")) else ""
                r["close_date"] = str(r["close_date"]) if pd.notna(r.get("close_date")) else ""
                r["realized_pnl"] = round(float(r.get("realized_pnl") or 0), 2)

        # Total closed P&L = option legs + equity legs
        closed_options_pnl = round(sum(float(r.get("total_pnl") or 0) for r in closed_legs_list), 2)
        closed_equity_pnl = round(sum(float(r.get("realized_pnl") or 0) for r in closed_equity_list), 2)
        closed_legs_pnl = round(closed_options_pnl + closed_equity_pnl, 2)

        # Display semantics:
        # - Default view: total_return = realized (history) + unrealized (current)
        # - "Open positions only" view: show this position's closed legs + current open P&L.
        display_realized = total_realized
        display_total = round(total_realized + unrealized, 2)
        if positions_only:
            display_realized = closed_legs_pnl
            display_total = round(closed_legs_pnl + unrealized, 2)

        if not group.empty and "trade_date" in group.columns:
            num_trades = len(group)
            first_date = str(group["trade_date"].min())
            last_date = str(group["trade_date"].max())
        else:
            num_trades = 0
            first_date = ""
            last_date = ""

        sym_chart_df = all_chart_df[
            (all_chart_df["account"] == account) & (all_chart_df["symbol"] == symbol)
        ] if not all_chart_df.empty else pd.DataFrame()

        # For "Open positions only", clip the daily P&L series to the open
        # session start so the chart focuses on the live leg while still using
        # true end-of-day prices from mart_daily_pnl.
        if positions_only and open_start_val is not None and not sym_chart_df.empty and "date" in sym_chart_df.columns:
            sym_chart_df = sym_chart_df[sym_chart_df["date"] >= pd.to_datetime(open_start_val)]
            if not sym_chart_df.empty and not group.empty and "trade_date" in group.columns:
                first_date = str(
                    min(group["trade_date"].max(), sym_chart_df["date"].min())
                )

        # PERF: defer the heavy chart build. Stash the (already sliced +
        # positions_only-clipped) inputs and build ONLY the active symbol's
        # chart after the loop (see below). The navigate-mode tab strip loads
        # every other symbol's chart by re-entering this handler with
        # ?symbol=<sym>, so ``_build_chart_from_daily_pnl`` runs once per
        # VIEWED symbol instead of once per symbol on every page load.
        chart_data_list.append(
            (sym_chart_df, sym_current, closed_options_pnl, options_open_pnl)
        )

        # Trade table rows (convert dates to str for Jinja)
        trades_table = group.copy()
        trades_table["trade_date"] = trades_table["trade_date"].astype(str)
        trades_list = trades_table.to_dict(orient="records")

        # Positions table rows: combine open positions from current snapshot
        # with closed legs for this position, and add a status column.
        current_list = sym_current.to_dict(orient="records") if not sym_current.empty else []
        combined_positions = []
        # Position-level open date (for equity / fallback) — reuse open_start_val
        open_start_str = None
        if open_start_val is not None:
            try:
                open_start_str = str(pd.to_datetime(open_start_val).date())
            except Exception:
                open_start_str = None

        # Per-option open date from transaction history (sell_to_open / buy_to_open).
        # The current snapshot doesn't carry open dates, so we look up each
        # option's trade_symbol in the trade history to find its opening trade.
        option_open_date_map: dict = {}
        if not group.empty and "action" in group.columns and "trade_symbol" in group.columns:
            open_actions = {"option_sell_to_open", "option_buy_to_open"}
            opt_opens = group[
                group["action"].astype(str).str.lower().str.strip().isin(open_actions)
            ]
            for _, trade_row in opt_opens.iterrows():
                ts = str(trade_row.get("trade_symbol", "")).strip()
                td = trade_row.get("trade_date")
                if ts and td is not None:
                    td_str = str(td)
                    if ts not in option_open_date_map or td_str < option_open_date_map[ts]:
                        option_open_date_map[ts] = td_str

        for row in current_list:
            r = dict(row)
            r["status"] = "Open"
            ts = str(r.get("trade_symbol", "")).strip()
            if r.get("instrument_type") in ("Call", "Put") and ts in option_open_date_map:
                r["open_date"] = option_open_date_map[ts]
            else:
                r["open_date"] = open_start_str
            r["close_date"] = ""
            combined_positions.append(r)

        # Closed legs within the current open session always show in the
        # Positions table so you can see the full story of the live position.
        for leg in closed_legs_list:
            direction = str(leg.get("direction") or "")
            prem_recv = float(leg.get("premium_received") or 0)
            prem_paid = float(leg.get("premium_paid") or 0)
            cost_close = float(leg.get("cost_to_close") or 0)
            proceeds_close = float(leg.get("proceeds_from_close") or 0)
            if direction == "Sold":
                leg_cost = abs(cost_close)
                leg_proceeds = abs(prem_recv)
            else:
                leg_cost = abs(prem_paid)
                leg_proceeds = abs(proceeds_close)
            opt_pnl = float(leg.get("total_pnl") or 0)
            opt_return_pct = round(opt_pnl / leg_cost * 100, 2) if leg_cost else None
            combined_positions.append({
                "status": "Closed",
                "trade_symbol": leg.get("trade_symbol"),
                "description": leg.get("strategy") or "",
                "quantity": leg.get("quantity"),
                "current_price": None,
                "market_value": round(leg_proceeds, 2) if leg_proceeds else None,
                "cost_basis": round(leg_cost, 2) if leg_cost else None,
                "unrealized_pnl": opt_pnl,
                "unrealized_pnl_pct": opt_return_pct,
                "open_date": leg.get("open_date") or "",
                "close_date": leg.get("close_date") or "",
            })

        # Closed equity legs (shares sold / called away).
        for leg in closed_equity_list:
            eq_proceeds = float(leg.get("sell_proceeds") or 0)
            eq_cost = float(leg.get("cost_basis") or 0)
            eq_pnl = float(leg.get("realized_pnl") or 0)
            eq_return_pct = round(eq_pnl / eq_cost * 100, 2) if eq_cost else None
            combined_positions.append({
                "status": "Closed",
                "trade_symbol": leg.get("trade_symbol") or symbol,
                "description": leg.get("description") or "Equity Sold",
                "quantity": leg.get("quantity"),
                "current_price": leg.get("sale_price_per_share"),
                "market_value": round(eq_proceeds, 2) if eq_proceeds else None,
                "cost_basis": round(eq_cost, 2) if eq_cost else None,
                "unrealized_pnl": eq_pnl,
                "unrealized_pnl_pct": eq_return_pct,
                "open_date": leg.get("open_date") or "",
                "close_date": leg.get("close_date") or "",
            })

        # Quick story stats for this symbol/position (across option + equity legs)
        all_closed_for_stats = [
            *closed_legs_list,
            *[{
                "trade_symbol": r.get("trade_symbol") or symbol,
                "strategy": r.get("description") or "Equity Sold",
                "close_date": r.get("close_date") or "",
                "total_pnl": r.get("realized_pnl", 0),
            } for r in closed_equity_list],
        ]
        best_leg = None
        worst_leg = None
        if all_closed_for_stats:
            best_leg = max(all_closed_for_stats, key=lambda r: r.get("total_pnl", 0))
            worst_leg = min(all_closed_for_stats, key=lambda r: r.get("total_pnl", 0))

        open_start_val = open_start_map.get((str(account), str(symbol)))
        days_in_position = None
        if open_start_val is not None:
            try:
                days_in_position = (date.today() - pd.to_datetime(open_start_val).date()).days
            except Exception:
                days_in_position = None

        open_legs_count = sum(1 for r in combined_positions if r.get("status") == "Open")
        closed_legs_count = sum(1 for r in combined_positions if r.get("status") == "Closed")

        symbol_data.append({
            "account": account,
            "symbol": symbol,
            "total_realized": display_realized,
            "unrealized": unrealized,
            "total_return": display_total,
            "num_trades": num_trades,
            "first_date": first_date,
            "last_date": last_date,
            "strategies": strategies,
            "trades": trades_list,
            "current_positions": combined_positions,
            "story_days_in_position": days_in_position,
            "story_open_legs": open_legs_count,
            "story_closed_legs": closed_legs_count,
            "story_best_leg": best_leg,
            "story_worst_leg": worst_leg,
            "_chart_idx": len(chart_data_list) - 1,
        })

    # Sort by total return descending.
    symbol_data.sort(key=lambda x: x["total_return"], reverse=True)

    # Resolve the active symbol for the tab strip. Honor ?symbol= when it
    # matches a tab (cheap defense against stale bookmarks); otherwise fall
    # back to the top-of-sort row (current "P&L desc" default).
    requested_symbol = (request.args.get("symbol") or "").strip().upper()
    available_symbols = {str(s.get("symbol") or "").upper() for s in symbol_data}
    active_symbol = (
        requested_symbol
        if requested_symbol and requested_symbol in available_symbols
        else (symbol_data[0]["symbol"] if symbol_data else "")
    )

    # PERF: build ONLY the active symbol's chart. Non-active panes ship a null
    # placeholder; the navigate-mode tab strip reloads with ?symbol=<sym>,
    # re-entering this handler to build THAT symbol's chart on demand. This
    # turns "build every symbol's chart on every load" (the ~5s cold-load
    # hotspot for heavy accounts) into one build per viewed symbol. Same
    # single builder (_finish_symbol_chart) for eager + on-demand, so the
    # chart can never differ between the two paths.
    charts = [None] * len(symbol_data)
    for i, item in enumerate(symbol_data):
        if str(item.get("symbol") or "").upper() == active_symbol:
            sci = chart_data_list[item["_chart_idx"]]
            charts[i] = _finish_symbol_chart(
                sci[0], sci[1], positions_only, sci[2], sci[3]
            )
            break
    for item in symbol_data:
        del item["_chart_idx"]

    # Navigate-mode tab strip: tabs are real <a> links back to this handler
    # with the symbol + current filters preserved. hrefs are built as
    # ``{base}{symbol}{suffix}`` by _symbol_tabstrip.html.
    tab_href_suffix = ""
    if selected_account:
        tab_href_suffix += "&account=" + quote_plus(selected_account)
    if open_only:
        tab_href_suffix += "&open_only=1"
    if positions_only:
        tab_href_suffix += "&positions_only=1"
    tab_href_base = url_for("symbols_detail") + "?symbol="

    return render_template(
        "symbols.html",
        title="Daily P&L",
        symbol_data=symbol_data,
        # `tabs` is the same list of dicts; the partial reads
        # {symbol, account, total_return, num_trades, story_open_legs, strategies}
        tabs=symbol_data,
        active_symbol=active_symbol,
        mode="navigate",
        tab_href_base=tab_href_base,
        tab_href_suffix=tab_href_suffix,
        chart_data_json=json.dumps(charts),
        accounts=accounts,
        selected_account=selected_account,
        open_only=open_only,
        positions_only=positions_only,
        linked_brokerage_accounts=(user_accounts or []),
        viewer_is_admin=is_admin(current_user.username),
    )


