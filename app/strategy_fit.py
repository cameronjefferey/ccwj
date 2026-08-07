"""Strategy fit page (/strategy-fit) — strategy x sector cross-tab.

Extracted verbatim from app/routes.py (routes.py refactor, Aug 2026).
Endpoint name unchanged (`strategy_fit`). The AI-insights sibling routes
live in app/strategy_fit_insights.py.
"""

import json
import pandas as pd
from flask import render_template, request
from flask_login import login_required, current_user

from app import app
from app.bigquery_client import get_bigquery_client
from app.llm import llm_available as _llm_available
from app.models import get_strategy_fit_insight_for_user
from app.tenant_scope import (
    filter_df_by_tenant_ids as _filter_df_by_tenant_ids,
    tenant_sql_and as _tenant_sql_and,
)
from app.routes import (
    _bq_parallel,
    _df_normalize_account_column,
    _redirect_if_no_accounts,
    _tenants_for_scope,
    _user_account_list,
)


# ======================================================================
# Strategy fit  (/strategy-fit)
# ======================================================================
#
# Cross-tab of strategy x sector (or strategy x subsector when drilled into
# a single sector) so users can see "what strategies work best in what
# kinds of companies?". Same tenancy guarantees as /sectors — query is
# scoped by _account_sql_and AND the DataFrame is _filter_df_by_accounts'd
# before any aggregation.
# ----------------------------------------------------------------------

STRATEGY_FIT_QUERY = """
    SELECT
        account,
        symbol,
        strategy,
        status,
        total_pnl,
        realized_pnl,
        unrealized_pnl,
        total_return,
        num_individual_trades,
        num_winners,
        num_losers,
        sector,
        subsector
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1
    {tenant_filter}
"""

# Per-option-contract grain for the DTE / Moneyness slices. Shaped so the
# matrix builder can consume it identically to the positions_summary path:
# realized = closed contracts, unrealized = open contracts; winners/losers
# only counted on closed contracts so win-rate semantics match the rest of
# the app. underlying_symbol is exposed as `symbol` to keep the per-cell
# symbol drill-down code path uniform.
STRATEGY_FIT_OPTIONS_QUERY = """
    SELECT
        account,
        UPPER(TRIM(underlying_symbol)) AS symbol,
        COALESCE(strategy, 'Other Option') AS strategy,
        status,
        dte_bucket,
        moneyness_at_open,
        total_pnl,
        CASE WHEN status = 'Closed' THEN total_pnl ELSE 0 END AS realized_pnl,
        CASE WHEN status = 'Open'   THEN total_pnl ELSE 0 END AS unrealized_pnl,
        num_trades AS num_individual_trades,
        CASE WHEN status = 'Closed' AND total_pnl >  0 THEN 1 ELSE 0 END AS num_winners,
        CASE WHEN status = 'Closed' AND total_pnl <= 0 THEN 1 ELSE 0 END AS num_losers
    FROM `ccwj-dbt.analytics.int_option_trade_kinds`
    WHERE 1=1
    {tenant_filter}
"""

# Fixed display order for non-categorical buckets so the dimension reads
# left-to-right naturally regardless of P&L. Anything not listed here
# (e.g. an unexpected bucket value) falls through and is appended after,
# sorted by total P&L desc, by the matrix builder.
DIM_FIXED_COL_ORDER = {
    "dte":        ["0-7 DTE", "8-30 DTE", "31-60 DTE", "61-90 DTE", "91+ DTE", "Unknown"],
    "moneyness":  ["ITM", "ATM", "OTM", "Unknown"],
}

# Map dim -> (column field in DataFrame, human label for headers/lede).
DIM_META = {
    "sector":     ("sector",            "Sector",     "sectors"),
    "subsector":  ("subsector",         "Subsector",  "subsectors"),
    "dte":        ("dte_bucket",        "DTE",        "DTE buckets"),
    "moneyness":  ("moneyness_at_open", "Moneyness",  "moneyness buckets"),
}


def _build_strategy_fit_matrix(
    df,
    *,
    col_field: str,
    col_order_override: list | None = None,
    equity_strategies: list | None = None,
):
    """Aggregate a normalized trade DataFrame into the dict of template
    variables that strategy_fit.html consumes (cells, row/col totals,
    sweet/soft callouts, baselines, color scales).

    Pure aggregation — no I/O, no tenancy logic. The caller is responsible
    for scoping `df` to the user's accounts (SQL `account_filter` AND
    `_filter_df_by_tenant_ids(df, tenant_ids)`) BEFORE handing it in.

    Required columns on `df`:
        account, symbol, strategy, <col_field>,
        total_pnl, realized_pnl, unrealized_pnl,
        num_individual_trades, num_winners, num_losers

    Args:
        col_field:           name of the column that becomes the matrix
                             columns (e.g. "sector", "dte_bucket").
        col_order_override:  fixed left-to-right column order (e.g. for
                             DTE buckets). Unknown bucket values that
                             show up in the data but aren't in the
                             override are appended after, P&L-sorted.
        equity_strategies:   strategies that have no rows in `df` (e.g.
                             equity-only Buy and Hold on the DTE slice)
                             but should still appear as N/A rows so the
                             user can see why nothing's there.
    """
    empty = {
        "row_labels": [],
        "col_labels": [],
        "cells": {},
        "cell_symbols_map": {},
        "row_totals": {},
        "col_totals": {},
        "grand_total": None,
        "max_abs_pnl": 1.0,
        "max_abs_expectancy": 1.0,
        "max_abs_edge": 1.0,
        "baseline_expectancy": 0.0,
        "baseline_win_rate": 0.0,
        "sweet_spots": [],
        "soft_spots": [],
        "equity_strategies": sorted(equity_strategies or []),
    }
    if df is None or df.empty:
        # Even with no cell data we still want equity-N/A rows visible so
        # the user sees the dimension is meaningful but doesn't apply.
        if equity_strategies:
            empty["row_labels"] = sorted(equity_strategies)
        return empty

    cell_agg = (
        df.groupby(["strategy", col_field], dropna=False)
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
    closed = cell_agg["num_winners"] + cell_agg["num_losers"]
    cell_agg["win_rate"] = cell_agg["num_winners"] / closed.replace(0, pd.NA)
    cell_agg["win_rate"] = cell_agg["win_rate"].fillna(0)

    # Expectancy = avg P&L per trade. The single most decision-relevant metric
    # because it normalizes for volume — "per trade I take, am I making money?"
    cell_agg["expectancy"] = cell_agg["total_pnl"] / cell_agg["num_trades"].replace(0, pd.NA)
    cell_agg["expectancy"] = cell_agg["expectancy"].fillna(0)

    overall_total_pnl = float(df["total_pnl"].sum())
    overall_trades = int(df["num_individual_trades"].sum())
    overall_winners = int(df["num_winners"].sum())
    overall_losers = int(df["num_losers"].sum())
    overall_closed = overall_winners + overall_losers
    baseline_expectancy = (overall_total_pnl / overall_trades) if overall_trades else 0.0
    baseline_win_rate = (overall_winners / overall_closed) if overall_closed else 0.0
    cell_agg["edge_expectancy"] = cell_agg["expectancy"] - baseline_expectancy
    cell_agg["edge_win_rate"] = cell_agg["win_rate"] - baseline_win_rate

    # Row order: best-performing strategies on top.
    row_order = (
        cell_agg.groupby("strategy")["total_pnl"].sum().sort_values(ascending=False)
        .index.tolist()
    )
    # Equity-only strategies (e.g. Buy and Hold on a DTE slice) trail the
    # data rows so the matrix still shows "you traded these too, just not
    # in this dimension." Sorted alphabetically for stable ordering.
    extra_equity = sorted(
        s for s in (equity_strategies or []) if s not in set(row_order)
    )
    row_order = list(row_order) + extra_equity

    # Column order: fixed where the dimension is categorical (DTE,
    # moneyness, market cap), P&L-sorted otherwise.
    if col_order_override is not None:
        present_cols = set(cell_agg[col_field].dropna().unique().tolist())
        col_order = [c for c in col_order_override if c in present_cols]
        # Anything new the data shows that we didn't anticipate — append
        # P&L-sorted so we don't silently drop columns.
        leftover = (
            cell_agg[~cell_agg[col_field].isin(col_order)]
            .groupby(col_field)["total_pnl"].sum().sort_values(ascending=False)
            .index.tolist()
        )
        col_order = col_order + [c for c in leftover if c not in col_order]
    else:
        col_order = (
            cell_agg.groupby(col_field)["total_pnl"].sum().sort_values(ascending=False)
            .index.tolist()
        )

    cells: dict = {}
    for r in cell_agg.to_dict(orient="records"):
        cells.setdefault(r["strategy"], {})[r[col_field]] = r

    # Per-cell symbol breakdown (top 5 by P&L) — the drill-panel uses this
    # so users can answer "what symbols are carrying this cell?" without
    # leaving the page.
    cell_sym_agg = (
        df.groupby(["strategy", col_field, "symbol"], dropna=False)
        .agg(
            total_pnl=("total_pnl", "sum"),
            num_trades=("num_individual_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
        )
        .reset_index()
        .sort_values("total_pnl", ascending=False)
    )
    cell_symbols_map: dict = {}
    for _, r in cell_sym_agg.iterrows():
        key = f"{r['strategy']}||{r[col_field]}"
        cell_symbols_map.setdefault(key, []).append({
            "symbol": str(r["symbol"]),
            "total_pnl": float(r["total_pnl"]),
            "num_trades": int(r["num_trades"]),
            "num_winners": int(r["num_winners"]),
            "num_losers": int(r["num_losers"]),
        })
    cell_symbols_map = {k: v[:5] for k, v in cell_symbols_map.items()}

    row_totals_agg = (
        cell_agg.groupby("strategy")
        .agg(
            total_pnl=("total_pnl", "sum"),
            num_trades=("num_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
            num_symbols=("num_symbols", "sum"),
        )
        .reset_index()
    )
    rclosed = row_totals_agg["num_winners"] + row_totals_agg["num_losers"]
    row_totals_agg["win_rate"] = (row_totals_agg["num_winners"] / rclosed.replace(0, pd.NA)).fillna(0)
    row_totals = {r["strategy"]: r for r in row_totals_agg.to_dict(orient="records")}

    col_totals_agg = (
        cell_agg.groupby(col_field)
        .agg(
            total_pnl=("total_pnl", "sum"),
            num_trades=("num_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
            num_symbols=("num_symbols", "sum"),
        )
        .reset_index()
    )
    cclosed = col_totals_agg["num_winners"] + col_totals_agg["num_losers"]
    col_totals_agg["win_rate"] = (col_totals_agg["num_winners"] / cclosed.replace(0, pd.NA)).fillna(0)
    col_totals = {r[col_field]: r for r in col_totals_agg.to_dict(orient="records")}

    grand = {
        "total_pnl": float(cell_agg["total_pnl"].sum()),
        "num_trades": int(cell_agg["num_trades"].sum()),
        "num_winners": int(cell_agg["num_winners"].sum()),
        "num_losers": int(cell_agg["num_losers"].sum()),
        "expectancy": baseline_expectancy,
        "win_rate": baseline_win_rate,
    }

    records = cell_agg.to_dict(orient="records")
    abs_pnls = [abs(c["total_pnl"]) for c in records if c["total_pnl"]]
    abs_exps = [abs(c["expectancy"]) for c in records if c["expectancy"]]
    abs_edges = [abs(c["edge_expectancy"]) for c in records if c["edge_expectancy"]]
    max_abs_pnl = max(abs_pnls) if abs_pnls else 1.0
    max_abs_expectancy = max(abs_exps) if abs_exps else 1.0
    max_abs_edge = max(abs_edges) if abs_edges else 1.0

    # Sample-size and win-rate guarded callouts so we don't celebrate a
    # 1-trade fluke or a coin-flip strategy that lucked into R:R. Cells
    # whose column value is "Unknown" are excluded from the narrative
    # surface (sweet/soft callouts) — naming "Unknown" as edge isn't
    # actionable. The cell stays in the matrix and the user can toggle
    # the Unknown column on/off; we just don't editorialize about it.
    MIN_TRADES_FOR_CALLOUT = 5
    qualified = cell_agg[
        (cell_agg["num_trades"] >= MIN_TRADES_FOR_CALLOUT)
        & (cell_agg[col_field].astype(str) != "Unknown")
    ].copy()

    sweet_spots: list = []
    soft_spots: list = []
    if not qualified.empty:
        sweet_df = qualified[
            (qualified["expectancy"] > 0) & (qualified["win_rate"] >= 0.45)
        ].sort_values("expectancy", ascending=False).head(3)
        soft_df = qualified[qualified["expectancy"] < 0].sort_values(
            "expectancy", ascending=True
        ).head(2)
        sweet_spots = sweet_df.to_dict(orient="records")
        soft_spots = soft_df.to_dict(orient="records")

    return {
        "row_labels": row_order,
        "col_labels": col_order,
        "cells": cells,
        "cell_symbols_map": cell_symbols_map,
        "row_totals": row_totals,
        "col_totals": col_totals,
        "grand_total": grand,
        "max_abs_pnl": max_abs_pnl,
        "max_abs_expectancy": max_abs_expectancy,
        "max_abs_edge": max_abs_edge,
        "baseline_expectancy": baseline_expectancy,
        "baseline_win_rate": baseline_win_rate,
        "sweet_spots": sweet_spots,
        "soft_spots": soft_spots,
        "equity_strategies": sorted(equity_strategies or []),
    }


def _strategy_fit_insight_context(selected_account: str) -> dict:
    """Pull the cached AI strategy-fit insight for the current user/account
    scope and convert its markdown to HTML for the template.

    Returns a small dict that's safe to **-unpack into render_template()
    in all code paths (success, empty, error)."""
    ctx = {
        "ai_summary": None,
        "ai_full_html": None,
        "ai_generated_at": None,
        "ai_enabled": app.config.get("INSIGHTS_ENABLED", True),
        "ai_available": _llm_available(),
    }
    if not ctx["ai_enabled"]:
        return ctx
    try:
        cached = get_strategy_fit_insight_for_user(
            current_user.id, tenant_filter=selected_account or ""
        )
    except Exception:
        cached = None
    if cached:
        from app.insights import _md_to_html
        ctx["ai_summary"] = cached.get("summary")
        ctx["ai_full_html"] = _md_to_html(cached.get("full_analysis") or "")
        ctx["ai_generated_at"] = cached.get("generated_at")
    return ctx


def _strategy_fit_render_payload(
    *,
    matrix: dict,
    dim: str,
    drill_sector: str,
    accounts: list,
    selected_account: str,
    insight_ctx: dict,
    error: str | None = None,
) -> dict:
    """Compose the kwargs to render strategy_fit.html. Centralized so the
    error/empty/data paths share one shape and can't drift."""
    col_field, dim_label, dim_label_plural = DIM_META.get(
        dim, DIM_META["sector"]
    )
    # AI insight payload was built for sector/subsector — null it out on
    # other dims so the template's "AI Insight" card hides cleanly.
    if dim not in ("sector", "subsector"):
        insight_ctx = {
            **insight_ctx,
            "ai_summary": None,
            "ai_full_html": None,
            "ai_generated_at": None,
        }
    return dict(
        error=error,
        row_labels=matrix.get("row_labels", []),
        col_labels=matrix.get("col_labels", []),
        cells=matrix.get("cells", {}),
        cell_symbols_json=json.dumps(matrix.get("cell_symbols_map", {})),
        row_totals=matrix.get("row_totals", {}),
        col_totals=matrix.get("col_totals", {}),
        grand_total=matrix.get("grand_total"),
        max_abs_pnl=matrix.get("max_abs_pnl", 1.0),
        max_abs_expectancy=matrix.get("max_abs_expectancy", 1.0),
        max_abs_edge=matrix.get("max_abs_edge", 1.0),
        baseline_expectancy=matrix.get("baseline_expectancy", 0.0),
        baseline_win_rate=matrix.get("baseline_win_rate", 0.0),
        sweet_spots=matrix.get("sweet_spots", []),
        soft_spots=matrix.get("soft_spots", []),
        equity_strategies=matrix.get("equity_strategies", []),
        col_field=col_field,
        dim=dim,
        # mode is preserved for backward-compat in the template (it used
        # to be sector|subsector only); now mirrors dim 1:1.
        mode=dim,
        dim_label=dim_label,
        dim_label_plural=dim_label_plural,
        drill_sector=drill_sector,
        accounts=accounts,
        selected_account=selected_account,
        **insight_ctx,
    )


@app.route("/strategy-fit")
@login_required
def strategy_fit():
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

    drill_sector = request.args.get("sector", "")  # implies subsector mode

    # Resolve the column dimension. Drilling into a sector wins (for
    # backward URL compat) and forces subsector mode. Otherwise read ?dim=
    # and validate against the supported set. 'industry' is the pre-rename
    # alias for 'subsector' — accept it so old bookmarks keep working.
    requested_dim = (request.args.get("dim", "") or "").strip().lower()
    if requested_dim == "industry":
        requested_dim = "subsector"
    if drill_sector:
        dim = "subsector"
    elif requested_dim in ("dte", "moneyness", "subsector", "sector"):
        dim = requested_dim
    else:
        dim = "sector"

    insight_ctx = _strategy_fit_insight_context(selected_account)

    # Fan out the queries we need. positions_summary is always needed —
    # for sector/subsector it's the data source, and for dte/moneyness
    # it's where we discover the equity-only strategy set so the matrix
    # can show "N/A — equity" rows.
    queries = {"summary": STRATEGY_FIT_QUERY.format(tenant_filter=tenant_filter)}
    if dim in ("dte", "moneyness"):
        queries["options"] = STRATEGY_FIT_OPTIONS_QUERY.format(tenant_filter=tenant_filter)

    try:
        dfs = _bq_parallel(client, queries)
    except Exception as exc:
        # Don't swallow this silently — a schema drift here once shipped a red
        # banner to every Strategy Fit visitor for hours before anyone noticed.
        app.logger.exception("strategy_fit: BigQuery query failed: %s", exc)
        return render_template(
            "strategy_fit.html",
            **_strategy_fit_render_payload(
                matrix={},
                dim=dim,
                drill_sector=drill_sector,
                accounts=[],
                selected_account="",
                insight_ctx=insight_ctx,
                error=str(exc),
            ),
        )

    summary_df = _df_normalize_account_column(dfs["summary"])
    summary_df = _filter_df_by_tenant_ids(summary_df, tenant_ids)
    # tenant scope already narrowed to the selected account's tenant_id

    for col in ("total_pnl", "realized_pnl", "unrealized_pnl", "total_return",
                "num_individual_trades", "num_winners", "num_losers"):
        if col in summary_df.columns:
            summary_df.loc[:, col] = pd.to_numeric(summary_df[col], errors="coerce").fillna(0)
    for col in ("sector", "subsector", "strategy"):
        if col in summary_df.columns:
            summary_df.loc[:, col] = (
                summary_df[col].fillna("Unknown").astype(str).str.strip().replace("", "Unknown")
            )

    accounts_for_filter = (
        sorted(user_accounts)
        if user_accounts
        else (sorted(summary_df["account"].dropna().unique().tolist())
              if not summary_df.empty else [])
    )

    if summary_df.empty:
        return render_template(
            "strategy_fit.html",
            **_strategy_fit_render_payload(
                matrix={},
                dim=dim,
                drill_sector=drill_sector,
                accounts=accounts_for_filter,
                selected_account=selected_account,
                insight_ctx=insight_ctx,
            ),
        )

    if dim in ("dte", "moneyness"):
        options_df = _df_normalize_account_column(dfs["options"])
        # Tenancy belt-and-braces: re-filter the per-contract frame by
        # the user's accounts BEFORE any grouping so a SQL regression
        # can't leak another tenant's contracts into the matrix.
        options_df = _filter_df_by_tenant_ids(options_df, tenant_ids)
        # tenant scope already narrowed to the selected account's tenant_id

        for col in ("total_pnl", "realized_pnl", "unrealized_pnl",
                    "num_individual_trades", "num_winners", "num_losers"):
            if col in options_df.columns:
                options_df.loc[:, col] = pd.to_numeric(options_df[col], errors="coerce").fillna(0)
        for col in ("strategy", "dte_bucket", "moneyness_at_open", "symbol"):
            if col in options_df.columns:
                options_df.loc[:, col] = (
                    options_df[col].fillna("Unknown").astype(str).str.strip().replace("", "Unknown")
                )

        col_field = DIM_META[dim][0]
        # Equity-only strategies = strategies the user has in
        # positions_summary but that have NO option contracts. We mark
        # these as full N/A rows in the template so users see why their
        # equity strategy doesn't appear in the data area.
        all_strategies = set(summary_df["strategy"].dropna().astype(str).unique().tolist())
        option_strategies = set(
            options_df["strategy"].dropna().astype(str).unique().tolist()
        ) if not options_df.empty else set()
        equity_strategies = sorted(all_strategies - option_strategies)

        matrix = _build_strategy_fit_matrix(
            options_df,
            col_field=col_field,
            col_order_override=DIM_FIXED_COL_ORDER[dim],
            equity_strategies=equity_strategies,
        )
    else:
        df = summary_df
        if dim == "subsector":
            # Drill: filter to one sector, columns become subsectors.
            df = df[df["sector"] == drill_sector]
            col_field = "subsector"
            col_order_override = None
        else:
            col_field = "sector"
            col_order_override = None

        matrix = _build_strategy_fit_matrix(
            df,
            col_field=col_field,
            col_order_override=col_order_override,
        )

    return render_template(
        "strategy_fit.html",
        **_strategy_fit_render_payload(
            matrix=matrix,
            dim=dim,
            drill_sector=drill_sector,
            accounts=accounts_for_filter,
            selected_account=selected_account,
            insight_ctx=insight_ctx,
        ),
    )



