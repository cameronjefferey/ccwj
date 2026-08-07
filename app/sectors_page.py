"""Sectors page (/sectors, legacy /industries) — sector/subsector rollup.

Extracted verbatim from app/routes.py (routes.py refactor, Aug 2026).
Endpoint names unchanged (`sectors`, `industries_legacy`).
"""

import pandas as pd
from flask import render_template, request, redirect, url_for
from flask_login import login_required

from app import app
from app.bigquery_client import get_bigquery_client
from app.tenant_scope import (
    filter_df_by_tenant_ids as _filter_df_by_tenant_ids,
    tenant_sql_and as _tenant_sql_and,
)
from app.routes import (
    _df_normalize_account_column,
    _redirect_if_no_accounts,
    _tenants_for_scope,
    _user_account_list,
)


# ======================================================================
# Sectors  (/sectors)
# ======================================================================
#
# Sector / subsector rollup of positions_summary, scoped to the logged-in
# user's accounts. Powers the "Sectors" page in the Portfolio nav.
# (Originally /industries — renamed to standardize on the finance term
# "sector → subsector" hierarchy. The /industries URL still resolves via
# a redirect for old bookmarks.)
# Tenancy: positions_summary is multi-tenant -> we MUST scope the SQL with
# _account_sql_and AND filter the resulting DataFrame with
# _filter_df_by_accounts before aggregating, per
# .cursor/rules/bigquery-tenant-isolation.mdc.
# ----------------------------------------------------------------------

SECTORS_QUERY = """
    SELECT
        account,
        symbol,
        strategy,
        status,
        total_pnl,
        realized_pnl,
        unrealized_pnl,
        total_premium_received,
        total_dividend_income,
        total_return,
        num_individual_trades,
        num_winners,
        num_losers,
        sector,
        subsector,
        company_name
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1
    {tenant_filter}
"""


@app.route("/industries")
@login_required
def industries_legacy():
    """Backward-compatible redirect for the old /industries URL. The page
    moved to /sectors when we renamed industry → subsector."""
    return redirect(url_for("sectors", **request.args.to_dict(flat=True)), code=301)


@app.route("/sectors")
@login_required
def sectors():
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

    try:
        df = client.query(
            SECTORS_QUERY.format(tenant_filter=tenant_filter)
        ).to_dataframe()
    except Exception as exc:
        return render_template(
            "sectors.html",
            error=str(exc),
            sectors=[],
            sector_rows=[],
            subsector_rows=[],
            subsectors_by_sector={},
            unknown_count=0,
            kpis={},
            accounts=[],
            selected_account="",
        )

    df = _df_normalize_account_column(df)
    df = _filter_df_by_tenant_ids(df, tenant_ids)
    # tenant scope already narrowed to the selected account's tenant_id

    for col in (
        "total_pnl", "realized_pnl", "unrealized_pnl",
        "total_premium_received", "total_dividend_income", "total_return",
        "num_individual_trades", "num_winners", "num_losers",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    for col in ("sector", "subsector"):
        if col in df.columns:
            df[col] = df[col].fillna("Unknown").astype(str).str.strip().replace("", "Unknown")

    accounts_for_filter = (
        sorted(user_accounts)
        if user_accounts
        else (sorted(df["account"].dropna().unique().tolist()) if not df.empty else [])
    )

    if df.empty:
        return render_template(
            "sectors.html",
            error=None,
            sectors=[],
            sector_rows=[],
            subsector_rows=[],
            subsectors_by_sector={},
            unknown_count=0,
            kpis={
                "total_pnl": 0.0, "realized_pnl": 0.0, "unrealized_pnl": 0.0,
                "num_subsectors": 0, "num_symbols": 0, "num_trades": 0,
                "win_rate": 0.0,
            },
            accounts=accounts_for_filter,
            selected_account=selected_account,
        )

    overall_winners = int(df["num_winners"].sum())
    overall_losers = int(df["num_losers"].sum())
    overall_closed = overall_winners + overall_losers
    kpis = {
        "total_pnl": float(df["total_pnl"].sum()),
        "realized_pnl": float(df["realized_pnl"].sum()),
        "unrealized_pnl": float(df["unrealized_pnl"].sum()),
        "num_subsectors": int(df["subsector"].nunique()),
        "num_symbols": int(df.groupby(["account", "symbol"]).ngroups),
        "num_trades": int(df["num_individual_trades"].sum()),
        "win_rate": (overall_winners / overall_closed) if overall_closed else 0.0,
    }

    # Per-subsector rollup: collapse strategy granularity, aggregate over the
    # user's accounts. One row per (sector, subsector).
    subsector_agg = (
        df.groupby(["sector", "subsector"], dropna=False)
        .agg(
            total_pnl=("total_pnl", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            unrealized_pnl=("unrealized_pnl", "sum"),
            premium_received=("total_premium_received", "sum"),
            dividend_income=("total_dividend_income", "sum"),
            total_return=("total_return", "sum"),
            num_trades=("num_individual_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
            num_symbols=("symbol", "nunique"),
        )
        .reset_index()
    )
    closed = subsector_agg["num_winners"] + subsector_agg["num_losers"]
    subsector_agg["win_rate"] = subsector_agg["num_winners"] / closed.replace(0, pd.NA)
    subsector_agg["win_rate"] = subsector_agg["win_rate"].fillna(0)

    # Top symbol per (sector, subsector) by total_return — useful "what's
    # actually carrying this subsector?" tooltip on the card.
    sym_in_sub = (
        df.groupby(["sector", "subsector", "symbol"], dropna=False)["total_return"]
        .sum()
        .reset_index()
    )
    if not sym_in_sub.empty:
        sym_in_sub = sym_in_sub.sort_values(
            ["sector", "subsector", "total_return"], ascending=[True, True, False]
        )
        top_symbol_map = (
            sym_in_sub.groupby(["sector", "subsector"])
            .first()
            .reset_index()[["sector", "subsector", "symbol", "total_return"]]
            .rename(columns={"symbol": "top_symbol", "total_return": "top_symbol_return"})
        )
        subsector_agg = subsector_agg.merge(
            top_symbol_map, on=["sector", "subsector"], how="left"
        )
    else:
        subsector_agg["top_symbol"] = ""
        subsector_agg["top_symbol_return"] = 0.0

    subsector_agg = subsector_agg.sort_values("total_return", ascending=False)
    subsector_rows = subsector_agg.to_dict(orient="records")

    # Sector rollup — this is now the primary view on the page, so it carries
    # the same shape as subsector_rows: realized / unrealized / premium /
    # dividends / total_return so the sector cards have everything at a glance.
    sector_agg = (
        df.groupby(["sector"], dropna=False)
        .agg(
            total_pnl=("total_pnl", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            unrealized_pnl=("unrealized_pnl", "sum"),
            premium_received=("total_premium_received", "sum"),
            dividend_income=("total_dividend_income", "sum"),
            total_return=("total_return", "sum"),
            num_subsectors=("subsector", "nunique"),
            num_symbols=("symbol", "nunique"),
            num_trades=("num_individual_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
        )
        .reset_index()
    )
    s_closed = sector_agg["num_winners"] + sector_agg["num_losers"]
    sector_agg["win_rate"] = sector_agg["num_winners"] / s_closed.replace(0, pd.NA)
    sector_agg["win_rate"] = sector_agg["win_rate"].fillna(0)

    # Best / worst symbol per sector — drives the "what's carrying this?" /
    # "what's dragging?" callouts on each sector card.
    sym_per_sector = (
        df.groupby(["sector", "symbol"], dropna=False)["total_return"]
        .sum()
        .reset_index()
    )
    if not sym_per_sector.empty:
        top_per_sector = (
            sym_per_sector.sort_values(["sector", "total_return"], ascending=[True, False])
            .groupby("sector").first().reset_index()
            .rename(columns={"symbol": "top_symbol", "total_return": "top_symbol_return"})
        )
        worst_per_sector = (
            sym_per_sector.sort_values(["sector", "total_return"], ascending=[True, True])
            .groupby("sector").first().reset_index()
            .rename(columns={"symbol": "worst_symbol", "total_return": "worst_symbol_return"})
        )
        sector_agg = sector_agg.merge(top_per_sector[["sector", "top_symbol", "top_symbol_return"]], on="sector", how="left")
        sector_agg = sector_agg.merge(worst_per_sector[["sector", "worst_symbol", "worst_symbol_return"]], on="sector", how="left")
    else:
        sector_agg["top_symbol"] = ""
        sector_agg["top_symbol_return"] = 0.0
        sector_agg["worst_symbol"] = ""
        sector_agg["worst_symbol_return"] = 0.0

    sector_agg = sector_agg.sort_values("total_pnl", ascending=False)
    sector_rows = sector_agg.to_dict(orient="records")
    sectors_list = sector_agg["sector"].tolist()

    # Group subsectors under their sector for the collapsible drill-down on
    # the page. Order each sector's subsectors by total_return desc.
    subsectors_by_sector: dict[str, list[dict]] = {}
    for r in subsector_rows:
        subsectors_by_sector.setdefault(r["sector"], []).append(r)
    for sec in subsectors_by_sector:
        subsectors_by_sector[sec].sort(
            key=lambda x: x.get("total_return", 0), reverse=True
        )

    unknown_count = int(
        ((df["sector"] == "Unknown") | (df["subsector"] == "Unknown"))
        .pipe(lambda s: s.groupby([df["account"], df["symbol"]]).any())
        .sum()
    )

    return render_template(
        "sectors.html",
        error=None,
        sectors=sectors_list,
        sector_rows=sector_rows,
        subsector_rows=subsector_rows,
        subsectors_by_sector=subsectors_by_sector,
        unknown_count=unknown_count,
        kpis=kpis,
        accounts=accounts_for_filter,
        selected_account=selected_account,
    )



