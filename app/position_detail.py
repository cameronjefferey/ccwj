"""Position Detail page (/position/<symbol>) + leg tag routes.

The deep-dive page — the most logic-heavy surface in the product.
Extracted verbatim from app/routes.py (routes.py refactor, Aug 2026).
Endpoint names unchanged (`position_detail`, `add_position_tag`,
`remove_position_tag`).

Key invariants live in AGENTS.md and
.cursor/rules/position-detail-orphan-tenancy-reconciliation.mdc:
hero total, Breakdown-by-Type total, and chart terminal must agree
(admin invariant card fires past $1); chart machinery itself lives in
app/pnl_charts.py.
"""

import json
import logging
import re
from datetime import datetime, date, timedelta  # noqa: F401

import pandas as pd
from flask import render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from urllib.parse import quote_plus

from app import app
from app.extensions import limiter
from app.bigquery_client import get_bigquery_client
from app.query_cache import cached_query_df, cached_payload, frame_fingerprint, timed
from app.skeleton import skeleton_page
from app.models import get_tenant_ids_for_user, is_admin
from app.utils import user_local_today
from app.tenant_scope import (
    filter_df_by_tenant_ids as _filter_df_by_tenant_ids,
    tenant_sql_and as _tenant_sql_and,
)
from app.pnl_charts import (
    CHART_DATA_ALL_QUERY,
    CHART_DATA_QUERY,
    CHART_KPI_ALIGN_TOLERANCE_DOLLARS,
    CHART_SUBSTITUTION_KPI_MARGIN,
    _addback_phantom_writeoffs_to_summary,
    _align_position_pnl_chart_with_kpi,
    _build_chart_from_daily_pnl,
    _build_chart_from_daily_pnl_partition,
    _build_option_matrices,
    _chart_data_for_json,
    _chart_data_terminal,
    _collapse_mart_daily_pnl_duplicate_grain,
    _collect_activity_candidate_dates,
    _cumulative_pnl_from_leg_closes,
    _cumulative_pnl_from_stg_trades,
    _dedupe_enriched_current_positions,
    _drop_phantom_equity_writeoffs,
    _equity_slice_for_live_chart,
    _filter_current_for_chart_partition,
    _merge_position_pnl_chart_payloads,
    _narrow_mart_daily_pnl_chart_df_to_summary_tenant,
    _snap_position_chart_terminal_to_breakdown,
    _synthetic_cumulative_pnl_for_position,
)
from app.routes import (
    _account_label_map,
    _bq_parallel,
    _df_normalize_account_column,
    _legs_df_to_sessions_list,
    _norm_account_label,
    _parse_date,
    _redirect_if_no_accounts,
    _resolve_position_leg_filter,
    _tags_for_leg_range,
    _tenant_label_map_for_user,
    _tenants_for_scope,
    _user_account_list,
    _user_tenant_list,
)

_log = logging.getLogger(__name__)

# Fills the trader placed — not broker DRIPs, cash dividends, or transfers.
_NOT_PLACED_TRADE_ACTIONS = frozenset({
    "dividend_reinvest", "dividend", "cash_transfer",
    "margin_interest", "credit_interest", "adr_fee",
})


def _count_placed_fills(df):
    """Count history rows that are trades the user placed.

    DRIP reinvestments stay in the raw log (labeled) and in share lots;
    they must not inflate the hero fill count. Cash dividends / transfers
    / interest are the same class — income or cash movement, not a trade.
    """
    if df is None or df.empty:
        return 0
    mask = pd.Series(True, index=df.index)
    if "action" in df.columns:
        mask &= ~df["action"].astype(str).isin(_NOT_PLACED_TRADE_ACTIONS)
    if "is_dividend_reinvestment" in df.columns:
        drip = df["is_dividend_reinvestment"]
        mask &= ~drip.fillna(False).astype(bool)
    return int(mask.sum())


# ======================================================================
# Position Detail  (/position/<symbol>)
# ======================================================================

POSITION_SUMMARY_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
    ORDER BY account, strategy
"""

POSITION_TRADES_QUERY = """
    SELECT
        h.account,
        h.tenant_id,
        h.underlying_symbol AS symbol,
        h.trade_date,
        -- Surface DRIPs as their own action so the Raw Transaction Log
        -- and trade-history aggregations can show "I didn't choose to
        -- buy this — Schwab reinvested my dividend" rather than a
        -- chaotic stream of tiny equity_buy fills. Detection lives
        -- in `int_drip_fills` (downstream of `stg_daily_prices` so
        -- stg_history stays out of the price-dependent build pass).
        CASE WHEN d.matched_ex_div_date IS NOT NULL
             THEN 'dividend_reinvest'
             ELSE h.action
        END AS action,
        h.action_raw,
        h.trade_symbol,
        h.instrument_type,
        h.description,
        h.quantity,
        h.price,
        h.fees,
        h.amount,
        (d.matched_ex_div_date IS NOT NULL) AS is_dividend_reinvestment
    FROM `ccwj-dbt.analytics.stg_history` h
    LEFT JOIN `ccwj-dbt.analytics.int_drip_fills` d
        ON  (d.tenant_id IS NOT DISTINCT FROM h.tenant_id)
        AND d.account            = h.account
        AND (d.user_id IS NOT DISTINCT FROM h.user_id)
        AND d.trade_date         = h.trade_date
        AND d.underlying_symbol  = h.underlying_symbol
        AND ABS(COALESCE(h.quantity, 0) - COALESCE(d.quantity, 0)) < 1e-9
    WHERE h.trade_date IS NOT NULL
      AND (
        UPPER(TRIM(COALESCE(h.underlying_symbol, ''))) = UPPER(TRIM('{symbol}'))
        OR UPPER(TRIM(SPLIT(COALESCE(h.trade_symbol, ''), ' ')[SAFE_OFFSET(0)])) = UPPER(TRIM('{symbol}'))
      )
    {tenant_filter}
    ORDER BY h.trade_date DESC
"""

POSITION_CURRENT_QUERY = """
    SELECT
        account,
        user_id,
        tenant_id,
        underlying_symbol AS symbol,
        instrument_type,
        trade_symbol,
        description,
        quantity,
        current_price,
        market_value,
        cost_basis,
        unrealized_pnl,
        unrealized_pnl_pct,
        -- option_expiry / option_strike / option_type are needed by the
        -- chart's live-today override so it can defensively drop any
        -- past-expiry option rows from open-MTM addition (the dbt layer
        -- already filters auto-closed contracts via
        -- int_enriched_current.option_contract_status, but selecting
        -- these columns also lets test fixtures and post-build readers
        -- run the same expiry mask). See _build_chart_from_daily_pnl
        -- and the OTM-at-expiry inference in int_option_contracts.
        option_expiry,
        option_strike,
        option_type,
        -- Realized wedge of a partially-closed OPEN option contract (0 for
        -- equity rows and fully-open options). Added to the Breakdown-by-Type
        -- options-realized so a partial close's booked P&L reconciles with the
        -- chart / Strategy Breakdown instead of tripping the invariant card.
        option_realized_pnl
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE UPPER(TRIM(COALESCE(underlying_symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
"""

POSITION_CLOSED_LEGS_QUERY = """
    SELECT
        sc.account,
        sc.tenant_id,
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
      AND UPPER(TRIM(COALESCE(sc.symbol, ''))) = UPPER(TRIM('{symbol}'))
    {sc_tenant_filter}
"""

POSITION_CLOSED_EQUITY_QUERY = """
    SELECT
        account,
        tenant_id,
        symbol,
        trade_symbol,
        session_id,
        open_date,
        close_date,
        quantity,
        sale_price_per_share,
        sell_proceeds,
        cost_basis,
        realized_pnl,
        description
    FROM `ccwj-dbt.analytics.int_closed_equity_legs`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
"""

POSITION_LEGS_QUERY = """
    SELECT
        tenant_id,
        account,
        user_id,
        symbol,
        leg_id,
        leg_type,
        status,
        open_date,
        last_activity_date,
        equity_pnl,
        closed_options_pnl,
        open_options_pnl,
        combined_pnl,
        options_count,
        open_options_count,
        max_quantity_held,
        num_trades,
        options_only,
        display_leg_num,
        days_held
    FROM `ccwj-dbt.analytics.int_position_legs`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
    ORDER BY account, tenant_id, display_leg_num
"""

# Distinct accounts (tenants) that have traded this symbol, scoped to the
# viewer's FULL owned tenant set — NOT the ?tenants= on/off subset. Powers
# the Position Detail account-toggle bar so a toggled-OFF account can still
# be turned back on (it must appear in the list even when it's filtered out
# of the main legs query). Isolation is still enforced (owned tenants only).
POSITION_ACCOUNTS_QUERY = """
    SELECT DISTINCT
        tenant_id,
        account
    FROM `ccwj-dbt.analytics.int_position_legs`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
"""

# Lightweight per-(account,symbol) rollup for the position-detail tab strip.
# Reads `positions_summary` (the precomputed mart) so the tab strip lists
# every symbol the user has ever traded with one trip to BQ. We deliberately
# keep the projection minimal — the strip only needs total_return for the
# pill, num_trades for the title, and a single "Open if any leg open"
# status for the dot. SCOPED with `_account_sql_and` per
# `.cursor/rules/bigquery-tenant-isolation.mdc`; the resulting frame also
# passes through `_filter_df_by_accounts` in Python before serialization.
SYMBOL_TABS_QUERY = """
    SELECT
        account,
        tenant_id,
        symbol,
        SUM(COALESCE(total_return, 0)) AS total_return,
        SUM(COALESCE(num_individual_trades, 0)) AS num_trades,
        MAX(IF(LOWER(TRIM(COALESCE(status, ''))) = 'open', 1, 0)) AS has_open_leg,
        STRING_AGG(DISTINCT strategy, '|' ORDER BY strategy) AS strategies_pipe
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE symbol IS NOT NULL
      {tenant_filter}
    GROUP BY account, tenant_id, symbol
"""

# Win/Loss matrix cells are PRE-BUCKETED in dbt (mart_option_win_matrix).
# Flask no longer loops over raw contracts to build the DTE x strike grid;
# it just reshapes these aggregated cells into the template's nested dict.
# The mart already restricts to closed contracts with a known strike
# distance (the old ``status='Closed' AND strike_distance IS NOT NULL``).
POSITION_MATRIX_QUERY = """
    SELECT
        account,
        user_id,
        tenant_id,
        underlying_symbol,
        trade_symbol,
        strategy,
        dte_label,
        dte_order,
        strike_col,
        strike_order,
        trade_count,
        wins,
        sum_pnl
    FROM `ccwj-dbt.analytics.mart_option_win_matrix`
    WHERE UPPER(TRIM(COALESCE(underlying_symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
"""

# Next-earnings date for a single symbol. Symbol-level public market data
# (yfinance via scripts/refresh_earnings_calendar.py) — no account or
# user_id column in stg_earnings_calendar, so no tenant filter is needed
# or possible here. The page itself is already tenant-scoped via the
# other position queries above; this just decorates the hero with
# "next earnings in N days" context.
# No DATE_DIFF "days until" column ON PURPOSE: CURRENT_DATE() is UTC and
# rolls to tomorrow at 5pm PT / 8pm ET, so a SQL day count is off by one
# every US evening (plus query-cache staleness). The lower bound is padded a
# day for the same reason; Python computes days_until against the user's
# profile-timezone today and hides the pill once the date has passed.
POSITION_EARNINGS_QUERY = """
    SELECT
        next_earnings_date,
        earnings_window_start,
        earnings_window_end
    FROM `ccwj-dbt.analytics.stg_earnings_calendar`
    WHERE UPPER(TRIM(symbol)) = UPPER(TRIM('{symbol}'))
      AND next_earnings_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ORDER BY next_earnings_date
    LIMIT 1
"""

def _equity_raw_trades_for_partial_close_outcome(
    trades: list,
    *,
    trade_symbol: str,
    account: str,
    session_range,
    close_milestone,
):
    """``int_closed_equity_legs`` is one mart row PER partial sell inside a chapter.
    When attaching ``raw_trades`` for drill-down, include only fills chronological
    through this row's realization date — otherwise each partial shows the SAME
    full session history (duplicate Leg 1 + duplicate buy + later sells visible
    everywhere). JEPI bought 2000 sold 1000 twice was the canonical bug."""
    ts = str(trade_symbol or "").strip()
    acct_o = str(account or "").strip()

    def _row_date(tv):
        try:
            return pd.to_datetime(tv).date()
        except Exception:
            return None

    out = []
    for t in trades or []:
        if str(t.get("instrument_type") or "") != "Equity":
            continue
        if str(t.get("trade_symbol") or "").strip() != ts:
            continue
        if acct_o and str(t.get("account") or "").strip() != acct_o:
            continue
        td = _row_date(t.get("trade_date"))
        if td is None:
            continue
        if session_range and session_range[0]:
            end = session_range[1] or date.today()
            if not (session_range[0] <= td <= end):
                continue
        cm = _row_date(close_milestone) if close_milestone is not None else None
        if cm is not None and td > cm:
            continue
        out.append(t)
    return sorted(out, key=lambda r: str(r.get("trade_date") or ""))


def _merge_position_strategy_breakdown(
    symbol: str,
    summary_df: pd.DataFrame,
    closed_legs_df: pd.DataFrame,
    closed_equity_df: pd.DataFrame,
) -> pd.DataFrame:
    """Return a strategy table that includes any (account, strategy) in closed legs/equity
    missing from positions_summary, so the breakdown matches the Position Legs / history.

    positions_summary is one row per (account, symbol, strategy); in edge cases a closed
    strategy can be absent from the mart while int_strategy_classification still has legs.
    """
    existing = set()
    if summary_df is not None and not summary_df.empty and "account" in summary_df.columns:
        for _, r in summary_df.iterrows():
            a = r.get("account")
            s = r.get("strategy")
            if a is None or (isinstance(s, float) and pd.isna(s)) or s is None:
                continue
            st = str(s).strip()
            if not st:
                continue
            existing.add((str(a).strip(), st))

    def _row_from_option_group(acct: str, strat: str, sub: pd.DataFrame) -> dict:
        total = float(sub["total_pnl"].sum()) if "total_pnl" in sub.columns else 0.0
        prem_r = float(sub["premium_received"].sum()) if "premium_received" in sub.columns else 0.0
        prem_p = float(sub["premium_paid"].sum()) if "premium_paid" in sub.columns else 0.0
        n = len(sub)
        wins = int((sub["total_pnl"] > 0).sum()) if "total_pnl" in sub.columns else 0
        losses = n - wins
        wr = wins / n if n else 0.0
        days_mean = 0.0
        if "days_in_trade" in sub.columns:
            days_mean = float(sub["days_in_trade"].fillna(0).mean() or 0.0)
        od = (
            sub["open_date"].dropna().min() if "open_date" in sub.columns else None
        )
        cd = (
            sub["close_date"].dropna().max() if "close_date" in sub.columns else None
        )
        avg_pnl = total / n if n else 0.0
        return {
            "account": acct,
            "symbol": symbol,
            "strategy": strat,
            "status": "Closed",
            "total_pnl": round(total, 2),
            "realized_pnl": round(total, 2),
            "unrealized_pnl": 0.0,
            "total_premium_received": round(prem_r, 2),
            "total_premium_paid": round(prem_p, 2),
            "num_trade_groups": n,
            "num_individual_trades": n,
            "num_winners": wins,
            "num_losers": losses,
            "win_rate": wr,
            "avg_pnl_per_trade": round(avg_pnl, 2),
            "avg_days_in_trade": round(days_mean, 1),
            "first_trade_date": od,
            "last_trade_date": cd,
            "total_dividend_income": 0.0,
            "dividend_count": 0,
            "total_return": round(total, 2),
        }

    def _row_from_equity_group(acct: str, lbl: str, sub: pd.DataFrame) -> dict:
        real = float(sub["realized_pnl"].sum()) if "realized_pnl" in sub.columns else 0.0
        n = len(sub)
        wins = int((sub["realized_pnl"] > 0).sum()) if "realized_pnl" in sub.columns else 0
        losses = n - wins
        wr = wins / n if n else 0.0
        od = sub["open_date"].dropna().min() if "open_date" in sub.columns else None
        cd = sub["close_date"].dropna().max() if "close_date" in sub.columns else None
        days_mean = 0.0
        for _, er in sub.iterrows():
            o = er.get("open_date")
            c = er.get("close_date")
            if pd.notna(o) and pd.notna(c):
                try:
                    days_mean += (pd.to_datetime(c) - pd.to_datetime(o)).days
                except Exception:
                    pass
        if n:
            days_mean = round(days_mean / n, 1)
        avg_pnl = real / n if n else 0.0
        return {
            "account": acct,
            "symbol": symbol,
            "strategy": lbl,
            "status": "Closed",
            "total_pnl": round(real, 2),
            "realized_pnl": round(real, 2),
            "unrealized_pnl": 0.0,
            "total_premium_received": 0.0,
            "total_premium_paid": 0.0,
            "num_trade_groups": n,
            "num_individual_trades": n,
            "num_winners": wins,
            "num_losers": losses,
            "win_rate": wr,
            "avg_pnl_per_trade": round(avg_pnl, 2),
            "avg_days_in_trade": days_mean,
            "first_trade_date": od,
            "last_trade_date": cd,
            "total_dividend_income": 0.0,
            "dividend_count": 0,
            "total_return": round(real, 2),
        }

    # Equity bucket: positions_summary's "Buy and Hold" row gets reclassified
    # to "Dividend" when dividend income > trade gain — and Coinbase / crypto
    # holdings come through as "Crypto". All three occupy the same
    # equity-strategy slot in the breakdown — only one of them can ever exist
    # for a given (account, symbol). Track which accounts already have one
    # so we don't synthesize a duplicate Buy-and-Hold row alongside a real
    # Dividend / Crypto row from the mart.
    EQUITY_BUCKET = ("Buy and Hold", "Dividend", "Crypto")
    equity_covered_accounts: set[str] = set()
    for acct_existing, strat_existing in existing:
        if strat_existing in EQUITY_BUCKET:
            equity_covered_accounts.add(acct_existing)

    extra: list[dict] = []

    if closed_legs_df is not None and not closed_legs_df.empty and "strategy" in closed_legs_df.columns:
        g = closed_legs_df.copy()
        g = g[g["strategy"].notna() & (g["strategy"].astype(str).str.strip() != "")]
        for (acct, strat), sub in g.groupby(
            [g["account"].astype(str), g["strategy"].astype(str)]
        ):
            acct, strat = str(acct).strip(), str(strat).strip()
            if (acct, strat) in existing:
                continue
            extra.append(_row_from_option_group(acct, strat, sub))
            existing.add((acct, strat))

    # NOTE: `closed_equity_df` is `int_closed_equity_legs`, whose `description`
    # column is the LEG TYPE ("Equity Sold" / "Cost Written Off"), NOT a strategy.
    # Promoting the description into the strategy breakdown was creating spurious
    # rows: a single Buy-and-Hold session would render as three rows in the
    # Strategy Breakdown table (Buy and Hold + Equity Sold + Cost Written Off),
    # each one looking like a separate strategy outcome. The Position Legs section
    # already surfaces individual sells/transfers — the strategy breakdown should
    # stick to one row per real (account, strategy) classification.
    #
    # The original intent was: if positions_summary lacks a row for a closed
    # equity session that is recorded in int_closed_equity_legs, synthesize a
    # "Buy and Hold"-shaped row so the table isn't blank. We preserve that
    # narrow fallback by labeling synthetic equity rows "Buy and Hold" rather
    # than borrowing the leg description.
    if closed_equity_df is not None and not closed_equity_df.empty and "account" in closed_equity_df.columns:
        g = closed_equity_df.copy()
        for acct, sub in g.groupby(g["account"].astype(str)):
            acct = str(acct).strip()
            # Skip if positions_summary already has any equity-bucket row for
            # this account (Buy and Hold or its Dividend reclassification).
            # Otherwise we'd render two rows for the same closed equity session
            # — one "Dividend" with $16k divs, one synthetic "Buy and Hold"
            # with $0 divs — and they'd look like separate strategies.
            if acct in equity_covered_accounts:
                continue
            extra.append(_row_from_equity_group(acct, "Buy and Hold", sub))
            existing.add((acct, "Buy and Hold"))
            equity_covered_accounts.add(acct)

    if not extra:
        return summary_df if summary_df is not None else pd.DataFrame()

    extra_df = pd.DataFrame(extra)
    if summary_df is None or summary_df.empty:
        out = extra_df
    else:
        extra_df = extra_df.reindex(columns=list(summary_df.columns))
        # Drop all-NA columns from extra_df before concat to avoid pandas 2.x
        # FutureWarning about dtype-inferring through empty/all-NA columns.
        extra_df = extra_df.dropna(axis=1, how="all")
        out = pd.concat([summary_df, extra_df], ignore_index=True)

    if "status" in out.columns:
        _open = out["status"].astype(str).str.lower().eq("open")
        out = out.assign(_o=_open)
        if "total_return" in out.columns:
            out = out.sort_values(["_o", "total_return"], ascending=[False, False])
        else:
            out = out.sort_values("_o", ascending=False)
        out = out.drop(columns=["_o"])
    return out


def _fetch_int_strategy_classification_by_symbol(
    client, safe_symbol: str, tenant_ids
) -> pd.DataFrame:
    """User-scoped rows from int_strategy_classification for one symbol. Used when
    positions_summary is empty but we still need strategy breakdown (mart lag / path gaps).
    """
    if tenant_ids is not None and not tenant_ids:
        return pd.DataFrame()
    acct = _tenant_sql_and(tenant_ids)
    sql = f"""
    SELECT
        account, tenant_id, symbol, strategy, status, total_pnl, num_trades,
        is_winner, premium_received, premium_paid, days_in_trade,
        open_date, close_date
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{safe_symbol}'))
    {acct}
    """
    try:
        df = cached_query_df(client, sql, label="strategy_classification_fallback")
    except Exception as exc:
        app.logger.exception(
            "int_strategy_classification by symbol failed for %s: %s", safe_symbol, exc
        )
        return pd.DataFrame()
    df = _df_normalize_account_column(df)
    return _filter_df_by_tenant_ids(df, tenant_ids)


def _fetch_closed_option_legs_from_classification(
    client, safe_symbol: str, tenant_ids
) -> pd.DataFrame:
    """Closed option contract rows from int_strategy_classification only (no join).

    POSITION_CLOSED_LEGS joins to int_option_contracts. When that join misses (drift,
    renames, or partial loads), the page loses all closed option history. This query
    matches the P&L in classification and is the same grain as the join: one row per
    closed option trade group.
    """
    if tenant_ids is not None and not tenant_ids:
        return pd.DataFrame()
    acct = _tenant_sql_and(tenant_ids, col="sc.tenant_id")
    sql = f"""
    SELECT
        sc.account,
        sc.tenant_id,
        sc.symbol,
        sc.strategy,
        sc.trade_symbol,
        sc.open_date,
        sc.close_date,
        sc.total_pnl,
        sc.status,
        CAST(COALESCE(sc.num_trades, 1) AS INT64) AS quantity,
        sc.premium_received,
        sc.premium_paid,
        CAST(NULL AS FLOAT64) AS cost_to_close,
        CAST(NULL AS FLOAT64) AS proceeds_from_close,
        sc.direction,
        sc.close_type,
        sc.days_in_trade
    FROM `ccwj-dbt.analytics.int_strategy_classification` sc
    WHERE sc.status = 'Closed'
      AND sc.trade_group_type = 'option_contract'
      AND UPPER(TRIM(COALESCE(sc.symbol, ''))) = UPPER(TRIM('{safe_symbol}'))
    {acct}
    """
    try:
        df = cached_query_df(client, sql, label="closed_option_legs_fallback")
    except Exception as exc:
        app.logger.exception(
            "closed option legs fallback (classification) failed for %s: %s",
            safe_symbol,
            exc,
        )
        return pd.DataFrame()
    df = _df_normalize_account_column(df)
    return _filter_df_by_tenant_ids(df, tenant_ids)


def _rollup_int_strategy_to_summary_shape(cdf: pd.DataFrame) -> pd.DataFrame:
    """Replicate the strategy_summary grain of positions_summary from raw classification rows."""
    if cdf is None or cdf.empty or "account" not in cdf.columns or "strategy" not in cdf.columns:
        return pd.DataFrame()
    cdf = cdf.copy()
    for c in (
        "total_pnl", "num_trades", "premium_received", "premium_paid", "days_in_trade",
    ):
        if c in cdf.columns:
            cdf[c] = pd.to_numeric(cdf[c], errors="coerce").fillna(0.0)
    if "is_winner" in cdf.columns:
        cdf["is_winner"] = cdf["is_winner"].fillna(False).astype(bool)
    else:
        cdf = cdf.assign(is_winner=False)
    if "status" in cdf.columns:
        cdf["_st"] = cdf["status"].astype(str).str.strip().str.lower()
    else:
        cdf["_st"] = "unknown"
    if "symbol" not in cdf.columns:
        return pd.DataFrame()
    out = []
    for (acct, sym, strat), sub in cdf.groupby(
        [cdf["account"].astype(str), cdf["symbol"].astype(str), cdf["strategy"].astype(str)]
    ):
        ssub = sub.copy()
        is_open = ssub["_st"].eq("open")
        n_closed = int((~is_open).sum())
        c_real = float(ssub.loc[~is_open, "total_pnl"].sum()) if n_closed else 0.0
        c_unrl = float(ssub.loc[is_open, "total_pnl"].sum()) if is_open.any() else 0.0
        tot = float(ssub["total_pnl"].sum())
        pcr = float(ssub["premium_received"].sum()) if "premium_received" in ssub else 0.0
        ppd = float(ssub["premium_paid"].sum()) if "premium_paid" in ssub else 0.0
        n_groups = len(ssub)
        n_indiv = int(ssub["num_trades"].sum()) if "num_trades" in ssub else n_groups
        closed_mask = ~is_open
        if "is_winner" in ssub.columns:
            w_m = ssub[closed_mask & ssub["is_winner"]]
            l_m = ssub[closed_mask & ~ssub["is_winner"]]
            n_w = int(len(w_m))
            n_l = int(len(l_m))
        else:
            closed_pn = ssub.loc[closed_mask, "total_pnl"]
            n_w = int((closed_pn > 0).sum())
            n_l = int((closed_pn <= 0).sum())
        win_rate = n_w / (n_w + n_l) if (n_w + n_l) else 0.0
        avg_p = c_real / n_closed if n_closed else 0.0
        avg_d = 0.0
        if "days_in_trade" in ssub.columns:
            avg_d = float(ssub["days_in_trade"].fillna(0).mean() or 0.0)
        ftd, ltd = None, None
        if "open_date" in ssub.columns:
            ftd = ssub["open_date"].min()
        if "close_date" in ssub.columns:
            ltd = ssub["close_date"].max()
        row_status = "Open" if is_open.any() else "Closed"
        out.append(
            {
                "account": str(acct).strip(),
                "symbol": str(sym).strip(),
                "strategy": str(strat).strip(),
                "status": row_status,
                "total_pnl": round(tot, 2),
                "realized_pnl": round(c_real, 2),
                "unrealized_pnl": round(c_unrl, 2),
                "total_premium_received": round(pcr, 2),
                "total_premium_paid": round(ppd, 2),
                "num_trade_groups": n_groups,
                "num_individual_trades": n_indiv,
                "num_winners": n_w,
                "num_losers": n_l,
                "win_rate": win_rate,
                "avg_pnl_per_trade": round(avg_p, 2),
                "avg_days_in_trade": round(avg_d, 1) if avg_d else 0.0,
                "first_trade_date": ftd,
                "last_trade_date": ltd,
                "total_dividend_income": 0.0,
                "dividend_count": 0,
                "total_return": round(tot, 2),
            }
        )
    return pd.DataFrame(out) if out else pd.DataFrame()


def _supplement_summary_with_rolled(
    summary_df: pd.DataFrame, rolled_df: pd.DataFrame
) -> pd.DataFrame:
    """Return summary_df with rows from rolled_df whose (account, strategy) are
    missing. Keeps the mart as source of truth when it has the pair; fills gaps
    from int_strategy_classification so closed history shows up even when the
    mart lags (common right after a sync/CSV seed write, before dbt rebuilds).

    **Equity slot (Buy and Hold / Dividend):** ``positions_summary`` renames a
    top dividend-ranking ``Buy and Hold`` row to strategy label ``Dividend``
    post-aggregation — but rolled rows from ``int_strategy_classification``
    always say ``Buy and Hold``. Supplements previously keyed only on
    ``(account, strategy)``, so they'd add a second equity row with the realized
    P&L while the mart row already folded trade + dividends. That summed to
    ~trade_return + dividends + trade_return in the Strategy Breakdown and
    tripped the reconciliation invariant ($4,312 = exactly the double-count).
    Skip rolling in ``Buy and Hold`` when this account × symbol already has
    *either* label from the mart.
    """
    if rolled_df is None or rolled_df.empty:
        return summary_df if summary_df is not None else pd.DataFrame()
    if summary_df is None or summary_df.empty:
        return rolled_df
    # ``Crypto`` joins the equity-strategy slot for the same reason
    # ``Dividend`` does: it's the rename ``positions_summary`` applies
    # to a ``Buy and Hold`` row whose symbol is on the crypto whitelist
    # (Coinbase via SnapTrade). Without it, a rolled ``Buy and Hold``
    # from ``int_strategy_classification`` (which already says
    # ``Crypto`` for crypto symbols) would supplement on top of the
    # mart's ``Crypto`` row and double-count the realized P&L for
    # BTC / ETH / etc.
    _EQUITY_STRAT_SLOT = frozenset({"Buy and Hold", "Dividend", "Crypto"})
    existing: set[tuple[str, str]] = set()
    equity_slot_covered: set[tuple[str, str]] = set()
    for _, r in summary_df.iterrows():
        a = r.get("account")
        s = r.get("strategy")
        sym = (
            str(r.get("symbol") or "").strip()
            if r.get("symbol") is not None
            else ""
        )
        if a is None or s is None or (isinstance(s, float) and pd.isna(s)):
            continue
        st = str(s).strip()
        if not st:
            continue
        ac = str(a).strip()
        existing.add((ac, st))
        if sym and st in _EQUITY_STRAT_SLOT:
            equity_slot_covered.add((ac, sym))
    mask = []
    for _, r in rolled_df.iterrows():
        a = str(r.get("account") or "").strip()
        s = str(r.get("strategy") or "").strip()
        sym = (
            str(r.get("symbol") or "").strip()
            if r.get("symbol") is not None
            else ""
        )
        if not a or not s:
            mask.append(False)
            continue
        if (a, s) in existing:
            mask.append(False)
            continue
        # Mart already occupies the lone equity-slot row for this symbol.
        if s in _EQUITY_STRAT_SLOT and sym and (a, sym) in equity_slot_covered:
            mask.append(False)
            continue
        mask.append(True)
    add = rolled_df[mask] if mask else rolled_df.iloc[0:0]
    if add.empty:
        return summary_df
    add = add.reindex(columns=list(summary_df.columns))
    add = add.dropna(axis=1, how="all")
    return pd.concat([summary_df, add], ignore_index=True)


def _synthetic_open_strategy_from_current(current_df: pd.DataFrame) -> pd.DataFrame:
    """When there is a live snapshot in int_enriched_current but no mart / classification rows
    (only unrealized in positions_summary or empty), show one Open row so Strategy Breakdown is not empty.
    """
    if current_df is None or current_df.empty:
        return pd.DataFrame()
    from app.upload import is_crypto_symbol
    rows = []
    for _, r in current_df.iterrows():
        acct = str(r.get("account", "") or "").strip()
        it = str(r.get("instrument_type", "") or "")
        sym = str(r.get("symbol", "") or "").strip()
        if it == "Call":
            lab = "Long Call"
        elif it == "Put":
            lab = "Long Put"
        elif it == "Equity":
            # Equity rows for crypto symbols (Coinbase via SnapTrade
            # currently ship as security_type='Equity') get the Crypto
            # label so the strategy breakdown matches what the warehouse
            # would have surfaced via int_strategy_classification.
            lab = "Crypto" if is_crypto_symbol(sym) else "Buy and Hold"
        else:
            lab = "Open"
        u = float(r.get("unrealized_pnl") or 0)
        rows.append(
            {
                "account": acct,
                "symbol": sym,
                "strategy": lab,
                "status": "Open",
                "total_pnl": round(u, 2),
                "realized_pnl": 0.0,
                "unrealized_pnl": round(u, 2),
                "total_premium_received": 0.0,
                "total_premium_paid": 0.0,
                "num_trade_groups": 1,
                "num_individual_trades": 0,
                "num_winners": 0,
                "num_losers": 0,
                "win_rate": 0.0,
                "avg_pnl_per_trade": 0.0,
                "avg_days_in_trade": 0.0,
                "first_trade_date": None,
                "last_trade_date": None,
                "total_dividend_income": 0.0,
                "dividend_count": 0,
                "total_return": round(u, 2),
            }
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _compute_breakdown_by_type(
    *,
    client,
    safe_symbol: str,
    tenant_scope,
    closed_equity_df: pd.DataFrame,
    closed_legs_df: pd.DataFrame,
    current_df: pd.DataFrame,
    leg_predicate,
    dividends_df: pd.DataFrame = None,
):
    """Build the Equity / Options / Dividends rollup the position page renders
    above Strategy Breakdown.

    All P&L source frames passed in here are already leg-filtered by the
    caller (closed_legs_df, closed_equity_df by date overlap; current_df is
    cleared in position_detail() when no selected leg is Open). For dividends we
    have to do the leg-scope here because there is no per-row dividend
    frame upstream — int_dividend_events is queried directly and filtered
    by ``trade_date`` against ``leg_predicate``.

    leg_predicate: callable(date) -> bool when leg-filtered, else None.
    When None, every dividend event for the symbol counts.

    Returns a list of dict rows ready for Jinja:
        type, total, realized, unrealized, count, count_label, count_open
    Empty list when there is no activity at all (page won't render the card).

    Crypto positions (``safe_symbol`` on the ``CRYPTO_SYMBOLS`` whitelist
    — Coinbase via SnapTrade today) emit a ``Crypto`` row in place of the
    Equity row and suppress the Dividends row. The mechanics of crypto
    holdings on this product are identical to a long equity sit-and-hold
    (buy → hold → sell, no options, no ex-div) so the math is the same;
    relabeling preserves the asset-class signal in the UI. See
    ``app.upload.CRYPTO_SYMBOLS`` and ``stg_crypto_symbols`` for the
    source of truth.

    Numbers should sum to the page-level kpis['total_return'] within
    rounding (positions_summary uses rounded P&L per strategy; the mart's
    open_options unrealized has full precision).
    """
    from app.upload import is_crypto_symbol
    is_crypto = is_crypto_symbol(safe_symbol)
    eq_realized = 0.0
    eq_unrealized = 0.0
    eq_session_count = 0
    eq_open_count = 0
    if closed_equity_df is not None and not closed_equity_df.empty:
        if "realized_pnl" in closed_equity_df.columns:
            eq_realized = float(
                pd.to_numeric(closed_equity_df["realized_pnl"], errors="coerce")
                .fillna(0)
                .sum()
            )
        # int_closed_equity_legs has one row per *closure event* (each sell
        # in a session), not per session. Count distinct session_ids so the
        # UI says "1 session" when a trader sold their PLTR position over
        # three trips, not "3 sessions".
        if "session_id" in closed_equity_df.columns:
            eq_session_count += int(
                closed_equity_df[["account", "session_id"]].drop_duplicates().shape[0]
            )
        else:
            eq_session_count += len(closed_equity_df)
    if current_df is not None and not current_df.empty and "instrument_type" in current_df.columns:
        eq_open = current_df[current_df["instrument_type"] == "Equity"]
        if not eq_open.empty and "unrealized_pnl" in eq_open.columns:
            eq_unrealized = float(
                pd.to_numeric(eq_open["unrealized_pnl"], errors="coerce")
                .fillna(0)
                .sum()
            )
            eq_session_count += len(eq_open)
            eq_open_count += len(eq_open)

    opt_realized = 0.0
    opt_unrealized = 0.0
    opt_count = 0
    opt_open_count = 0
    if closed_legs_df is not None and not closed_legs_df.empty and "total_pnl" in closed_legs_df.columns:
        opt_realized = float(
            pd.to_numeric(closed_legs_df["total_pnl"], errors="coerce")
            .fillna(0)
            .sum()
        )
        opt_count += len(closed_legs_df)
    if current_df is not None and not current_df.empty and "instrument_type" in current_df.columns:
        opt_open = current_df[current_df["instrument_type"].isin(["Call", "Put"])]
        if not opt_open.empty and "unrealized_pnl" in opt_open.columns:
            opt_unrealized = float(
                pd.to_numeric(opt_open["unrealized_pnl"], errors="coerce")
                .fillna(0)
                .sum()
            )
            opt_count += len(opt_open)
            opt_open_count += len(opt_open)
            # Partial-close realized wedge: a contract that's still Open
            # (some sold, some held) has already-booked realized P&L that
            # lives on int_enriched_current.option_realized_pnl — NOT in
            # closed_legs_df (status='Closed' only) nor in unrealized_pnl
            # (the open remainder's MTM). Add it so the Options row totals
            # realized_on_closed + unrealized_on_open and the breakdown
            # reconciles with the chart / Strategy Breakdown (CRWV Aug 2026:
            # +$10,736 realized on 10 sold + $20,465 unrealized on 15 held).
            if "option_realized_pnl" in opt_open.columns:
                opt_realized += float(
                    pd.to_numeric(opt_open["option_realized_pnl"], errors="coerce")
                    .fillna(0)
                    .sum()
                )

    div_total = 0.0
    div_count = 0
    # Admin (`tenant_scope is None`) must run the query unscoped so
    # `_tenant_sql_and(None)` returns an empty filter and the admin sees
    # every tenant's data — same precedent as the rest of the position page.
    # Pre-fix the `is not None` guard short-circuited admin browsers and
    # `breakdown_rows.Dividends.total = 0` then OVERRODE the correctly-
    # computed Hero `dividend_income` (line ~3216 sync block) with $0,
    # producing the May 2026 JEPI bug: $0 dividends in Hero / Breakdown-
    # by-Type while Strategy Breakdown showed $77,780 (the same data).
    # Empty list `[]` (logged-in user with zero linked accounts) still
    # short-circuits — that's the correct "no data to show" path.
    #
    # Crypto holdings don't pay dividends in our pipeline (no ex-div
    # calendar from yfinance, no broker dividend rows for BTC/ETH/etc.).
    # Skip the query entirely so the breakdown card doesn't render a
    # noisy ``$0 dividends`` row for every crypto position page. If
    # staking yield ever lands as a dividend event we'll revisit.
    if is_crypto:
        pass
    elif tenant_scope is None or len(tenant_scope) > 0:
        try:
            # Prefer the frame fetched in the position_detail parallel batch
            # (one wave instead of a serial ~1-2s BQ round trip here). Fall
            # back to a direct query for any other caller / when not provided.
            if dividends_df is not None:
                div_df = dividends_df
            else:
                tenant_filter = _tenant_sql_and(tenant_scope)
                div_df = cached_query_df(
                    client,
                    POSITION_DIVIDENDS_QUERY.format(
                        symbol=safe_symbol, tenant_filter=tenant_filter
                    ),
                )
            # Belt-and-suspenders tenancy guard. The SQL is already user_id +
            # account scoped via _account_sql_and, but the BQ-tenant rule
            # requires a Python filter on every BQ result before any
            # re-aggregation. See .cursor/rules/bigquery-tenant-isolation.mdc.
            div_df = _filter_df_by_tenant_ids(div_df, tenant_scope)
            if not div_df.empty:
                if leg_predicate is not None and "trade_date" in div_df.columns:
                    div_df = div_df.copy()
                    div_df["_d"] = pd.to_datetime(div_df["trade_date"]).dt.date
                    div_df = div_df[div_df["_d"].apply(leg_predicate)]
                if not div_df.empty and "amount" in div_df.columns:
                    div_total = float(
                        pd.to_numeric(div_df["amount"], errors="coerce")
                        .fillna(0)
                        .sum()
                    )
                    div_count = len(div_df)
        except Exception as exc:
            # Dividends are a nice-to-have on the breakdown; if int_dividend_events
            # is unavailable or schema-drifted, log and show a 0 row rather than
            # crashing the whole position page.
            app.logger.exception(
                "breakdown by-type dividends fetch failed for %s: %s", safe_symbol, exc
            )

    eq_total = eq_realized + eq_unrealized
    opt_total = opt_realized + opt_unrealized

    if (
        eq_session_count == 0
        and opt_count == 0
        and div_count == 0
    ):
        return []

    equity_or_crypto_row = {
        # Relabel the equity row as Crypto for crypto positions. The
        # underlying math is identical (sessions / realized / unrealized
        # all come from the same int_equity_sessions →
        # int_closed_equity_legs path); only the row label changes so
        # the user sees their BTC / ETH / USDC bucketed by asset class
        # instead of fused into "Equity" alongside their VOO and JEPI.
        "type": "Crypto" if is_crypto else "Equity",
        "total": round(eq_total, 2),
        "realized": round(eq_realized, 2),
        "unrealized": round(eq_unrealized, 2),
        "count": eq_session_count,
        "count_label": (
            ("holding" if eq_session_count == 1 else "holdings")
            if is_crypto
            else ("session" if eq_session_count == 1 else "sessions")
        ),
        "count_open": eq_open_count,
    }
    rows = [
        equity_or_crypto_row,
        {
            "type": "Options",
            "total": round(opt_total, 2),
            "realized": round(opt_realized, 2),
            "unrealized": round(opt_unrealized, 2),
            "count": opt_count,
            "count_label": "contract" if opt_count == 1 else "contracts",
            "count_open": opt_open_count,
        },
    ]
    if not is_crypto:
        # Suppress the Dividends row for crypto — we never query for it
        # above (no ex-div feed) and rendering ``$0 dividends`` would
        # be noisy on every BTC / ETH page.
        rows.append({
            "type": "Dividends",
            "total": round(div_total, 2),
            "realized": round(div_total, 2),
            # Dividends are realized cash income — no mark-to-market component,
            # so leave a sentinel the template can render as an em-dash.
            "unrealized": None,
            "count": div_count,
            "count_label": "event" if div_count == 1 else "events",
            "count_open": 0,
        })
    return rows


def _realized_pnl_from_closed_frames(
    closed_legs_df: pd.DataFrame, closed_equity_df: pd.DataFrame
) -> float:
    """Sum realized P&L from closed option contract legs and closed equity lots."""
    r = 0.0
    if (
        closed_legs_df is not None
        and not closed_legs_df.empty
        and "total_pnl" in closed_legs_df.columns
    ):
        r += float(closed_legs_df["total_pnl"].sum())
    if (
        closed_equity_df is not None
        and not closed_equity_df.empty
        and "realized_pnl" in closed_equity_df.columns
    ):
        r += float(closed_equity_df["realized_pnl"].sum())
    return r


def _premium_totals_from_closed_options(closed_legs_df: pd.DataFrame) -> tuple:
    if closed_legs_df is None or closed_legs_df.empty:
        return 0.0, 0.0
    pr = (
        float(closed_legs_df["premium_received"].sum())
        if "premium_received" in closed_legs_df.columns
        else 0.0
    )
    pp = (
        float(closed_legs_df["premium_paid"].sum())
        if "premium_paid" in closed_legs_df.columns
        else 0.0
    )
    return pr, pp


# Pre-aggregated daily P&L data for chart rendering (single symbol)
# Per-symbol dividend events for the Breakdown-by-Type card. Extracted to a
# module constant (was inlined inside _compute_breakdown_by_type) so the
# position_detail route can fetch it in the SAME parallel _bq_parallel wave
# as everything else instead of paying a separate ~1-2s BigQuery job round
# trip serially after the batch. _compute_breakdown_by_type still owns the
# tenant + leg filtering of the returned frame.
POSITION_DIVIDENDS_QUERY = """
    SELECT account, tenant_id, user_id, symbol, trade_date, amount
    FROM `ccwj-dbt.analytics.int_dividend_events`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
"""

# Synthesized opening balances (int_opening_balances): positions whose buys
# predate the imported broker history window. Powers the "history starts
# here" disclosure banner — the share quantity is provable arithmetic, but
# the opening COST is estimated (price_source says how), so the page must
# say so and offer the CSV-upload path to fill the gap with real fills.
POSITION_OPENING_BALANCES_QUERY = """
    SELECT account, tenant_id, user_id, symbol, opening_date,
           first_trade_date, opening_qty, price_source, est_amount
    FROM `ccwj-dbt.analytics.int_opening_balances`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
"""

# Symbol-grain public market data (same class as POSITION_EARNINGS_QUERY):
# no tenant column exists and none is needed — a split applies identically
# to every holder. Do NOT run the result through _filter_df_by_tenant_ids
# (it would fail-closed to empty for non-admins).
POSITION_SPLITS_QUERY = """
    SELECT symbol, split_date, split_ratio
    FROM `ccwj-dbt.analytics.stg_split_events`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
    ORDER BY split_date
"""


# ── Position story mode ──────────────────────────────────────────────────
# Plain-English narrative of the position ("Story" card) plus event markers
# overlaid on the cumulative P&L chart. The engine lives in
# app/position_story.py: semantic maneuver detection (rolls, wheels,
# covered calls, assignments, kept premium) + interludes narrated from the
# daily-mark chart series. Built entirely from frames the page already
# fetches — no extra queries.
from app.position_story import build_position_story, compose_mirror  # noqa: E402
from app.execution_quality import (  # noqa: E402
    POSITION_EXECUTION_QUERY,
    exit_notes as _execution_exit_notes,
    symbol_execution_sentences as _symbol_execution_sentences,
)


@app.route("/position/<symbol>")
@login_required
@skeleton_page
def position_detail(symbol):
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()

    # Escape symbol for SQL (prevent injection)
    safe_symbol = symbol.replace("'", "''")

    # `_tenant_sql_and` scopes by broker-stable `tenant_id`; `?account=`
    # maps to tenant_ids via `_tenants_for_scope`.
    selected_account = request.args.get("account", "").strip()
    tenant_scope = _tenants_for_scope(selected_account)

    # Full owned-tenant scope (ignores the ?tenants= on/off subset) so the
    # account-toggle bar can list every account that traded this symbol —
    # including ones currently toggled off. Isolation is preserved: for a
    # non-admin this is exactly their owned tenants, for admin it's None.
    all_owned_scope = _user_tenant_list()

    try:
        _pos_acct = _tenant_sql_and(tenant_scope)
        _pos_all_acct = _tenant_sql_and(all_owned_scope)
        _pos_sc_acct = _tenant_sql_and(tenant_scope, col="sc.tenant_id")
        # POSITION_TRADES_QUERY joins stg_history (alias h) to int_drip_fills (alias d);
        # both tables have an `account` column so the filter must be scoped to h.
        _pos_h_acct = _tenant_sql_and(tenant_scope, col="h.tenant_id")
        # Single parallel wave. Every query below is a tiny (~MB) read whose
        # cost is BigQuery's fixed per-job latency, so the win is running them
        # ALL AT ONCE rather than in serial phases. The chart (mart_daily_pnl)
        # and dividends (int_dividend_events) reads used to run serially AFTER
        # this batch (a ~2s round trip each); they are URL-derived and
        # independent of the batch, so they join the wave here. All the
        # batch-result-dependent logic (summary narrowing, leg filtering)
        # stays in Python after the fetch.
        from app.upload import is_crypto_symbol
        _is_crypto = is_crypto_symbol(safe_symbol)
        _pos_queries = {
            "summary": POSITION_SUMMARY_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
            "trades": POSITION_TRADES_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_h_acct
            ),
            "current": POSITION_CURRENT_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
            "closed_legs": POSITION_CLOSED_LEGS_QUERY.format(
                symbol=safe_symbol, sc_tenant_filter=_pos_sc_acct
            ),
            "closed_equity": POSITION_CLOSED_EQUITY_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
            "matrix": POSITION_MATRIX_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
            "legs": POSITION_LEGS_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
            # Account-toggle bar source: every account that traded this
            # symbol across the viewer's FULL owned set (not the ?tenants=
            # subset), so a toggled-off account is still listed.
            "accounts_all": POSITION_ACCOUNTS_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_all_acct
            ),
            # Lightweight all-symbols rollup that powers the symbol tab strip
            # at the top of the page. Scoped by `tenant_scope` so the
            # tabs match the page's account filter (when ?account= is set the
            # strip narrows; otherwise it spans the viewer's accounts).
            "tabs": SYMBOL_TABS_QUERY.format(tenant_filter=_pos_acct),
            # Symbol-level next-earnings date for the hero pill. No account
            # filter — stg_earnings_calendar is symbol-grain public data.
            "earnings": POSITION_EARNINGS_QUERY.format(symbol=safe_symbol),
            # Cumulative daily P&L for the chart (post-processed below).
            "chart": CHART_DATA_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
            # Stock splits for the story engine: a split is both a story
            # beat ("your 100 shares became 300") and required for correct
            # running-share state — stg_history quantities are in the
            # share-units of their fill date (see stock-splits rule). No
            # tenant filter — stg_split_events is symbol-grain public
            # market data (like earnings above); running it through the
            # tenant filter would fail-closed to empty.
            "splits": POSITION_SPLITS_QUERY.format(symbol=safe_symbol),
            # Execution review (int_option_exit_quality): after-the-fact
            # verdicts on early closes / rolls, graded against the
            # underlying's close at each contract's expiry. Feeds the
            # mirror sentences and the day-row verdict notes.
            "execution": POSITION_EXECUTION_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
            # Synthesized opening balances → "history starts here" banner.
            "opening": POSITION_OPENING_BALANCES_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
        }
        # Crypto positions don't pay dividends in our pipeline, so
        # _compute_breakdown_by_type skips them — don't fetch the frame.
        if not _is_crypto:
            _pos_queries["dividends"] = POSITION_DIVIDENDS_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            )
        dfs = _bq_parallel(client, _pos_queries)
        summary_df = dfs["summary"]
        trades_df = dfs["trades"]
        current_df = dfs["current"]
        closed_legs_df = dfs["closed_legs"]
        closed_equity_df = dfs["closed_equity"]
        matrix_df = dfs["matrix"]
        legs_df = dfs["legs"]
        accounts_all_df = dfs.get("accounts_all", pd.DataFrame())
        tabs_df = dfs["tabs"]
        earnings_df = dfs["earnings"]
        # Fetched in the batch above; post-processed later in the route
        # (chart) / inside _compute_breakdown_by_type (dividends). Not run
        # through _df_normalize_account_column to match their prior serial
        # behavior.
        chart_df = dfs.get("chart", pd.DataFrame())
        dividends_df = dfs.get("dividends")
        splits_df = dfs.get("splits", pd.DataFrame())
        execution_df = dfs.get("execution", pd.DataFrame())
        opening_df = dfs.get("opening", pd.DataFrame())
        summary_df = _df_normalize_account_column(summary_df)
        trades_df = _df_normalize_account_column(trades_df)
        current_df = _df_normalize_account_column(current_df)
        closed_legs_df = _df_normalize_account_column(closed_legs_df)
        closed_equity_df = _df_normalize_account_column(closed_equity_df)
        matrix_df = _df_normalize_account_column(matrix_df)
        legs_df = _df_normalize_account_column(legs_df)
        accounts_all_df = _df_normalize_account_column(accounts_all_df)
        tabs_df = _df_normalize_account_column(tabs_df)
    except Exception as exc:
        return render_template(
            "position_detail.html",
            symbol=symbol,
            error=str(exc),
            kpis={},
            strategy_rows=[],
            breakdown_rows=[],
            trades=[],
            trade_outcomes=[],
            current_positions=[],
            option_matrices=[],
            sessions=[],
            legs_by_account=[],
            account_toggles=[],
            tenants_param="",
            selected_legs=[],
            leg_param="",
            chart_data_json="{}",
            story_days=[],
            story_markers_json="[]",
            story_mirror=[],
            has_underlying_price=False,
            symbol_sector="",
            symbol_subsector="",
            symbol_company="",
            symbol_next_earnings=None,
            opening_balances=[],
            tabs=[],
            active_symbol=symbol,
            tab_href_base="/position/",
            tab_href_suffix="",
            mode="navigate",
        )

    # Clean numeric types for summary
    num_cols = [
        "total_pnl", "realized_pnl", "unrealized_pnl",
        "total_premium_received", "total_premium_paid",
        "num_trade_groups", "num_individual_trades",
        "num_winners", "num_losers", "win_rate",
        "avg_pnl_per_trade", "avg_days_in_trade",
        "total_dividend_income", "dividend_count", "total_return",
    ]
    for col in num_cols:
        if col in summary_df.columns:
            summary_df[col] = pd.to_numeric(summary_df[col], errors="coerce").fillna(0)

    # Clean trades
    for col in ["amount", "quantity", "price", "fees"]:
        if col in trades_df.columns:
            trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
    if "trade_date" in trades_df.columns:
        trades_df["trade_date"] = pd.to_datetime(trades_df["trade_date"]).dt.date

    # Clean current positions
    for col in ["unrealized_pnl", "market_value", "quantity", "current_price", "cost_basis"]:
        if col in current_df.columns:
            current_df[col] = pd.to_numeric(current_df[col], errors="coerce").fillna(0)
    if "unrealized_pnl_pct" in current_df.columns:
        current_df["unrealized_pnl_pct"] = pd.to_numeric(
            current_df["unrealized_pnl_pct"], errors="coerce"
        ).fillna(0)

    # Filter to user's accounts (must run on every BQ frame — queries are by symbol
    # only, so unfiltered closed_legs/closed_equity/matrix would include all tenants.)
    # Use ``tenant_scope`` so admin viewing a non-personal selected_account
    # doesn't strip the just-fetched rows. ``_filter_df_by_accounts`` still
    # enforces the user_id boundary for non-admins.
    summary_df = _filter_df_by_tenant_ids(summary_df, tenant_scope)
    trades_df = _filter_df_by_tenant_ids(trades_df, tenant_scope)
    current_df = _filter_df_by_tenant_ids(current_df, tenant_scope)
    closed_legs_df = _filter_df_by_tenant_ids(closed_legs_df, tenant_scope)
    closed_equity_df = _filter_df_by_tenant_ids(closed_equity_df, tenant_scope)
    matrix_df = _filter_df_by_tenant_ids(matrix_df, tenant_scope)
    # Tab strip data has the same tenancy boundary as everything else above.
    tabs_df = _filter_df_by_tenant_ids(tabs_df, tenant_scope)
    opening_df = _filter_df_by_tenant_ids(opening_df, tenant_scope)
    # earnings_df is symbol-grain PUBLIC market data (stg_earnings_calendar
    # has no tenant/account/user column at all — it cannot leak tenant rows).
    # It must NOT go through _filter_df_by_tenant_ids: the filter fails
    # CLOSED on a missing tenant_id column, which silently blanked the
    # earnings hero pill for every non-admin user (Aug 2026 regression when
    # the fail-closed behavior shipped). The tenant-isolation rule governs
    # tenant data; this frame carries none.

    # Joined closed legs are empty: int_option_contracts can fail to match while
    # int_strategy_classification still has closed option P&L — use classification only.
    if closed_legs_df.empty and (
        tenant_scope is None
        or (isinstance(tenant_scope, list) and len(tenant_scope) > 0)
    ):
        _cl_sup = _fetch_closed_option_legs_from_classification(
            client, safe_symbol, tenant_scope
        )
        if not _cl_sup.empty:
            closed_legs_df = _cl_sup
            for col in ["total_pnl", "premium_received", "premium_paid", "days_in_trade"]:
                if col in closed_legs_df.columns:
                    closed_legs_df[col] = pd.to_numeric(
                        closed_legs_df[col], errors="coerce"
                    ).fillna(0)

    # Optional filters carried from Positions page
    selected_account = request.args.get("account", "")
    selected_strategy = request.args.get("strategy", "")
    selected_statuses = request.args.getlist("status")
    selected_start_date = request.args.get("start_date", "")
    selected_end_date = request.args.get("end_date", "")

    start_date = _parse_date(selected_start_date)
    end_date = _parse_date(selected_end_date)

    # No secondary ``account == selected_account`` narrowing: tenant scope
    # (resolved from the selected display label, incl. disambiguated
    # colliding labels) already filtered these frames by tenant_id.
    if not current_df.empty:
        current_df = _dedupe_enriched_current_positions(current_df)
    # Full tenant-scoped history seeds the narrative state when strategy,
    # date, or leg filters hide earlier fills. The story engine replays only
    # rows before its first visible day, silently and split-aware.
    story_seed_trades = trades_df.copy()
    if selected_strategy:
        if "strategy" in summary_df.columns:
            summary_df = summary_df[summary_df["strategy"] == selected_strategy]
        if "strategy" in trades_df.columns:
            trades_df = trades_df[trades_df["strategy"] == selected_strategy]
    if selected_statuses and "status" in summary_df.columns:
        summary_df = summary_df[summary_df["status"].isin(selected_statuses)]
    if start_date is not None and "trade_date" in trades_df.columns:
        trades_df = trades_df[trades_df["trade_date"] >= start_date]
    if end_date is not None and "trade_date" in trades_df.columns:
        trades_df = trades_df[trades_df["trade_date"] <= end_date]

    # ── Position legs (read from int_position_legs mart) ──
    # The mart owns the canonical leg definition (equity sessions + option-only
    # orphan legs, with Open status whenever any attached option is still live
    # so the pill agrees with the banner). _legs_df_to_sessions_list reshapes
    # the mart rows into the legacy dict shape the template + downstream
    # helpers consume, preserving the leg_id ↔ session_id contract that keeps
    # bookmarked ?leg=<n> URLs working.
    legs_df = _filter_df_by_tenant_ids(legs_df, tenant_scope)
    # Tenant scope already narrowed legs to the selected account's tenant.

    sessions_list = _legs_df_to_sessions_list(legs_df)

    # ── Attach user-defined leg tags (Postgres, isolated by user_id) ──
    # Match each stored tag to the current leg by date-containment (see
    # _tags_for_leg_range). tenant_scope restricts the read to in-scope
    # accounts; a distinct-tag list feeds the "+ tag" autocomplete datalist.
    from app.models import (
        get_leg_tags_for_symbol as _get_leg_tags_for_symbol,
        get_distinct_tags_for_user as _get_distinct_tags_for_user,
    )
    _leg_tag_rows = _get_leg_tags_for_symbol(
        getattr(current_user, "id", None), symbol, tenant_scope
    )
    for s in sessions_list:
        s["tags"] = _tags_for_leg_range(
            _leg_tag_rows, s.get("tenant_id"), s.get("open_date"),
            s.get("last_trade_date"),
        )
    all_user_tags = _get_distinct_tags_for_user(getattr(current_user, "id", None))

    # ── Group legs by account ──
    # When a symbol is traded across several accounts, leg_id / display_leg
    # restart per tenant (so two accounts can both show "Leg 1"). Attach a
    # disambiguated per-account label to each session so the template can
    # GROUP the pills under account headers instead of interleaving a
    # confusing run of duplicate leg numbers.
    _viewer_id = getattr(current_user, "id", None)
    _tenant_label_map = _tenant_label_map_for_user(_viewer_id)
    _acct_nick_map = _account_label_map(_viewer_id)

    def _account_display_for(tenant_id, account_raw):
        label = _tenant_label_map.get(tenant_id or "") if tenant_id else None
        if not label:
            raw = account_raw or ""
            label = _acct_nick_map.get(raw, raw)
        return label or "Account"

    for s in sessions_list:
        s["account_display"] = _account_display_for(
            s.get("tenant_id"), s.get("account")
        )

    legs_by_account = []
    _leg_groups = {}
    for s in sessions_list:
        _leg_groups.setdefault(s.get("tenant_id") or "", []).append(s)
    for _tid, _sess in _leg_groups.items():
        legs_by_account.append({
            "tenant_id": _tid,
            "label": _sess[0].get("account_display") or "Account",
            "sessions": _sess,
        })
    legs_by_account.sort(key=lambda g: g["label"])

    # ── Account toggle bar (turn entire accounts on/off) ──
    # Built from the FULL owned-tenant set (accounts_all_df), so an account
    # that's currently toggled off still appears and can be turned back on.
    accounts_all_df = _filter_df_by_tenant_ids(accounts_all_df, all_owned_scope)
    selected_tenant_set = set(tenant_scope) if tenant_scope is not None else None
    account_toggles = []
    if (
        accounts_all_df is not None
        and not accounts_all_df.empty
        and "tenant_id" in accounts_all_df.columns
    ):
        _seen_tids = set()
        for _, _r in accounts_all_df.iterrows():
            _tid = str(_r.get("tenant_id") or "")
            if not _tid or _tid in _seen_tids:
                continue
            _seen_tids.add(_tid)
            account_toggles.append({
                "tenant_id": _tid,
                "label": _account_display_for(_tid, str(_r.get("account") or "")),
                "selected": True if selected_tenant_set is None else (_tid in selected_tenant_set),
            })
        account_toggles.sort(key=lambda a: a["label"])
    # Preserve the current account subset on leg "Show All" / navigation.
    tenants_param = request.args.get("tenants", "").strip()

    leg_param, selected_legs = _resolve_position_leg_filter(
        sessions_list, request.args.get("leg", "")
    )

    # Build date ranges for selected sessions
    _leg_ranges = []
    _has_open_leg = False
    for s in sessions_list:
        if s["session_id"] in selected_legs:
            od = pd.to_datetime(s["open_date"]).date() if s["open_date"] else None
            ltd = pd.to_datetime(s["last_trade_date"]).date() if s["last_trade_date"] else None
            is_open = str(s.get("status", "")).strip().lower() == "open"
            if is_open:
                _has_open_leg = True
            _leg_ranges.append((od, ltd if not is_open else date.today()))

    def _in_leg_range(d):
        """Return True if date d falls within any selected leg's date range."""
        if not _leg_ranges:
            return True
        for lo, hi in _leg_ranges:
            if lo and hi and lo <= d <= hi:
                return True
            if lo and not hi and d >= lo:
                return True
        return False

    # Snapshot before leg filter so hero + chart can use full symbol history
    # when the selected leg has no trade rows in-range (common for new option legs).
    trades_pre_leg = trades_df.copy()

    # Apply leg filter to trades
    if leg_param and "trade_date" in trades_df.columns and _leg_ranges:
        trades_df = trades_df[trades_df["trade_date"].apply(_in_leg_range)]

    # Apply leg filter to current positions (only show if an open leg is selected)
    if leg_param and not _has_open_leg:
        current_df = current_df.iloc[0:0]

    # For open equity positions: if cost_basis is missing/zero, derive from trade history
    # so unrealized P&L = market_value - cost_basis (true P/L for open positions)
    if not current_df.empty and not trades_df.empty and "action" in trades_df.columns:
        for idx, row in current_df.iterrows():
            if row.get("instrument_type") != "Equity":
                continue
            cost_basis = float(row.get("cost_basis") or 0)
            market_value = float(row.get("market_value") or 0)
            if market_value <= 0:
                continue
            if cost_basis is None or cost_basis == 0:
                acct, sym = row.get("account"), row.get("symbol")
                buys = trades_df[
                    (trades_df["account"] == acct)
                    & (trades_df["symbol"] == sym)
                    & (trades_df["action"].astype(str).str.lower().str.strip() == "buy")
                ]
                if not buys.empty:
                    cost_basis = abs(float(buys["amount"].sum()))
                    current_df.at[idx, "cost_basis"] = cost_basis
                    current_df.at[idx, "unrealized_pnl"] = market_value - cost_basis
                    if cost_basis:
                        current_df.at[idx, "unrealized_pnl_pct"] = 100.0 * (market_value - cost_basis) / cost_basis

    # ── Filter closed legs early so KPIs can use them ──
    # (tenant scope already narrowed to the selected account's tenant)
    if selected_strategy and not closed_legs_df.empty and "strategy" in closed_legs_df.columns:
        closed_legs_df = closed_legs_df[closed_legs_df["strategy"] == selected_strategy]
    # Defense in depth: drop ``int_closed_equity_legs`` "Cost Written Off"
    # rows when ``int_enriched_current`` still shows the symbol held on
    # the same account. dbt's ``account_symbol_holdings`` suppression is
    # the source of truth, but BigQuery may serve stale rows until the
    # next ``dbt build``. Doing the same suppression here prevents
    # phantom writeoffs from poisoning legs / breakdown / chart-substitute
    # in the meantime. See ``_drop_phantom_equity_writeoffs`` doc.
    pre_strip_n = len(closed_equity_df)
    closed_equity_df, _stripped_writeoffs = _drop_phantom_equity_writeoffs(
        closed_equity_df, current_df
    )
    if len(closed_equity_df) != pre_strip_n:
        try:
            app.logger.info(
                "position_detail: stripped %d phantom Cost Written Off "
                "row(s) for %s/%s — broker still holds the shares; "
                "dbt int_closed_equity_legs.account_symbol_holdings will "
                "make this redundant after the next build.",
                pre_strip_n - len(closed_equity_df),
                selected_account or "ALL",
                safe_symbol,
            )
        except Exception as exc:
            _log.info(
                "position_detail: stripped %d phantom writeoff row(s) for %s/%s "
                "(logger unavailable: %s)",
                pre_strip_n - len(closed_equity_df),
                selected_account or "ALL",
                safe_symbol,
                exc,
            )
        # ``positions_summary`` aggregates the same phantom row into a
        # Closed strategy rollup. Reverse it so Strategy Breakdown
        # agrees with Position Legs + Breakdown by Type until dbt
        # rebuilds and the source row is gone.
        summary_df = _addback_phantom_writeoffs_to_summary(
            summary_df, _stripped_writeoffs
        )
    # Before leg scoping, keep copies for "first/last activity" on the page
    closed_legs_pre_leg = closed_legs_df.copy()
    closed_equity_pre_leg = closed_equity_df.copy()
    if leg_param and _leg_ranges:
        if not closed_legs_df.empty and "open_date" in closed_legs_df.columns:
            closed_legs_df["_od"] = pd.to_datetime(closed_legs_df["open_date"]).dt.date
            closed_legs_df = closed_legs_df[closed_legs_df["_od"].apply(_in_leg_range)]
            closed_legs_df = closed_legs_df.drop(columns=["_od"])
        # Equity session leg-filter: use open_date overlap, NOT session_id.
        # int_closed_equity_legs.session_id is the int_equity_sessions
        # session number (1, 2, ...), which used to also be our leg pill's
        # session_id. Under the merged-interval int_position_legs the pill
        # leg_id is sequential per merged chapter and may not equal the
        # equity session_id at all (a single equity session can be merged
        # into a leg labeled 2 because an earlier orphan-options leg got
        # leg_id 1). Filtering by session_id collisions used to spill the
        # equity session into the wrong leg's tables — visible bug for
        # PLTR / Cameron Investment ?leg=1 (Buy and Hold appeared in the
        # Nov 2024 orphan leg's strategy table).
        if not closed_equity_df.empty and "open_date" in closed_equity_df.columns:
            closed_equity_df["_od"] = pd.to_datetime(closed_equity_df["open_date"]).dt.date
            closed_equity_df = closed_equity_df[closed_equity_df["_od"].apply(_in_leg_range)]
            closed_equity_df = closed_equity_df.drop(columns=["_od"])

    # Min/max activity for hero + chart when summary/leg filter hides dates (e.g. open option leg).
    _activity_all_dates = _collect_activity_candidate_dates(
        trades_pre_leg, closed_legs_pre_leg, closed_equity_pre_leg, sessions_list
    )
    _activity_date_min = min(_activity_all_dates) if _activity_all_dates else None
    _activity_date_max = max(_activity_all_dates) if _activity_all_dates else None

    # Status (needed for open-only realized logic)
    status_col = None
    for c in ("status", "Status", "STATUS"):
        if c in (summary_df.columns if not summary_df.empty else []):
            status_col = c
            break
    statuses = summary_df[status_col].unique().tolist() if status_col and not summary_df.empty else []
    _has_open = any(str(s).strip().lower() == "open" for s in statuses if s is not None)
    _has_closed = any(str(s).strip().lower() == "closed" for s in statuses if s is not None)
    # Open equity/options from snapshots have no positions_summary row until trades exist in stg_history.
    if not _has_open and not current_df.empty:
        _has_open = True
    if _has_open:
        overall_status = "Open"
    else:
        overall_status = "Closed"

    # When leg filter is active, override overall_status based on selected sessions
    if leg_param:
        overall_status = "Open" if _has_open_leg else "Closed"

    # ── KPIs and Strategy Rows ──
    # When leg filter is active, recompute from filtered trade data instead of summary_df
    if leg_param and _leg_ranges:
        # Filter summary_df by date overlap with selected leg ranges
        if not summary_df.empty and "first_trade_date" in summary_df.columns:
            summary_df["_ftd"] = pd.to_datetime(summary_df["first_trade_date"]).dt.date
            summary_df = summary_df[summary_df["_ftd"].apply(_in_leg_range)]
            summary_df = summary_df.drop(columns=["_ftd"])

    total_winners = int(summary_df["num_winners"].sum()) if not summary_df.empty else 0
    total_losers = int(summary_df["num_losers"].sum()) if not summary_df.empty else 0
    total_closed = total_winners + total_losers

    _sell_actions = ("equity_sell", "option_sell_to_close", "option_buy_to_close")
    has_sell_trades = (
        not trades_df.empty
        and "action" in trades_df.columns
        and trades_df["action"].astype(str).str.strip().isin(_sell_actions).any()
    )
    # True only for snapshot-only / no-history edge cases (not "any open position").
    is_open_only = (total_closed == 0 and not current_df.empty) or (
        not has_sell_trades and not current_df.empty
    )

    if leg_param and _leg_ranges:
        realized_for_display = _realized_pnl_from_closed_frames(
            closed_legs_df, closed_equity_df
        )
    else:
        has_closed_frame = (not closed_legs_pre_leg.empty) or (
            not closed_equity_pre_leg.empty
        )
        if has_closed_frame:
            realized_for_display = _realized_pnl_from_closed_frames(
                closed_legs_pre_leg, closed_equity_pre_leg
            )
        else:
            realized_for_display = (
                float(summary_df["realized_pnl"].sum()) if not summary_df.empty else 0.0
            )

    if app.debug and symbol == "ATZAF":
        app.logger.warning(
            "position_detail ATZAF: status_col=%s overall_status=%s total_closed=%s is_open_only=%s realized_for_display=%s",
            status_col, overall_status, total_closed, is_open_only, realized_for_display,
        )

    kpis = {}
    # positions_summary is trade-derived; open lots synced without matching history have current_df only.
    _show_position_kpis = (
        leg_param
        or not summary_df.empty
        or not current_df.empty
        or not trades_df.empty
    )
    if _show_position_kpis:
        # Prefer positions_summary's unrealized_pnl when we have it — it is trade-derived
        # and rolls up *every* open leg (equity + each option contract). int_enriched_current
        # can be partial for a symbol (e.g. broker positions feed has the open option but not
        # the long stock, or vice versa) which is what was making the hero disagree with the
        # strategy-breakdown row underneath it. Only fall back to current_df when summary is
        # empty (positions imported with no transaction history at all).
        if not summary_df.empty and "unrealized_pnl" in summary_df.columns:
            unrealized_from_summary = float(summary_df["unrealized_pnl"].sum())
        elif not current_df.empty and "unrealized_pnl" in current_df.columns:
            unrealized_from_summary = float(current_df["unrealized_pnl"].sum())
        else:
            unrealized_from_summary = 0.0

        # When leg-filtered, premium = filtered closed options only (never full-history
        # legs when the filtered frame is empty for that range).
        if leg_param and _leg_ranges:
            if not closed_legs_df.empty:
                pr, pp = _premium_totals_from_closed_options(closed_legs_df)
            else:
                pr, pp = 0.0, 0.0
            premium_collected, premium_paid = pr, pp
        else:
            pr, pp = _premium_totals_from_closed_options(closed_legs_pre_leg)
            premium_collected, premium_paid = pr, pp
            if (premium_collected == 0.0 and premium_paid == 0.0) and not summary_df.empty:
                premium_collected = float(summary_df["total_premium_received"].sum())
                premium_paid = float(summary_df["total_premium_paid"].sum())

        # Trade count: use row count when summary is empty (e.g. Schwab positions-only path)
        if leg_param or summary_df.empty:
            trade_count = _count_placed_fills(trades_df)
            if trade_count == 0:
                trade_count = _count_placed_fills(trades_pre_leg)
        else:
            trade_count = int(
                summary_df["num_individual_trades"].sum()
            ) if "num_individual_trades" in summary_df.columns else 0
            if trade_count == 0:
                trade_count = _count_placed_fills(trades_pre_leg)

        # Date range: prefer stg (trades_pre_leg) when present — positions_summary can lag
        # and show 0 trades + a bogus same-day "first" and "last" as-of stamp.
        if leg_param and not trades_df.empty and "trade_date" in trades_df.columns:
            first_trade = str(trades_df["trade_date"].min())[:10]
            last_trade = str(trades_df["trade_date"].max())[:10]
        elif (not leg_param) and (not trades_pre_leg.empty) and "trade_date" in trades_pre_leg.columns:
            first_trade = str(trades_pre_leg["trade_date"].min())[:10]
            last_trade = str(trades_pre_leg["trade_date"].max())[:10]
        elif not summary_df.empty and "first_trade_date" in summary_df.columns:
            first_trade = str(pd.to_datetime(summary_df["first_trade_date"].min()).date())
            last_trade = str(pd.to_datetime(summary_df["last_trade_date"].max()).date())
        elif not trades_df.empty and "trade_date" in trades_df.columns:
            first_trade = str(trades_df["trade_date"].min())[:10]
            last_trade = str(trades_df["trade_date"].max())[:10]
        else:
            first_trade = ""
            last_trade = ""
        if (not first_trade) and _activity_date_min is not None:
            first_trade = str(_activity_date_min)
        if (not last_trade) and _activity_date_max is not None:
            last_trade = str(_activity_date_max)

        # Open, still no real range (e.g. only summary as-of) — session open + through today
        if (
            not leg_param
            and overall_status == "Open"
            and sessions_list
            and (not first_trade or (first_trade == last_trade and trade_count == 0))
        ):
            ods = []
            for s in sessions_list:
                if str(s.get("status", "")).strip().lower() == "open" and s.get("open_date"):
                    try:
                        ods.append(pd.to_datetime(s["open_date"]).date())
                    except Exception:
                        pass
            if ods:
                d0 = min(ods)
                first_trade = str(d0)[:10]
                last_trade = str(date.today())

        # Stg placed-fill count for hero. Prefer it when history exists so a
        # lagging positions_summary cannot show 0; DRIPs / cash dividends
        # are not trades the user placed (see _count_placed_fills).
        _fills = _count_placed_fills(trades_pre_leg)
        _n_legs = (
            (len(closed_legs_pre_leg) if not closed_legs_pre_leg.empty else 0)
            + (len(closed_equity_pre_leg) if not closed_equity_pre_leg.empty else 0)
            + (len(current_df) if not current_df.empty else 0)
        )
        if _fills > 0:
            trade_count = _fills
        elif trade_count == 0 and _n_legs > 0:
            trade_count = _n_legs

        # Win/loss: from filtered closed legs when leg-filtered; otherwise from all
        # symbol closed legs (positions_summary is wrong when open rows mask closed stats).
        if leg_param and _leg_ranges:
            opt_wins = int((closed_legs_df["total_pnl"] > 0).sum()) if not closed_legs_df.empty and "total_pnl" in closed_legs_df.columns else 0
            opt_losses = int((closed_legs_df["total_pnl"] <= 0).sum()) if not closed_legs_df.empty and "total_pnl" in closed_legs_df.columns else 0
            eq_wins = int((closed_equity_df["realized_pnl"] > 0).sum()) if not closed_equity_df.empty and "realized_pnl" in closed_equity_df.columns else 0
            eq_losses = int((closed_equity_df["realized_pnl"] <= 0).sum()) if not closed_equity_df.empty and "realized_pnl" in closed_equity_df.columns else 0
            total_winners = opt_wins + eq_wins
            total_losers = opt_losses + eq_losses
            total_closed = total_winners + total_losers
        elif (not closed_legs_pre_leg.empty) or (not closed_equity_pre_leg.empty):
            opt_wins = int((closed_legs_pre_leg["total_pnl"] > 0).sum()) if not closed_legs_pre_leg.empty and "total_pnl" in closed_legs_pre_leg.columns else 0
            opt_losses = int((closed_legs_pre_leg["total_pnl"] <= 0).sum()) if not closed_legs_pre_leg.empty and "total_pnl" in closed_legs_pre_leg.columns else 0
            eq_wins = int((closed_equity_pre_leg["realized_pnl"] > 0).sum()) if not closed_equity_pre_leg.empty and "realized_pnl" in closed_equity_pre_leg.columns else 0
            eq_losses = int((closed_equity_pre_leg["realized_pnl"] <= 0).sum()) if not closed_equity_pre_leg.empty and "realized_pnl" in closed_equity_pre_leg.columns else 0
            total_winners = opt_wins + eq_wins
            total_losers = opt_losses + eq_losses
            total_closed = total_winners + total_losers

        avg_days_val = float(summary_df["avg_days_in_trade"].mean()) if not summary_df.empty else 0.0
        if pd.isna(avg_days_val):
            avg_days_val = 0.0
        if (not closed_legs_pre_leg.empty) and "days_in_trade" in closed_legs_pre_leg.columns:
            d_alt = float(closed_legs_pre_leg["days_in_trade"].fillna(0).mean() or 0.0)
            if d_alt > 0 and avg_days_val == 0.0:
                avg_days_val = d_alt

        div_income = (
            float(summary_df["total_dividend_income"].sum()) if not summary_df.empty else 0.0
        )

        kpis = {
            "total_return": realized_for_display + unrealized_from_summary + div_income,
            "realized_pnl": realized_for_display,
            "unrealized_pnl": unrealized_from_summary,
            "premium_collected": premium_collected,
            "premium_paid": premium_paid,
            "dividend_income": div_income,
            "win_rate": total_winners / total_closed if total_closed else 0,
            "avg_days": avg_days_val,
            "total_trades": trade_count,
            "num_winners": total_winners,
            "num_losers": total_losers,
            "first_trade": first_trade,
            "last_trade": last_trade,
        }

    # Strategy rows.
    #
    # Two distinct data paths because the question "what are my strategy
    # results?" has different right answers depending on scope:
    #
    #  • No leg filter (whole symbol) — positions_summary is the source of
    #    truth, supplemented from int_strategy_classification when the mart
    #    lags by a dbt run (common right after a sync/CSV seed write).
    #
    #  • Leg-filtered — positions_summary CANNOT be used. It aggregates per
    #    (account, symbol, strategy) across the entire symbol history, so its
    #    per-strategy P&L, trade count, win-rate are full-symbol numbers
    #    that don't move when you click a leg pill (which is exactly the
    #    "Strategy Breakdown didn't update" bug). Rebuild the strategy
    #    rollup from int_strategy_classification rows whose open_date falls
    #    inside the selected leg(s) — same grain as positions_summary, but
    #    scoped correctly. Also skip the supplement step (it would re-inject
    #    full-history numbers).
    #
    # See `_compute_breakdown_by_type`'s gate comment — `is None` means
    # admin and must NOT short-circuit.
    # Empty list still short-circuits (genuine "no tenants" state).
    if leg_param and _leg_ranges:
        summary_for_strat = pd.DataFrame()
        if tenant_scope is None or len(tenant_scope) > 0:
            int_raw = _fetch_int_strategy_classification_by_symbol(
                client, safe_symbol, tenant_scope
            )
            if not int_raw.empty and "open_date" in int_raw.columns:
                int_raw = int_raw.copy()
                int_raw["_od"] = pd.to_datetime(int_raw["open_date"]).dt.date
                int_raw = int_raw[int_raw["_od"].apply(_in_leg_range)].drop(
                    columns=["_od"]
                )
                if not int_raw.empty:
                    summary_for_strat = _rollup_int_strategy_to_summary_shape(int_raw)
    else:
        summary_for_strat = summary_df
        if tenant_scope is None or len(tenant_scope) > 0:
            int_raw = _fetch_int_strategy_classification_by_symbol(
                client, safe_symbol, tenant_scope
            )
            if not int_raw.empty:
                rolled = _rollup_int_strategy_to_summary_shape(int_raw)
                if not rolled.empty:
                    summary_for_strat = _supplement_summary_with_rolled(
                        summary_for_strat, rolled
                    )
    _cl_for_strat = closed_legs_pre_leg if not leg_param else closed_legs_df
    _eq_for_strat = closed_equity_pre_leg if not leg_param else closed_equity_df
    merged_strategy_df = _merge_position_strategy_breakdown(
        safe_symbol, summary_for_strat, _cl_for_strat, _eq_for_strat
    )
    if merged_strategy_df.empty and not current_df.empty:
        syn = _synthetic_open_strategy_from_current(current_df)
        if not syn.empty:
            merged_strategy_df = syn
    strategy_rows = (
        merged_strategy_df.to_dict(orient="records")
        if not merged_strategy_df.empty
        else []
    )

    # Disambiguate the Strategy Breakdown's Account column by tenant_id.
    # N physical accounts can share one broker `account` string (e.g. 5
    # SnapTrade "Schwab Account" tenants), so labeling off the account
    # string collapses them all to a single nickname (last-write-wins in
    # `_account_label_map`) — the page then renders 5 identical-looking
    # "Sara Investment" rows for what are really 5 distinct accounts
    # (Emmory / Sara 401k / Sara Investment / Cameron Investment /
    # Cameron 401k). Resolve the label off the broker-stable tenant_id so
    # each row shows its own nickname; fall back to the raw account label
    # when no per-tenant mapping exists (admin browsing, synthesized
    # cross-tenant closed rows that carry no tenant_id).
    _tenant_labels = _tenant_label_map_for_user(getattr(current_user, "id", None))
    for _sr in strategy_rows:
        _tid = _sr.get("tenant_id")
        _lbl = _tenant_labels.get(_tid) if _tid else None
        _sr["account_display"] = _lbl or _norm_account_label(_sr.get("account"))

    # ── Breakdown by type (equity / options / dividends) ──
    # Sums roll up across the selected legs (or the whole symbol when no
    # leg filter is active). Sources:
    #   - equity realized:    closed_equity_df (already leg-filtered)
    #   - equity unrealized:  current_df rows where instrument_type='Equity'
    #   - options realized:   closed_legs_df (already leg-filtered)
    #   - options unrealized: current_df rows where instrument_type in (Call, Put)
    #   - dividends:          int_dividend_events filtered by leg date range
    # See _compute_breakdown_by_type for the full contract.
    breakdown_rows = _compute_breakdown_by_type(
        client=client,
        safe_symbol=safe_symbol,
        tenant_scope=tenant_scope,
        closed_equity_df=closed_equity_df,
        closed_legs_df=closed_legs_df,
        current_df=current_df,
        leg_predicate=(_in_leg_range if (leg_param and _leg_ranges) else None),
        dividends_df=dividends_df,
    )

    # Headline KPI used ``Σ positions_summary.total_dividend_income`` + realized
    # frames + unreal — but Breakdown-by-type / mart chart fold dividends from
    # ``int_dividend_events`` (synthesised ex-div × holdings etc.). Those streams
    # can materially diverge (~12k on BE Schwab •••0044): hero read low while
    # ledger + chart agreed. Pin hero ``total_return`` to the same Σ as the card
    # above Strategy Breakdown so reconciliation and user trust aren't split.
    if kpis and breakdown_rows:
        ledger_total = sum(float(r.get("total") or 0) for r in breakdown_rows)
        kpis["total_return"] = round(ledger_total, 2)
        for _br in breakdown_rows:
            if str(_br.get("type") or "") == "Dividends":
                kpis["dividend_income"] = round(float(_br.get("total") or 0), 2)
                break

    # Build chart data from pre-aggregated mart_daily_pnl
    chart_data = {"dates": [], "equity": [], "options": [], "dividends": [], "total": [], "underlying_price": [], "has_underlying_price": False}
    prices_through_date = None
    try:
        # chart_df was fetched in the parallel batch above (was a serial
        # ~2s BQ round trip here). Everything below is unchanged Python
        # post-processing on the same frame.
        chart_df = _filter_df_by_tenant_ids(chart_df, tenant_scope)
        chart_df = _narrow_mart_daily_pnl_chart_df_to_summary_tenant(
            chart_df, summary_df
        )
        # Filter chart data by selected session date ranges and re-zero cumulative columns
        if leg_param and _leg_ranges and not chart_df.empty and "date" in chart_df.columns:
            chart_df["_d"] = pd.to_datetime(chart_df["date"]).dt.date
            chart_df = chart_df[chart_df["_d"].apply(_in_leg_range)].copy()
            chart_df = chart_df.drop(columns=["_d"])
            if not chart_df.empty:
                # Re-zero cumulative columns relative to the leg's
                # first day so the chart starts at $0 inside the
                # filtered window. ``cumulative_options_pnl`` is now
                # realize-on-close cumulative (see mart_daily_pnl
                # header) — its baseline subtraction still produces a
                # well-defined "delta during this leg" series.
                for cum_col in (
                    "cumulative_options_pnl",
                    "cumulative_dividends_pnl",
                    "cumulative_other_pnl",
                ):
                    if cum_col in chart_df.columns:
                        baseline = float(chart_df[cum_col].iloc[0] or 0)
                        chart_df[cum_col] = chart_df[cum_col].astype(float) - baseline
                # Open MTM and snapshot diagnostics cover ALL open
                # options for the symbol, not just those in the
                # selected leg. Zero them out so the chart's
                # within-leg series isn't inflated by other legs'
                # open contracts. Realized contributions inside the
                # leg window are still attributed via the rezeroed
                # cumulative.
                for col in (
                    "open_options_unrealized_pnl",
                    "option_market_value",
                    "option_cost_basis",
                ):
                    if col in chart_df.columns:
                        chart_df[col] = 0 if col == "open_options_unrealized_pnl" else None
        if not chart_df.empty:
            # Cache the computed chart payload keyed on the (tenant- and
            # leg-scoped) input frames + today. The equity P&L walk is a
            # heavy row-by-row Python state machine; on a warm cache we skip
            # it and only pay the vectorized fingerprint hash. Tenant-safe:
            # the key is a content hash of the already tenant-scoped inputs.
            _chart_key = (
                "pos_chart",
                str(date.today()),
                frame_fingerprint(chart_df, current_df),
            )
            with timed("chart"):
                chart_data = cached_payload(
                    _chart_key,
                    lambda: _build_chart_from_daily_pnl(chart_df, current_df),
                )
            # Latest date we have close_price for (from pipeline); user can run current_position_stock_price.py to refresh
            if "date" in chart_df.columns:
                prices_through_date = str(chart_df["date"].max())[:10]
    except Exception as exc:
        app.logger.exception(
            "position_detail chart query or build failed for %s: %s", safe_symbol, exc
        )

    # Prefer stg/leg when mart is unusably short — but NEVER replace a mart chart
    # whose terminal agrees with KPI with ``_cumulative_pnl_from_*`` substitutes.
    #
    # Those substitutes are legacy cash-close stepping (only closed legs / raw
    # stg HISTORY amounts): they omit open unrealized MTM, realize-on-close option
    # shape, ``int_dividend_events``, etc. After a Schwab sync, ``trades_pre_leg``
    # often spans *more calendar days than mart_daily_pnl* while the mart spine
    # still reconciles KPI + breakdown. The naive rule ``best_n > n_m`` then
    # threw away the correct mart series (~\$85k) for a truncated cash ladder
    # (~\$20k) — reconciliation invariant explosion (May 2026 BE).
    _chart_dates = chart_data.get("dates") or []
    n_m = len(_chart_dates)
    kp_ref = float(kpis.get("total_return") or 0) if kpis else None
    mart_term = _chart_data_terminal(chart_data)

    ch_stg = (
        _cumulative_pnl_from_stg_trades(trades_pre_leg, current_df)
        if not trades_pre_leg.empty else None
    )
    n_stg = len(ch_stg["dates"]) if ch_stg and ch_stg.get("dates") else 0
    ch_leg = _cumulative_pnl_from_leg_closes(closed_legs_pre_leg, closed_equity_pre_leg)
    n_leg = len(ch_leg["dates"]) if ch_leg and ch_leg.get("dates") else 0

    cands_src = []
    if ch_leg and n_leg >= 2:
        cands_src.append(("leg", ch_leg, n_leg))
    if ch_stg and n_stg >= 2:
        cands_src.append(("stg", ch_stg, n_stg))

    if cands_src:
        # Tie-break: prefer candidates with more x-points, leg path over stg.
        cands_src.sort(key=lambda t: (-t[2], 0 if t[0] == "leg" else 1))
        _, cand_data, best_n = cands_src[0]
        cand_term = _chart_data_terminal(cand_data)
        mart_useless = n_m <= 2
        substitute = False

        if mart_useless:
            # Mart spine is insufficient — pick whichever substitute lands closest to
            # KPI (prefer longer tie-break among equally-close substitutes).
            if kp_ref is not None:
                scored = []
                for _nm, cd, bn in cands_src:
                    g = abs(_chart_data_terminal(cd) - kp_ref)
                    scored.append((g, -bn, 0 if _nm == "leg" else 1, cd))
                scored.sort(key=lambda z: z[:3])
                chart_data = scored[0][3]
            else:
                chart_data = cand_data
        elif kp_ref is not None:
            gap_mart_k = abs(mart_term - kp_ref)
            gap_cand_k = abs(cand_term - kp_ref)
            materially_better_cand = gap_cand_k + 5 < gap_mart_k
            extended_but_not_worse = (
                best_n > n_m
                and gap_cand_k <= gap_mart_k + CHART_SUBSTITUTION_KPI_MARGIN
                and gap_cand_k
                <= max(250.0, 0.01 * max(abs(kp_ref), 1.0))
            )
            substitute = materially_better_cand or extended_but_not_worse
            # Never discard a KPI-aligned mart spine for cash-flow substitutes that
            # miss open unreal / realize-on-close / synthesized dividends (~\$65k on BE).
            if substitute and gap_cand_k > gap_mart_k + CHART_SUBSTITUTION_KPI_MARGIN:
                substitute = False
            if substitute:
                chart_data = cand_data

    # Chart.js needs at least two x values to draw a line; a single mart day
    # (e.g. new option leg) would otherwise show only a blank chart.
    _chart_dates = chart_data.get("dates") or []
    if kpis and (not _chart_dates or len(_chart_dates) < 2):
        chart_data = _synthetic_cumulative_pnl_for_position(
            kpis, sessions_list, leg_param, selected_legs, current_df
        )

    if kpis:
        _align_position_pnl_chart_with_kpi(chart_data, kpis)
        _snap_position_chart_terminal_to_breakdown(
            chart_data, breakdown_rows
        )

    # Trade history rows
    trades_for_table = trades_df.copy()
    if "trade_date" in trades_for_table.columns:
        trades_for_table["trade_date"] = trades_for_table["trade_date"].astype(str)
    trades = trades_for_table.to_dict(orient="records") if not trades_for_table.empty else []
    # Disambiguate each trade's Account cell by tenant_id (same reason as the
    # Strategy Breakdown — all of a user's "Schwab Account" tenants share one
    # broker label). `_tenant_labels` was built above for strategy_rows.
    for _t in trades:
        _tid = _t.get("tenant_id")
        _t["account_display"] = (
            (_tenant_labels.get(_tid) if _tid else None)
            or _norm_account_label(_t.get("account"))
        )

    # Current positions
    current_positions = current_df.to_dict(orient="records") if not current_df.empty else []
    for _p in current_positions:
        _tid = _p.get("tenant_id")
        _p["account_display"] = (
            (_tenant_labels.get(_tid) if _tid else None)
            or _norm_account_label(_p.get("account"))
        )

    # ── Closed option legs (with cost/proceeds) ──
    closed_legs_list = []
    if not closed_legs_df.empty:
        closed_legs_list = closed_legs_df.sort_values("close_date").to_dict(orient="records")
        for r in closed_legs_list:
            r["open_date"] = str(r["open_date"]) if pd.notna(r.get("open_date")) else ""
            r["close_date"] = str(r["close_date"]) if pd.notna(r.get("close_date")) else ""
            r["total_pnl"] = round(float(r.get("total_pnl") or 0), 2)

    # ── Closed equity legs ──
    closed_equity_list = []
    if not closed_equity_df.empty:
        closed_equity_list = closed_equity_df.sort_values("close_date").to_dict(orient="records")
        for r in closed_equity_list:
            r["open_date"] = str(r["open_date"]) if pd.notna(r.get("open_date")) else ""
            r["close_date"] = str(r["close_date"]) if pd.notna(r.get("close_date")) else ""
            r["realized_pnl"] = round(float(r.get("realized_pnl") or 0), 2)

    # ── Trade Outcomes ──
    trade_outcomes = []
    for leg in closed_legs_list:
        direction = str(leg.get("direction") or "")
        prem_recv = float(leg.get("premium_received") or 0)
        prem_paid = float(leg.get("premium_paid") or 0)
        cost_close = float(leg.get("cost_to_close") or 0)
        proceeds_close = float(leg.get("proceeds_from_close") or 0)
        if direction == "Sold":
            o_cost = abs(cost_close)
            o_proceeds = abs(prem_recv)
        else:
            o_cost = abs(prem_paid)
            o_proceeds = abs(proceeds_close)
        o_pnl = float(leg.get("total_pnl") or 0)
        o_return = round(o_pnl / o_cost * 100, 1) if o_cost else None
        trade_outcomes.append({
            "trade_symbol": leg.get("trade_symbol"),
            "strategy": leg.get("strategy") or "",
            "direction": direction,
            "close_type": str(leg.get("close_type") or ""),
            "open_date": leg.get("open_date") or "",
            "close_date": leg.get("close_date") or "",
            "days_held": leg.get("days_in_trade"),
            "quantity": leg.get("quantity"),
            "cost": round(o_cost, 2),
            "proceeds": round(o_proceeds, 2),
            "pnl": round(o_pnl, 2),
            "return_pct": o_return,
            "is_winner": o_pnl > 0,
            "type": "option",
            "tenant_id": leg.get("tenant_id"),
            "account": str(leg.get("account") or "").strip(),
        })
    for leg in closed_equity_list:
        eq_proceeds = float(leg.get("sell_proceeds") or 0)
        eq_cost = float(leg.get("cost_basis") or 0)
        eq_pnl = float(leg.get("realized_pnl") or 0)
        eq_return = round(eq_pnl / eq_cost * 100, 1) if eq_cost else None
        od = leg.get("open_date") or ""
        cd = leg.get("close_date") or ""
        try:
            days = (pd.to_datetime(cd) - pd.to_datetime(od)).days if od and cd else None
        except Exception:
            days = None
        trade_outcomes.append({
            "trade_symbol": leg.get("trade_symbol") or symbol,
            "strategy": leg.get("description") or "Equity Sold",
            "direction": "Sold",
            "close_type": "Sold",
            "open_date": od,
            "close_date": cd,
            "days_held": days,
            "quantity": leg.get("quantity"),
            "cost": round(eq_cost, 2),
            "proceeds": round(eq_proceeds, 2),
            "pnl": round(eq_pnl, 2),
            "return_pct": eq_return,
            "is_winner": eq_pnl > 0,
            "type": "equity",
            "session_id": leg.get("session_id"),
            "tenant_id": leg.get("tenant_id"),
            "account": str(leg.get("account") or "").strip(),
        })
    trade_outcomes.sort(key=lambda x: x.get("close_date") or "", reverse=True)
    for _o in trade_outcomes:
        _tid = _o.get("tenant_id")
        _o["account_display"] = (
            (_tenant_labels.get(_tid) if _tid else None)
            or _norm_account_label(_o.get("account"))
        )

    # Attach raw transactions to each outcome for drill-down
    # Build session date range lookup for scoping equity trades
    _session_ranges = {}
    for s in sessions_list:
        sid = s.get("session_id")
        if sid is not None:
            s_od = pd.to_datetime(s["open_date"]).date() if s.get("open_date") else None
            s_ltd = pd.to_datetime(s["last_trade_date"]).date() if s.get("last_trade_date") else None
            s_open = str(s.get("status", "")).strip().lower() == "open"
            _session_ranges[sid] = (s_od, s_ltd if not s_open else date.today())

    trades_by_symbol = {}
    for t in trades:
        ts = str(t.get("trade_symbol") or "")
        trades_by_symbol.setdefault(ts, []).append(t)

    for o in trade_outcomes:
        ts = str(o.get("trade_symbol") or "")
        if o["type"] == "option":
            matching = trades_by_symbol.get(ts, [])
        else:
            sid = o.get("session_id")
            s_range = _session_ranges.get(sid)
            matching = _equity_raw_trades_for_partial_close_outcome(
                trades,
                trade_symbol=ts,
                account=str(o.get("account") or "").strip(),
                session_range=s_range,
                close_milestone=o.get("close_date"),
            )
        o["raw_trades"] = matching

    # Assign leg numbers to trade outcomes and open positions
    def _date_to_leg(d_str):
        """Return display_leg number for a date string, or None.
        Prefers equity sessions over orphan (options-only) sessions to avoid
        the orphan's wide date range swallowing trades that belong to a real session."""
        if not d_str or not sessions_list:
            return None
        try:
            d = pd.to_datetime(d_str).date()
        except Exception:
            return None
        # First pass: check equity sessions (non-orphan)
        for s in sessions_list:
            if s.get("options_only"):
                continue
            s_od = pd.to_datetime(s["open_date"]).date() if s.get("open_date") else None
            s_ltd = pd.to_datetime(s["last_trade_date"]).date() if s.get("last_trade_date") else None
            s_open = str(s.get("status", "")).strip().lower() == "open"
            s_end = s_ltd if not s_open else date.today()
            if s_od and s_end and s_od <= d <= s_end:
                return s["display_leg"]
        # Second pass: fall back to orphan (options-only) sessions
        for s in sessions_list:
            if not s.get("options_only"):
                continue
            s_od = pd.to_datetime(s["open_date"]).date() if s.get("open_date") else None
            s_ltd = pd.to_datetime(s["last_trade_date"]).date() if s.get("last_trade_date") else None
            s_end = s_ltd or date.today()
            if s_od and s_end and s_od <= d <= s_end:
                return s["display_leg"]
        return None

    for o in trade_outcomes:
        o["leg_num"] = _date_to_leg(o.get("open_date") or o.get("close_date"))
    # ``int_closed_equity_legs`` emits one outcome row per sell inside the same
    # equity chapter; merged ``int_position_legs`` assigns one display leg for that
    # whole span → every partial closure gets the SAME leg_num. Label partials so
    # it reads as intentional (one chapter, sequential exits), not buggy duplication.
    _eq_sess = {}
    for o in trade_outcomes:
        if o.get("type") != "equity" or o.get("session_id") is None:
            continue
        k = (o.get("account"), o["session_id"])
        _eq_sess.setdefault(k, []).append(o)
    for lst in _eq_sess.values():
        lst_chrono = sorted(lst, key=lambda x: x.get("close_date") or "")
        n = len(lst_chrono)
        for i, o in enumerate(lst_chrono, start=1):
            o["equity_partial_ix"] = i
            o["equity_partial_n"] = n
    for p in current_positions:
        # Open positions belong to the latest open session
        open_sessions = [s for s in sessions_list if str(s.get("status", "")).strip().lower() == "open"]
        p["leg_num"] = open_sessions[-1]["display_leg"] if open_sessions else (sessions_list[-1]["display_leg"] if sessions_list else None)

    # ── Option matrices (DTE × Strike Distance heatmap) ──
    # (tenant scope already narrowed matrix_df to the selected account's tenant)
    # Filter matrix by selected legs (date range overlap via trade_symbol matching closed legs)
    if leg_param and _leg_ranges and not matrix_df.empty:
        filtered_trade_syms = set(r.get("trade_symbol") for r in closed_legs_list)
        if "trade_symbol" in matrix_df.columns:
            matrix_df = matrix_df[matrix_df["trade_symbol"].isin(filtered_trade_syms)]
    # matrix_df is tenant-scoped to the current ?account/?tenant selection
    # upstream, so the matrices honor the filter by construction.
    with timed("matrix"):
        option_matrices = (
            _build_option_matrices(matrix_df, symbol) if not matrix_df.empty else []
        )

    # Available accounts for filter. Non-admin: the full disambiguated
    # account set so each physical account (incl. colliding "Schwab
    # Account"s) is selectable even after tenant scope narrowed the data
    # to one. Admin: data-derived (summary may be empty for open-only lots).
    if user_accounts:
        all_accounts = sorted(user_accounts)
    elif not summary_df.empty and "account" in summary_df.columns:
        all_accounts = sorted(summary_df["account"].dropna().unique())
    elif not current_df.empty and "account" in current_df.columns:
        all_accounts = sorted(current_df["account"].dropna().unique())
    else:
        all_accounts = []

    # Sector / subsector: take the first non-Unknown value we can find from
    # either summary or current. Both sources are joined to stg_symbol_metadata
    # in dbt, so they should agree — falling through is just defensive.
    def _first_nonempty(df_, col):
        if df_ is None or df_.empty or col not in df_.columns:
            return ""
        vals = df_[col].dropna().astype(str).str.strip()
        vals = vals[(vals != "") & (vals.str.lower() != "unknown")]
        if vals.empty:
            # Fall back to whatever we have, including 'Unknown', so the UI
            # can still render a label rather than nothing.
            any_vals = df_[col].dropna().astype(str).str.strip()
            return any_vals.iloc[0] if not any_vals.empty else ""
        return vals.iloc[0]

    symbol_sector = _first_nonempty(summary_df, "sector") or _first_nonempty(current_df, "sector")
    symbol_subsector = _first_nonempty(summary_df, "subsector") or _first_nonempty(current_df, "subsector")
    symbol_company = _first_nonempty(summary_df, "company_name") or _first_nonempty(current_df, "company_name")

    # Next-earnings pill for the hero. dict form: {"date": "YYYY-MM-DD",
    # "display": "Tue Jun 15", "days_until": 28} or None if the symbol
    # has no upcoming earnings (ETFs, indices, crypto, or a symbol whose
    # last yfinance fetch returned no calendar). Template hides the pill
    # entirely when None — no "NaT" / "None" leaks to the UI.
    symbol_next_earnings = None
    try:
        if earnings_df is not None and not earnings_df.empty:
            erow = earnings_df.iloc[0]
            ed = erow.get("next_earnings_date")
            if ed is not None and not (hasattr(ed, "__float__") and pd.isna(ed)):
                ed_date = ed.date() if hasattr(ed, "date") and not isinstance(ed, date) else ed
                # Day count vs the USER's today (profile tz), not SQL's UTC
                # CURRENT_DATE() — see POSITION_EARNINGS_QUERY comment.
                try:
                    days_until = (ed_date - user_local_today()).days
                except TypeError:
                    days_until = None
                if days_until is None or days_until >= 0:
                    symbol_next_earnings = {
                        "date": ed_date.strftime("%Y-%m-%d") if hasattr(ed_date, "strftime") else str(ed_date)[:10],
                        "display": ed_date.strftime("%a %b %-d") if hasattr(ed_date, "strftime") else str(ed_date)[:10],
                        "days_until": days_until,
                    }
    except Exception:
        symbol_next_earnings = None

    # Cross-source reconciliation invariant.
    #
    # Σ strategy_rows.total_pnl is NOT a reliable ledger rollup — attribution
    # spreads equity realization across strategies (Wheel, CSP, Dividend/Buy &
    # Hold, …). Summing labeled rows may disagree with ledger paths while still
    # being "correct by label" (May 2026 BE: breakdown ≈ chart; strategy rows
    # lower by ~ dividends + equity credited elsewhere).
    #
    # Compare three full-symbol measures grounded in fills + mart spine:
    #   - Hero KPI total_return — realized (+ unreal + Σ summary dividends).
    #   - Breakdown by Type — Σ equity/options/dividend rollups above Strategy.
    #   - Chart terminal — mart_daily_pnl walk.
    #
    # Partition drift (Σ strategies vs KPI) logs at INFO for debugging only.
    invariant_warning = None
    try:
        strategy_partition_sum = round(
            sum(float(r.get("total_pnl") or 0) for r in strategy_rows), 2
        )
        kpi_total = round(float(kpis.get("total_return") or 0), 2) if kpis else 0.0
        # ``breakdown_rows`` dicts come from ``_compute_breakdown_by_type``,
        # which emits ``"total"`` (not ``"total_pnl"`` — that key belongs to the
        # strategy_rows shape from positions_summary).
        bt_total = round(sum(float(r.get("total") or 0) for r in breakdown_rows), 2)
        chart_terminal = round(float((chart_data.get("total") or [0.0])[-1] or 0.0), 2)
        if abs(strategy_partition_sum - kpi_total) > 1.0:
            app.logger.info(
                "position_detail strategy partition sum vs KPI: %s/%s "
                "partition=%.2f kpi=%.2f (labels need not match ledger rollups)",
                selected_account or "ALL",
                safe_symbol,
                strategy_partition_sum,
                kpi_total,
            )
        # Skip when the by-type card didn't render — nothing to reconcile.
        if breakdown_rows:
            worst_gap = max(
                abs(kpi_total - bt_total),
                abs(bt_total - chart_terminal),
                abs(kpi_total - chart_terminal),
            )
            if worst_gap > 1.0:
                invariant_warning = {
                    "hero_total_return": kpi_total,
                    "breakdown_by_type_total": bt_total,
                    "chart_terminal": chart_terminal,
                    "worst_gap": round(worst_gap, 2),
                }
                app.logger.warning(
                    "position_detail invariant: %s/%s ledger totals disagree — "
                    "kpi=%.2f, breakdown_by_type=%.2f, chart_terminal=%.2f (gap=%.2f)",
                    selected_account or "ALL",
                    safe_symbol,
                    kpi_total,
                    bt_total,
                    chart_terminal,
                    worst_gap,
                )
    except Exception as exc:
        # Invariant computation must never break the page render. Log and move
        # on — the worst case here is "no canary" not "broken page".
        app.logger.exception(
            "position_detail invariant calc failed for %s: %s", safe_symbol, exc
        )

    # Build the symbol tab strip payload from the lightweight `tabs_df`
    # rollup. One row per (symbol) — when the user spans multiple accounts we
    # collapse so each ticker shows up once in the strip with combined P&L
    # and trade count, and "open" wins over "closed" for the dot.
    tabs = []
    if not tabs_df.empty:
        tdf = tabs_df.copy()
        for col in ("total_return", "num_trades"):
            if col in tdf.columns:
                tdf[col] = pd.to_numeric(tdf[col], errors="coerce").fillna(0)
        if "has_open_leg" in tdf.columns:
            tdf["has_open_leg"] = pd.to_numeric(tdf["has_open_leg"], errors="coerce").fillna(0).astype(int)
        # Collapse to one row per symbol across the in-scope accounts.
        agg_funcs = {
            "total_return": "sum",
            "num_trades": "sum",
            "has_open_leg": "max",
        }
        if "strategies_pipe" in tdf.columns:
            agg_funcs["strategies_pipe"] = lambda s: "|".join(sorted({
                p for v in s.dropna() for p in str(v).split("|") if p
            }))
        if "account" in tdf.columns:
            agg_funcs["account"] = lambda s: ", ".join(sorted({str(v) for v in s.dropna() if str(v).strip()}))
        rolled = tdf.groupby("symbol", as_index=False).agg(agg_funcs)
        rolled = rolled.sort_values("total_return", ascending=False)
        for r in rolled.to_dict(orient="records"):
            strats_pipe = str(r.get("strategies_pipe") or "")
            tabs.append({
                "symbol": r.get("symbol"),
                "account": r.get("account") or "",
                "total_return": round(float(r.get("total_return") or 0.0), 2),
                "num_trades": int(r.get("num_trades") or 0),
                "status": "Open" if int(r.get("has_open_leg") or 0) else "Closed",
                "strategies": [s for s in strats_pipe.split("|") if s] if strats_pipe else [],
            })

    # Tab strip uses navigate-mode anchors. Preserve ?account= so the
    # destination page stays in the same scope (admin + non-admin tenancy
    # reasoning above continues to hold).
    tab_qs = ""
    if selected_account:
        tab_qs = "?account=" + quote_plus(selected_account)

    # ── "History starts here" disclosure ─────────────────────────────
    # One row per account whose imported history begins mid-position
    # (int_opening_balances synthesized an opening fill). The share count
    # is provable; the COST is estimated — the banner says which method
    # priced it and offers the CSV-upload path to replace the estimate
    # with real fills. Already tenant-filtered above.
    opening_balances = []
    try:
        if opening_df is not None and not opening_df.empty:
            for _, ob in opening_df.iterrows():
                qty = float(ob.get("opening_qty") or 0.0)
                if qty <= 0:
                    continue
                opening_balances.append({
                    "account": str(ob.get("account") or ""),
                    "qty": round(qty, 2),
                    "first_trade_date": ob.get("first_trade_date"),
                    "est_cost": round(abs(float(ob.get("est_amount") or 0.0)), 2),
                    "price_source": str(ob.get("price_source") or ""),
                })
    except Exception as exc:
        app.logger.warning("opening-balance banner build failed for %s: %s", symbol, exc)
        opening_balances = []

    # ── Story mode: narrative timeline + chart event markers ─────────
    # Built from the ALREADY tenant- and leg-filtered trades_df; dividends
    # get the same tenant + leg treatment here (the raw batch frame is
    # unfiltered — _compute_breakdown_by_type filters its own copy).
    try:
        _story_div_df = _filter_df_by_tenant_ids(
            dividends_df if dividends_df is not None else pd.DataFrame(),
            tenant_scope,
        )
        if leg_param and not _story_div_df.empty and "trade_date" in _story_div_df.columns:
            _story_div_df = _story_div_df.copy()
            _story_div_df["_d"] = pd.to_datetime(_story_div_df["trade_date"]).dt.date
            _story_div_df = _story_div_df[_story_div_df["_d"].apply(_in_leg_range)]
        # Execution review: after-the-fact verdicts graded in dbt
        # (int_option_exit_quality). Tenant-filtered like every other
        # frame; the notes hook onto completing closes inside the engine.
        _exec_df = _filter_df_by_tenant_ids(execution_df, tenant_scope)
        _exit_notes = _execution_exit_notes(_exec_df)
        story_days, story_markers, story_stats = build_position_story(
            trades_df,
            _story_div_df,
            chart_data,
            splits_df=splits_df,
            seed_trades_df=story_seed_trades,
            exit_notes=_exit_notes,
            label_map=_tenant_label_map,
        )
        # The mirror prologue: how this position was traded + where it
        # sits in the trader's book. Rank comes from the tab-strip rollup
        # (already tenant-scoped) so the mirror needs no extra query.
        book_rank, book_size = None, None
        ranked = sorted(
            (t for t in tabs if t.get("total_return") is not None),
            key=lambda t: t["total_return"], reverse=True,
        )
        for i, t in enumerate(ranked):
            if str(t.get("symbol") or "").upper() == symbol.upper():
                book_rank, book_size = i + 1, len(ranked)
                break
        story_mirror = compose_mirror(story_stats, symbol, book_rank, book_size)
        # Execution sentences extend the mirror: same evidence-only voice,
        # but graded against the market's record instead of the fills.
        story_mirror = story_mirror + _symbol_execution_sentences(_exec_df)
    except Exception as exc:
        app.logger.warning("position story build failed for %s: %s", symbol, exc)
        story_days, story_markers, story_mirror = [], [], []

    return render_template(
        "position_detail.html",
        symbol=symbol,
        kpis=kpis,
        overall_status=overall_status,
        strategy_rows=strategy_rows,
        breakdown_rows=breakdown_rows,
        trades=trades,
        trade_outcomes=trade_outcomes,
        current_positions=current_positions,
        option_matrices=option_matrices,
        sessions=sessions_list,
        legs_by_account=legs_by_account,
        all_user_tags=all_user_tags,
        account_toggles=account_toggles,
        tenants_param=tenants_param,
        selected_legs=selected_legs,
        leg_param=leg_param,
        chart_data_json=json.dumps(_chart_data_for_json(chart_data)),
        story_days=story_days,
        story_markers_json=json.dumps(story_markers),
        story_mirror=story_mirror,
        has_underlying_price=chart_data.get("has_underlying_price", False),
        prices_through_date=prices_through_date,
        accounts=all_accounts,
        selected_account=selected_account,
        symbol_sector=symbol_sector,
        symbol_subsector=symbol_subsector,
        symbol_company=symbol_company,
        symbol_next_earnings=symbol_next_earnings,
        invariant_warning=invariant_warning,
        opening_balances=opening_balances,
        viewer_is_admin=is_admin(current_user.username),
        tabs=tabs,
        active_symbol=symbol,
        tab_href_base="/position/",
        tab_href_suffix=tab_qs,
        mode="navigate",
    )


def _peek_num(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return f


def _peek_date(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, datetime):
        return val.date().isoformat()
    if isinstance(val, date):
        return val.isoformat()
    text = str(val).strip()
    return text[:10] if len(text) >= 10 else None


def compose_position_peek(symbol, summary_df, current_df, label_map=None,
                          today=None):
    """JSON-ready snapshot for the right-side position peek drawer.

    Rolls ``positions_summary`` (lifetime) together with live
    ``int_enriched_current`` lots so Today / Overview can show the
    position without navigating to the full page. ``label_map`` is
    tenant_id → display nickname.
    """
    from app.option_formatting import format_option_symbol

    symbol = str(symbol or "").strip().upper()
    label_map = label_map or {}
    today = today or date.today()
    empty = {
        "symbol": symbol,
        "company_name": None,
        "status": None,
        "strategies": [],
        "total_pnl": 0.0,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "dividends": 0.0,
        "fills": 0,
        "winners": 0,
        "losers": 0,
        "win_rate": None,
        "first_trade": None,
        "last_trade": None,
        "accounts": [],
        "holdings": [],
        "position_url": url_for("position_detail", symbol=symbol),
    }
    summary = summary_df if summary_df is not None else pd.DataFrame()
    current = current_df if current_df is not None else pd.DataFrame()

    strategies = []
    seen_strat = set()
    statuses = set()
    total_pnl = realized = unrealized = dividends = 0.0
    fills = winners = losers = 0
    first_trade = last_trade = None
    company_name = None
    per_account = {}

    if not summary.empty:
        for _, r in summary.iterrows():
            strat = str(r.get("strategy") or "").strip()
            if strat and strat not in seen_strat:
                seen_strat.add(strat)
                strategies.append(strat)
            st = str(r.get("status") or "").strip()
            if st:
                statuses.add(st)
            total_pnl += _peek_num(r.get("total_pnl")) or 0.0
            realized += _peek_num(r.get("realized_pnl")) or 0.0
            unrealized += _peek_num(r.get("unrealized_pnl")) or 0.0
            dividends += _peek_num(r.get("total_dividend_income")) or 0.0
            fills += int(_peek_num(r.get("num_individual_trades")) or 0)
            winners += int(_peek_num(r.get("num_winners")) or 0)
            losers += int(_peek_num(r.get("num_losers")) or 0)
            fd = _peek_date(r.get("first_trade_date"))
            ld = _peek_date(r.get("last_trade_date"))
            if fd and (first_trade is None or fd < first_trade):
                first_trade = fd
            if ld and (last_trade is None or ld > last_trade):
                last_trade = ld
            if company_name is None:
                cn = r.get("company_name")
                if cn is not None and not (isinstance(cn, float) and pd.isna(cn)):
                    text = str(cn).strip()
                    if text and text.lower() != "nan":
                        company_name = text
            tid = str(r.get("tenant_id") or "").strip()
            bucket = per_account.setdefault(tid, {
                "tenant_id": tid,
                "account": label_map.get(tid, str(r.get("account") or "")),
                "total_pnl": 0.0,
                "status": st,
            })
            bucket["total_pnl"] += _peek_num(r.get("total_pnl")) or 0.0
            if st == "Open":
                bucket["status"] = "Open"

    if "Open" in statuses and ("Closed" in statuses or "Mixed" in statuses):
        status = "Mixed"
    elif "Open" in statuses:
        status = "Open"
    elif "Mixed" in statuses:
        status = "Mixed"
    elif statuses:
        status = "Closed"
    else:
        status = None

    closed = winners + losers
    win_rate = (winners / closed) if closed else None

    holdings = []
    if current is not None and not current.empty:
        current = _dedupe_enriched_current_positions(current)
        for _, r in current.iterrows():
            qty = _peek_num(r.get("quantity")) or 0.0
            if abs(qty) < 1e-9:
                continue
            itype = str(r.get("instrument_type") or "")
            is_opt = itype.lower() in ("call", "put") or itype.lower().startswith("option")
            expiry = _peek_date(r.get("option_expiry"))
            if is_opt and expiry and expiry < today.isoformat():
                continue
            tid = str(r.get("tenant_id") or "").strip()
            trade_symbol = str(r.get("trade_symbol") or "").strip()
            if is_opt:
                label = format_option_symbol(trade_symbol or symbol, with_ticker=False)
                qty_label = f"{abs(qty):.0f} ct"
            else:
                label = "Shares"
                if abs(qty - round(qty)) < 1e-6:
                    qty_label = f"{abs(round(qty)):,.0f} sh"
                else:
                    qty_label = f"{abs(qty):,.4f} sh".rstrip("0").rstrip(".") + " sh"
            holdings.append({
                "kind": "option" if is_opt else "equity",
                "label": label,
                "qty_label": qty_label,
                "quantity": qty,
                "price": _peek_num(r.get("current_price")),
                "market_value": _peek_num(r.get("market_value")),
                "cost_basis": _peek_num(r.get("cost_basis")),
                "unrealized_pnl": _peek_num(r.get("unrealized_pnl")),
                "account": label_map.get(tid, str(r.get("account") or "")),
                "tenant_id": tid,
            })
        holdings.sort(key=lambda h: (0 if h["kind"] == "equity" else 1, h["label"]))

    accounts = sorted(
        ({**b, "total_pnl": round(b["total_pnl"], 2)} for b in per_account.values()),
        key=lambda a: abs(a["total_pnl"]),
        reverse=True,
    )
    return {
        "symbol": symbol,
        "company_name": company_name,
        "status": status,
        "strategies": strategies,
        "total_pnl": round(total_pnl, 2),
        "realized_pnl": round(realized, 2),
        "unrealized_pnl": round(unrealized, 2),
        "dividends": round(dividends, 2),
        "fills": fills,
        "winners": winners,
        "losers": losers,
        "win_rate": round(win_rate, 4) if win_rate is not None else None,
        "first_trade": first_trade,
        "last_trade": last_trade,
        "accounts": accounts,
        "holdings": holdings,
        "position_url": url_for("position_detail", symbol=symbol),
    }


_PEEK_SYMBOL_RE = re.compile(r"^[A-Za-z0-9.\-]{1,16}$")


@app.route("/api/position/<symbol>/peek")
@login_required
def position_peek(symbol):
    """Lightweight JSON for the right-side position drawer on Today / Overview.

    Reuses Position Detail's summary + current queries (same tenant scope
    and cache keys) so a click does not pay for the full page build.
    """
    bounce = _redirect_if_no_accounts()
    if bounce:
        return jsonify({"error": "no_accounts"}), 403
    if not _PEEK_SYMBOL_RE.match(symbol or ""):
        return jsonify({"error": "not_found"}), 404

    safe_symbol = symbol.replace("'", "''")
    selected_account = request.args.get("account", "").strip()
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)
    client = get_bigquery_client()
    try:
        batch = _bq_parallel(client, {
            "summary": POSITION_SUMMARY_QUERY.format(
                symbol=safe_symbol, tenant_filter=tenant_filter),
            "current": POSITION_CURRENT_QUERY.format(
                symbol=safe_symbol, tenant_filter=tenant_filter),
        })
    except Exception as exc:
        _log.warning("position peek query failed for %s: %s", symbol, exc)
        return jsonify({"error": "unavailable"}), 503

    summary_df = _filter_df_by_tenant_ids(batch.get("summary"), tenant_ids)
    current_df = _filter_df_by_tenant_ids(batch.get("current"), tenant_ids)
    if (summary_df is None or summary_df.empty) and (
            current_df is None or current_df.empty):
        return jsonify({"error": "not_found"}), 404

    label_map = _tenant_label_map_for_user(current_user.id)
    payload = compose_position_peek(
        symbol, summary_df, current_df, label_map=label_map)
    qs = []
    for key in ("account", "tenant", "tenants", "groups"):
        val = request.args.get(key)
        if val:
            qs.append(f"{key}={quote_plus(val)}")
    if qs:
        payload["position_url"] = payload["position_url"] + "?" + "&".join(qs)
    return jsonify(payload)


def _position_redirect_back(symbol):
    """Bounce back to the position page after a tag write, preserving the leg /
    account / tenant query string the user was looking at."""
    nxt = (request.form.get("next") or "").strip()
    # Only honor same-origin relative paths (avoid open-redirect).
    if nxt.startswith("/") and not nxt.startswith("//"):
        return redirect(nxt)
    ref = request.referrer or ""
    if ref:
        return redirect(ref)
    return redirect(url_for("position_detail", symbol=symbol))


def _tag_request_wants_json():
    """True when the tag write came from the inline (fetch) UI rather than a
    plain <form> submit — so we answer with JSON and skip the page reload."""
    if (request.headers.get("X-Requested-With") or "") == "XMLHttpRequest":
        return True
    accept = request.headers.get("Accept") or ""
    return "application/json" in accept


@app.route("/position/<symbol>/tags", methods=["POST"])
@login_required
@limiter.limit("60 per minute; 600 per hour")
def add_position_tag(symbol):
    """Attach one or more user-defined tags to one leg of a position.

    The ``tag`` field may carry several comma- (or newline-) separated tags so
    the inline UI can add a batch in one round-trip. XHR callers get JSON back
    (the normalized tags actually added + the user's full tag vocabulary for
    autocomplete); plain form posts fall back to a flash + redirect.

    Isolation: the write is rejected unless ``tenant_id`` is owned by the
    current user (``get_tenant_ids_for_user``). Tags are user metadata in
    Postgres, keyed on user_id — they never enter the warehouse.
    """
    from app.models import add_position_leg_tag, get_distinct_tags_for_user

    tenant_id = (request.form.get("tenant_id") or "").strip()
    leg_open_date = (request.form.get("leg_open_date") or "").strip()
    raw = (request.form.get("tag") or "").strip()
    # Split on commas / newlines so "earningsfollower, swing" adds both.
    candidates = [t.strip() for t in re.split(r"[,\n]+", raw) if t.strip()]
    wants_json = _tag_request_wants_json()

    owned = set(get_tenant_ids_for_user(current_user.id) or [])
    if not tenant_id or tenant_id not in owned:
        if wants_json:
            return jsonify(ok=False,
                           error="You can only tag your own positions."), 403
        flash("You can only tag your own positions.", "warning")
        return _position_redirect_back(symbol)
    if not leg_open_date or not candidates:
        if wants_json:
            return jsonify(ok=False, error="Pick a tag to add."), 400
        flash("Pick a tag to add.", "warning")
        return _position_redirect_back(symbol)

    added = []
    for cand in candidates:
        result = add_position_leg_tag(
            current_user.id, tenant_id, symbol, leg_open_date, cand
        )
        if result and result not in added:
            added.append(result)

    if wants_json:
        return jsonify(
            ok=bool(added),
            tags=added,
            all_tags=get_distinct_tags_for_user(current_user.id),
        )
    if added:
        flash("Tagged this leg “{}”.".format(", ".join(added)), "success")
    else:
        flash("We couldn't save that tag just now.", "danger")
    return _position_redirect_back(symbol)


@app.route("/position/<symbol>/tags/delete", methods=["POST"])
@login_required
@limiter.limit("60 per minute; 600 per hour")
def remove_position_tag(symbol):
    """Remove a user-defined tag from one leg of a position."""
    from app.models import remove_position_leg_tag

    tenant_id = (request.form.get("tenant_id") or "").strip()
    leg_open_date = (request.form.get("leg_open_date") or "").strip()
    tag = (request.form.get("tag") or "").strip()
    wants_json = _tag_request_wants_json()

    owned = set(get_tenant_ids_for_user(current_user.id) or [])
    if not tenant_id or tenant_id not in owned:
        # Silent no-op on an unowned tenant — nothing to remove anyway.
        if wants_json:
            return jsonify(ok=False), 403
        return _position_redirect_back(symbol)

    remove_position_leg_tag(current_user.id, tenant_id, symbol, leg_open_date, tag)
    if wants_json:
        return jsonify(ok=True, tag=tag)
    return _position_redirect_back(symbol)

