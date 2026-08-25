"""Positions list page (/positions).

Extracted verbatim from app/routes.py (routes.py refactor, Aug 2026).
Endpoint name is unchanged (`positions`); shared tenancy/label helpers
still live in app.routes.

DATE_FILTERED_QUERY duplicates the dividend-attribution semantics of
dbt/macros/attribute_dividends_to_strategy.sql on purpose (runtime date
params can't flow into a dbt macro) — see the ATTRIBUTION_INVARIANT
comments in both files.

Buy-and-Hold → Dividend only when yield is the *story*, not because a
stock happened to pay a coupon. See ``yield_is_primary``.
"""

from datetime import datetime, date, timedelta  # noqa: F401

import pandas as pd
from flask import render_template, request
from flask_login import login_required, current_user
from google.cloud import bigquery

from app import app
from app.bigquery_client import get_bigquery_client
from app.query_cache import cached_query_df
from app.skeleton import skeleton_page
# is_admin is not called directly here, but the positions fixtures
# patch.object(positions_page, "is_admin") — keep it importable.
from app.models import is_admin  # noqa: F401
from app.tenant_scope import (
    filter_df_by_tenant_ids as _filter_df_by_tenant_ids,
    tenant_sql_and as _tenant_sql_and,
)
from app.routes import (
    ACCOUNT_LEGS_QUERY,
    _norm_account_label,
    _norm_tag_date,
    _parse_date,
    _redirect_if_no_accounts,
    _tags_for_leg_range,
    _tenant_label_map_for_user,
    _tenants_for_scope,
    _user_account_list,
)


# Dust coupon: $17 on a $6k loss is not a dividend strategy.
DIVIDEND_RECLASS_FLOOR = 50.0


def yield_is_primary(dividend_income, trade_pnl, *, floor=DIVIDEND_RECLASS_FLOOR):
    """True when cash yield outweighs the price move (either direction).

    The old SQL used ``div > GREATEST(trade_pnl, 0)``. On a loser that
    floor is $0, so any dividend reclassified the row as Dividend —
    UFO −$6,554 + $17.45, VRT −$2,220 + $3.13, MSFT −$310 + $15.80.
    Those are buy-and-hold that happen to pay a coupon.

    Equivalent to: dividends are more than half of (|price P&L| + dividends)
    and beat the dust floor. SCHD +$78 / $118 div still qualifies; a
    JEPI that is underwater on price but paid $3k on a $2k dip still
    qualifies. Token coupons do not.
    """
    try:
        div = float(dividend_income)
        pnl = float(trade_pnl)
    except (TypeError, ValueError):
        return False
    return div >= floor and div > abs(pnl)


# ------------------------------------------------------------------
# SQL: date-filtered re-aggregation of positions_summary
# This CANNOT be a dbt model because it requires runtime date parameters
# from the user's filter selection. It re-aggregates int_strategy_classification
# with a WHERE clause on dates — essentially positions_summary with a date window.
# ------------------------------------------------------------------
DATE_FILTERED_QUERY = """
-- Date-filtered re-aggregation that mirrors positions_summary so the date
-- picker on /positions stays consistent with the un-filtered mart. Mirrors
-- the dividends-as-first-class semantics:
--   * total_pnl folds in attributed dividend income
--   * Buy-and-Hold reclassified to "Dividend" only when yield outweighs
--     the price move (div > ABS(trade_pnl) and div >= $50). A coupon on
--     a loser is still Buy and Hold.
--   * total_return preserved as alias of total_pnl for back-compat
--
-- ATTRIBUTION_INVARIANT: The dividend ranking + attribution + Buy-and-Hold
-- reclassification logic below MUST stay in sync with the canonical
-- definition in dbt/macros/attribute_dividends_to_strategy.sql (which is
-- imported by dbt/models/marts/positions_summary.sql). This runtime SQL
-- can't call the dbt macro directly because dbt macros compile at
-- `dbt build` time, not at request time, and we need the start_date /
-- end_date URL params to flow into the source filter. The duplication is
-- intentional and documented; if you change the macro, mirror the change
-- here AND verify with the integration test
-- tests/test_positions_filter_discipline.py::test_date_filtered_at_full_window_matches_mart.
WITH classified AS (
    SELECT *
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE open_date <= @end_date
      AND COALESCE(close_date, CURRENT_DATE()) >= @start_date
      {tenant_filter}
),

-- Read dividends from int_dividend_events (per-event). int_dividend_events
-- UNIONs CSV-reported dividends (from stg_history.action='dividend') with
-- yfinance-synthesized ex-div × holdings events. Reading stg_history
-- directly here was broken for ~99% of users: Schwab Connect drops
-- DIVIDEND_OR_INTEREST transactions and most users have never run a manual
-- CSV upload, so JEPI / JEPQ / SCHD positions reported $0 dividend income
-- on /position even when the user clearly owned thousands of shares for
-- years. Going through int_dividend_events instead respects the date
-- range filter while picking up synthetic dividends.
dividends AS (
    SELECT
        tenant_id,
        account,
        user_id,
        symbol,
        SUM(amount) AS total_dividend_income,
        COUNT(*) AS dividend_count
    FROM `ccwj-dbt.analytics.int_dividend_events`
    WHERE trade_date >= @start_date
      AND trade_date <= @end_date
      {tenant_filter}
    GROUP BY 1, 2, 3, 4
),

strategy_summary AS (
    SELECT
        tenant_id,
        account,
        user_id,
        symbol,
        strategy,

        -- Match positions_summary's 2-state status. The mart deliberately
        -- folds "both open and closed positions for this (account, symbol,
        -- strategy)" into 'Open' rather than emitting a 3rd 'Mixed' state,
        -- per its inline comment "to keep the UX simple". This runtime
        -- query used to emit 'Mixed' too, so the same page showed
        -- different status counts in the all-time view (mart, no Mixed)
        -- vs the date-filtered view (runtime, with Mixed). Folding here
        -- restores ATTRIBUTION_INVARIANT and stops users from seeing
        -- chips that vanish when they clear the date filter.
        CASE
            WHEN COUNTIF(status = 'Open') > 0 THEN 'Open'
            ELSE 'Closed'
        END AS status,

        SUM(total_pnl) AS total_pnl,
        -- Use pre-split realized_pnl / unrealized_pnl from
        -- int_strategy_classification rather than deriving from total_pnl
        -- by status. The pre-split version correctly attributes the
        -- already-realized portion of a still-open equity session (one
        -- with interim sells) to realized_pnl. The old "CASE WHEN
        -- status='Closed' THEN total_pnl" derivation lumped 100% of an
        -- Open session's P&L into unrealized — even after the trader
        -- had banked $X selling half the position. positions_summary has
        -- always done it this way; this restores ATTRIBUTION_INVARIANT.
        SUM(realized_pnl)   AS realized_pnl,
        SUM(unrealized_pnl) AS unrealized_pnl,

        SUM(premium_received) AS total_premium_received,
        SUM(ABS(premium_paid)) AS total_premium_paid,

        COUNT(*) AS num_trade_groups,
        SUM(num_trades) AS num_individual_trades,
        COUNTIF(is_winner AND status = 'Closed') AS num_winners,
        COUNTIF(NOT is_winner AND status = 'Closed') AS num_losers,

        SAFE_DIVIDE(
            COUNTIF(is_winner AND status = 'Closed'),
            NULLIF(COUNTIF(status = 'Closed'), 0)
        ) AS win_rate,

        SAFE_DIVIDE(
            SUM(CASE WHEN status = 'Closed' THEN total_pnl ELSE 0 END),
            NULLIF(COUNTIF(status = 'Closed'), 0)
        ) AS avg_pnl_per_trade,

        ROUND(AVG(days_in_trade), 1) AS avg_days_in_trade,
        MIN(open_date) AS first_trade_date,
        MAX(COALESCE(close_date, CURRENT_DATE())) AS last_trade_date

    FROM classified
    GROUP BY 1, 2, 3, 4, 5
),

with_dividend_rank AS (
    SELECT
        ss.*,
        ROW_NUMBER() OVER (
            PARTITION BY ss.tenant_id, ss.account, ss.user_id, ss.symbol
            ORDER BY
                CASE ss.strategy
                    WHEN 'Wheel'        THEN 1
                    WHEN 'Covered Call'  THEN 2
                    WHEN 'Buy and Hold'  THEN 3
                    ELSE 99
                END
        ) AS dividend_rank
    FROM strategy_summary ss
),

with_attributed AS (
    SELECT
        wdr.*,
        CASE WHEN wdr.dividend_rank = 1
            THEN COALESCE(d.total_dividend_income, 0)
            ELSE 0
        END AS attributed_dividend_income,
        CASE WHEN wdr.dividend_rank = 1
            THEN COALESCE(d.dividend_count, 0)
            ELSE 0
        END AS attributed_dividend_count
    FROM with_dividend_rank wdr
    LEFT JOIN dividends d
        ON (wdr.tenant_id IS NOT DISTINCT FROM d.tenant_id)
        AND wdr.account = d.account
        AND (wdr.user_id IS NOT DISTINCT FROM d.user_id)
        AND wdr.symbol = d.symbol
),

final AS (
    SELECT
        wa.tenant_id,
        wa.account,
        wa.user_id,
        wa.symbol,
        CASE
            WHEN wa.dividend_rank = 1
                 AND wa.strategy = 'Buy and Hold'
                 AND wa.attributed_dividend_income >= 50
                 AND wa.attributed_dividend_income > ABS(wa.total_pnl)
                THEN 'Dividend'
            ELSE wa.strategy
        END AS strategy,
        wa.status,
        ROUND(wa.total_pnl + wa.attributed_dividend_income, 2) AS total_pnl,
        ROUND(wa.total_pnl, 2)        AS trade_only_pnl,
        ROUND(wa.realized_pnl, 2)     AS realized_pnl,
        ROUND(wa.unrealized_pnl, 2)   AS unrealized_pnl,
        ROUND(wa.total_premium_received, 2) AS total_premium_received,
        ROUND(wa.total_premium_paid, 2) AS total_premium_paid,
        wa.num_trade_groups,
        wa.num_individual_trades,
        wa.num_winners,
        wa.num_losers,
        ROUND(wa.win_rate, 4) AS win_rate,
        ROUND(wa.avg_pnl_per_trade, 2) AS avg_pnl_per_trade,
        wa.avg_days_in_trade,
        wa.first_trade_date,
        wa.last_trade_date,
        ROUND(wa.attributed_dividend_income, 2) AS total_dividend_income,
        wa.attributed_dividend_count            AS dividend_count,
        ROUND(wa.total_pnl + wa.attributed_dividend_income, 2) AS total_return
    FROM with_attributed wa
)

SELECT * FROM final
ORDER BY tenant_id, account, user_id, symbol, strategy
"""

# ------------------------------------------------------------------
# Default (no date filter): use the pre-built mart
# ------------------------------------------------------------------
DEFAULT_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {tenant_filter}
    ORDER BY account, symbol, strategy
"""

ERROR_DEFAULTS = dict(
    error="",
    rows=[],
    symbol_rows=[],
    kpis={},
    strategy_chart=[],
    accounts=[],
    strategies=[],
    symbols=[],
    subsectors=[],
    sectors=[],
    tags=[],
    user_accounts=[],
    status_counts={"Open": 0, "Closed": 0, "Mixed": 0},
    selected_account="",
    selected_strategy="",
    selected_statuses=[],
    selected_symbol="",
    selected_subsector="",
    selected_sector="",
    selected_tag="",
    selected_start_date="",
    selected_end_date="",
    date_filtered=False,
    page=1,
    total_pages=1,
    total_rows=0,
    per_page=25,
    today=date.today(),
    timedelta=timedelta,
)


# Trade-group grain (one row per equity session / option contract) used to
# rebuild the positions frame scoped to ONLY a tagged leg. Mirrors the columns
# positions_summary aggregates so the tag-scoped rollup lands on the same grain.
POSITIONS_TAG_STRAT_QUERY = """
    SELECT
        tenant_id, account, user_id, symbol, strategy, status,
        open_date, close_date, days_in_trade,
        total_pnl, realized_pnl, unrealized_pnl,
        num_trades, premium_received, premium_paid, is_winner
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE 1=1 {tenant_filter}
"""


def _tag_scoped_positions_df(client, tenant_ids, tenant_filter, tag_rows,
                             selected_tag, base_df,
                             start_date=None, end_date=None):
    """Rebuild the positions_summary-shaped frame scoped to ONLY the legs that
    carry ``selected_tag``.

    A user tag anchors a single LEG (one chapter of a (tenant, symbol)), but
    ``positions_summary`` is per (tenant, account, symbol, strategy) — it rolls
    up EVERY leg of the symbol. Filtering the mart by "this symbol has a tagged
    leg" therefore reported the WHOLE symbol's P&L (all 8 ASTS legs) when only
    one leg (Leg 8) was tagged — realized $5,624 shown for an earningsfollower
    tag that only owns the +$905 open leg. This re-aggregates
    ``int_strategy_classification`` trade groups, keeping only those whose
    ``open_date`` falls inside a tagged leg's ``[open_date, last_activity_date]``
    window (the same date-containment ``_tags_for_leg_range`` and the Position
    Detail leg scoping use), then rolls them up to the mart's grain so the KPI
    hero and both tables read the tagged-leg P&L only.

    Dividends / premium-collected are attributed per symbol in the mart and
    can't be leg-scoped, so ``total_dividend_income`` is 0 here and
    ``total_return`` == trade P&L — consistent with the /accounts Tag Breakdown,
    which also sums combined leg P&L without dividends.
    """
    empty = base_df.iloc[0:0].copy()
    if not selected_tag or not tag_rows:
        return empty

    # 1) Tagged leg date windows per (tenant, symbol).
    try:
        legs_df = cached_query_df(
            client, ACCOUNT_LEGS_QUERY.format(tenant_filter=tenant_filter)
        )
    except Exception:
        return empty
    legs_df = _filter_df_by_tenant_ids(legs_df, tenant_ids)
    ranges_by_key = {}  # (tenant_id, symbol_upper) -> [(lo_date, hi_date), ...]
    if not legs_df.empty:
        for _, lr in legs_df.iterrows():
            matched = _tags_for_leg_range(
                tag_rows, lr.get("tenant_id"),
                lr.get("open_date"), lr.get("last_activity_date"),
                symbol=lr.get("symbol"),
            )
            if selected_tag not in matched:
                continue
            tid = str(lr.get("tenant_id") or "")
            sym = str(lr.get("symbol") or "").upper()
            lo = _norm_tag_date(lr.get("open_date"))
            hi = _norm_tag_date(lr.get("last_activity_date")) or lo
            ranges_by_key.setdefault((tid, sym), []).append((lo, hi))
    if not ranges_by_key:
        return empty

    # 2) Trade groups for the in-scope tenants.
    try:
        sc = cached_query_df(
            client, POSITIONS_TAG_STRAT_QUERY.format(tenant_filter=tenant_filter)
        )
    except Exception:
        return empty
    sc = _filter_df_by_tenant_ids(sc, tenant_ids)
    if sc.empty:
        return empty

    # 3) Keep trade groups whose open_date lands inside a tagged leg window for
    #    the SAME (tenant, symbol). An optional date-filter window narrows
    #    further so tag + time-range combine sanely.
    def _in_tag(row):
        key = (str(row.get("tenant_id") or ""),
               str(row.get("symbol") or "").upper())
        wins = ranges_by_key.get(key)
        if not wins:
            return False
        od = _norm_tag_date(row.get("open_date"))
        if od is None:
            return False
        if start_date is not None and od < start_date:
            return False
        if end_date is not None and od > end_date:
            return False
        for lo, hi in wins:
            if (lo is None or od >= lo) and (hi is None or od <= hi):
                return True
        return False

    sc = sc[sc.apply(_in_tag, axis=1)]
    if sc.empty:
        return empty

    # 4) Coerce + roll up to the positions_summary grain
    #    (tenant, account, user_id, symbol, strategy).
    for c in ["total_pnl", "realized_pnl", "unrealized_pnl",
              "premium_received", "premium_paid", "num_trades", "days_in_trade"]:
        if c in sc.columns:
            sc[c] = pd.to_numeric(sc[c], errors="coerce").fillna(0)
    sc["is_winner"] = sc["is_winner"].astype(bool)
    _closed = sc["status"].astype(str).eq("Closed")
    sc["_win_closed"] = (sc["is_winner"] & _closed).astype(int)
    sc["_los_closed"] = (~sc["is_winner"] & _closed).astype(int)
    sc["_open_cnt"] = (~_closed).astype(int)
    sc["_closed_pnl"] = sc["total_pnl"].where(_closed, 0.0)
    sc["_premium_paid_abs"] = sc["premium_paid"].abs()

    grouped = (
        sc.groupby(["tenant_id", "account", "user_id", "symbol", "strategy"],
                   dropna=False)
        .agg(
            total_pnl=("total_pnl", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            unrealized_pnl=("unrealized_pnl", "sum"),
            total_premium_received=("premium_received", "sum"),
            total_premium_paid=("_premium_paid_abs", "sum"),
            num_trade_groups=("total_pnl", "size"),
            num_individual_trades=("num_trades", "sum"),
            num_winners=("_win_closed", "sum"),
            num_losers=("_los_closed", "sum"),
            _closed_pnl=("_closed_pnl", "sum"),
            _open_cnt=("_open_cnt", "sum"),
            avg_days_in_trade=("days_in_trade", "mean"),
        )
        .reset_index()
    )

    _n_closed = grouped["num_winners"] + grouped["num_losers"]
    _denom = _n_closed.where(_n_closed > 0, 1)
    grouped["status"] = grouped["_open_cnt"].gt(0).map({True: "Open", False: "Closed"})
    grouped["win_rate"] = (grouped["num_winners"] / _denom).where(_n_closed > 0, 0.0)
    grouped["avg_pnl_per_trade"] = (
        (grouped["_closed_pnl"] / _denom).where(_n_closed > 0, 0.0).round(2)
    )
    grouped["total_dividend_income"] = 0.0
    grouped["dividend_count"] = 0
    grouped["total_return"] = grouped["total_pnl"].round(2)
    for c in ["total_pnl", "realized_pnl", "unrealized_pnl",
              "total_premium_received", "total_premium_paid", "avg_days_in_trade"]:
        grouped[c] = grouped[c].round(2)
    grouped = grouped.drop(columns=["_closed_pnl", "_open_cnt"])

    # 5) Attach sector/subsector from the mart frame (per tenant + symbol).
    meta_cols = [c for c in ["sector", "subsector"]
                 if not base_df.empty and c in base_df.columns]
    if meta_cols and {"tenant_id", "symbol"}.issubset(base_df.columns):
        meta = (
            base_df[["tenant_id", "symbol"] + meta_cols]
            .drop_duplicates(["tenant_id", "symbol"])
        )
        grouped = grouped.merge(meta, on=["tenant_id", "symbol"], how="left")
    for c in ["sector", "subsector"]:
        if c in grouped.columns:
            grouped[c] = grouped[c].fillna("Unknown")

    return grouped


@app.route("/positions")
@login_required
@skeleton_page
def positions():
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()

    # ------------------------------------------------------------------
    # 1. Read filter params
    # ------------------------------------------------------------------
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)
    selected_strategy = request.args.get("strategy", "")
    # Multi-select status; default is all (current + history) so users see their
    # full book unless they explicitly narrow it.
    selected_statuses = request.args.getlist("status")
    selected_symbol = request.args.get("symbol", "")
    # 'subsector' is the new param; 'industry' is the pre-rename alias and is
    # still accepted so any old bookmarks / external links keep working.
    selected_subsector = (
        request.args.get("subsector", "") or request.args.get("industry", "")
    )
    selected_sector = request.args.get("sector", "")
    selected_start_date = request.args.get("start_date", "")
    selected_end_date = request.args.get("end_date", "")
    # User-defined leg tag filter (Postgres). Normalized to match stored tags.
    selected_tag = (request.args.get("tag", "") or "").strip().lower()
    page = max(1, int(request.args.get("page", 1)))

    start_date = _parse_date(selected_start_date)
    end_date = _parse_date(selected_end_date)
    date_filtered = start_date is not None or end_date is not None

    # ------------------------------------------------------------------
    # 2. Query BigQuery
    # ------------------------------------------------------------------
    try:
        if date_filtered:
            # Fill open boundaries with wide defaults
            effective_start = start_date or date(2000, 1, 1)
            effective_end = end_date or date.today()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "DATE", effective_start),
                    bigquery.ScalarQueryParameter("end_date", "DATE", effective_end),
                ]
            )
            df = cached_query_df(client, DATE_FILTERED_QUERY.format(tenant_filter=tenant_filter), job_config=job_config)
        else:
            df = cached_query_df(client, DEFAULT_QUERY.format(tenant_filter=tenant_filter))
    except Exception as exc:
        ctx = dict(ERROR_DEFAULTS)
        ctx["error"] = str(exc)
        # Even on error, pass the auth account list so the hero can render
        # the right "you have N accounts but couldn't load data" message
        # rather than the generic "no accounts linked" copy.
        ctx["user_accounts"] = user_accounts or []
        return render_template("positions.html", **ctx)

    # ------------------------------------------------------------------
    # 3. Tenant-scope BEFORE any aggregation or coercion
    #
    # IMPORTANT tenancy rule (keep): the hero, KPIs, chart, and every table
    # below MUST read off DataFrames that have already been scoped to the
    # logged-in user's accounts. The SQL is already account-scoped via
    # _account_sql_and, but the BQ-tenant rule requires a Python re-filter
    # before any re-aggregation (which includes the numeric coercion below
    # — fillna/to_numeric are arguably re-aggregation work). Do not move
    # this back below the coercion. See
    # .cursor/rules/bigquery-tenant-isolation.mdc.
    # ------------------------------------------------------------------
    df = _filter_df_by_tenant_ids(df, tenant_ids)

    # ------------------------------------------------------------------
    # 4. Clean up types (now safe — frame is tenant-scoped)
    # ------------------------------------------------------------------
    numeric_cols = [
        "total_pnl", "realized_pnl", "unrealized_pnl",
        "total_premium_received", "total_premium_paid",
        "num_trade_groups", "num_individual_trades",
        "num_winners", "num_losers", "win_rate",
        "avg_pnl_per_trade", "avg_days_in_trade",
        "total_dividend_income", "dividend_count", "total_return",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    for col in ["first_trade_date", "last_trade_date"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("NaT", "")

    accounts = sorted(df["account"].dropna().unique())
    strategies = sorted(df["strategy"].dropna().unique())
    symbols = sorted(df["symbol"].dropna().unique())
    subsectors = (
        sorted(df["subsector"].dropna().unique())
        if "subsector" in df.columns else []
    )
    sectors = (
        sorted(df["sector"].dropna().unique())
        if "sector" in df.columns else []
    )
    # User-defined leg tags (Postgres) for the filter dropdown.
    from app.models import (
        get_distinct_tags_for_user as _get_distinct_tags_for_user,
        get_all_leg_tags_for_user as _get_all_leg_tags_for_user,
    )
    tags = _get_distinct_tags_for_user(current_user.id)

    filtered = df.copy()
    # NOTE: no secondary ``account == selected_account`` narrowing here.
    # ``_tenants_for_scope(selected_account)`` already resolved the
    # selected display label (incl. disambiguated colliding labels like
    # "Schwab Account (\u2022\u20226342)") to specific tenant_ids, and the
    # SQL ``tenant_filter`` + ``_filter_df_by_tenant_ids`` already scoped
    # the frame to them. A label-equality filter here would wrongly empty
    # the frame for disambiguated labels (the mart's raw ``account`` is
    # still "Schwab Account").
    if selected_strategy:
        filtered = filtered[filtered["strategy"] == selected_strategy]
    if selected_statuses:
        filtered = filtered[filtered["status"].isin(selected_statuses)]
    if selected_symbol:
        filtered = filtered[filtered["symbol"] == selected_symbol]
    if selected_subsector and "subsector" in filtered.columns:
        filtered = filtered[filtered["subsector"] == selected_subsector]
    if selected_sector and "sector" in filtered.columns:
        filtered = filtered[filtered["sector"] == selected_sector]
    if selected_tag:
        # Tags anchor a single LEG, but positions_summary is per symbol/strategy
        # (it rolls up EVERY leg). Filtering the mart by "symbol has a tagged
        # leg" reported the whole symbol's P&L — all 8 ASTS legs / $5,624
        # realized — for a tag that only owns one +$905 open leg. Rebuild the
        # frame scoped to ONLY the tagged legs' trade groups so the hero + both
        # tables read the tagged-leg P&L. This REPLACES `filtered` wholesale, so
        # the other active filters (applied above) are re-applied below.
        _tag_rows = _get_all_leg_tags_for_user(current_user.id, tenant_ids)
        filtered = _tag_scoped_positions_df(
            client, tenant_ids, tenant_filter, _tag_rows, selected_tag, df,
            start_date=start_date, end_date=end_date,
        )
        if selected_strategy and "strategy" in filtered.columns:
            filtered = filtered[filtered["strategy"] == selected_strategy]
        if selected_statuses and "status" in filtered.columns:
            filtered = filtered[filtered["status"].isin(selected_statuses)]
        if selected_symbol and "symbol" in filtered.columns:
            filtered = filtered[filtered["symbol"] == selected_symbol]
        if selected_subsector and "subsector" in filtered.columns:
            filtered = filtered[filtered["subsector"] == selected_subsector]
        if selected_sector and "sector" in filtered.columns:
            filtered = filtered[filtered["sector"] == selected_sector]

    # Status counts for hero chips. Must read from `filtered`, NOT `df`,
    # so the chips agree with the body. Reading from `df` was a long-
    # standing UI lie: the chip said "12 open" even when the user had
    # filtered to one symbol with 1 open position. Hero / body
    # disagreement on the same page is exactly the same bug class as
    # Position Detail's "Strategy Breakdown didn't update" — a
    # sub-aggregation reading from the wrong source.
    status_counts = {"Open": 0, "Closed": 0, "Mixed": 0}
    if "status" in filtered.columns and not filtered.empty:
        vc = filtered["status"].fillna("").value_counts()
        for k in list(status_counts.keys()):
            status_counts[k] = int(vc.get(k, 0))

    # ------------------------------------------------------------------
    # 5. KPIs
    # ------------------------------------------------------------------
    total_winners = int(filtered["num_winners"].sum())
    total_losers = int(filtered["num_losers"].sum())
    total_closed = total_winners + total_losers

    kpis = {
        "total_return": float(filtered["total_return"].sum()),
        "realized_pnl": float(filtered["realized_pnl"].sum()),
        "unrealized_pnl": float(filtered["unrealized_pnl"].sum()),
        "dividend_income": (
            float(filtered["total_dividend_income"].sum())
            if "total_dividend_income" in filtered.columns
            else 0.0
        ),
        "premium_collected": float(filtered["total_premium_received"].sum()),
        "win_rate": total_winners / total_closed if total_closed else 0,
        "num_positions": len(filtered),
        "total_trades": int(filtered["num_individual_trades"].sum()),
        # Closed-trade-group counts. Distinct from total_trades, which sums
        # num_individual_trades (each open + close + roll fill counts). The
        # template's Quick Stats card used to derive winners as
        # total_trades * win_rate, which is wrong: win_rate is the
        # winner-share of *closed groups*, so multiplying by per-fill trade
        # count over-reports winners by 2-3x. Pass the raw counts through
        # and let the template render them directly.
        "num_winners": total_winners,
        "num_losers": total_losers,
        "num_closed_groups": total_closed,
    }

    # ------------------------------------------------------------------
    # 6. Chart data: total P&L by strategy
    # ------------------------------------------------------------------
    strategy_chart = (
        filtered.groupby("strategy")["total_pnl"]
        .sum()
        .sort_values(ascending=True)
        .reset_index()
        .rename(columns={"total_pnl": "pnl"})
        .to_dict(orient="records")
    )

    # ------------------------------------------------------------------
    # 7. Symbol-level summary (grouped by account + symbol)
    # ------------------------------------------------------------------
    if not filtered.empty:
        # Carry sector / subsector through the symbol-level rollup. Each
        # (account, symbol) maps to a single sector/subsector, so 'first' is
        # safe and fast.
        agg_kwargs = dict(
            total_pnl=("total_pnl", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            unrealized_pnl=("unrealized_pnl", "sum"),
            total_premium_received=("total_premium_received", "sum"),
            total_dividend_income=("total_dividend_income", "sum"),
            total_return=("total_return", "sum"),
            num_individual_trades=("num_individual_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
            num_strategies=("strategy", "nunique"),
            strategies=("strategy", lambda x: ", ".join(sorted(x.unique()))),
        )
        if "sector" in filtered.columns:
            agg_kwargs["sector"] = ("sector", "first")
        if "subsector" in filtered.columns:
            agg_kwargs["subsector"] = ("subsector", "first")
        # Grain on tenant_id (not the broker `account` string) so a symbol
        # held in several physical accounts that share one display label
        # (e.g. 5 "Schwab Account" tenants holding QTUM) shows one row per
        # account instead of collapsing into a single misleading nickname.
        # `account` rides along for display/fallback. Falls back to the
        # account string only if the frame predates the tenant_id grain.
        _sym_grain = (
            (["tenant_id"] if "tenant_id" in filtered.columns else [])
            + ["account", "symbol"]
        )
        symbol_agg = (
            filtered.groupby(_sym_grain)
            .agg(**agg_kwargs)
            .reset_index()
        )
        closed = symbol_agg["num_winners"] + symbol_agg["num_losers"]
        symbol_agg["win_rate"] = symbol_agg["num_winners"] / closed.replace(0, pd.NA)
        symbol_agg["win_rate"] = symbol_agg["win_rate"].fillna(0)
        symbol_agg = symbol_agg.sort_values("total_return", ascending=False)
        symbol_rows = symbol_agg.to_dict(orient="records")
    else:
        symbol_rows = []

    # ------------------------------------------------------------------
    # 8. Strategy detail rows (aggregated by account × strategy, paginated)
    # ------------------------------------------------------------------
    if not filtered.empty:
        # Same tenant_id grain as the symbol rollup above so each physical
        # account's strategy line is distinct (see _sym_grain comment).
        _strat_grain = (
            (["tenant_id"] if "tenant_id" in filtered.columns else [])
            + ["account", "strategy"]
        )
        strat_agg = (
            filtered.groupby(_strat_grain)
            .agg(
                status=("status", lambda xs: "Open" if (xs == "Open").any() else "Closed"),
                total_pnl=("total_pnl", "sum"),
                realized_pnl=("realized_pnl", "sum"),
                unrealized_pnl=("unrealized_pnl", "sum"),
                total_premium_received=("total_premium_received", "sum"),
                total_dividend_income=("total_dividend_income", "sum"),
                total_return=("total_return", "sum"),
                num_individual_trades=("num_individual_trades", "sum"),
                num_winners=("num_winners", "sum"),
                num_losers=("num_losers", "sum"),
                avg_pnl_per_trade=("avg_pnl_per_trade", "mean"),
                avg_days_in_trade=("avg_days_in_trade", "mean"),
            )
            .reset_index()
        )
        closed_ct = strat_agg["num_winners"] + strat_agg["num_losers"]
        strat_agg["win_rate"] = strat_agg["num_winners"] / closed_ct.replace(0, pd.NA)
        strat_agg["win_rate"] = strat_agg["win_rate"].fillna(0)
        strat_agg = strat_agg.sort_values("total_return", ascending=False)
        all_rows = strat_agg.to_dict(orient="records")
    else:
        all_rows = []

    per_page = 25
    total_rows = len(all_rows)
    total_pages = max(1, (total_rows + per_page - 1) // per_page)
    page = min(page, total_pages)
    start_idx = (page - 1) * per_page
    rows = all_rows[start_idx : start_idx + per_page]

    # Resolve a per-row display label off the broker-stable tenant_id so the
    # Account column shows each account's own nickname (Emmory / Sara 401k /
    # ...) rather than the colliding broker `account` string. Falls back to
    # the raw account label when a row carries no tenant_id (admin browsing
    # or pre-grain frames).
    _tenant_labels = _tenant_label_map_for_user(getattr(current_user, "id", None))

    def _label_rows(_rows):
        for _r in _rows:
            _tid = _r.get("tenant_id")
            _r["account_display"] = (
                (_tenant_labels.get(_tid) if _tid else None)
                or _norm_account_label(_r.get("account"))
            )
        return _rows

    _label_rows(rows)
    _label_rows(symbol_rows)

    return render_template(
        "positions.html",
        rows=rows,
        symbol_rows=symbol_rows,
        kpis=kpis,
        strategy_chart=strategy_chart,
        accounts=accounts,
        strategies=strategies,
        symbols=symbols,
        subsectors=subsectors,
        sectors=sectors,
        tags=tags,
        # `user_accounts` is the auth list (every account the user has
        # linked), used by the hero to decide between "you haven't
        # connected anything yet" and "your filter just returned nothing".
        # `accounts` is the data list (accounts that have positions in the
        # current view) and powers the Account dropdown. Distinct names
        # because they answer different questions.
        user_accounts=user_accounts,
        status_counts=status_counts,
        selected_account=selected_account,
        selected_strategy=selected_strategy,
        selected_statuses=selected_statuses,
        selected_symbol=selected_symbol,
        selected_subsector=selected_subsector,
        selected_sector=selected_sector,
        selected_tag=selected_tag,
        selected_start_date=selected_start_date,
        selected_end_date=selected_end_date,
        date_filtered=date_filtered,
        page=page,
        total_pages=total_pages,
        total_rows=total_rows,
        per_page=per_page,
        today=date.today(),
        timedelta=timedelta,
    )

