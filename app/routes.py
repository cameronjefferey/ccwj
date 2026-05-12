from flask import render_template, request, redirect, url_for, Response, flash, abort
from werkzeug.exceptions import RequestEntityTooLarge
from flask_login import login_required, current_user
from app import app
from app.extensions import limiter
from app.bigquery_client import get_bigquery_client
from app.schwab import (
    SCHWAB_FULL_HISTORY_LOOKBACK_DAYS,
    _schwab_transaction_lookback_days,
)
from app.models import (
    add_account_for_user,
    get_accounts_for_user,
    get_schwab_connections,
    get_strategy_fit_insight_for_user,
    is_admin,
)
from google.cloud import bigquery
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor
import math
import os
import pandas as pd
import json


def _bq_parallel(client, queries):
    """Run multiple BigQuery queries in parallel and return results dict.

    queries: dict of {name: sql_string} or {name: (sql_string, job_config)}
    Returns: dict of {name: DataFrame}
    """
    results = {}

    def _run(name, spec):
        if isinstance(spec, tuple):
            sql, cfg = spec
            return name, client.query(sql, job_config=cfg).to_dataframe()
        return name, client.query(spec).to_dataframe()

    with ThreadPoolExecutor(max_workers=min(len(queries), 8)) as pool:
        futures = [pool.submit(_run, n, s) for n, s in queries.items()]
        for f in futures:
            name, df = f.result()
            results[name] = df

    return results


def _redirect_if_no_accounts():
    """Bounce a freshly signed-up user to /get-started instead of letting
    them land on a data-driven page where every BigQuery query gets
    AND 1=0'd and the UI shows "we're calculating…" forever.

    Returns a Flask redirect response when the current user has zero
    linked accounts (and isn't an admin), or None when the caller should
    continue rendering normally.
    """
    if current_user.is_authenticated and not is_admin(current_user.username):
        if len(get_accounts_for_user(current_user.id)) == 0:
            # Skip if they're already on /get-started or coming back from
            # an upload; the upload-processing screen redirects through
            # weekly-review with from_upload=1 during the 3–5 min lag.
            if request.endpoint == "get_started":
                return None
            if request.args.get("from_upload") == "1" or request.args.get("from_sync") == "1":
                return None
            return redirect(url_for("get_started"))
    return None


def _user_account_list():
    """
    Return the list of accounts the current user is allowed to see,
    or None if the user is an admin (meaning: no filter, show everything).

    Includes labels from user_accounts (upload / sync) and from Schwab OAuth
    rows so BigQuery filters match the Account column in seeds. If Schwab is
    connected but user_accounts was never populated, we add the Schwab labels
    here (idempotent) so queries are not forced to AND 1=0.

    Sharing labels across users is allowed — e.g. a parent and a child
    can both call their account "Schwab Account". Tenant isolation is
    enforced at the row level by ``user_id`` everywhere downstream
    (every BQ query passes through ``_account_sql_and`` / its sibling
    that adds a ``user_id IS NOT DISTINCT FROM`` predicate, and every
    DataFrame is then re-filtered by ``_filter_df_by_accounts`` which
    drops rows whose ``user_id`` is a different populated id). See
    ``docs/USER_ID_TENANCY.md`` and
    ``.cursor/rules/bigquery-tenant-isolation.mdc``.
    """
    if is_admin(current_user.username):
        return None                     # admin → no restriction
    names = list(get_accounts_for_user(current_user.id))
    have = set(names)
    for row in get_schwab_connections(current_user.id):
        label = (row.get("account_name") or "").strip() or str(
            row.get("account_number") or ""
        ).strip()
        if not label:
            continue
        if label not in have:
            add_account_for_user(current_user.id, label)
            have.add(label)
            names.append(label)
    return sorted(names)


def _resolve_filter_user_id():
    """Return the ``user_id`` to scope BigQuery reads by for the current
    request, or ``None`` for admin / unauthenticated paths (no scoping).

    The legacy ``_account_sql_*`` and ``_filter_df_by_accounts`` helpers
    use this to automatically add a ``user_id`` predicate to every read
    they shape, so two users sharing an ``account_name`` cannot see each
    other's rows. See ``docs/USER_ID_TENANCY.md`` for the full story.
    """
    try:
        from flask_login import current_user
        if not getattr(current_user, "is_authenticated", False):
            return None
        if is_admin(current_user.username):
            return None
        return int(current_user.id)
    except Exception:
        return None


def _account_sql_filter(accounts, col="account", user_col="user_id"):
    """Build a ``WHERE`` clause that scopes a BigQuery read to the current
    tenant. ``accounts=None`` means admin (no account filter).

    Despite the name, this helper now adds a ``user_id`` predicate too —
    that's the actual security boundary; ``account_name`` alone leaks
    across tenants when two users share a label. The legacy name is
    kept so existing call sites pick up the security upgrade for free.
    See ``docs/USER_ID_TENANCY.md``.
    """
    return _user_scoped_filter(
        _resolve_filter_user_id(), accounts, col=col, user_col=user_col,
    )


def _account_sql_and(accounts, col="account", user_col="user_id"):
    """``AND``-shaped sibling of ``_account_sql_filter`` for joining onto an
    existing ``WHERE``. Adds the user_id predicate too. See
    ``docs/USER_ID_TENANCY.md``.
    """
    return _user_scoped_and(
        _resolve_filter_user_id(), accounts, col=col, user_col=user_col,
    )


def _filter_df_by_accounts(df, accounts, col="account", user_col="user_id"):
    """Filter a DataFrame to rows owned by the current tenant.

    Like the SQL helpers above, this now drops rows whose ``user_id`` is
    a different populated id than the current user's. Stage 0/1
    leniency: rows with NULL ``user_id`` are kept when their ``account``
    is in the user's allowed list. Admin / unauthenticated bypass the
    user check. See ``docs/USER_ID_TENANCY.md``.
    """
    return _filter_df_by_user(
        df, _resolve_filter_user_id(), accounts, col=col, user_col=user_col,
    )


def _dedupe_enriched_current_positions(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate open rows from ``int_enriched_current`` (same contract or
    equity line merged twice). Seed/snapshot regressions can emit byte-near
    duplicates; the UI should not show twin 200-share lines with identical cost.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if "trade_symbol" in out.columns:
        out["trade_symbol"] = (
            out["trade_symbol"].astype(str).str.strip().replace({"nan": ""})
        )
    key = [c for c in ("account", "user_id", "instrument_type", "trade_symbol") if c in out.columns]
    if len(key) < 2:
        return df
    return out.drop_duplicates(subset=key, keep="last").reset_index(drop=True)


def _narrow_mart_daily_pnl_chart_df_to_summary_tenant(
    chart_df: pd.DataFrame, summary_df: pd.DataFrame
) -> pd.DataFrame:
    """When admin scope merges two Postgres tenants under one ``account`` label,
    ``mart_daily_pnl`` can return parallel date spines. Stateful
    ``_build_chart_from_daily_pnl`` would process every row and double-count
    equity fills. Align the chart frame to the same ``user_id`` distribution
    as ``positions_summary`` for this page (mode wins on ties)."""
    if chart_df is None or chart_df.empty or "user_id" not in chart_df.columns:
        return chart_df
    m_ids = pd.to_numeric(chart_df["user_id"], errors="coerce").dropna().unique()
    if len(m_ids) <= 1:
        return chart_df
    if summary_df is None or summary_df.empty or "user_id" not in summary_df.columns:
        app.logger.warning(
            "mart_daily_pnl chart has %s distinct user_ids but summary lacks user_id; "
            "cannot narrow chart tenant",
            len(m_ids),
        )
        return chart_df
    s_ids = pd.to_numeric(summary_df["user_id"], errors="coerce").dropna()
    if s_ids.empty:
        return chart_df
    uid_keep = int(s_ids.astype(int).value_counts().index[0])
    m_num = pd.to_numeric(chart_df["user_id"], errors="coerce")
    narrowed = chart_df.loc[m_num.eq(uid_keep)].copy()
    if narrowed.empty:
        app.logger.warning(
            "chart tenant narrow: summary user_id=%s absent from mart chart; "
            "keeping un-narrowed frame",
            uid_keep,
        )
        return chart_df
    return narrowed


def _filter_current_for_chart_partition(
    current_df: pd.DataFrame, account, user_id_key
) -> pd.DataFrame:
    """Slice ``int_enriched_current`` rows for one chart partition
    (``account`` × optional ``user_id``). Required when ``mart_daily_pnl``
    spans multiple partitions for the same symbol — the live today-row patch
    must not mix snapshots across tenants."""
    if current_df is None or current_df.empty or "account" not in current_df.columns:
        return pd.DataFrame()
    m = current_df["account"].astype(str) == str(account).strip()
    if "user_id" in current_df.columns:
        uid_series = pd.to_numeric(current_df["user_id"], errors="coerce")
        if user_id_key is None or pd.isna(user_id_key):
            m &= uid_series.isna()
        else:
            uk = float(pd.to_numeric(pd.Series([user_id_key]), errors="coerce").iloc[0])
            m &= uid_series == uk
    return current_df.loc[m].copy()


def _merge_position_pnl_chart_payloads(parts: list) -> dict:
    """Sum cumulative position-chart series across partitions (each partition
    was built with its own equity cost-basis state machine).

    Rows missing on sparse partitions forward-fill within that partition before
    summing so inactive accounts contribute zero before their first date."""
    empty = {
        "dates": [], "equity": [], "options": [], "dividends": [],
        "total": [], "underlying_price": [], "has_underlying_price": False,
    }
    parts = [p for p in (parts or []) if p and (p.get("dates") or [])]
    if not parts:
        return empty
    if len(parts) == 1:
        return parts[0]
    all_dates = sorted(set(d for p in parts for d in p["dates"]))
    idx = pd.Index(all_dates)
    keys = ["equity", "options", "dividends", "total"]
    merged = {k: pd.Series(0.0, index=idx, dtype=float) for k in keys}
    price_acc = pd.Series(index=idx, dtype=float)
    for p in parts:
        ds = p["dates"]
        for k in keys:
            vals = p[k][: len(ds)]
            s = pd.Series(vals, index=pd.Index(ds))
            s = s[~s.index.duplicated(keep="last")].sort_index()
            s = s.reindex(idx).ffill().fillna(0.0)
            merged[k] = merged[k].add(s, fill_value=0.0)
        pr = (p.get("underlying_price") or [None] * len(ds))[: len(ds)]
        ps = pd.Series(pr, index=pd.Index(ds))
        ps = ps[~ps.index.duplicated(keep="last")].sort_index()
        ps = ps.reindex(idx)
        price_acc = ps.combine_first(price_acc)

    def _rnd_series(s):
        return [round(float(x), 2) for x in s.tolist()]

    prices_out = []
    for x in price_acc.tolist():
        if x is None or pd.isna(x):
            prices_out.append(None)
        else:
            prices_out.append(round(float(x), 2))
    return {
        "dates": list(idx),
        "equity": _rnd_series(merged["equity"]),
        "options": _rnd_series(merged["options"]),
        "dividends": _rnd_series(merged["dividends"]),
        "total": _rnd_series(merged["total"]),
        "underlying_price": prices_out,
        "has_underlying_price": bool(price_acc.notna().any()),
    }


# ------------------------------------------------------------------
# User-id-aware tenancy helpers — see docs/USER_ID_TENANCY.md.
#
# These are the security boundary going forward. The legacy
# ``_account_sql_*`` and ``_filter_df_by_accounts`` helpers above filter
# only by ``account`` (a free-form label) — and that string can collide
# across users. Two users with ``account_name = 'investment1'`` would
# each see the other's rows on every page. The cross-tenant guard in
# ``_user_account_list`` hides the conflict at request time, but the
# correct fix is to scope every BigQuery read by the row owner's
# ``user_id`` (Postgres ``users.id``), which is now stamped onto every
# user-tied row through the dbt pipeline.
#
# Stage 0 / 1 leniency: legacy rows in BigQuery still have
# ``user_id IS NULL`` until the operator runs
# ``scripts/backfill_seed_user_ids.py``. The helpers below admit
# ``user_id IS NULL`` rows whose ``account`` matches the user's allowed
# list so the app keeps working during the backfill window. Stage 4
# drops the NULL leg once every seed cell is populated.
# ------------------------------------------------------------------


def _qualified_user_col(col, user_col):
    """If ``col`` is qualified (e.g. ``sc.account``) and ``user_col`` is
    the bare default ``user_id``, prefix ``user_col`` with the same alias
    so the predicate isn't ambiguous in JOINs. Callers can still pass an
    explicit ``user_col`` to override.
    """
    if user_col != "user_id":
        return user_col
    if "." not in col:
        return user_col
    alias = col.rsplit(".", 1)[0]
    return f"{alias}.user_id"


def _user_scoped_filter(user_id, accounts, *, col="account", user_col="user_id"):
    """Return a ``WHERE``-prefixed clause that scopes a BQ read to a tenant.

    Tenant = ``(user_id, account_name)``. ``account_name`` alone is not
    a security boundary — see ``docs/USER_ID_TENANCY.md``.

    Args:
        user_id: ``int`` Postgres ``users.id`` of the current user.
            ``None`` means admin (no user_id predicate).
        accounts: list of account labels the user is allowed to see, or
            ``None`` for admin (no account predicate).
        col: BQ column for ``account``. Defaults to ``account``.
        user_col: BQ column for ``user_id``. Defaults to the alias of
            ``col`` (``sc.user_id`` when ``col="sc.account"``).

    Returns ``""`` when both filters are skipped (admin), else a string
    starting with ``WHERE``.
    """
    user_col = _qualified_user_col(col, user_col)
    parts = []
    if user_id is not None:
        # OR (user_id IS NULL) is the Stage 0/1 leniency leg — drops in
        # Stage 4 once all legacy rows are backfilled.
        parts.append(f"({user_col} = {int(user_id)} OR {user_col} IS NULL)")
    if accounts is None:
        pass
    elif not accounts:
        parts.append("1 = 0")
    else:
        quoted = ", ".join(
            f"'{a.replace(chr(39), chr(39) + chr(39))}'" for a in accounts
        )
        expr = f"TRIM(CAST({col} AS STRING))"
        parts.append(f"{expr} IN ({quoted})")
    if not parts:
        return ""
    return "WHERE " + " AND ".join(parts)


def _user_scoped_and(user_id, accounts, *, col="account", user_col="user_id"):
    """Same shape as ``_user_scoped_filter`` but as an ``AND`` clause for
    joining onto an existing ``WHERE``. Returns ``""`` when both filters
    are skipped.
    """
    user_col = _qualified_user_col(col, user_col)
    parts = []
    if user_id is not None:
        parts.append(f"({user_col} = {int(user_id)} OR {user_col} IS NULL)")
    if accounts is None:
        pass
    elif not accounts:
        parts.append("1 = 0")
    else:
        quoted = ", ".join(
            f"'{a.replace(chr(39), chr(39) + chr(39))}'" for a in accounts
        )
        expr = f"TRIM(CAST({col} AS STRING))"
        parts.append(f"{expr} IN ({quoted})")
    if not parts:
        return ""
    return "AND " + " AND ".join(parts)


def _filter_df_by_user(df, user_id, accounts, *, col="account", user_col="user_id"):
    """DataFrame analogue of ``_user_scoped_filter``.

    Drops rows whose ``user_col`` is a populated id different from
    ``user_id``. Rows with ``user_col`` NULL are kept *only* when their
    ``col`` matches one of ``accounts`` (Stage 0/1 leniency for legacy
    rows in BigQuery that haven't been backfilled yet). Admin
    (``user_id is None``) bypasses the user check.
    """
    if df is None:
        return df
    if df.empty:
        return df
    if user_id is None and accounts is None:
        return df

    out = df

    if user_id is not None and user_col in out.columns:
        target = int(user_id)

        def _norm_uid(v):
            if v is None:
                return None
            if isinstance(v, float) and pd.isna(v):
                return None
            try:
                return int(v)
            except (TypeError, ValueError):
                s = str(v).strip()
                if not s:
                    return None
                try:
                    return int(float(s))
                except (TypeError, ValueError):
                    return None

        norm = out[user_col].map(_norm_uid)
        # Keep rows where user_id matches, OR where user_id is NULL AND
        # the row's account is in the user's allowed list (legacy lenience).
        match_user = norm == target
        if accounts is None:
            keep_null = norm.isna()
        else:
            want = {
                str(a).strip()
                for a in accounts
                if a is not None and str(a).strip() != ""
            }
            if col in out.columns:
                acc_str = out[col].map(
                    lambda v: None
                    if v is None or (isinstance(v, float) and pd.isna(v))
                    else str(v).strip()
                )
                keep_null = norm.isna() & acc_str.isin(want)
            else:
                keep_null = norm.isna()
        out = out[match_user | keep_null]

    if accounts is not None and col in out.columns:
        if not accounts:
            return out.iloc[0:0]
        want = {
            str(a).strip()
            for a in accounts
            if a is not None and str(a).strip() != ""
        }

        def _norm_acc(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            return str(v).strip()

        m = out[col].map(_norm_acc).isin(want)
        out = out[m]

    return out


def _df_normalize_account_column(df):
    """BigQuery to_dataframe() sometimes returns Account; app filters on account."""
    if df is None or df.empty:
        return df
    if "Account" in df.columns and "account" not in df.columns:
        return df.rename(columns={"Account": "account"})
    return df


def _legs_df_to_sessions_list(legs_df):
    """Reshape int_position_legs rows into the legacy ``sessions_list`` dict
    shape that the position_detail template and downstream helpers consume.

    Maintains the historic key contract:
      - ``session_id`` ← ``leg_id``       (positive for equity sessions,
                                           negative for options-only legs)
      - ``display_leg`` ← ``display_leg_num`` (chronological 1..N)
      - ``last_trade_date`` ← ``last_activity_date`` (string YYYY-MM-DD)
      - ``options_pnl`` ← ``closed_options_pnl + open_options_pnl``

    Replaces ~150 lines of stateful Python (orphan-grouping, gap-id
    assignment, P&L overlap re-aggregation) — the dbt mart owns all of
    that now. Returns ``[]`` for an empty / None DataFrame.
    """
    if legs_df is None or legs_df.empty:
        return []

    df = legs_df.copy()
    for col in (
        "equity_pnl", "closed_options_pnl", "open_options_pnl",
        "combined_pnl", "max_quantity_held",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in (
        "options_count", "open_options_count", "num_trades",
        "leg_id", "display_leg_num", "days_held",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    if "display_leg_num" in df.columns:
        df = df.sort_values("display_leg_num")

    out = []
    for _, r in df.iterrows():
        od = r.get("open_date")
        ld = r.get("last_activity_date")
        equity_pnl = round(float(r.get("equity_pnl") or 0), 2)
        options_pnl = round(
            float(r.get("closed_options_pnl") or 0) + float(r.get("open_options_pnl") or 0),
            2,
        )
        combined = round(
            float(r.get("combined_pnl") or (equity_pnl + options_pnl)), 2
        )
        out.append({
            "session_id": int(r["leg_id"]),
            "display_leg": int(r["display_leg_num"]),
            "status": str(r.get("status") or "Closed"),
            "open_date": str(od) if od is not None and not pd.isna(od) else "",
            "last_trade_date": str(ld) if ld is not None and not pd.isna(ld) else "",
            "equity_pnl": equity_pnl,
            "options_pnl": options_pnl,
            "options_count": int(r.get("options_count") or 0),
            "combined_pnl": combined,
            "total_pnl": combined,
            "days_held": int(r.get("days_held") or 0),
            "max_quantity_held": float(r.get("max_quantity_held") or 0),
            "num_trades": int(r.get("num_trades") or 0),
            "options_only": bool(r.get("options_only") or False),
            "open_options_count": int(r.get("open_options_count") or 0),
        })
    return out


def _iter_symbols_for_daily_detail(trades_df, pnl_df, current_df, open_pairs):
    """
    Row keys (account, symbol) for /symbols. dbt can classify open options from
    the current snapshot alone (int_option_contracts.snapshot_only_options) so
    positions_summary has a row with no stg_history rows — the Positions page
    still works. This iterator unions trade-history keys with positions_summary
    and current so Daily Detail matches that catalog.
    """
    seen = set()
    out = []
    if (
        not trades_df.empty
        and "account" in trades_df.columns
        and "symbol" in trades_df.columns
    ):
        for (acc, sym), _ in trades_df.groupby(["account", "symbol"]):
            k = (str(acc), str(sym))
            if open_pairs is not None and k not in open_pairs:
                continue
            if k not in seen:
                seen.add(k)
                out.append((acc, sym))
    for df in (pnl_df, current_df):
        if df is None or df.empty or "account" not in df.columns or "symbol" not in df.columns:
            continue
        for _, row in df.drop_duplicates(["account", "symbol"]).iterrows():
            acc, sym = row["account"], row["symbol"]
            k = (str(acc), str(sym))
            if open_pairs is not None and k not in open_pairs:
                continue
            if k in seen:
                continue
            seen.add(k)
            out.append((acc, sym))
    return out


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
--   * Buy-and-Hold reclassified to "Dividend" when div income > price gain
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
      {account_filter}
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
        account,
        user_id,
        symbol,
        SUM(amount) AS total_dividend_income,
        COUNT(*) AS dividend_count
    FROM `ccwj-dbt.analytics.int_dividend_events`
    WHERE trade_date >= @start_date
      AND trade_date <= @end_date
      {account_filter}
    GROUP BY 1, 2, 3
),

strategy_summary AS (
    SELECT
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
    GROUP BY 1, 2, 3, 4
),

with_dividend_rank AS (
    SELECT
        ss.*,
        ROW_NUMBER() OVER (
            PARTITION BY ss.account, ss.user_id, ss.symbol
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
        ON wdr.account = d.account
        AND (wdr.user_id IS NOT DISTINCT FROM d.user_id)
        AND wdr.symbol = d.symbol
),

final AS (
    SELECT
        wa.account,
        wa.user_id,
        wa.symbol,
        CASE
            WHEN wa.dividend_rank = 1
                 AND wa.strategy = 'Buy and Hold'
                 AND wa.attributed_dividend_income > GREATEST(wa.total_pnl, 0)
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
ORDER BY account, user_id, symbol, strategy
"""

# ------------------------------------------------------------------
# Default (no date filter): use the pre-built mart
# ------------------------------------------------------------------
DEFAULT_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {account_filter}
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
    user_accounts=[],
    status_counts={"Open": 0, "Closed": 0, "Mixed": 0},
    selected_account="",
    selected_strategy="",
    selected_statuses=[],
    selected_symbol="",
    selected_subsector="",
    selected_sector="",
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


def _parse_date(value):
    """Return a date object if value is a valid YYYY-MM-DD string, else None."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None



# ------------------------------------------------------------------
# Feature pages for marketing (logged-out)
# ------------------------------------------------------------------
FEATURES = {
    "strategy-auto-detection": {
        "title": "Strategy auto-detection",
        "subtitle": "Every position classified automatically—no manual tagging.",
        "demo_partial": "features/_demo_strategy.html",
        "value_bullets": [
            "Covered Calls, Cash-Secured Puts, Wheels, spreads, and Buy and Hold—all identified from your trade data.",
            "See exactly which strategies drive your returns and which drain performance.",
            "Stop guessing. Know whether the Wheel is outperforming CSPs for your portfolio.",
        ],
    },
    "ai-trading-insights": {
        "title": "AI trading insights",
        "subtitle": "Personalized analysis of your trading style and performance.",
        "demo_partial": "features/_demo_insights.html",
        "value_bullets": [
            "Get a data-driven overview: what's working, what's leaking, and why.",
            "Observations grounded in your actual trades—not generic advice.",
            "The 'wow' moment when the app shows it truly understands your trading.",
        ],
    },
    "performance-charts": {
        "title": "Performance charts",
        "subtitle": "Cumulative P&L over time, broken down by equity, options, and dividends.",
        "demo_partial": "features/_demo_charts.html",
        "value_bullets": [
            "Visualize your progress. See how each strategy contributes over time.",
            "Portfolio-wide and per-account charts so nothing stays hidden.",
            "The full picture—not just today's balance, but the journey.",
        ],
    },
    "position-detail": {
        "title": "Position detail",
        "subtitle": "Drill into any symbol: trades, strategies, and cumulative P&L.",
        "demo_partial": "features/_demo_position.html",
        "value_bullets": [
            "Click any symbol to see its full story: every trade, every strategy, every dollar.",
            "Understand why a position performed the way it did—before your next move.",
            "Trade history, current positions, and charts in one place.",
        ],
    },
    "multi-account": {
        "title": "Multi-account",
        "subtitle": "Track all your Schwab accounts in one place.",
        "demo_partial": "features/_demo_multiaccount.html",
        "value_bullets": [
            "IRA, taxable, joint—see portfolio-wide metrics and per-account breakdowns.",
            "Filter by account on every view: positions, tax center, performance.",
            "One dashboard. All your accounts.",
        ],
    },
}


@app.route("/features/<slug>")
def feature_detail(slug):
    """Feature detail page with demo and value prop."""
    if slug == "ai-trading-insights" and not app.config.get("INSIGHTS_ENABLED", True):
        abort(404)
    feature = FEATURES.get(slug)
    if not feature:
        abort(404)
    return render_template(
        "features/detail.html",
        title=feature["title"],
        feature=feature,
        all_features=FEATURES,
        current_slug=slug,
    )


@app.route("/pricing")
def pricing():
    """Pricing placeholder for marketing."""
    waitlisted = False
    try:
        if current_user.is_authenticated:
            from app.models import is_user_on_pro_waitlist
            waitlisted = is_user_on_pro_waitlist(current_user.id)
    except Exception:
        waitlisted = False
    return render_template(
        "pricing.html",
        title="Pricing",
        pro_waitlisted=waitlisted,
    )


@app.route("/pro/waitlist", methods=["POST"])
def pro_waitlist():
    """Add an email (or current user) to the Pro tier waitlist."""
    from app.models import add_pro_waitlist_entry
    from app.utils import demo_block_writes

    # Demo: every visitor would be 'demo' on the waitlist, which is noise
    # and would confuse outreach later.
    blocked = demo_block_writes("joining the Pro waitlist")
    if blocked:
        return blocked

    email = (request.form.get("email") or "").strip().lower()
    user_id = current_user.id if current_user.is_authenticated else None

    if not user_id and not email:
        flash("Enter an email address so we can notify you.", "warning")
        return redirect(url_for("pricing"))

    if not user_id:
        # Light email validation
        if "@" not in email or "." not in email or len(email) > 320:
            flash("That email doesn't look right. Try again?", "warning")
            return redirect(url_for("pricing"))

    try:
        add_pro_waitlist_entry(user_id=user_id, email=email or None)
        flash("You're on the waitlist. We'll be in touch when Pro is ready.", "success")
    except Exception as exc:
        app.logger.exception("Pro waitlist signup failed: %s", exc)
        flash("Couldn't add you to the waitlist right now. Try again in a moment.", "danger")

    return redirect(url_for("pricing"))


# ------------------------------------------------------------------
# Beta feedback inbox
# ------------------------------------------------------------------


@app.route("/feedback", methods=["POST"])
@limiter.limit("5 per minute; 30 per hour")
def submit_feedback():
    """
    Footer Send-Feedback button posts here.

    Anonymous users CAN submit (we capture their IP for spam triage) so
    a tester who hits a 500 on a logged-out page can still report it.
    Demo user is allowed — feedback from the demo seat is signal, not
    noise. We hard-cap the body at 4 KB in the model layer.

    Returns JSON for XHR clients (the modal uses fetch) and redirects
    for plain form submits so the route degrades gracefully without JS.
    """
    from app.models import save_feedback

    body = (request.form.get("body") or request.form.get("message") or "").strip()
    page_path = (request.form.get("page_path") or request.referrer or "")[:512]

    user_id = current_user.id if current_user.is_authenticated else None
    username = current_user.username if current_user.is_authenticated else None

    wants_json = (
        request.accept_mimetypes.best == "application/json"
        or request.headers.get("X-Requested-With", "") == "XMLHttpRequest"
    )

    if not body:
        if wants_json:
            return {"ok": False, "error": "Tell us what's up — the message can't be empty."}, 400
        flash("Tell us what's up — the message can't be empty.", "warning")
        return redirect(request.referrer or url_for("index"))

    new_id = save_feedback(
        user_id=user_id,
        username=username,
        body=body,
        page_path=page_path or None,
        user_agent=(request.headers.get("User-Agent") or "")[:512] or None,
        ip_address=request.remote_addr,
    )

    if new_id is None:
        if wants_json:
            return {"ok": False, "error": "We couldn't save that just now. Try again in a minute."}, 500
        flash("We couldn't save that just now. Try again in a minute.", "danger")
        return redirect(request.referrer or url_for("index"))

    if wants_json:
        return {"ok": True, "id": new_id}
    flash("Thanks — feedback received. We read every message.", "success")
    return redirect(request.referrer or url_for("index"))


@app.route("/faq")
def faq():
    """FAQ page for marketing."""
    return render_template("faq.html", title="FAQ")


@app.route("/privacy")
def privacy():
    """Plain-English privacy policy."""
    return render_template("privacy.html", title="Privacy")


@app.route("/terms")
def terms():
    """Plain-English terms of service."""
    return render_template("terms.html", title="Terms")


@app.route("/contact")
def contact():
    """Contact / support page."""
    return render_template("contact.html", title="Contact")


@app.route("/sitemap.xml")
def sitemap():
    """Simple sitemap for SEO."""
    base = request.url_root.rstrip("/")
    pages = [
        ("", "daily", "1.0"),
        ("/pricing", "monthly", "0.8"),
        ("/faq", "monthly", "0.7"),
    ]
    for slug in FEATURES:
        if slug == "ai-trading-insights" and not app.config.get("INSIGHTS_ENABLED", True):
            continue
        pages.append((f"/features/{slug}", "monthly", "0.7"))
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    for path, freq, prio in pages:
        xml += f"  <url><loc>{base}{path}</loc><changefreq>{freq}</changefreq><priority>{prio}</priority></url>\n"
    xml += "</urlset>"
    return Response(xml, mimetype="application/xml")


@app.route("/robots.txt")
def robots():
    """Basic robots.txt for crawlers."""
    base = request.url_root.rstrip("/")
    return Response(
        f"User-agent: *\nAllow: /\nDisallow: /positions\nDisallow: /upload\nDisallow: /insights\nDisallow: /settings\nDisallow: /accounts\nDisallow: /symbols\nDisallow: /position/\nSitemap: {base}/sitemap.xml\n",
        mimetype="text/plain",
    )


@app.route("/")
@app.route("/index")
def index():
    """Public landing page, or redirect to weekly review (home) if logged in."""
    if current_user.is_authenticated:
        return redirect(url_for("weekly_review"))
    return render_template("landing.html", title="Home")


@app.route("/healthz")
def healthz():
    """Liveness probe — does NOT touch DB or BigQuery so it stays green even
    if Postgres is briefly unreachable. Render uses this to know the worker
    process itself is alive."""
    return ("ok", 200, {"Content-Type": "text/plain", "Cache-Control": "no-store"})


@app.route("/healthz/db")
def healthz_db():
    """Readiness probe — confirms Postgres pool can hand out a connection
    in well under gunicorn's request timeout. Returns 503 fast on failure
    rather than hanging the request."""
    from app.db import get_conn
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return ("ok", 200, {"Content-Type": "text/plain", "Cache-Control": "no-store"})
    except Exception as exc:
        app.logger.warning("healthz/db failed: %s", exc)
        return (f"db_unavailable: {exc.__class__.__name__}", 503,
                {"Content-Type": "text/plain", "Cache-Control": "no-store"})



@app.route("/get-started")
@login_required
def get_started():
    """Onboarding checklist for new users — tracks real progress."""
    user_accounts = get_accounts_for_user(current_user.id)
    has_uploaded = len(user_accounts) > 0

    # Check if data is actually available in BigQuery. We swallow the
    # exception so a transient BQ outage doesn't break the onboarding
    # page (the user can still see step 1/2/3 and the "refresh to check"
    # link), but the failure is logged so the operator can spot a
    # genuinely stuck pipeline. AGENTS.md flagged the silent pass as
    # known debt — replace with a logged warning.
    has_data = False
    if has_uploaded:
        try:
            client = get_bigquery_client()
            where = _account_sql_filter(user_accounts)
            check_q = f"SELECT COUNT(*) AS cnt FROM `ccwj-dbt.analytics.positions_summary` {where}"
            result = client.query(check_q).to_dataframe()
            has_data = int(result.iloc[0]["cnt"]) > 0 if not result.empty else False
        except Exception as exc:
            app.logger.warning(
                "get_started has_data check failed for user_id=%s: %s",
                current_user.id, exc,
            )

    schwab_enabled = bool(os.environ.get("SCHWAB_APP_KEY") and os.environ.get("SCHWAB_APP_SECRET"))
    schwab_connected = bool(
        schwab_enabled and get_schwab_connections(current_user.id)
    )
    schwab_full_history_days = SCHWAB_FULL_HISTORY_LOOKBACK_DAYS
    schwab_routine_days = _schwab_transaction_lookback_days()

    return render_template(
        "get_started.html",
        title="Get Started",
        has_uploaded=has_uploaded,
        has_data=has_data,
        schwab_enabled=schwab_enabled,
        schwab_connected=schwab_connected,
        schwab_full_history_days=schwab_full_history_days,
        schwab_routine_days=schwab_routine_days,
    )


@app.route("/ping")
@limiter.exempt
def ping():
    return "Flask app is alive"


@app.route("/positions")
@login_required
def positions():
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    acct_filter = _account_sql_and(user_accounts)

    # ------------------------------------------------------------------
    # 1. Read filter params
    # ------------------------------------------------------------------
    selected_account = request.args.get("account", "")
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
            df = client.query(DATE_FILTERED_QUERY.format(account_filter=acct_filter), job_config=job_config).to_dataframe()
        else:
            df = client.query(DEFAULT_QUERY.format(account_filter=acct_filter)).to_dataframe()
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
    df = _filter_df_by_accounts(df, user_accounts)

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

    filtered = df.copy()
    if selected_account:
        filtered = filtered[filtered["account"] == selected_account]
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
        symbol_agg = (
            filtered.groupby(["account", "symbol"])
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
        strat_agg = (
            filtered.groupby(["account", "strategy"])
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


# ======================================================================
# Position Detail  (/position/<symbol>)
# ======================================================================

POSITION_SUMMARY_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
    {account_filter}
    ORDER BY account, strategy
"""

POSITION_TRADES_QUERY = """
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
    WHERE trade_date IS NOT NULL
      AND (
        UPPER(TRIM(COALESCE(underlying_symbol, ''))) = UPPER(TRIM('{symbol}'))
        OR UPPER(TRIM(SPLIT(COALESCE(trade_symbol, ''), ' ')[SAFE_OFFSET(0)])) = UPPER(TRIM('{symbol}'))
      )
    {account_filter}
    ORDER BY trade_date DESC
"""

POSITION_CURRENT_QUERY = """
    SELECT
        account,
        user_id,
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
    WHERE UPPER(TRIM(COALESCE(underlying_symbol, ''))) = UPPER(TRIM('{symbol}'))
    {account_filter}
"""

POSITION_CLOSED_LEGS_QUERY = """
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
      ON sc.account = oc.account
     AND sc.trade_symbol = oc.trade_symbol
     AND sc.user_id IS NOT DISTINCT FROM oc.user_id
    WHERE sc.status = 'Closed'
      AND sc.trade_group_type = 'option_contract'
      AND UPPER(TRIM(COALESCE(sc.symbol, ''))) = UPPER(TRIM('{symbol}'))
    {sc_account_filter}
"""

POSITION_CLOSED_EQUITY_QUERY = """
    SELECT
        account,
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
    {account_filter}
"""

POSITION_LEGS_QUERY = """
    SELECT
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
    {account_filter}
    ORDER BY account, display_leg_num
"""

POSITION_MATRIX_QUERY = """
    SELECT
        account,
        underlying_symbol,
        strategy,
        trade_symbol,
        dte_at_open,
        dte_bucket,
        strike_distance,
        underlying_price_at_open,
        pnl_pct,
        total_pnl,
        direction,
        option_type,
        outcome
    FROM `ccwj-dbt.analytics.int_option_trade_kinds`
    WHERE status = 'Closed'
      AND strike_distance IS NOT NULL
      AND UPPER(TRIM(COALESCE(underlying_symbol, ''))) = UPPER(TRIM('{symbol}'))
    {account_filter}
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
    # to "Dividend" when dividend income > trade gain. They occupy the same
    # equity-strategy slot in the breakdown — only one of them can ever exist
    # for a given (account, symbol). Track which accounts already have one
    # so we don't synthesize a duplicate Buy-and-Hold row alongside a real
    # Dividend row from the mart.
    EQUITY_BUCKET = ("Buy and Hold", "Dividend")
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
    client, safe_symbol: str, user_accounts
) -> pd.DataFrame:
    """User-scoped rows from int_strategy_classification for one symbol. Used when
    positions_summary is empty but we still need strategy breakdown (mart lag / path gaps).
    """
    if user_accounts is not None and not user_accounts:
        return pd.DataFrame()
    acct = _account_sql_and(user_accounts, col="account")
    sql = f"""
    SELECT
        account, symbol, strategy, status, total_pnl, num_trades, is_winner,
        premium_received, premium_paid, days_in_trade, open_date, close_date
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{safe_symbol}'))
    {acct}
    """
    try:
        df = client.query(sql).to_dataframe()
    except Exception as exc:
        app.logger.exception(
            "int_strategy_classification by symbol failed for %s: %s", safe_symbol, exc
        )
        return pd.DataFrame()
    df = _df_normalize_account_column(df)
    return _filter_df_by_accounts(df, user_accounts)


def _fetch_closed_option_legs_from_classification(
    client, safe_symbol: str, user_accounts
) -> pd.DataFrame:
    """Closed option contract rows from int_strategy_classification only (no join).

    POSITION_CLOSED_LEGS joins to int_option_contracts. When that join misses (drift,
    renames, or partial loads), the page loses all closed option history. This query
    matches the P&L in classification and is the same grain as the join: one row per
    closed option trade group.
    """
    if user_accounts is not None and not user_accounts:
        return pd.DataFrame()
    acct = _account_sql_and(user_accounts, col="sc.account")
    sql = f"""
    SELECT
        sc.account,
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
        df = client.query(sql).to_dataframe()
    except Exception as exc:
        app.logger.exception(
            "closed option legs fallback (classification) failed for %s: %s",
            safe_symbol,
            exc,
        )
        return pd.DataFrame()
    df = _df_normalize_account_column(df)
    return _filter_df_by_accounts(df, user_accounts)


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
    mart lags (common right after a Schwab/CSV seed commit, before dbt rebuilds).

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
    _EQUITY_STRAT_SLOT = frozenset({"Buy and Hold", "Dividend"})
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
    rows = []
    for _, r in current_df.iterrows():
        acct = str(r.get("account", "") or "").strip()
        it = str(r.get("instrument_type", "") or "")
        if it == "Call":
            lab = "Long Call"
        elif it == "Put":
            lab = "Long Put"
        elif it == "Equity":
            lab = "Buy and Hold"
        else:
            lab = "Open"
        u = float(r.get("unrealized_pnl") or 0)
        sym = str(r.get("symbol", "") or "").strip()
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
    strat_accounts_scope,
    closed_equity_df: pd.DataFrame,
    closed_legs_df: pd.DataFrame,
    current_df: pd.DataFrame,
    leg_predicate,
):
    """Build the Equity / Options / Dividends rollup the position page renders
    above Strategy Breakdown.

    All P&L source frames passed in here are already leg-filtered by the
    caller (closed_legs_df, closed_equity_df by date overlap; current_df is
    cleared in routes.py when no selected leg is Open). For dividends we
    have to do the leg-scope here because there is no per-row dividend
    frame upstream — int_dividend_events is queried directly and filtered
    by ``trade_date`` against ``leg_predicate``.

    leg_predicate: callable(date) -> bool when leg-filtered, else None.
    When None, every dividend event for the symbol counts.

    Returns a list of dict rows ready for Jinja:
        type, total, realized, unrealized, count, count_label, count_open
    Empty list when there is no activity at all (page won't render the card).

    Numbers should sum to the page-level kpis['total_return'] within
    rounding (positions_summary uses rounded P&L per strategy; the mart's
    open_options unrealized has full precision).
    """
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

    div_total = 0.0
    div_count = 0
    if strat_accounts_scope is not None and len(strat_accounts_scope) > 0:
        try:
            acct_filter = _account_sql_and(strat_accounts_scope, col="account")
            div_df = client.query(
                """
                SELECT account, user_id, symbol, trade_date, amount
                FROM `ccwj-dbt.analytics.int_dividend_events`
                WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
                {account_filter}
                """.format(symbol=safe_symbol, account_filter=acct_filter)
            ).to_dataframe()
            # Belt-and-suspenders tenancy guard. The SQL is already user_id +
            # account scoped via _account_sql_and, but the BQ-tenant rule
            # requires a Python filter on every BQ result before any
            # re-aggregation. See .cursor/rules/bigquery-tenant-isolation.mdc.
            div_df = _filter_df_by_accounts(div_df, strat_accounts_scope)
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

    return [
        {
            "type": "Equity",
            "total": round(eq_total, 2),
            "realized": round(eq_realized, 2),
            "unrealized": round(eq_unrealized, 2),
            "count": eq_session_count,
            "count_label": "session" if eq_session_count == 1 else "sessions",
            "count_open": eq_open_count,
        },
        {
            "type": "Options",
            "total": round(opt_total, 2),
            "realized": round(opt_realized, 2),
            "unrealized": round(opt_unrealized, 2),
            "count": opt_count,
            "count_label": "contract" if opt_count == 1 else "contracts",
            "count_open": opt_open_count,
        },
        {
            "type": "Dividends",
            "total": round(div_total, 2),
            "realized": round(div_total, 2),
            # Dividends are realized cash income — no mark-to-market component,
            # so leave a sentinel the template can render as an em-dash.
            "unrealized": None,
            "count": div_count,
            "count_label": "event" if div_count == 1 else "events",
            "count_open": 0,
        },
    ]


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
CHART_DATA_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.mart_daily_pnl`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
      {account_filter}
    ORDER BY date
"""

# Pre-aggregated daily P&L data for all symbols (account-level charts)
CHART_DATA_ALL_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.mart_daily_pnl`
    WHERE 1=1 {account_filter}
    ORDER BY symbol, date
"""


@app.route("/position/<symbol>")
@login_required
def position_detail(symbol):
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()

    # Escape symbol for SQL (prevent injection)
    safe_symbol = symbol.replace("'", "''")

    # When the URL pins a specific account (drill-in from /positions or any
    # bookmarked link), narrow the position-level fetches to JUST that account
    # rather than the viewer's full account list. Two reasons:
    #
    # 1. **Admin scope.** `_account_sql_and` drops the user_id predicate for
    #    admins, so a non-admin's data falls in. But the account-name filter
    #    is built from `user_accounts` = the viewer's PERSONAL account list
    #    (Postgres `user_accounts`). For an admin viewing an account they
    #    don't personally own (e.g. happycameron viewing Sara Investment),
    #    the filter `account IN ('investment1')` returns ZERO rows and the
    #    page silently shows empty current_df / closed_legs_df / etc. while
    #    Strategy Breakdown and the chart (which already use
    #    `[selected_account]`) populate normally. The reconciliation invariant
    #    catches the resulting Strategy=$X / Breakdown-by-Type=$0 mismatch
    #    on the page, but the right fix is to scope the position-level fetch
    #    consistently with strat/chart.
    # 2. **Non-admin bookmark.** A non-admin pasting `?account=...` for an
    #    account they don't own gets `_account_sql_and` adding their
    #    `user_id` predicate AND `account IN (...)`. SQL returns 0 rows;
    #    page renders empty. No tenancy leak, just no data — same as today.
    #
    # `_account_sql_and` continues to enforce `(user_id = X OR user_id IS NULL)`
    # for non-admins, so this change does NOT widen the security boundary.
    pos_accounts_scope = (
        [request.args.get("account", "").strip()]
        if request.args.get("account", "").strip()
        else user_accounts
    )

    try:
        _pos_acct = _account_sql_and(pos_accounts_scope, col="account")
        _pos_sc_acct = _account_sql_and(pos_accounts_scope, col="sc.account")
        dfs = _bq_parallel(client, {
            "summary": POSITION_SUMMARY_QUERY.format(
                symbol=safe_symbol, account_filter=_pos_acct
            ),
            "trades": POSITION_TRADES_QUERY.format(
                symbol=safe_symbol, account_filter=_pos_acct
            ),
            "current": POSITION_CURRENT_QUERY.format(
                symbol=safe_symbol, account_filter=_pos_acct
            ),
            "closed_legs": POSITION_CLOSED_LEGS_QUERY.format(
                symbol=safe_symbol, sc_account_filter=_pos_sc_acct
            ),
            "closed_equity": POSITION_CLOSED_EQUITY_QUERY.format(
                symbol=safe_symbol, account_filter=_pos_acct
            ),
            "matrix": POSITION_MATRIX_QUERY.format(
                symbol=safe_symbol, account_filter=_pos_acct
            ),
            "legs": POSITION_LEGS_QUERY.format(
                symbol=safe_symbol, account_filter=_pos_acct
            ),
        })
        summary_df = dfs["summary"]
        trades_df = dfs["trades"]
        current_df = dfs["current"]
        closed_legs_df = dfs["closed_legs"]
        closed_equity_df = dfs["closed_equity"]
        matrix_df = dfs["matrix"]
        legs_df = dfs["legs"]
        summary_df = _df_normalize_account_column(summary_df)
        trades_df = _df_normalize_account_column(trades_df)
        current_df = _df_normalize_account_column(current_df)
        closed_legs_df = _df_normalize_account_column(closed_legs_df)
        closed_equity_df = _df_normalize_account_column(closed_equity_df)
        matrix_df = _df_normalize_account_column(matrix_df)
        legs_df = _df_normalize_account_column(legs_df)
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
            selected_legs=[],
            leg_param="",
            chart_data_json="{}",
            has_underlying_price=False,
            symbol_sector="",
            symbol_subsector="",
            symbol_company="",
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
    # Use ``pos_accounts_scope`` so admin viewing a non-personal selected_account
    # doesn't strip the just-fetched rows. ``_filter_df_by_accounts`` still
    # enforces the user_id boundary for non-admins.
    summary_df = _filter_df_by_accounts(summary_df, pos_accounts_scope)
    trades_df = _filter_df_by_accounts(trades_df, pos_accounts_scope)
    current_df = _filter_df_by_accounts(current_df, pos_accounts_scope)
    closed_legs_df = _filter_df_by_accounts(closed_legs_df, pos_accounts_scope)
    closed_equity_df = _filter_df_by_accounts(closed_equity_df, pos_accounts_scope)
    matrix_df = _filter_df_by_accounts(matrix_df, pos_accounts_scope)

    # Joined closed legs are empty: int_option_contracts can fail to match while
    # int_strategy_classification still has closed option P&L — use classification only.
    if closed_legs_df.empty and (
        pos_accounts_scope is None
        or (isinstance(pos_accounts_scope, list) and len(pos_accounts_scope) > 0)
    ):
        _cl_sup = _fetch_closed_option_legs_from_classification(
            client, safe_symbol, pos_accounts_scope
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

    if selected_account:
        summary_df = summary_df[summary_df["account"] == selected_account]
        trades_df = trades_df[trades_df["account"] == selected_account]
        current_df = current_df[current_df["account"] == selected_account]
    if not current_df.empty:
        current_df = _dedupe_enriched_current_positions(current_df)
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
    legs_df = _filter_df_by_accounts(legs_df, pos_accounts_scope)
    if selected_account and not legs_df.empty:
        legs_df = legs_df[legs_df["account"] == selected_account]

    sessions_list = _legs_df_to_sessions_list(legs_df)

    leg_param = request.args.get("leg", "")
    if leg_param:
        selected_legs = []
        for x in leg_param.split(","):
            x = x.strip()
            try:
                selected_legs.append(int(x))
            except ValueError:
                pass
    else:
        selected_legs = [s["session_id"] for s in sessions_list]

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
    if selected_account:
        if not closed_legs_df.empty:
            closed_legs_df = closed_legs_df[closed_legs_df["account"] == selected_account]
        if not closed_equity_df.empty:
            closed_equity_df = closed_equity_df[closed_equity_df["account"] == selected_account]
    if selected_strategy and not closed_legs_df.empty and "strategy" in closed_legs_df.columns:
        closed_legs_df = closed_legs_df[closed_legs_df["strategy"] == selected_strategy]
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
            trade_count = len(trades_df)
            if trade_count == 0 and not trades_pre_leg.empty:
                trade_count = len(trades_pre_leg)
        else:
            trade_count = int(
                summary_df["num_individual_trades"].sum()
            ) if "num_individual_trades" in summary_df.columns else 0
            if trade_count == 0 and not trades_pre_leg.empty:
                trade_count = len(trades_pre_leg)

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

        # Stg row count for hero; if summary says 0 trades but legs exist, show leg count.
        _fills = len(trades_pre_leg) if not trades_pre_leg.empty else 0
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
    #    lags by a dbt run (common right after a Schwab/CSV seed commit).
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
    # Account scoping: when the user has filtered to a single account, the
    # classification fetch MUST be restricted to that account too. Otherwise
    # int_strategy rows from the user's other accounts get rolled in and the
    # page shows mixed-account rows even though the URL is scoped
    # (`?account=X`) — the JEPI/0044 visible bug.
    strat_accounts_scope = (
        [selected_account] if selected_account else user_accounts
    )
    if leg_param and _leg_ranges:
        summary_for_strat = pd.DataFrame()
        if strat_accounts_scope is not None and len(strat_accounts_scope) > 0:
            int_raw = _fetch_int_strategy_classification_by_symbol(
                client, safe_symbol, strat_accounts_scope
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
        if strat_accounts_scope is not None and len(strat_accounts_scope) > 0:
            int_raw = _fetch_int_strategy_classification_by_symbol(
                client, safe_symbol, strat_accounts_scope
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
        strat_accounts_scope=strat_accounts_scope,
        closed_equity_df=closed_equity_df,
        closed_legs_df=closed_legs_df,
        current_df=current_df,
        leg_predicate=(_in_leg_range if (leg_param and _leg_ranges) else None),
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
        acct_filter = _account_sql_and([selected_account] if selected_account else user_accounts)
        chart_df = client.query(
            CHART_DATA_QUERY.format(symbol=safe_symbol, account_filter=acct_filter)
        ).to_dataframe()
        chart_df = _filter_df_by_accounts(chart_df, pos_accounts_scope)
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
            chart_data = _build_chart_from_daily_pnl(chart_df, current_df)
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

    # Trade history rows
    trades_for_table = trades_df.copy()
    if "trade_date" in trades_for_table.columns:
        trades_for_table["trade_date"] = trades_for_table["trade_date"].astype(str)
    trades = trades_for_table.to_dict(orient="records") if not trades_for_table.empty else []

    # Current positions
    current_positions = current_df.to_dict(orient="records") if not current_df.empty else []

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
            "account": str(leg.get("account") or "").strip(),
        })
    trade_outcomes.sort(key=lambda x: x.get("close_date") or "", reverse=True)

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
    if selected_account:
        matrix_df = matrix_df[matrix_df["account"] == selected_account] if not matrix_df.empty else matrix_df
    # Filter matrix by selected legs (date range overlap via trade_symbol matching closed legs)
    if leg_param and _leg_ranges and not matrix_df.empty:
        filtered_trade_syms = set(r.get("trade_symbol") for r in closed_legs_list)
        if "trade_symbol" in matrix_df.columns:
            matrix_df = matrix_df[matrix_df["trade_symbol"].isin(filtered_trade_syms)]
    _matrix_default_account = ""
    if user_accounts and len(user_accounts) == 1:
        _matrix_default_account = user_accounts[0]
    option_matrices = _build_option_matrices(
        matrix_df, selected_account or _matrix_default_account, symbol
    ) if not matrix_df.empty else []
    if not option_matrices and not matrix_df.empty:
        all_mats = []
        for acct in matrix_df["account"].unique():
            all_mats.extend(_build_option_matrices(matrix_df, acct, symbol))
        seen = set()
        for m in all_mats:
            if m["strategy"] not in seen:
                seen.add(m["strategy"])
                option_matrices.append(m)

    # Available accounts for filter (summary may be empty for open-only Schwab lots)
    if not summary_df.empty and "account" in summary_df.columns:
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
        selected_legs=selected_legs,
        leg_param=leg_param,
        chart_data_json=json.dumps(_chart_data_for_json(chart_data)),
        has_underlying_price=chart_data.get("has_underlying_price", False),
        prices_through_date=prices_through_date,
        accounts=all_accounts,
        selected_account=selected_account,
        symbol_sector=symbol_sector,
        symbol_subsector=symbol_subsector,
        symbol_company=symbol_company,
        invariant_warning=invariant_warning,
        viewer_is_admin=is_admin(current_user.username),
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
      {account_filter}
    ORDER BY underlying_symbol, trade_date
"""

OPEN_SESSION_START_QUERY = """
    SELECT
        account,
        symbol,
        MIN(open_date) AS open_start
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE status = 'Open'
      {account_filter}
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
      ON sc.account = oc.account
     AND sc.trade_symbol = oc.trade_symbol
     AND sc.user_id IS NOT DISTINCT FROM oc.user_id
    WHERE sc.status = 'Closed'
      AND sc.trade_group_type = 'option_contract'
      {closed_legs_account_filter}
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
    WHERE 1=1 {account_filter}
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
    WHERE 1=1 {account_filter}
"""

STRATEGIES_MAP_QUERY = """
    SELECT account, symbol, strategy
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {account_filter}
"""

SYMBOLS_PNL_QUERY = """
    SELECT account, symbol, status, realized_pnl, unrealized_pnl
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {account_filter}
"""

def _build_option_matrices(matrix_df, account, symbol):
    """Build per-strategy DTE × Strike-Distance heatmap matrices for one position."""
    import math

    df = matrix_df[
        (matrix_df["account"] == account)
        & (matrix_df["underlying_symbol"] == symbol)
    ].copy()
    if df.empty:
        return []

    DTE_BINS = [
        (0, 7, "0–7"),
        (8, 14, "8–14"),
        (15, 30, "15–30"),
        (31, 60, "31–60"),
        (61, 999, "61+"),
    ]

    def dte_label(dte):
        for lo, hi, lbl in DTE_BINS:
            if lo <= dte <= hi:
                return lbl
        return "61+"

    # Bucket strike distance as % of the underlying at open so the matrix
    # is readable across symbols of any price (a $50 distance means very
    # different things on a $50 stock vs a $500 stock). Buckets are signed
    # to preserve ITM vs OTM directionality.
    PCT_BINS = [
        (-9999, -10, "<-10%"),
        (-10, -5, "-10 to -5%"),
        (-5, -2, "-5 to -2%"),
        (-2, 2, "ATM ±2%"),
        (2, 5, "+2 to +5%"),
        (5, 10, "+5 to +10%"),
        (10, 9999, ">+10%"),
    ]

    def strike_bucket(row):
        dist = row.get("strike_distance")
        underlying = row.get("underlying_price_at_open")
        if dist is None or not underlying or underlying <= 0:
            return "—"
        pct = (dist / underlying) * 100
        for lo, hi, lbl in PCT_BINS:
            if lo <= pct < hi:
                return lbl
        return "—"

    matrices = []
    for strategy, grp in df.groupby("strategy"):
        grp = grp.copy()
        grp["dte_label"] = grp["dte_at_open"].apply(dte_label)
        grp["strike_col"] = grp.apply(strike_bucket, axis=1)

        # Preserve PCT_BINS order so columns read left-to-right ITM → OTM
        col_range = [lbl for _, _, lbl in PCT_BINS if lbl in grp["strike_col"].values]
        if "—" in grp["strike_col"].values and "—" not in col_range:
            col_range.append("—")

        dte_order = [lbl for _, _, lbl in DTE_BINS if lbl in grp["dte_label"].values]

        rows = []
        for dte_lbl in dte_order:
            cells = []
            for col_val in col_range:
                bucket = grp[
                    (grp["dte_label"] == dte_lbl) & (grp["strike_col"] == col_val)
                ]
                if bucket.empty:
                    cells.append({"count": 0, "avg_pnl": None, "win_rate": None})
                else:
                    wins = int((bucket["total_pnl"] > 0).sum())
                    total = len(bucket)
                    avg_pnl_dollar = bucket["total_pnl"].mean()
                    cells.append({
                        "count": total,
                        "avg_pnl": round(float(avg_pnl_dollar), 0) if not math.isnan(avg_pnl_dollar) else None,
                        "win_rate": round(wins / total * 100, 0),
                        "wins": wins,
                    })
            rows.append({"dte_label": dte_lbl, "cells": cells})

        matrices.append({
            "strategy": strategy,
            "trade_count": len(grp),
            "col_headers": col_range,
            "rows": rows,
        })

    return matrices


def _chart_data_for_json(obj):
    """Recursively make chart data JSON/JS-safe (NaN/Inf break Chart.js parsing)."""
    if isinstance(obj, dict):
        return {k: _chart_data_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_chart_data_for_json(x) for x in obj]
    if isinstance(obj, bool) or obj is None:
        return obj
    if isinstance(obj, int):
        return obj
    try:
        f = float(obj)
    except (TypeError, ValueError):
        return obj
    if not math.isfinite(f):
        return None
    return f


def _collect_activity_candidate_dates(
    trades_pre_leg, closed_legs_pre_leg, closed_equity_pre_leg, sessions_list
):
    """
    Dates that represent when the user first/last touched this symbol, using
    trades and strategy metadata before leg scoping. Used when summary/stg rows
    are missing or leg-filtered to empty.
    """
    out = []
    if (
        trades_pre_leg is not None
        and not trades_pre_leg.empty
        and "trade_date" in trades_pre_leg.columns
    ):
        td = pd.to_datetime(trades_pre_leg["trade_date"], errors="coerce")
        out.extend([x for x in td.dropna().dt.date.tolist() if x is not None])
    for df, cols in (
        (closed_legs_pre_leg, ("open_date", "close_date")),
        (closed_equity_pre_leg, ("open_date", "close_date")),
    ):
        if df is None or df.empty:
            continue
        for c in cols:
            if c not in df.columns:
                continue
            for v in df[c].dropna():
                ts = pd.to_datetime(v, errors="coerce")
                if pd.isna(ts):
                    continue
                try:
                    out.append(ts.date())
                except Exception:
                    pass
    for s in sessions_list or []:
        for key in ("open_date", "last_trade_date"):
            v = s.get(key)
            if not v:
                continue
            ts = pd.to_datetime(v, errors="coerce")
            if pd.isna(ts):
                continue
            try:
                out.append(ts.date())
            except Exception:
                pass
    return [d for d in out if d is not None]


def _synthetic_cumulative_pnl_for_position(kpis, sessions_list, leg_param, selected_legs, current_df):
    """
    When mart_daily_pnl has no rows in-range (new leg, pipeline lag, leg filter) or
    the chart query failed, draw a 2-point cumulative P&L line consistent with KPIs.
    """
    empty = {
        "dates": [], "equity": [], "options": [], "dividends": [],
        "total": [], "underlying_price": [], "has_underlying_price": False,
    }
    if not kpis:
        return empty

    realized = float(kpis.get("realized_pnl") or 0)
    unreal = float(kpis.get("unrealized_pnl") or 0)
    div_d = float(kpis.get("dividend_income") or 0)
    tot_end = round(float(kpis.get("total_return") or 0), 2)

    eq_unreal = 0.0
    opt_unreal = 0.0
    if (
        not current_df.empty
        and "instrument_type" in current_df.columns
        and "unrealized_pnl" in current_df.columns
    ):
        eq_df = current_df[current_df["instrument_type"] == "Equity"]
        op_df = current_df[current_df["instrument_type"].isin(["Call", "Put"])]
        if not eq_df.empty:
            eq_unreal = float(eq_df["unrealized_pnl"].sum())
        if not op_df.empty:
            opt_unreal = float(op_df["unrealized_pnl"].sum())
    if abs(eq_unreal + opt_unreal - unreal) > 0.02:
        eq_unreal, opt_unreal = unreal, 0.0

    eq_end = round(realized + eq_unreal, 2)
    opt_end = round(opt_unreal, 2)

    start_d = None
    if leg_param and sessions_list and selected_legs:
        ods = []
        for s in sessions_list:
            if s.get("session_id") in selected_legs and s.get("open_date"):
                try:
                    ods.append(pd.to_datetime(s["open_date"]).date())
                except Exception:
                    pass
        if ods:
            start_d = min(ods)
    if start_d is None and kpis.get("first_trade"):
        try:
            start_d = pd.to_datetime(kpis["first_trade"]).date()
        except Exception:
            start_d = None

    end_d = date.today()
    if start_d is None:
        start_d = end_d - timedelta(days=1) if end_d > date(2000, 1, 2) else end_d
    if start_d > end_d:
        start_d = end_d
    if start_d == end_d:
        start_d = end_d - timedelta(days=1) if end_d > date(2000, 1, 2) else end_d

    d0, d1 = str(start_d), str(end_d)

    p0, p1 = None, None
    if not current_df.empty and "instrument_type" in current_df.columns and "current_price" in current_df.columns:
        eqp = current_df[current_df["instrument_type"] == "Equity"]
        if not eqp.empty:
            c = float(eqp["current_price"].iloc[0] or 0)
            if c > 0:
                p1 = round(c, 2)

    return {
        "dates": [d0, d1],
        "equity": [0.0, eq_end],
        "options": [0.0, opt_end],
        "dividends": [0.0, div_d],
        "total": [0.0, tot_end],
        "underlying_price": [p0, p1],
        "has_underlying_price": p1 is not None,
    }


CHART_SUBSTITUTION_KPI_MARGIN = 25.0  # slack when judging mart vs substitute vs KPI headline


def _chart_data_terminal(chart_data):
    """Last ``total`` point from cumulative P&amp;L chart payload, or 0."""
    if not chart_data:
        return 0.0
    pts = chart_data.get("total")
    if not pts:
        return 0.0
    try:
        return round(float(pts[-1] or 0), 2)
    except Exception:
        return 0.0


CHART_KPI_ALIGN_TOLERANCE_DOLLARS = 1.00


def _align_position_pnl_chart_with_kpi(chart_data, kpis):
    """
    Cosmetic rounding-noise reconciliation between the chart's terminal value
    and the page's KPI ``total_return``. Bounded: above
    ``CHART_KPI_ALIGN_TOLERANCE_DOLLARS`` of disagreement we DO NOT rescale —
    we leave the chart untouched so the page-level invariant card surfaces
    the structural disagreement instead of silently distorting the series.

    History (May 2026):
      This function used to unconditionally rescale the chart's equity /
      options / dividends streams by ``f = kpi / chart_total[-1]``,
      effectively forcing the chart's terminal value to match the KPI no
      matter how big the gap. That hid a real bug in BE/Sara where
      ``mart_daily_pnl`` was sourcing today's close from yfinance ($283.92)
      while the KPI sourced today's price from broker ($262.70),
      producing a chart_total of $11,709 silently rescaled to $7,465.
      Every per-day equity/options point on the chart was then ~36%
      smaller than the math actually produced — meaningless cosmetic
      values that "happened" to sum to the KPI. The rescale was a band-aid
      over a structural bug; removing the band-aid surfaced the bug, which
      was then fixed at source (`mart_daily_pnl.sql` "PRICE PRECEDENCE"
      comment + `int_option_contracts.sql` open-contract total_pnl).

      After those source fixes, the chart's terminal value reconciles to
      the KPI by construction. The only legitimate disagreement is
      sub-dollar rounding noise (sequential 2dp rounding through several
      pandas / Jinja layers), which this function still absorbs.

      If you find this function firing on a real position, that's signal:
      either a new yfinance/broker source split has been introduced, or
      another rounding-precision drift has appeared upstream. Investigate
      the upstream source rather than widening the tolerance here.
    """
    if not chart_data or not kpis or not chart_data.get("total"):
        return
    n = len(chart_data["total"])
    if n < 1:
        return
    t_end = float(chart_data["total"][-1] or 0.0)
    k = float(kpis.get("total_return", 0) or 0.0)
    gap = abs(t_end - k)
    if gap <= 0.02:
        return
    if gap > CHART_KPI_ALIGN_TOLERANCE_DOLLARS:
        # Structural disagreement, not rounding. DO NOT rescale.
        # The page-level invariant card in position_detail will surface
        # this on the rendered page (admin-only). Log here too so the
        # disagreement is searchable in production logs even when the
        # admin canary doesn't fire (e.g. non-admin viewer, or the
        # invariant card itself has a bug).
        try:
            app.logger.warning(
                "_align_position_pnl_chart_with_kpi: refusing to rescale "
                "chart series \u2014 gap of $%.2f exceeds tolerance $%.2f. "
                "chart_terminal=$%.2f, kpi_total_return=$%.2f. "
                "This indicates a real source disagreement (broker vs "
                "yfinance, rounding-precision drift, or duplicate rows). "
                "Investigate upstream rather than widening the tolerance.",
                gap, CHART_KPI_ALIGN_TOLERANCE_DOLLARS, t_end, k,
            )
        except Exception:
            pass
        return

    # Sub-dollar gap: real rounding noise. Apply the legacy rescale logic
    # so the chart cosmetically agrees with the KPI to the cent.
    if abs(t_end) < 1e-9:
        # Edge case: chart terminal is ~0 but KPI isn't (e.g. all-realized
        # closed-leg series with open-only KPI). Can't compute a scale
        # factor; place the KPI delta on the most-active stream so the
        # stacked sum matches `total`.
        if abs(k) > 0.02 and n >= 1:
            tlist = [0.0] * (n - 1) + [round(k, 2)]
            chart_data["total"] = tlist
            e_abs = sum(
                abs(float(x or 0)) for x in (chart_data.get("equity") or [0.0] * n)[:n]
            )
            o_abs = sum(
                abs(float(x or 0)) for x in (chart_data.get("options") or [0.0] * n)[:n]
            )
            d_abs = sum(
                abs(float(x or 0)) for x in (chart_data.get("dividends") or [0.0] * n)[:n]
            )
            for key in ("equity", "options", "dividends"):
                if key in chart_data and len(chart_data.get(key) or []) == n:
                    chart_data[key] = [0.0] * n
            mx = max(d_abs, e_abs, o_abs)
            if mx < 1e-9:
                if "options" in chart_data and len(chart_data["options"]) == n:
                    chart_data["options"][-1] = round(k, 2)
                elif "equity" in chart_data and len(chart_data["equity"]) == n:
                    chart_data["equity"][-1] = round(k, 2)
                elif "dividends" in chart_data and len(chart_data["dividends"]) == n:
                    chart_data["dividends"][-1] = round(k, 2)
            else:
                _tie = {"options": 0, "equity": 1, "dividends": 2}
                streams = [
                    (d_abs, "dividends"),
                    (e_abs, "equity"),
                    (o_abs, "options"),
                ]
                streams.sort(key=lambda t: (-t[0], _tie.get(t[1], 9)))
                for _score, sname in streams:
                    if sname in chart_data and len(chart_data[sname]) == n:
                        chart_data[sname][-1] = round(k, 2)
                        break
        return
    f = k / t_end
    if not all(
        len(chart_data.get(skey) or []) == n
        for skey in ("equity", "options", "dividends")
    ):
        chart_data["total"] = [round(float(x) * f, 2) for x in chart_data["total"]]
        return
    for key in ("equity", "options", "dividends"):
        arr = chart_data.get(key) or []
        chart_data[key] = [round(float(x) * f, 2) for x in arr]
    chart_data["total"] = [
        round(
            float(chart_data["equity"][i] or 0)
            + float(chart_data["options"][i] or 0)
            + float(chart_data["dividends"][i] or 0),
            2,
        )
        for i in range(n)
    ]


def _cumulative_pnl_from_stg_trades(trades_df, current_df):
    """
    Cumulative P&L by calendar day from stg_history (cash flow per row). Used when
    mart_daily_pnl is sparse but stg has years of RDDT fills (symbol match quirks).
    """
    empty = {
        "dates": [],
        "equity": [],
        "options": [],
        "dividends": [],
        "total": [],
        "underlying_price": [],
        "has_underlying_price": False,
    }
    if trades_df is None or trades_df.empty or "amount" not in trades_df.columns:
        return None
    t = trades_df.copy()
    if "trade_date" not in t.columns or "instrument_type" not in t.columns:
        return None
    t["td"] = pd.to_datetime(t["trade_date"], errors="coerce").dt.normalize()
    t = t[pd.notna(t["td"])]
    if t.empty:
        return None
    t["amount"] = pd.to_numeric(t["amount"], errors="coerce").fillna(0.0)
    it = t["instrument_type"].fillna("").str.strip()
    a = t["amount"]
    t["_div"] = a.where(
        (it == "Dividend") | it.str.contains("ividend", case=False, na=False), 0.0
    )
    t["_eq"] = a.where(it == "Equity", 0.0)
    t["_op"] = a.where(it.isin(["Call", "Put"]), 0.0)
    t["_oth"] = a - t["_div"] - t["_eq"] - t["_op"]
    g = t.groupby("td", as_index=False).agg(
        {"_eq": "sum", "_op": "sum", "_div": "sum", "_oth": "sum"}
    )
    g = g.sort_values("td")
    g["c_eq"] = g["_eq"].cumsum()
    g["c_op"] = (g["_op"] + g["_oth"]).cumsum()  # fees/margin in with options line for chart
    g["c_div"] = g["_div"].cumsum()
    g["tot"] = g["c_eq"] + g["c_op"] + g["c_div"]
    dates = [str(pd.Timestamp(x).date()) for x in g["td"].tolist()]
    return {
        "dates": dates,
        "equity": [round(x, 2) for x in g["c_eq"]],
        "options": [round(x, 2) for x in g["c_op"]],
        "dividends": [round(x, 2) for x in g["c_div"]],
        "total": [round(x, 2) for x in g["tot"]],
        "underlying_price": [None] * len(dates),
        "has_underlying_price": False,
    }


def _cumulative_pnl_from_leg_closes(closed_legs_pre_leg, closed_equity_pre_leg):
    """
    Step cumulative P&L from closed option legs and closed equity by close_date.
    Fallback when stg is empty but int_* legs exist.
    """
    events = []  # (date, d_eq, d_op, d_div)
    if closed_legs_pre_leg is not None and not closed_legs_pre_leg.empty and "close_date" in closed_legs_pre_leg.columns:
        for _, r in closed_legs_pre_leg.iterrows():
            d = r.get("close_date")
            if pd.isna(d):
                continue
            pnl = float(r.get("total_pnl") or 0)
            d0 = pd.to_datetime(d).date()
            events.append((d0, 0.0, pnl, 0.0))
    if closed_equity_pre_leg is not None and not closed_equity_pre_leg.empty and "close_date" in closed_equity_pre_leg.columns:
        for _, r in closed_equity_pre_leg.iterrows():
            d = r.get("close_date")
            if pd.isna(d):
                continue
            pnl = float(r.get("realized_pnl") or 0)
            d0 = pd.to_datetime(d).date()
            events.append((d0, pnl, 0.0, 0.0))
    if not events:
        return None
    events.sort(key=lambda x: x[0])
    byd = {}
    for d0, e, o, di in events:
        byd.setdefault(d0, [0.0, 0.0, 0.0])
        byd[d0][0] += e
        byd[d0][1] += o
        byd[d0][2] += di
    d_sorted = sorted(byd)
    c_eq, c_op, c_div = 0.0, 0.0, 0.0
    dates, eq, op, div, tot = [], [], [], [], []
    for d0 in d_sorted:
        c_eq += byd[d0][0]
        c_op += byd[d0][1]
        c_div += byd[d0][2]
        dates.append(str(d0))
        eq.append(round(c_eq, 2))
        op.append(round(c_op, 2))
        div.append(round(c_div, 2))
        tot.append(round(c_eq + c_op + c_div, 2))
    return {
        "dates": dates,
        "equity": eq,
        "options": op,
        "dividends": div,
        "total": tot,
        "underlying_price": [None] * len(dates),
        "has_underlying_price": False,
    }


def _collapse_mart_daily_pnl_duplicate_grain(daily_df: pd.DataFrame) -> pd.DataFrame:
    """Collapse duplicate ``mart_daily_pnl`` rows before stateful equity P&L.

    Natural grain is ``(account, user_id, symbol, date)``. Sync/backfill bugs
    can emit identical twins — ``_build_chart_from_daily_pnl`` processes each
    row and sums ``equity_*`` deltas, doubling buys/sells and inflating terminal
    P&L (May 2026 BE chart ~2× hero).

    Prefers populated ``user_id`` over ``NULL`` when deduping
    ``(account, symbol, date)``, then merges strict four-key collisions with
    ``keep=\"last\"`` (later ingestion wins).
    """
    if daily_df is None or daily_df.empty:
        return daily_df
    if not {"account", "symbol", "date"}.issubset(daily_df.columns):
        return daily_df
    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    stab = "__r_i__"
    df[stab] = range(len(df))
    ks3 = ["account", "symbol", "date"]

    if "user_id" in df.columns:
        uid_col = pd.to_numeric(df["user_id"], errors="coerce")
        df["__prefer_uid__"] = uid_col.notna().astype(int)
        df = df.sort_values(
            by=ks3 + ["__prefer_uid__", "user_id", stab],
            ascending=[True, True, True, False, True, True],
            na_position="last",
        )
        df = df.drop_duplicates(subset=ks3, keep="first").drop(
            columns=["__prefer_uid__"]
        )
        ks4 = ks3 + ["user_id"]
        df = df.sort_values(by=ks4 + [stab]).drop_duplicates(
            subset=ks4, keep="last"
        )
    else:
        df = df.sort_values(by=ks3 + [stab]).drop_duplicates(subset=ks3, keep="last")

    return df.drop(columns=[stab]).reset_index(drop=True)


def _build_chart_from_daily_pnl(daily_df, current_df):
    """Chart builder entrypoint — partitions ``mart_daily_pnl`` rows so each
    ``(account × user_id)`` slice runs its **own** equity cost-basis state
    machine.

    Without partitioning, ``position_detail`` with multiple brokerage labels
    merged into one symbol view feeds interleaved rows into a single walker —
    sells on account A partially consume basis accumulated from account B,
    corrupting cumulative equity (often reading as ~2× hero KPI vs mart).
    """
    empty = {
        "dates": [], "equity": [], "options": [], "dividends": [],
        "total": [], "underlying_price": [], "has_underlying_price": False,
    }
    if daily_df is None or daily_df.empty:
        return empty
    work = _collapse_mart_daily_pnl_duplicate_grain(daily_df.copy())
    if work.empty:
        return empty
    part_cols = ["account"]
    if "user_id" in work.columns:
        part_cols.append("user_id")
    gb = work.groupby(part_cols, dropna=False)
    if gb.ngroups <= 1:
        return _build_chart_from_daily_pnl_partition(work.sort_values("date"), current_df)
    parts = []
    for key, sub in gb:
        if isinstance(key, tuple):
            acct = key[0]
            uid_k = key[1] if len(key) > 1 else None
        else:
            acct = key
            uid_k = None
        cdf = _filter_current_for_chart_partition(current_df, acct, uid_k)
        parts.append(
            _build_chart_from_daily_pnl_partition(sub.sort_values("date"), cdf)
        )
    return _merge_position_pnl_chart_payloads(parts)


def _build_chart_from_daily_pnl_partition(daily_df, current_df):
    """
    Build cumulative P&L chart from pre-aggregated mart_daily_pnl data.

    Options, dividends, and other: read pre-computed cumulative sums.
    Equity: compute running average-cost P&L (stateful — buy/sell events
    from the mart, with daily mark-to-market via close_price).
    """
    empty = {
        "dates": [], "equity": [], "options": [], "dividends": [],
        "total": [], "underlying_price": [], "has_underlying_price": False,
    }
    if daily_df.empty:
        return empty

    daily_df = daily_df.sort_values("date")

    shares_held = 0.0
    total_cost = 0.0
    cum_realized = 0.0
    short_shares = 0.0
    short_cost_basis = 0.0
    position_is_closed = current_df.empty
    last_trade_date = None

    dates, equity_s, options_s, dividends_s, total_s, price_s = (
        [], [], [], [], [], [],
    )
    last_cumulative_options_realized = 0.0
    last_open_options_unrealized = 0.0
    # Track when option series steps (realization or MTM change) so the
    # "skip quiet days for closed positions" branch doesn't drop a real
    # event day. Without this an OTM-expiry crystallization (no fill in
    # stg_history → has_trade=False on close_date) would be silently
    # skipped from the rendered series.
    prev_options_realized_for_skip = 0.0
    prev_options_open_mtm_for_skip = 0.0

    for _, row in daily_df.iterrows():
        buy_qty = float(row.get("equity_buy_qty") or 0)
        buy_cost = float(row.get("equity_buy_cost") or 0)
        sell_qty = float(row.get("equity_sell_qty") or 0)
        sell_proceeds = float(row.get("equity_sell_proceeds") or 0)
        has_trade = bool(row.get("has_trade"))

        if has_trade:
            last_trade_date = row["date"]

        # Skip quiet days for closed positions — but DO NOT skip days
        # where the options series steps. Realization-on-close days
        # (especially OTM expiries that have no fill in stg_history)
        # would otherwise vanish from the chart. Compare today's
        # mart-side option fields against the most recent rendered
        # values: any change is a real event the user should see.
        cur_realized_for_skip = float(row.get("cumulative_options_pnl") or 0)
        cur_open_mtm_for_skip = float(row.get("open_options_unrealized_pnl") or 0)
        options_step_today = (
            cur_realized_for_skip != prev_options_realized_for_skip
            or cur_open_mtm_for_skip != prev_options_open_mtm_for_skip
        )
        if (position_is_closed
                and shares_held == 0
                and short_shares == 0
                and not has_trade
                and not options_step_today):
            continue
        prev_options_realized_for_skip = cur_realized_for_skip
        prev_options_open_mtm_for_skip = cur_open_mtm_for_skip

        # Process sells first (may create short position)
        if sell_qty > 0:
            remaining_sell = sell_qty
            remaining_proceeds = sell_proceeds
            if shares_held > 0:
                sold_long = min(remaining_sell, shares_held)
                avg = total_cost / shares_held if shares_held > 0 else 0
                frac = sold_long / sell_qty if sell_qty > 0 else 1
                sold_long_proceeds = sell_proceeds * frac
                cum_realized += sold_long_proceeds - avg * sold_long
                total_cost = max(0, total_cost - avg * sold_long)
                shares_held = max(0, shares_held - sold_long)
                remaining_sell -= sold_long
                remaining_proceeds -= sold_long_proceeds
            if remaining_sell > 0:
                short_shares += remaining_sell
                short_cost_basis += remaining_proceeds

        # Process buys (may cover short position)
        if buy_qty > 0:
            remaining_buy = buy_qty
            remaining_cost = buy_cost
            if short_shares > 0:
                covered = min(remaining_buy, short_shares)
                frac = covered / buy_qty if buy_qty > 0 else 1
                cover_cost = buy_cost * frac
                avg_short = short_cost_basis / short_shares if short_shares > 0 else 0
                cum_realized += avg_short * covered - cover_cost
                short_cost_basis = max(0, short_cost_basis - avg_short * covered)
                short_shares = max(0, short_shares - covered)
                remaining_buy -= covered
                remaining_cost -= cover_cost
            if remaining_buy > 0:
                shares_held += remaining_buy
                total_cost += remaining_cost

        close = float(row.get("close_price") or 0)
        # If no close price on a buy day, use avg cost so open position doesn't show full cost as "loss"
        if close <= 0 and buy_qty > 0 and buy_cost > 0 and shares_held > 0:
            close = buy_cost / buy_qty
        unrealized = 0
        if close > 0:
            if shares_held > 0:
                unrealized = shares_held * close - total_cost
            if short_shares > 0:
                unrealized -= (short_shares * close - short_cost_basis)
        eq_pnl = cum_realized + unrealized

        # Options P&L = realize-on-close cumulative + open-contract MTM
        # at this date. mart_daily_pnl exposes both halves separately
        # (see model header for the attribution rule); the chart simply
        # sums them. Post-fix this means a STO premium does NOT appear
        # as a step on STO date — instead the option contributes daily
        # MTM until close_date, then crystallizes at the realized total.
        # See AGENTS.md "Option P&L Attribution".
        cum_realized_opt = float(row.get("cumulative_options_pnl") or 0)
        open_unreal_opt = float(row.get("open_options_unrealized_pnl") or 0)
        opt_pnl = cum_realized_opt + open_unreal_opt
        div_pnl = float(row.get("cumulative_dividends_pnl") or 0)
        oth_pnl = float(row.get("cumulative_other_pnl") or 0)
        last_cumulative_options_realized = cum_realized_opt
        last_open_options_unrealized = open_unreal_opt

        dates.append(str(row["date"])[:10])
        equity_s.append(round(eq_pnl, 2))
        options_s.append(round(opt_pnl, 2))
        dividends_s.append(round(div_pnl, 2))
        total_s.append(round(eq_pnl + opt_pnl + div_pnl + oth_pnl, 2))
        # Underlying close for the chart: use whenever the mart has a price.
        # Do not require shares_held > 0 here — that failed when the chart date range
        # starts after the equity open (leg filter) or carry-forward is missing rows.
        price_s.append(round(close, 2) if close > 0 else None)

    if not dates:
        return empty

    today_str = str(date.today())

    # Guard: BigQuery's ``current_date()`` runs in UTC and can be one
    # calendar day ahead of US local time after ~5pm PT. The mart's
    # dense spine therefore sometimes includes a "tomorrow" row from
    # the trader's perspective. Trim any rows past today so the chart
    # x-axis stops at today and the LIVE override below patches the
    # right cell. Pre-fix, the spine ended on UTC-tomorrow with stale
    # carry-forward values, the append-today branch added a duplicate
    # row out-of-order ([..., 5/11, 5/12, 5/11]), and the chart's
    # "terminal" sat on the wrong index — DELL ••••0044 stayed on
    # pre-fix int_equity_sessions arithmetic instead of the live
    # snapshot mv − cb.
    while dates and dates[-1] > today_str:
        dates.pop()
        equity_s.pop()
        options_s.pop()
        dividends_s.pop()
        total_s.pop()
        price_s.pop()

    if not current_df.empty:
        # LIVE TODAY OVERRIDE.
        #
        # The mart's dense date spine emits a row for current_date()
        # (and the contract daily-pnl spine extends to today for
        # currently-owned contracts via the ``currently_owned`` CTE
        # in ``int_option_contract_daily_pnl``). That row reflects
        # the LATEST DAILY SNAPSHOT, which can be 1-3 trading days
        # stale (Schwab's nightly sync hasn't booked today yet, or
        # the user's connection paused). For "today" the broker's
        # LIVE snapshot in stg_current is the source of truth — it's
        # always intra-day fresh. We must therefore override the
        # mart's today row with values computed from current_df so
        # the chart's terminal value matches the headline KPIs and
        # the positions_summary mart (which also reads stg_current
        # live for unrealized).
        #
        # When the chart already ends at today (mart spine), REPLACE
        # the last row's equity/options/total with the live-derived
        # numbers. When the chart ends before today (rare — happens
        # when the position has zero mart history), APPEND today.
        #
        # Pre-fix the patch only fired on APPEND (``dates[-1] != today``)
        # because the mart used to leave today empty. After the dense-
        # spine rework, today is always present and the patch was being
        # silently skipped, so the chart "snapped to 0" or "stuck on
        # the last snapshot" while positions_summary read live MTM.
        # That tripped the reconciliation invariant on every position
        # whose snapshot table lagged stg_current (real example May
        # 2026: JPM 0044 chart=$320 vs strategy_breakdown=$30,940).
        #
        # Using ``unrealized_pnl`` (not ``market_value``) matches the
        # snapshot-derived MTM used in mart_daily_pnl; current_df came
        # from int_enriched_current which has the corrected sign.
        # See AGENTS.md "Option P&L Attribution".
        opt_mask = current_df["instrument_type"].isin(["Call", "Put"])
        if "option_expiry" in current_df.columns:
            today_ts = pd.Timestamp(date.today())
            opt_expiry_series = pd.to_datetime(
                current_df["option_expiry"], errors="coerce"
            )
            opt_mask = opt_mask & (
                opt_expiry_series.isna() | (opt_expiry_series >= today_ts)
            )
        if "unrealized_pnl" in current_df.columns:
            opt_unreal_today = float(
                current_df.loc[opt_mask, "unrealized_pnl"].sum()
            )
        elif "market_value" in current_df.columns:
            opt_unreal_today = float(
                current_df.loc[opt_mask, "market_value"].sum()
            )
        else:
            opt_unreal_today = last_open_options_unrealized
        today_option_pnl = last_cumulative_options_realized + opt_unreal_today
        eq_row = current_df[current_df["instrument_type"] == "Equity"]
        today_eq = equity_s[-1]
        # When the broker's live snapshot has equity AND a current
        # price, prefer the snapshot's market_value - cost_basis as
        # today's unrealized. This is the same number positions_summary
        # surfaces in the headline KPI / Strategy Breakdown row, and
        # it correctly accounts for shares the trader holds that
        # aren't in trade history (broker-side journal entries,
        # transfer-ins, dividend reinvestments — Schwab's sync
        # occasionally drops these from the transactions feed but
        # always reflects them in the positions snapshot). When the
        # snapshot is missing or the user fully closed today, fall
        # back to the running-cost-basis trade-history calc.
        if not eq_row.empty:
            mv_col = (
                float(eq_row["market_value"].sum())
                if "market_value" in eq_row.columns else 0.0
            )
            # ``cost_basis`` is the canonical name (int_enriched_current,
            # CURRENT_POSITIONS_QUERY). ``cost_bases`` is the original
            # CSV-seed typo that survives in some test fixtures and the
            # raw ``current_positions`` seed schema; accept either so
            # this helper works against both production and test data.
            cb_col = 0.0
            for cb_name in ("cost_basis", "cost_bases"):
                if cb_name in eq_row.columns:
                    cb_col = float(eq_row[cb_name].sum())
                    break
            unreal_snap = (mv_col - cb_col) if (mv_col or cb_col) else None
            if unreal_snap is not None:
                today_eq = cum_realized + unreal_snap
            elif shares_held > 0 or short_shares > 0:
                p = float(eq_row["current_price"].iloc[0] or 0)
                if p:
                    unreal = 0
                    if shares_held > 0:
                        unreal = shares_held * p - total_cost
                    if short_shares > 0:
                        unreal -= (short_shares * p - short_cost_basis)
                    today_eq = cum_realized + unreal
        today_price = None
        if not eq_row.empty and "current_price" in eq_row.columns:
            cp_nonnull = pd.to_numeric(eq_row["current_price"], errors="coerce").dropna()
            today_price = float(cp_nonnull.iloc[0]) if len(cp_nonnull) else None

        if dates[-1] == today_str:
            equity_s[-1] = round(today_eq, 2)
            options_s[-1] = round(today_option_pnl, 2)
            total_s[-1] = round(
                today_eq + today_option_pnl + dividends_s[-1], 2
            )
            if today_price is not None:
                price_s[-1] = round(today_price, 2)
        else:
            dates.append(today_str)
            equity_s.append(round(today_eq, 2))
            options_s.append(round(today_option_pnl, 2))
            dividends_s.append(dividends_s[-1])
            price_s.append(round(today_price, 2) if today_price else None)
            total_s.append(
                round(today_eq + today_option_pnl + dividends_s[-1], 2)
            )

    return {
        "dates": dates,
        "equity": equity_s,
        "options": options_s,
        "dividends": dividends_s,
        "total": total_s,
        "underlying_price": price_s,
        "has_underlying_price": any(p is not None for p in price_s),
    }


@app.route("/symbols")
@login_required
def symbols_detail():
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    acct_filter = _account_sql_and(user_accounts)

    try:
        dfs = _bq_parallel(client, {
            "trades": TRADES_QUERY.format(account_filter=acct_filter),
            "current": CURRENT_POSITIONS_QUERY.format(account_filter=acct_filter),
            "strat": STRATEGIES_MAP_QUERY.format(account_filter=acct_filter),
            "pnl": SYMBOLS_PNL_QUERY.format(account_filter=acct_filter),
            "open_start": OPEN_SESSION_START_QUERY.format(account_filter=acct_filter),
            "closed_legs": CLOSED_LEGS_QUERY.format(
                closed_legs_account_filter=_account_sql_and(user_accounts, col="sc.account")),
            "closed_equity": CLOSED_EQUITY_LEGS_QUERY.format(account_filter=acct_filter),
        })
        trades_df = dfs["trades"]
        current_df = dfs["current"]
        strat_df = dfs["strat"]
        pnl_df = dfs["pnl"]
        open_start_df = dfs["open_start"]
        closed_legs_df = dfs["closed_legs"]
        closed_equity_df = dfs["closed_equity"]
    except Exception as exc:
        app.logger.exception("Daily P&L by symbol load failed: %s", exc)
        return render_template(
            "symbols.html",
            title="Daily P&L by symbol",
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
    trades_df = _filter_df_by_accounts(trades_df, user_accounts)
    current_df = _filter_df_by_accounts(current_df, user_accounts)
    strat_df = _filter_df_by_accounts(strat_df, user_accounts)
    pnl_df = _filter_df_by_accounts(pnl_df, user_accounts)
    if not open_start_df.empty:
        open_start_df = _filter_df_by_accounts(open_start_df, user_accounts)
    if not closed_legs_df.empty:
        closed_legs_df = _filter_df_by_accounts(closed_legs_df, user_accounts)
    if not closed_equity_df.empty:
        closed_equity_df = _filter_df_by_accounts(closed_equity_df, user_accounts)

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
    if not accounts and user_accounts:
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

    if selected_account:
        trades_df = trades_df[trades_df["account"] == selected_account]
        current_df = current_df[current_df["account"] == selected_account]
        strat_df = strat_df[strat_df["account"] == selected_account]
        pnl_df = pnl_df[pnl_df["account"] == selected_account]
        if not open_start_df.empty:
            open_start_df = open_start_df[open_start_df["account"] == selected_account]
        if not closed_legs_df.empty:
            closed_legs_df = closed_legs_df[closed_legs_df["account"] == selected_account]
        if not closed_equity_df.empty:
            closed_equity_df = closed_equity_df[closed_equity_df["account"] == selected_account]

    # Restrict to symbols that have a current open position (match current_positions / int_enriched_current)
    if open_only:
        open_pairs = set(zip(current_df["account"].astype(str), current_df["symbol"].astype(str))) if not current_df.empty else set()
    else:
        open_pairs = None

    # Fetch pre-aggregated chart data from mart
    try:
        acct_filter = _account_sql_and([selected_account] if selected_account else user_accounts)
        all_chart_df = client.query(
            CHART_DATA_ALL_QUERY.format(account_filter=acct_filter)
        ).to_dataframe()
        all_chart_df = _filter_df_by_accounts(all_chart_df, user_accounts)
        if selected_account and not all_chart_df.empty:
            all_chart_df = all_chart_df[all_chart_df["account"] == selected_account]
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

        chart = _build_chart_from_daily_pnl(sym_chart_df, sym_current)

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

        chart_data_list.append(chart)

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

    # Sort by total return descending; rebuild chart list in matching order
    symbol_data.sort(key=lambda x: x["total_return"], reverse=True)
    sorted_charts = [chart_data_list[item["_chart_idx"]] for item in symbol_data]
    for item in symbol_data:
        del item["_chart_idx"]

    return render_template(
        "symbols.html",
        title="Daily P&L by symbol",
        symbol_data=symbol_data,
        chart_data_json=json.dumps(sorted_charts),
        accounts=accounts,
        selected_account=selected_account,
        open_only=open_only,
        positions_only=positions_only,
        linked_brokerage_accounts=(user_accounts or []),
        viewer_is_admin=is_admin(current_user.username),
    )


# ======================================================================
# Account Performance  (/accounts)
# ======================================================================

ACCOUNT_BALANCES_QUERY = """
    SELECT account, row_type, market_value, cost_basis,
           unrealized_pnl, unrealized_pnl_pct, percent_of_account
    FROM `ccwj-dbt.analytics.stg_account_balances`
    WHERE 1=1 {account_filter}
"""

STRATEGY_CLASSIFICATION_QUERY = """
    SELECT account, symbol, strategy, status, open_date, close_date,
           total_pnl, num_trades
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE 1=1 {account_filter}
"""

ACCOUNT_POSITIONS_SUMMARY_QUERY = """
    SELECT account, strategy,
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
    WHERE 1=1 {account_filter}
    GROUP BY account, strategy
    ORDER BY account, strategy
"""


def _build_account_chart_from_daily_pnl(daily_df, current_df):
    """
    Build account-level cumulative P&L chart from mart_daily_pnl.

    Aggregates across all symbols.  Options/dividends/other use running
    sums of daily amounts.  Equity requires per-symbol average-cost tracking.
    """
    empty = {"dates": [], "equity": [], "options": [], "dividends": [], "total": []}
    if daily_df.empty:
        return empty

    daily_df = _collapse_mart_daily_pnl_duplicate_grain(daily_df)
    daily_df = daily_df.sort_values("date")
    all_dates = sorted(daily_df["date"].dropna().unique())

    eq_state = {}
    cum_div = cum_oth = 0.0
    dates_out, equity_s, options_s, dividends_s, total_s = [], [], [], [], []

    # Account-level options P&L follows the same realize-on-close +
    # MTM-while-open rule as the position page (see AGENTS.md
    # "Option P&L Attribution"). For each day:
    #   - cumulative_options_pnl is already realized cumulative across
    #     all closed contracts as of that date. Per-symbol values are
    #     additive across symbols (each contract appears in exactly one
    #     symbol's series).
    #   - open_options_unrealized_pnl is point-in-time MTM of all open
    #     contracts on this date. Sum across symbols.
    # Pre-fix this routine ran ``cum_opt += sum(options_amount)``,
    # which credited STO premium on STO date — the position-page bug
    # except worse because it couldn't even mark-to-market.
    options_per_symbol_realized = {}  # (account, symbol) -> last realized cum

    for d in all_dates:
        day = daily_df[daily_df["date"] == d]

        # Update per-symbol realized cumulative from the mart (carried
        # forward across days when no new realization happened).
        for _, r in day.iterrows():
            key = (r["account"], r["symbol"])
            options_per_symbol_realized[key] = float(
                r.get("cumulative_options_pnl") or 0
            )
        realized_total = sum(options_per_symbol_realized.values())

        # Open MTM at this date is sum across the (account, symbol)
        # rows present today. Symbols with no row today contribute 0
        # (per-contract spine ends at close_date — see
        # int_option_contract_daily_pnl).
        open_mtm_total = float(day.get(
            "open_options_unrealized_pnl",
            pd.Series(dtype=float),
        ).fillna(0).sum()) if "open_options_unrealized_pnl" in day.columns else 0.0

        cum_opt = realized_total + open_mtm_total
        cum_div += float(day["dividends_amount"].sum())
        cum_oth += float(day["other_amount"].sum())

        for _, row in day.iterrows():
            key = (row["account"], row["symbol"])
            if key not in eq_state:
                eq_state[key] = {"shares": 0.0, "cost": 0.0, "realized": 0.0}
            s = eq_state[key]
            bq = float(row.get("equity_buy_qty") or 0)
            bc = float(row.get("equity_buy_cost") or 0)
            sq = float(row.get("equity_sell_qty") or 0)
            sp = float(row.get("equity_sell_proceeds") or 0)

            if bq > 0:
                s["shares"] += bq
                s["cost"] += bc
            if sq > 0 and s["shares"] > 0:
                avg = s["cost"] / s["shares"]
                sold = min(sq, s["shares"])
                s["realized"] += sp - avg * sold
                s["cost"] = max(0, s["cost"] - avg * sold)
                s["shares"] = max(0, s["shares"] - sold)
            elif sq > 0:
                s["realized"] += sp

        eq_total = sum(s["realized"] for s in eq_state.values())
        for _, row in day.iterrows():
            key = (row["account"], row["symbol"])
            s = eq_state[key]
            close = float(row.get("close_price") or 0)
            if close > 0 and s["shares"] > 0:
                eq_total += s["shares"] * close - s["cost"]

        dates_out.append(str(d)[:10])
        equity_s.append(round(eq_total, 2))
        options_s.append(round(cum_opt, 2))
        dividends_s.append(round(cum_div, 2))
        total_s.append(round(eq_total + cum_opt + cum_div + cum_oth, 2))

    today = date.today()
    today_str = str(today)
    if not current_df.empty and dates_out and dates_out[-1] != today_str:
        # Synthetic today row when the mart hasn't been built yet for
        # today (sync ran but dbt hasn't refreshed yet).
        #
        # Equity: keep the legacy behavior of adding today's snapshot
        # unrealized to the last mart-day equity value. There's a
        # well-known dimensional issue here (equity_s[-1] already
        # includes mark-to-market at the mart's close price for that
        # day, so adding today's unrealized double-counts when the
        # mart is fresh as of yesterday). Pre-existing; out of scope
        # for the option-attribution rewrite.
        #
        # Options: under realize-on-close, the right value is
        #   today_options = (last realized cumulative across symbols)
        #                 + (LIVE open MTM from current_df today)
        # This is a REPLACEMENT not an addition: the last loop
        # iteration's options_s value already had open MTM for the
        # mart's last day, and we want today's broker MTM instead.
        eq_unreal = float(current_df.loc[current_df["instrument_type"] == "Equity", "unrealized_pnl"].sum())
        # Filter to genuinely-open option contracts (calendar beats
        # stale snapshot — see _build_chart_from_daily_pnl for the
        # same rationale).
        opt_mask = current_df["instrument_type"].isin(["Call", "Put"])
        if "option_expiry" in current_df.columns:
            today_ts = pd.Timestamp(date.today())
            opt_expiry_series = pd.to_datetime(
                current_df["option_expiry"], errors="coerce"
            )
            opt_mask = opt_mask & (
                opt_expiry_series.isna() | (opt_expiry_series >= today_ts)
            )
        opt_unreal_today = float(
            current_df.loc[opt_mask, "unrealized_pnl"].sum()
        )
        last_realized_total = sum(options_per_symbol_realized.values())
        today_options = round(last_realized_total + opt_unreal_today, 2)
        if eq_unreal != 0 or today_options != options_s[-1]:
            dates_out.append(today_str)
            equity_s.append(round(equity_s[-1] + eq_unreal, 2))
            options_s.append(today_options)
            dividends_s.append(dividends_s[-1])
            total_s.append(round(equity_s[-1] + today_options + dividends_s[-1] + cum_oth, 2))

    return {
        "dates": dates_out,
        "equity": equity_s,
        "options": options_s,
        "dividends": dividends_s,
        "total": total_s,
    }


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

    return {
        "dates": [str(d) for d in all_dates],
        "series": series,
    }


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
    {account_filter}
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
    acct_filter = _account_sql_and(user_accounts)

    selected_account = request.args.get("account", "")

    try:
        df = client.query(
            SECTORS_QUERY.format(account_filter=acct_filter)
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
    df = _filter_df_by_accounts(df, user_accounts)

    if selected_account:
        df = df[df["account"] == selected_account]

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

    accounts_for_filter = sorted(df["account"].dropna().unique().tolist()) if not df.empty else []

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
    {account_filter}
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
    {account_filter}
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
    `_filter_df_by_accounts(df, user_accounts)`) BEFORE handing it in.

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
        "ai_available": bool(os.environ.get("GEMINI_API_KEY", "").strip()),
    }
    if not ctx["ai_enabled"]:
        return ctx
    try:
        cached = get_strategy_fit_insight_for_user(
            current_user.id, account_filter=selected_account or ""
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
    acct_filter = _account_sql_and(user_accounts)

    selected_account = request.args.get("account", "")
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
    queries = {"summary": STRATEGY_FIT_QUERY.format(account_filter=acct_filter)}
    if dim in ("dte", "moneyness"):
        queries["options"] = STRATEGY_FIT_OPTIONS_QUERY.format(account_filter=acct_filter)

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
    summary_df = _filter_df_by_accounts(summary_df, user_accounts)
    if selected_account and not summary_df.empty:
        summary_df = summary_df[summary_df["account"] == selected_account].copy()

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
        sorted(summary_df["account"].dropna().unique().tolist())
        if not summary_df.empty else []
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
        options_df = _filter_df_by_accounts(options_df, user_accounts)
        if selected_account and not options_df.empty:
            options_df = options_df[options_df["account"] == selected_account].copy()

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


@app.route("/accounts")
@login_required
def accounts():
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    acct_filter = _account_sql_and(user_accounts)

    try:
        dfs = _bq_parallel(client, {
            "balances": ACCOUNT_BALANCES_QUERY.format(account_filter=acct_filter),
            "trades": TRADES_QUERY.format(account_filter=acct_filter),
            "current": CURRENT_POSITIONS_QUERY.format(account_filter=acct_filter),
            "strat_class": STRATEGY_CLASSIFICATION_QUERY.format(account_filter=acct_filter),
            "strat_summary": ACCOUNT_POSITIONS_SUMMARY_QUERY.format(account_filter=acct_filter),
        })
        balances_df = dfs["balances"]
        trades_df = dfs["trades"]
        current_df = dfs["current"]
        strat_class_df = dfs["strat_class"]
        strat_summary_df = dfs["strat_summary"]
    except Exception as exc:
        return render_template(
            "accounts.html",
            error=str(exc),
            kpis={},
            summary_chart_json="{}",
            strategy_chart_json="{}",
            strategy_rows=[],
            accounts=[],
            selected_account="",
        )

    # ------------------------------------------------------------------
    # Clean types
    # ------------------------------------------------------------------
    for col in ["market_value", "cost_basis", "unrealized_pnl", "unrealized_pnl_pct", "percent_of_account"]:
        if col in balances_df.columns:
            balances_df[col] = pd.to_numeric(balances_df[col], errors="coerce").fillna(0)

    for col in ["amount", "quantity", "price", "fees"]:
        trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
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
    balances_df = _filter_df_by_accounts(balances_df, user_accounts)
    trades_df = _filter_df_by_accounts(trades_df, user_accounts)
    current_df = _filter_df_by_accounts(current_df, user_accounts)
    strat_class_df = _filter_df_by_accounts(strat_class_df, user_accounts)
    strat_summary_df = _filter_df_by_accounts(strat_summary_df, user_accounts)

    all_accounts = sorted(trades_df["account"].dropna().unique())
    selected_account = request.args.get("account", "")

    if selected_account:
        balances_df = balances_df[balances_df["account"] == selected_account]
        trades_df = trades_df[trades_df["account"] == selected_account]
        current_df = current_df[current_df["account"] == selected_account]
        strat_class_df = strat_class_df[strat_class_df["account"] == selected_account]
        strat_summary_df = strat_summary_df[strat_summary_df["account"] == selected_account]

    # ------------------------------------------------------------------
    # KPIs from balances
    # ------------------------------------------------------------------
    cash_rows = balances_df[balances_df["row_type"] == "cash"]
    total_rows = balances_df[balances_df["row_type"] == "account_total"]

    cash_balance = float(cash_rows["market_value"].sum())
    account_value = float(total_rows["market_value"].sum())
    invested_value = account_value - cash_balance
    acct_cost_basis = float(total_rows["cost_basis"].sum())

    # Realized + unrealized + total_return all come from the same source
    # (positions_summary) so the three KPIs reconcile: total_return =
    # realized + unrealized + dividends. Mixing the snapshot's unrealized
    # with positions_summary's realized has shipped a $300+ discrepancy.
    realized_pnl = float(strat_summary_df["realized_pnl"].sum())
    acct_unrealized = float(strat_summary_df["unrealized_pnl"].sum())
    total_return = float(strat_summary_df["total_return"].sum())
    # Surfacing dividends as its own KPI so the math reconciles for the
    # reader: realized + unrealized + dividends = total return. Without
    # this card the row silently failed by ~$200-300 (the missing piece
    # was always dividends), and investors / power users noticed.
    dividend_income = (
        float(strat_summary_df["dividend_income"].sum())
        if "dividend_income" in strat_summary_df.columns else 0.0
    )

    kpis = {
        "account_value": account_value,
        "cash_balance": cash_balance,
        "invested_value": invested_value,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": acct_unrealized,
        "dividend_income": dividend_income,
        "total_return": total_return,
    }

    # ------------------------------------------------------------------
    # Chart 1: Cumulative P&L over time (summary) — from mart_daily_pnl
    # ------------------------------------------------------------------
    try:
        acct_filter_sql = _account_sql_and([selected_account] if selected_account else user_accounts)
        chart_df = client.query(
            CHART_DATA_ALL_QUERY.format(account_filter=acct_filter_sql)
        ).to_dataframe()
        chart_df = _filter_df_by_accounts(chart_df, user_accounts)
        if selected_account and not chart_df.empty:
            chart_df = chart_df[chart_df["account"] == selected_account]
        summary_chart = _build_account_chart_from_daily_pnl(chart_df, current_df)
    except Exception:
        summary_chart = {"dates": [], "equity": [], "options": [], "dividends": [], "total": []}

    # ------------------------------------------------------------------
    # Chart 2: Strategy P&L over time
    # ------------------------------------------------------------------
    strategy_chart = _build_strategy_time_chart(strat_class_df)

    # ------------------------------------------------------------------
    # Strategy summary table
    # ------------------------------------------------------------------
    if not strat_summary_df.empty:
        strat_summary_df["win_rate"] = strat_summary_df.apply(
            lambda r: r["num_winners"] / (r["num_winners"] + r["num_losers"])
            if (r["num_winners"] + r["num_losers"]) > 0 else 0,
            axis=1,
        )
        strategy_rows = strat_summary_df.to_dict(orient="records")
    else:
        strategy_rows = []

    return render_template(
        "accounts.html",
        kpis=kpis,
        summary_chart_json=json.dumps(summary_chart),
        strategy_chart_json=json.dumps(strategy_chart),
        strategy_rows=strategy_rows,
        accounts=all_accounts,
        selected_account=selected_account,
    )


@app.errorhandler(RequestEntityTooLarge)
def request_entity_too_large(_e):
    """CSV uploads exceed MAX_CONTENT_LENGTH (see config MAX_UPLOAD_MB)."""
    flash(
        "Upload too large. Try a shorter date range in your export, or raise MAX_UPLOAD_MB.",
        "danger",
    )
    if current_user.is_authenticated:
        return redirect(url_for("upload"))
    return redirect(url_for("index"))
