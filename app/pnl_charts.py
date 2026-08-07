"""Cumulative P&L chart builders + chart-partition hygiene helpers.

Shared by Position Detail, Symbols, and Accounts. This is the stateful
"heavy Python" the AGENTS.md architecture section flags as known debt
(running average-cost equity P&L simulated row by row) — kept verbatim,
just extracted out of app/routes.py (routes.py refactor, Aug 2026).

Option P&L attribution contract (realize-on-close + MTM-while-open) is
documented in AGENTS.md; the chart formula at any date is
``cumulative_options_pnl + open_options_unrealized_pnl`` and nothing else.
Tests: tests/test_chart_options_pnl.py, tests/test_position_detail_helpers.py,
tests/test_account_chart_trim.py, tests/test_data_isolation.py.
"""

from datetime import datetime, date, timedelta  # noqa: F401

import logging
import math

import pandas as pd

from app import app
from app.query_cache import timed
from app.tenant_scope import filter_df_by_tenant_ids as _filter_df_by_tenant_ids

_log = logging.getLogger(__name__)


def _dedupe_enriched_current_positions(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate open rows from ``int_enriched_current`` (same contract or
    equity line merged twice). Seed/snapshot regressions can emit byte-near
    duplicates; the UI should not show twin 200-share lines with identical cost.

    The dedup key leads with ``tenant_id``: the same symbol held in multiple
    physical accounts that share a display ``account`` label (e.g. 5 SnapTrade
    "Schwab Account" tenants all holding QTUM) is NOT a duplicate — each is a
    real, separately-held lot. Without ``tenant_id`` in the key all 5 collapse
    to one row, which silently undercounts the Hero total, Breakdown-by-Type
    equity, and the open-legs table to a single tenant's P&L.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if "trade_symbol" in out.columns:
        out["trade_symbol"] = (
            out["trade_symbol"].astype(str).str.strip().replace({"nan": ""})
        )
    key = [c for c in ("tenant_id", "account", "user_id", "instrument_type", "trade_symbol") if c in out.columns]
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
    current_df: pd.DataFrame, account, user_id_key, tenant_id_key=None
) -> pd.DataFrame:
    """Slice ``int_enriched_current`` rows for one chart partition
    (``account`` × optional ``user_id``). Required when ``mart_daily_pnl``
    spans multiple partitions for the same symbol — the live today-row patch
    must not mix snapshots across tenants.

    When the mart partition has a populated ``user_id`` but Schwab/sync
    snapshot rows still have ``user_id IS NULL`` (Stage 0 backfill lag),
    strict equality would yield an **empty** slice, skipping the entire
    live MTM patch — chart terminal sticks on realized-only while hero
    and Breakdown-by-type include broker unrealized (IYW-style gap).
    Prefer exact ``user_id`` match; fall back to NULL-id rows **only**
    for the same ``account`` (DataFrame already passed
    ``_filter_df_by_accounts``)."""
    if current_df is None or current_df.empty or "account" not in current_df.columns:
        return pd.DataFrame()
    # When the mart partition is keyed by the broker-stable tenant_id (the
    # v2 grain), prefer matching the snapshot on tenant_id so two physical
    # accounts sharing an ``account`` label (e.g. several "Schwab Account"s)
    # don't pool their live snapshot rows into one chart partition.
    if tenant_id_key is not None and "tenant_id" in current_df.columns:
        m = current_df["tenant_id"].astype(str) == str(tenant_id_key).strip()
        return current_df.loc[m].copy()
    m = current_df["account"].astype(str) == str(account).strip()
    if "user_id" in current_df.columns:
        uid_series = pd.to_numeric(current_df["user_id"], errors="coerce")
        if user_id_key is None or pd.isna(user_id_key):
            m &= uid_series.isna()
        else:
            uk = float(pd.to_numeric(pd.Series([user_id_key]), errors="coerce").iloc[0])
            m_uk = uid_series == uk
            if m_uk.any():
                m &= m_uk
            else:
                m &= uid_series.isna()
    return current_df.loc[m].copy()


def _drop_phantom_equity_writeoffs(
    closed_equity_df: pd.DataFrame, current_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Strip ``int_closed_equity_legs`` "Cost Written Off" rows when the
    broker snapshot still shows the symbol held on the same ``account``.

    Returns ``(kept_df, removed_df)`` so callers can reverse the bogus
    realized contribution out of any other mart (e.g. ``positions_summary``)
    that aggregated the same writeoff into a strategy rollup.

    Mirrors the ``account_symbol_holdings`` suppression added to
    ``int_closed_equity_legs.sql`` so the position page renders correctly
    even before BigQuery has been re-built. Failure mode pinned by IYW /
    Emmory Investment (May 2026): an orphan-tenant split (Schwab synced
    before ``user_id`` was linked to the masked ``account``) leaves a
    session with ``total_buy_qty > total_sell_qty`` under one
    ``(account, user_id)`` partition while the **same shares** are still
    open under another partition. dbt emits a phantom *Cost Written Off*
    for the residual; that row poisons everything downstream — Position
    Legs adds a bogus 10-share -$1,966 line, Breakdown by Type rolls
    realized down to -$1,957, Strategy Breakdown shows a fake
    "Dividend (Closed)" -$1,957 row, the chart-substitute path inherits
    that cliff, and the reconciliation banner fires.

    Suppression rule (matches dbt): drop the writeoff row when
    ``int_enriched_current`` shows shares of the same ``(account, symbol)``
    >= the writeoff row's residual quantity. Otherwise the row may be a
    real loss (genuine off-platform transfer / corporate action) and is
    left alone."""
    empty_removed = pd.DataFrame()
    if closed_equity_df is None or closed_equity_df.empty:
        return closed_equity_df, empty_removed
    if "description" not in closed_equity_df.columns:
        return closed_equity_df, empty_removed
    desc = closed_equity_df["description"].astype(str).str.strip().str.lower()
    is_writeoff = desc.eq("cost written off")
    if not is_writeoff.any():
        return closed_equity_df, empty_removed
    sym_col = next(
        (c for c in ("symbol", "trade_symbol") if c in closed_equity_df.columns),
        None,
    )
    if sym_col is None or "account" not in closed_equity_df.columns \
            or "quantity" not in closed_equity_df.columns:
        return closed_equity_df, empty_removed
    if current_df is None or current_df.empty \
            or "instrument_type" not in current_df.columns:
        return closed_equity_df, empty_removed
    it = current_df["instrument_type"].astype(str).str.strip().str.lower()
    eq_open = current_df.loc[it.eq("equity")]
    if eq_open.empty:
        return closed_equity_df, empty_removed
    cur_sym_col = next(
        (c for c in ("symbol", "underlying_symbol") if c in eq_open.columns),
        None,
    )
    if cur_sym_col is None or "account" not in eq_open.columns \
            or "quantity" not in eq_open.columns:
        return closed_equity_df, empty_removed
    held = (
        pd.DataFrame({
            "account": eq_open["account"].astype(str).str.strip(),
            "symbol": eq_open[cur_sym_col].astype(str).str.strip(),
            "qty": pd.to_numeric(eq_open["quantity"], errors="coerce")
                .fillna(0).abs(),
        })
        .groupby(["account", "symbol"], as_index=False)["qty"].sum()
    )
    held_map = {
        (r.account, r.symbol): float(r.qty)
        for r in held.itertuples(index=False)
    }
    cw_acct = closed_equity_df["account"].astype(str).str.strip()
    cw_sym = closed_equity_df[sym_col].astype(str).str.strip()
    cw_qty = pd.to_numeric(
        closed_equity_df["quantity"], errors="coerce"
    ).fillna(0).abs()
    held_for_row = pd.Series(
        [held_map.get((a, s), 0.0) for a, s in zip(cw_acct, cw_sym)],
        index=closed_equity_df.index,
        dtype=float,
    )
    drop_mask = is_writeoff & (cw_qty > 0) & (held_for_row >= cw_qty)
    if not drop_mask.any():
        return closed_equity_df, empty_removed
    return (
        closed_equity_df.loc[~drop_mask].copy(),
        closed_equity_df.loc[drop_mask].copy(),
    )


def _addback_phantom_writeoffs_to_summary(
    summary_df: pd.DataFrame, removed_writeoffs: pd.DataFrame
) -> pd.DataFrame:
    """Reverse the bogus realized contribution from
    ``_drop_phantom_equity_writeoffs`` out of ``positions_summary``.

    ``positions_summary`` aggregates ``int_closed_equity_legs`` into per
    ``(account, symbol, strategy, status)`` rollups. When dbt still
    emits a phantom "Cost Written Off" row, ``positions_summary``'s
    Closed strategy row for that ``(account, symbol)`` carries the
    bogus realized P&L. After the Python strip, that strategy row would
    still show the phantom number — so Strategy Breakdown disagrees
    with Position Legs + Breakdown by Type.

    Per ``(account, symbol)`` writeoff bucket: find the Closed strategy
    row with the realized P&L closest to ``-addback`` and add the
    writeoff back to ``realized_pnl`` / ``total_pnl`` / ``total_return``.
    Trade and date counters are left alone — the underlying fills are
    real, only the writeoff *amount* was the dbt artifact."""
    if summary_df is None or summary_df.empty:
        return summary_df
    if removed_writeoffs is None or removed_writeoffs.empty:
        return summary_df
    if "realized_pnl" not in summary_df.columns:
        return summary_df
    if "realized_pnl" not in removed_writeoffs.columns:
        return summary_df
    sym_col_wo = next(
        (c for c in ("symbol", "trade_symbol")
         if c in removed_writeoffs.columns),
        None,
    )
    sym_col_s = next(
        (c for c in ("symbol", "trade_symbol") if c in summary_df.columns),
        None,
    )
    if sym_col_wo is None or sym_col_s is None \
            or "account" not in removed_writeoffs.columns \
            or "account" not in summary_df.columns:
        return summary_df
    addbacks = (
        removed_writeoffs.assign(
            _addback=pd.to_numeric(
                removed_writeoffs["realized_pnl"], errors="coerce"
            ).fillna(0).abs()
        )
        .groupby(
            [
                removed_writeoffs["account"].astype(str).str.strip(),
                removed_writeoffs[sym_col_wo].astype(str).str.strip(),
            ],
            as_index=True,
        )["_addback"]
        .sum()
    )
    if addbacks.empty:
        return summary_df
    out = summary_df.copy()
    s_acct = out["account"].astype(str).str.strip()
    s_sym = out[sym_col_s].astype(str).str.strip()
    s_status = (
        out["status"].astype(str).str.strip().str.lower()
        if "status" in out.columns else None
    )
    money_cols = [
        c for c in (
            "realized_pnl", "total_pnl", "total_return"
        ) if c in out.columns
    ]
    for (acct, sym), addback in addbacks.items():
        if not addback or addback <= 0:
            continue
        mask = s_acct.eq(acct) & s_sym.eq(sym)
        if s_status is not None:
            mask = mask & s_status.eq("closed")
        candidates = out.loc[mask]
        if candidates.empty:
            continue
        # Pick the row whose realized_pnl is most-closely the writeoff
        # carrier (realized ≈ -addback). On exact match this lands on
        # the dominant carrier; on partial overlap it still shrinks the
        # row that absorbed the most writeoff.
        cand_realized = pd.to_numeric(
            candidates["realized_pnl"], errors="coerce"
        ).fillna(0)
        target_idx = (cand_realized + addback).abs().idxmin()
        for col in money_cols:
            cur = float(pd.to_numeric(
                pd.Series([out.at[target_idx, col]]), errors="coerce"
            ).fillna(0).iloc[0])
            out.at[target_idx, col] = round(cur + float(addback), 2)
    return out


def _equity_slice_for_live_chart(current_df: pd.DataFrame) -> pd.DataFrame:
    """Rows that carry equity MTM for the LIVE today-row patch.

    Match case-insensitively and strip whitespace — BQ/pandas sometimes
    surface ``\"equity\"`` or padded values; strict ``== \"Equity\"``
    skipped the patch so the chart terminal stayed at the walker's
    realized-only value while KPIs used broker ``unrealized_pnl``."""
    if current_df is None or current_df.empty:
        return pd.DataFrame()
    if "instrument_type" not in current_df.columns:
        return pd.DataFrame()
    it = current_df["instrument_type"].astype(str).str.strip().str.lower()
    return current_df.loc[it.eq("equity")].copy()


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


def _collapse_mart_daily_pnl_duplicate_grain(daily_df: pd.DataFrame) -> pd.DataFrame:
    """Collapse duplicate ``mart_daily_pnl`` rows before stateful equity P&L.

    Natural grain is ``(tenant_id, account, user_id, symbol, date)``.
    Sync/backfill bugs can emit identical twins —
    ``_build_chart_from_daily_pnl`` processes each row and sums
    ``equity_*`` deltas, doubling buys/sells and inflating terminal P&L
    (May 2026 BE chart ~2× hero).

    CRITICAL: ``tenant_id`` leads the dedup key when present. Several
    physical accounts can share an ``account`` display label (e.g. multiple
    "Schwab Account"s); deduping on ``(account, symbol, date)`` alone would
    collapse those distinct tenants' same-symbol/day rows into one and drop
    the rest. Prefers populated ``user_id`` over ``NULL`` when deduping,
    then merges strict full-key collisions with ``keep=\"last\"`` (later
    ingestion wins).
    """
    if daily_df is None or daily_df.empty:
        return daily_df
    if not {"account", "symbol", "date"}.issubset(daily_df.columns):
        return daily_df
    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    stab = "__r_i__"
    df[stab] = range(len(df))
    _tenant_key = ["tenant_id"] if "tenant_id" in df.columns else []
    ks3 = _tenant_key + ["account", "symbol", "date"]

    if "user_id" in df.columns:
        uid_col = pd.to_numeric(df["user_id"], errors="coerce")
        df["__prefer_uid__"] = uid_col.notna().astype(int)
        df = df.sort_values(
            by=ks3 + ["__prefer_uid__", "user_id", stab],
            # ks3 cols ascending, then prefer populated uid (desc), then
            # user_id asc, then stable index asc.
            ascending=([True] * len(ks3)) + [False, True, True],
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
    # Partition leads with the broker-stable tenant_id (v2 grain) when
    # present so several physical accounts sharing a display label each run
    # their OWN equity cost-basis state machine. ``account``/``user_id``
    # remain in the key for legacy / NULL-tenant rows.
    part_cols = []
    if "tenant_id" in work.columns:
        part_cols.append("tenant_id")
    part_cols.append("account")
    if "user_id" in work.columns:
        part_cols.append("user_id")
    gb = work.groupby(part_cols, dropna=False)
    if gb.ngroups <= 1:
        return _build_chart_from_daily_pnl_partition(work.sort_values("date"), current_df)
    parts = []
    for key, sub in gb:
        key = key if isinstance(key, tuple) else (key,)
        keyed = dict(zip(part_cols, key))
        tenant_k = keyed.get("tenant_id")
        acct = keyed.get("account")
        uid_k = keyed.get("user_id")
        cdf = _filter_current_for_chart_partition(current_df, acct, uid_k, tenant_k)
        parts.append(
            _build_chart_from_daily_pnl_partition(sub.sort_values("date"), cdf)
        )
    return _merge_position_pnl_chart_payloads(parts)


def _walk_equity_terminal(daily_df):
    """Dry-run of the average-cost equity walk — returns the terminal
    ``(shares_held, total_cost, short_shares, short_cost_basis)`` WITHOUT
    building the chart series.

    Used only by the incomplete-history snapshot-lot gate in
    ``_build_chart_from_daily_pnl_partition`` to decide whether the normal
    walk's terminal unrealized materially diverges from the broker snapshot
    (a real spike) before deciding to take over. Mirrors the buy/sell/short
    logic in the main loop exactly so the comparison is apples-to-apples."""
    shares_held = 0.0
    total_cost = 0.0
    short_shares = 0.0
    short_cost_basis = 0.0
    if daily_df is None or daily_df.empty:
        return shares_held, total_cost, short_shares, short_cost_basis
    for _, row in daily_df.sort_values("date").iterrows():
        buy_qty = float(row.get("equity_buy_qty") or 0)
        buy_cost = float(row.get("equity_buy_cost") or 0)
        sell_qty = float(row.get("equity_sell_qty") or 0)
        sell_proceeds = float(row.get("equity_sell_proceeds") or 0)
        if sell_qty > 0:
            remaining_sell = sell_qty
            remaining_proceeds = sell_proceeds
            if shares_held > 0:
                sold_long = min(remaining_sell, shares_held)
                avg = total_cost / shares_held if shares_held > 0 else 0
                frac = sold_long / sell_qty if sell_qty > 0 else 1
                sold_long_proceeds = sell_proceeds * frac
                total_cost = max(0, total_cost - avg * sold_long)
                shares_held = max(0, shares_held - sold_long)
                remaining_sell -= sold_long
                remaining_proceeds -= sold_long_proceeds
            if remaining_sell > 0:
                short_shares += remaining_sell
                short_cost_basis += remaining_proceeds
        if buy_qty > 0:
            remaining_buy = buy_qty
            remaining_cost = buy_cost
            if short_shares > 0:
                covered = min(remaining_buy, short_shares)
                frac = covered / buy_qty if buy_qty > 0 else 1
                cover_cost = buy_cost * frac
                avg_short = short_cost_basis / short_shares if short_shares > 0 else 0
                short_cost_basis = max(0, short_cost_basis - avg_short * covered)
                short_shares = max(0, short_shares - covered)
                remaining_buy -= covered
                remaining_cost -= cover_cost
            if remaining_buy > 0:
                shares_held += remaining_buy
                total_cost += remaining_cost
    return shares_held, total_cost, short_shares, short_cost_basis


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

    # ── INCOMPLETE-HISTORY SNAPSHOT-LOT MODE ──
    # SnapTrade / Schwab return only a short window of transactions, so a
    # long-held equity lot (often years old, frequently pre-split) never has
    # its opening buys in stg_history. The visible fills then show MORE sells
    # than buys and the average-cost walk reconstructs a nonsensical (often
    # negative) equity curve, which the LIVE TODAY override snaps to the
    # broker's true unrealized — the vertical "spike" at the right edge.
    #
    # ``int_equity_sessions`` already handles this: when the visible equity
    # trades net to <= 0 still-open shares (they never form a positive
    # session → dropped as orphans) while the broker snapshot holds shares,
    # it emits a ``snapshot_equity_sessions`` row for the held lot with
    # realized = 0 and unrealized = mv − cb (see that model's snapshot gate
    # + total_pnl clamp). We mirror that here so the chart reconciles with
    # the hero KPI instead of crediting phantom realized:
    #   • seed the broker lot (qty @ broker avg cost) as the opening holding,
    #   • SKIP the orphan equity buy/sell fills (do not touch shares/realized),
    #   • let realized stay 0 — the whole gain rides in daily mark-to-market.
    # Real case DXCM (user 18): visible history 1,000 bought / 1,900 sold, but
    # the broker holds 1,100 @ $22.63 (pre-4:1-split basis) → mart realized $0,
    # unrealized $70,742. Pre-fix the chart drifted to −$32k then jumped to
    # +$47k on the last day.
    #
    # Gate mirrors the mart exactly (net trade-open <= 0 AND broker holds a
    # LONG lot). A clean, reconciling history (net open > 0) keeps the normal
    # average-cost walk untouched; a short book is left alone.
    snapshot_lot_mode = False
    if (not current_df.empty
            and {"equity_buy_qty", "equity_sell_qty"}.issubset(daily_df.columns)):
        _eq_seed = _equity_slice_for_live_chart(current_df)
        if not _eq_seed.empty and "quantity" in _eq_seed.columns:
            _broker_qty = float(
                pd.to_numeric(_eq_seed["quantity"], errors="coerce").fillna(0).sum()
            )
            _broker_cb = 0.0
            for _cbn in ("cost_basis", "cost_bases"):
                if _cbn in _eq_seed.columns:
                    _broker_cb = float(
                        pd.to_numeric(_eq_seed[_cbn], errors="coerce").fillna(0).sum()
                    )
                    break
            _sell_qty_hist = float(
                pd.to_numeric(daily_df["equity_sell_qty"], errors="coerce").fillna(0).sum()
            )
            _buy_qty_hist = float(
                pd.to_numeric(daily_df["equity_buy_qty"], errors="coerce").fillna(0).sum()
            )
            _net_trade_open = _buy_qty_hist - _sell_qty_hist
            _unaccounted = _broker_qty - _net_trade_open
            # Only mirror the mart when it is EXACT to do so — i.e. the visible
            # history yields NO realizable equity P&L, so we can't drop a real
            # number by seeding the missing lot at realized 0:
            #   • net_trade_open <= 0  → sells net >= buys; the trades never
            #     form a positive session (dropped as orphans) and the mart
            #     clamps realized to 0 (DXCM: 1,000 bought / 1,900 sold,
            #     holds 1,100).
            #   • total sells == 0     → buy-only holding; no sells means
            #     realized is provably 0 (JEPQ: 4,825 bought but 5,826 held —
            #     1,000 transferred in before the window).
            #
            # The remaining class — history has BOTH real sells (genuine
            # realized) AND unaccounted extra shares (e.g. IYW 489cb2: nets
            # 2,200, holds 3,300, $83k realized) — is deliberately NOT handled
            # here: skipping the trades would drop that realized, and the
            # walk's average-cost realized can't match the mart's clamped
            # realized on incomplete history. Those still need the mart's
            # realized driven into the chart (tracked as follow-up).
            _no_realizable = (_net_trade_open <= 1e-6) or (_sell_qty_hist <= 1e-6)
            if (_broker_qty > 1e-6 and _no_realizable
                    and _unaccounted > max(1.0, 0.02 * _broker_qty)):
                # POST-WALK RECONCILIATION GATE. Do NOT take over just because
                # the broker holds a few more (fractional / dividend-reinvested)
                # shares than history — those positions already reconcile and
                # the average-cost walk is correct. Only act when the normal
                # walk's terminal unrealized MATERIALLY diverges from the
                # broker's (a real spike). A cheap dry-run of the average-cost
                # engine gives the walk's terminal state without the series.
                _bh, _bc, _sh, _sc = _walk_equity_terminal(daily_df)
                _last_close = 0.0
                for _cp in reversed(daily_df["close_price"].tolist()
                                    if "close_price" in daily_df.columns else []):
                    _cpv = float(_cp) if _cp is not None and pd.notna(_cp) else 0.0
                    if _cpv > 0:
                        _last_close = _cpv
                        break
                _walk_unreal = 0.0
                if _last_close > 0:
                    if _bh > 0:
                        _walk_unreal += _bh * _last_close - _bc
                    if _sh > 0:
                        _walk_unreal -= (_sh * _last_close - _sc)
                _broker_unreal = 0.0
                if "unrealized_pnl" in _eq_seed.columns:
                    _broker_unreal = float(
                        pd.to_numeric(_eq_seed["unrealized_pnl"], errors="coerce").fillna(0).sum()
                    )
                if abs(_broker_unreal - _walk_unreal) > 2000.0:
                    if _net_trade_open <= 1e-6:
                        # ORPHAN: sells outrun buys → the walk would go
                        # spuriously short and book phantom realized. Skip ALL
                        # equity fills and seed the whole broker lot; realized
                        # stays 0 exactly as the mart clamps it. (DXCM, BKH)
                        snapshot_lot_mode = True
                        shares_held = _broker_qty
                        total_cost = _broker_cb
                    else:
                        # BUY-ONLY + TRANSFER: the visible buys are REAL and
                        # must mark-to-market from their own trade dates — only
                        # the (broker_qty − net_bought) shares transferred in
                        # BEFORE the window are missing. Seed just that
                        # held-before sleeve at its residual cost basis
                        # (broker_cb − walked buy cost) from the spine start;
                        # the real buys then accumulate on top so the walk ends
                        # at exactly (broker_qty @ broker_cb). This avoids the
                        # fabrication of showing the FULL current lot before it
                        # was actually held — JEPQ pre-fix opened at −$99k in
                        # 2024 for shares mostly bought in 2025. Realized stays
                        # 0 (no sells).
                        shares_held = _unaccounted
                        total_cost = max(0.0, _broker_cb - _bc)

    # mart_daily_pnl's dense spine starts at the account's earliest trade
    # date ACROSS ALL SYMBOLS, so a per-symbol slice can carry a long
    # flat-$0 prefix before this symbol's first fill (e.g. BE's chart
    # opening on 1/29 when the first BE trade was in April). Each position
    # chart should begin when the position actually opened. Skip leading
    # rows until the first activity; the closed-position branch below
    # already trims these for closed positions, this covers OPEN ones.
    position_started = False

    dates, equity_s, options_s, dividends_s, total_s, price_s = (
        [], [], [], [], [], [],
    )
    last_cumulative_options_realized = 0.0
    last_open_options_unrealized = 0.0
    last_cumulative_other_pnl = 0.0
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

        # Trim the leading pre-open prefix. Until the position's first
        # activity, every series value is 0 and there are no holdings —
        # rendering those days makes the chart start before the position
        # existed. Mark "started" on the first fill (equity or option) or
        # the first non-zero cumulative series, then render every day after.
        if not position_started:
            _div_now = float(row.get("cumulative_dividends_pnl") or 0)
            _oth_now = float(row.get("cumulative_other_pnl") or 0)
            if (has_trade or buy_qty > 0 or sell_qty > 0
                    or cur_realized_for_skip != 0 or cur_open_mtm_for_skip != 0
                    or _div_now != 0 or _oth_now != 0):
                position_started = True
            else:
                continue

        # Process sells first (may create short position). Skipped in
        # snapshot-lot mode: the visible fills are orphans the mart discarded,
        # so touching the seeded broker lot with them would re-introduce the
        # phantom realized / spike we are mirroring the mart to avoid.
        if not snapshot_lot_mode and sell_qty > 0:
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

        # Process buys (may cover short position). Skipped in snapshot-lot mode
        # for the same reason as sells above.
        if not snapshot_lot_mode and buy_qty > 0:
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
        last_cumulative_other_pnl = oth_pnl
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
        # the user's connection paused). For "today" we override the
        # mart's row with values computed from ``current_df`` (which
        # comes from ``int_enriched_current``) so the chart's terminal
        # matches the headline KPIs / positions_summary / Breakdown-by-
        # type — all of which read the SAME ``int_enriched_current``.
        #
        # CLOSE-BASED REPORTING (June 2026): ``int_enriched_current``
        # now prices today's EQUITY at the official yfinance close once
        # it is published (after the bell), falling back to the broker
        # live mark only intraday — see int_enriched_current header +
        # AGENTS.md "Pricing Precedence". So this override automatically
        # uses the close when published; we no longer paint the broker's
        # transient after-hours mark onto the terminal. Options/cash stay
        # broker-derived. Because both the mart today-row and this override
        # resolve to close-when-published, the chart terminal == hero by
        # construction (the reconciliation invariant) with no rescaling.
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
        eq_row = _equity_slice_for_live_chart(current_df)
        today_eq = equity_s[-1]
        # When the broker's live snapshot has equity AND a current
        # price, prefer the snapshot's unrealized columns. Sum of
        # ``unrealized_pnl`` matches positions_summary / Breakdown-by-
        # type and works even when the mart trade-history walker thinks
        # shares_flat (e.g. bogus same-day churn in ``mart_daily_pnl``).
        #
        # If we only trusted mv−cb and (mv,cb) were both falsy because
        # columns were missing, we fell through to ``shares_held > 0`` —
        # but that's false when the walker already flattened the lot —
        # so we'd leave ``today_eq`` at the walker terminal (pure
        # realized −\$1,957) while KPIs added +\$349 broker unreal —
        # IYW invariant gap (May 2026).
        if not eq_row.empty:
            if "unrealized_pnl" in eq_row.columns:
                ur_sum = pd.to_numeric(
                    eq_row["unrealized_pnl"], errors="coerce"
                ).fillna(0.0).sum()
                today_eq = cum_realized + float(ur_sum)
            else:
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
                today_eq + today_option_pnl + dividends_s[-1]
                + last_cumulative_other_pnl,
                2,
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
                round(
                    today_eq + today_option_pnl + dividends_s[-1]
                    + last_cumulative_other_pnl,
                    2,
                )
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

    # Equity cost-basis state and per-symbol realized options are keyed by
    # the broker-stable tenant_id (v2 grain) when present, so several
    # physical accounts sharing a display label (e.g. multiple "Schwab
    # Account"s) don't fuse one symbol's running average-cost state.
    _has_tenant = "tenant_id" in daily_df.columns

    def _eq_key(r):
        if _has_tenant and pd.notna(r.get("tenant_id")):
            return (r.get("tenant_id"), r["symbol"])
        return (r["account"], r["symbol"])

    eq_state = {}
    # Last-known close per equity key, carried forward across days on which a
    # symbol has NO mart row. mart_daily_pnl's equity spine is sparse for
    # thinly-priced / crypto holdings (e.g. USDC has ~50 rows, VRT ~34), so
    # on most days at least one currently-held symbol is absent. The prior
    # "skip the whole day unless EVERY held symbol is present" rule then
    # dropped ~9 months of trading days for a multi-symbol portfolio,
    # collapsing all that P&L into the lone synthetic "today" point (the
    # giant end-of-chart spike). Carrying the last close forward marks every
    # held lot on every trading day regardless of which symbols reported.
    last_close = {}
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

    # mart_daily_pnl's dense spine starts at the account's earliest activity
    # date, but for a freshly-connected account that can be weeks of leading
    # flat-$0 days before the first trade (e.g. an Alpaca account created in
    # late June rendering a flat line back to mid-May). Skip those leading
    # zero days so the chart begins when the account actually started trading
    # — mirrors the per-position chart's ``position_started`` trim.
    account_started = False

    # Iterate one date at a time via a single groupby pass. Previously this
    # re-scanned the WHOLE frame per date (``daily_df[daily_df["date"] == d]``
    # inside a loop over every date) — O(dates × rows), which for a
    # day-trader's dense multi-symbol spine blew the account-chart build to
    # ~16s of pure Python (see REQUEST_TIMING steps=acct_chart). groupby is a
    # single O(rows) partition; groups come back sorted by date and NaN dates
    # are dropped, matching the old ``sorted(...dropna().unique())`` semantics.
    for d, day in daily_df.groupby("date", sort=True):

        # Materialize this date's rows ONCE as plain dicts and reuse across
        # all three passes below. ``iterrows`` builds a fresh Series per row
        # and is ~5-10× slower than ``to_dict("records")`` — for the heavy
        # day-trader account (dense spine × many symbols) the three iterrows
        # passes were ~8s of the acct_chart build. ``_eq_key`` and every
        # ``.get(...)`` / ``[...]`` access below work identically on a dict.
        day_records = day.to_dict("records")

        # Update per-symbol realized cumulative from the mart (carried
        # forward across days when no new realization happened).
        for r in day_records:
            key = _eq_key(r)
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

        for row in day_records:
            key = _eq_key(row)
            if key not in eq_state:
                eq_state[key] = {
                    "shares": 0.0, "cost": 0.0,
                    "short_shares": 0.0, "short_cost": 0.0,
                    "realized": 0.0,
                }
            s = eq_state[key]
            bq = float(row.get("equity_buy_qty") or 0)
            bc = float(row.get("equity_buy_cost") or 0)
            sq = float(row.get("equity_sell_qty") or 0)
            sp = float(row.get("equity_sell_proceeds") or 0)

            # Sells first: close any long lot (realized vs avg cost), then
            # open/extend a SHORT with the remainder. Without the short branch
            # a sale with no long inventory booked the ENTIRE proceeds as
            # realized profit (zero cost basis), so a short-heavy day-trader's
            # equity line rocketed to a phantom gain (cameronbot: +$46,937 on
            # 10 short positions). Mirrors _build_chart_from_daily_pnl.
            if sq > 0:
                remaining_sell = sq
                remaining_proceeds = sp
                if s["shares"] > 0:
                    sold_long = min(remaining_sell, s["shares"])
                    avg = s["cost"] / s["shares"] if s["shares"] > 0 else 0.0
                    frac = sold_long / sq if sq > 0 else 1.0
                    sold_long_proceeds = sp * frac
                    s["realized"] += sold_long_proceeds - avg * sold_long
                    s["cost"] = max(0.0, s["cost"] - avg * sold_long)
                    s["shares"] = max(0.0, s["shares"] - sold_long)
                    remaining_sell -= sold_long
                    remaining_proceeds -= sold_long_proceeds
                if remaining_sell > 0:
                    s["short_shares"] += remaining_sell
                    s["short_cost"] += remaining_proceeds

            # Buys: cover any short first (realized vs avg short proceeds),
            # then extend the long.
            if bq > 0:
                remaining_buy = bq
                remaining_cost = bc
                if s["short_shares"] > 0:
                    covered = min(remaining_buy, s["short_shares"])
                    frac = covered / bq if bq > 0 else 1.0
                    cover_cost = bc * frac
                    avg_short = s["short_cost"] / s["short_shares"] if s["short_shares"] > 0 else 0.0
                    s["realized"] += avg_short * covered - cover_cost
                    s["short_cost"] = max(0.0, s["short_cost"] - avg_short * covered)
                    s["short_shares"] = max(0.0, s["short_shares"] - covered)
                    remaining_buy -= covered
                    remaining_cost -= cover_cost
                if remaining_buy > 0:
                    s["shares"] += remaining_buy
                    s["cost"] += remaining_cost

        # Record today's closes, then mark EVERY held lot at its last-known
        # close (carry forward for symbols absent from today's group).
        for row in day_records:
            close = float(row.get("close_price") or 0)
            if close > 0:
                last_close[_eq_key(row)] = close
        eq_total = sum(s["realized"] for s in eq_state.values())
        for key, s in eq_state.items():
            close = last_close.get(key, 0.0)
            if close > 0:
                if s["shares"] > 0:
                    eq_total += s["shares"] * close - s["cost"]
                if s["short_shares"] > 0:
                    eq_total += s["short_cost"] - s["short_shares"] * close

        # Trim the leading pre-first-trade prefix. Until the account has any
        # activity, every series value is 0 and there are no holdings. Detect
        # activity via a trade today, any held/short equity, or any non-zero
        # cumulative series (eq_total can be 0 on the first buy day when the
        # mark equals cost, so a trade-today check is required — not just the
        # totals).
        if not account_started:
            day_buy = float(day["equity_buy_qty"].fillna(0).sum()) if "equity_buy_qty" in day.columns else 0.0
            day_sell = float(day["equity_sell_qty"].fillna(0).sum()) if "equity_sell_qty" in day.columns else 0.0
            has_holdings = any(
                abs(s["shares"]) > 1e-9 or abs(s["short_shares"]) > 1e-9
                for s in eq_state.values()
            )
            if (day_buy > 0 or day_sell > 0 or has_holdings
                    or abs(eq_total) > 1e-9 or abs(cum_opt) > 1e-9
                    or abs(cum_div) > 1e-9 or abs(cum_oth) > 1e-9):
                account_started = True
            else:
                continue

        # ── Skip genuine non-trading days (weekends / market holidays) ──
        # yfinance publishes no weekend/holiday equity close, so an equity
        # symbol has no row on those dates (SPY jumps Fri→Mon). OPTION
        # contracts carry a CALENDAR-dense spine, so a weekend/holiday still
        # emits a date-group containing ONLY option-bearing symbols. Because
        # equity is now marked from ``last_close`` (carried forward), a
        # missing symbol no longer drops its MTM — so the only reason to skip
        # is cosmetic: don't plot flat weekend/holiday points (the user asked
        # to exclude them). Skip weekends, and skip any WEEKDAY on which no
        # currently-held equity symbol reported a close (a market holiday /
        # pure option-only group). A day where at least one held equity is
        # priced is a real trading session → emit. All running state is
        # already updated above, so the next emitted point is exact.
        _held_keys = {
            k for k, s in eq_state.items()
            if abs(s["shares"]) > 1e-9 or abs(s["short_shares"]) > 1e-9
        }
        _priced_today = any(
            float(r.get("close_price") or 0) > 0 and _eq_key(r) in _held_keys
            for r in day_records
        )
        _is_weekend = pd.Timestamp(d).weekday() >= 5
        # Skip weekends always; skip weekdays only when we hold equity yet
        # none of it printed a close today (holiday). If we hold no equity at
        # all (options/cash only), emit so those series still render.
        if _is_weekend or (bool(_held_keys) and not _priced_today):
            continue

        dates_out.append(str(d)[:10])
        equity_s.append(round(eq_total, 2))
        options_s.append(round(cum_opt, 2))
        dividends_s.append(round(cum_div, 2))
        total_s.append(round(eq_total + cum_opt + cum_div + cum_oth, 2))

    # Anchor the whole series at $0 the day BEFORE the first trading day so
    # every chart provably STARTS at zero. The account had no P&L before it
    # began trading, but the first EMITTED point is already the first day's
    # cumulative P&L (a day-trader can bank realized gains on day one), so the
    # un-rebased "All time" view opened mid-air — real report: cameronbot's
    # Alpaca all-time curve started well above $0. A single baseline point is
    # NOT the weeks-of-flat-$0 prefix the account_started trim above removes;
    # it's the zero the curve climbs from. The terminal (lifetime total) is
    # untouched so the chart still reconciles with the Total Return KPI, and
    # windowed ranges rebase client-side against their own window start — the
    # anchor is the earliest date, so it only ever precedes the ALL curve (and
    # if a window reaches all the way back, it rebases against this 0 anyway).
    if dates_out:
        _anchor = (date.fromisoformat(dates_out[0]) - timedelta(days=1)).isoformat()
        dates_out.insert(0, _anchor)
        equity_s.insert(0, 0.0)
        options_s.insert(0, 0.0)
        dividends_s.insert(0, 0.0)
        total_s.insert(0, 0.0)

    today = date.today()
    today_str = str(today)
    # Synthesize a "today" point ONLY when the mart is genuinely STALE (its
    # last emitted trading day is several days behind today). Rationale: this
    # is a close-based end-of-day report and the mart is rebuilt nightly, so
    # in the normal case its last row IS the latest trading session (e.g. on
    # a Monday the last row is Friday's close). Appending a live snapshot
    # point in that case only introduces a discontinuity, because the live
    # snapshot (int_enriched_current) prices open EQUITY off the broker's
    # cost basis while this chart's terminal is the average-cost walk over
    # stg_history buys — the two disagree for transferred-in lots that never
    # had a buy row (a Schwab portfolio can carry $40k+ of such open
    # unrealized invisible to the walk), so the final point would SPIKE. Only
    # when the mart is actually behind (sync/dbt lagged for days) is filling
    # "today" from the live snapshot worth the basis mismatch. Normal weekend
    # gap (Fri->Mon = 3 days, or +1 for a Monday holiday) does NOT append.
    _last_emitted = date.fromisoformat(dates_out[-1]) if dates_out else None
    _mart_is_stale = (
        _last_emitted is not None and (today - _last_emitted).days > 4
    )
    if (not current_df.empty and dates_out and dates_out[-1] != today_str
            and today.weekday() < 5 and _mart_is_stale):
        # Synthetic today row when the mart hasn't been built yet for
        # today (sync ran but dbt hasn't refreshed yet).
        #
        # Equity: the value is REALIZED-to-date + today's OPEN unrealized
        # (a REPLACEMENT of the last mart day's open mark-to-market, NOT an
        # addition on top of it). ``equity_s[-1]`` already equals
        # ``realized + open-MTM-at-the-last-mart-close``; the previous code
        # did ``equity_s[-1] + eq_unreal`` which double-counted the ENTIRE
        # open-equity unrealized, drawing a giant spike at the final point
        # (Schwab-heavy "All Accounts": a ~+$95k jump that made the chart
        # terminal disagree with the Total Return KPI; Alpaca: a phantom
        # -$6.6k drop). Rebuilding from realized + live unrealized removes
        # the spike and reconciles the terminal with the account hero.
        #
        # CLOSE-BASED REPORTING (June 2026): ``current_df`` comes from
        # ``int_enriched_current``, whose equity unrealized is priced at the
        # official close once published (broker live mark only intraday), so
        # this synthetic row snaps to the close too. See AGENTS.md
        # "Pricing Precedence".
        #
        # Options: same realize-on-close replacement —
        #   today_options = (last realized cumulative across symbols)
        #                 + (LIVE open MTM from current_df today)
        eq_realized_total = sum(s["realized"] for s in eq_state.values())
        eq_unreal = float(current_df.loc[current_df["instrument_type"] == "Equity", "unrealized_pnl"].sum())
        today_equity = round(eq_realized_total + eq_unreal, 2)
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
        if today_equity != equity_s[-1] or today_options != options_s[-1]:
            dates_out.append(today_str)
            equity_s.append(today_equity)
            options_s.append(today_options)
            dividends_s.append(dividends_s[-1])
            total_s.append(round(today_equity + today_options + dividends_s[-1] + cum_oth, 2))

    return {
        "dates": dates_out,
        "equity": equity_s,
        "options": options_s,
        "dividends": dividends_s,
        "total": total_s,
    }


def _build_option_matrices(matrix_df, symbol):
    """Reshape pre-bucketed matrix cells into per-strategy heatmaps.

    The DTE x Strike-Distance bucketing now happens in dbt
    (``mart_option_win_matrix``); ``matrix_df`` already carries one row per
    (tenant, account, user, strategy, dte_label, strike_col) with raw
    ``trade_count`` / ``wins`` / ``sum_pnl``. This function only:
      1. combines cells across tenants/accounts for the scoped view, then
      2. rounds avg P&L and win rate ONCE (after the union), matching the
         old per-contract math exactly.

    ``matrix_df`` arrives ALREADY tenant-scoped (SQL ``{tenant_filter}`` +
    ``_filter_df_by_tenant_ids`` in the caller), so no account predicate is
    applied here — the old display-label filter never matched the warehouse
    broker label and re-fused colliding-label accounts.
    """
    import math

    df = matrix_df[matrix_df["underlying_symbol"] == symbol].copy()
    if df.empty:
        return []

    for col in ("trade_count", "wins", "sum_pnl"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Canonical label ordering (mirrors the dbt bucket labels). Columns read
    # left-to-right ITM -> OTM; the em-dash "unknown" column is appended last.
    PCT_ORDER = ["<-10%", "-10 to -5%", "-5 to -2%", "ATM ±2%", "+2 to +5%", "+5 to +10%", ">+10%"]
    DTE_ORDER = ["0–7", "8–14", "15–30", "31–60", "61+"]

    matrices = []
    for strategy, grp in df.groupby("strategy"):
        # Combine duplicate cells across tenants/accounts. sum_pnl + count
        # aggregate cleanly: mean over the union == Σsum_pnl / Σcount.
        agg = grp.groupby(["dte_label", "strike_col"], as_index=False).agg(
            count=("trade_count", "sum"),
            wins=("wins", "sum"),
            sum_pnl=("sum_pnl", "sum"),
        )
        present_cols = set(agg["strike_col"])
        col_range = [lbl for lbl in PCT_ORDER if lbl in present_cols]
        if "—" in present_cols:
            col_range.append("—")

        present_dtes = set(agg["dte_label"])
        dte_order = [lbl for lbl in DTE_ORDER if lbl in present_dtes]

        cell_map = {(r["dte_label"], r["strike_col"]): r for _, r in agg.iterrows()}

        rows = []
        for dte_lbl in dte_order:
            cells = []
            for col_val in col_range:
                r = cell_map.get((dte_lbl, col_val))
                total = int(r["count"]) if r is not None else 0
                if total <= 0:
                    cells.append({"count": 0, "avg_pnl": None, "win_rate": None})
                else:
                    wins = int(r["wins"])
                    avg_pnl_dollar = float(r["sum_pnl"]) / total
                    cells.append({
                        "count": total,
                        "avg_pnl": round(avg_pnl_dollar, 0) if not math.isnan(avg_pnl_dollar) else None,
                        "win_rate": round(wins / total * 100, 0),
                        "wins": wins,
                    })
            rows.append({"dte_label": dte_lbl, "cells": cells})

        matrices.append({
            "strategy": strategy,
            "trade_count": int(agg["count"].sum()),
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


def _snap_position_chart_terminal_to_breakdown(
    chart_data: dict | None, breakdown_rows: list | None
) -> None:
    """Pinned hero + Breakdown-by-type both use Σ ``breakdown_rows`` totals.

    The cumulative chart can still end on a mart-only realization (broker
    open-equity unreal and/or dividend cumulative missing on the spine)
    when substitution or LIVE patching does not fully apply — IYW Emmory:
    hero ≈ -$1.607 vs chart ≈ realized -$1.957 .

    Bump **only** the last plotted bucket (not proportional history
    rescale): adjust ``total[-1]`` to the ledger and apply the delta to the
    equity stream so stacked components remain consistent."""
    tol = 1.0  # Must match CHART_KPI_ALIGN_TOLERANCE_DOLLARS.
    if not chart_data or not breakdown_rows or not chart_data.get("total"):
        return
    totals = chart_data["total"]
    n = len(totals)
    if n < 1:
        return
    ledger = round(
        sum(float(r.get("total") or 0) for r in breakdown_rows), 2,
    )
    tail = round(float(totals[-1] or 0), 2)
    if abs(ledger - tail) <= tol:
        return
    delta = round(ledger - tail, 2)
    try:
        app.logger.info(
            "snap chart terminal to breakdown ledger: Δ=%.2f ledger=%.2f "
            "was_tail=%.2f",
            delta,
            ledger,
            tail,
        )
    except Exception:
        _log.info(
            "snap chart terminal to breakdown ledger: Δ=%.2f ledger=%.2f "
            "was_tail=%.2f",
            delta,
            ledger,
            tail,
        )
    totals[-1] = round(float(totals[-1] or 0) + delta, 2)
    eq = chart_data.get("equity")
    if eq is not None and len(eq) == n:
        eq[-1] = round(float(eq[-1] or 0) + delta, 2)


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
            _log.warning(
                "_align_position_pnl_chart_with_kpi: refusing rescale "
                "gap=$%.2f chart=$%.2f kpi=$%.2f",
                gap, t_end, k,
            )
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




# Chart-source SQL shared by Position Detail, Symbols, and Accounts.
CHART_DATA_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.mart_daily_pnl`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
      {tenant_filter}
    ORDER BY date
"""

# Pre-aggregated daily P&L data for all symbols (account-level charts)
CHART_DATA_ALL_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.mart_daily_pnl`
    WHERE 1=1 {tenant_filter}
    ORDER BY symbol, date
"""

