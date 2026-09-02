"""Query-cache lifecycle operations: rebuild-triggered flush + warm.

WHY THIS EXISTS
---------------
Reporting is CLOSE-BASED: warehouse data only changes when a dbt rebuild
finishes (workflow_dispatch after seed writes, evening prices refresh,
nightly backstop). Between rebuilds the same 12-15 BigQuery queries per
page return byte-identical results, so the query cache can hold results
for HOURS — as long as it is flushed at the exact moment the data really
changes. This module provides that hook:

    POST /internal/cache/flush   (X-Cache-Flush-Token: <CACHE_FLUSH_TOKEN>)

called as the LAST step of ``bigquery_update.yml`` and
``prices_refresh.yml``. It clears the shared cache and then WARMS the
hottest query sets in a background thread so even the first visit after a
rebuild is fast.

TENANT SAFETY
-------------
Warming runs the same tenant-scoped SQL the pages run (the tenant filter
is inlined per user), so a warmed entry is only ever hit by requests for
that same tenant scope — identical guarantee to ``app/query_cache.py``.
The warmer only POPULATES the query-result layer; every request still
applies ``_filter_df_by_tenant_ids`` on top.

STALENESS BOUND
---------------
``query_cache.clear()`` empties this worker's in-process L1 and the shared
Redis L2. The OTHER Gunicorn worker's L1 survives until its own TTL
(default 10 min) — so post-rebuild staleness is bounded by the L1 TTL,
same as before this module existed. The big win is the shared L2 TTL can
now be a day instead of 10 minutes.
"""

import hmac
import logging
import os
import sys
import threading
import time

from flask import abort, jsonify, request

from app import app
from app.extensions import csrf, limiter
from app import query_cache

# Same pattern as the REQUEST_TIMING logger in app/__init__.py: module
# loggers have no stdout handler in prod (root logger only surfaces
# WARNING+ to stderr), so CACHE_FLUSH / CACHE_WARM lines would be
# invisible in Render logs without an explicit handler. These lines are
# the only evidence the rebuild->flush->warm chain is alive.
_log = logging.getLogger("happytrader.cache")
if not _log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("%(message)s"))
    _log.addHandler(_h)
    _log.setLevel(logging.INFO)
    _log.propagate = False

# Only one warm pass at a time; a second flush while warming just flushes.
_warm_lock = threading.Lock()


def warehouse_tenants_present(tenant_ids):
    """Return the subset of ``tenant_ids`` that already have Overview rows.

    Reads ``positions_summary`` (same grain as Get Started / first look).
    Fail closed: missing tenants, a BQ error, or an empty list → empty set,
    so a later rebuild can retry the data-ready email.
    """
    from app.tenant_scope import (
        filter_df_by_tenant_ids,
        sanitize_tenant_id,
        tenant_sql_filter,
    )

    safe = [sanitize_tenant_id(t) for t in (tenant_ids or [])]
    safe = [t for t in safe if t]
    if not safe:
        return set()
    try:
        from app.bigquery_client import get_bigquery_client
        from app.query_cache import cached_query_df
        where = tenant_sql_filter(safe)
        sql = (
            "SELECT DISTINCT tenant_id "
            "FROM `ccwj-dbt.analytics.positions_summary` "
            f"{where}"
        )
        # After ?ready=1 the cache was just flushed; a miss is a real BQ
        # read. The post-connect poll reuses the same helper, so an empty
        # result stays cached until the next flush instead of re-querying
        # every 8s.
        df = cached_query_df(
            get_bigquery_client(), sql, label="data_ready_tenants",
        )
        # SQL already scopes to ``safe``; DataFrame filter is the same
        # belt-and-suspenders as every user-facing BQ read (fail closed
        # if tenant_id is missing from the frame).
        df = filter_df_by_tenant_ids(df, safe)
    except Exception as exc:
        _log.warning("warehouse_tenants_present failed: %s", exc)
        return set()
    if df is None or df.empty or "tenant_id" not in df.columns:
        return set()
    found = set()
    for raw in df["tenant_id"].tolist():
        tid = sanitize_tenant_id(raw)
        if tid:
            found.add(tid)
    return found


def warehouse_has_rows_for_tenants(tenant_ids):
    """True when Overview can render at least one row for these tenants."""
    return bool(warehouse_tenants_present(tenant_ids))


def _send_data_ready_after_rebuild():
    """Email users whose first warehouse is now queryable.

    A tenant row is not enough — connect registers ``broker_tenants``
    before any sync, so a coincidental rebuild used to mail "your data
    is ready" while Overview was still empty. Require
    ``positions_summary`` rows for that user's tenants. Dedupe:
    ``email_sends`` kind ``data_ready``, key ``user_id``. Do not record
    a send when the warehouse is still empty (retry on the next
    ``?ready=1`` flush). Never raises into the flush request.
    """
    try:
        from app.db import fetch_all
        from app.email import app_base_url, send_data_ready_email
        from app.models import User, record_email_send
        from app.utils import DEMO_USERNAME

        rows = fetch_all(
            "SELECT user_id, tenant_id FROM broker_tenants "
            "WHERE user_id IS NOT NULL AND connection_status = 'active'"
        ) or []
        by_user = {}
        for row in rows:
            uid = row.get("user_id")
            tid = row.get("tenant_id")
            if uid is None or not tid:
                continue
            by_user.setdefault(uid, []).append(tid)
        if not by_user:
            return

        live = warehouse_tenants_present(
            [t for tids in by_user.values() for t in tids]
        )
        if not live:
            return

        for uid, tids in by_user.items():
            if not any(t in live for t in tids):
                continue
            u = User.get_by_id(uid)
            if not u or not (u.email or "").strip():
                continue
            if (u.username or "").lower() == DEMO_USERNAME:
                continue
            if not record_email_send(
                "data_ready", str(uid), user_id=uid, to_email=u.email
            ):
                continue
            send_data_ready_email(
                to=u.email,
                username=u.username,
                dashboard_url=f"{app_base_url()}/overview",
            )
            _log.info("data_ready emailed user_id=%s", uid)
    except Exception as exc:
        _log.warning("data_ready after rebuild failed: %s", exc)


def _warm_scopes():
    """(user_id, tenant_id list) pairs to warm: every user with linked
    tenants, plus one unscoped pass (admin view / shared queries).
    Filters are rendered per-query inside ``_warm_one_scope`` because
    different queries need different column prefixes (e.g. the trader
    story's ``h.tenant_id``)."""
    from app.db import fetch_all
    from app.models import get_broker_tenants_for_user

    scopes = []
    try:
        rows = fetch_all(
            "SELECT DISTINCT user_id FROM broker_tenants "
            "WHERE user_id IS NOT NULL"
        )
    except Exception as exc:
        _log.warning("cache warm: could not list users: %s", exc)
        rows = []
    for row in rows:
        uid = row.get("user_id")
        tenants = get_broker_tenants_for_user(uid) or []
        ids = [t["tenant_id"] for t in tenants if t.get("tenant_id")]
        if ids:
            scopes.append((uid, ids))
    # Admin sees the unscoped variant (tenant_ids=None -> "" filter).
    scopes.append((None, None))
    return scopes


_PER_TENANT_WARM_CAP = 6
_PD_WARM_SYMBOLS = 8


def _stamp_pages_warm(uid, tenant_ids, symbols=()):
    from app.query_cache import mark_page_warm
    if uid is None:
        return
    extra = ""
    if tenant_ids is not None and len(tenant_ids) == 1:
        extra = tenant_ids[0]
    for page in ("weekly_review", "today_view", "positions", "accounts",
                 "trader_story"):
        mark_page_warm(uid, page, extra)
    if not extra:
        for sym in symbols:
            mark_page_warm(uid, "position_detail", str(sym).strip())


def _warm_one_scope(client, uid, tenant_ids, *, heavy=True):
    """Run the hot query sets for one tenant scope through the cache."""
    from app.models import get_user_profile
    from app.query_cache import cached_query_df
    from app.tenant_scope import filter_df_by_tenant_ids, tenant_sql_and
    from app.weekly_review import (
        _bq_parallel,
        _date_in_user_tz,
        _iso_week_start,
        _snapshot_as_of_date,
        _us_market_session,
        build_daily_review_batch,
        build_today_batch,
    )
    from app.positions_page import DEFAULT_QUERY as POSITIONS_DEFAULT_QUERY
    from app.trader_story import story_query_batch
    from app.accounts_page import (
        _apply_dividend_strategy_labels,
        _primary_strategy_map,
        accounts_chart_payload,
        accounts_query_batch,
    )
    from app.position_detail import position_detail_query_batch

    tenant_filter = tenant_sql_and(tenant_ids)

    tz = "America/New_York"
    if uid is not None:
        prof = get_user_profile(uid) or {}
        tz = (prof.get("timezone") or tz).strip() or tz
    today = _date_in_user_tz(tz)
    this_week = _iso_week_start(today)
    market_session = _us_market_session()
    session_date = _snapshot_as_of_date(today, market_session)

    # Overview (close-based landing page) — full batch so /overview/below hits too.
    batch = build_daily_review_batch(
        tenant_filter, today, this_week,
        trades_as_of=session_date, moves_as_of=session_date)
    overview_dfs = _bq_parallel(client, batch)

    # Live /today page (calendar-today last-trade bars).
    _bq_parallel(client, build_today_batch(tenant_filter, today))

    # Positions list default (all-time) query.
    cached_query_df(
        client,
        POSITIONS_DEFAULT_QUERY.format(tenant_filter=tenant_filter),
        label="warm_positions",
    )

    # Accounts performance + the Python chart payload (the 4s walk).
    acct_dfs = _bq_parallel(
        client, accounts_query_batch(tenant_filter, include_trades=False))
    try:
        current_df = filter_df_by_tenant_ids(
            acct_dfs.get("current"), tenant_ids)
        strat_summary_df = filter_df_by_tenant_ids(
            acct_dfs.get("strat_summary"), tenant_ids)
        strat_class_df = filter_df_by_tenant_ids(
            acct_dfs.get("strat_class"), tenant_ids)
        strat_class_df = _apply_dividend_strategy_labels(
            strat_class_df, strat_summary_df)
        strategy_map = _primary_strategy_map(strat_summary_df, strat_class_df)
        accounts_chart_payload(client, tenant_ids, current_df, strategy_map)
    except Exception as exc:
        _log.warning("cache warm: accounts chart user=%r failed: %s", uid, exc)

    symbols = []
    pos = overview_dfs.get("positions") if isinstance(overview_dfs, dict) else None
    if pos is not None and not getattr(pos, "empty", True) and "symbol" in pos.columns:
        symbols = (
            pos["symbol"].dropna().astype(str).str.upper().str.strip()
            .loc[lambda s: s != ""]
            .unique()
            .tolist()
        )
        symbols = symbols[:_PD_WARM_SYMBOLS]

    if heavy:
        _bq_parallel(client, story_query_batch(tenant_ids))
        owned = tenant_ids
        for sym in symbols:
            try:
                _bq_parallel(
                    client,
                    position_detail_query_batch(
                        str(sym).replace("'", "''"), tenant_ids, owned),
                )
            except Exception as exc:
                _log.warning(
                    "cache warm: position %s user=%r failed: %s", sym, uid, exc)

    _stamp_pages_warm(uid, tenant_ids, symbols if heavy else ())


def _warm_worker():
    """Background warm pass. Never raises; logs a one-line summary."""
    from app.bigquery_client import get_bigquery_client

    t0 = time.perf_counter()
    ok = failed = 0
    try:
        client = get_bigquery_client()
        for uid, tenant_ids in _warm_scopes():
            try:
                _warm_one_scope(client, uid, tenant_ids, heavy=True)
                ok += 1
                ids = list(tenant_ids or [])
                if uid is not None and 1 < len(ids) <= _PER_TENANT_WARM_CAP:
                    for tid in ids:
                        try:
                            _warm_one_scope(
                                client, uid, [tid], heavy=False)
                            ok += 1
                        except Exception as exc:
                            failed += 1
                            _log.warning(
                                "cache warm: tenant %s user=%r failed: %s",
                                tid, uid, exc)
            except Exception as exc:
                failed += 1
                _log.warning("cache warm: scope user=%r failed: %s", uid, exc)
    except Exception as exc:
        _log.warning("cache warm: aborted: %s", exc)
    finally:
        _warm_lock.release()
    _log.info(
        "CACHE_WARM done scopes_ok=%d scopes_failed=%d elapsed_ms=%.0f",
        ok, failed, (time.perf_counter() - t0) * 1000.0,
    )


@app.route("/internal/cache/flush", methods=["POST"])
@csrf.exempt
@limiter.limit("30 per hour")
def internal_cache_flush():
    """Flush the query cache (and warm it) after a warehouse rebuild.

    Auth: constant-time compare of ``X-Cache-Flush-Token`` against the
    ``CACHE_FLUSH_TOKEN`` env var. Fails closed when the env var is unset.
    ``?warm=0`` skips the warm pass (flush only).
    ``?ready=1`` (warehouse rebuild only) emails users whose first
    Overview is now queryable.
    """
    expected = (os.environ.get("CACHE_FLUSH_TOKEN") or "").strip()
    provided = (request.headers.get("X-Cache-Flush-Token") or "").strip()
    if not expected or not hmac.compare_digest(provided, expected):
        abort(403)

    query_cache.clear()

    # Seed write is not yet queryable. bigquery_update.yml passes
    # ``?ready=1`` after a warehouse rebuild so Overview actually has
    # rows. prices_refresh.yml flushes without that flag — a close snap
    # must not tell a user whose first rebuild is still queued that
    # their data is ready.
    if request.args.get("ready") == "1":
        threading.Thread(
            target=_send_data_ready_after_rebuild,
            name="data-ready-email",
            daemon=True,
        ).start()

    warming = False
    if request.args.get("warm", "1") != "0":
        # Non-blocking: if a warm pass is already running let it finish
        # (it started from a just-flushed cache at most a rebuild ago).
        if _warm_lock.acquire(blocking=False):
            threading.Thread(
                target=_warm_worker, name="cache-warm", daemon=True
            ).start()
            warming = True
    _log.info("CACHE_FLUSH ok warming=%s", warming)
    return jsonify({"flushed": True, "warming": warming})
