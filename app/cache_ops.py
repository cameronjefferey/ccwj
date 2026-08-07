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
import threading
import time

from flask import abort, jsonify, request

from app import app
from app.extensions import csrf, limiter
from app import query_cache

_log = logging.getLogger(__name__)

# Only one warm pass at a time; a second flush while warming just flushes.
_warm_lock = threading.Lock()


def _warm_scopes():
    """(user_id, tenant_filter) pairs to warm: every user with linked
    tenants, plus one unscoped pass (admin view / shared queries)."""
    from app.db import fetch_all
    from app.models import get_broker_tenants_for_user
    from app.tenant_scope import tenant_sql_and

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
            scopes.append((uid, tenant_sql_and(ids)))
    # Admin sees the unscoped variant (tenant_ids=None -> "" filter).
    scopes.append((None, ""))
    return scopes


def _warm_one_scope(client, uid, tenant_filter):
    """Run the hot query sets for one tenant scope through the cache."""
    from app.models import get_user_profile
    from app.query_cache import cached_query_df
    from app.weekly_review import (
        _bq_parallel,
        _date_in_user_tz,
        _iso_week_start,
        build_daily_review_batch,
    )
    from app.positions_page import DEFAULT_QUERY as POSITIONS_DEFAULT_QUERY

    tz = "America/New_York"
    if uid is not None:
        prof = get_user_profile(uid) or {}
        tz = (prof.get("timezone") or tz).strip() or tz
    today = _date_in_user_tz(tz)
    this_week = _iso_week_start(today)

    # Daily Review core batch (the primary landing page, 11 queries).
    batch = build_daily_review_batch(tenant_filter, today, this_week)
    _bq_parallel(client, batch)

    # Positions list default (all-time) query.
    cached_query_df(
        client,
        POSITIONS_DEFAULT_QUERY.format(tenant_filter=tenant_filter),
        label="warm_positions",
    )


def _warm_worker():
    """Background warm pass. Never raises; logs a one-line summary."""
    from app.bigquery_client import get_bigquery_client

    t0 = time.perf_counter()
    ok = failed = 0
    try:
        client = get_bigquery_client()
        for uid, tenant_filter in _warm_scopes():
            try:
                _warm_one_scope(client, uid, tenant_filter)
                ok += 1
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
    """
    expected = (os.environ.get("CACHE_FLUSH_TOKEN") or "").strip()
    provided = (request.headers.get("X-Cache-Flush-Token") or "").strip()
    if not expected or not hmac.compare_digest(provided, expected):
        abort(403)

    query_cache.clear()

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
