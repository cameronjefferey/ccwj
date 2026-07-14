"""Tenant-safe in-process cache for BigQuery read results.

WHY THIS EXISTS
---------------
Every user-facing page runs 8-15 BigQuery queries synchronously in the
request handler, uncached. Because reporting is CLOSE-BASED (data only
really changes on the evening dbt build + webhook syncs), the same SQL
is re-executed on every page load and every filter toggle for no benefit.
This module memoizes the resulting DataFrames for a short TTL so warm
loads skip BigQuery entirely.

TENANT ISOLATION (non-negotiable — see
``.cursor/rules/bigquery-tenant-isolation.mdc``)
------------------------------------------------
The cache key is derived from the FULL SQL TEXT (after the dataset
override) plus the query PARAMETERS. Tenant scope is inlined into the SQL
by ``_tenant_sql_and`` (``AND tenant_id IN ('snaptrade:...', ...)``), so
two different tenants ALWAYS produce different SQL and therefore different
cache keys — a cache hit can never return another tenant's rows.

Crucially, this cache stores the RAW query result, i.e. BEFORE the
per-request ``_filter_df_by_tenant_ids`` call. Callers keep running that
Python filter on every request regardless of cache hit/miss, so it
remains the belt-and-suspenders backstop even if the key logic ever had a
bug. NEVER move caching to a layer that skips ``_filter_df_by_tenant_ids``.

BACKEND
-------
In-process ``cachetools.TTLCache`` guarded by a lock. This is per-worker
(Gunicorn runs 2 workers x 4 gthread threads) and is wiped on deploy /
``--max-requests`` recycle — acceptable for a short TTL. The public
surface (``get`` / ``set`` / ``make_key`` / ``clear``) is intentionally
backend-agnostic so a shared Redis backend can be dropped in later via an
env var, mirroring the rate limiter's ``RATELIMIT_STORAGE_URI`` pattern.
"""
import copy
import hashlib
import logging
import os
import sys
import threading

import pandas as pd
from cachetools import TTLCache

from app.bigquery_client import _apply_dataset_override

_log = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "").strip() or default)
    except (TypeError, ValueError):
        return default


# Default 10 min: reporting is close-based, so a few minutes of staleness
# is invisible to users (even a manual "Sync now" waits on a dbt build
# before data reaches BigQuery). Override with QUERY_CACHE_TTL_SECONDS.
_TTL_SECONDS = _env_int("QUERY_CACHE_TTL_SECONDS", 600)
_MAXSIZE = _env_int("QUERY_CACHE_MAXSIZE", 512)

_cache = TTLCache(maxsize=_MAXSIZE, ttl=_TTL_SECONDS)
_lock = threading.Lock()


def _running_under_pytest() -> bool:
    return "pytest" in sys.modules or bool(os.environ.get("PYTEST_CURRENT_TEST"))


def _bump(counter: str) -> None:
    """Increment a per-request cache counter on ``flask.g`` (best-effort).

    Lets the request-timing logger in ``app/__init__.py`` report cache
    hit/miss rates per page so we can SEE whether a slow load was cold
    (all misses) or warm. No-op outside a request context (CLI, tests).
    """
    try:
        from flask import g, has_request_context
        if has_request_context():
            setattr(g, counter, getattr(g, counter, 0) + 1)
    except Exception:
        pass


def cache_enabled() -> bool:
    """Cache on by default in prod; forced OFF under pytest so tests never
    see stale cross-test data (individual cache tests opt back in)."""
    raw = os.environ.get("QUERY_CACHE_ENABLED")
    if raw is not None:
        return raw.strip().lower() in ("1", "true", "yes", "on")
    return not _running_under_pytest()


def _serialize_params(job_config) -> str:
    """Deterministically serialize a QueryJobConfig's query parameters.

    Parameterized queries (dates, ``@strategy``, array params) share
    identical SQL text across users/dates, so the params MUST be part of
    the key or a cache hit would return the wrong slice.
    """
    if job_config is None:
        return ""
    params = getattr(job_config, "query_parameters", None) or []
    parts = []
    for p in params:
        name = getattr(p, "name", None)
        ptype = getattr(p, "type_", None) or getattr(p, "array_type", None)
        value = getattr(p, "value", None)
        if value is None:
            value = getattr(p, "values", None)
        parts.append(f"{name}:{ptype}:{value!r}")
    return "|".join(parts)


def make_key(sql: str, job_config=None) -> str:
    """Build the cache key from the effective SQL + serialized params.

    We apply the dataset override so a dev build (``BQ_DATASET=analytics_dev``)
    and prod never collide on the same key, and hash the whole thing to
    keep keys small and bounded.
    """
    effective_sql = _apply_dataset_override(sql or "")
    raw = f"{effective_sql}\x00{_serialize_params(job_config)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get(key):
    with _lock:
        return _cache.get(key)


def set(key, value):  # noqa: A001 - deliberate cache-style API
    with _lock:
        _cache[key] = value


def clear():
    with _lock:
        _cache.clear()


def _execute(client, sql, job_config):
    """Run the query, passing ``job_config`` only when present.

    Non-parameterized reads keep the original ``client.query(sql)``
    signature (no ``job_config`` kwarg) so lightweight test stubs and any
    caller that never expected the kwarg keep working unchanged.
    """
    if job_config is None:
        return client.query(sql).to_dataframe()
    return client.query(sql, job_config=job_config).to_dataframe()


def cached_query_df(client, sql, job_config=None):
    """Run ``client.query(sql, job_config).to_dataframe()`` with caching.

    - Cache MISS (or disabled): execute against BigQuery, store the result,
      return a copy.
    - Cache HIT: return a copy of the stored DataFrame.

    A COPY is always returned because callers mutate the frame in place
    (numeric coercion, adding columns); handing back the cached object
    would poison every subsequent reader. Errors are never cached — they
    propagate to the caller, preserving ``_bq_parallel``'s per-query
    empty-DataFrame-on-error contract.
    """
    if not cache_enabled():
        return _execute(client, sql, job_config)

    key = make_key(sql, job_config)
    hit = get(key)
    if hit is not None:
        _bump("_qc_query_hits")
        return hit.copy()

    _bump("_qc_query_misses")
    df = _execute(client, sql, job_config)
    set(key, df)
    return df.copy()


def frame_fingerprint(*frames) -> str:
    """Content fingerprint for one or more DataFrames.

    Used to key CACHED COMPUTED PAYLOADS (e.g. the position P&L chart) on
    the exact TENANT-SCOPED input data rather than on request params. This
    is the same tenant-isolation guarantee as ``cached_query_df``: identical
    inputs hash identically, and any difference in the (already tenant- and
    leg-filtered) rows -- including a different tenant's rows -- produces a
    different key, so a cache hit can never serve another tenant's chart.

    ``hash_pandas_object`` is a vectorized C hash: far cheaper than the
    row-by-row Python equity walk it lets us skip on a cache hit.
    """
    parts = []
    for df in frames:
        if df is None:
            parts.append("none")
            continue
        try:
            h = int(pd.util.hash_pandas_object(df, index=True).sum())
            parts.append(f"{df.shape}:{h}")
        except Exception:
            # Defensive: unhashable cell types -> fall back to a coarse
            # shape+columns signature. Never raise from a cache-key helper.
            parts.append(f"{getattr(df, 'shape', None)}:{tuple(getattr(df, 'columns', []))}")
    return "|".join(parts)


def cached_payload(key, producer):
    """Memoize a JSON-serializable computed payload (dict/list of scalars).

    ``producer`` is a zero-arg callable that returns the payload. A DEEP
    COPY is stored and returned so downstream mutation of the payload
    (e.g. chart rebasing / KPI alignment) never corrupts the cached copy.
    Disabled -> just calls ``producer()``.
    """
    if not cache_enabled():
        return producer()
    hit = get(key)
    if hit is not None:
        _bump("_qc_payload_hits")
        return copy.deepcopy(hit)
    _bump("_qc_payload_misses")
    value = producer()
    set(key, copy.deepcopy(value))
    return copy.deepcopy(value)
