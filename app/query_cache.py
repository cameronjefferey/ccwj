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
import contextlib
import contextvars
import copy
import hashlib
import logging
import os
import pickle
import sys
import threading
import time

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


# ----------------------------------------------------------------------
# Optional shared L2 cache (Redis / Render Key Value)
# ----------------------------------------------------------------------
# The in-process L1 above is PER-WORKER: Gunicorn runs 2 workers, recycles
# them on ``--max-requests``, and the TTL is short — so a real user whose
# visits are spread out (and load-balanced across workers) misses L1 on
# essentially every page, forcing a fresh 2-4s BigQuery round trip. A shared
# L2 lets any worker reuse any other worker's (and prior request's) result.
# Reporting is CLOSE-BASED, so a shared short-TTL cache is safe — same
# tenant-isolation guarantee as L1 (keys derive from the tenant-scoped SQL /
# tenant-scoped frame fingerprint; see module docstring).
#
# ROBUSTNESS CONTRACT: Redis is a SPEEDUP, never a hard dependency. Every op
# is wrapped so a missing / slow / broken instance silently degrades to L1
# (and then to a live BQ query). We NEVER raise from a cache path.
_REDIS_URL = os.environ.get("QUERY_CACHE_REDIS_URL", "").strip()
_REDIS_TTL = _env_int("QUERY_CACHE_REDIS_TTL_SECONDS", _TTL_SECONDS)
# Don't round-trip values too big to be worth (de)serialization + network,
# or that would thrash a small shared store.
_REDIS_MAX_BYTES = _env_int("QUERY_CACHE_REDIS_MAX_BYTES", 8 * 1024 * 1024)
# Namespace so a pickle-format / schema change can be invalidated wholesale
# by bumping the version; dev/prod use different URLs so they never collide.
_REDIS_PREFIX = "qc:v1:"

_redis_client = None
_redis_init_done = False
_redis_lock = threading.Lock()


def _get_redis():
    """Lazily build the shared cache client; return None if unavailable.

    Never raises. On the first failure it logs once and disables further
    attempts for the life of the process, so a wrong/unreachable URL costs
    one connect timeout total — not one per request.
    """
    global _redis_client, _redis_init_done
    if _redis_init_done:
        return _redis_client
    with _redis_lock:
        if _redis_init_done:
            return _redis_client
        _redis_init_done = True
        if not _REDIS_URL:
            _redis_client = None
            return None
        try:
            import redis  # optional dependency; only needed in prod
            client = redis.Redis.from_url(
                _REDIS_URL,
                socket_connect_timeout=1.0,
                socket_timeout=1.0,
                retry_on_timeout=False,
                health_check_interval=30,
            )
            client.ping()
            _redis_client = client
            _log.info("query_cache: shared L2 cache connected")
        except Exception as exc:
            _log.warning(
                "query_cache: shared L2 cache unavailable (%s); "
                "using in-process cache only", exc,
            )
            _redis_client = None
    return _redis_client


def _redis_key(key) -> str:
    """Stringify an L1 key for Redis. ``make_key`` yields a sha256 hex str;
    ``cached_payload`` passes a tuple. Both hash to a bounded namespaced key."""
    if isinstance(key, str):
        raw = key
    elif isinstance(key, tuple):
        raw = "\x00".join(str(p) for p in key)
    else:
        raw = str(key)
    return _REDIS_PREFIX + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _running_under_pytest() -> bool:
    return "pytest" in sys.modules or bool(os.environ.get("PYTEST_CURRENT_TEST"))


class _ReqStats:
    """Per-request profiling accumulator (thread-safe).

    ``_bq_parallel`` runs queries on ``ThreadPoolExecutor`` worker threads,
    so ``flask.g`` (thread-local to the request thread) is invisible there
    and undercounts BQ activity. We accumulate here instead and propagate
    the object into the worker threads via ``contextvars`` (see
    ``propagate_context`` / ``_bq_parallel``). All mutation is under a lock
    because several query threads report concurrently.
    """

    __slots__ = ("lock", "query_hits", "query_miss", "payload_hits",
                 "payload_miss", "bq_ms", "queries", "steps")

    def __init__(self):
        self.lock = threading.Lock()
        self.query_hits = 0
        self.query_miss = 0
        self.payload_hits = 0
        self.payload_miss = 0
        self.bq_ms = 0.0          # summed wall-clock of MISS executions
        self.queries = []          # (label, ms, hit) per cached_query_df call
        self.steps = {}            # named step -> summed ms (chart/matrix/...)

    def add_query(self, label, ms, hit):
        with self.lock:
            if hit:
                self.query_hits += 1
            else:
                self.query_miss += 1
                self.bq_ms += ms
            self.queries.append((label or "?", ms, hit))

    def add_payload(self, hit):
        with self.lock:
            if hit:
                self.payload_hits += 1
            else:
                self.payload_miss += 1

    def add_step(self, name, ms):
        with self.lock:
            self.steps[name] = self.steps.get(name, 0.0) + ms


# Per-request stats live in a ContextVar (not flask.g) so they survive the
# hop onto _bq_parallel worker threads when the request's context is copied.
_req_stats: contextvars.ContextVar = contextvars.ContextVar(
    "qc_req_stats", default=None
)


def start_request_stats() -> "_ReqStats":
    stats = _ReqStats()
    _req_stats.set(stats)
    return stats


def get_request_stats():
    return _req_stats.get()


def propagate_context():
    """Snapshot the current context so a worker thread can adopt it.

    Call in the REQUEST thread; run the returned context's ``.run`` in the
    worker so the ``_req_stats`` ContextVar (and thus per-query timing)
    reaches ``cached_query_df`` inside the pool. Each caller gets its own
    copy — the same context object cannot be entered concurrently.
    """
    return contextvars.copy_context()


@contextlib.contextmanager
def timed(step: str):
    """Accumulate wall-clock into a named step (``chart``/``matrix``/...).

    No-op when there is no active request-stats object (CLI, tests).
    """
    t0 = time.perf_counter()
    try:
        yield
    finally:
        stats = _req_stats.get()
        if stats is not None:
            stats.add_step(step, (time.perf_counter() - t0) * 1000.0)


def format_stats(stats) -> str:
    """Compact one-line summary for the REQUEST_TIMING log."""
    if stats is None:
        return "bq_ms=0 nq=0 qhit=0 qmiss=0 chit=0 cmiss=0"
    misses = [(lbl, ms) for (lbl, ms, hit) in stats.queries if not hit]
    slow = max(misses, key=lambda x: x[1], default=None)
    slow_str = f" slow={slow[0]}:{slow[1]:.0f}" if slow else ""
    steps = sorted(stats.steps.items(), key=lambda x: x[1], reverse=True)
    steps_str = ""
    if steps:
        steps_str = " steps=" + ",".join(f"{n}:{ms:.0f}" for n, ms in steps)
    nq = stats.query_hits + stats.query_miss
    return (
        f"bq_ms={stats.bq_ms:.0f} nq={nq} "
        f"qhit={stats.query_hits} qmiss={stats.query_miss} "
        f"chit={stats.payload_hits} cmiss={stats.payload_miss}"
        f"{slow_str}{steps_str}"
    )


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
    # L1 (per-worker, fast) first.
    with _lock:
        val = _cache.get(key)
    if val is not None:
        return val
    # L1 miss → shared L2 (cross-worker). Never hold ``_lock`` across the
    # network call. Any failure → treat as a miss.
    client = _get_redis()
    if client is None:
        return None
    try:
        blob = client.get(_redis_key(key))
    except Exception:
        return None
    if blob is None:
        return None
    try:
        val = pickle.loads(blob)
    except Exception:
        return None
    # Promote into L1 so subsequent same-worker reads skip the round trip.
    with _lock:
        _cache[key] = val
    return val


def set(key, value):  # noqa: A001 - deliberate cache-style API
    with _lock:
        _cache[key] = value
    client = _get_redis()
    if client is None:
        return
    try:
        blob = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        return
    if len(blob) > _REDIS_MAX_BYTES:
        return
    try:
        client.setex(_redis_key(key), _REDIS_TTL, blob)
    except Exception:
        pass


def clear():
    with _lock:
        _cache.clear()
    # Best-effort L2 flush of our namespace (used by tests / admin). TTL
    # expiry covers prod; this just makes an explicit clear immediate.
    client = _get_redis()
    if client is None:
        return
    try:
        for k in client.scan_iter(match=_REDIS_PREFIX + "*", count=500):
            client.delete(k)
    except Exception:
        pass


def _execute(client, sql, job_config):
    """Run the query, passing ``job_config`` only when present.

    Non-parameterized reads keep the original ``client.query(sql)``
    signature (no ``job_config`` kwarg) so lightweight test stubs and any
    caller that never expected the kwarg keep working unchanged.
    """
    if job_config is None:
        return client.query(sql).to_dataframe()
    return client.query(sql, job_config=job_config).to_dataframe()


def cached_query_df(client, sql, job_config=None, label=None):
    """Run ``client.query(sql, job_config).to_dataframe()`` with caching.

    - Cache MISS (or disabled): execute against BigQuery, store the result,
      return a copy.
    - Cache HIT: return a copy of the stored DataFrame.

    A COPY is always returned because callers mutate the frame in place
    (numeric coercion, adding columns); handing back the cached object
    would poison every subsequent reader. Errors are never cached — they
    propagate to the caller, preserving ``_bq_parallel``'s per-query
    empty-DataFrame-on-error contract.

    ``label`` (e.g. the ``_bq_parallel`` query name) is recorded with the
    execution time so the REQUEST_TIMING log can name the slowest cold
    query.
    """
    if not cache_enabled():
        return _execute(client, sql, job_config)

    key = make_key(sql, job_config)
    hit = get(key)
    stats = _req_stats.get()
    if hit is not None:
        if stats is not None:
            stats.add_query(label, 0.0, True)
        return hit.copy()

    t0 = time.perf_counter()
    df = _execute(client, sql, job_config)
    exec_ms = (time.perf_counter() - t0) * 1000.0
    set(key, df)
    if stats is not None:
        stats.add_query(label, exec_ms, False)
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
    stats = _req_stats.get()
    if hit is not None:
        if stats is not None:
            stats.add_payload(True)
        return copy.deepcopy(hit)
    if stats is not None:
        stats.add_payload(False)
    value = producer()
    set(key, copy.deepcopy(value))
    return copy.deepcopy(value)
