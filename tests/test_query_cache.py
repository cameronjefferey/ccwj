"""Query cache — tenant-safety + correctness guards (app/query_cache.py).

The cache memoizes raw BigQuery DataFrame results for a short TTL. Because
tenant scope is inlined into the SQL (``AND tenant_id IN (...)``), the key
MUST distinguish different tenant filters and different query parameters,
or a cache hit could return the wrong tenant's / wrong slice's rows — a
security incident per .cursor/rules/bigquery-tenant-isolation.mdc.
"""

import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest
from google.cloud import bigquery

from app import query_cache
from app.query_cache import cached_query_df, cached_payload, frame_fingerprint, make_key


class _FakeJob:
    def __init__(self, df):
        self._df = df

    def to_dataframe(self):
        return self._df


class _FakeClient:
    """Records every query() call and returns a fresh DataFrame each time.

    A fresh frame per call lets a test detect a cache HIT (client not
    re-invoked) vs a MISS (new frame minted).
    """

    def __init__(self, df_factory=None):
        self.calls = []
        self._df_factory = df_factory or (lambda: pd.DataFrame({"v": [1, 2, 3]}))

    def query(self, sql, job_config=None, **kwargs):
        self.calls.append((sql, job_config))
        return _FakeJob(self._df_factory())


@pytest.fixture(autouse=True)
def _clean_cache():
    query_cache.clear()
    yield
    query_cache.clear()


@pytest.fixture
def cache_on(monkeypatch):
    # Cache is OFF under pytest by default; individual tests opt in.
    monkeypatch.setenv("QUERY_CACHE_ENABLED", "1")


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------

def test_make_key_differs_by_tenant_filter():
    base = "SELECT * FROM `ccwj-dbt.analytics.positions_summary` WHERE 1=1 {f}"
    a = base.format(f="AND tenant_id IN ('snaptrade:aaa')")
    b = base.format(f="AND tenant_id IN ('snaptrade:bbb')")
    assert make_key(a) != make_key(b)


def test_make_key_differs_by_query_params():
    sql = "SELECT * FROM t WHERE d BETWEEN @start_date AND @end_date"
    cfg1 = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", "2026-01-01"),
        bigquery.ScalarQueryParameter("end_date", "DATE", "2026-02-01"),
    ])
    cfg2 = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", "2026-03-01"),
        bigquery.ScalarQueryParameter("end_date", "DATE", "2026-04-01"),
    ])
    assert make_key(sql, cfg1) != make_key(sql, cfg2)
    # Identical SQL + identical params -> identical key.
    cfg1_again = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", "2026-01-01"),
        bigquery.ScalarQueryParameter("end_date", "DATE", "2026-02-01"),
    ])
    assert make_key(sql, cfg1) == make_key(sql, cfg1_again)


def test_make_key_differs_by_array_params():
    sql = "SELECT * FROM t WHERE symbol IN UNNEST(@symbols)"
    cfg1 = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("symbols", "STRING", ["JEPI", "XLU"]),
    ])
    cfg2 = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("symbols", "STRING", ["SPY", "QQQ"]),
    ])
    assert make_key(sql, cfg1) != make_key(sql, cfg2)


def test_make_key_folds_dataset_override(monkeypatch):
    """Dev and prod must never collide on the same key."""
    sql = "SELECT * FROM `ccwj-dbt.analytics.positions_summary`"
    monkeypatch.delenv("BQ_DATASET", raising=False)
    prod_key = make_key(sql)
    monkeypatch.setenv("BQ_DATASET", "analytics_dev")
    dev_key = make_key(sql)
    assert prod_key != dev_key


# ---------------------------------------------------------------------------
# Behaviour
# ---------------------------------------------------------------------------

def test_cache_hit_skips_second_query(cache_on):
    client = _FakeClient()
    sql = "SELECT 1 FROM t WHERE tenant_id IN ('snaptrade:aaa')"
    cached_query_df(client, sql)
    cached_query_df(client, sql)
    assert len(client.calls) == 1  # second call served from cache


def test_distinct_tenants_do_not_share_cache(cache_on):
    client = _FakeClient()
    sql_a = "SELECT 1 FROM t WHERE tenant_id IN ('snaptrade:aaa')"
    sql_b = "SELECT 1 FROM t WHERE tenant_id IN ('snaptrade:bbb')"
    cached_query_df(client, sql_a)
    cached_query_df(client, sql_b)
    assert len(client.calls) == 2  # different tenants -> separate BQ reads


def test_returned_frame_is_a_copy_no_poisoning(cache_on):
    client = _FakeClient(df_factory=lambda: pd.DataFrame({"v": [1, 2, 3]}))
    sql = "SELECT v FROM t WHERE tenant_id IN ('snaptrade:aaa')"

    first = cached_query_df(client, sql)
    first.loc[0, "v"] = 999          # caller mutates its copy
    first["extra"] = "poison"

    second = cached_query_df(client, sql)  # served from cache
    assert len(client.calls) == 1
    assert second.loc[0, "v"] == 1         # not poisoned by first caller
    assert "extra" not in second.columns


def test_cache_disabled_under_pytest_by_default(monkeypatch):
    monkeypatch.delenv("QUERY_CACHE_ENABLED", raising=False)
    assert query_cache.cache_enabled() is False
    client = _FakeClient()
    sql = "SELECT 1 FROM t WHERE tenant_id IN ('snaptrade:aaa')"
    cached_query_df(client, sql)
    cached_query_df(client, sql)
    assert len(client.calls) == 2  # no caching -> every call hits BQ


def test_errors_are_not_cached(cache_on):
    class _BoomClient:
        def __init__(self):
            self.calls = 0

        def query(self, sql, job_config=None, **kwargs):
            self.calls += 1
            raise RuntimeError("boom")

    client = _BoomClient()
    sql = "SELECT 1 FROM t"
    with pytest.raises(RuntimeError):
        cached_query_df(client, sql)
    with pytest.raises(RuntimeError):
        cached_query_df(client, sql)
    assert client.calls == 2  # error never memoized


# ---------------------------------------------------------------------------
# Computed-payload cache (chart builders)
# ---------------------------------------------------------------------------

def test_frame_fingerprint_differs_by_content():
    a = pd.DataFrame({"symbol": ["JEPI"], "pnl": [100.0]})
    b = pd.DataFrame({"symbol": ["XLU"], "pnl": [100.0]})
    assert frame_fingerprint(a) != frame_fingerprint(b)
    # Same content -> same fingerprint (stable across rebuilds).
    a2 = pd.DataFrame({"symbol": ["JEPI"], "pnl": [100.0]})
    assert frame_fingerprint(a) == frame_fingerprint(a2)


def test_frame_fingerprint_handles_none_and_multiple():
    df = pd.DataFrame({"x": [1]})
    assert frame_fingerprint(df, None) != frame_fingerprint(df, df)


def test_cached_payload_skips_producer_on_hit(cache_on):
    calls = {"n": 0}

    def producer():
        calls["n"] += 1
        return {"dates": ["2026-01-01"], "total": [1.0]}

    key = ("pos_chart", "2026-07-13", "fp123")
    first = cached_payload(key, producer)
    second = cached_payload(key, producer)
    assert calls["n"] == 1               # producer ran once
    assert first == second


def test_cached_payload_deepcopies_no_poisoning(cache_on):
    key = ("pos_chart", "2026-07-13", "fp456")
    first = cached_payload(key, lambda: {"total": [1.0, 2.0], "nested": {"a": 1}})
    first["total"].append(999)           # mutate returned payload
    first["nested"]["a"] = 42
    second = cached_payload(key, lambda: {"total": [1.0, 2.0], "nested": {"a": 1}})
    assert second["total"] == [1.0, 2.0]  # cached copy untouched
    assert second["nested"]["a"] == 1


def test_cached_payload_disabled_runs_producer_each_time(monkeypatch):
    monkeypatch.delenv("QUERY_CACHE_ENABLED", raising=False)
    calls = {"n": 0}

    def producer():
        calls["n"] += 1
        return {"v": 1}

    key = ("pos_chart", "2026-07-13", "fp789")
    cached_payload(key, producer)
    cached_payload(key, producer)
    assert calls["n"] == 2


# ---------------------------------------------------------------------------
# Per-request profiling stats (thread-aware)
# ---------------------------------------------------------------------------

def test_stats_count_hits_and_misses_and_bq_ms(cache_on):
    stats = query_cache.start_request_stats()
    client = _FakeClient()
    sql = "SELECT 1 FROM t WHERE tenant_id IN ('snaptrade:aaa')"
    cached_query_df(client, sql, label="q1")   # miss
    cached_query_df(client, sql, label="q1")   # hit
    assert stats.query_miss == 1
    assert stats.query_hits == 1
    assert stats.bq_ms >= 0.0
    labels = [lbl for (lbl, _ms, _hit) in stats.queries]
    assert labels == ["q1", "q1"]


def test_stats_are_thread_aware_via_context_propagation(cache_on):
    """The whole point of the ContextVar: cache activity on _bq_parallel
    worker THREADS must still land on the request's stats object."""
    stats = query_cache.start_request_stats()
    client = _FakeClient()

    def _work(i):
        sql = f"SELECT {i} FROM t WHERE tenant_id IN ('snaptrade:aaa')"
        cached_query_df(client, sql, label=f"q{i}")

    with ThreadPoolExecutor(max_workers=4) as pool:
        # Mirror _bq_parallel: copy the request context per task so the
        # worker thread sees the same _req_stats object.
        futures = [
            pool.submit(query_cache.propagate_context().run, _work, i)
            for i in range(4)
        ]
        for f in futures:
            f.result()

    # Without context propagation these misses would be invisible (the
    # blind spot we fixed): assert all four were counted.
    assert stats.query_miss == 4


def test_timed_records_named_step(cache_on):
    stats = query_cache.start_request_stats()
    with query_cache.timed("chart"):
        time.sleep(0.005)
    assert "chart" in stats.steps
    assert stats.steps["chart"] >= 4.0  # ~5ms, allow scheduling slack


def test_format_stats_names_slowest_query_and_steps(cache_on):
    stats = query_cache.start_request_stats()
    stats.add_query("fast", 5.0, False)
    stats.add_query("slow", 900.0, False)
    stats.add_query("cached", 0.0, True)
    stats.add_step("chart", 1234.0)
    out = query_cache.format_stats(stats)
    assert "slow=slow:900" in out
    assert "chart:1234" in out
    assert "qmiss=2" in out
    assert "qhit=1" in out


def test_stats_helpers_noop_without_active_request(cache_on):
    # Simulate "no active request stats" (CLI / background thread).
    query_cache._req_stats.set(None)
    assert query_cache.get_request_stats() is None
    with query_cache.timed("chart"):
        pass  # must not raise
    client = _FakeClient()
    # cached_query_df must still work when there's no stats object.
    df = cached_query_df(client, "SELECT 1 FROM t", label="x")
    assert not df.empty
