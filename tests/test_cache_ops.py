"""Tests for the rebuild-triggered cache flush endpoint (app/cache_ops.py).

The endpoint is the freshness linchpin of the long-TTL query cache: the
GitHub workflows call it when the warehouse actually changes. It must
fail closed (no token / wrong token / unset env -> 403) and flush + kick
the warm thread on a valid token.
"""

import pytest

from app import app as flask_app


@pytest.fixture
def client(monkeypatch):
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as c:
        yield c


def test_flush_403_when_env_unset(client, monkeypatch):
    monkeypatch.delenv("CACHE_FLUSH_TOKEN", raising=False)
    resp = client.post(
        "/internal/cache/flush",
        headers={"X-Cache-Flush-Token": "anything"},
    )
    assert resp.status_code == 403


def test_flush_403_on_wrong_token(client, monkeypatch):
    monkeypatch.setenv("CACHE_FLUSH_TOKEN", "correct-token")
    resp = client.post(
        "/internal/cache/flush",
        headers={"X-Cache-Flush-Token": "wrong-token"},
    )
    assert resp.status_code == 403


def test_flush_403_without_header(client, monkeypatch):
    monkeypatch.setenv("CACHE_FLUSH_TOKEN", "correct-token")
    resp = client.post("/internal/cache/flush")
    assert resp.status_code == 403


def test_flush_ok_clears_and_warms(client, monkeypatch):
    import app.cache_ops as cache_ops

    monkeypatch.setenv("CACHE_FLUSH_TOKEN", "correct-token")

    cleared = {"n": 0}
    monkeypatch.setattr(cache_ops.query_cache, "clear", lambda: cleared.__setitem__("n", cleared["n"] + 1))

    started = {}

    class _FakeThread:
        def __init__(self, target=None, name=None, daemon=None):
            started["target"] = target

        def start(self):
            started["started"] = True
            # Don't actually run the warm pass in tests; release the lock
            # the endpoint acquired so later tests aren't wedged.
            cache_ops._warm_lock.release()

    monkeypatch.setattr(cache_ops.threading, "Thread", _FakeThread)

    resp = client.post(
        "/internal/cache/flush",
        headers={"X-Cache-Flush-Token": "correct-token"},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["flushed"] is True
    assert body["warming"] is True
    assert cleared["n"] == 1
    assert started.get("started") is True
    assert started.get("target") is cache_ops._warm_worker


def test_flush_warm_skippable(client, monkeypatch):
    import app.cache_ops as cache_ops

    monkeypatch.setenv("CACHE_FLUSH_TOKEN", "correct-token")
    monkeypatch.setattr(cache_ops.query_cache, "clear", lambda: None)

    resp = client.post(
        "/internal/cache/flush?warm=0",
        headers={"X-Cache-Flush-Token": "correct-token"},
    )
    assert resp.status_code == 200
    assert resp.get_json()["warming"] is False
    # Lock must not be held after a warm-skipped flush.
    assert cache_ops._warm_lock.acquire(blocking=False)
    cache_ops._warm_lock.release()
