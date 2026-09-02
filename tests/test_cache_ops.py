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


def test_flush_without_ready_does_not_email(client, monkeypatch):
    import app.cache_ops as cache_ops

    monkeypatch.setenv("CACHE_FLUSH_TOKEN", "correct-token")
    monkeypatch.setattr(cache_ops.query_cache, "clear", lambda: None)
    started = []

    class _FakeThread:
        def __init__(self, target=None, name=None, daemon=None):
            started.append(target)

        def start(self):
            if cache_ops._warm_lock.locked():
                cache_ops._warm_lock.release()

    monkeypatch.setattr(cache_ops.threading, "Thread", _FakeThread)
    resp = client.post(
        "/internal/cache/flush?warm=0",
        headers={"X-Cache-Flush-Token": "correct-token"},
    )
    assert resp.status_code == 200
    assert cache_ops._send_data_ready_after_rebuild not in started


def test_flush_ready_starts_data_ready_email(client, monkeypatch):
    import app.cache_ops as cache_ops

    monkeypatch.setenv("CACHE_FLUSH_TOKEN", "correct-token")
    monkeypatch.setattr(cache_ops.query_cache, "clear", lambda: None)
    started = []

    class _FakeThread:
        def __init__(self, target=None, name=None, daemon=None):
            started.append(target)

        def start(self):
            if cache_ops._warm_lock.locked():
                cache_ops._warm_lock.release()

    monkeypatch.setattr(cache_ops.threading, "Thread", _FakeThread)
    resp = client.post(
        "/internal/cache/flush?ready=1&warm=0",
        headers={"X-Cache-Flush-Token": "correct-token"},
    )
    assert resp.status_code == 200
    assert started == [cache_ops._send_data_ready_after_rebuild]


def test_send_data_ready_emails_users_with_tenants(monkeypatch):
    import app.cache_ops as cache_ops
    import app.db as db_mod

    class _U:
        email = "a@example.com"
        username = "ada"

    sent = []
    monkeypatch.setattr(db_mod, "fetch_all",
                        lambda sql: [{"user_id": 9}])
    monkeypatch.setattr("app.models.User.get_by_id", lambda uid: _U())
    monkeypatch.setattr(
        "app.models.record_email_send",
        lambda *a, **k: True,
    )
    monkeypatch.setattr(
        "app.email.send_data_ready_email",
        lambda **k: sent.append(k),
    )
    monkeypatch.setattr("app.email.app_base_url", lambda: "https://ht.test")
    cache_ops._send_data_ready_after_rebuild()
    assert len(sent) == 1
    assert sent[0]["to"] == "a@example.com"
    assert sent[0]["dashboard_url"] == "https://ht.test/overview"


def test_accounts_query_batch_skips_full_history_by_default():
    from app.accounts_page import accounts_query_batch
    batch = accounts_query_batch("AND tenant_id IN ('snaptrade:abc')")
    assert "trades" not in batch
    assert "balances" in batch
    assert "attribution" in batch
    with_trades = accounts_query_batch(
        "AND tenant_id IN ('snaptrade:abc')", include_trades=True)
    assert "trades" in with_trades


def test_position_detail_batch_includes_trades_and_chart():
    from app.position_detail import position_detail_query_batch
    batch = position_detail_query_batch(
        "JPM", ["snaptrade:abc"], ["snaptrade:abc"])
    assert "trades" in batch
    assert "chart" in batch
    assert "int_drip_fills" in batch["trades"]
    assert "UPPER(TRIM('{symbol}'))" in batch["trades"] or "JPM" in batch["trades"]
