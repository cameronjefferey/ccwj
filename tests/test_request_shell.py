"""Signed-in shell: one Postgres connection, and a short per-user cache."""
import time
from datetime import date

from psycopg.pq import TransactionStatus


def test_shell_cache_reuses_a_loader_until_invalidate(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    from app import shell_cache

    shell_cache.clear()
    calls = []

    def loader():
        calls.append(1)
        return {"n": len(calls)}

    assert shell_cache.load(9, "plan_row", loader)["n"] == 1
    assert shell_cache.load(9, "plan_row", loader)["n"] == 1
    assert calls == [1]
    shell_cache.invalidate(9)
    assert shell_cache.load(9, "plan_row", loader)["n"] == 2
    shell_cache.clear()


def test_one_postgres_connection_serves_the_request(monkeypatch):
    from app import app
    from app.db import bind_request_thread, close_request_connection, fetch_one, request_connect_count

    opens = []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            return None

        def fetchone(self):
            return {"ok": 1}

    class _Conn:
        closed = False

        def __init__(self):
            self.info = type("Info", (), {"transaction_status": TransactionStatus.IDLE})()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return _Cursor()

        def close(self):
            self.closed = True

    def _connect():
        opens.append(1)
        return _Conn()

    monkeypatch.setattr("app.db._connect", _connect)
    with app.test_request_context("/faq"):
        bind_request_thread()
        assert fetch_one("SELECT 1")["ok"] == 1
        assert fetch_one("SELECT 1")["ok"] == 1
        assert request_connect_count() == 1
        assert len(opens) == 1
        close_request_connection()


def test_option_chain_stops_waiting_after_the_timeout(monkeypatch):
    from app import paper_practice as practice

    monkeypatch.setattr(practice, "_CHAIN_LOAD_TIMEOUT", 0.05)

    def _hang(*args, **kwargs):
        time.sleep(0.4)
        return []

    monkeypatch.setattr(practice, "_read_option_chain", _hang)
    started = time.monotonic()
    try:
        practice.load_option_chain("SPY", date(2026, 10, 16), [500])
        raised = False
    except TimeoutError:
        raised = True
    assert raised
    assert time.monotonic() - started < 0.3
