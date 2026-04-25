"""
Postgres connection helpers.

The application uses a single global ``psycopg_pool.ConnectionPool`` so each
HTTP request can grab a connection cheaply. Connections are configured to
return rows as ``dict`` (via ``psycopg.rows.dict_row``) so callers can use
column-name access — matching the previous ``sqlite3.Row`` ergonomics.

Usage:

    from app.db import get_conn, fetch_all, fetch_one, execute

    rows = fetch_all("SELECT id FROM users WHERE username = %s", (name,))
    row  = fetch_one("SELECT * FROM users WHERE id = %s", (uid,))
    execute("UPDATE users SET password_hash = %s WHERE id = %s", (h, uid))

The pool is opened lazily on first use so importing this module is cheap and
won't fail at import time if ``DATABASE_URL`` isn't set yet (e.g. during
``flask --help`` or test collection).
"""
from __future__ import annotations

import atexit
import os
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterable, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout


_pool: Optional[ConnectionPool] = None
_pool_lock = threading.Lock()


def _pool_check_connection(conn):
    """Verify the connection is alive before use (drops stale TLS sessions)."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1")


def _run_db_twice(fn):
    """Run fn(); retry once on pool timeout, network blips, or dead pooled connections.

    After a laptop sleep or long idle, the first request often uses a connection
    Postgres already closed; a single retry on *any* OperationalError usually fixes it.
    """
    last: Optional[BaseException] = None
    for attempt in range(2):
        try:
            return fn()
        except PoolTimeout as e:
            last = e
            if attempt == 0:
                time.sleep(0.2)
                continue
            raise
        except psycopg.OperationalError as e:
            last = e
            if attempt == 0:
                time.sleep(0.2)
                continue
            raise
        except psycopg.InterfaceError as e:
            # e.g. connection already closed — common right after idle / pool handoff
            last = e
            if attempt == 0:
                time.sleep(0.2)
                continue
            raise
    assert last is not None
    raise last


def _close_pool_at_exit() -> None:
    global _pool
    if _pool is not None:
        try:
            _pool.close()
        except Exception:
            pass
        _pool = None


atexit.register(_close_pool_at_exit)


def _normalize_url(url: str) -> str:
    """Render and Heroku still hand out ``postgres://`` URLs which newer
    libraries reject. Normalize to ``postgresql://``."""
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://"):]
    return url


def _build_pool() -> ConnectionPool:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Add it to .env, e.g.\n"
            "  DATABASE_URL=postgresql://user:pass@localhost:5432/happytrader"
        )
    # Managed Postgres (e.g. Render) silently kills idle TCP sessions,
    # often well under 5 minutes. Two coordinated defenses:
    #   1. min_size=0 — don't try to keep idle conns "warm". After a long
    #      idle period, "warm" conns are actually dead at the network layer
    #      and the pool's pre-ping just reports them all bad at once,
    #      timing the request out instead of opening a fresh one.
    #   2. TCP keepalives — heartbeats on whatever idle conns *do* exist,
    #      so the OS notices a dead route long before psycopg does.
    # Plus shorter max_idle so anything we do keep is recycled before
    # Render's load balancer kills it.
    max_idle = float(os.environ.get("DATABASE_POOL_MAX_IDLE", "120"))
    max_lifetime = float(os.environ.get("DATABASE_POOL_MAX_LIFETIME", "1800"))
    # pool timeout: how long a request waits for a free connection.
    # Keep this WELL below gunicorn's --timeout (120s) so a stuck pool
    # surfaces as a fast 500 instead of hanging the worker until gunicorn
    # SIGKILLs it. 10s is plenty for normal contention; bump via env if
    # you see legitimate burst PoolTimeouts.
    pool_wait = float(os.environ.get("DATABASE_POOL_TIMEOUT", "10"))
    return ConnectionPool(
        conninfo=_normalize_url(url),
        # Don't pre-warm idle connections. For a low-traffic app, the cost
        # of a fresh connect on cold cache is tiny vs. a request hanging
        # waiting for the pool to discover all its "warm" conns are dead.
        min_size=int(os.environ.get("DATABASE_POOL_MIN", "0")),
        max_size=int(os.environ.get("DATABASE_POOL_MAX", "12")),
        kwargs={
            "row_factory": dict_row,
            # TCP-level keepalives so idle conns don't get silently dropped
            # by Render's network. After 60s of idle, send a probe every 10s
            # up to 5 times before declaring the conn dead.
            "keepalives": 1,
            "keepalives_idle": 60,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        },
        check=_pool_check_connection,
        max_idle=max_idle,
        max_lifetime=max_lifetime,
        timeout=pool_wait,
        open=True,
    )


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = _build_pool()
    return _pool


@contextmanager
def get_conn():
    """Yield a pooled connection. The pool's context manager commits on
    success and rolls back on exception."""
    pool = get_pool()
    with pool.connection() as conn:
        yield conn


def fetch_all(sql: str, params: Iterable[Any] = ()) -> list[dict]:
    def _go():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return cur.fetchall()

    return _run_db_twice(_go)


def fetch_one(sql: str, params: Iterable[Any] = ()) -> Optional[dict]:
    def _go():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return cur.fetchone()

    return _run_db_twice(_go)


def execute(sql: str, params: Iterable[Any] = ()) -> None:
    def _go():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))

    _run_db_twice(_go)


def execute_returning(sql: str, params: Iterable[Any] = ()) -> Optional[dict]:
    """Run an INSERT/UPDATE/DELETE ... RETURNING and return the first row."""

    def _go():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return cur.fetchone()

    return _run_db_twice(_go)
