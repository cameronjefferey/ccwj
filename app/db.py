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
from contextlib import contextmanager
from typing import Any, Iterable, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


_pool: Optional[ConnectionPool] = None
_pool_lock = threading.Lock()


def _pool_check_connection(conn):
    """Verify the connection is alive before use (drops stale TLS sessions)."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1")


def _transient_conn_error(exc: BaseException) -> bool:
    """True for flaky network / SSL issues where a retry often succeeds."""
    if not isinstance(exc, psycopg.OperationalError):
        return False
    msg = str(exc).lower()
    needles = (
        "ssl",
        "tls",
        "bad record",
        "consuming input failed",
        "connection reset",
        "server closed the connection",
        "could not receive data",
        "eof detected",
        "broken pipe",
    )
    return any(n in msg for n in needles)


def _run_db_twice(fn):
    """Run fn(); on transient OperationalError retry once with a new connection."""
    last: Optional[BaseException] = None
    for attempt in range(2):
        try:
            return fn()
        except psycopg.OperationalError as e:
            last = e
            if attempt == 0 and _transient_conn_error(e):
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
    # Managed Postgres (e.g. Render) often closes idle TLS sessions; shorter
    # max_idle / max_lifetime plus check= pre-ping reduces ssl/tls alert errors.
    max_idle = float(os.environ.get("DATABASE_POOL_MAX_IDLE", "300"))
    max_lifetime = float(os.environ.get("DATABASE_POOL_MAX_LIFETIME", "1800"))
    return ConnectionPool(
        conninfo=_normalize_url(url),
        min_size=1,
        max_size=int(os.environ.get("DATABASE_POOL_MAX", "10")),
        kwargs={"row_factory": dict_row},
        check=_pool_check_connection,
        max_idle=max_idle,
        max_lifetime=max_lifetime,
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
