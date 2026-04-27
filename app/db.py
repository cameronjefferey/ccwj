"""
Postgres connection helpers.

Each call to :func:`get_conn` opens a fresh psycopg connection and closes it
when the context exits. We deliberately do **not** use a connection pool.

Why no pool?
    We previously used ``psycopg_pool.ConnectionPool``. Behind Render's
    network (which silently terminates idle TCP/TLS sessions), the pool's
    background "keep min_size connections alive" thread repeatedly fails to
    re-establish dead connections and ends up in a wedged state where
    ``pool.connection()`` blocks until ``timeout`` and raises ``PoolTimeout``,
    even though Postgres itself is healthy and has plenty of capacity. We
    confirmed this by watching ``pg_stat_activity`` while the app was 503'ing:
    zero connections from the web service, no orphans, no errors on the
    server side. The pool was just stuck.

    Per-request connections trade ~10-50ms of TLS handshake (web service and
    Postgres are in the same Render region) for the property that no
    persistent client-side state can ever wedge. Each request stands alone.
    A failed handshake on one request cannot break the next.

Usage:

    from app.db import get_conn, fetch_all, fetch_one, execute

    rows = fetch_all("SELECT id FROM users WHERE username = %s", (name,))
    row  = fetch_one("SELECT * FROM users WHERE id = %s", (uid,))
    execute("UPDATE users SET password_hash = %s WHERE id = %s", (h, uid))
"""
from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Any, Iterable, Optional

import psycopg
from psycopg.rows import dict_row


def _run_db_twice(fn):
    """Run *fn*; retry once on a transient connection error.

    With per-request connections the most common transient failure is a
    flaky TCP/TLS handshake or a dropped read mid-query. A single retry
    with a small backoff converts these from user-visible errors into
    ~200ms hiccups. Logic errors and bad SQL still surface immediately.
    """
    last: Optional[BaseException] = None
    for attempt in range(2):
        try:
            return fn()
        except psycopg.OperationalError as e:
            last = e
            if attempt == 0:
                time.sleep(0.2)
                continue
            raise
        except psycopg.InterfaceError as e:
            last = e
            if attempt == 0:
                time.sleep(0.2)
                continue
            raise
    assert last is not None
    raise last


def _normalize_url(url: str) -> str:
    """Render and Heroku still hand out ``postgres://`` URLs which newer
    libraries reject. Normalize to ``postgresql://``."""
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://"):]
    return url


def _connect() -> psycopg.Connection:
    """Open a single fresh Postgres connection.

    ``connect_timeout`` caps how long a TCP/TLS/auth handshake can take
    before psycopg gives up — without it, libpq blocks for ~75s on a stuck
    handshake, which would cascade into gunicorn worker timeouts.

    TCP keepalives ensure that a half-open route surfaces as an error
    rather than blocking forever on read.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Add it to .env, e.g.\n"
            "  DATABASE_URL=postgresql://user:pass@localhost:5432/happytrader"
        )
    return psycopg.connect(
        _normalize_url(url),
        row_factory=dict_row,
        connect_timeout=int(os.environ.get("DATABASE_CONNECT_TIMEOUT", "10")),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
    )


@contextmanager
def get_conn():
    """Yield a fresh Postgres connection.

    The connection is wrapped in ``with conn:``, so it commits on a clean
    exit and rolls back on exception. The outer ``finally`` always closes
    the underlying socket, even if commit/rollback raises.
    """
    conn = _connect()
    try:
        with conn:
            yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


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
