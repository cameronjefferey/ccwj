"""
Postgres connection helpers.

Each call to :func:`get_conn` used to open a fresh psycopg connection.
A signed-in page runs the shell (plan, tenants, SnapTrade rows, email,
Simple view) as many separate queries, and each TLS handshake stacked.
Inside one request the first query still opens a connection, and the
rest of that request's queries reuse it. The connection is closed at
the end of the request. Worker threads never share it: psycopg
connections are not thread-safe, and ``_bq_parallel`` copies the
request context onto other threads.

We deliberately do **not** use a connection pool.

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
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterable, Optional

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row

# The request thread's connection. threading.local, not flask.g: a copied
# request context on a BigQuery worker must not see this socket.
_local = threading.local()


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


def bind_request_thread() -> None:
    """Mark this thread as the owner of the request's Postgres connection."""
    if getattr(_local, "request_ident", None) != threading.get_ident():
        _local.request_ident = threading.get_ident()
        _local.opens = 0


def request_connect_count() -> int:
    if getattr(_local, "request_ident", None) != threading.get_ident():
        return 0
    return int(getattr(_local, "opens", 0) or 0)


def close_request_connection() -> None:
    """Close the request thread's connection. Other threads are left alone."""
    if getattr(_local, "request_ident", None) != threading.get_ident():
        return
    conn = getattr(_local, "conn", None)
    _local.conn = None
    _local.request_ident = None
    _local.opens = 0
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


def _request_connection():
    """The open connection for this request thread, or None to open a private one."""
    try:
        from flask import has_request_context
    except Exception:
        return None
    if not has_request_context():
        return None
    if getattr(_local, "request_ident", None) != threading.get_ident():
        return None
    conn = getattr(_local, "conn", None)
    if conn is not None:
        if getattr(conn, "closed", False):
            _local.conn = None
        else:
            return conn
    conn = _connect()
    _local.conn = conn
    _local.opens = int(getattr(_local, "opens", 0) or 0) + 1
    return conn


def _drop_request_connection() -> None:
    if getattr(_local, "request_ident", None) != threading.get_ident():
        return
    conn = getattr(_local, "conn", None)
    _local.conn = None
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


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


def _idle(conn) -> bool:
    try:
        return conn.info.transaction_status == TransactionStatus.IDLE
    except Exception:
        return True


@contextmanager
def get_conn():
    """Yield a Postgres connection.

    On the request thread this is the one connection opened for that
    request. Each call still commits or rolls back its own transaction.
    A nested call (a write inside an open transaction) joins that
    transaction instead of committing early. A dropped socket is closed
    so the retry opens a new one.

    Outside a request, or on a worker thread, this still opens a private
    connection and closes it before returning.
    """
    shared = _request_connection()
    if shared is None:
        conn = _connect()
        try:
            with conn:
                yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return
    nested = not _idle(shared)
    try:
        if nested:
            yield shared
            return
        with shared:
            yield shared
    except (psycopg.OperationalError, psycopg.InterfaceError):
        _drop_request_connection()
        raise


@contextmanager
def advisory_lock(key: int):
    """Hold a cluster-wide Postgres session advisory lock for the block.

    Serializes a critical section across ALL web workers/processes (unlike a
    Python ``threading.Lock``, which is per-process). Used to serialize
    webhook-triggered broker syncs so concurrent GitHub seed pushes can't race
    the ref update (the push path is not fast-forward-safe under concurrency).

    Blocks until the lock is acquired. Holds a dedicated connection (autocommit
    so the session-level lock persists regardless of transaction state) for the
    duration, and always unlocks + closes on exit.
    """
    conn = _connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (key,))
        yield
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (key,))
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _params_include_ephemeral_demo(params: Iterable[Any]) -> bool:
    """True when a query is keyed by a public demo session id.

    Those ids are not ``users.id`` values. Refusing them here means a demo
    session cannot read or write another user's Postgres rows even if a
    caller forgets to special-case the id.
    """
    try:
        from app.demo_guard import is_ephemeral_demo_id
    except Exception:
        return False
    for value in params or ():
        if is_ephemeral_demo_id(value):
            return True
    return False


def fetch_all(sql: str, params: Iterable[Any] = ()) -> list[dict]:
    if _params_include_ephemeral_demo(params):
        return []

    def _go():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return cur.fetchall()

    return _run_db_twice(_go)


def fetch_one(sql: str, params: Iterable[Any] = ()) -> Optional[dict]:
    if _params_include_ephemeral_demo(params):
        return None

    def _go():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return cur.fetchone()

    return _run_db_twice(_go)


def execute(sql: str, params: Iterable[Any] = ()) -> None:
    if _params_include_ephemeral_demo(params):
        return None

    def _go():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))

    _run_db_twice(_go)


def execute_returning(sql: str, params: Iterable[Any] = ()) -> Optional[dict]:
    """Run an INSERT/UPDATE/DELETE ... RETURNING and return the first row."""
    if _params_include_ephemeral_demo(params):
        return None

    def _go():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return cur.fetchone()

    return _run_db_twice(_go)
