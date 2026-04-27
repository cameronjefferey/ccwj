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
from psycopg_pool import ConnectionPool


_pool: Optional[ConnectionPool] = None
_pool_lock = threading.Lock()


def _run_db_twice(fn):
    """Run fn(); retry once on a stale pooled connection (OperationalError /
    InterfaceError). After a laptop sleep or long idle, the first request often
    uses a connection Postgres already closed; a single retry usually fixes it.

    We do NOT retry on PoolTimeout. PoolTimeout means the pool genuinely couldn't
    hand out a connection within `timeout` seconds (DB unreachable, conn limit
    saturated, network blip). Retrying just doubles the user's wait — the pool
    won't magically recover in another `timeout` seconds. Surface it fast so the
    error handler can render a 503 "try again in a moment" page instead of
    burning 20s on a spinner before showing a generic 500.
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
    # Managed Postgres (e.g. Render) silently kills idle TCP sessions, and
    # OOM-killed gunicorn workers can leave their connections held server-side
    # for minutes (Postgres only times them out when its own keepalives fire).
    # That can starve a fresh worker on boot. Defenses:
    #   1. Modest pool ceiling (max_size=4 per worker × 2 workers = 8 conns).
    #      Render's smaller Postgres tiers cap connections aggressively, and
    #      a SIGKILL'd worker leaves orphans behind.
    #   2. TCP keepalives so the OS reaps dead conns before psycopg does.
    #   3. Shorter max_idle so idle conns recycle before Render's edge kills
    #      them at the network layer.
    #   4. Tunable min_size — defaults to 1 (one warm conn so the first
    #      request doesn't pay full TLS handshake), low enough that even
    #      after a worker SIGKILL we don't crowd Postgres on the next boot.
    # Recycle aggressively (60s default): Render's network silently kills idle
    # TCP sessions to managed Postgres after a couple minutes. Recycling before
    # the edge does it for us means fewer "stale conn discovered on first
    # request after idle" round-trips.
    max_idle = float(os.environ.get("DATABASE_POOL_MAX_IDLE", "60"))
    max_lifetime = float(os.environ.get("DATABASE_POOL_MAX_LIFETIME", "1800"))
    # pool_wait: how long a request waits for a free connection. Keep small
    # (5s default): a longer wait doesn't save a request whose pool is already
    # saturated; it just blocks a worker thread and makes the user stare at a
    # spinner. The 503 handler turns this into a fast, clear "try again in a
    # moment" page instead of a 20s timeout → generic 500. CRITICAL: keep this
    # WELL below gunicorn's --timeout (120s); otherwise stuck pool waits pile
    # up across all 4 threads in a worker, the worker gets SIGKILL'd, and its
    # Postgres conns are orphaned server-side — a feedback loop that takes the
    # site down for minutes after a single hiccup.
    pool_wait = float(os.environ.get("DATABASE_POOL_TIMEOUT", "5"))
    return ConnectionPool(
        conninfo=_normalize_url(url),
        min_size=int(os.environ.get("DATABASE_POOL_MIN", "1")),
        # CRITICAL: keep this small. With 2 gunicorn workers, total conns =
        # 2 × max_size; an OOM-killed worker leaves its conns orphaned on the
        # server until Postgres reaps them. Raising this risks "too many
        # connections" on a fresh worker boot.
        max_size=int(os.environ.get("DATABASE_POOL_MAX", "4")),
        kwargs={
            "row_factory": dict_row,
            # connect_timeout: cap how long a brand-new TCP+TLS+auth handshake
            # can take. Without this, libpq will sit on a dead route for tens
            # of seconds, eating the entire pool_wait window with a single
            # failed connection attempt instead of failing fast and trying
            # again.
            "connect_timeout": 5,
            # TCP-level keepalives so the OS notices a dead route long before
            # psycopg does. Tightened from 60/10/5 → 15/5/3: detect a dead
            # conn in ~30s instead of ~110s, so post-idle requests don't hang
            # waiting for libpq to figure out the route is gone.
            "keepalives": 1,
            "keepalives_idle": 15,
            "keepalives_interval": 5,
            "keepalives_count": 3,
        },
        # NOTE: deliberately NO check= callback. A per-checkout SELECT 1 adds
        # a round-trip to every successful query and, worse, on a half-open
        # TCP it hangs until the OS keepalives fire — burning the entire
        # pool_wait window on a stale conn we could have just thrown away.
        # Stale-conn handling lives in `_run_db_twice` instead: the actual
        # query fails fast with OperationalError/InterfaceError, we discard
        # the conn, retry once, succeed. That's cheaper and degrades better.
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
