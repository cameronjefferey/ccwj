"""Short cache for the signed-in shell (nav, banners, plan, paper flag).

Those reads are Postgres, not SnapTrade or BigQuery. Each one used to
open its own connection, and a signed-in page did that about fifteen
times before the first byte. The values here live for two minutes and
are dropped when a sync, connect, or profile write changes them.

Disabled under pytest so a test that inserts a row and reads it back
does not see another test's cache. Production leaves the env var unset.
"""
from __future__ import annotations

import os
import threading
import time

SHELL_TTL_SECONDS = 120

_MISS = object()
_lock = threading.Lock()
_store: dict[tuple, tuple[float, object]] = {}


def enabled() -> bool:
    if os.environ.get("SHELL_CACHE_DISABLED") == "1":
        return False
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return False
    return True


def _copy(value):
    if isinstance(value, list):
        return [_copy(item) for item in value]
    if isinstance(value, dict):
        return {key: _copy(item) for key, item in value.items()}
    return value


def get(user_id, key):
    if user_id is None or not enabled():
        return _MISS
    now = time.monotonic()
    with _lock:
        hit = _store.get((user_id, key))
        if not hit or hit[0] <= now:
            if hit:
                _store.pop((user_id, key), None)
            return _MISS
        value = hit[1]
    return _copy(value)


def put(user_id, key, value) -> None:
    if user_id is None or not enabled():
        return
    expires = time.monotonic() + SHELL_TTL_SECONDS
    frozen = _copy(value)
    with _lock:
        if len(_store) > 2000:
            now = time.monotonic()
            dead = [item for item, (exp, _) in _store.items() if exp <= now]
            for item in dead:
                _store.pop(item, None)
            if len(_store) > 2000:
                for item in list(_store)[:500]:
                    _store.pop(item, None)
        _store[(user_id, key)] = (expires, frozen)


def load(user_id, key, loader):
    """Return the cached value, or call ``loader`` and store it."""
    hit = get(user_id, key)
    if hit is not _MISS:
        return hit
    value = loader()
    put(user_id, key, value)
    return value


def invalidate(user_id) -> None:
    """Drop every cached shell value for one user. Safe if nothing is cached."""
    if user_id is None:
        return
    with _lock:
        for item in [key for key in _store if key[0] == user_id]:
            _store.pop(item, None)


def clear() -> None:
    with _lock:
        _store.clear()
