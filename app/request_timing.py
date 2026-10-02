"""Per-request outbound time and shell-hook time.

``begin_request_timing`` runs in ``before_request``. SnapTrade and Reddit
calls add themselves with ``add_outbound``. Context processors record
their own wall time with ``stage``. A background thread does not copy
this context unless it calls ``contextvars.copy_context`` itself, so a
CAPI post or a paper sync does not add to the page that queued it.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from contextvars import ContextVar

_slot: ContextVar[list | None] = ContextVar("ht_outbound_slot", default=None)
_hooks: ContextVar[dict | None] = ContextVar("ht_hook_slot", default=None)


def begin_outbound() -> None:
    begin_request_timing()


def begin_request_timing() -> None:
    _slot.set([0.0, 0])
    _hooks.set({})


def add_outbound(seconds: float) -> None:
    slot = _slot.get()
    if slot is None:
        return
    slot[0] += float(seconds)
    slot[1] += 1


def outbound_ms_and_count() -> tuple[float, int]:
    slot = _slot.get()
    if not slot:
        return 0.0, 0
    return slot[0] * 1000.0, int(slot[1])


@contextmanager
def stage(name: str):
    """Add this block's wall time to the request's hook breakdown."""
    start = time.perf_counter()
    try:
        yield
    finally:
        hooks = _hooks.get()
        if hooks is None:
            return
        elapsed = time.perf_counter() - start
        hooks[name] = hooks.get(name, 0.0) + elapsed


def hook_ms() -> dict[str, float]:
    hooks = _hooks.get()
    if not hooks:
        return {}
    return {name: ms * 1000.0 for name, ms in hooks.items()}


def format_hooks(hooks: dict | None) -> str:
    if not hooks:
        return "hooks=-"
    parts = [f"{name}:{ms:.0f}" for name, ms in hooks.items()]
    return "hooks=" + ",".join(parts)
