"""Unit tests for the v2 broker_tenants helpers in ``app/models.py``.

Most helpers are thin Postgres wrappers and are exercised in
integration via ``app/snaptrade.py``. The two helpers WORTH unit-testing
without a Postgres connection are:

- ``build_tenant_id`` — the format spec contract from
  ``docs/V2_TENANT_KEY_DESIGN.md``. Every emitter / consumer in the
  warehouse depends on the exact ``"<broker_slug>:<broker_uuid>"``
  shape. A bug here is the v2 equivalent of the v1 SERIAL collision.

The other helpers (``get_or_create_broker_tenant``,
``get_tenant_ids_for_user``, ``mark_tenant_connection_broken``, etc.)
are integration-tested in the cutover flow (Phase 6) and via the
``broker-sync-safety`` skill smoke checks.
"""
from __future__ import annotations

import pytest

from app.models import build_tenant_id


def test_build_tenant_id_canonical_shape():
    """The locked v2 format: ``<broker_slug>:<broker_uuid>``."""
    assert (
        build_tenant_id("snaptrade", "bed78305-a764-4c4d-b4c7-fe59e391f661")
        == "snaptrade:bed78305-a764-4c4d-b4c7-fe59e391f661"
    )


def test_build_tenant_id_lowercases_broker_slug():
    """broker_slug is normalized to lowercase — the dim is
    case-insensitive on the slug part so 'snaptrade' and 'SnapTrade'
    don't fork into two tenants."""
    assert (
        build_tenant_id("SnapTrade", "abc-123")
        == "snaptrade:abc-123"
    )


def test_build_tenant_id_preserves_uuid_case():
    """broker_uuid case IS preserved — SnapTrade UUIDs are lowercase by
    convention but Schwab hashes are uppercase hex. Whatever the broker
    ships is what we store."""
    upper = "ABCDEF1234567890"
    assert build_tenant_id("snaptrade", upper) == f"snaptrade:{upper}"


def test_build_tenant_id_strips_whitespace():
    """Leading/trailing whitespace is tolerated and stripped."""
    assert (
        build_tenant_id("  snaptrade  ", "  abc  ")
        == "snaptrade:abc"
    )


def test_build_tenant_id_empty_broker_slug_raises():
    """Empty broker_slug must fail FAST — a None-coalesced
    ``"":abc"`` tenant_id would silently corrupt the warehouse."""
    with pytest.raises(ValueError):
        build_tenant_id("", "abc-123")
    with pytest.raises(ValueError):
        build_tenant_id(None, "abc-123")
    with pytest.raises(ValueError):
        build_tenant_id("   ", "abc-123")


def test_build_tenant_id_empty_broker_uuid_raises():
    """Empty broker_uuid must fail FAST — a ``"snaptrade:"`` row
    would silently collide with every other empty-uuid row."""
    with pytest.raises(ValueError):
        build_tenant_id("snaptrade", "")
    with pytest.raises(ValueError):
        build_tenant_id("snaptrade", None)
    with pytest.raises(ValueError):
        build_tenant_id("snaptrade", "   ")


def test_build_tenant_id_no_double_colon_when_uuid_has_colon():
    """Unlikely (broker UUIDs don't contain colons), but if a future
    broker ever shipped one we'd still get a single-level split. The
    helper does NOT escape the colon — split-on-first-colon is the
    documented parsing convention in ``docs/V2_TENANT_KEY_DESIGN.md``."""
    weird_uuid = "abc:def"
    out = build_tenant_id("snaptrade", weird_uuid)
    assert out == "snaptrade:abc:def"
    assert out.split(":", 1) == ["snaptrade", "abc:def"]
