"""Unit tests for the multi-account (?tenants=) scope resolution used by
the Position Detail account-toggle bar. See routes._tenants_for_scope."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest


OWNED = [
    {"tenant_id": "snaptrade:aaa", "account_name": "Schwab Account"},
    {"tenant_id": "snaptrade:bbb", "account_name": "Schwab Account"},
    {"tenant_id": "snaptrade:ccc", "account_name": "Fidelity Account"},
]


def _resolve(query_string, admin=False, owned=OWNED, selected_account=""):
    from app import app
    import app.routes as routes

    user = SimpleNamespace(id=9, username="cam", is_authenticated=True)
    with app.test_request_context("/position/AAPL" + query_string):
        with patch.object(routes, "current_user", user), \
             patch.object(routes, "is_admin", lambda u: admin), \
             patch.object(
                 routes, "get_broker_tenants_for_user", lambda uid: owned
             ):
            return routes._tenants_for_scope(selected_account)


def test_tenants_param_selects_subset_of_owned():
    scope = _resolve("?tenants=snaptrade:aaa,snaptrade:ccc")
    assert scope == ["snaptrade:aaa", "snaptrade:ccc"]


def test_tenants_param_drops_unowned_ids():
    """A URL must never widen tenancy: an id the user doesn't own is dropped."""
    scope = _resolve("?tenants=snaptrade:aaa,snaptrade:HACK")
    assert scope == ["snaptrade:aaa"]


def test_tenants_param_all_unowned_falls_back_to_all_owned():
    scope = _resolve("?tenants=snaptrade:HACK1,snaptrade:HACK2")
    assert scope == ["snaptrade:aaa", "snaptrade:bbb", "snaptrade:ccc"]


def test_tenants_param_dedups():
    scope = _resolve("?tenants=snaptrade:bbb,snaptrade:bbb")
    assert scope == ["snaptrade:bbb"]


def test_no_selection_returns_all_owned_for_user():
    scope = _resolve("")
    assert scope == ["snaptrade:aaa", "snaptrade:bbb", "snaptrade:ccc"]


def test_single_tenant_param_still_wins_over_tenants():
    """?tenant= (single, broker-stable) takes precedence over ?tenants=."""
    scope = _resolve("?tenant=snaptrade:ccc&tenants=snaptrade:aaa,snaptrade:bbb")
    assert scope == ["snaptrade:ccc"]


def test_admin_tenants_param_passthrough():
    """Admin may address any tenant set (no owned-intersection)."""
    scope = _resolve("?tenants=snaptrade:zzz,snaptrade:yyy", admin=True)
    assert scope == ["snaptrade:zzz", "snaptrade:yyy"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
