"""Regression tests for account-local Position Detail leg ids."""

from app.routes import _resolve_position_leg_filter


def _session(tenant_id, session_id):
    return {"tenant_id": tenant_id, "session_id": session_id}


def test_leg_filter_is_preserved_for_one_tenant():
    sessions = [
        _session("snaptrade:aaa", 1),
        _session("snaptrade:aaa", 2),
    ]

    leg_param, selected = _resolve_position_leg_filter(sessions, "2")

    assert leg_param == "2"
    assert selected == [2]


def test_ambiguous_leg_filter_is_dropped_across_tenants():
    sessions = [
        _session("snaptrade:aaa", 1),
        _session("snaptrade:aaa", 2),
        _session("snaptrade:bbb", 1),
    ]

    leg_param, selected = _resolve_position_leg_filter(sessions, "1")

    assert leg_param == ""
    assert selected == [1, 2, 1]
