"""Admin overview dashboard — classification, attention, page-view gating."""

from types import SimpleNamespace

from app.admin_overview import (
    _attention,
    _classify_users,
    _mix_rows,
    should_record_page_view,
)
from app.plan import STATE_ACTIVE, STATE_FROZEN, STATE_NO_DATA, STATE_TRIALING


def test_classify_splits_demo_admin_and_real_plans():
    rows = [
        {"username": "demo", "plan": "trial", "trial_started_at": None},
        {"username": "cameron", "plan": "active", "trial_started_at": None,
         "ai_subscription_status": "active"},
        {"username": "alice", "plan": "trial", "trial_started_at": None},
        {"username": "bob", "plan": "active", "trial_started_at": None,
         "subscription_cancel_at_period_end": True},
    ]
    out = _classify_users(rows, admin_names={"cameron"})
    assert out["paying"] == 1
    assert out["canceling"] == 1
    assert out["ai_addon"] == 0  # cameron is admin, skipped
    assert out["mix"]["demo"] == 1
    assert out["mix"]["admin"] == 1
    assert out["mix"][STATE_NO_DATA] == 1
    assert out["mix"][STATE_ACTIVE] == 1
    assert len(out["real"]) == 2


def test_mix_rows_skip_zeros():
    rows = _mix_rows({STATE_ACTIVE: 3, STATE_TRIALING: 1, STATE_FROZEN: 0})
    keys = [r["key"] for r in rows]
    assert keys == [STATE_ACTIVE, STATE_TRIALING]
    assert abs(sum(r["pct"] for r in rows) - 100) < 0.2


def test_attention_flags_broken_and_frozen():
    classified = {
        "mix": {STATE_FROZEN: 2, STATE_ACTIVE: 1},
        "canceling": 0,
    }
    items = _attention(classified, broken=[{"username": "a"}], open_feedback=0,
                       failed_syncs=0, pending_first_sync=0)
    titles = " ".join(i["title"] for i in items)
    assert "broken" in titles.lower()
    assert "frozen" in titles.lower()
    assert not any(i["tone"] == "ok" for i in items)


def test_attention_all_clear():
    classified = {"mix": {STATE_ACTIVE: 4}, "canceling": 0}
    items = _attention(classified, broken=[], open_feedback=0,
                       failed_syncs=0, pending_first_sync=0)
    assert items[0]["tone"] == "ok"


def test_should_record_skips_api_redirect_and_skeleton_refetch():
    ok_req = SimpleNamespace(
        method="GET", path="/daily-review", endpoint="weekly_review",
        headers={},
    )
    ok_resp = SimpleNamespace(status_code=200)
    assert should_record_page_view(ok_req, ok_resp) is True

    skip_full = SimpleNamespace(
        method="GET", path="/daily-review", endpoint="weekly_review",
        headers={"X-HT-Full": "1"},
    )
    assert should_record_page_view(skip_full, ok_resp) is False

    api = SimpleNamespace(
        method="GET", path="/api/nav/symbols", endpoint="api_nav_symbols",
        headers={},
    )
    assert should_record_page_view(api, ok_resp) is False

    admin = SimpleNamespace(
        method="GET", path="/admin", endpoint="admin_overview",
        headers={},
    )
    assert should_record_page_view(admin, ok_resp) is False

    redir = SimpleNamespace(status_code=302)
    assert should_record_page_view(ok_req, redir) is False


def test_admin_overview_hidden_from_anonymous():
    from app import app
    client = app.test_client()
    resp = client.get("/admin")
    assert resp.status_code in (302, 303, 401)
    assert b"How the site is doing" not in resp.data
