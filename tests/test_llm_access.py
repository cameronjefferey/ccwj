"""Paid-LLM gate: add-on / beta / admin only; fail closed."""
import app.llm_access as access
from app.plan import PLAN_BETA, PLAN_TRIAL, PLAN_ACTIVE


def test_admin_can_use_paid(monkeypatch):
    monkeypatch.setattr(access, "get_user_plan_row", lambda uid: {
        "username": "cameron3", "plan": PLAN_TRIAL,
    })
    monkeypatch.setattr("app.models.is_admin", lambda name: name == "cameron3")
    assert access.user_can_use_paid_llm(1) is True


def test_beta_can_use_paid(monkeypatch):
    monkeypatch.setattr(access, "get_user_plan_row", lambda uid: {
        "username": "pat", "plan": PLAN_BETA,
    })
    monkeypatch.setattr("app.models.is_admin", lambda name: False)
    assert access.user_can_use_paid_llm(2) is True


def test_demo_cannot_use_paid(monkeypatch):
    monkeypatch.setattr(access, "get_user_plan_row", lambda uid: {
        "username": "demo", "plan": PLAN_BETA,
    })
    monkeypatch.setattr("app.models.is_admin", lambda name: False)
    assert access.user_can_use_paid_llm(3) is False


def test_pro_without_addon_cannot_use_paid(monkeypatch):
    monkeypatch.setattr(access, "get_user_plan_row", lambda uid: {
        "username": "pat", "plan": PLAN_ACTIVE,
    })
    monkeypatch.setattr("app.models.is_admin", lambda name: False)
    monkeypatch.setattr(access, "fetch_one", lambda *a, **k: {
        "ai_subscription_status": None,
    })
    assert access.user_can_use_paid_llm(4) is False


def test_addon_paying_can_use_paid(monkeypatch):
    monkeypatch.setattr(access, "get_user_plan_row", lambda uid: {
        "username": "pat", "plan": PLAN_TRIAL,
    })
    monkeypatch.setattr("app.models.is_admin", lambda name: False)
    monkeypatch.setattr(access, "fetch_one", lambda *a, **k: {
        "ai_subscription_status": "active",
    })
    assert access.user_can_use_paid_llm(5) is True


def test_missing_column_fails_closed(monkeypatch):
    monkeypatch.setattr(access, "get_user_plan_row", lambda uid: {
        "username": "pat", "plan": PLAN_TRIAL,
    })
    monkeypatch.setattr("app.models.is_admin", lambda name: False)
    monkeypatch.setattr(
        access, "fetch_one",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("column missing")),
    )
    assert access.user_can_use_paid_llm(6) is False


def test_none_user_is_denied():
    assert access.user_can_use_paid_llm(None) is False
