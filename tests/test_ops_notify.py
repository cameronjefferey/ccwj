"""Product-event Telegram pings (signups, subscribe, first data, …)."""

import json
import os

import app.ops_notify as on
from app.billing import (
    _notify_ai_activation,
    _notify_ai_ended,
    _notify_pro_activation,
    _notify_pro_ended,
    _pro_interval_label,
)
from app.plan import PLAN_ACTIVE, PLAN_TRIAL


def test_default_events_include_signup_and_subscribe(monkeypatch):
    monkeypatch.delenv("OPS_NOTIFY_EVENTS", raising=False)
    enabled = on.enabled_events()
    assert "signup" in enabled
    assert "subscribe" in enabled
    assert "feedback" in enabled
    assert "first_data" in enabled


def test_quiet_with_none(monkeypatch):
    monkeypatch.setenv("OPS_NOTIFY_EVENTS", "none")
    assert on.enabled_events() == frozenset()
    assert on.event_enabled("signup") is False


def test_allowlist_subset(monkeypatch):
    monkeypatch.setenv("OPS_NOTIFY_EVENTS", "subscribe, cancel")
    assert on.enabled_events() == {"subscribe", "cancel"}
    assert on.event_enabled("signup") is False
    assert on.event_enabled("subscribe") is True


def test_notify_skips_when_unset(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.delenv("OPS_NOTIFY_EVENTS", raising=False)
    monkeypatch.setenv("OPS_NOTIFY_SYNC", "1")
    assert on.notify("signup", "HappyTrader: new signup @x", username="x") is False


def test_notify_skips_demo_except_feedback(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    monkeypatch.setenv("OPS_NOTIFY_SYNC", "1")
    monkeypatch.delenv("OPS_NOTIFY_EVENTS", raising=False)
    assert on.notify("signup", "nope", username="demo") is False

    captured = {}

    class _Resp:
        def read(self):
            return json.dumps({"ok": True}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout=12):
        captured["ok"] = True
        return _Resp()

    monkeypatch.setattr(on.urllib.request, "urlopen", fake_urlopen)
    assert on.notify("feedback", "HappyTrader: feedback from @demo\nbug", username="demo") is True
    assert captured.get("ok") is True


def test_notify_sends_when_configured(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "99")
    monkeypatch.setenv("OPS_NOTIFY_SYNC", "1")
    monkeypatch.delenv("OPS_NOTIFY_EVENTS", raising=False)
    captured = {}

    class _Resp:
        def read(self):
            return json.dumps({"ok": True}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout=12):
        captured["url"] = req.full_url
        captured["body"] = req.data.decode()
        return _Resp()

    monkeypatch.setattr(on.urllib.request, "urlopen", fake_urlopen)
    assert on.notify("signup", "HappyTrader: new signup @alice", username="alice") is True
    assert captured["url"].endswith("/bottok/sendMessage")
    assert "chat_id=99" in captured["body"]
    assert "alice" in captured["body"]


def test_notify_respects_allowlist(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "99")
    monkeypatch.setenv("OPS_NOTIFY_SYNC", "1")
    monkeypatch.setenv("OPS_NOTIFY_EVENTS", "subscribe")
    assert on.notify("signup", "nope", username="alice") is False


def test_pro_interval_label(monkeypatch):
    monkeypatch.setenv("STRIPE_PRICE_MONTHLY", "price_m")
    monkeypatch.setenv("STRIPE_PRICE_ANNUAL", "price_a")
    assert _pro_interval_label("price_m") == "monthly"
    assert _pro_interval_label("price_a") == "annual"
    assert _pro_interval_label("price_other") == "Pro"


def test_pro_activation_notifies_only_on_transition(monkeypatch):
    pings = []
    monkeypatch.setattr(
        "app.ops_notify.notify",
        lambda kind, text, username=None: pings.append((kind, text)),
    )
    before = {"username": "alice", "plan": PLAN_TRIAL, "subscription_cancel_at_period_end": False}
    _notify_pro_activation(
        7, before=before, stripe_price_id="price_m", cancel_at_period_end=False,
    )
    assert pings and pings[0][0] == "subscribe"
    pings.clear()
    already = {"username": "alice", "plan": PLAN_ACTIVE, "subscription_cancel_at_period_end": False}
    _notify_pro_activation(
        7, before=already, stripe_price_id="price_m", cancel_at_period_end=False,
    )
    assert pings == []
    _notify_pro_activation(
        7, before=already, stripe_price_id="price_m", cancel_at_period_end=True,
    )
    assert pings and pings[0][0] == "canceling"


def test_pro_ended_skips_non_paying(monkeypatch):
    pings = []
    monkeypatch.setattr(
        "app.ops_notify.notify",
        lambda kind, text, username=None: pings.append(kind),
    )
    _notify_pro_ended(7, before={"username": "bob", "plan": PLAN_TRIAL}, status="canceled")
    assert pings == []
    _notify_pro_ended(7, before={"username": "bob", "plan": PLAN_ACTIVE}, status="canceled")
    assert pings == ["cancel"]


def test_ai_activation_skips_already_paying(monkeypatch):
    pings = []
    monkeypatch.setattr(
        "app.ops_notify.notify",
        lambda kind, text, username=None: pings.append(kind),
    )
    _notify_ai_activation(7, before={"username": "c", "ai_subscription_status": "active"})
    assert pings == []
    _notify_ai_activation(7, before={"username": "c", "ai_subscription_status": None})
    assert pings == ["subscribe_ai"]
    _notify_ai_ended(7, before={"username": "c", "ai_subscription_status": "active"}, status="canceled")
    assert pings[-1] == "cancel_ai"


def test_user_label_uses_passed_username():
    assert on.user_label(9, username="pat") == "@pat"
    assert on.user_label(None, username="") == "user_id=None"
