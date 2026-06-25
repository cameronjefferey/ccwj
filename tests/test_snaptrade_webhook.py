"""Tests for the SnapTrade webhook endpoint (event-driven sync).

The webhook is the "once SnapTrade finishes updating, kick off our sync" trigger
(ACCOUNT_HOLDINGS_UPDATED). These tests pin: secret verification, that a valid
holdings event queues exactly one background sync for the mapped user/account,
and that other events / unknown users / bad payloads never queue work.
"""
import json
import types

import pytest

from app import app, webhooks
from app import models as _models


class _FakeThread:
    instances = []

    def __init__(self, target=None, args=(), **kwargs):
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.started = False
        _FakeThread.instances.append(self)

    def start(self):
        self.started = True


@pytest.fixture(autouse=True)
def _no_real_threads(monkeypatch):
    # Patch ONLY the webhooks module's threading reference (not the global
    # stdlib module — flask-limiter uses threading.Timer internally).
    _FakeThread.instances = []
    monkeypatch.setattr(webhooks, "threading",
                        types.SimpleNamespace(Thread=_FakeThread))
    yield
    _FakeThread.instances = []


def _post(payload):
    with app.test_client() as c:
        return c.post(
            "/webhooks/snaptrade",
            data=json.dumps(payload),
            content_type="application/json",
        )


def test_webhook_bad_payload_400(monkeypatch):
    monkeypatch.setenv("SNAPTRADE_WEBHOOK_SECRET", "shh")
    with app.test_client() as c:
        r = c.post("/webhooks/snaptrade", data=b"not json",
                   content_type="application/json")
    assert r.status_code == 400
    assert _FakeThread.instances == []


def test_webhook_rejects_bad_secret(monkeypatch):
    monkeypatch.setenv("SNAPTRADE_WEBHOOK_SECRET", "right-secret")
    r = _post({
        "eventType": "ACCOUNT_HOLDINGS_UPDATED",
        "userId": "snap-user-1",
        "accountId": "acc-1",
        "webhookSecret": "WRONG",
    })
    assert r.status_code == 401
    assert _FakeThread.instances == [], "must not queue a sync on bad secret"


def test_webhook_triggers_sync_on_holdings_updated(monkeypatch):
    monkeypatch.setenv("SNAPTRADE_WEBHOOK_SECRET", "shh")
    monkeypatch.setattr(_models, "get_user_id_by_snaptrade_user_id",
                        lambda sid: 9 if sid == "snap-user-1" else None)
    r = _post({
        "eventType": "ACCOUNT_HOLDINGS_UPDATED",
        "userId": "snap-user-1",
        "accountId": "acc-1",
        "webhookSecret": "shh",
    })
    assert r.status_code == 200
    assert len(_FakeThread.instances) == 1
    t = _FakeThread.instances[0]
    assert t.target is webhooks._run_snaptrade_holdings_sync
    assert t.args == (9, "acc-1")
    assert t.started is True


def test_webhook_ignores_non_holdings_events(monkeypatch):
    monkeypatch.setenv("SNAPTRADE_WEBHOOK_SECRET", "shh")
    r = _post({
        "eventType": "CONNECTION_UPDATED",
        "userId": "snap-user-1",
        "accountId": "acc-1",
        "webhookSecret": "shh",
    })
    assert r.status_code == 200
    assert _FakeThread.instances == [], "only ACCOUNT_HOLDINGS_UPDATED syncs"


def test_webhook_unknown_user_no_sync(monkeypatch):
    monkeypatch.setenv("SNAPTRADE_WEBHOOK_SECRET", "shh")
    monkeypatch.setattr(_models, "get_user_id_by_snaptrade_user_id",
                        lambda sid: None)
    r = _post({
        "eventType": "ACCOUNT_HOLDINGS_UPDATED",
        "userId": "ghost",
        "accountId": "acc-1",
        "webhookSecret": "shh",
    })
    assert r.status_code == 200
    assert _FakeThread.instances == []


def test_webhook_missing_account_id_no_sync(monkeypatch):
    monkeypatch.setenv("SNAPTRADE_WEBHOOK_SECRET", "shh")
    monkeypatch.setattr(_models, "get_user_id_by_snaptrade_user_id",
                        lambda sid: 9)
    r = _post({
        "eventType": "ACCOUNT_HOLDINGS_UPDATED",
        "userId": "snap-user-1",
        "webhookSecret": "shh",
    })
    assert r.status_code == 200
    assert _FakeThread.instances == []
