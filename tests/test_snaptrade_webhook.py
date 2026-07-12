"""Tests for the SnapTrade webhook endpoint (event-driven sync).

The webhook is the "once SnapTrade finishes updating, kick off our sync" trigger
(ACCOUNT_HOLDINGS_UPDATED). SnapTrade authenticates via the `Signature` header
(base64 HMAC-SHA256 of the canonical JSON body, keyed by the consumer key —
webhook secrets are deprecated). These tests pin: signature verification, that a
valid holdings event queues exactly one background sync for the mapped
user/account, and that other events / unknown users / bad payloads never queue.
"""
import base64
import hashlib
import hmac
import json
import types

import pytest

from app import app, webhooks
from app import models as _models

_CONSUMER_KEY = "test-consumer-key"


def _sign(payload: dict, key: str = _CONSUMER_KEY) -> str:
    content = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    digest = hmac.new(key.encode(), content.encode(), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


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
    monkeypatch.setenv("SNAPTRADE_CONSUMER_KEY", _CONSUMER_KEY)
    # Reset the per-account debounce/coalesce state so keys don't leak across
    # tests (the FakeThread never runs the worker that would clear them).
    webhooks._scheduled_keys.clear()
    webhooks._pending_sync_at.clear()
    yield
    _FakeThread.instances = []
    webhooks._scheduled_keys.clear()
    webhooks._pending_sync_at.clear()


def _post(payload, signature=None):
    body = json.dumps(payload)
    headers = {"Content-Type": "application/json"}
    if signature is not None:
        headers["Signature"] = signature
    with app.test_client() as c:
        return c.post("/webhooks/snaptrade", data=body, headers=headers)


def test_webhook_bad_payload_400():
    with app.test_client() as c:
        r = c.post("/webhooks/snaptrade", data=b"not json",
                   content_type="application/json")
    assert r.status_code == 400
    assert _FakeThread.instances == []


def test_webhook_rejects_bad_signature():
    r = _post({
        "eventType": "ACCOUNT_HOLDINGS_UPDATED",
        "userId": "snap-user-1",
        "accountId": "acc-1",
    }, signature="WRONG")
    assert r.status_code == 401
    assert _FakeThread.instances == [], "must not queue a sync on bad signature"


def test_webhook_rejects_missing_signature():
    payload = {"eventType": "ACCOUNT_HOLDINGS_UPDATED", "userId": "u", "accountId": "a"}
    r = _post(payload)  # no Signature header
    assert r.status_code == 401
    assert _FakeThread.instances == []


def test_webhook_triggers_sync_on_holdings_updated(monkeypatch):
    monkeypatch.setattr(_models, "get_user_id_by_snaptrade_user_id",
                        lambda sid: 9 if sid == "snap-user-1" else None)
    payload = {
        "eventType": "ACCOUNT_HOLDINGS_UPDATED",
        "userId": "snap-user-1",
        "accountId": "acc-1",
    }
    r = _post(payload, signature=_sign(payload))
    assert r.status_code == 200
    assert len(_FakeThread.instances) == 1
    t = _FakeThread.instances[0]
    # The event now goes through the per-account debounce worker (which then
    # calls _run_snaptrade_holdings_sync once the account goes quiet).
    assert t.target is webhooks._run_debounced_snaptrade_sync
    assert t.args == (9, "acc-1")
    assert t.started is True


def test_webhook_coalesces_burst_for_same_account(monkeypatch):
    # Under the real-time plan SnapTrade fires many holdings events per day.
    # A burst for the SAME account must spawn only ONE debounce worker; later
    # events are absorbed (they just bump the pending timestamp).
    monkeypatch.setattr(_models, "get_user_id_by_snaptrade_user_id",
                        lambda sid: 9 if sid == "snap-user-1" else None)
    payload = {
        "eventType": "ACCOUNT_HOLDINGS_UPDATED",
        "userId": "snap-user-1",
        "accountId": "acc-1",
    }
    sig = _sign(payload)
    for _ in range(4):
        assert _post(payload, signature=sig).status_code == 200
    assert len(_FakeThread.instances) == 1, "burst must coalesce into one worker"

    # A different account is independent — it gets its own worker.
    other = {
        "eventType": "ACCOUNT_HOLDINGS_UPDATED",
        "userId": "snap-user-1",
        "accountId": "acc-2",
    }
    assert _post(other, signature=_sign(other)).status_code == 200
    assert len(_FakeThread.instances) == 2


def test_webhook_ignores_non_holdings_events():
    payload = {
        "eventType": "CONNECTION_UPDATED",
        "userId": "snap-user-1",
        "accountId": "acc-1",
    }
    r = _post(payload, signature=_sign(payload))
    assert r.status_code == 200
    assert _FakeThread.instances == [], "only ACCOUNT_HOLDINGS_UPDATED syncs"


def test_webhook_unknown_user_no_sync(monkeypatch):
    monkeypatch.setattr(_models, "get_user_id_by_snaptrade_user_id",
                        lambda sid: None)
    payload = {
        "eventType": "ACCOUNT_HOLDINGS_UPDATED",
        "userId": "ghost",
        "accountId": "acc-1",
    }
    r = _post(payload, signature=_sign(payload))
    assert r.status_code == 200
    assert _FakeThread.instances == []


def test_webhook_missing_account_id_no_sync(monkeypatch):
    monkeypatch.setattr(_models, "get_user_id_by_snaptrade_user_id",
                        lambda sid: 9)
    payload = {"eventType": "ACCOUNT_HOLDINGS_UPDATED", "userId": "snap-user-1"}
    r = _post(payload, signature=_sign(payload))
    assert r.status_code == 200
    assert _FakeThread.instances == []


# ---------------------------------------------------------------------------
# Retry-on-failure for the off-thread webhook sync (no svix redelivery reaches
# us here, so a transient failure must self-heal instead of stranding the
# account until the next cron — real case 2026-07-09, account 8c597f1a).
# ---------------------------------------------------------------------------

def _wire_holdings_sync(monkeypatch, sync_fn):
    """Patch everything ``_run_snaptrade_holdings_sync`` imports so we can drive
    just the retry loop with a fake ``_sync_one_connection``."""
    import contextlib
    from app import db as _db
    from app import snaptrade as _snap

    monkeypatch.setattr(webhooks, "_WEBHOOK_SYNC_RETRY_BACKOFF_SECONDS", 0)

    @contextlib.contextmanager
    def _fake_lock(_key):
        yield
    monkeypatch.setattr(_db, "advisory_lock", _fake_lock)
    monkeypatch.setattr(_models, "get_snaptrade_account",
                        lambda u, a: {"first_sync_completed": True})
    monkeypatch.setattr(_snap, "_bulk_sync_lookback_days", lambda *a, **k: 60)
    monkeypatch.setattr(_snap, "_routine_lookback_days", lambda: 60)
    monkeypatch.setattr(_snap, "SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS", 3650,
                        raising=False)
    monkeypatch.setattr(_snap, "_sync_one_connection", sync_fn)


def test_holdings_sync_retries_until_success(monkeypatch):
    monkeypatch.setattr(webhooks, "_WEBHOOK_SYNC_MAX_ATTEMPTS", 3)
    calls = {"n": 0}

    def _flaky(user_id, acc_row, lookback_days=None, **_kw):
        calls["n"] += 1
        if calls["n"] < 3:
            return {"ok": False, "error": "transient"}
        return {"ok": True, "history_rows": 1, "current_rows": 1,
                "github_pushed": True}

    _wire_holdings_sync(monkeypatch, _flaky)
    webhooks._run_snaptrade_holdings_sync(9, "acc-1")
    assert calls["n"] == 3, "must retry an ok=false sync until it succeeds"


def test_holdings_sync_retries_on_exception(monkeypatch):
    monkeypatch.setattr(webhooks, "_WEBHOOK_SYNC_MAX_ATTEMPTS", 3)
    calls = {"n": 0}

    def _raises_then_ok(user_id, acc_row, lookback_days=None, **_kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return {"ok": True}

    _wire_holdings_sync(monkeypatch, _raises_then_ok)
    webhooks._run_snaptrade_holdings_sync(9, "acc-1")
    assert calls["n"] == 2, "a raised exception must also be retried"


def test_holdings_sync_stops_after_max_attempts(monkeypatch):
    monkeypatch.setattr(webhooks, "_WEBHOOK_SYNC_MAX_ATTEMPTS", 3)
    calls = {"n": 0}

    def _always_fail(user_id, acc_row, lookback_days=None, **_kw):
        calls["n"] += 1
        return {"ok": False, "error": "still down"}

    _wire_holdings_sync(monkeypatch, _always_fail)
    webhooks._run_snaptrade_holdings_sync(9, "acc-1")
    assert calls["n"] == 3, "must give up after _WEBHOOK_SYNC_MAX_ATTEMPTS (no infinite loop)"


# ---------------------------------------------------------------------------
# Weekend auto-sync is HISTORY-ONLY (suppress full-warehouse rebuilds on
# snapshot drift while markets are closed; still ingest Friday's T+1 fills).
# ---------------------------------------------------------------------------

def _capture_history_only(monkeypatch, *, weekend, first_done=True):
    from app import snaptrade as _snap
    seen = {}

    def _fake(user_id, acc_row, lookback_days=None, history_only=False, **_kw):
        seen["history_only"] = history_only
        return {"ok": True, "history_rows": 0, "current_rows": 0,
                "github_pushed": False}

    _wire_holdings_sync(monkeypatch, _fake)
    # _wire_holdings_sync stubs get_snaptrade_account → first_sync_completed True;
    # override for the first-sync case.
    monkeypatch.setattr(_models, "get_snaptrade_account",
                        lambda u, a: {"first_sync_completed": first_done})
    monkeypatch.setattr(_snap, "_market_closed_all_day", lambda: weekend)
    webhooks._run_snaptrade_holdings_sync(9, "acc-1")
    return seen


def test_weekend_auto_sync_is_history_only(monkeypatch):
    seen = _capture_history_only(monkeypatch, weekend=True)
    assert seen["history_only"] is True


def test_weekday_auto_sync_is_full(monkeypatch):
    seen = _capture_history_only(monkeypatch, weekend=False)
    assert seen["history_only"] is False


def test_weekend_first_sync_is_full(monkeypatch):
    # A brand-new account still needs its initial snapshot, even on a weekend.
    seen = _capture_history_only(monkeypatch, weekend=True, first_done=False)
    assert seen["history_only"] is False
