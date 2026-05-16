"""SnapTrade multi-account orchestration — unit tests.

Pins the per-row sync wrapper, bulk loop, first-sync lookback policy,
and the broken-connection flag handling. NO network calls (the
SnapTrade SDK is mocked); NO Postgres (model helpers are
monkey-patched).
"""
from __future__ import annotations

import os

# Tests import the app module which calls init_db on import unless
# this env var is set. Set it BEFORE any app.* import.
os.environ.setdefault("HAPPYTRADER_SKIP_DB_INIT", "1")

import pytest

from app import models as _models
from app import snaptrade as _snap
from app.schwab import _bulk_sync_lookback_days


# ---------------------------------------------------------------------------
# _stable_account_name — the warehouse tenancy label
# ---------------------------------------------------------------------------


def test_stable_account_name_uses_last_four_when_available():
    name = _snap._stable_account_name("FIDELITY", "X12345678")
    assert name == "Fidelity ••••5678"


def test_stable_account_name_falls_back_when_no_digits():
    name = _snap._stable_account_name("VANGUARD", "ABCD")
    assert name == "Vanguard Account"


def test_stable_account_name_handles_empty_broker():
    name = _snap._stable_account_name("", "12345678")
    assert name == "Broker ••••5678"


# ---------------------------------------------------------------------------
# _bulk_sync_lookback_days — re-used from Schwab; assert SnapTrade
# inherits the SAME first-sync-vs-routine semantics so both connectors'
# bulk loops behave identically per row.
# ---------------------------------------------------------------------------


def test_bulk_lookback_first_sync_pending_picks_full_history():
    assert _bulk_sync_lookback_days(
        first_done=False, force_full_history=False, routine_days=60, full_days=1825,
    ) == 1825


def test_bulk_lookback_first_sync_done_picks_routine():
    assert _bulk_sync_lookback_days(
        first_done=True, force_full_history=False, routine_days=60, full_days=1825,
    ) == 60


def test_bulk_lookback_force_full_overrides_first_done():
    assert _bulk_sync_lookback_days(
        first_done=True, force_full_history=True, routine_days=60, full_days=1825,
    ) == 1825


# ---------------------------------------------------------------------------
# _sync_one_connection — never raises; failures land structured
# ---------------------------------------------------------------------------


@pytest.fixture
def _patched_models(monkeypatch):
    """Stub every Postgres helper _sync_one_connection touches so the
    test can run without a DB."""
    record = {
        "first_sync_marked": [],
        "broken_marked": [],
        "broken_cleared": [],
        "sync_attempts": [],
    }

    monkeypatch.setattr(_snap, "mark_snaptrade_first_sync_completed",
                        lambda u, a: record["first_sync_marked"].append((u, a)))
    monkeypatch.setattr(_snap, "mark_snaptrade_connection_broken",
                        lambda u, a: record["broken_marked"].append((u, a)))
    monkeypatch.setattr(_snap, "clear_snaptrade_connection_broken",
                        lambda u, a: record["broken_cleared"].append((u, a)))

    def _record_attempt(u, a, *, error=None):
        record["sync_attempts"].append((u, a, error))
    monkeypatch.setattr(_snap, "record_snaptrade_sync_attempt", _record_attempt)

    return record


def test_sync_one_returns_session_expired_when_no_snap_user(monkeypatch, _patched_models):
    """If the user has no snaptrade_connections row (or the SDK can't
    init), the wrapper must return a structured failure rather than
    raise. The bulk loop relies on this so one bad row doesn't kill
    the whole sync."""
    monkeypatch.setattr(_snap, "get_snaptrade_user", lambda u: None)
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: None)

    res = _snap._sync_one_connection(
        user_id=9,
        acc_row={"snaptrade_account_id": "abc", "account_name": "X"},
        lookback_days=60,
    )
    assert res["ok"] is False
    assert res["error"] == "session_expired"
    assert _patched_models["sync_attempts"] == [(9, "abc", "session_expired")]


def test_sync_one_marks_connection_broken_on_auth_error(monkeypatch, _patched_models):
    """When the SDK raises an auth-shaped error (broker grant revoked),
    flip ``connection_broken_at`` so the banner surfaces and the cron
    stops retrying silently. Mirrors Schwab's
    ``mark_schwab_refresh_token_invalid``."""
    monkeypatch.setattr(_snap, "get_snaptrade_user",
                        lambda u: {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"})
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())

    def _boom(*args, **kwargs):
        raise _snap._SnapTradeAuthError(
            "get_user_account_positions", RuntimeError("401 Unauthorized"),
        )

    monkeypatch.setattr(_snap, "_run_sync", _boom)

    res = _snap._sync_one_connection(
        user_id=9,
        acc_row={"snaptrade_account_id": "abc", "account_name": "X"},
        lookback_days=60,
    )
    assert res["ok"] is False
    assert res["error"] == "connection_broken"
    assert _patched_models["broken_marked"] == [(9, "abc")]


def test_sync_one_clears_broken_flag_and_marks_first_sync_on_success(monkeypatch, _patched_models):
    """Successful sync flips both flags exactly once: clear any stale
    broken-connection state AND mark first_sync_completed so the next
    cron picks the routine lookback."""
    monkeypatch.setattr(_snap, "get_snaptrade_user",
                        lambda u: {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"})
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(_snap, "_run_sync", lambda *a, **k: {
        "history_rows": 12,
        "current_rows": 5,
        "lookback_days": 1825,
        "github_pushed": True,
        "github_error": None,
        "github_head_sha": "deadbeef",
        "github_seed_push_skipped": False,
        "github_skip_reason": None,
    })

    res = _snap._sync_one_connection(
        user_id=9,
        acc_row={"snaptrade_account_id": "abc", "account_name": "X"},
        lookback_days=1825,
    )
    assert res["ok"] is True
    assert res["history_rows"] == 12
    assert res["current_rows"] == 5
    assert res["github_head_sha"] == "deadbeef"
    assert _patched_models["first_sync_marked"] == [(9, "abc")]
    assert _patched_models["broken_cleared"] == [(9, "abc")]
    assert _patched_models["sync_attempts"] == [(9, "abc", None)]


def test_sync_one_unknown_error_records_string_truncated(monkeypatch, _patched_models):
    """Unexpected exceptions don't crash the loop and don't flip the
    broken flag (broker side may be fine; the bug is on us). The
    failure is recorded with a truncated string so the UI can show
    something."""
    monkeypatch.setattr(_snap, "get_snaptrade_user",
                        lambda u: {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"})
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(_snap, "_run_sync",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("X" * 1000)))

    res = _snap._sync_one_connection(
        user_id=9,
        acc_row={"snaptrade_account_id": "abc", "account_name": "X"},
        lookback_days=60,
    )
    assert res["ok"] is False
    assert res["error"] == "unknown"
    assert _patched_models["broken_marked"] == []  # NOT auth-shaped
    assert len(_patched_models["sync_attempts"]) == 1
    err = _patched_models["sync_attempts"][0][2]
    assert err is not None and len(err) <= 500


# ---------------------------------------------------------------------------
# _looks_like_auth_error — string-shape heuristic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("msg,expected", [
    ("401 Unauthorized", True),
    ("403 Forbidden", True),
    ("Authorization disabled by user", True),
    ("Authorization expired — please reconnect", True),
    ("AUTH REVOKED at the broker", True),
    ("Reconnect required", True),
    ("500 Internal Server Error", False),
    ("Network timeout", False),
    ("Account not found", False),
])
def test_looks_like_auth_error_pattern_matches(msg, expected):
    """Heuristic only — ANY new auth-shaped string SnapTrade ships
    needs to land in this list so the cron flips the broken flag and
    stops silently retrying."""
    assert _snap._looks_like_auth_error(RuntimeError(msg)) is expected


# ---------------------------------------------------------------------------
# Postgres helpers — pure SQL spies (no DB)
# ---------------------------------------------------------------------------


class _ExecuteSpy:
    def __init__(self):
        self.calls = []

    def __call__(self, sql, params=None):
        self.calls.append((sql, params))


def test_save_snaptrade_user_upserts_with_clear_semantics(monkeypatch):
    """``save_snaptrade_user`` must be a pure UPSERT (idempotent on
    re-register). Tests pin the SQL shape so a refactor that changes
    ON CONFLICT semantics can't quietly change re-register behaviour."""
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    _models.save_snaptrade_user(7, "snap-user-id", "snap-secret")

    assert len(spy.calls) == 1
    sql, params = spy.calls[0]
    assert "INSERT INTO snaptrade_connections" in sql
    assert "ON CONFLICT (user_id) DO UPDATE" in sql
    assert params == (7, "snap-user-id", "snap-secret")


def test_upsert_snaptrade_account_resets_broken_flag(monkeypatch):
    """A re-link of a previously-broken account (user reconnected
    through SnapTrade) must clear ``connection_broken_at`` so the
    banner dismisses on the next page load. Same ergonomic as
    ``save_schwab_connection`` clearing ``refresh_token_invalid_at``."""
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    _models.upsert_snaptrade_account(
        7, "abc",
        broker_slug="FIDELITY",
        account_number_masked="****1234",
        account_name="Fidelity ••••1234",
    )

    sql, _ = spy.calls[0]
    assert "connection_broken_at  = NULL" in sql or "connection_broken_at = NULL" in sql.replace("  ", " ")


def test_get_snaptrade_accounts_short_circuits_for_none_user(monkeypatch):
    """Anonymous request must NOT query the DB."""
    called = {"n": 0}

    def _spy(sql, params):
        called["n"] += 1
        return []

    monkeypatch.setattr(_models, "fetch_all", _spy)
    assert _models.get_snaptrade_accounts(None) == []
    assert called["n"] == 0


def test_get_expired_snaptrade_accounts_swallows_db_errors(monkeypatch):
    """Banner inject context-processor must not blow up the page on a
    transient DB hiccup. Same defensiveness as the Schwab
    equivalent."""
    def _boom(sql, params):
        raise RuntimeError("db down")

    monkeypatch.setattr(_models, "fetch_all", _boom)
    assert _models.get_expired_snaptrade_accounts(7) == []


def test_get_snaptrade_account_nicknames_falls_back_to_account_name(monkeypatch):
    monkeypatch.setattr(
        _models, "fetch_all",
        lambda sql, params: [
            {"account_name": "Fidelity ••••1234", "display_nickname": None},
            {"account_name": "Vanguard ••••5678", "display_nickname": "  "},
            {"account_name": "Robinhood Account", "display_nickname": "Trading"},
        ],
    )
    out = _models.get_snaptrade_account_nicknames(7)
    assert out == {
        "Fidelity ••••1234": "Fidelity ••••1234",
        "Vanguard ••••5678": "Vanguard ••••5678",
        "Robinhood Account": "Trading",
    }


def test_record_sync_attempt_updates_last_at_and_error(monkeypatch):
    """Tests the SQL contract ``record_snaptrade_sync_attempt`` writes:
    must SET both ``last_sync_at`` (always) and ``last_sync_error``
    (NULL on success, message on failure)."""
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    _models.record_snaptrade_sync_attempt(7, "abc", error=None)
    _models.record_snaptrade_sync_attempt(7, "abc", error="boom")

    assert len(spy.calls) == 2
    for sql, _ in spy.calls:
        assert "SET last_sync_at = NOW()" in sql
        assert "last_sync_error = %s" in sql
    assert spy.calls[0][1] == (None, 7, "abc")
    assert spy.calls[1][1] == ("boom", 7, "abc")


# ---------------------------------------------------------------------------
# Force-refresh throttle — must NOT call SnapTrade's billed endpoint
# when the user pressed Refresh-from-broker recently. SnapTrade charges
# per call so a rogue caller pinning the button could rack up our bill
# in seconds without this gate.
# ---------------------------------------------------------------------------


def test_force_refresh_throttles_when_last_refresh_recent(monkeypatch):
    """Pressing Refresh-from-broker within the throttle window MUST NOT
    call SnapTrade's billed ``refresh_brokerage_authorization`` endpoint.
    The route should return ``ok=False`` with a user-friendly message
    that mentions the wait time, and the SDK must NOT be called at all.
    """
    from datetime import datetime, timezone, timedelta

    # Simulate "user just pressed it 30 seconds ago" — well inside any
    # reasonable throttle window.
    fake_acc = {
        "snaptrade_account_id": "acc-id-1",
        "first_sync_completed": True,
        "brokerage_authorization_id": "auth-1",
        "last_force_refresh_at": datetime.now(timezone.utc) - timedelta(seconds=30),
    }
    monkeypatch.setattr(_snap, "_snaptrade_config", lambda: ("cid", "ckey", "redir"))
    monkeypatch.setattr(_snap, "get_snaptrade_user", lambda uid: {
        "snaptrade_user_id": "u", "snaptrade_secret": "s",
    })
    monkeypatch.setattr(_snap, "get_snaptrade_account", lambda uid, aid: fake_acc)

    sdk_call_count = {"refresh": 0, "details": 0}

    class _MockClient:
        class connections:
            @staticmethod
            def refresh_brokerage_authorization(**kwargs):
                sdk_call_count["refresh"] += 1
                return {"detail": "Connection scheduled for refresh"}

        class account_information:
            @staticmethod
            def get_user_account_details(**kwargs):
                sdk_call_count["details"] += 1
                return type("R", (), {"body": {"brokerage_authorization": "auth-1"}})()

    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: _MockClient())

    ok, message, throttle_remaining = _snap._force_refresh_brokerage(
        user_id=7, snaptrade_account_id="acc-id-1", throttle_seconds=600,
    )

    assert ok is False, "Throttled refresh must return ok=False"
    assert sdk_call_count["refresh"] == 0, \
        "Throttle must NOT call SnapTrade's billed refresh endpoint"
    assert sdk_call_count["details"] == 0, \
        "Throttle must short-circuit BEFORE any SnapTrade call (no auth-id lookup either)"
    assert throttle_remaining is not None and 500 <= throttle_remaining <= 600, \
        f"Should report ~570s remaining, got {throttle_remaining}"
    assert "minute" in message.lower(), \
        f"User-facing message should mention wait time; got: {message!r}"


def test_force_refresh_calls_sdk_when_throttle_window_elapsed(monkeypatch):
    """Once the throttle window has fully elapsed, the next press
    SHOULD call SnapTrade and stamp ``last_force_refresh_at = NOW()``
    so the next-next press is throttled fresh.
    """
    from datetime import datetime, timezone, timedelta

    fake_acc = {
        "snaptrade_account_id": "acc-id-2",
        "first_sync_completed": True,
        "brokerage_authorization_id": "auth-2",
        "last_force_refresh_at": datetime.now(timezone.utc) - timedelta(hours=1),
    }
    monkeypatch.setattr(_snap, "_snaptrade_config", lambda: ("cid", "ckey", "redir"))
    monkeypatch.setattr(_snap, "get_snaptrade_user", lambda uid: {
        "snaptrade_user_id": "u", "snaptrade_secret": "s",
    })
    monkeypatch.setattr(_snap, "get_snaptrade_account", lambda uid, aid: fake_acc)

    sdk_call_count = {"refresh": 0, "details": 0}
    stamp_calls = []

    class _MockClient:
        class connections:
            @staticmethod
            def refresh_brokerage_authorization(**kwargs):
                sdk_call_count["refresh"] += 1
                assert kwargs.get("authorization_id") == "auth-2"
                return {"detail": "Connection scheduled for refresh"}

        class account_information:
            @staticmethod
            def get_user_account_details(**kwargs):
                sdk_call_count["details"] += 1
                return type("R", (), {"body": {"brokerage_authorization": "auth-2"}})()

    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: _MockClient())
    monkeypatch.setattr(
        _snap, "stamp_snaptrade_force_refresh_attempt",
        lambda uid, aid: stamp_calls.append((uid, aid)),
    )

    ok, message, _ = _snap._force_refresh_brokerage(
        user_id=7, snaptrade_account_id="acc-id-2", throttle_seconds=600,
    )

    assert ok is True, f"Refresh past throttle should succeed; got: {message!r}"
    assert sdk_call_count["refresh"] == 1, "Refresh endpoint should be called exactly once"
    assert sdk_call_count["details"] == 0, \
        "Cached brokerage_authorization_id must be re-used (no extra detail call)"
    assert stamp_calls == [(7, "acc-id-2")], \
        "Must stamp last_force_refresh_at on success for the next throttle window"


def test_force_refresh_caches_authorization_id_on_first_call(monkeypatch):
    """If the brokerage_authorization_id is not yet cached on the row,
    the first refresh fetches it via ``get_user_account_details`` and
    persists it to Postgres so subsequent refreshes skip the lookup."""
    fake_acc = {
        "snaptrade_account_id": "acc-id-3",
        "first_sync_completed": True,
        "brokerage_authorization_id": None,  # not cached yet
        "last_force_refresh_at": None,        # never refreshed
    }
    monkeypatch.setattr(_snap, "_snaptrade_config", lambda: ("cid", "ckey", "redir"))
    monkeypatch.setattr(_snap, "get_snaptrade_user", lambda uid: {
        "snaptrade_user_id": "u", "snaptrade_secret": "s",
    })
    monkeypatch.setattr(_snap, "get_snaptrade_account", lambda uid, aid: fake_acc)

    set_calls = []
    monkeypatch.setattr(
        _snap, "set_snaptrade_brokerage_authorization_id",
        lambda uid, aid, auth: set_calls.append((uid, aid, auth)),
    )
    monkeypatch.setattr(_snap, "stamp_snaptrade_force_refresh_attempt", lambda uid, aid: None)

    refresh_kwargs = {}

    class _MockClient:
        class connections:
            @staticmethod
            def refresh_brokerage_authorization(**kwargs):
                refresh_kwargs.update(kwargs)
                return {"detail": "Connection scheduled for refresh"}

        class account_information:
            @staticmethod
            def get_user_account_details(**kwargs):
                return type("R", (), {"body": {
                    "brokerage_authorization": "newly-discovered-auth-uuid",
                }})()

    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: _MockClient())

    ok, _msg, _ = _snap._force_refresh_brokerage(
        user_id=7, snaptrade_account_id="acc-id-3",
    )
    assert ok is True
    # Auth ID must be (a) used in the refresh call, (b) cached for next time.
    assert refresh_kwargs.get("authorization_id") == "newly-discovered-auth-uuid"
    assert set_calls == [(7, "acc-id-3", "newly-discovered-auth-uuid")], \
        "Must cache the discovered authorization_id so subsequent presses skip the lookup"


def test_force_refresh_returns_friendly_error_on_unconfigured_server(monkeypatch):
    """If SnapTrade env vars aren't set, the route should NOT raise —
    it should return a user-friendly message. Mirrors how /snaptrade/sync
    behaves on the same condition."""
    monkeypatch.setattr(_snap, "_snaptrade_config", lambda: None)
    ok, message, _ = _snap._force_refresh_brokerage(user_id=7, snaptrade_account_id="x")
    assert ok is False
    assert "not configured" in message.lower()


def test_stamp_force_refresh_attempt_writes_correct_sql(monkeypatch):
    """Pin the SQL ``stamp_snaptrade_force_refresh_attempt`` writes —
    must SET ``last_force_refresh_at = NOW()`` and ``updated_at = NOW()``
    so the throttle gate downstream reads the right column."""
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    _models.stamp_snaptrade_force_refresh_attempt(7, "acc-1")

    assert len(spy.calls) == 1
    sql, params = spy.calls[0]
    assert "SET last_force_refresh_at = NOW()" in sql
    assert "updated_at = NOW()" in sql
    assert params == (7, "acc-1")


def test_set_brokerage_authorization_id_writes_correct_sql(monkeypatch):
    """Pin the SQL contract for the auth-id cache writer."""
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    ok = _models.set_snaptrade_brokerage_authorization_id(7, "acc-1", "auth-uuid-xyz")
    assert ok is True
    assert len(spy.calls) == 1
    sql, params = spy.calls[0]
    assert "SET brokerage_authorization_id = %s" in sql
    assert "updated_at = NOW()" in sql
    assert params == ("auth-uuid-xyz", 7, "acc-1")


def test_set_brokerage_authorization_id_short_circuits_on_empty_input():
    """Empty/None auth ids must NOT issue a write — those would NULL
    out a previously-cached value and force a re-lookup on every
    subsequent refresh, defeating the whole point of caching."""
    assert _models.set_snaptrade_brokerage_authorization_id(7, "acc-1", None) is False
    assert _models.set_snaptrade_brokerage_authorization_id(7, "acc-1", "") is False
    assert _models.set_snaptrade_brokerage_authorization_id(7, "acc-1", "   ") is False
