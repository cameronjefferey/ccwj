"""Reverse-trial plan machinery (app/plan.py + app/plan_lifecycle_cli.py).

Pins:
- plan-state derivation at every boundary (day 29/30/59/60), grandfathering,
  and the admin/demo exemption
- the sync gate refusing frozen users at the _sync_one_connection chokepoint
  WITHOUT recording a sync attempt (a frozen skip must never look like a
  broken connection)
- plan_block_writes flash+redirect / JSON 403 behavior
- start_trial_clock's once-only, trial-plan-only SQL guard
- lifecycle CLI milestone mapping, email dedupe, and the state-driven
  day-60 disconnect (ordering: authorizations → SnapTrade user → local rows
  → broker_tenants marked disconnected but KEPT)
"""
import types
from datetime import datetime, timedelta, timezone

import pytest

import app.plan as plan_mod
import app.plan_lifecycle_cli as cli_mod
from app.plan import (
    GRACE_DAYS,
    STATE_ACTIVE,
    STATE_BETA,
    STATE_FROZEN,
    STATE_GRACE_EXPIRED,
    STATE_NO_DATA,
    STATE_TRIALING,
    TRIAL_DAYS,
    derive_plan_state,
)

NOW = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)


def _started(days_ago):
    return NOW - timedelta(days=days_ago)


# ---------------------------------------------------------------------------
# Derivation boundaries
# ---------------------------------------------------------------------------


def test_no_clock_is_no_data():
    assert derive_plan_state("trial", None, now=NOW) == STATE_NO_DATA


@pytest.mark.parametrize("days,expected", [
    (0, STATE_TRIALING),
    (TRIAL_DAYS - 1, STATE_TRIALING),                 # day 29
    (TRIAL_DAYS, STATE_FROZEN),                       # day 30
    (TRIAL_DAYS + GRACE_DAYS - 1, STATE_FROZEN),      # day 59
    (TRIAL_DAYS + GRACE_DAYS, STATE_GRACE_EXPIRED),   # day 60
    (400, STATE_GRACE_EXPIRED),
])
def test_trial_clock_boundaries(days, expected):
    assert derive_plan_state("trial", _started(days), now=NOW) == expected


def test_beta_and_active_never_freeze():
    old = _started(400)
    assert derive_plan_state("beta", old, now=NOW) == STATE_BETA
    assert derive_plan_state("active", old, now=NOW) == STATE_ACTIVE


def test_exempt_overrides_everything():
    assert derive_plan_state("trial", _started(400), exempt=True, now=NOW) == STATE_BETA


def test_missing_plan_defaults_to_trial():
    assert derive_plan_state(None, _started(5), now=NOW) == STATE_TRIALING
    assert derive_plan_state("", None, now=NOW) == STATE_NO_DATA


def test_day_60_disconnected_tenants_remain_readable(monkeypatch):
    """Cleanup keeps the mirror, so read scoping must keep its tenant IDs."""
    import app.models as models_mod

    queries = []

    def _fetch_all(sql, params):
        queries.append((sql, params))
        return [{"tenant_id": "snaptrade:kept"}]

    monkeypatch.setattr(models_mod, "fetch_all", _fetch_all)

    assert models_mod.get_tenant_ids_for_user(7) == ["snaptrade:kept"]
    assert models_mod.get_broker_tenants_for_user(7) == [
        {"tenant_id": "snaptrade:kept"}
    ]
    assert len(queries) == 2
    for sql, params in queries:
        assert "connection_status IN ('active', 'disconnected')" in sql
        assert params == (7,)


def test_plan_state_fails_open_when_row_missing(monkeypatch):
    monkeypatch.setattr(plan_mod, "get_user_plan_row", lambda uid: None)
    assert plan_mod.plan_state(123) == STATE_BETA
    assert plan_mod.user_sync_allowed(123) is True


def test_user_sync_allowed_states(monkeypatch):
    for state, allowed in [
        (STATE_NO_DATA, True), (STATE_TRIALING, True), (STATE_BETA, True),
        (STATE_ACTIVE, True), (STATE_FROZEN, False), (STATE_GRACE_EXPIRED, False),
    ]:
        monkeypatch.setattr(plan_mod, "plan_state", lambda uid, s=state: s)
        assert plan_mod.user_sync_allowed(1) is allowed, state


# ---------------------------------------------------------------------------
# start_trial_clock — once-only, trial-plan-only, by SQL construction
# ---------------------------------------------------------------------------


def test_start_trial_clock_sql_guard(monkeypatch):
    captured = {}

    def fake_returning(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return None

    monkeypatch.setattr(plan_mod, "execute_returning", fake_returning)
    plan_mod.start_trial_clock(42)
    assert "trial_started_at IS NULL" in captured["sql"]
    assert "plan = %s" in captured["sql"]
    assert "RETURNING username" in captured["sql"]
    assert captured["params"] == (42, "trial")


def test_set_user_plan_rejects_unknown(monkeypatch):
    monkeypatch.setattr(plan_mod, "execute", lambda *a, **k: None)
    assert plan_mod.set_user_plan(1, "platinum") is False
    assert plan_mod.set_user_plan(1, "active") is True


# ---------------------------------------------------------------------------
# plan_status_for_banner
# ---------------------------------------------------------------------------


def test_banner_none_for_beta_active_no_data(monkeypatch):
    for plan, ts in [("beta", _started(400)), ("active", _started(400)), ("trial", None)]:
        monkeypatch.setattr(
            plan_mod, "get_user_plan_row",
            lambda uid, p=plan, t=ts: {"plan": p, "trial_started_at": t, "username": "u"},
        )
        monkeypatch.setattr(plan_mod, "_is_exempt_username", lambda u: False)
        assert plan_mod.plan_status_for_banner(1, now=NOW) is None


def test_banner_shapes(monkeypatch):
    monkeypatch.setattr(plan_mod, "_is_exempt_username", lambda u: False)

    monkeypatch.setattr(
        plan_mod, "get_user_plan_row",
        lambda uid: {"plan": "trial", "trial_started_at": _started(10), "username": "u"},
    )
    b = plan_mod.plan_status_for_banner(1, now=NOW)
    assert b["state"] == STATE_TRIALING
    assert b["days_left"] == TRIAL_DAYS - 10

    monkeypatch.setattr(
        plan_mod, "get_user_plan_row",
        lambda uid: {"plan": "trial", "trial_started_at": _started(35), "username": "u"},
    )
    b = plan_mod.plan_status_for_banner(1, now=NOW)
    assert b["state"] == STATE_FROZEN
    assert b["disconnect_in_days"] == TRIAL_DAYS + GRACE_DAYS - 35
    assert b["frozen_on"] == (_started(35) + timedelta(days=TRIAL_DAYS)).date()


# ---------------------------------------------------------------------------
# plan_block_writes — route-level gate
# ---------------------------------------------------------------------------


def _fake_user(uid=1, authenticated=True):
    return types.SimpleNamespace(is_authenticated=authenticated, id=uid)


def test_plan_block_writes_passes_for_active_states(monkeypatch):
    from app import app as flask_app

    monkeypatch.setattr(plan_mod, "current_user", _fake_user())
    for state in (STATE_NO_DATA, STATE_TRIALING, STATE_BETA, STATE_ACTIVE):
        monkeypatch.setattr(plan_mod, "plan_state", lambda uid, s=state: s)
        with flask_app.test_request_context("/upload", method="POST"):
            assert plan_mod.plan_block_writes("testing") is None


def test_plan_block_writes_blocks_frozen_html(monkeypatch):
    from app import app as flask_app

    monkeypatch.setattr(plan_mod, "current_user", _fake_user())
    monkeypatch.setattr(plan_mod, "plan_state", lambda uid: STATE_FROZEN)
    with flask_app.test_request_context("/upload", method="POST"):
        resp = plan_mod.plan_block_writes("uploading new trade data")
        assert resp is not None
        assert resp.status_code == 302
        assert "/pricing" in resp.headers["Location"]


def test_plan_block_writes_blocks_frozen_json(monkeypatch):
    from app import app as flask_app

    monkeypatch.setattr(plan_mod, "current_user", _fake_user())
    monkeypatch.setattr(plan_mod, "plan_state", lambda uid: STATE_GRACE_EXPIRED)
    with flask_app.test_request_context(
        "/api/something", method="POST",
        headers={"X-Requested-With": "XMLHttpRequest"},
    ):
        resp, status = plan_mod.plan_block_writes("testing")
        assert status == 403
        assert resp.get_json()["error"] == "plan_frozen"


def test_plan_block_writes_ignores_anonymous(monkeypatch):
    from app import app as flask_app

    monkeypatch.setattr(plan_mod, "current_user", _fake_user(authenticated=False))
    with flask_app.test_request_context("/upload", method="POST"):
        assert plan_mod.plan_block_writes("testing") is None


# ---------------------------------------------------------------------------
# _sync_one_connection chokepoint
# ---------------------------------------------------------------------------


def test_sync_one_connection_refuses_frozen_user(monkeypatch):
    import app.snaptrade as snaptrade_mod

    monkeypatch.setattr(plan_mod, "user_sync_allowed", lambda uid: False)

    def _must_not_be_called(*a, **k):
        raise AssertionError("frozen user must not reach SnapTrade")

    monkeypatch.setattr(snaptrade_mod, "get_snaptrade_user", _must_not_be_called)
    monkeypatch.setattr(
        snaptrade_mod, "record_snaptrade_sync_attempt", _must_not_be_called,
    )

    out = snaptrade_mod._sync_one_connection(
        7,
        {"snaptrade_account_id": "acc-1", "account_name": "Schwab Account",
         "display_nickname": None, "first_sync_completed": True},
        lookback_days=30,
    )
    assert out["ok"] is False
    assert out["error"] == "plan_frozen"


# ---------------------------------------------------------------------------
# Lifecycle CLI
# ---------------------------------------------------------------------------


class _EmailLog:
    def __init__(self):
        self.sends = []       # (kind, dedupe_key)
        self.emails = []      # (fn_name, kwargs)
        self.seen_keys = set()

    def record(self, kind, dedupe_key, **kw):
        key = (kind, dedupe_key)
        if key in self.seen_keys:
            return False
        self.seen_keys.add(key)
        self.sends.append(key)
        return True

    def sender(self, name):
        def _send(**kwargs):
            self.emails.append((name, kwargs))
        return _send


@pytest.fixture
def lifecycle_env(monkeypatch):
    log = _EmailLog()
    import app.models as models_mod
    import app.email as email_mod

    monkeypatch.setattr(models_mod, "record_email_send", log.record)
    monkeypatch.setattr(email_mod, "app_base_url", lambda: "http://test")
    for fn in ("send_trial_week_left_email", "send_trial_frozen_email",
               "send_disconnect_warning_email", "send_disconnected_email"):
        monkeypatch.setattr(email_mod, fn, log.sender(fn))
    # No SnapTrade rows by default → no disconnect action
    monkeypatch.setattr(models_mod, "get_snaptrade_accounts", lambda uid: [])
    monkeypatch.setattr(
        cli_mod, "disconnect_user_brokerages",
        lambda uid: (_ for _ in ()).throw(AssertionError("unexpected disconnect")),
    )
    return log


def _rec(uid, days, email="u@example.com"):
    return {
        "user_id": uid, "username": f"user{uid}", "email": email,
        "trial_started_at": _started(days),
    }


def test_lifecycle_day_25_sends_week_left_once(monkeypatch, lifecycle_env):
    monkeypatch.setattr(cli_mod, "_list_running_trials", lambda: [_rec(1, 25)])
    counts = cli_mod.run_plan_lifecycle(now=NOW)
    assert counts["week_left"] == 1
    assert [e[0] for e in lifecycle_env.emails] == ["send_trial_week_left_email"]
    # second daily run: deduped
    counts = cli_mod.run_plan_lifecycle(now=NOW)
    assert counts["week_left"] == 0
    assert len(lifecycle_env.emails) == 1


def test_lifecycle_day_35_sends_frozen_not_week_left(monkeypatch, lifecycle_env):
    monkeypatch.setattr(cli_mod, "_list_running_trials", lambda: [_rec(2, 35)])
    counts = cli_mod.run_plan_lifecycle(now=NOW)
    assert counts["frozen"] == 1
    assert counts["week_left"] == 0
    assert counts["warning"] == 0


def test_lifecycle_day_54_sends_warning(monkeypatch, lifecycle_env):
    monkeypatch.setattr(cli_mod, "_list_running_trials", lambda: [_rec(3, 54)])
    counts = cli_mod.run_plan_lifecycle(now=NOW)
    assert counts["warning"] == 1


def test_lifecycle_day_60_disconnects_when_rows_exist(monkeypatch, lifecycle_env):
    import app.models as models_mod

    monkeypatch.setattr(cli_mod, "_list_running_trials", lambda: [_rec(4, 60)])
    monkeypatch.setattr(
        models_mod, "get_snaptrade_accounts",
        lambda uid: [{"snaptrade_account_id": "acc-1"}],
    )
    disconnected = []
    monkeypatch.setattr(
        cli_mod, "disconnect_user_brokerages",
        lambda uid: disconnected.append(uid) or 1,
    )
    counts = cli_mod.run_plan_lifecycle(now=NOW)
    assert disconnected == [4]
    assert counts["disconnected"] == 1
    assert ("plan_disconnected", f"4:{_started(60).date().isoformat()}") in lifecycle_env.seen_keys


def test_lifecycle_retries_when_remote_disconnect_is_unconfirmed(
    monkeypatch, lifecycle_env,
):
    import app.models as models_mod

    monkeypatch.setattr(cli_mod, "_list_running_trials", lambda: [_rec(4, 60)])
    monkeypatch.setattr(
        models_mod, "get_snaptrade_accounts",
        lambda uid: [{"snaptrade_account_id": "acc-1"}],
    )
    attempts = []
    monkeypatch.setattr(
        cli_mod, "disconnect_user_brokerages",
        lambda uid: attempts.append(uid) or None,
    )

    counts = cli_mod.run_plan_lifecycle(now=NOW)

    assert attempts == [4]
    assert counts["disconnected"] == 0
    assert not any(e[0] == "send_disconnected_email" for e in lifecycle_env.emails)


def test_lifecycle_day_60_csv_only_user_no_disconnect_email(monkeypatch, lifecycle_env):
    monkeypatch.setattr(cli_mod, "_list_running_trials", lambda: [_rec(5, 61)])
    counts = cli_mod.run_plan_lifecycle(now=NOW)
    assert counts["disconnected"] == 0
    assert not any(e[0] == "send_disconnected_email" for e in lifecycle_env.emails)


def test_lifecycle_no_email_user_still_counts_but_sends_nothing(monkeypatch, lifecycle_env):
    monkeypatch.setattr(cli_mod, "_list_running_trials", lambda: [_rec(6, 25, email="")])
    counts = cli_mod.run_plan_lifecycle(now=NOW)
    assert counts["week_left"] == 0
    assert lifecycle_env.emails == []


# ---------------------------------------------------------------------------
# Disconnect ordering
# ---------------------------------------------------------------------------


def test_disconnect_ordering_and_tenant_rows_kept(monkeypatch):
    import app.models as models_mod
    import app.snaptrade as snaptrade_mod

    calls = []

    class _FakeConnections:
        def remove_brokerage_authorization(self, **kw):
            calls.append(("remove_auth", kw["authorization_id"]))

    class _FakeAuth:
        def delete_snap_trade_user(self, **kw):
            calls.append(("delete_user", kw["user_id"]))

    fake_client = types.SimpleNamespace(
        connections=_FakeConnections(), authentication=_FakeAuth(),
    )

    monkeypatch.setattr(
        models_mod, "get_snaptrade_accounts",
        lambda uid: [
            {"snaptrade_account_id": "acc-1", "brokerage_authorization_id": "auth-1"},
            {"snaptrade_account_id": "acc-2", "brokerage_authorization_id": None},
        ],
    )
    monkeypatch.setattr(
        models_mod, "get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "st-user", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(snaptrade_mod, "_get_snaptrade_client", lambda: fake_client)
    monkeypatch.setattr(
        models_mod, "remove_snaptrade_account",
        lambda uid, acc: calls.append(("remove_local", acc)),
    )
    monkeypatch.setattr(
        models_mod, "remove_snaptrade_user",
        lambda uid: calls.append(("remove_connection_row", uid)),
    )
    monkeypatch.setattr(
        models_mod, "mark_broker_tenants_disconnected",
        lambda uid: calls.append(("mark_tenants", uid)) or True,
    )

    removed = cli_mod.disconnect_user_brokerages(9)
    assert removed == 2
    # Authorization removal uses the cached auth id, falling back to acct id
    assert calls[0] == ("remove_auth", "auth-1")
    assert calls[1] == ("remove_auth", "acc-2")
    # SnapTrade user delete AFTER authorizations, BEFORE local cleanup
    assert calls[2] == ("delete_user", "st-user")
    assert ("remove_local", "acc-1") in calls and ("remove_local", "acc-2") in calls
    assert calls[-2] == ("remove_connection_row", 9)
    # broker_tenants rows marked disconnected but NEVER deleted
    assert calls[-1] == ("mark_tenants", 9)


def test_disconnect_preserves_local_rows_when_remote_delete_fails(monkeypatch):
    import app.models as models_mod
    import app.snaptrade as snaptrade_mod

    calls = []

    class _FakeConnections:
        def remove_brokerage_authorization(self, **kw):
            calls.append(("remove_auth", kw["authorization_id"]))

    class _FakeAuth:
        def delete_snap_trade_user(self, **kw):
            calls.append(("delete_user", kw["user_id"]))
            raise RuntimeError("temporary SnapTrade outage")

    fake_client = types.SimpleNamespace(
        connections=_FakeConnections(), authentication=_FakeAuth(),
    )
    monkeypatch.setattr(
        models_mod, "get_snaptrade_accounts",
        lambda uid: [
            {"snaptrade_account_id": "acc-1", "brokerage_authorization_id": "auth-1"},
        ],
    )
    monkeypatch.setattr(
        models_mod, "get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "st-user", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(snaptrade_mod, "_get_snaptrade_client", lambda: fake_client)

    def _must_preserve(*args, **kwargs):
        raise AssertionError("local state must remain until remote deletion succeeds")

    monkeypatch.setattr(models_mod, "remove_snaptrade_account", _must_preserve)
    monkeypatch.setattr(models_mod, "remove_snaptrade_user", _must_preserve)
    monkeypatch.setattr(models_mod, "mark_broker_tenants_disconnected", _must_preserve)

    assert cli_mod.disconnect_user_brokerages(9) is None
    assert calls == [("remove_auth", "auth-1"), ("delete_user", "st-user")]


def test_disconnect_preserves_local_rows_when_client_is_unavailable(monkeypatch):
    import app.models as models_mod
    import app.snaptrade as snaptrade_mod

    monkeypatch.setattr(
        models_mod, "get_snaptrade_accounts",
        lambda uid: [
            {"snaptrade_account_id": "acc-1", "brokerage_authorization_id": "auth-1"},
        ],
    )
    monkeypatch.setattr(
        models_mod, "get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "st-user", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(snaptrade_mod, "_get_snaptrade_client", lambda: None)

    def _must_preserve(*args, **kwargs):
        raise AssertionError("local state must remain while SnapTrade is unavailable")

    monkeypatch.setattr(models_mod, "remove_snaptrade_account", _must_preserve)
    monkeypatch.setattr(models_mod, "remove_snaptrade_user", _must_preserve)
    monkeypatch.setattr(models_mod, "mark_broker_tenants_disconnected", _must_preserve)

    assert cli_mod.disconnect_user_brokerages(9) is None
