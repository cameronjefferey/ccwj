"""Settings, onboarding, and connect-broker audit (Oct 2026).

Pins the logic fixes from that pass: cash-only accounts leave the
still-syncing screen, broker chips collapse Schwab names, sync flashes
never echo a stack trace, and a nickname or group save does not claim
success or leak an internal error for an account the login does not own.
"""
from datetime import date

from flask import get_flashed_messages, render_template

from app import app
from app.linked_accounts import sync_status_label
from app.marketing import onboarding_warehouse_ready
from app.models import User
from app.profile_page import settings_header_counts
from app.snaptrade import public_sync_failure_reason


class _SessionUser:
    is_active = True
    is_anonymous = False

    def __init__(self, username="ada", user_id=7):
        self.id = user_id
        self.username = username

    @property
    def is_authenticated(self):
        return True

    def get_id(self):
        return str(self.id)


def _login(client, monkeypatch, username="ada"):
    user = _SessionUser(username)

    def get_by_id(user_id):
        if str(user_id) == str(user.id):
            return user
        return None

    monkeypatch.setattr(User, "get_by_id", staticmethod(get_by_id))
    with client.session_transaction() as sess:
        sess["_user_id"] = str(user.id)
        sess["_fresh"] = True
    return user


def _flashes(client):
    with client.session_transaction() as sess:
        return list(sess.get("_flashes") or [])


def test_sync_status_label_never_echoes_the_stored_error():
    raw = "Traceback (most recent call last):\nValueError: boom"
    assert sync_status_label({
        "last_sync_error": raw,
        "first_sync_completed": True,
    }) == "Last sync didn't finish"
    assert sync_status_label({
        "connection_broken_at": "2026-10-01",
        "last_sync_error": raw,
    }) == "Reconnect needed"
    assert sync_status_label({
        "last_sync_error": "connection_broken:activities",
        "first_sync_completed": True,
    }) == "Reconnect needed"
    assert sync_status_label({
        "last_sync_error": "connection_broken_pending:orders",
        "first_sync_completed": True,
    }) is None
    assert sync_status_label({
        "first_sync_completed": False,
    }) == "Waiting for the first sync"
    assert sync_status_label({
        "first_sync_completed": True,
    }) is None
    assert raw not in (sync_status_label({"last_sync_error": raw}) or "")


def test_public_sync_failure_reason_hides_traces_and_maps_codes():
    assert public_sync_failure_reason(
        "connection_broken",
        "SnapTrade timed out while reading your trade history. Try again in a minute.",
    ).startswith("SnapTrade timed out")
    assert public_sync_failure_reason(
        "connection_broken", "Traceback (most recent call last):\nboom",
    ) == "reconnect the broker first"
    long_exc = "x" * 300
    assert public_sync_failure_reason(long_exc) == (
        "the sync didn't finish. Try again in a minute"
    )
    assert "Traceback" not in public_sync_failure_reason(
        "unknown", "Traceback: secret",
    )
    assert public_sync_failure_reason("session_expired") == (
        "the sign-in session expired. Connect again"
    )
    assert public_sync_failure_reason("plan_frozen") == (
        "updates are paused until the plan is active"
    )
    assert public_sync_failure_reason(None, "line one\nline two") == (
        "the sync didn't finish. Try again in a minute"
    )


def test_settings_header_collapses_schwab_and_counts_manuals():
    snap = [
        {"broker_slug": "Schwab"},
        {"broker_slug": "Charles Schwab"},
    ]
    profile_rows = [
        {"tenant_id": "snaptrade:a"},
        {"tenant_id": "snaptrade:b"},
        {"tenant_id": "manual:manual:7:csv"},
    ]
    accounts, brokers = settings_header_counts(
        snap, profile_rows, ["phantom"], [{"tenant_id": "extra"}],
    )
    assert accounts == 3
    assert brokers == 1

    fallback_accounts, fallback_brokers = settings_header_counts(
        [], [], ["CSV only"], [{"tenant_id": "t1"}, {"tenant_id": "t2"}],
    )
    assert fallback_accounts == 2
    assert fallback_brokers == 0


def test_onboarding_warehouse_ready_accepts_a_balance_only_tenant(monkeypatch):
    monkeypatch.setattr(
        "app.cache_ops.warehouse_tenants_present",
        lambda ids: {"snaptrade:cash"},
    )
    assert onboarding_warehouse_ready(["snaptrade:cash"]) is True
    assert onboarding_warehouse_ready([]) is False
    assert onboarding_warehouse_ready([None, ""]) is False

    def boom(ids):
        raise RuntimeError("bq down")

    monkeypatch.setattr("app.cache_ops.warehouse_tenants_present", boom)
    assert onboarding_warehouse_ready(["snaptrade:cash"]) is False


def test_cash_only_account_leaves_the_still_syncing_screen(monkeypatch):
    monkeypatch.setattr(
        "app.marketing.get_tenant_ids_for_user",
        lambda uid: ["snaptrade:cash"],
    )
    monkeypatch.setattr("app.snaptrade.snaptrade_enabled", lambda: True)
    monkeypatch.setattr(
        "app.models.get_snaptrade_accounts",
        lambda uid: [{"snaptrade_account_id": "cash-1"}],
    )
    monkeypatch.setattr(
        "app.cache_ops.warehouse_tenants_present",
        lambda ids: {"snaptrade:cash"},
    )
    monkeypatch.setattr(
        "app.first_look.render_first_look_view", lambda: None,
    )

    client = app.test_client()
    _login(client, monkeypatch)
    html = client.get("/get-started").get_data(as_text=True)
    assert "This account is in Overview." in html
    assert "Open Overview" in html
    assert "first sync is still running" not in html
    assert "Nothing has landed in the warehouse" not in html


def test_connected_account_without_rows_stays_on_the_syncing_screen(monkeypatch):
    monkeypatch.setattr(
        "app.marketing.get_tenant_ids_for_user",
        lambda uid: ["snaptrade:new"],
    )
    monkeypatch.setattr("app.snaptrade.snaptrade_enabled", lambda: True)
    monkeypatch.setattr(
        "app.models.get_snaptrade_accounts",
        lambda uid: [{"snaptrade_account_id": "new-1"}],
    )
    monkeypatch.setattr(
        "app.cache_ops.warehouse_tenants_present", lambda ids: set(),
    )

    client = app.test_client()
    _login(client, monkeypatch)
    html = client.get("/get-started").get_data(as_text=True)
    assert "The first sync is still running." in html
    assert "Refresh status" in html
    assert "This account is in Overview." not in html


def test_nickname_for_another_login_does_not_claim_success(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_account", lambda uid, aid: None,
    )
    saved = {"called": False}

    def update(uid, aid, nick):
        saved["called"] = True
        return True

    monkeypatch.setattr(
        "app.snaptrade.update_snaptrade_account_nickname", update,
    )

    client = app.test_client()
    _login(client, monkeypatch)
    resp = client.post("/snaptrade/accounts/nickname", data={
        "snaptrade_account_id": "someone-elses",
        "nickname": "Stolen",
    })
    assert resp.status_code == 302
    text = " ".join(msg for _cat, msg in _flashes(client))
    assert "isn't on your login" in text
    assert "Nickname saved" not in text
    assert saved["called"] is False


def test_nickname_save_failure_is_not_reported_as_saved(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_account",
        lambda uid, aid: {"snaptrade_account_id": aid},
    )
    monkeypatch.setattr(
        "app.snaptrade.update_snaptrade_account_nickname",
        lambda uid, aid, nick: False,
    )
    client = app.test_client()
    _login(client, monkeypatch)
    client.post("/snaptrade/accounts/nickname", data={
        "snaptrade_account_id": "mine",
        "nickname": "Roth",
    })
    text = " ".join(msg for _cat, msg in _flashes(client))
    assert "Couldn't save that nickname" in text
    assert "Nickname saved" not in text


def test_disconnect_failure_preserves_local_row_and_hides_exception(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "u", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_account",
        lambda uid, aid: {
            "snaptrade_account_id": aid,
            "brokerage_authorization_id": "auth-9",
        },
    )
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_accounts",
        lambda uid: [{
            "snaptrade_account_id": "acct-9",
            "brokerage_authorization_id": "auth-9",
        }],
    )

    class _Connections:
        def list_brokerage_authorizations(self, **kwargs):
            return [{"id": "auth-9"}]

        def list_brokerage_authorization_accounts(self, **kwargs):
            return [{"id": "acct-9"}]

        def remove_brokerage_authorization(self, **kwargs):
            raise RuntimeError("SnapTrade 403 authorization_id mismatch secret")

    class _Client:
        connections = _Connections()

    monkeypatch.setattr("app.snaptrade._get_snaptrade_client", lambda: _Client())
    removed = {}
    monkeypatch.setattr(
        "app.snaptrade.remove_snaptrade_account",
        lambda uid, aid: removed.update(uid=uid, aid=aid),
    )

    client = app.test_client()
    _login(client, monkeypatch)
    resp = client.post("/snaptrade/accounts/disconnect", data={
        "snaptrade_account_id": "acct-9",
    })
    assert resp.status_code == 302
    flashes = _flashes(client)
    text = " ".join(msg for _cat, msg in flashes)
    assert "nothing was removed from HappyTrader" in text
    assert "Account disconnected" not in text
    assert "403" not in text
    assert "authorization_id mismatch" not in text
    assert removed == {}


def test_disconnect_uses_authorization_id_and_removes_connection_siblings(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "u", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_account",
        lambda uid, aid: {
            "snaptrade_account_id": aid,
            "brokerage_authorization_id": "auth-shared",
        },
    )
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_accounts",
        lambda uid: [
            {
                "snaptrade_account_id": "acct-1",
                "brokerage_authorization_id": "auth-shared",
            },
            {
                "snaptrade_account_id": "acct-2",
                "brokerage_authorization_id": None,
            },
            {
                "snaptrade_account_id": "acct-other",
                "brokerage_authorization_id": "auth-other",
            },
        ],
    )

    revoked = {}
    cached = []

    class _Connections:
        def list_brokerage_authorizations(self, **kwargs):
            return [{"id": "auth-shared"}, {"id": "auth-other"}]

        def list_brokerage_authorization_accounts(self, **kwargs):
            assert kwargs["authorization_id"] == "auth-shared"
            return [{"id": "acct-1"}, {"id": "acct-2"}]

        def remove_brokerage_authorization(self, **kwargs):
            revoked.update(kwargs)

    class _Client:
        connections = _Connections()

    monkeypatch.setattr("app.snaptrade._get_snaptrade_client", lambda: _Client())
    monkeypatch.setattr(
        "app.snaptrade.set_snaptrade_brokerage_authorization_id",
        lambda uid, aid, auth: cached.append((uid, aid, auth)) or True,
    )
    removed = []
    monkeypatch.setattr(
        "app.snaptrade.remove_snaptrade_account",
        lambda uid, aid: removed.append((uid, aid)),
    )

    client = app.test_client()
    _login(client, monkeypatch)
    resp = client.post("/snaptrade/accounts/disconnect", data={
        "snaptrade_account_id": "acct-1",
    })

    assert resp.status_code == 302
    assert revoked["authorization_id"] == "auth-shared"
    assert cached == [(7, "acct-2", "auth-shared")]
    assert removed == [(7, "acct-1"), (7, "acct-2")]
    text = " ".join(msg for _cat, msg in _flashes(client))
    assert "2 linked accounts were removed" in text


def test_disconnect_resolves_an_uncached_authorization_before_revoke(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "u", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_account",
        lambda uid, aid: {
            "snaptrade_account_id": aid,
            "brokerage_authorization_id": None,
        },
    )
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_accounts",
        lambda uid: [{
            "snaptrade_account_id": "acct-old",
            "brokerage_authorization_id": None,
        }],
    )
    calls = {"cached": [], "revoked": []}

    class _AccountInformation:
        def get_user_account_details(self, **kwargs):
            assert kwargs["account_id"] == "acct-old"
            return {"brokerage_authorization": "auth-resolved"}

    class _Connections:
        def list_brokerage_authorizations(self, **kwargs):
            return [{"id": "auth-resolved"}]

        def list_brokerage_authorization_accounts(self, **kwargs):
            return [{"id": "acct-old"}]

        def remove_brokerage_authorization(self, **kwargs):
            calls["revoked"].append(kwargs["authorization_id"])

    class _Client:
        account_information = _AccountInformation()
        connections = _Connections()

    monkeypatch.setattr("app.snaptrade._get_snaptrade_client", lambda: _Client())
    monkeypatch.setattr(
        "app.snaptrade.set_snaptrade_brokerage_authorization_id",
        lambda uid, aid, auth: calls["cached"].append((uid, aid, auth)) or True,
    )
    removed = []
    monkeypatch.setattr(
        "app.snaptrade.remove_snaptrade_account",
        lambda uid, aid: removed.append((uid, aid)),
    )

    client = app.test_client()
    _login(client, monkeypatch)
    resp = client.post("/snaptrade/accounts/disconnect", data={
        "snaptrade_account_id": "acct-old",
    })

    assert resp.status_code == 302
    assert calls["cached"] == [(7, "acct-old", "auth-resolved")]
    assert calls["revoked"] == ["auth-resolved"]
    assert removed == [(7, "acct-old")]


def test_disconnect_cleans_local_rows_when_revoke_response_is_lost(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "u", "snaptrade_secret": "s"},
    )
    row = {
        "snaptrade_account_id": "acct-1",
        "brokerage_authorization_id": "auth-1",
    }
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_account", lambda uid, aid: row,
    )
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_accounts", lambda uid: [row],
    )

    class _Connections:
        list_count = 0

        def list_brokerage_authorizations(self, **kwargs):
            self.list_count += 1
            if self.list_count == 1:
                return [{"id": "auth-1"}]
            return []

        def list_brokerage_authorization_accounts(self, **kwargs):
            return [{"id": "acct-1"}]

        def remove_brokerage_authorization(self, **kwargs):
            raise TimeoutError("response lost")

    class _Client:
        connections = _Connections()

    monkeypatch.setattr("app.snaptrade._get_snaptrade_client", lambda: _Client())
    removed = []
    monkeypatch.setattr(
        "app.snaptrade.remove_snaptrade_account",
        lambda uid, aid: removed.append((uid, aid)),
    )

    client = app.test_client()
    _login(client, monkeypatch)
    resp = client.post("/snaptrade/accounts/disconnect", data={
        "snaptrade_account_id": "acct-1",
    })

    assert resp.status_code == 302
    assert removed == [(7, "acct-1")]
    assert "Account disconnected" in " ".join(
        msg for _cat, msg in _flashes(client)
    )


def test_disconnect_refuses_a_mismatched_cached_authorization(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "u", "snaptrade_secret": "s"},
    )
    row = {
        "snaptrade_account_id": "acct-stale",
        "brokerage_authorization_id": "auth-other",
    }
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_account", lambda uid, aid: row,
    )
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_accounts", lambda uid: [row],
    )
    revoked = []

    class _Connections:
        def list_brokerage_authorizations(self, **kwargs):
            return [{"id": "auth-other"}]

        def list_brokerage_authorization_accounts(self, **kwargs):
            return [{"id": "acct-actually-on-auth"}]

        def remove_brokerage_authorization(self, **kwargs):
            revoked.append(kwargs["authorization_id"])

    class _Client:
        connections = _Connections()

    monkeypatch.setattr("app.snaptrade._get_snaptrade_client", lambda: _Client())
    removed = []
    monkeypatch.setattr(
        "app.snaptrade.remove_snaptrade_account",
        lambda uid, aid: removed.append((uid, aid)),
    )

    client = app.test_client()
    _login(client, monkeypatch)
    resp = client.post("/snaptrade/accounts/disconnect", data={
        "snaptrade_account_id": "acct-stale",
    })

    assert resp.status_code == 302
    assert revoked == []
    assert removed == []
    assert "no longer matches" in " ".join(
        msg for _cat, msg in _flashes(client)
    )


def test_disconnect_rejects_an_account_not_owned_by_the_login(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_account", lambda uid, aid: None,
    )
    client_called = {"value": False}
    monkeypatch.setattr(
        "app.snaptrade._get_snaptrade_client",
        lambda: client_called.update(value=True),
    )
    removed = []
    monkeypatch.setattr(
        "app.snaptrade.remove_snaptrade_account",
        lambda uid, aid: removed.append((uid, aid)),
    )

    client = app.test_client()
    _login(client, monkeypatch)
    resp = client.post("/snaptrade/accounts/disconnect", data={
        "snaptrade_account_id": "someone-elses",
    })

    assert resp.status_code == 302
    assert client_called["value"] is False
    assert removed == []
    assert "isn't on your login" in " ".join(
        msg for _cat, msg in _flashes(client)
    )


def test_remove_snaptrade_account_invalidates_shell_cache(monkeypatch):
    from app import models

    calls = []
    monkeypatch.setattr(
        models, "execute",
        lambda sql, params: calls.append(("execute", params)),
    )
    monkeypatch.setattr(
        models, "_forget_shell",
        lambda uid: calls.append(("invalidate", uid)),
    )

    models.remove_snaptrade_account(7, "acct-1")

    assert calls == [
        ("execute", (7, "acct-1")),
        ("invalidate", 7),
    ]

    calls.clear()
    assert models.set_snaptrade_brokerage_authorization_id(
        7, "acct-1", "auth-1",
    ) is True
    assert calls == [
        ("execute", ("auth-1", 7, "acct-1")),
        ("invalidate", 7),
    ]


def test_seed_store_error_is_logged_not_flashed():
    from app.snaptrade import _flash_and_redirect_after_sync

    secret = "SeedStoreError: table analytics_raw.trade_history denied"
    with app.test_request_context("/snaptrade/accounts"):
        _flash_and_redirect_after_sync({
            "ok": True,
            "label": "Schwab Account",
            "tenant_id": "snaptrade:abc",
            "history_rows": 1,
            "current_rows": 0,
            "github_pushed": False,
            "github_head_sha": None,
            "github_error": secret,
            "github_seed_push_skipped": False,
            "github_no_changes": False,
        }, first_done=True)
        text = " ".join(get_flashed_messages())
    assert "couldn't store the update" in text
    assert secret not in text
    assert "Couldn't push to the cloud" not in text


def test_group_save_hides_internal_errors(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)

    def raise_user_id(*_a, **_k):
        raise ValueError("user_id is required")

    monkeypatch.setattr(
        "app.profile_page.create_account_group", raise_user_id,
    )
    client = app.test_client()
    _login(client, monkeypatch)
    client.post("/profile", data={
        "action": "save_account_group",
        "group_name": "Kids",
        "tenant_id": "snaptrade:abc",
    })
    text = " ".join(msg for _cat, msg in _flashes(client))
    assert text == "Couldn't save that group. Try again."
    assert "user_id" not in text

    def raise_type(*_a, **_k):
        raise TypeError("unsupported operand type(s)")

    monkeypatch.setattr("app.profile_page.create_account_group", raise_type)
    client.post("/profile", data={
        "action": "save_account_group",
        "group_name": "Kids",
        "tenant_id": "snaptrade:abc",
    })
    text = " ".join(msg for _cat, msg in _flashes(client))
    assert "unsupported operand" not in text
    assert "Couldn't save that group" in text


def _stub_profile_reads(monkeypatch, accounts):
    monkeypatch.setattr("app.profile_page.get_user_profile", lambda uid: {
        "timezone": "America/New_York",
        "default_route": "weekly_review",
    })
    monkeypatch.setattr("app.profile_page.get_accounts_for_user", lambda uid: [])
    monkeypatch.setattr("app.profile_page.get_uploads_for_user", lambda uid: [])
    monkeypatch.setattr("app.profile_page.count_uploads_for_user", lambda uid: 0)
    monkeypatch.setattr(
        "app.profile_page.get_broker_tenants_for_user", lambda uid: [],
    )
    monkeypatch.setattr("app.profile_page.list_account_groups", lambda uid: [])
    monkeypatch.setattr("app.snaptrade.snaptrade_enabled", lambda: True)
    monkeypatch.setattr(
        "app.models.get_snaptrade_accounts", lambda uid: accounts,
    )
    monkeypatch.setattr("app.plan.plan_state", lambda uid: "trialing")
    monkeypatch.setattr("app.billing.subscription_summary", lambda uid: None)
    monkeypatch.setattr(
        "app.early_broker.subscribe_offer_for_user", lambda uid, **k: None,
    )
    monkeypatch.setattr(
        "app.plan.plan_status_for_banner",
        lambda uid: {
            "state": "trialing",
            "days_left": 12,
            "frozen_on": date(2026, 11, 2),
            "disconnect_on": None,
            "disconnect_in_days": None,
        },
    )


def test_settings_shows_reconnect_and_a_labeled_trial_date(monkeypatch):
    accounts = [{
        "snaptrade_account_id": "acct-1",
        "tenant_id": "snaptrade:acct-1",
        "account_name": "Schwab Account",
        "display_nickname": "Roth",
        "broker_slug": "schwab",
        "connection_broken_at": "2026-10-01T12:00:00+00:00",
        "first_sync_completed": True,
        "holdings_last_successful_sync": "2026-09-30",
        "last_sync_at": "2026-10-01T15:00:00+00:00",
        "last_sync_error": "Traceback (most recent call last): secret",
    }]
    _stub_profile_reads(monkeypatch, accounts)
    client = app.test_client()
    _login(client, monkeypatch)

    overview = client.get("/profile").get_data(as_text=True)
    assert "Open accounts to reconnect" in overview
    assert "Reconnect needed" in overview
    assert "Broker data as of Sep 30, 2026" in overview
    assert "Traceback" not in overview
    assert "New trades won't come in until you reconnect." in overview

    billing = client.get("/profile?tab=billing").get_data(as_text=True)
    assert "30-day free trial, no credit card" in billing
    assert "12 days left" in billing
    assert "Updates stop Nov 2, 2026" in billing
    assert "2026-11-02" not in billing


def test_linked_account_row_hides_the_raw_sync_error():
    account = {
        "snaptrade_account_id": "acct-1",
        "tenant_id": "snaptrade:acct-1",
        "account_name": "Schwab Account",
        "display_nickname": "Roth",
        "broker_slug": "Schwab",
        "account_number_masked": "1234",
        "connection_broken_at": None,
        "first_sync_completed": True,
        "holdings_last_successful_sync": "2026-09-30T00:00:00+00:00",
        "last_sync_error": "ValueError: secret internals",
    }
    sibling = {
        **account,
        "snaptrade_account_id": "acct-2",
        "tenant_id": "snaptrade:acct-2",
        "display_nickname": "Taxable",
        "account_number_masked": "5678",
        "last_sync_error": None,
    }
    group = {
        "broker_label": "Schwab",
        "authorization_id": "auth-1",
        "needs_reconnect": False,
        "accounts": [account, sibling],
    }
    with app.test_request_context("/snaptrade/accounts"):
        html = render_template(
            "snaptrade_accounts.html",
            accounts=[account, sibling],
            connection_groups=[group],
            any_reconnect_needed=False,
            snaptrade_enabled=True,
        )
    assert "Broker data as of Sep 30, 2026" in html
    assert "Last sync didn&#39;t finish" in html
    assert "ValueError" not in html
    assert "secret internals" not in html
    assert "Disconnect this brokerage connection and its 2 linked accounts" in html
    assert "Disconnect connection" in html
