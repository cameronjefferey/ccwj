"""Broker-first onboarding: SnapTrade is the path, CSV is older history.

Pins the public steps, signup field preservation, the empty vs connected
/get-started states, the pre-portal interstitial, and an honest cancel
when the Connection Portal returns without a new brokerage.
"""
from urllib.parse import urlparse

import pytest

from app import app
from app.models import User
from app.plan import pricing_story


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


def _client():
    return app.test_client()


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


def _path(response):
    return urlparse(response.headers.get("Location") or "").path


def test_landing_how_it_works_is_broker_first():
    html = _client().get("/").get_data(as_text=True)
    assert "Connect your brokerage" in html
    assert "Sync your accounts" in html
    assert "See your trading profile" in html
    assert "Export from Schwab" not in html
    assert "Need older history?" in html
    assert "Upload a CSV" in html or "upload a CSV" in html
    assert "few hours" in html


def test_signup_explains_snaptrade_next_and_keeps_fields_on_mismatch(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setitem(app.config, "SIGNUP_ENABLED", True)
    monkeypatch.setitem(app.config, "SIGNUP_INVITE_CODE", "")
    monkeypatch.setitem(app.config, "RATELIMIT_ENABLED", False)
    monkeypatch.setattr(
        "app.auth.User.get_by_email", staticmethod(lambda email: None),
    )

    client = _client()
    page = client.get("/signup")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    assert "securely connecting a brokerage through SnapTrade" in body
    assert "never stores your broker password" in body

    resp = client.post("/signup", data={
        "username": "ada_trader",
        "email": "ada@example.com",
        "password": "Secret1pass",
        "confirm": "Secret2pass",
    })
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Passwords do not match." in html
    assert 'value="ada_trader"' in html
    assert 'value="ada@example.com"' in html
    assert 'value="Secret1pass"' in html
    assert 'value="Secret2pass"' in html


def test_get_started_without_broker_is_connect_first(monkeypatch):
    monkeypatch.setattr(
        "app.marketing.get_tenant_ids_for_user", lambda uid: [],
    )
    monkeypatch.setattr("app.snaptrade.snaptrade_enabled", lambda: True)
    monkeypatch.setattr("app.models.get_snaptrade_accounts", lambda uid: [])

    client = _client()
    _login(client, monkeypatch)
    resp = client.get("/get-started")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Connect brokerage" in html
    assert "I'll do this later" in html
    assert "Skip to overview" in html
    assert "Upload a CSV" in html
    assert "Two ways to bring your trades" not in html
    # CSV is present, but not as a peer primary button.
    assert 'btn btn-primary btn-lg' in html
    assert html.index("Connect brokerage") < html.index("Upload a CSV")


def test_get_started_with_broker_offers_connect_another(monkeypatch):
    monkeypatch.setattr(
        "app.marketing.get_tenant_ids_for_user", lambda uid: [],
    )
    monkeypatch.setattr("app.snaptrade.snaptrade_enabled", lambda: True)
    monkeypatch.setattr(
        "app.models.get_snaptrade_accounts",
        lambda uid: [{"snaptrade_account_id": "abc"}],
    )

    client = _client()
    _login(client, monkeypatch)
    resp = client.get("/get-started")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Connect another account" in html
    assert "Manage accounts" in html
    assert "Here's what we found." not in html


def test_upload_leads_with_broker_connect(monkeypatch):
    monkeypatch.setattr("app.upload.get_accounts_for_user", lambda uid: [])
    monkeypatch.setattr("app.upload.get_broker_tenants_for_user", lambda uid: [])
    monkeypatch.setattr("app.upload.get_uploads_for_user", lambda uid: [])

    client = _client()
    _login(client, monkeypatch)
    resp = client.get("/upload")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Prefer connecting your broker?" in html
    assert "Upload older history" in html
    assert html.index("Prefer connecting your broker?") < html.index("Upload older history")
    assert "15–40 minutes" in html or "15-40 minutes" in html


def test_snaptrade_connect_get_is_an_interstitial(monkeypatch):
    from app import snaptrade as snap

    monkeypatch.setattr(snap, "snaptrade_enabled", lambda: True)

    def _must_not_open_portal():
        raise AssertionError("interstitial must not open the SnapTrade portal")

    monkeypatch.setattr(snap, "_get_snaptrade_client", _must_not_open_portal)
    monkeypatch.setattr(snap, "get_snaptrade_accounts", lambda uid: [])

    client = _client()
    _login(client, monkeypatch)
    resp = client.get("/snaptrade/connect?return_to=/get-started")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Continue to SnapTrade" in html
    assert "Read-only" in html
    assert "never stores" in html.lower() or "does not store" in html
    assert 'name="portal_confirmed" value="1"' in html
    assert 'name="return_to" value="/get-started"' in html
    assert 'href="/get-started"' in html


def test_reconnect_post_shows_interstitial_with_the_grant_id(monkeypatch):
    from app import snaptrade as snap

    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setattr(snap, "snaptrade_enabled", lambda: True)
    monkeypatch.setattr(snap, "get_snaptrade_accounts", lambda uid: [{
        "snaptrade_account_id": "abc",
    }])

    def _must_not_open_portal():
        raise AssertionError("unconfirmed reconnect must not open the portal")

    monkeypatch.setattr(snap, "_get_snaptrade_client", _must_not_open_portal)

    client = _client()
    _login(client, monkeypatch)
    resp = client.post("/snaptrade/connect", data={
        "reconnect_authorization_id": "auth-42",
        "reconnect_broker_label": "Schwab",
        "return_to": "/snaptrade/accounts",
    })
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Reconnect Schwab" in html
    assert 'name="reconnect_authorization_id" value="auth-42"' in html
    assert 'name="portal_confirmed" value="1"' in html
    assert "Continue to SnapTrade" in html


def test_snaptrade_accounts_puts_connect_above_existing_cards(monkeypatch):
    from app import snaptrade as snap

    monkeypatch.setattr(snap, "snaptrade_enabled", lambda: True)
    monkeypatch.setattr(snap, "get_snaptrade_accounts", lambda uid: [{
        "snaptrade_account_id": "abc",
        "account_name": "Schwab Account",
        "display_nickname": "Trading",
        "account_number_masked": "",
        "broker_slug": "schwab",
        "brokerage_authorization_id": "auth-1",
        "connection_broken_at": None,
        "holdings_last_successful_sync": None,
        "first_sync_completed": True,
    }])

    client = _client()
    _login(client, monkeypatch)
    resp = client.get("/snaptrade/accounts")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Connect an account" in html
    assert "Connected accounts" in html
    assert html.index("Connect an account") < html.index("Connected accounts")
    assert "Add a broker or another account" in html


def test_callback_cancel_does_not_claim_connected_or_open_name_now(monkeypatch):
    import types
    from app import snaptrade as snap

    monkeypatch.setattr(snap, "current_user", types.SimpleNamespace(id=7))
    monkeypatch.setattr(
        snap, "get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "u", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(
        snap, "_list_snaptrade_accounts",
        lambda client, creds: ([{"id": "existing-acc"}], True),
    )
    monkeypatch.setattr(
        snap, "get_snaptrade_accounts",
        lambda uid: [{"snaptrade_account_id": "existing-acc"}],
    )
    kicked = []
    monkeypatch.setattr(snap, "_kick_post_connect_sync", lambda uid: kicked.append(uid))

    with app.test_request_context("/snaptrade/callback"):
        from flask import session
        session["snaptrade_callback_user_id"] = 7
        resp = snap.snaptrade_callback.__wrapped__()
        flashes = session.get("_flashes") or []

    assert resp.status_code == 302
    location = resp.location or ""
    assert "name-now" not in location
    assert _path(resp).rstrip("/").endswith("/snaptrade/accounts")
    messages = " ".join(str(item) for item in flashes)
    assert "Connected" not in messages
    assert "without adding a brokerage" in messages
    assert kicked == []


def test_callback_with_a_new_account_still_starts_sync(monkeypatch):
    import types
    from app import snaptrade as snap

    monkeypatch.setattr(snap, "current_user", types.SimpleNamespace(id=7))
    monkeypatch.setattr(
        snap, "get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "u", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(
        snap, "_list_snaptrade_accounts",
        lambda client, creds: ([{
            "id": "new-acc",
            "institution_name": "Schwab",
            "number": "9988",
            "brokerage_authorization": "auth-9",
        }], True),
    )

    def _accounts(uid):
        if not _accounts.calls:
            _accounts.calls += 1
            return []
        _accounts.calls += 1
        return [{
            "snaptrade_account_id": "new-acc",
            "first_sync_completed": False,
            "account_name": "Schwab Account",
        }]

    _accounts.calls = 0
    monkeypatch.setattr(snap, "get_snaptrade_accounts", _accounts)
    monkeypatch.setattr(
        snap, "_ensure_snaptrade_tenant_id", lambda **kwargs: "snaptrade:new-acc",
    )
    monkeypatch.setattr(
        snap, "get_broker_tenant",
        lambda tid: {"account_name": "Schwab Account", "display_nickname": None},
    )
    monkeypatch.setattr(snap, "upsert_snaptrade_account", lambda *a, **k: None)
    monkeypatch.setattr(snap, "clear_snaptrade_connection_broken", lambda *a, **k: None)
    monkeypatch.setattr(snap, "add_account_for_user", lambda *a, **k: None)
    monkeypatch.setattr(
        snap, "set_snaptrade_brokerage_authorization_id", lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "app.early_broker.maybe_stamp_early_broker_cohort", lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "app.routes._snaptrade_accounts_needing_nickname", lambda rows: [],
    )
    kicked = []
    monkeypatch.setattr(snap, "_kick_post_connect_sync", lambda uid: kicked.append(uid))

    with app.test_request_context("/snaptrade/callback"):
        from flask import session
        session["snaptrade_callback_user_id"] = 7
        resp = snap.snaptrade_callback.__wrapped__()
        flashes = session.get("_flashes") or []

    assert resp.status_code == 302
    assert "sync" in (resp.location or "")
    assert "name-now" not in (resp.location or "")
    messages = " ".join(str(item) for item in flashes)
    assert "Connected 1 account" in messages
    assert kicked == [7]


def test_pricing_lead_does_not_promise_the_profile_in_minutes():
    lead = pricing_story(None)["lead"]
    assert "few hours" in lead
    assert "within minutes" in lead
    assert "full trading profile in minutes" not in lead


def test_callback_empty_portal_for_a_new_user_is_not_a_success(monkeypatch):
    import types
    from app import snaptrade as snap

    monkeypatch.setattr(snap, "current_user", types.SimpleNamespace(id=7))
    monkeypatch.setattr(
        snap, "get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "u", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(
        snap, "_list_snaptrade_accounts", lambda client, creds: ([], True),
    )
    monkeypatch.setattr(snap, "get_snaptrade_accounts", lambda uid: [])

    with app.test_request_context("/snaptrade/callback"):
        from flask import session
        session["snaptrade_callback_user_id"] = 7
        resp = snap.snaptrade_callback.__wrapped__()
        flashes = session.get("_flashes") or []

    assert resp.status_code == 302
    assert _path(resp).rstrip("/").endswith("/snaptrade/accounts")
    messages = " ".join(str(item) for item in flashes)
    assert "No brokerage was connected" in messages
    assert "Connected" not in messages
