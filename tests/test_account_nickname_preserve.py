"""Connect and sync must not replace a user-set account nickname.

Fixtures are made up. Privacy mode still masks labels as Account N.
"""
from app import app
from app.models import (
    get_or_create_broker_tenant,
    reactivate_snaptrade_tenant,
    upsert_snaptrade_account,
)
from app.privacy import PRIVACY_MASK
from app.routes import _rows_with_snaptrade_nicknames
from app.snaptrade import _preserved_display_nickname
from tests.test_broker_first_onboarding import _SessionUser
from tests.test_post_connect_onboarding import _callback, _wire_portal_return


TENANT = "snaptrade:fixture-cedar"
PAPER = "snaptrade:fixture-paper"
NICK = "Cedar IRA"


def test_preserved_nickname_keeps_the_row_about_to_be_deleted():
    assert _preserved_display_nickname(
        None,
        {"display_nickname": "  "},
        {"display_nickname": NICK},
        {"display_nickname": "North Desk"},
    ) == NICK
    assert _preserved_display_nickname(
        {"display_nickname": "North Desk"},
        {"display_nickname": NICK},
    ) == "North Desk"
    assert _preserved_display_nickname({}, None) is None


def test_upsert_keeps_a_stored_nickname_and_ignores_a_blank(monkeypatch):
    calls = []

    def _execute(sql, params):
        calls.append((sql, params))

    monkeypatch.setattr("app.models.execute", _execute)
    monkeypatch.setattr("app.models._forget_shell", lambda user_id: None)

    upsert_snaptrade_account(
        7,
        "fixture-new",
        tenant_id=TENANT,
        display_nickname="   ",
        broker_slug="Example",
        account_number_masked="9999",
        account_name="Example Broker Account",
    )

    sql, params = calls[0]
    assert "NULLIF(btrim(snaptrade_accounts.display_nickname), '')" in sql
    assert "NULLIF(btrim(EXCLUDED.display_nickname), '')" in sql
    assert "display_nickname       = EXCLUDED.display_nickname" not in sql
    assert params[4] is None


def test_reactivate_and_create_do_not_write_display_nickname(monkeypatch):
    captured = {}

    def _returning(sql, params):
        captured["reactivate"] = sql
        return {"tenant_id": TENANT}

    monkeypatch.setattr("app.models.execute_returning", _returning)
    monkeypatch.setattr("app.models._forget_shell", lambda user_id: None)
    reactivate_snaptrade_tenant(
        7,
        TENANT,
        account_name="Example Broker Account",
        broker_label="Example",
    )
    assert "display_nickname" not in captured["reactivate"]
    assert "account_name =" not in captured["reactivate"]

    def _fetch(sql, params):
        captured["select"] = sql
        return {
            "tenant_id": TENANT,
            "user_id": 7,
            "account_name": "Old Example Label",
            "account_mask": None,
            "broker_label": "Example",
            "institution_account_id": None,
            "snaptrade_connection_id": None,
        }

    def _execute(sql, params):
        captured["update"] = sql

    monkeypatch.setattr("app.models.fetch_one", _fetch)
    monkeypatch.setattr("app.models.execute", _execute)
    get_or_create_broker_tenant(
        7,
        "snaptrade",
        "fixture-cedar",
        "Example Broker Account",
        broker_label="Example",
    )
    assert "display_nickname" not in captured["update"]


def test_overlay_does_not_replace_a_tenant_nickname(monkeypatch):
    monkeypatch.setattr(
        "app.models.get_snaptrade_accounts",
        lambda uid: [{
            "tenant_id": TENANT,
            "display_nickname": "Other Desk",
        }],
    )
    rows = _rows_with_snaptrade_nicknames(7, [{
        "tenant_id": TENANT,
        "account_name": "Example Broker Account",
        "display_nickname": NICK,
    }])
    assert rows[0]["display_nickname"] == NICK

    filled = _rows_with_snaptrade_nicknames(7, [{
        "tenant_id": TENANT,
        "account_name": "Example Broker Account",
        "display_nickname": "  ",
    }])
    assert filled[0]["display_nickname"] == "Other Desk"
    assert filled[0] is not None


def test_rotated_connect_keeps_the_nickname_and_fills_a_blank_tenant(monkeypatch):
    prior = [{
        "snaptrade_account_id": "fixture-old",
        "tenant_id": TENANT,
        "account_name": "Example Broker Account",
        "display_nickname": NICK,
        "first_sync_completed": True,
        "connection_broken_at": None,
    }]
    remote = [{
        "id": "fixture-new",
        "institution_name": "Example Broker",
        "connection_broker": "Example Broker",
        "number": "9999",
        "brokerage_authorization": "auth-fixture",
    }]
    snap, _kicked = _wire_portal_return(monkeypatch, prior=prior, remote=remote)
    saved = {}
    removed = []
    tenant_writes = []

    def _upsert(*args, **kwargs):
        saved["args"] = args
        saved["kwargs"] = kwargs

    monkeypatch.setattr(snap, "upsert_snaptrade_account", _upsert)
    monkeypatch.setattr(
        snap, "_ensure_snaptrade_tenant_id", lambda **kwargs: TENANT,
    )
    monkeypatch.setattr(
        snap, "get_broker_tenant",
        lambda tid: {
            "tenant_id": tid,
            "account_name": "Example Broker Account",
            "display_nickname": None,
        },
    )
    monkeypatch.setattr(
        snap, "remove_snaptrade_account",
        lambda uid, aid: removed.append(aid),
    )
    monkeypatch.setattr(
        snap, "update_broker_tenant_display_nickname",
        lambda uid, tid, nick: tenant_writes.append((uid, tid, nick)),
    )

    resp, _messages = _callback(snap)
    assert resp.status_code == 302
    assert saved["kwargs"]["display_nickname"] == NICK
    assert saved["kwargs"]["tenant_id"] == TENANT
    assert saved["args"][1] == "fixture-new"
    assert tenant_writes == [(7, TENANT, NICK)]
    assert removed == ["fixture-old"]


def test_connect_does_not_replace_a_different_tenant_nickname(monkeypatch):
    """Adding another brokerage re-saves every account. The saved nickname stays."""
    prior = [{
        "snaptrade_account_id": "fixture-same",
        "tenant_id": TENANT,
        "account_name": "Example Broker Account",
        "display_nickname": NICK,
        "first_sync_completed": True,
        "connection_broken_at": None,
    }]
    remote = [
        {
            "id": "fixture-same",
            "institution_name": "Example Broker",
            "connection_broker": "Example Broker",
            "number": "9999",
            "brokerage_authorization": "auth-old",
        },
        {
            "id": "fixture-empower",
            "institution_name": "Other Firm",
            "connection_broker": "Empower",
            "number": "8888",
            "brokerage_authorization": "auth-new",
        },
    ]
    snap, _kicked = _wire_portal_return(monkeypatch, prior=prior, remote=remote)
    saved = []
    tenant_writes = []
    removed = []

    def _ensure(**kwargs):
        aid = kwargs["snaptrade_account_id"]
        if aid == "fixture-same":
            return TENANT
        return "snaptrade:fixture-empower"

    monkeypatch.setattr(snap, "_ensure_snaptrade_tenant_id", _ensure)
    monkeypatch.setattr(
        snap, "upsert_snaptrade_account",
        lambda *args, **kwargs: saved.append(kwargs),
    )
    monkeypatch.setattr(
        snap, "get_broker_tenant",
        lambda tid: {
            "tenant_id": tid,
            "account_name": "Example Broker Account",
            "display_nickname": "North Desk" if tid == TENANT else None,
        },
    )
    monkeypatch.setattr(
        snap, "update_broker_tenant_display_nickname",
        lambda *args: tenant_writes.append(args),
    )
    monkeypatch.setattr(
        snap, "remove_snaptrade_account",
        lambda uid, aid: removed.append(aid),
    )

    _callback(snap)
    by_name = {row["broker_slug"]: row["display_nickname"] for row in saved}
    assert by_name["Example Broker"] == NICK
    assert tenant_writes == []
    assert removed == []


def test_account_label_shows_nickname_when_privacy_is_off(monkeypatch):
    from flask_login import login_user

    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: False)
    monkeypatch.setattr(
        "app.models.get_broker_tenants_for_user",
        lambda uid: [{
            "tenant_id": TENANT,
            "account_name": "Example Broker Account",
            "display_nickname": None,
            "account_mask": "9999",
            "broker_uuid": "fixture-cedar",
        }],
    )
    monkeypatch.setattr(
        "app.models.get_snaptrade_accounts",
        lambda uid: [{
            "tenant_id": TENANT,
            "display_nickname": NICK,
            "account_name": "Example Broker Account",
        }],
    )
    with app.test_request_context("/profile?tab=account"):
        login_user(_SessionUser())
        rendered = app.jinja_env.from_string(
            "{{ (nick or name) | account_label(tid) }}|{{ broker | privacy_hide }}"
        ).render(
            nick=NICK,
            name="Example Broker Account",
            tid=TENANT,
            broker="Example Broker",
        )
    assert rendered == f"{NICK}|Example Broker"
    assert "Account 1" not in rendered
    assert PRIVACY_MASK not in rendered


def test_account_label_stays_masked_when_privacy_is_on(monkeypatch):
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: True)
    monkeypatch.setattr(
        "app.privacy.viewer_slots",
        lambda: (
            {TENANT: "Account 1", PAPER: "Account 6"},
            {NICK: "Account 1"},
        ),
    )
    monkeypatch.setattr(
        "app.privacy.viewer_paper_tenant_ids", lambda: {PAPER},
    )
    with app.test_request_context("/profile?tab=account"):
        label = app.jinja_env.filters["account_label"]
        hide = app.jinja_env.filters["privacy_hide"]
        real = label(NICK, TENANT)
        paper = label(NICK, PAPER)
        broker = hide("Example Broker")
    assert real == "Account 1"
    assert paper == "Account 6 · Paper"
    assert broker == PRIVACY_MASK
    assert NICK not in real
    assert NICK not in paper
