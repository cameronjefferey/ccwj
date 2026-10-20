"""Second brokerage must not reopen first-time onboarding.

The survey is for a person who has never finished it and had nothing
linked before this connect. Fixtures are made up.
"""
from datetime import datetime, timezone

import pytest

from app import app
from app.onboarding_gate import (
    first_connect_needs_quiz,
    onboarding_quiz_still_pending,
)
from app.snaptrade import (
    _broker_label_for_account,
    _connected_syncing_message,
    _list_snaptrade_accounts,
)
from tests.test_broker_first_onboarding import _client, _login, _path


def test_saved_skip_is_finished_onboarding(monkeypatch):
    monkeypatch.setattr(
        "app.models.get_onboarding_response",
        lambda uid: {"answers": {"skipped": True}},
    )
    assert first_connect_needs_quiz(7, [], []) is False


def test_prior_accounts_or_tenants_skip_the_quiz_before_the_new_row_exists():
    prior = [{"snaptrade_account_id": "older-acc", "first_sync_completed": True}]
    assert first_connect_needs_quiz(7, prior, []) is False
    assert first_connect_needs_quiz(7, [], ["manual:manual:7:older-book"]) is False


def test_brand_new_user_still_needs_the_quiz(monkeypatch):
    monkeypatch.setattr("app.models.get_onboarding_response", lambda uid: None)
    assert first_connect_needs_quiz(7, [], []) is True


def test_stale_first_flag_hides_the_quiz_once_an_account_is_established(monkeypatch):
    monkeypatch.setattr("app.models.get_onboarding_response", lambda uid: None)
    monkeypatch.setattr(
        "app.models.get_snaptrade_accounts",
        lambda uid: [{
            "snaptrade_account_id": "older-acc",
            "tenant_id": "snaptrade:older-acc",
            "first_sync_completed": True,
            "created_at": datetime(2024, 4, 4, tzinfo=timezone.utc),
        }],
    )
    monkeypatch.setattr(
        "app.models.get_tenant_ids_for_user",
        lambda uid: ["snaptrade:older-acc"],
    )
    assert onboarding_quiz_still_pending(7) is False


def test_just_added_account_inside_the_window_still_shows_the_quiz(monkeypatch):
    monkeypatch.setattr("app.models.get_onboarding_response", lambda uid: None)
    monkeypatch.setattr(
        "app.models.get_snaptrade_accounts",
        lambda uid: [{
            "snaptrade_account_id": "new-acc",
            "tenant_id": "snaptrade:new-acc",
            "first_sync_completed": False,
            "created_at": datetime.now(timezone.utc),
        }],
    )
    monkeypatch.setattr(
        "app.models.get_tenant_ids_for_user",
        lambda uid: ["snaptrade:new-acc"],
    )
    assert onboarding_quiz_still_pending(7) is True


def test_processing_page_shows_the_quiz_only_while_it_is_pending(monkeypatch):
    monkeypatch.setattr("app.models.get_onboarding_response", lambda uid: None)
    monkeypatch.setattr("app.models.get_snaptrade_accounts", lambda uid: [])
    monkeypatch.setattr("app.models.get_tenant_ids_for_user", lambda uid: [])
    monkeypatch.setattr(
        "app.early_broker.early_broker_notice_for_user", lambda uid: None,
    )

    client = _client()
    _login(client, monkeypatch)
    fresh = client.get("/sync/processing?first=1&connecting=1")
    assert fresh.status_code == 200
    html = fresh.get_data(as_text=True)
    assert "Why are you here?" in html
    assert 'id="onboardingForm"' in html
    assert "/get-started" in html

    monkeypatch.setattr(
        "app.models.get_snaptrade_accounts",
        lambda uid: [{
            "snaptrade_account_id": "older-acc",
            "tenant_id": "snaptrade:older-acc",
            "first_sync_completed": True,
            "created_at": datetime(2024, 4, 4, tzinfo=timezone.utc),
        }],
    )
    monkeypatch.setattr(
        "app.models.get_tenant_ids_for_user",
        lambda uid: ["snaptrade:older-acc"],
    )
    stale = client.get("/sync/processing?first=1&connecting=1")
    assert stale.status_code == 200
    body = stale.get_data(as_text=True)
    assert "Why are you here?" not in body
    assert 'id="onboardingForm"' not in body
    assert '"/overview?from_sync=1"' in body or '"/overview?from_sync=1"' in body.replace("&amp;", "&")


def test_background_sync_stamps_do_not_hide_the_first_survey(monkeypatch):
    """A post-connect pull stamps last_sync_at before history is ready.
    That must not take down the only survey."""
    now = datetime.now(timezone.utc)
    monkeypatch.setattr("app.models.get_onboarding_response", lambda uid: None)
    monkeypatch.setattr(
        "app.models.get_snaptrade_accounts",
        lambda uid: [{
            "snaptrade_account_id": "new-acc",
            "tenant_id": "snaptrade:new-acc",
            "first_sync_completed": False,
            "last_sync_at": now,
            "holdings_last_successful_sync": now,
            "created_at": now,
        }],
    )
    monkeypatch.setattr(
        "app.models.get_tenant_ids_for_user",
        lambda uid: ["snaptrade:new-acc"],
    )
    assert onboarding_quiz_still_pending(7) is True

    monkeypatch.setattr(
        "app.early_broker.early_broker_notice_for_user", lambda uid: None,
    )
    client = _client()
    _login(client, monkeypatch)
    page = client.get("/sync/processing?first=1&connecting=1")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert 'id="onboardingForm"' in html
    assert 'name="why_here[]"' in html
    assert 'type="radio"' not in html
    assert "Pick any that apply." in html


def test_old_account_without_the_first_sync_flag_hides_a_bookmarked_quiz(monkeypatch):
    """Accounts that predate first_sync_completed still count as onboarded."""
    now = datetime.now(timezone.utc)
    monkeypatch.setattr("app.models.get_onboarding_response", lambda uid: None)
    monkeypatch.setattr(
        "app.models.get_snaptrade_accounts",
        lambda uid: [{
            "snaptrade_account_id": "older-acc",
            "tenant_id": "snaptrade:older-acc",
            "first_sync_completed": False,
            "last_sync_at": now,
            "created_at": datetime(2024, 4, 4, tzinfo=timezone.utc),
        }],
    )
    monkeypatch.setattr(
        "app.models.get_tenant_ids_for_user",
        lambda uid: ["snaptrade:older-acc"],
    )
    assert onboarding_quiz_still_pending(7) is False

    monkeypatch.setattr(
        "app.early_broker.early_broker_notice_for_user", lambda uid: None,
    )
    client = _client()
    _login(client, monkeypatch)
    page = client.get("/sync/processing?first=1&connecting=1")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    assert "Why are you here?" not in body
    assert 'id="onboardingForm"' not in body


def test_survey_stores_several_answers_for_a_former_radio(monkeypatch):
    saved = {}

    def _save(**kwargs):
        saved.update(kwargs)
        return True

    monkeypatch.setattr("app.models.save_onboarding_response", _save)
    monkeypatch.setattr("app.models.update_user_profile", lambda *a, **k: None)
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    client = _client()
    _login(client, monkeypatch)
    resp = client.post(
        "/onboarding/why-here",
        data={
            "why_here[]": ["lose_money_options", "understand_patterns"],
            "primary_style[]": ["covered_calls", "cash_secured_puts"],
            "one_thing": "What did the covered calls actually make?",
        },
        headers={"X-Requested-With": "XMLHttpRequest"},
    )
    assert resp.status_code == 200
    answers = saved["answers"]
    assert answers["why_here"] == ["lose_money_options", "understand_patterns"]
    assert answers["primary_style"] == ["covered_calls", "cash_secured_puts"]
    assert answers["one_thing"].startswith("What did the covered calls")


def test_skip_stores_a_finished_survey(monkeypatch):
    saved = {}

    def _save(**kwargs):
        saved.update(kwargs)
        return True

    monkeypatch.setattr("app.models.save_onboarding_response", _save)
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    client = _client()
    _login(client, monkeypatch)
    resp = client.post(
        "/onboarding/why-here",
        data={"skipped": "1"},
        headers={"X-Requested-With": "XMLHttpRequest"},
    )
    assert resp.status_code == 200
    assert saved["user_id"] == 7
    assert saved["answers"] == {"skipped": True}


def _wire_portal_return(monkeypatch, *, prior, remote):
    import types
    from app import snaptrade as snap

    monkeypatch.setattr(snap, "current_user", types.SimpleNamespace(id=7))
    monkeypatch.setattr(
        snap, "get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "u", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(
        snap, "_list_snaptrade_accounts", lambda client, creds: (remote, True),
    )
    monkeypatch.setattr(snap, "get_snaptrade_accounts", lambda uid: list(prior))
    monkeypatch.setattr(
        snap, "_ensure_snaptrade_tenant_id",
        lambda **kwargs: "snaptrade:" + kwargs["snaptrade_account_id"],
    )
    monkeypatch.setattr(
        snap, "get_broker_tenant",
        lambda tid: {"account_name": "Broker Account", "display_nickname": None},
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
    monkeypatch.setattr("app.models.get_onboarding_response", lambda uid: None)
    monkeypatch.setattr("app.models.get_tenant_ids_for_user", lambda uid: [])
    monkeypatch.setattr(
        "app.routes._snaptrade_accounts_needing_nickname", lambda rows: [],
    )
    kicked = []
    monkeypatch.setattr(snap, "_kick_post_connect_sync", lambda uid: kicked.append(uid))
    return snap, kicked


def _callback(snap, *, return_to=None):
    with app.test_request_context("/snaptrade/callback"):
        from flask import session
        session["snaptrade_callback_user_id"] = 7
        if return_to:
            session["snaptrade_return_to"] = return_to
        resp = snap.snaptrade_callback.__wrapped__()
        flashes = session.get("_flashes") or []
    return resp, " ".join(str(item) for item in flashes)


@pytest.mark.parametrize("return_to,expected", [
    ("/snaptrade/accounts", "/snaptrade/accounts"),
    ("/profile?tab=account", "/profile"),
    (None, "/overview"),
])
def test_existing_user_adding_empower_skips_the_quiz(monkeypatch, return_to, expected):
    prior = [{
        "snaptrade_account_id": "older-acc",
        "tenant_id": "snaptrade:older-acc",
        "account_name": "Schwab Account",
        "first_sync_completed": True,
        "connection_broken_at": None,
    }]
    remote = [
        {
            "id": "older-acc",
            "institution_name": "Charles Schwab",
            "connection_broker": "Charles Schwab",
            "number": "1111",
            "brokerage_authorization": "auth-old",
        },
        {
            "id": "new-empower",
            "institution_name": "Coinbase",
            "connection_broker": "Empower",
            "number": "2222",
            "brokerage_authorization": "auth-new",
        },
    ]
    snap, kicked = _wire_portal_return(monkeypatch, prior=prior, remote=remote)
    resp, messages = _callback(snap, return_to=return_to)

    assert resp.status_code == 302
    assert _path(resp).rstrip("/").endswith(expected) or expected in (resp.location or "")
    assert "sync/processing" not in (resp.location or "")
    assert "first=1" not in (resp.location or "")
    assert "Why are you here" not in messages
    assert "Empower connected, syncing now" in messages
    assert "Coinbase" not in messages
    assert kicked == [7]


def test_old_account_without_the_first_sync_flag_skips_the_quiz_on_another_broker(monkeypatch):
    """Adding a broker must not quiz someone whose older account never
    flipped first_sync_completed."""
    prior = [{
        "snaptrade_account_id": "older-acc",
        "tenant_id": "snaptrade:older-acc",
        "account_name": "Schwab Account",
        "first_sync_completed": False,
        "last_sync_at": datetime(2024, 4, 4, tzinfo=timezone.utc),
        "created_at": datetime(2024, 4, 4, tzinfo=timezone.utc),
        "connection_broken_at": None,
    }]
    remote = [
        {
            "id": "older-acc",
            "institution_name": "Charles Schwab",
            "connection_broker": "Charles Schwab",
            "number": "1111",
            "brokerage_authorization": "auth-old",
        },
        {
            "id": "new-empower",
            "institution_name": "Coinbase",
            "connection_broker": "Empower",
            "number": "2222",
            "brokerage_authorization": "auth-new",
        },
    ]
    snap, kicked = _wire_portal_return(monkeypatch, prior=prior, remote=remote)
    resp, messages = _callback(snap, return_to="/profile?tab=account")

    assert resp.status_code == 302
    assert "sync/processing" not in (resp.location or "")
    assert "first=1" not in (resp.location or "")
    assert "Why are you here" not in messages
    assert "Empower connected, syncing now" in messages
    assert kicked == [7]


def test_grant_name_wins_when_the_account_names_a_different_broker():
    label = _broker_label_for_account({
        "institution_name": "Coinbase",
        "connection_broker": "Empower",
    })
    assert label == "Empower"
    assert _connected_syncing_message(["Empower"]) == "Empower connected, syncing now"


def test_account_list_copies_the_authorization_brokerage():
    class Connections:
        @staticmethod
        def list_brokerage_authorizations(**_kwargs):
            return [{
                "id": "auth-empower",
                "brokerage": {"name": "Empower", "slug": "EMPOWER"},
            }]

        @staticmethod
        def list_brokerage_authorization_accounts(*, authorization_id, **_kwargs):
            return [{
                "id": "acct-made-up",
                "institution_name": "Coinbase",
                "brokerage_authorization": authorization_id,
            }]

    accounts, complete = _list_snaptrade_accounts(
        type("Client", (), {"connections": Connections()})(),
        {"snaptrade_user_id": "user", "snaptrade_secret": "secret"},
    )
    assert complete is True
    assert accounts[0]["connection_broker"] == "Empower"
    assert _broker_label_for_account(accounts[0]) == "Empower"
