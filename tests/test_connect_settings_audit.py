"""Settings, signup, and connect-broker audit (Oct 28, 2026).

User-facing states that were still wrong after the Oct 20 pass:
a paper connect lost ``paper=1`` on the nickname step, a CSV-only
Settings badge said Not connected, a same-day reconnect said
"since today", and save failures told the person to check server logs.
"""
from pathlib import Path

from app import app
from app.models import User
from tests.test_onboarding_settings_audit import _flashes, _login


ROOT = Path(__file__).resolve().parents[1]


def test_paper_connect_keeps_practice_through_the_nickname_step(monkeypatch):
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
            "id": "paper-acc",
            "institution_name": "Alpaca Paper",
            "number": "1111",
            "brokerage_authorization": "auth-paper",
        }], True),
    )

    def _accounts(uid):
        if not _accounts.calls:
            _accounts.calls += 1
            return []
        _accounts.calls += 1
        return [{
            "snaptrade_account_id": "paper-acc",
            "tenant_id": "snaptrade:paper-acc",
            "first_sync_completed": False,
            "account_name": "Alpaca Paper Account",
            "display_nickname": None,
        }]

    _accounts.calls = 0
    monkeypatch.setattr(snap, "get_snaptrade_accounts", _accounts)
    monkeypatch.setattr(
        snap, "_ensure_snaptrade_tenant_id",
        lambda **kwargs: "snaptrade:paper-acc",
    )
    monkeypatch.setattr(
        snap, "get_broker_tenant",
        lambda tid: {
            "account_name": "Alpaca Paper Account",
            "display_nickname": None,
        },
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
    monkeypatch.setattr("app.plan.user_has_real_brokerage", lambda uid: False)
    monkeypatch.setattr(
        "app.paper_accounts.note_connect_view", lambda *a, **k: None,
    )
    monkeypatch.setattr(snap, "_kick_post_connect_sync", lambda uid: None)

    with app.test_request_context("/snaptrade/callback"):
        from flask import session
        session["snaptrade_callback_user_id"] = 7
        resp = snap.snaptrade_callback.__wrapped__()

    location = resp.location or ""
    assert resp.status_code == 302
    assert "name-now" in location
    assert "paper=1" in location
    assert "first=1" in location
    assert "connecting=1" in location


def test_nickname_step_keeps_paper_and_does_not_claim_a_blank_save(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    rows = [{
        "snaptrade_account_id": "paper-acc",
        "tenant_id": "snaptrade:paper-acc",
        "account_name": "Alpaca Paper Account",
        "display_nickname": None,
    }]
    monkeypatch.setattr("app.snaptrade.get_snaptrade_accounts", lambda uid: rows)
    saved = []
    monkeypatch.setattr(
        "app.snaptrade.update_snaptrade_account_nickname",
        lambda uid, aid, nick: saved.append((aid, nick)) or True,
    )

    client = app.test_client()
    _login(client, monkeypatch)
    page = client.get(
        "/snaptrade/accounts/name-now?connecting=1&first=1&paper=1",
    )
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert "paper=1" in html
    assert "Skip for now" in html

    blank = client.post(
        "/snaptrade/accounts/name-now?connecting=1&first=1&paper=1",
        data={"nickname_paper-acc": "   "},
    )
    assert blank.status_code == 302
    assert "paper=1" in (blank.location or "")
    assert "sync/processing" in (blank.location or "")
    text = " ".join(msg for _cat, msg in _flashes(client))
    assert "nicknames saved" not in text.lower()
    assert saved == []

    named = client.post(
        "/snaptrade/accounts/name-now?connecting=1&first=1&paper=1",
        data={"nickname_paper-acc": "Practice"},
    )
    assert named.status_code == 302
    assert "paper=1" in (named.location or "")
    text = " ".join(msg for _cat, msg in _flashes(client))
    assert "nicknames saved" in text.lower()
    assert saved == [("paper-acc", "Practice")]


def test_nickname_step_stays_put_when_the_save_fails(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_accounts",
        lambda uid: [{
            "snaptrade_account_id": "acct-1",
            "account_name": "Schwab Account",
        }],
    )
    monkeypatch.setattr(
        "app.snaptrade.update_snaptrade_account_nickname",
        lambda uid, aid, nick: False,
    )
    client = app.test_client()
    _login(client, monkeypatch)
    resp = client.post(
        "/snaptrade/accounts/name-now?connecting=1&first=1",
        data={"nickname_acct-1": "Roth"},
    )
    assert resp.status_code == 302
    assert "name-now" in (resp.location or "")
    text = " ".join(msg for _cat, msg in _flashes(client))
    assert "Couldn't save those nicknames" in text
    assert "nicknames saved" not in text.lower()


def test_settings_save_failures_do_not_mention_server_logs(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setattr(
        "app.paper_accounts.set_app_view", lambda *a, **k: False,
    )
    monkeypatch.setattr(
        "app.profile_page.update_user_profile", lambda *a, **k: False,
    )
    client = app.test_client()
    _login(client, monkeypatch)

    view = client.post("/profile?tab=preferences", data={
        "action": "set_app_view",
        "app_view": "simple",
    })
    assert view.status_code == 302
    view_text = " ".join(msg for _cat, msg in _flashes(client))
    assert "Couldn't save that view. Try again." in view_text
    assert "server logs" not in view_text.lower()

    notes = client.post("/profile?tab=notifications", data={
        "action": "save_profile",
        "settings_tab": "notifications",
    })
    assert notes.status_code == 302
    notes_text = " ".join(msg for _cat, msg in _flashes(client))
    assert "Couldn't save notification settings. Try again." in notes_text
    assert "server logs" not in notes_text.lower()

    profile = client.post("/profile?tab=preferences", data={
        "action": "save_profile",
        "settings_tab": "preferences",
        "timezone": "America/New_York",
        "display_name": "Ada",
        "default_route": "weekly_review",
    })
    assert profile.status_code == 302
    profile_text = " ".join(msg for _cat, msg in _flashes(client))
    assert "Couldn't save that profile. Try again." in profile_text
    assert "server logs" not in profile_text.lower()
    assert "migration" not in profile_text.lower()


def test_password_change_on_the_legacy_settings_post_lands_on_security(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)

    class _User(User):
        def check_password(self, password):
            return password == "old-pass-1"

    user = _User(7, "ada", "hash", "ada@example.com")
    monkeypatch.setattr(User, "get_by_id", staticmethod(lambda uid: user))
    updated = {}
    monkeypatch.setattr(
        User, "update_password",
        staticmethod(lambda uid, pw: updated.update(uid=uid, pw=pw)),
    )
    client = app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = "7"
        sess["_fresh"] = True
    resp = client.post("/settings", data={
        "current_password": "old-pass-1",
        "new_password": "new-pass-1",
        "confirm_password": "new-pass-1",
    })
    assert resp.status_code == 302
    assert "tab=security" in (resp.location or "")
    assert updated["pw"] == "new-pass-1"


def test_same_day_reconnect_copy_does_not_say_since_today():
    banner = (ROOT / "app/templates/base.html").read_text()
    assert "since today" not in banner
    assert "it just stopped syncing" in banner
    accounts = (ROOT / "app/templates/snaptrade_accounts.html").read_text()
    assert "Opens the connection portal" not in accounts
    assert "not a lifetime archive" in accounts


def test_survey_skip_waits_for_a_saved_skip():
    page = (ROOT / "app/templates/sync_processing.html").read_text()
    handler = page.split("dismissBtn.addEventListener", 1)[1].split(
        "form.addEventListener", 1,
    )[0]
    assert handler.index("if (preview)") < handler.index("fetch(")
    fetch_part = handler.split("fetch(", 1)[1]
    assert fetch_part.index("res.ok") < fetch_part.index("leaveSurvey()")
    assert "removeChild(card)" not in handler.split("fetch(", 1)[0]


def test_auth_forms_do_not_disable_the_submit_button_before_the_request():
    login = (ROOT / "app/templates/login.html").read_text()
    signup = (ROOT / "app/templates/signup.html").read_text()
    assert login.count("bootstrap.bundle.min.js") == 1
    login_listener = login.split('addEventListener("submit"', 1)[1]
    signup_listener = signup.split("Turnstile cancels", 1)[1]
    for listener in (login_listener, signup_listener):
        assert "setTimeout" in listener
        assert listener.index("setTimeout") < listener.index("btn.disabled = true")
    assert "data-turnstile-ok" in signup_listener
    assert signup_listener.index("data-turnstile-ok") < signup_listener.index(
        'btn.setAttribute("data-submitting"',
    )
