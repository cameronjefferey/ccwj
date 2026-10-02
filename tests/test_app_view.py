"""Simple view follows the learning path, and a chosen view stays put.

Signup has no view picker. The onboarding paper button and a first
connection that is only Alpaca Paper set Simple. Settings, the hold
card, and the Full-view prompt record a choice that those paths will
not replace. The first real brokerage offers a switch and leaves the
view alone.
"""
from urllib.parse import urlparse

from app import app
from app.models import User
from app.paper_accounts import (
    apply_learning_view,
    connect_view_action,
    dismiss_full_view_offer,
    note_connect_view,
    offer_full_view_prompt,
    set_app_view,
)


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


def _login(client, monkeypatch, user=None):
    user = user or _SessionUser()

    def get_by_id(user_id):
        if str(user_id) == str(user.id):
            return user
        return None

    monkeypatch.setattr(User, "get_by_id", staticmethod(get_by_id))
    with client.session_transaction() as sess:
        sess["_user_id"] = str(user.id)
        sess["_fresh"] = True
    return user


def _capture_sql(monkeypatch):
    sqls = []

    def execute(sql, params=None):
        sqls.append((" ".join(sql.split()), params))

    monkeypatch.setattr("app.db.execute", execute)
    monkeypatch.setattr("app.models._postgres_user_id", lambda uid: int(uid))
    return sqls


def test_learning_view_writes_simple_only_when_unchosen(monkeypatch):
    sqls = _capture_sql(monkeypatch)
    assert apply_learning_view(7) is True
    assert set_app_view(7, "full") is False
    sql, params = sqls[0]
    assert "app_view = 'simple'" in sql
    assert "app_view_chosen = FALSE" in sql
    assert params == (7,)
    assert sqls[1:] == []


def test_a_click_records_the_choice_and_clears_the_prompt(monkeypatch):
    sqls = _capture_sql(monkeypatch)
    assert set_app_view(7, "full", chosen=True) is True
    sql, params = sqls[0]
    assert "app_view = %s" in sql
    assert "app_view_chosen = TRUE" in sql
    assert "full_view_offer = FALSE" in sql
    assert params == ("full", 7)


def test_full_view_prompt_does_not_change_the_view(monkeypatch):
    sqls = _capture_sql(monkeypatch)
    assert offer_full_view_prompt(7) is True
    assert dismiss_full_view_offer(7) is True
    offer_sql, offer_params = sqls[0]
    dismiss_sql, dismiss_params = sqls[1]
    assert "full_view_offer = TRUE" in offer_sql
    assert "app_view = 'simple'" in offer_sql
    assert "SET app_view" not in offer_sql
    assert offer_params == (7,)
    assert "full_view_offer = FALSE" in dismiss_sql
    assert dismiss_params == (7,)


def test_connect_view_action_matrix():
    paper = dict(had_real=False, newly_saved=1, paper_saved=1)
    real = dict(had_real=False, newly_saved=1, paper_saved=0)
    mixed = dict(had_real=False, newly_saved=2, paper_saved=1)
    assert connect_view_action(**paper) == "simple"
    assert connect_view_action(**real) == "offer_full"
    assert connect_view_action(**mixed) == "offer_full"
    assert connect_view_action(had_real=True, newly_saved=1, paper_saved=1) is None
    assert connect_view_action(had_real=None, newly_saved=1, paper_saved=1) is None
    assert connect_view_action(had_real=False, newly_saved=0, paper_saved=0) is None


def test_note_connect_view_sets_simple_or_offers(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "app.paper_accounts.apply_learning_view",
        lambda uid: calls.append(("simple", uid)) or True,
    )
    monkeypatch.setattr(
        "app.paper_accounts.offer_full_view_prompt",
        lambda uid: calls.append(("offer", uid)) or True,
    )
    assert note_connect_view(3, had_real=False, newly_saved=1, paper_saved=1) == "simple"
    assert note_connect_view(3, had_real=False, newly_saved=1, paper_saved=0) == "offer_full"
    assert note_connect_view(3, had_real=True, newly_saved=1, paper_saved=0) is None
    assert calls == [("simple", 3), ("offer", 3)]


def test_paper_onboarding_asks_for_simple_without_forcing_it(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    seen = []
    monkeypatch.setattr(
        "app.paper_accounts.apply_learning_view",
        lambda uid: seen.append(uid) or True,
    )
    client = app.test_client()
    user = _login(client, monkeypatch)
    resp = client.post("/get-started/paper")
    assert resp.status_code == 302
    assert urlparse(resp.headers["Location"]).path == "/practice"
    assert seen == [user.id]


def test_settings_and_the_prompt_record_a_choice(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    calls = []

    def _set(uid, view, chosen=False):
        calls.append((uid, view, chosen))
        return True

    monkeypatch.setattr("app.paper_accounts.set_app_view", _set)
    dismissed = []
    monkeypatch.setattr(
        "app.paper_accounts.dismiss_full_view_offer",
        lambda uid: dismissed.append(uid) or True,
    )
    client = app.test_client()
    user = _login(client, monkeypatch)
    switched = client.post("/profile", data={
        "action": "set_app_view",
        "app_view": "full",
        "from": "offer",
    })
    assert switched.status_code == 302
    assert urlparse(switched.headers["Location"]).path in {"/overview", "/weekly-review"}
    kept = client.post("/profile", data={
        "action": "dismiss_full_view_offer",
        "next": "/practice",
    })
    assert kept.status_code == 302
    assert urlparse(kept.headers["Location"]).path == "/practice"
    assert calls == [(user.id, "full", True)]
    assert dismissed == [user.id]


def test_full_view_prompt_renders_for_a_simple_user(monkeypatch):
    monkeypatch.setattr("app.paper_accounts.viewer_flags", lambda uid: (True, True))
    monkeypatch.setattr("app.paper_accounts.full_view_offer_open", lambda uid: True)
    client = app.test_client()
    _login(client, monkeypatch)
    html = client.get("/learn").get_data(as_text=True)
    assert "A real brokerage is connected." in html
    assert "Switch to Full view" in html
    assert 'name="from" value="offer"' in html
    assert "Keep Simple" in html
    assert 'name="action" value="dismiss_full_view_offer"' in html

    monkeypatch.setattr("app.paper_accounts.full_view_offer_open", lambda uid: False)
    quiet = client.get("/learn").get_data(as_text=True)
    assert "A real brokerage is connected." not in quiet
