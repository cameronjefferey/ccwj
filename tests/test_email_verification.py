"""Email verification write-gate (app/auth.email_block_writes).

Unverified signups must not open SnapTrade connections or dispatch
warehouse rebuilds. Mirrors plan_block_writes: HTML redirect, JSON 403,
admin exempt, fail-open.
"""
import types

from app import auth as auth_mod


def _fake_user(uid=1, authenticated=True, username="cam"):
    return types.SimpleNamespace(
        is_authenticated=authenticated, id=uid, username=username,
    )


def test_email_block_writes_passes_when_verified(monkeypatch):
    from app import app as flask_app

    monkeypatch.setattr(auth_mod, "current_user", _fake_user())
    monkeypatch.setattr(auth_mod, "email_needs_verification", lambda uid: False)
    monkeypatch.setattr("app.models.is_admin", lambda u: False)
    with flask_app.test_request_context("/upload", method="POST"):
        assert auth_mod.email_block_writes("testing") is None


def test_email_block_writes_blocks_unverified_html(monkeypatch):
    from app import app as flask_app

    monkeypatch.setattr(auth_mod, "current_user", _fake_user())
    monkeypatch.setattr(auth_mod, "email_needs_verification", lambda uid: True)
    monkeypatch.setattr("app.models.is_admin", lambda u: False)
    with flask_app.test_request_context("/upload", method="POST"):
        resp = auth_mod.email_block_writes("connecting a brokerage account")
        assert resp is not None
        assert resp.status_code == 302
        assert "/profile" in resp.headers["Location"]


def test_email_block_writes_blocks_unverified_json(monkeypatch):
    from app import app as flask_app

    monkeypatch.setattr(auth_mod, "current_user", _fake_user())
    monkeypatch.setattr(auth_mod, "email_needs_verification", lambda uid: True)
    monkeypatch.setattr("app.models.is_admin", lambda u: False)
    with flask_app.test_request_context(
        "/api/something", method="POST",
        headers={"X-Requested-With": "XMLHttpRequest"},
    ):
        resp, status = auth_mod.email_block_writes("testing")
        assert status == 403
        assert resp.get_json()["error"] == "email_unverified"


def test_email_block_writes_exempts_admin(monkeypatch):
    from app import app as flask_app

    monkeypatch.setattr(auth_mod, "current_user", _fake_user(username="admin"))
    monkeypatch.setattr(auth_mod, "email_needs_verification", lambda uid: True)
    monkeypatch.setattr("app.models.is_admin", lambda u: u == "admin")
    with flask_app.test_request_context("/upload", method="POST"):
        assert auth_mod.email_block_writes("testing") is None


def test_email_block_writes_fails_open_on_error(monkeypatch):
    from app import app as flask_app

    monkeypatch.setattr(auth_mod, "current_user", _fake_user())

    def _boom(uid):
        raise RuntimeError("db down")

    monkeypatch.setattr(auth_mod, "email_needs_verification", _boom)
    with flask_app.test_request_context("/upload", method="POST"):
        assert auth_mod.email_block_writes("testing") is None


def test_email_block_writes_ignores_anonymous(monkeypatch):
    from app import app as flask_app

    monkeypatch.setattr(auth_mod, "current_user", _fake_user(authenticated=False))
    with flask_app.test_request_context("/upload", method="POST"):
        assert auth_mod.email_block_writes("testing") is None
