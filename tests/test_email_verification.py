"""Email verification write-gate (app/auth.email_block_writes) and the
verify-email redirect landing on account setup, not Overview.

Unverified signups must not open SnapTrade connections or dispatch
warehouse rebuilds. Mirrors plan_block_writes: HTML redirect, JSON 403,
admin exempt, fail-open.

A brand-new signup has nothing to show on Overview yet — the actual next
step after confirming an email address is connecting/uploading a
brokerage, so ``verify_email`` should redirect there. Those tests need a
real Postgres user + login session (``TEST_DATABASE_URL``, see
conftest.py) and are skipped otherwise.
"""
import os
import types
import uuid

import pytest

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


# ---------------------------------------------------------------------------
# verify_email redirect target — requires a real Postgres user + login
# session, so these are DB-gated (skipped without TEST_DATABASE_URL).
# ---------------------------------------------------------------------------

pytestmark_db = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL not set; skipping DB-dependent tests",
)


def _unique_username(prefix: str = "test_verify") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _create_user(conn, username, password="testpass123", email=None):
    from werkzeug.security import generate_password_hash

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (username, password_hash, email) VALUES (%s, %s, %s) RETURNING id",
            (username, generate_password_hash(password), email),
        )
        row = cur.fetchone()
    conn.commit()
    return int(row["id"])


@pytestmark_db
def test_verify_email_redirects_authenticated_user_to_get_started(client, db_conn):
    from app.models import mint_email_verification_token

    client.post("/logout")
    username = _unique_username()
    email = f"{username}@example.com"
    user_id = _create_user(db_conn, username, email=email)

    client.post(
        "/login",
        data={"username": username, "password": "testpass123"},
        follow_redirects=False,
    )

    token = mint_email_verification_token(user_id)
    resp = client.get(f"/verify-email/{token}", follow_redirects=False)

    assert resp.status_code == 302
    assert resp.headers["Location"].rstrip("/").endswith("/get-started")
    client.post("/logout")


@pytestmark_db
def test_verify_email_redirects_anonymous_user_to_login(client, db_conn):
    from app.models import mint_email_verification_token

    client.post("/logout")
    username = _unique_username()
    email = f"{username}@example.com"
    user_id = _create_user(db_conn, username, email=email)

    token = mint_email_verification_token(user_id)
    resp = client.get(f"/verify-email/{token}", follow_redirects=False)

    assert resp.status_code == 302
    assert resp.headers["Location"].rstrip("/").endswith("/login")
