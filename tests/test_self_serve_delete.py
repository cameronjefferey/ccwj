"""Self-serve account deletion (/profile/delete-account).

The route reuses the admin-delete machinery (tenant-resolved warehouse purge
via purge_user_id_from_seeds, Postgres cascade via delete_user) plus a
SnapTrade deregistration step. These tests pin the guards and the
failure-ordering contract:

  * wrong password / wrong confirmation text → nothing deleted
  * admin accounts refused (can't lock everyone out of /admin)
  * warehouse purge failure ABORTS the whole delete (no half-deleted
    state where Postgres is gone but BQ rows live on)
  * happy path: purge → SnapTrade deregister (best-effort) → Postgres
    delete → session logged out

External effects (BQ purge, SnapTrade API) are monkeypatched — the
route's contract is the ordering and the refusal branches, not the
side effects themselves (purge has its own tests in test_upload_merge).
"""
import os
import uuid

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL not set; skipping DB-dependent tests",
)


def _unique_username(prefix: str = "test_del") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _create_user(conn, username: str, password: str = "testpass123") -> int:
    from werkzeug.security import generate_password_hash

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (username, password_hash, email) VALUES (%s, %s, %s) RETURNING id",
            (username, generate_password_hash(password), f"{username}@example.com"),
        )
        row = cur.fetchone()
    conn.commit()
    return int(row["id"])


def _user_exists(conn, user_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM users WHERE id = %s", (user_id,))
        return cur.fetchone() is not None


def _login(client, username: str, password: str = "testpass123"):
    return client.post(
        "/login", data={"username": username, "password": password},
        follow_redirects=True,
    )


@pytest.fixture
def patched_externals(monkeypatch):
    """Neutralize external effects; record whether they ran."""
    calls = {"stripe": [], "purged": [], "snaptrade": []}

    def fake_cancel_subscription(user_id):
        calls["stripe"].append(user_id)
        return True, None

    import app.billing as billing_mod
    monkeypatch.setattr(
        billing_mod,
        "cancel_subscription_for_account_deletion",
        fake_cancel_subscription,
    )

    def fake_purge(user_id, *, commit_message):
        calls["purged"].append(user_id)
        return True, None, {"seeds/trade_history.csv": 3}, "dispatch:123"

    import app.upload as upload_mod
    monkeypatch.setattr(upload_mod, "purge_user_id_from_seeds", fake_purge)

    # No snaptrade_users row exists for these test users, so the route's
    # deregistration block is a no-op — but patch the client factory
    # anyway so an accidental call can never hit the real API.
    import app.snaptrade as snaptrade_mod
    monkeypatch.setattr(
        snaptrade_mod, "_get_snaptrade_client",
        lambda: (_ for _ in ()).throw(AssertionError("must not build real client")),
    )
    return calls


class TestSelfServeDelete:
    def test_wrong_password_refuses(self, client, db_conn, patched_externals):
        username = _unique_username()
        uid = _create_user(db_conn, username)
        _login(client, username)
        resp = client.post(
            "/profile/delete-account",
            data={"password": "not-the-password", "confirm_text": "DELETE"},
            follow_redirects=True,
        )
        assert resp.status_code == 200
        assert _user_exists(db_conn, uid)
        assert patched_externals["stripe"] == []
        assert patched_externals["purged"] == []
        client.post("/logout")

    def test_wrong_confirm_text_refuses(self, client, db_conn, patched_externals):
        username = _unique_username()
        uid = _create_user(db_conn, username)
        _login(client, username)
        resp = client.post(
            "/profile/delete-account",
            data={"password": "testpass123", "confirm_text": "delete"},
            follow_redirects=True,
        )
        assert resp.status_code == 200
        assert _user_exists(db_conn, uid)
        assert patched_externals["stripe"] == []
        assert patched_externals["purged"] == []
        client.post("/logout")

    def test_stripe_failure_aborts_before_warehouse(
        self, client, db_conn, patched_externals, monkeypatch
    ):
        username = _unique_username()
        uid = _create_user(db_conn, username)

        import app.billing as billing_mod
        monkeypatch.setattr(
            billing_mod,
            "cancel_subscription_for_account_deletion",
            lambda user_id: (False, "stripe down"),
        )
        _login(client, username)
        resp = client.post(
            "/profile/delete-account",
            data={"password": "testpass123", "confirm_text": "DELETE"},
            follow_redirects=True,
        )
        assert resp.status_code == 200
        assert _user_exists(db_conn, uid)
        assert patched_externals["purged"] == []
        client.post("/logout")

    def test_purge_failure_aborts_before_postgres(self, client, db_conn, monkeypatch):
        username = _unique_username()
        uid = _create_user(db_conn, username)

        import app.upload as upload_mod
        monkeypatch.setattr(
            upload_mod, "purge_user_id_from_seeds",
            lambda user_id, *, commit_message: (False, "bq down", {}, None),
        )
        _login(client, username)
        resp = client.post(
            "/profile/delete-account",
            data={"password": "testpass123", "confirm_text": "DELETE"},
            follow_redirects=True,
        )
        assert resp.status_code == 200
        # Warehouse purge failed → Postgres user must survive.
        assert _user_exists(db_conn, uid)
        client.post("/logout")

    def test_happy_path_deletes_and_logs_out(self, client, db_conn, patched_externals):
        username = _unique_username()
        uid = _create_user(db_conn, username)
        _login(client, username)
        resp = client.post(
            "/profile/delete-account",
            data={"password": "testpass123", "confirm_text": "DELETE"},
            follow_redirects=True,
        )
        assert resp.status_code == 200
        assert not _user_exists(db_conn, uid)
        assert patched_externals["stripe"] == [uid]
        assert patched_externals["purged"] == [uid]
        # Session is gone: a login-required page must bounce to /login.
        follow = client.get("/daily-review", follow_redirects=False)
        assert follow.status_code in (301, 302)
        assert "/login" in (follow.headers.get("Location") or "")

    def test_admin_refused(self, client, db_conn, patched_externals, monkeypatch):
        username = _unique_username("test_admin")
        uid = _create_user(db_conn, username)

        import app.models as models_mod
        real_is_admin = models_mod.is_admin
        monkeypatch.setattr(
            models_mod, "is_admin",
            lambda u: True if u == username else real_is_admin(u),
        )
        _login(client, username)
        resp = client.post(
            "/profile/delete-account",
            data={"password": "testpass123", "confirm_text": "DELETE"},
            follow_redirects=True,
        )
        assert resp.status_code == 200
        assert _user_exists(db_conn, uid)
        assert patched_externals["stripe"] == []
        assert patched_externals["purged"] == []
        client.post("/logout")
