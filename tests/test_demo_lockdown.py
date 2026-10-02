"""Public demo gate, origin redirect, and response headers."""
import json
import time
from types import SimpleNamespace

import pytest
from flask_login import login_user


def _client(app):
    return app.test_client()


def _csrf_off(monkeypatch, app):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.delenv("TURNSTILE_SITE_KEY", raising=False)
    monkeypatch.delenv("TURNSTILE_SECRET_KEY", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)


def test_demo_start_get_does_not_log_in(app):
    client = _client(app)
    resp = client.get("/demo/start")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'method="POST"' in body
    assert "Start the demo" in body
    with client.session_transaction() as sess:
        assert "_user_id" not in sess


def test_demo_start_post_creates_ephemeral_session(app, monkeypatch):
    _csrf_off(monkeypatch, app)
    client = _client(app)
    resp = client.post("/demo/start", follow_redirects=False)
    assert resp.status_code == 302
    # The demo login stays a browser session. The first-party attribution
    # cookie is separate and is allowed to carry a Max-Age.
    session_cookies = [
        c for c in resp.headers.getlist("Set-Cookie") if c.startswith("session=")
    ]
    assert session_cookies
    for cookie in session_cookies:
        assert "604800" not in cookie
        assert "Max-Age" not in cookie
    with client.session_transaction() as sess:
        uid = str(sess.get("_user_id") or "")
        assert uid.startswith("demo-session:")
        assert uid != "1"
        assert sess.get("_demo_started_at")


def test_demo_start_post_without_csrf_does_not_log_in(app, monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", True)
    client = _client(app)
    resp = client.post("/demo/start", follow_redirects=False)
    assert resp.status_code == 400
    with client.session_transaction() as sess:
        assert "_user_id" not in sess


def test_turnstile_success_and_failure(app, monkeypatch):
    _csrf_off(monkeypatch, app)
    monkeypatch.setenv("TURNSTILE_SITE_KEY", "site-key")
    monkeypatch.setenv("TURNSTILE_SECRET_KEY", "secret-key")
    seen = {}

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def read(self):
            return json.dumps(self._payload).encode()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def _open(req, timeout=5):
        seen["url"] = req.full_url
        seen["body"] = req.data.decode()
        return _Resp({"success": True})

    monkeypatch.setattr("app.demo_guard.urllib.request.urlopen", _open)
    client = _client(app)
    ok = client.post(
        "/demo/start",
        data={"cf-turnstile-response": "tok"},
        follow_redirects=False,
    )
    assert ok.status_code == 302
    assert "challenges.cloudflare.com/turnstile/v0/siteverify" in seen["url"]
    assert "secret=secret-key" in seen["body"]
    assert "response=tok" in seen["body"]

    client.post("/logout")

    def _reject(req, timeout=5):
        return _Resp({"success": False})

    monkeypatch.setattr("app.demo_guard.urllib.request.urlopen", _reject)
    bad = client.post(
        "/demo/start",
        data={"cf-turnstile-response": "nope"},
        follow_redirects=False,
    )
    assert bad.status_code == 400
    with client.session_transaction() as sess:
        assert "_user_id" not in sess

    missing = client.post("/demo/start", data={}, follow_redirects=False)
    assert missing.status_code == 400


def test_turnstile_unset_skips_and_warns(app, monkeypatch, caplog):
    _csrf_off(monkeypatch, app)
    from app.demo_guard import reset_demo_caps
    reset_demo_caps()
    client = _client(app)
    with caplog.at_level("WARNING"):
        resp = client.post("/demo/start", follow_redirects=False)
    assert resp.status_code == 302
    assert any("TURNSTILE_SITE_KEY" in rec.message for rec in caplog.records)


def test_per_ip_demo_cap(app, monkeypatch):
    _csrf_off(monkeypatch, app)
    from app.demo_guard import IP_SESSION_CAP, reset_demo_caps
    reset_demo_caps()
    client = _client(app)
    remote = {"REMOTE_ADDR": "203.0.113.50"}
    for _ in range(IP_SESSION_CAP):
        client.delete_cookie("ht_demo", path="/")
        resp = client.post("/demo/start", environ_base=remote, follow_redirects=False)
        assert resp.status_code == 302
        assert client.post("/logout", environ_base=remote).status_code in (302, 200)
    client.delete_cookie("ht_demo", path="/")
    blocked = client.post("/demo/start", environ_base=remote, follow_redirects=False)
    assert blocked.status_code == 429
    body = blocked.get_data(as_text=True)
    assert f"{IP_SESSION_CAP} demos today" in body
    assert "Slow down" not in body
    assert "Create an account" in body


def test_page_cap_stops_the_session(app, monkeypatch):
    _csrf_off(monkeypatch, app)
    from app.demo_guard import note_demo_page, reset_demo_caps
    reset_demo_caps()
    client = _client(app)
    assert client.post("/demo/start", follow_redirects=False).status_code == 302
    with client.session_transaction() as sess:
        token = sess["_demo_token"]
    for _ in range(150):
        assert note_demo_page(token)
    resp = client.get("/overview")
    assert resp.status_code == 429
    assert "150 pages" in resp.get_data(as_text=True)


def test_demo_idle_and_hard_limit(app, monkeypatch):
    _csrf_off(monkeypatch, app)
    client = _client(app)
    assert client.post("/demo/start", follow_redirects=False).status_code == 302
    with client.session_transaction() as sess:
        sess["_demo_started_at"] = time.time() - 100
        sess["_last_activity_ts"] = time.time() - (31 * 60)
    idle = client.get("/overview", follow_redirects=False)
    assert idle.status_code == 302
    assert "/demo/start" in (idle.headers.get("Location") or "")

    assert client.post("/demo/start", follow_redirects=False).status_code == 302
    with client.session_transaction() as sess:
        sess["_demo_started_at"] = time.time() - (2 * 60 * 60 + 5)
        sess["_last_activity_ts"] = time.time()
    hard = client.get("/overview", follow_redirects=False)
    assert hard.status_code == 302
    assert "/demo/start" in (hard.headers.get("Location") or "")


def test_shared_demo_cookie_is_retired_on_app_pages(app, monkeypatch):
    from flask_login import UserMixin
    from app.models import User

    class _Shared(UserMixin):
        id = 1
        username = "demo"

        def get_id(self):
            return "1"

    monkeypatch.setattr(User, "get_by_id", staticmethod(lambda uid: _Shared() if str(uid) == "1" else None))
    client = _client(app)
    with client.session_transaction() as sess:
        sess["_user_id"] = "1"
        sess["_fresh"] = True
    resp = client.get("/overview", follow_redirects=False)
    assert resp.status_code == 302
    assert "/demo/start" in (resp.headers.get("Location") or "")
    with client.session_transaction() as sess:
        assert sess.get("_user_id") != "1"


def test_demo_cannot_be_admin_and_cannot_read_other_tenants(app, monkeypatch):
    from app.demo_guard import DEMO_TENANT_ID, DemoSessionUser, demo_tenant_row
    from app.models import is_admin
    from app.tenant_scope import resolve_filter_tenant_ids

    monkeypatch.setenv("ADMIN_USERS", "demo,cameron3")
    assert is_admin("demo") is False
    assert is_admin("Demo") is False
    assert is_admin("cameron3") is True

    row = demo_tenant_row()
    assert row["tenant_id"] == DEMO_TENANT_ID
    assert row["user_id"] is None

    with app.test_request_context("/position/JEPI?tenant=snaptrade:victim"):
        login_user(DemoSessionUser("isolated"))
        assert resolve_filter_tenant_ids(None) == [DEMO_TENANT_ID]
        assert resolve_filter_tenant_ids(
            ["snaptrade:victim", DEMO_TENANT_ID]
        ) == [DEMO_TENANT_ID]
        assert resolve_filter_tenant_ids(["snaptrade:victim"]) == []


def test_onrender_host_redirects_except_healthz(app):
    client = _client(app)
    resp = client.get(
        "/login?next=/overview",
        base_url="https://ccwj.onrender.com",
        follow_redirects=False,
    )
    assert resp.status_code == 301
    assert resp.headers["Location"] == "https://happytrader.me/login?next=/overview"
    health = client.get("/healthz", base_url="https://ccwj.onrender.com")
    assert health.status_code == 200
    assert health.get_data(as_text=True).startswith("ok")
    local = client.get("/login", base_url="http://notonrender.com")
    assert local.status_code == 200


def test_security_headers_and_demo_noindex(app, monkeypatch):
    _csrf_off(monkeypatch, app)
    client = _client(app)
    login = client.get("/login")
    assert login.headers["X-Content-Type-Options"] == "nosniff"
    assert login.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"
    assert login.headers["X-Frame-Options"] == "SAMEORIGIN"
    assert "max-age=31536000" in login.headers["Strict-Transport-Security"]
    assert login.headers["Content-Security-Policy"] == "frame-ancestors 'self'"
    report = login.headers["Content-Security-Policy-Report-Only"]
    assert "cdn.jsdelivr.net" in report
    assert "fonts.googleapis.com" in report
    assert "youtube-nocookie.com" in report
    assert "challenges.cloudflare.com" in report
    assert "js.stripe.com" in report
    assert "snaptrade.com" in report
    assert "X-Robots-Tag" not in login.headers

    gate = client.get("/demo/start")
    assert gate.headers["X-Robots-Tag"] == "noindex, nofollow"
    assert client.post("/demo/start", follow_redirects=False).status_code == 302
    inside = client.get("/faq")
    assert inside.headers["X-Robots-Tag"] == "noindex, nofollow"


def test_demo_start_reuses_cookie_and_limits_friendly(app, monkeypatch):
    """A return visit is the same demo. A new visitor past the daily cap
    gets the demo page, not the generic Slow down 429."""
    from app.demo_guard import IP_SESSION_CAP, reset_demo_caps
    from app.extensions import limiter

    _csrf_off(monkeypatch, app)
    reset_demo_caps()
    monkeypatch.setitem(app.config, "RATELIMIT_ENABLED", True)
    limiter.reset()
    try:
        client = _client(app)
        remote = {"REMOTE_ADDR": "203.0.113.77"}
        first = client.post("/demo/start", environ_base=remote, follow_redirects=False)
        assert first.status_code == 302
        assert any(
            c.startswith("ht_demo=") for c in first.headers.getlist("Set-Cookie")
        )
        client.post("/logout", environ_base=remote)
        second = client.post("/demo/start", environ_base=remote, follow_redirects=False)
        assert second.status_code == 302
        assert "Slow down" not in second.get_data(as_text=True)
        client.post("/logout", environ_base=remote)

        for _ in range(IP_SESSION_CAP - 1):
            client.delete_cookie("ht_demo", path="/")
            resp = client.post("/demo/start", environ_base=remote, follow_redirects=False)
            assert resp.status_code == 302
            client.post("/logout", environ_base=remote)
        client.delete_cookie("ht_demo", path="/")
        blocked = client.post("/demo/start", environ_base=remote, follow_redirects=False)
        assert blocked.status_code == 429
        body = blocked.get_data(as_text=True)
        assert f"{IP_SESSION_CAP} demos today" in body
        assert "That's the end of this demo" in body
        assert "Create an account" in body
        assert "Slow down" not in body
        assert "a little fast" not in body
    finally:
        limiter.reset()


def test_real_client_ip_prefers_cloudflare(app):
    from app.client_ip import proxy_x_for_hops, real_client_ip

    assert proxy_x_for_hops("2") == 2
    assert proxy_x_for_hops("nope") == 1
    assert proxy_x_for_hops("") == 1

    with app.test_request_context(
        "/",
        headers={"CF-Connecting-IP": "198.51.100.8", "CF-Ray": "abc123"},
        environ_base={"REMOTE_ADDR": "10.0.0.1"},
    ):
        assert real_client_ip() == "198.51.100.8"
    with app.test_request_context(
        "/",
        headers={"CF-Connecting-IP": "198.51.100.8"},
        environ_base={"REMOTE_ADDR": "10.0.0.1"},
    ):
        assert real_client_ip() == "10.0.0.1"


def test_login_rejects_demo_with_the_generic_error(app):
    """demo/demo123 must not open a session, and must look like a bad password."""
    from app.db import execute
    from app.models import User

    demo = User.get_by_username("demo")
    assert demo is not None
    # Earlier tests can leave failure rows for this username. Lockout
    # would replace the generic error this test is pinning.
    execute(
        "DELETE FROM login_attempts WHERE username_lc IN "
        "('demo', 'demo-lock@example.com')"
    )
    User.update_password(demo.id, "demo123")
    User.update_email(demo.id, "demo-lock@example.com")
    client = _client(app)
    try:
        for identifier, password in (
            ("demo", "demo123"),
            ("demo", "not-the-password"),
            ("Demo", "demo123"),
            ("demo-lock@example.com", "demo123"),
        ):
            resp = client.post(
                "/login",
                data={"username": identifier, "password": password},
                follow_redirects=True,
            )
            assert resp.status_code == 200
            body = resp.get_data(as_text=True)
            assert "Invalid username/email or password." in body
            assert "public demo starts" not in body
            with client.session_transaction() as sess:
                assert "_user_id" not in sess
    finally:
        User.update_email(demo.id, None)
        execute(
            "DELETE FROM login_attempts WHERE username_lc IN "
            "('demo', 'demo-lock@example.com')"
        )

    other = "lockdown_login_ok"
    if User.get_by_username(other) is None:
        User.create(other, "Correcthorse1")
    ok = client.post(
        "/login",
        data={"username": other, "password": "Correcthorse1"},
        follow_redirects=False,
    )
    assert ok.status_code == 302
    assert "/login" not in (ok.headers.get("Location") or "")


def test_forgot_password_and_reset_link_skip_demo(app, monkeypatch):
    from datetime import datetime, timedelta, timezone

    from app.db import execute
    from app.models import User, _hash_reset_token, mint_password_reset_token

    demo = User.get_by_username("demo")
    assert demo is not None
    with pytest.raises(ValueError, match="demo account"):
        mint_password_reset_token(demo.id)

    sent = []
    monkeypatch.setattr(
        "app.auth.send_password_reset_email", lambda **kwargs: sent.append(kwargs)
    )
    User.update_email(demo.id, "demo-reset@example.com")
    client = _client(app)
    try:
        known = client.post(
            "/forgot-password",
            data={"email": "demo-reset@example.com"},
            follow_redirects=True,
        )
        unknown = client.post(
            "/forgot-password",
            data={"email": "nobody-lockdown@example.com"},
            follow_redirects=True,
        )
    finally:
        User.update_email(demo.id, None)
    assert sent == []
    known_body = known.get_data(as_text=True)
    unknown_body = unknown.get_data(as_text=True)
    assert "If that email is on file" in known_body
    assert "If that email is on file" in unknown_body

    before = User.get_by_username("demo").password_hash
    raw = "demo-reset-token-lockdown"
    execute(
        "DELETE FROM password_reset_tokens WHERE token_hash = %s",
        (_hash_reset_token(raw),),
    )
    execute(
        """INSERT INTO password_reset_tokens
           (user_id, token_hash, expires_at, requester_ip)
           VALUES (%s, %s, %s, %s)""",
        (
            demo.id,
            _hash_reset_token(raw),
            datetime.now(timezone.utc) + timedelta(hours=1),
            "127.0.0.1",
        ),
    )
    reset = client.post(
        f"/reset-password/{raw}",
        data={"new_password": "Newpass123", "confirm_password": "Newpass123"},
        follow_redirects=True,
    )
    body = reset.get_data(as_text=True)
    assert "invalid or expired" in body
    assert "Password updated" not in body
    assert User.get_by_username("demo").password_hash == before


def test_ensure_demo_user_create_password_is_not_demo123(monkeypatch):
    from app import models

    captured = {}

    monkeypatch.setattr(
        models.User, "get_by_username", staticmethod(lambda username: None)
    )

    def _create(username, password, email=None):
        captured["username"] = username
        captured["password"] = password

    monkeypatch.setattr(models.User, "create", staticmethod(_create))
    models.ensure_demo_user()
    assert captured["username"] == "demo"
    assert captured["password"] != "demo123"
    assert len(captured["password"]) >= 32


def test_ensure_demo_user_rotates_dedicated_row_only(monkeypatch):
    from app import models

    dedicated = SimpleNamespace(id=7, username="demo", email=None)
    state = {"user": dedicated}
    updated = []

    def _get(username):
        return state["user"]

    def _update(user_id, new_password):
        updated.append((user_id, new_password))
        state["user"] = SimpleNamespace(id=user_id, username="demo", email=None)

    monkeypatch.setattr(models.User, "get_by_username", staticmethod(_get))
    monkeypatch.setattr(models.User, "update_password", staticmethod(_update))
    for name in (
        "remove_account_for_user",
        "add_account_for_user",
        "ensure_user_profile",
        "_ensure_demo_insight",
        "_seed_demo_mirror_scores",
    ):
        monkeypatch.setattr(models, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(
        models, "get_or_create_broker_tenant", lambda *args, **kwargs: "demo:demo-account"
    )
    models.ensure_demo_user()
    assert updated and updated[0][0] == 7
    assert updated[0][1] != "demo123"
    assert len(updated[0][1]) >= 32

    # A person who registered as ``demo`` (has an email, does not own the
    # demo tenant) keeps their hash.
    updated.clear()
    state["user"] = SimpleNamespace(
        id=8, username="demo", email="ada@example.com"
    )
    monkeypatch.setattr(
        models, "get_broker_tenant", lambda tenant_id: {"user_id": 99}
    )
    models.ensure_demo_user()
    assert updated == []

    # Owning the demo tenant makes an emailed row dedicated.
    state["user"] = SimpleNamespace(
        id=8, username="demo", email="ada@example.com"
    )
    monkeypatch.setattr(
        models, "get_broker_tenant", lambda tenant_id: {"user_id": 8}
    )
    models.ensure_demo_user()
    assert updated and updated[0][0] == 8


def test_demo_check_password_never_succeeds():
    from app.models import User

    user = User(id=1, username="demo", password_hash="not-a-real-hash")
    assert user.check_password("demo123") is False
    assert user.check_password("") is False
