"""Public demo gate, origin redirect, and response headers."""
import json
import time

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
    cookie = resp.headers.get("Set-Cookie") or ""
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
    from app.demo_guard import reset_demo_caps
    reset_demo_caps()
    client = _client(app)
    remote = {"REMOTE_ADDR": "203.0.113.50"}
    for _ in range(3):
        resp = client.post("/demo/start", environ_base=remote, follow_redirects=False)
        assert resp.status_code == 302
        assert client.post("/logout", environ_base=remote).status_code in (302, 200)
    blocked = client.post("/demo/start", environ_base=remote, follow_redirects=False)
    assert blocked.status_code == 429
    assert "3 demos today" in blocked.get_data(as_text=True)


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


def test_demo_start_rate_limit_returns_friendly_429(app, monkeypatch):
    from app.extensions import limiter

    _csrf_off(monkeypatch, app)
    monkeypatch.setitem(app.config, "RATELIMIT_ENABLED", True)
    limiter.reset()
    try:
        client = _client(app)
        remote = {"REMOTE_ADDR": "203.0.113.77"}
        first = client.post("/demo/start", environ_base=remote, follow_redirects=False)
        assert first.status_code == 302
        client.post("/logout", environ_base=remote)
        second = client.post("/demo/start", environ_base=remote, follow_redirects=False)
        assert second.status_code == 429
        body = second.get_data(as_text=True)
        assert "a little fast" in body
        assert "card" in body
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
