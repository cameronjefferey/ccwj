"""Auth presentation bugs and outbound timeouts.

Signup must keep an invite the visitor typed and must not echo passwords.
A shared demo session must be able to open password recovery (same trap as
/login). GET /logout confirms; only POST ends the session. Error pages use
the house card. SnapTrade and Stripe HTTP calls cannot wait forever.
"""
from pathlib import Path
from urllib.parse import urlparse

from app import app
from app.models import User


class _SessionUser:
    is_active = True
    is_anonymous = False

    def __init__(self, username, user_id=1):
        self.id = user_id
        self.username = username

    @property
    def is_authenticated(self):
        return True

    def get_id(self):
        return str(self.id)


def _client():
    return app.test_client()


def _login(client, username, monkeypatch, user_id=1):
    user = _SessionUser(username, user_id)

    def get_by_id(uid):
        if str(uid) == str(user.id):
            return user
        return None

    monkeypatch.setattr(User, "get_by_id", staticmethod(get_by_id))
    with client.session_transaction() as sess:
        sess["_user_id"] = str(user.id)
        sess["_fresh"] = True
    return user


def _path(response):
    return urlparse(response.headers.get("Location") or "").path


def test_signup_keeps_invite_and_trial_line_without_echoing_passwords(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setitem(app.config, "SIGNUP_ENABLED", True)
    monkeypatch.setitem(app.config, "SIGNUP_INVITE_CODE", "beta-invite")
    monkeypatch.setitem(app.config, "RATELIMIT_ENABLED", False)
    monkeypatch.setattr("app.auth.User.get_by_email", staticmethod(lambda email: None))

    client = _client()
    page = client.get("/signup")
    assert page.status_code == 200
    assert "30-day free trial, no credit card" in page.get_data(as_text=True)

    resp = client.post("/signup", data={
        "username": "ada_trader",
        "email": "ada@example.com",
        "invite_code": "beta-invite",
        "password": "Secret1pass",
        "confirm": "Secret2pass",
    })
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Passwords do not match." in html
    assert 'value="beta-invite"' in html
    assert 'value="ada_trader"' in html
    assert "Secret1pass" not in html
    assert "Secret2pass" not in html
    assert 'name="password"' in html
    assert 'value="Secret1pass"' not in html


def test_demo_session_can_open_forgot_and_reset(monkeypatch):
    monkeypatch.setattr("app.auth.peek_password_reset_token", lambda token: 99)
    client = _client()
    _login(client, "demo", monkeypatch)

    forgot = client.get("/forgot-password")
    assert forgot.status_code == 200
    body = forgot.get_data(as_text=True)
    assert "Forgot password" in body
    with client.session_transaction() as sess:
        assert "_user_id" not in sess

    _login(client, "demo", monkeypatch)
    reset = client.get("/reset-password/one-time-token")
    assert reset.status_code == 200
    assert "Set a new password" in reset.get_data(as_text=True)
    with client.session_transaction() as sess:
        assert "_user_id" not in sess


def test_real_session_still_skips_password_recovery(monkeypatch):
    monkeypatch.setattr("app.auth.peek_password_reset_token", lambda token: 99)
    client = _client()
    _login(client, "alice", monkeypatch)

    forgot = client.get("/forgot-password")
    assert forgot.status_code == 302
    assert _path(forgot) == "/overview"

    reset = client.get("/reset-password/one-time-token")
    assert reset.status_code == 302
    assert _path(reset) == "/profile"
    assert "tab=account" in (reset.headers.get("Location") or "")
    with client.session_transaction() as sess:
        assert sess.get("_user_id") == "1"


def test_get_logout_confirms_and_post_ends_the_session(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    client = _client()
    anon = client.get("/logout")
    assert anon.status_code == 302
    assert _path(anon) == "/login"

    _login(client, "alice", monkeypatch)
    page = client.get("/logout")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert "Log out" in html
    assert 'method="post"' in html
    with client.session_transaction() as sess:
        assert sess.get("_user_id") == "1"

    done = client.post("/logout")
    assert done.status_code == 302
    # url_for('index') is the /index alias (registered before /).
    assert _path(done) in ("/", "/index")
    with client.session_transaction() as sess:
        assert "_user_id" not in sess


def test_nav_mobile_menu_and_settings_logout_posts():
    """GET /logout is a confirm page. Every control must POST so one
    click still ends the session. The phone menu is the same #navContent
    collapse, and Settings has no second logout link."""
    root = Path("app/templates")
    base = (root / "base.html").read_text()
    nav = base.split('id="navContent"', 1)[1].split("</nav>", 1)[0]
    assert nav.count("url_for('logout')") == 1
    before = nav.split("url_for('logout')", 1)[0]
    assert 'method="post"' in before[-240:]
    assert "navbar-toggler" in base
    assert 'data-bs-target="#navContent"' in base
    for name in ("profile.html", "settings.html"):
        text = (root / name).read_text()
        assert "logout" not in text.lower()
    for path in root.rglob("*.html"):
        text = path.read_text()
        assert 'href="/logout"' not in text
        assert "url_for('logout')" not in text or 'method="post"' in text


def test_unknown_path_is_a_house_404():
    resp = _client().get("/this-page-does-not-exist-ht")
    assert resp.status_code == 404
    html = resp.get_data(as_text=True)
    assert "Page not found" in html
    assert "btn btn-primary" in html
    assert "display-1" not in html


def test_500_template_is_a_house_card():
    from flask import render_template

    with app.test_request_context("/"):
        html = render_template("500.html")
    assert "Something went wrong" in html
    assert "btn btn-primary" in html
    assert "display-1" not in html


def test_snaptrade_timeout_replaces_none_only():
    from app.snaptrade import _SNAPTRADE_HTTP_TIMEOUT, _apply_snaptrade_timeout

    seen = {}

    class _ApiClient:
        def request(self, *args, **kwargs):
            seen["timeout"] = kwargs.get("timeout")
            return "ok"

    shared = _ApiClient()

    class _Api:
        def __init__(self):
            self.api_client = shared

    class _Client:
        account_information = _Api()
        api_status = _Api()

    client = _Client()
    wrapped = _apply_snaptrade_timeout(client)
    assert wrapped == 1
    assert client.account_information.api_client.request(method="GET", timeout=None) == "ok"
    assert seen["timeout"] == _SNAPTRADE_HTTP_TIMEOUT
    client.account_information.api_client.request(method="GET", timeout=3)
    assert seen["timeout"] == 3


def test_stripe_http_client_gets_a_timeout():
    from app.billing import _STRIPE_HTTP_TIMEOUT, _bound_stripe_http

    class _Sdk:
        default_http_client = None

    sdk = _Sdk()
    _bound_stripe_http(sdk)
    http = sdk.default_http_client
    assert http is not None
    timeout = getattr(http, "_timeout", None)
    if timeout is None:
        timeout = getattr(http, "timeout", None)
    assert timeout == _STRIPE_HTTP_TIMEOUT
    _bound_stripe_http(sdk)
    assert sdk.default_http_client is http
