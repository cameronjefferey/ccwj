"""Commercialization polish: leave the demo for signup, and no public 404s.

The shared demo is a real Flask-Login session (username ``demo``).
``/login`` and ``/signup`` used to treat every authenticated session as
"already in the app" and redirect to Overview, so the banner CTA never
reached the signup form. ``/docs`` and ``/blog`` had no routes. Browsers
also request ``/favicon.ico`` even when an SVG icon is linked.
"""

from urllib.parse import urlparse

from app import app
from app.models import User


class _SessionUser:
    is_active = True
    is_anonymous = False

    def __init__(self, username):
        self.id = 1
        self.username = username

    @property
    def is_authenticated(self):
        return True

    def get_id(self):
        return "1"


def _client():
    return app.test_client()


def _login(client, username, monkeypatch):
    user = _SessionUser(username)

    def get_by_id(user_id):
        if str(user_id) == "1":
            return user
        return None

    # load_user resolves User.get_by_id at request time.
    monkeypatch.setattr(User, "get_by_id", staticmethod(get_by_id))
    with client.session_transaction() as sess:
        sess["_user_id"] = "1"
        sess["_fresh"] = True
    return user


def _path(response):
    return urlparse(response.headers.get("Location") or "").path


def test_demo_signup_cta_reaches_the_form(monkeypatch):
    client = _client()
    _login(client, "demo", monkeypatch)

    faq = client.get("/faq")
    assert faq.status_code == 200
    html = faq.get_data(as_text=True)
    assert "Create your own account" in html
    assert 'href="/signup"' in html

    signup = client.get("/signup")
    assert signup.status_code == 200
    body = signup.get_data(as_text=True)
    assert "Create your account" in body
    assert "Demo mode" not in body

    # The shared demo session is gone, so Overview now asks them to sign in
    # instead of dropping them back into the mirror.
    overview = client.get("/overview")
    assert overview.status_code == 302
    assert _path(overview) == "/login"


def test_demo_login_page_renders_instead_of_bouncing(monkeypatch):
    client = _client()
    _login(client, "demo", monkeypatch)

    resp = client.get("/login?next=/pricing")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Sign in to your trading mirror" in body
    assert 'name="next" value="/pricing"' in body

    with client.session_transaction() as sess:
        assert "_user_id" not in sess


def test_real_account_still_skips_signup_and_login(monkeypatch):
    client = _client()
    _login(client, "alice", monkeypatch)

    signup = client.get("/signup")
    assert signup.status_code == 302
    assert _path(signup) == "/overview"

    login = client.get("/login")
    assert login.status_code == 302
    assert _path(login) == "/overview"

    with client.session_transaction() as sess:
        assert sess.get("_user_id") == "1"


def test_docs_and_blog_redirect_to_existing_pages():
    client = _client()
    docs = client.get("/docs")
    assert docs.status_code == 301
    assert _path(docs) == "/faq"

    docs_slash = client.get("/docs/")
    assert docs_slash.status_code == 301
    assert _path(docs_slash) == "/faq"

    blog = client.get("/blog")
    assert blog.status_code == 301
    assert _path(blog) == "/"

    blog_slash = client.get("/blog/")
    assert blog_slash.status_code == 301
    assert _path(blog_slash) == "/"

    # The destinations themselves are real pages, not another hop to 404.
    assert client.get("/faq").status_code == 200
    home = client.get("/")
    assert home.status_code == 200
    home_html = home.get_data(as_text=True)
    assert 'href="/docs"' not in home_html
    assert 'href="/blog"' not in home_html


def test_favicon_ico_and_linked_icons_return_200():
    client = _client()
    ico = client.get("/favicon.ico")
    assert ico.status_code == 200
    # ICO magic: reserved 0, type 1.
    assert ico.data[:4] == b"\x00\x00\x01\x00"
    assert "image" in (ico.mimetype or "")

    for path in (
        "/static/favicon.svg",
        "/static/icons/apple-touch-icon.png",
        "/static/icons/icon-192.png",
        "/static/icons/icon-512.png",
    ):
        resp = client.get(path)
        assert resp.status_code == 200, path
        assert len(resp.data) > 32

    page = client.get("/")
    html = page.get_data(as_text=True)
    assert 'rel="icon"' in html
    assert "/favicon.ico" in html
