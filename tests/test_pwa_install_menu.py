"""Account → Install app must not be a dead href=\"#\" link.

Chromium gets beforeinstallprompt.prompt(); every other browser gets the
install-steps modal. The item is omitted from the click path once the app
is already installed. Review / Portfolio / Account stay dropdown toggles.
"""

from pathlib import Path

from app import app
from app.models import User


ROOT = Path(__file__).resolve().parents[1]


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


def _login(client, monkeypatch):
    user = _SessionUser("alice")

    def get_by_id(user_id):
        if str(user_id) == "1":
            return user
        return None

    monkeypatch.setattr(User, "get_by_id", staticmethod(get_by_id))
    with client.session_transaction() as sess:
        sess["_user_id"] = "1"
        sess["_fresh"] = True


def _account_menu(html):
    start = html.find('id="userMenu"')
    assert start != -1
    end = html.find("</ul>", start)
    assert end != -1
    return html[start:end]


def test_install_control_is_a_button_and_help_modal_exists():
    template = (ROOT / "app/templates/base.html").read_text()
    script = (ROOT / "app/static/js/nav.js").read_text()

    assert 'id="ht-install-link"' in template
    assert 'href="#" id="ht-install-link"' not in template
    assert '<button type="button" class="dropdown-item" id="ht-install-link">Install app</button>' in template
    assert 'id="htInstallModal"' in template
    assert 'data-ht-install-help="ios"' in template
    assert "Add to Home Screen" in template
    assert 'data-ht-install-help="safari"' in template
    assert "Add to Dock" in template
    assert 'data-ht-install-help="chromium"' in template
    assert 'data-ht-install-help="other"' in template
    assert "html.ht-pwa-installed #ht-install-item" in template

    assert "beforeinstallprompt" in script
    assert "promptEvent.prompt()" in script
    assert "showInstallHelp()" in script
    assert "htInstallModal" in script
    assert 'if (!deferredInstall) return' not in script
    # Review / Portfolio / Account remain menu toggles on purpose.
    assert 'id="navReview" data-bs-toggle="dropdown"' in template
    assert 'id="navPortfolio" data-bs-toggle="dropdown"' in template
    assert 'id="userMenu"' in template
    assert 'data-bs-toggle="dropdown"' in template


def test_signed_in_menu_renders_install_button_not_hash_link(monkeypatch):
    client = app.test_client()
    _login(client, monkeypatch)

    resp = client.get("/faq")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    menu = _account_menu(html)

    assert 'id="ht-install-link"' in menu
    assert "Install app" in menu
    assert 'href="#"' not in menu.split('id="userMenu"', 1)[-1]
    assert 'id="htInstallModal"' in html
    assert "beforeinstallprompt" in html
    assert "window.__htDeferredInstall" in html


def test_logged_out_pages_do_not_offer_install():
    client = app.test_client()
    resp = client.get("/faq")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'id="ht-install-link"' not in html
    assert 'id="htInstallModal"' not in html
    assert "beforeinstallprompt" not in html
