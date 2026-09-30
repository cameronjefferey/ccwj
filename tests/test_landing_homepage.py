"""Logged-out homepage: video facade, trial CTA, and auth links."""

import re
from pathlib import Path
from urllib.parse import urlparse

from app import app
from app.marketing_videos import HERO_VIDEO, STORY_STEPS, TRADE_STORIES, resolve_learn_url
from app.models import User


class _SessionUser:
    is_active = True
    is_anonymous = False

    def __init__(self, user_id=7):
        self.id = user_id
        self.username = "ada"

    @property
    def is_authenticated(self):
        return True

    def get_id(self):
        return str(self.id)


def _open_signup(monkeypatch):
    monkeypatch.setitem(app.config, "SIGNUP_ENABLED", True)
    monkeypatch.setitem(app.config, "SIGNUP_INVITE_CODE", "")


_YOUTUBE_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")


def test_video_catalog_uses_public_ids():
    rows = [HERO_VIDEO, *STORY_STEPS, *TRADE_STORIES]
    assert [row["step"] for row in [HERO_VIDEO, *STORY_STEPS]] == ["hero", 1, 2, 3, 4, 5, 6]
    assert [row["youtube_id"] for row in rows] == [
        "NpU79Lwkdn4",
        "GZ3mPiagkLo",
        "uAmHW-4RtaA",
        "sCZVeeY_6SA",
        "u_YWl5fKEjo",
        "jKMUBsGDETc",
        "BwVHe9MmA9c",
        "VssdUIrHcjs",
        "kqxo9BPDMcs",
    ]
    for row in rows:
        assert {"step", "title", "caption", "youtube_id", "mp4_url", "poster"} <= set(row)
        assert _YOUTUBE_ID.fullmatch(row["youtube_id"])
        assert row["mp4_url"] == ""
        assert isinstance(row["poster"], str)
        assert row["title"]
        assert row["caption"]
    assert HERO_VIDEO["duration_label"] == "2:30"
    assert STORY_STEPS[-1]["links_learn"] is True
    assert "Options 101" in STORY_STEPS[-1]["caption"]


def test_homepage_renders_click_to_play_story(monkeypatch):
    _open_signup(monkeypatch)
    resp = app.test_client().get("/")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)

    assert "Watch the trading mirror" in html
    assert "Start your 30-day free trial, no credit card" in html
    assert html.count('class="ht-facade"') == 9
    assert 'data-youtube-id=""' not in html
    assert "https://i.ytimg.com/vi/NpU79Lwkdn4/maxresdefault.jpg" in html
    assert "https://i.ytimg.com/vi/uAmHW-4RtaA/maxresdefault.jpg" in html
    assert "ht-band-trades" in html
    assert "Two closed trades, written out" in html
    assert "Privacy mode masks account names" in html
    assert "ht-band-proof" in html
    assert "ht-band-how" in html
    assert "How it works" in html
    assert "Strategies detected" in html
    assert "See what's working" in html
    assert "ht-band-demo" in html
    assert "A mirror of a paper account" in html
    assert "trading bot" in html
    assert "Every position's full story" in html
    assert "Covered-call income tracked" in html
    assert "Which strategies actually work" in html
    assert "Where your edge is" in html
    assert "ht-band-learn" in html
    assert "ht-band-cta" in html
    assert "ht-reel" not in html
    assert "is-flip" not in html
    assert "ORCL" not in html
    assert "CFLT" not in html
    root = Path(__file__).resolve().parents[1]
    for name in (
        "marketing/amd-pnl.png",
        "marketing/msft-if-held.png",
        "marketing/strategies.png",
        "marketing/fit-matrix.png",
    ):
        assert name in html
        assert (root / "app" / "static" / name).is_file()
    assert 'href="/signup"' in html or "signup" in html
    assert 'href="/login"' in html
    assert "Sign in" in html
    assert 'name="description"' in html
    assert 'property="og:description"' in html
    assert "<title>Home - HappyTrader</title>" in html

    for title in (
        "Connect your brokerage",
        "Which strategies work",
        "See every trade and position",
        "If held to expiration",
        "Covered-call runs",
        "The fit matrix",
        "Learn options",
        "ONON calls, closed early",
        "RKLB covered calls",
    ):
        assert title in html

    # Facade: the embed host is only the string the click handler concatenates.
    assert "<iframe" not in html.lower()
    assert 'src="https://www.youtube-nocookie.com' not in html
    assert "https://www.youtube-nocookie.com/embed/" in html
    assert "no sign-up" not in html.lower()
    assert "no signup" not in html.lower()

    with app.app_context():
        learn = resolve_learn_url()
    if learn:
        assert f'href="{learn}"' in html
    else:
        assert 'href="/learn"' not in html
        assert "Options 101" in html


def test_homepage_redirects_authenticated_visitors(monkeypatch):
    user = _SessionUser()

    def get_by_id(user_id):
        if str(user_id) == str(user.id):
            return user
        return None

    monkeypatch.setattr(User, "get_by_id", staticmethod(get_by_id))
    client = app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = str(user.id)
        sess["_fresh"] = True
    resp = client.get("/")
    assert resp.status_code in (301, 302, 303)
    assert urlparse(resp.headers["Location"]).path == "/overview"
