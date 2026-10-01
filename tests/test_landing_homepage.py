"""Logged-out homepage: video facade, trial CTA, and auth links."""

import re
from html import escape as html_escape
from pathlib import Path
from urllib.parse import urlparse

from app import app
from app.marketing_videos import (
    CATCH_STORIES,
    CATCH_STORY_VIDEOS,
    HERO_VIDEO,
    STORY_STEPS,
    TRADE_STORIES,
    catch_stories,
    resolve_learn_url,
)
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


def _webp_width(blob):
    """Canvas width of a lossy VP8 WebP (the encoding we ship)."""
    assert blob[:4] == b"RIFF" and blob[8:12] == b"WEBP"
    assert blob[12:16] == b"VP8 ", blob[12:16]
    # 3-byte frame tag, then the 9d 01 2a start code, then a 14-bit width.
    assert blob[23:26] == b"\x9d\x01\x2a"
    return int.from_bytes(blob[26:28], "little") & 0x3FFF


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
    assert HERO_VIDEO["poster"] == "marketing/walkthrough_poster_1280.webp"
    assert HERO_VIDEO["poster_srcset"] == (
        ("marketing/walkthrough_poster_1280.webp", "1280w"),
        ("marketing/walkthrough_poster.webp", "1920w"),
    )
    for row in (*STORY_STEPS, *TRADE_STORIES):
        assert row["poster"] == ""
    assert STORY_STEPS[-1]["links_learn"] is True
    assert "Options 101" in STORY_STEPS[-1]["caption"]


def test_homepage_renders_click_to_play_story(monkeypatch):
    _open_signup(monkeypatch)
    resp = app.test_client().get("/")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)

    assert "Watch the trading mirror" in html
    assert 'class="ht-hero-primary"' in html
    assert ">Start your 30-day free trial</a>" in html
    assert 'class="ht-hero-sub">No credit card</p>' in html
    assert 'class="ht-hero-secondary"' in html
    assert ">Try the live demo</a>" in html
    assert 'class="ht-hero-signin"' in html
    assert "ht-text-cta" not in html
    # Closing band keeps the combined line. The hero splits it across button + subline.
    assert "Start your 30-day free trial, no credit card" in html
    stage = html.find('class="ht-stage"')
    cta = html.find('class="ht-hero-cta"')
    proof = html.find('class="ht-band ht-band-proof"')
    how = html.find('class="ht-band ht-band-how')
    assert 0 <= stage < cta < proof < how
    assert html.count('class="ht-facade"') == 9
    assert 'data-youtube-id=""' not in html
    assert "https://i.ytimg.com/vi/NpU79Lwkdn4/maxresdefault.jpg" not in html
    assert "/static/marketing/walkthrough_poster_1280.webp" in html
    assert "/static/marketing/walkthrough_poster.webp 1920w" in html
    assert 'srcset="/static/marketing/walkthrough_poster_1280.webp 1280w, /static/marketing/walkthrough_poster.webp 1920w"' in html
    assert 'sizes="(min-width: 960px) 920px, 100vw"' in html
    assert html.count("srcset=") == 6
    root = Path(__file__).resolve().parents[1]
    for name, width in (
        ("marketing/walkthrough_poster.webp", 1920),
        ("marketing/walkthrough_poster_1280.webp", 1280),
    ):
        path = root / "app" / "static" / name
        assert path.is_file()
        # WebP VP8X/VP8 canvas size lives in the RIFF header. Pillow is not
        # a test dependency, so read the width from the file itself.
        blob = path.read_bytes()
        assert blob[8:12] == b"WEBP"
        assert _webp_width(blob) == width
    assert "https://i.ytimg.com/vi/uAmHW-4RtaA/maxresdefault.jpg" in html
    assert "ht-band-catch" in html
    assert "Here's what you'd catch with HappyTrader" in html
    catch_at = html.index('id="ht-catch"')
    trades_at = html.index("Two closed trades, written out")
    assert catch_at < trades_at
    assert "ht-band-trades" in html
    assert ">30-day free trial, no credit card</a>" in html
    assert "For learning only, not investment advice" in html
    assert html.count("Watch the story") == 5
    for vid in (
        "abjZ4_UFhP4",
        "JB-Zvno6hYQ",
        "L1Wmhww0Xrk",
        "B7KZ9ZMelMc",
        "Sf-SOuSnw30",
    ):
        assert f"https://www.youtube.com/watch?v={vid}" in html
    assert html.count('class="ht-catch-panel"') == 5
    assert html.count('loading="lazy"') >= 5
    for story in CATCH_STORIES:
        assert story["caption"] in html
        assert html_escape(story["alt"], quote=True) in html
        assert f"/static/{story['image']}" in html
        assert f"/static/{story['image_sm']} 800w" in html
    for needle in (
        "−$3,333",
        "+$11,933",
        "+$931",
        "$69",
        "−$600",
        "$84.80",
        "−$2,357",
        "$6,265",
        "+$24k",
        "−$15k",
        "+$11k",
        "74%",
        "44%",
        "collecting premium",
        "nine times as much per trade",
        "+$75.34",
    ):
        assert needle in html
    assert "three times as much" not in html
    assert "+$45,487" not in html
    assert "+$14,677" not in html
    assert "net premium" not in html.lower()
    assert "100 shares" not in html
    presented = catch_stories()
    assert [row["id"] for row in presented] == [row["id"] for row in CATCH_STORIES]
    assert [row["youtube_id"] for row in presented] == [
        "abjZ4_UFhP4",
        "JB-Zvno6hYQ",
        "L1Wmhww0Xrk",
        "B7KZ9ZMelMc",
        "Sf-SOuSnw30",
    ]
    assert list(CATCH_STORY_VIDEOS.values()) == [row["youtube_id"] for row in presented]
    root = Path(__file__).resolve().parents[1]
    for story in CATCH_STORIES:
        blob = (root / "app" / "static" / story["image"]).read_bytes()
        assert _webp_width(blob) == story["width"]
        small = (root / "app" / "static" / story["image_sm"]).read_bytes()
        assert _webp_width(small) == 800
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
        "marketing/pnl_real.webp",
        "marketing/msft-if-held.png",
        "marketing/strategies.png",
        "marketing/fit-matrix.png",
    ):
        assert name in html
        assert (root / "app" / "static" / name).is_file()
    assert "marketing/amd-pnl.png" not in html
    assert "Real account · BE" in html
    assert "Cumulative P&amp;L on BE trades, April to September 2026, with trade-day markers" in html
    assert "account BE" not in html
    assert (
        "Every trade day on one line. The run-up, the drawdown and the recovery, "
        "with options and shares split out."
    ) in html
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


def test_catch_watch_link_appears_only_when_a_youtube_id_is_set(monkeypatch):
    from app import marketing_videos

    blank = {key: "" for key in marketing_videos.CATCH_STORY_VIDEOS}
    monkeypatch.setattr(marketing_videos, "CATCH_STORY_VIDEOS", dict(blank))
    html = app.test_client().get("/").get_data(as_text=True)
    assert "Watch the story" not in html
    monkeypatch.setattr(
        marketing_videos,
        "CATCH_STORY_VIDEOS",
        {**blank, "onon": "aaaaaaaaaaa"},
    )
    html = app.test_client().get("/").get_data(as_text=True)
    assert html.count("Watch the story") == 1
    assert "https://www.youtube.com/watch?v=aaaaaaaaaaa" in html
    monkeypatch.setattr(
        marketing_videos,
        "CATCH_STORY_VIDEOS",
        {**blank, "onon": "not-an-id"},
    )
    html = app.test_client().get("/").get_data(as_text=True)
    assert "Watch the story" not in html


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
