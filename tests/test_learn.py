"""Public /learn routes: Options 101 series."""

import json
import re

from app import app
from app import learn_catalog as catalog
from app import learn_progress
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


def _html(path):
    response = _client().get(path)
    assert response.status_code == 200, path
    return response.get_data(as_text=True)


def _description(html):
    match = re.search(r'<meta name="description" content="([^"]*)"', html)
    assert match, html[:500]
    return match.group(1)


def test_learn_routes_return_200():
    assert _client().get("/learn").status_code == 200
    assert _client().get("/learn/").status_code == 200
    for episode in catalog.episodes():
        assert _client().get(f"/learn/{episode['slug']}").status_code == 200
    missing = _client().get("/learn/not-an-episode")
    assert missing.status_code == 404


def test_seed_is_ten_published_episodes_with_real_ids():
    rows = catalog.episodes()
    assert [episode["number"] for episode in rows] == list(range(1, 11))
    assert [episode["slug"] for episode in rows] == [
        "what-is-an-option",
        "calls-and-puts",
        "strike-expiration-premium",
        "buying-and-selling",
        "covered-calls",
        "cash-secured-puts",
        "the-wheel",
        "spreads",
        "options-risk",
        "reading-a-position",
    ]
    assert [episode["youtube_id"] for episode in rows] == [
        "lin6soSWnmI",
        "B1EsiHXS2UA",
        "aQP_JvJPN2I",
        "MGpmYPm2-xc",
        "4Ei7inl8c6Q",
        "7QdlwSDnT6Q",
        "2neL6HYfWRM",
        "CIFL_UWQLY0",
        "4FgyBSbTCRA",
        "2Qq8FayvJ5c",
    ]
    assert all(episode["published"] for episode in rows)
    assert all(catalog.valid_youtube_id(episode["youtube_id"]) for episode in rows)
    for episode in rows:
        assert len(episode["shorts"]) == 4
        assert all(catalog.valid_youtube_id(short["youtube_id"]) for short in episode["shorts"])
    assert "price lock" in rows[0]["summary"].lower()
    assert "100 shares" in rows[2]["recap"].lower()
    assert "expires worthless" in rows[2]["recap"].lower()
    series = catalog.series()
    assert series["title"] == "Options 101: options explained in plain English"
    assert series["cta_button"] == "Start your free 30-day trial"
    assert series["cta_note"] == "Full access · No credit card"
    assert series["cta_heading"] == "See which option strategies actually work for you"
    assert series["disclaimer"] == "For learning only · not investment advice."
    assert series["playlist_url"] == "https://www.youtube.com/playlist?list=PLcVwygMVS3Ig"


def test_index_links_every_episode_and_the_playlist():
    html = _html("/learn")
    assert "<title>Options 101 - HappyTrader</title>" in html
    assert "Options 101: options explained in plain English" in html
    assert "Start with Episode 1" in html
    assert html.count('class="learn-card"') == 10
    assert html.count('class="learn-watch"') == 10
    assert "Coming soon" not in html
    for episode in catalog.episodes():
        assert f'href="/learn/{episode["slug"]}"' in html
        assert f"https://i.ytimg.com/vi/{episode['youtube_id']}/maxresdefault.jpg" in html
    assert html.count('class="yt-lite yt-lite-short"') == 40
    assert "https://www.youtube-nocookie.com/embed/BwVHe9MmA9c?" in html
    assert 'href="https://www.youtube.com/playlist?list=PLcVwygMVS3Ig"' in html
    assert ">Learn</a>" in html
    assert "https://www.youtube.com/embed" not in html
    assert "<iframe" not in html
    assert "https://i.ytimg.com/vi/lin6soSWnmI/maxresdefault.jpg" in html
    assert 'name="twitter:card" content="summary_large_image"' in html
    assert 'property="og:image"' in html
    assert "application/ld+json" not in html
    _assert_public_copy(html)


def test_episode_page_is_click_to_play():
    html = _html("/learn/what-is-an-option")
    assert "<title>What is an option? · Options 101 - HappyTrader</title>" in html
    assert "Words you&#39;ll learn" in html or "Words you'll learn" in html
    assert "A price lock" in html
    assert "Premium" in html
    assert 'href="/learn/calls-and-puts"' in html
    assert "Coming soon" not in html
    assert "<iframe" not in html
    assert "https://www.youtube-nocookie.com/embed/lin6soSWnmI?" in html
    assert "https://www.youtube.com/embed" not in html
    assert 'data-seek="80"' in html
    assert html.count('class="learn-short"') == 4
    assert "https://www.youtube-nocookie.com/embed/-EWMyYDpuTo?" not in html
    assert 'href="https://www.youtube.com/playlist?list=PLcVwygMVS3Ig"' in html
    assert 'name="robots" content="noindex"' not in html
    payload = json.loads(
        re.search(
            r'<script type="application/ld\+json">(.+?)</script>',
            html,
        ).group(1)
    )
    assert payload["@type"] == "VideoObject"
    assert payload["embedUrl"].startswith(
        "https://www.youtube-nocookie.com/embed/lin6soSWnmI"
    )
    _assert_public_copy(html)

    third = _html("/learn/strike-expiration-premium")
    assert "One contract is 100 shares" in third
    assert "Expires worthless" in third
    assert _description(html) != _description(third)

    hyphen = _html("/learn/cash-secured-puts")
    assert "https://www.youtube-nocookie.com/embed/-EWMyYDpuTo?" in hyphen
    assert "<iframe" not in hyphen


def test_every_episode_is_indexed():
    html = _html("/learn/covered-calls")
    assert 'name="robots" content="noindex"' not in html
    assert "Coming soon" not in html
    assert "https://www.youtube-nocookie.com/embed/4Ei7inl8c6Q?" in html
    sitemap = _client().get("/sitemap.xml").get_data(as_text=True)
    assert "/learn" in sitemap
    for episode in catalog.episodes():
        assert f"/learn/{episode['slug']}" in sitemap
    assert "/learn/assignment-and-exercise" not in sitemap
    assert "/learn/the-greeks" not in sitemap


def test_descriptions_are_unique_across_learn_pages():
    seen = {_description(_html("/learn"))}
    for episode in catalog.episodes():
        seen.add(_description(_html(f"/learn/{episode['slug']}")))
    assert len(seen) == 1 + len(catalog.episodes())


def test_logged_in_user_can_open_learn(monkeypatch):
    user = _SessionUser("alice")

    def get_by_id(user_id):
        return user if str(user_id) == "1" else None

    monkeypatch.setattr(User, "get_by_id", staticmethod(get_by_id))
    client = _client()
    with client.session_transaction() as sess:
        sess["_user_id"] = "1"
        sess["_fresh"] = True
    response = client.get("/learn/calls-and-puts")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Calls and puts" in html
    assert "Start your free 30-day trial" not in html
    assert 'href="/practice"' in html
    assert ">Practice</a>" in html
    index = client.get("/learn").get_data(as_text=True)
    assert "Start your free 30-day trial" not in index
    assert ">Practice</a>" in index


def test_later_episodes_show_a_duration():
    expected = {
        "buying-and-selling": "5:37",
        "covered-calls": "4:37",
        "cash-secured-puts": "4:41",
        "the-wheel": "4:19",
        "spreads": "4:45",
        "options-risk": "4:41",
        "reading-a-position": "3:42",
    }
    html = _html("/learn")
    for episode in catalog.episodes():
        if episode["number"] < 4:
            continue
        assert episode["duration"] == expected[episode["slug"]]
        assert catalog.duration_iso(episode["duration"])
        assert episode["duration"] in html
    assert 'aria-label="Next shorts"' in html
    assert "learn-shorts-nav" in html


def test_youtube_id_turns_on_nocookie_embed_and_video_metadata(monkeypatch):
    original = catalog.episodes()
    live = []
    for episode in original:
        row = dict(episode)
        row["chapters"] = list(episode["chapters"])
        row["shorts"] = [dict(short) for short in episode["shorts"]]
        row["vocab"] = list(episode["vocab"])
        live.append(row)
    live[0]["youtube_id"] = "aaaaaaaaaaa"
    live[0]["shorts"] = [{"youtube_id": "bbbbbbbbbbb", "title": "Price lock, short"}]

    def fake_episodes():
        return live

    monkeypatch.setattr(catalog, "episodes", fake_episodes)
    monkeypatch.setattr(
        catalog,
        "episode_by_slug",
        lambda slug: next((row for row in live if row["slug"] == slug), None),
    )
    monkeypatch.setattr(
        catalog,
        "published_episodes",
        lambda: [row for row in live if row["published"]],
    )
    monkeypatch.setattr(
        catalog,
        "series_shorts",
        lambda: [
            {"episode_slug": row["slug"], **short}
            for row in live if row["published"]
            for short in row["shorts"]
        ],
    )

    page = _html("/learn/what-is-an-option")
    assert "https://www.youtube-nocookie.com/embed/aaaaaaaaaaa?" in page
    assert "enablejsapi=1" in page
    assert 'loading="lazy"' in page
    assert "https://www.youtube.com/embed" not in page
    assert 'class="learn-watch"' not in page
    assert 'data-seek="80"' in page
    payload = json.loads(
        re.search(
            r'<script type="application/ld\+json">(.+?)</script>',
            page,
        ).group(1)
    )
    assert payload["@type"] == "VideoObject"
    assert payload["embedUrl"].startswith("https://www.youtube-nocookie.com/embed/aaaaaaaaaaa")
    assert payload["name"] == "What is an option?"

    index = _html("/learn")
    assert 'class="learn-watch"' in index
    assert 'href="/learn/what-is-an-option"' in index
    assert "https://www.youtube-nocookie.com/embed/bbbbbbbbbbb?" in index


def test_embed_url_rejects_anything_that_is_not_an_id():
    assert catalog.embed_url(None) is None
    assert catalog.embed_url("") is None
    assert catalog.embed_url("javascript:alert(1)") is None
    assert catalog.embed_url("aaaaaaaaaaa").startswith(
        "https://www.youtube-nocookie.com/embed/aaaaaaaaaaa?"
    )
    assert catalog.is_watchable({"published": True, "youtube_id": None}) is False
    assert catalog.is_watchable({"published": False, "youtube_id": "aaaaaaaaaaa"}) is False
    assert catalog.is_watchable({"published": True, "youtube_id": "aaaaaaaaaaa"}) is True


def _csrf(client):
    html = client.get("/learn").get_data(as_text=True)
    match = re.search(r'name="csrf-token" content="([^"]+)"', html)
    assert match, html[:400]
    return match.group(1)


def _login(monkeypatch, client, username):
    user = _SessionUser(username)

    def get_by_id(user_id):
        return user if str(user_id) == "1" else None

    monkeypatch.setattr(User, "get_by_id", staticmethod(get_by_id))
    with client.session_transaction() as sess:
        sess["_user_id"] = "1"
        sess["_fresh"] = True
    return user


def test_resume_is_local_until_a_signed_in_account_saves_it():
    html = _html("/learn")
    assert 'id="learn-start"' in html
    assert "Start with Episode 1" in html
    assert 'id="learn-progress"' in html
    assert 'data-learn-sync="0"' in html
    assert 'src="/static/js/learn-progress.js"' in html
    assert 'data-slug="what-is-an-option"' in html
    assert 'data-published="1"' in html
    assert 'data-published="0"' not in html
    episode = _html("/learn/what-is-an-option")
    assert 'data-slug="what-is-an-option"' in episode
    assert 'id="learn-resume"' in episode

    client = _client()
    assert client.get("/learn/progress").get_json() == {
        "updated": 0,
        "last": None,
        "done": [],
        "replays": [],
    }
    posted = client.post(
        "/learn/progress",
        json={"done": ["what-is-an-option"], "last": {"slug": "calls-and-puts", "t": 12}},
        headers={"X-CSRFToken": _csrf(client)},
    )
    assert posted.status_code == 204


def test_progress_keeps_catalog_titles_and_merges_finished_episodes():
    clean = learn_progress.sanitize({
        "updated": 5,
        "last": {
            "slug": "what-is-an-option",
            "t": 999999,
            "title": "<script>",
            "number": 99,
        },
        "done": ["what-is-an-option", "nope", "what-is-an-option"],
    })
    assert clean["last"]["title"] == "What is an option?"
    assert clean["last"]["number"] == 1
    assert clean["last"]["t"] == 8 * 60 * 60
    assert clean["done"] == ["what-is-an-option"]
    assert learn_progress.sanitize({"last": {"slug": "not-real", "t": 4}})["last"] is None

    merged = learn_progress.merge(
        {"updated": 10, "last": {"slug": "what-is-an-option"}, "done": ["what-is-an-option"]},
        {"updated": 20, "last": {"slug": "calls-and-puts"}, "done": ["calls-and-puts"]},
    )
    assert merged["last"]["slug"] == "calls-and-puts"
    assert merged["done"] == ["what-is-an-option", "calls-and-puts"]
    assert merged["updated"] == 20


def test_signed_in_progress_fails_open_and_skips_the_demo(monkeypatch):
    saved = {}

    def remember(user_id, payload):
        saved["user_id"] = user_id
        saved["payload"] = payload
        return learn_progress.sanitize(payload)

    def unavailable(_user_id):
        raise RuntimeError("no database")

    monkeypatch.setattr(learn_progress, "save_for_user", remember)
    monkeypatch.setattr(learn_progress, "load_for_user", unavailable)

    client = _client()
    _login(monkeypatch, client, "alice")
    page = client.get("/learn").get_data(as_text=True)
    assert 'data-learn-sync="1"' in page
    loaded = client.get("/learn/progress")
    assert loaded.status_code == 200
    assert loaded.get_json()["done"] == []
    body = {
        "updated": 30,
        "done": ["calls-and-puts"],
        "last": {"slug": "calls-and-puts", "t": 40, "title": "ignored"},
    }
    stored = client.post(
        "/learn/progress",
        json=body,
        headers={"X-CSRFToken": _csrf(client)},
    )
    assert stored.status_code == 200
    assert stored.get_json()["last"]["title"] == "Calls and puts"
    assert saved["user_id"] == 1

    demo = _client()
    _login(monkeypatch, demo, "demo")
    demo_page = demo.get("/learn/what-is-an-option").get_data(as_text=True)
    assert 'data-learn-sync="0"' in demo_page
    skipped = demo.post(
        "/learn/progress",
        json=body,
        headers={"X-CSRFToken": _csrf(demo)},
    )
    assert skipped.status_code == 204
    assert saved["user_id"] == 1


def test_empty_progress_does_not_wipe_a_saved_resume(monkeypatch):
    monkeypatch.setattr(
        learn_progress,
        "load_for_user",
        lambda user_id: {
            "updated": 4,
            "last": {"slug": "what-is-an-option", "t": 8, "title": "What is an option?", "number": 1},
            "done": [],
        },
    )

    def fail_write(*_args, **_kwargs):
        raise AssertionError("empty progress should not write")

    monkeypatch.setattr(learn_progress, "execute", fail_write)
    assert learn_progress.save_for_user(7, {})["last"]["slug"] == "what-is-an-option"


def test_lesson_ends_with_a_try_it_and_a_check():
    from flask import url_for

    from app import learn_try

    for episode in catalog.published_episodes():
        ticket = learn_try.for_lesson(episode["slug"])
        assert ticket["href"].startswith("/practice?")
        assert "symbol=SPY" in ticket["href"]
        assert len(learn_try.checks_for_lesson(episode["slug"])) == 2
        assert learn_try.deeper_for_lesson(episode["slug"])["label"] == "Go deeper"

    calls = _html("/learn/calls-and-puts")
    assert "Try it" in calls
    assert "Try a SPY call" in calls
    assert "side=call" in calls
    assert "Try a SPY put" in calls
    assert "Did that make sense?" in calls
    assert 'id="learn-mark-done"' in calls
    assert ">Mark as done<" in calls
    assert ">Go deeper<" in calls
    assert 'href="/learn/replay/long-call"' in calls

    covered = _html("/learn/covered-calls")
    assert "This ticket buys a call" in covered
    assert "side=call" in covered

    index = _html("/learn")
    assert "M3.2 8.4" in index
    assert 'class="learn-watched"' in index

    js = open("app/static/js/learn-progress.js", encoding="utf-8").read()
    assert "func: \"seekTo\"" in js
    assert "Picking up at " in js
    assert "__htLearnMarkDone" in js
    assert "X-CSRF-Token" in js

    with app.test_request_context():
        learn_login = url_for("login", next="/learn")
        episode_login = url_for("login", next="/learn/calls-and-puts")
    assert f'href="{learn_login}"' in index
    assert f'href="{episode_login}"' in calls
    login = _client().get("/login?next=/learn").get_data(as_text=True)
    assert 'name="next" value="/learn"' in login


def test_progress_save_rejects_a_missing_csrf_token(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", True)
    posted = _client().post(
        "/learn/progress",
        json={"done": ["what-is-an-option"]},
        headers={"Accept": "application/json"},
    )
    assert posted.status_code == 400
    assert posted.is_json
    assert "Refresh" in posted.get_json()["error"]


def _assert_public_copy(html):
    lowered = html.lower()
    for banned in ("no sign-up", "no signup", "live demo", "the only place", "guaranteed"):
        assert banned not in lowered
    assert "Start your free 30-day trial" in html
    assert "Full access · No credit card" in html
    assert "For learning only · not investment advice." in html
    assert 'href="/signup"' in html
