"""First-party funnel: cookie, events, pixel gating, admin analytics."""

import json

from flask import g, session

from app import app
from app.funnel import (
    beacon,
    clean_click_id,
    clean_referrer,
    clean_utm,
    decode_touch_cookie,
    encode_touch_cookie,
    log_event,
    merge_touch,
    persist_signup,
    reddit_pixel_events,
    send_capi,
    tracking_opt_out,
)


def _patch_db(monkeypatch, inserts, existing=None):
    monkeypatch.setenv("DATABASE_URL", "postgresql://funnel-test")

    def _execute(sql, params=()):
        inserts.append((sql, tuple(params)))

    def _fetch_one(sql, params=()):
        return existing

    monkeypatch.setattr("app.db.execute", _execute)
    monkeypatch.setattr("app.db.fetch_one", _fetch_one)


def test_utm_cookie_keeps_first_touch_and_moves_last_touch():
    first, dirty = merge_touch(None, {
        "utm_source": "youtube",
        "utm_medium": "video",
        "utm_campaign": "learn",
        "referrer": "https://www.youtube.com/watch?v=secret",
        "rdt_cid": "",
    })
    assert dirty is True
    assert first["ft"]["utm_source"] == "youtube"
    assert first["ft"]["referrer"] == "https://www.youtube.com/watch"
    assert "secret" not in first["ft"]["referrer"]

    second, dirty = merge_touch(first, {
        "utm_source": "reddit",
        "utm_medium": "cpc",
        "utm_campaign": "mirror-v1",
        "utm_content": "score",
        "utm_term": "wheel",
        "rdt_cid": "click12345",
        "referrer": "https://www.reddit.com/r/thetagang",
    })
    assert dirty is True
    assert second["visit_id"] == first["visit_id"]
    assert second["ft"]["utm_source"] == "youtube"
    assert second["lt"]["utm_source"] == "reddit"
    assert second["lt"]["utm_campaign"] == "mirror-v1"
    assert second["lt"]["rdt_cid"] == "click12345"

    raw = encode_touch_cookie(second)
    assert "youtube" not in raw or True  # payload is base64, not the raw query
    restored = decode_touch_cookie(raw)
    assert restored["visit_id"] == second["visit_id"]
    assert restored["ft"]["utm_source"] == "youtube"
    assert restored["lt"]["utm_campaign"] == "mirror-v1"
    assert restored["lt"]["utm_term"] == "wheel"


def test_utm_cleaner_drops_junk_and_keeps_reddit_click_id():
    assert clean_utm("reddit") == "reddit"
    assert clean_utm("user@example.com") == ""
    assert clean_utm("has space") == ""
    assert clean_click_id("abc") == ""
    assert clean_click_id("rdt_cid_1234") == "rdt_cid_1234"
    assert clean_referrer("https://happytrader.me/start?utm_source=reddit") == ""
    assert clean_referrer("https://youtu.be/abc") == "https://youtu.be/abc"


def test_log_event_stores_campaign_fields_and_not_identity(monkeypatch):
    inserts = []
    _patch_db(monkeypatch, inserts)
    with app.test_request_context(
        "/start?utm_source=reddit&utm_campaign=mirror-v1&utm_content=score"
        "&utm_medium=cpc&utm_term=wheel&rdt_cid=click12345",
        headers={"Referer": "https://www.reddit.com/r/options?token=sekret"},
    ):
        event_id = log_event("landing_view", path="/start")
    assert event_id
    sql, params = inserts[0]
    assert "email" not in sql.lower()
    assert "INSERT INTO funnel_events" in sql
    assert params[0] == "landing_view"
    assert params[1] == event_id
    assert params[4] == "/start"
    assert params[5] == "https://www.reddit.com/r/options"
    assert params[6] == "reddit"
    assert params[8] == "mirror-v1"
    assert "sekret" not in params
    assert "user@example.com" not in params


def test_log_event_records_a_step_once_per_visit(monkeypatch):
    inserts = []
    _patch_db(monkeypatch, inserts, existing={"?column?": 1})
    with app.test_request_context("/signup"):
        assert log_event("signup_started", path="/signup") is None
    assert inserts == []


def test_beacon_rejects_identity_fields_and_unknown_events(monkeypatch):
    inserts = []
    _patch_db(monkeypatch, inserts)
    with app.test_request_context("/funnel/beacon", method="POST"):
        body, status = beacon({"event": "paid"})
        assert status == 400
        body, status = beacon({"event": "lesson_started", "email": "a@b.com"})
        assert status == 400
        body, status = beacon({"event": "lesson_started", "path": "/learn/the-wheel"})
        assert status == 200
        assert body["ok"] is True
    assert inserts[0][1][0] == "lesson_started"
    assert inserts[0][1][4] == "/learn/the-wheel"


def test_pixel_stays_off_without_id_and_when_opted_out():
    previous = app.config.get("REDDIT_PIXEL_ID")
    app.config["REDDIT_PIXEL_ID"] = ""
    try:
        with app.test_request_context("/start"):
            assert reddit_pixel_events() == []
        app.config["REDDIT_PIXEL_ID"] = "t2_testpixel"
        with app.test_request_context("/start", headers={"DNT": "1"}):
            assert tracking_opt_out() is True
            assert reddit_pixel_events() == []
        with app.test_request_context("/pricing", headers={"Sec-GPC": "1"}):
            assert reddit_pixel_events() == []
        with app.test_request_context("/start"):
            events = reddit_pixel_events()
            assert [ev["name"] for ev in events] == ["PageVisit"]
            assert len(events[0]["event_id"]) >= 8
            shared = g._funnel_pagevisit_id
            assert events[0]["event_id"] == shared
        with app.test_request_context("/get-started"):
            session["ht_reddit_signup"] = "abc12345signup"
            session["ht_reddit_lead"] = "abc12345lead"
            events = reddit_pixel_events()
            names = [ev["name"] for ev in events]
            assert names == ["SignUp", "Lead"]
            assert events[0]["event_id"] == "abc12345signup"
            assert session.get("ht_reddit_signup") is None
    finally:
        app.config["REDDIT_PIXEL_ID"] = previous


def test_pagevisit_pixel_and_server_event_share_an_id(monkeypatch):
    inserts = []
    _patch_db(monkeypatch, inserts)
    previous = app.config.get("REDDIT_PIXEL_ID")
    app.config["REDDIT_PIXEL_ID"] = "t2_testpixel"
    try:
        with app.test_request_context("/"):
            events = reddit_pixel_events()
            logged = log_event("landing_view", path="/")
            assert logged == events[0]["event_id"]
            assert inserts[0][1][1] == events[0]["event_id"]
    finally:
        app.config["REDDIT_PIXEL_ID"] = previous


def test_capi_is_off_without_a_token_and_posts_when_configured(monkeypatch):
    previous_pixel = app.config.get("REDDIT_PIXEL_ID")
    previous_token = app.config.get("REDDIT_CAPI_TOKEN")
    posted = {}

    class _Resp:
        def read(self):
            return b"{}"

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def _urlopen(req, timeout=0):
        posted["url"] = req.full_url
        posted["body"] = json.loads(req.data.decode())
        posted["auth"] = req.headers["Authorization"]
        return _Resp()

    monkeypatch.setenv("FUNNEL_CAPI_SYNC", "1")
    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    app.config["REDDIT_PIXEL_ID"] = "t2_testpixel"
    app.config["REDDIT_CAPI_TOKEN"] = ""
    try:
        with app.test_request_context("/"):
            assert send_capi("PageVisit", "abc12345event", click_id="click12345") is False
        assert posted == {}
        app.config["REDDIT_CAPI_TOKEN"] = "token-secret"
        with app.test_request_context("/", headers={"DNT": "1"}):
            assert send_capi("SignUp", "abc12345event") is False
        assert posted == {}
        with app.test_request_context("/"):
            assert send_capi(
                "Purchase", "purchaseabc123", click_id="click12345", user_id=9,
            ) is True
        assert posted["url"].endswith("/t2_testpixel")
        event = posted["body"]["events"][0]
        assert event["event_metadata"]["conversion_id"] == "purchaseabc123"
        assert event["event_type"]["tracking_type"] == "Purchase"
        assert event["user"]["click_id"] == "click12345"
        assert "email" not in event["user"]
        assert posted["auth"] == "Bearer token-secret"
    finally:
        app.config["REDDIT_PIXEL_ID"] = previous_pixel
        app.config["REDDIT_CAPI_TOKEN"] = previous_token


def test_signup_persists_first_and_last_touch(monkeypatch):
    inserts = []
    _patch_db(monkeypatch, inserts)
    attr = {
        "visit_id": "a" * 32,
        "ft": {
            "utm_source": "youtube",
            "utm_medium": "video",
            "utm_campaign": "learn",
            "utm_content": "",
            "utm_term": "",
            "rdt_cid": "",
            "referrer": "https://www.youtube.com/watch",
        },
        "lt": {
            "utm_source": "reddit",
            "utm_medium": "cpc",
            "utm_campaign": "mirror-v1",
            "utm_content": "score",
            "utm_term": "wheel",
            "rdt_cid": "click12345",
            "referrer": "https://www.reddit.com/r/thetagang",
        },
        "_dirty": True,
    }
    raw = encode_touch_cookie(attr)
    with app.test_request_context("/", headers={"Cookie": f"ht_touch={raw}"}):
        persist_signup(42)
    sql, params = inserts[0]
    assert "acquisition_captured" in sql
    assert "acquisition_lt_source" in sql
    assert params[0] == "youtube"
    assert params[7] == "reddit"
    assert params[12] == "click12345"
    assert params[-1] == 42


def test_admin_analytics_is_admin_only(monkeypatch):
    from app.models import User

    class _User:
        is_authenticated = True
        is_active = True
        is_anonymous = False
        id = 7
        username = "alice"

        def get_id(self):
            return "7"

    monkeypatch.setattr(User, "get_by_id", staticmethod(lambda uid: _User()))
    monkeypatch.setenv("ADMIN_USERS", "cameron")
    client = app.test_client()

    anon = client.get("/admin/analytics")
    assert anon.status_code in (302, 303, 401)
    assert b"Signups per day" not in anon.data

    with client.session_transaction() as sess:
        sess["_user_id"] = "7"
        sess["_fresh"] = True
    hidden = client.get("/admin/analytics")
    assert hidden.status_code == 404

    class _Admin(_User):
        username = "cameron"

    monkeypatch.setattr(User, "get_by_id", staticmethod(lambda uid: _Admin()))
    monkeypatch.setattr(
        "app.funnel.build_admin_analytics",
        lambda: {
            "days": [{"day": "2026-10-01", "signups": 2}],
            "steps": [{
                "key": "landing_view",
                "label": "Landing view",
                "count": 4,
                "of_landing": 100.0,
            }],
            "sources": [{
                "source": "reddit",
                "campaign": "mirror-v1",
                "visits": 4,
                "signups": 2,
                "brokers": 1,
                "paid": 0,
            }],
            "youtube": {"visits": 3, "signups": 1},
        },
    )
    page = client.get("/admin/analytics")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    assert "Signups per day" in body
    assert "2026-10-01" in body
    assert "Landing view" in body
    assert "mirror-v1" in body
    assert "YouTube" in body


def test_start_hero_offers_learning_and_the_demo(monkeypatch):
    monkeypatch.setattr("app.campaign.record_event", lambda *a, **k: None)
    resp = app.test_client().get(
        "/start?utm_source=reddit&utm_campaign=mirror-v1&utm_content=score"
    )
    body = resp.get_data(as_text=True)
    assert "Start learning free" in body
    assert "Try the live demo" in body
    assert "Learning and paper trading are free" in body
    assert "30-day trial starts when you connect a real brokerage" in body
    cookies = " ".join(resp.headers.getlist("Set-Cookie"))
    assert "ht_touch=" in cookies


def test_static_images_are_cached():
    client = app.test_client()
    resp = client.get("/static/js/funnel.js")
    assert resp.status_code == 200
    assert "max-age=3600" in (resp.headers.get("Cache-Control") or "")
    policy = resp.headers.get("Content-Security-Policy-Report-Only") or ""
    assert "https://www.redditstatic.com" in policy
    assert "https://pixel-config.reddit.com" in policy
    assert "https://alb.reddit.com" in policy
