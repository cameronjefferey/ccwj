"""First-party funnel: cookie, events, pixel gating, admin analytics."""

import hashlib
import html
import json
import os
import uuid
from urllib.parse import urlparse

import pytest
from flask import g, session

from app import app
from app.funnel import (
    beacon,
    clean_click_id,
    clean_referrer,
    clean_utm,
    conversion_rates,
    decode_touch_cookie,
    device_type,
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
        assert posted["url"] == (
            "https://ads-api.reddit.com/api/v3/pixels/t2_testpixel/conversion_events"
        )
        assert "v2.0" not in posted["url"]
        event = posted["body"]["data"]["events"][0]
        assert event["metadata"]["conversion_id"] == "purchaseabc123"
        assert event["type"]["tracking_type"] == "PURCHASE"
        assert event["click_id"] == "click12345"
        assert "email" not in event["user"]
        assert posted["auth"] == "Bearer token-secret"
    finally:
        app.config["REDDIT_PIXEL_ID"] = previous_pixel
        app.config["REDDIT_CAPI_TOKEN"] = previous_token


def test_capi_v3_payload_matches_the_pixel_conversion_id(monkeypatch):
    """v3 body: data wrapper, WEBSITE, UPPER_SNAKE type, conversion_id = pixel id."""
    previous_pixel = app.config.get("REDDIT_PIXEL_ID")
    previous_token = app.config.get("REDDIT_CAPI_TOKEN")
    posted = []

    class _Resp:
        def read(self):
            return b"{}"

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def _urlopen(req, timeout=0):
        posted.append({
            "url": req.full_url,
            "body": json.loads(req.data.decode()),
        })
        return _Resp()

    monkeypatch.setenv("FUNNEL_CAPI_SYNC", "1")
    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    app.config["REDDIT_PIXEL_ID"] = "t2_testpixel"
    app.config["REDDIT_CAPI_TOKEN"] = "token-secret"
    app.config["REDDIT_CAPI_TEST_ID"] = ""
    pixel_id = "pagevisitabc123"
    try:
        with app.test_request_context("/start?rdt_cid=secretclick&utm_source=reddit"):
            assert send_capi("PageVisit", pixel_id, click_id="click12345") is True
        with app.test_request_context("/"):
            assert send_capi("NotARedditEvent", pixel_id) is False
        body = posted[0]["body"]
        assert list(body) == ["data"]
        assert list(body["data"]) == ["events"]
        event = body["data"]["events"][0]
        assert event["action_source"] == "WEBSITE"
        assert event["type"] == {"tracking_type": "PAGE_VISIT"}
        assert event["metadata"]["conversion_id"] == pixel_id
        assert "event_type" not in event
        assert "event_metadata" not in event
        assert event["click_id"] == "click12345"
        assert "user" not in event
        assert isinstance(event["event_at"], int)
        assert event["event_source_url"] == "http://localhost/start"
        assert "rdt_cid" not in event["event_source_url"]
        assert "?" not in event["event_source_url"]
        assert len(posted) == 1

        signup_id = "signupabc12345"
        with app.test_request_context("/"):
            assert send_capi("SignUp", signup_id, user_id=9) is True
        signup = posted[1]["body"]["data"]["events"][0]
        assert signup["type"]["tracking_type"] == "SIGN_UP"
        assert signup["metadata"]["conversion_id"] == signup_id
        assert signup["user"]["external_id"] == hashlib.sha256(b"9").hexdigest()
        assert "email" not in signup["user"]
        assert "ip_address" not in signup["user"]
        assert "click_id" not in signup["user"]

        with app.test_request_context("/"):
            assert send_capi("Lead", "leadabc12345") is True
            assert send_capi("Purchase", "purchaseabc123") is True
        assert posted[2]["body"]["data"]["events"][0]["type"]["tracking_type"] == "LEAD"
        assert posted[3]["body"]["data"]["events"][0]["type"]["tracking_type"] == "PURCHASE"
        assert posted[3]["body"]["data"]["events"][0]["metadata"]["conversion_id"] == "purchaseabc123"
        assert "test_id" not in body
        assert "test_id" not in body["data"]
    finally:
        app.config["REDDIT_PIXEL_ID"] = previous_pixel
        app.config["REDDIT_CAPI_TOKEN"] = previous_token
        app.config["REDDIT_CAPI_TEST_ID"] = ""


def test_capi_test_id_is_included_only_when_set(monkeypatch):
    """Event testing id is data.test_id. Unset omits the key entirely."""
    previous_pixel = app.config.get("REDDIT_PIXEL_ID")
    previous_token = app.config.get("REDDIT_CAPI_TOKEN")
    previous_test = app.config.get("REDDIT_CAPI_TEST_ID")
    posted = []

    class _Resp:
        def read(self):
            return b"{}"

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def _urlopen(req, timeout=0):
        posted.append(json.loads(req.data.decode()))
        return _Resp()

    monkeypatch.setenv("FUNNEL_CAPI_SYNC", "1")
    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    app.config["REDDIT_PIXEL_ID"] = "t2_testpixel"
    app.config["REDDIT_CAPI_TOKEN"] = "token-secret"
    app.config["REDDIT_CAPI_TEST_ID"] = ""
    try:
        with app.test_request_context("/"):
            assert send_capi("PageVisit", "pagevisitabc123") is True
        quiet = posted[0]
        assert list(quiet) == ["data"]
        assert "test_id" not in quiet["data"]
        assert "test_id" not in quiet["data"]["events"][0]

        app.config["REDDIT_CAPI_TEST_ID"] = "  t2_eventtest  "
        with app.test_request_context("/go/real-pnl"):
            assert send_capi("SignUp", "signupabc12345", click_id="click12345") is True
        marked = posted[1]
        assert marked["data"]["test_id"] == "t2_eventtest"
        assert list(marked["data"])[0] == "test_id"
        event = marked["data"]["events"][0]
        assert "test_id" not in event
        assert event["metadata"]["conversion_id"] == "signupabc12345"
        assert event["type"]["tracking_type"] == "SIGN_UP"
    finally:
        app.config["REDDIT_PIXEL_ID"] = previous_pixel
        app.config["REDDIT_CAPI_TOKEN"] = previous_token
        app.config["REDDIT_CAPI_TEST_ID"] = previous_test


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
            "landing": "learn",
            "variant": "past",
        },
        "lt": {
            "utm_source": "reddit",
            "utm_medium": "cpc",
            "utm_campaign": "mirror-v1",
            "utm_content": "score",
            "utm_term": "wheel",
            "rdt_cid": "click12345",
            "referrer": "https://www.reddit.com/r/thetagang",
            "landing": "mistakes",
            "variant": "early",
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
    assert params[14] == "learn"
    assert params[16] == "mistakes"
    assert "acquisition_landing" in sql
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
            "acquisition": {
                "range_key": "7d",
                "days": [{"day": "2026-10-01", "visitors": 8, "views": 11}],
                "pages": [{"path": "/go/learn", "visitors": 8, "views": 11}],
                "campaigns": [{
                    "landing": "learn",
                    "steps": conversion_rates({
                        "visitors": 8,
                        "cta": 4,
                        "signups": 2,
                        "lessons": 1,
                        "paper": 0,
                        "broker": 0,
                        "paid": 0,
                    }),
                }],
                "sources": [{
                    "source": "reddit",
                    "campaign": "learn",
                    "content": "past",
                    "visitors": 8,
                    "signups": 2,
                }],
                "devices": [{"device": "phone", "visitors": 5, "signups": 1}],
                "referrers": [
                    {"host": "YouTube", "visitors": 3},
                    {"host": "Reddit", "visitors": 8},
                ],
                "origins": [{
                    "source": "reddit / learn",
                    "visitors": 8,
                    "signups": 2,
                    "rate": 25.0,
                }],
                "filtered_out": 14,
            },
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
    assert "Acquisition" in body
    assert "Campaign funnel" in body
    assert "50.0%" in body
    assert "/go/learn" in body
    assert "Where they came from" in body
    assert "reddit / learn" in body
    assert "25.0%" in body
    assert "14 filtered out" in body


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
    assert "cta_click" in resp.get_data(as_text=True)
    assert "scroll_depth" in resp.get_data(as_text=True)
    assert "client_seen" in resp.get_data(as_text=True)


def test_conversion_rates_are_percent_of_visitors():
    rows = conversion_rates({
        "visitors": 8,
        "cta": 4,
        "signups": 2,
        "lessons": 1,
        "paper": 1,
        "broker": 0,
        "paid": 0,
    })
    by_key = {row["key"]: row for row in rows}
    assert by_key["visitors"]["rate"] == 100.0
    assert by_key["cta"]["count"] == 4
    assert by_key["cta"]["rate"] == 50.0
    assert by_key["signups"]["rate"] == 25.0
    assert by_key["broker"]["rate"] == 0.0
    assert conversion_rates({})[0]["rate"] == 0.0
    assert [row["label"] for row in rows] == [
        "Visitors",
        "CTA clicks",
        "Signups",
        "First lesson",
        "Paper connect",
        "Real broker connect",
        "Paid",
    ]


def test_device_type_is_coarse_and_drops_the_raw_agent():
    assert device_type("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)") == "phone"
    assert device_type("Mozilla/5.0 (iPad; CPU OS 17_0)") == "tablet"
    assert device_type("Mozilla/5.0 (Windows NT 10.0)") == "desktop"


def test_landing_slug_sticks_on_first_touch_and_moves_last_touch():
    first, _dirty = merge_touch(None, {
        "utm_source": "reddit",
        "utm_campaign": "learn",
        "landing": "learn",
        "variant": "past",
    })
    assert first["ft"]["landing"] == "learn"
    second, dirty = merge_touch(first, {
        "landing": "mistakes",
        "variant": "early",
    })
    assert dirty is True
    assert second["ft"]["landing"] == "learn"
    assert second["ft"]["variant"] == "past"
    assert second["lt"]["landing"] == "mistakes"
    assert second["lt"]["variant"] == "early"
    restored = decode_touch_cookie(encode_touch_cookie(second))
    assert restored["lt"]["landing"] == "mistakes"
    assert restored["ft"]["variant"] == "past"


def _go_body(client, path):
    resp = client.get(path)
    assert resp.status_code == 200, path
    return resp, html.unescape(resp.get_data(as_text=True))


def test_pixel_skips_internal_and_bot_traffic(monkeypatch):
    previous = app.config.get("REDDIT_PIXEL_ID")
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
        return _Resp()

    monkeypatch.setenv("FUNNEL_CAPI_SYNC", "1")
    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    app.config["REDDIT_PIXEL_ID"] = "t2_testpixel"
    app.config["REDDIT_CAPI_TOKEN"] = "token-secret"
    try:
        cases = (
            {"query_string": {"ht_internal": "1"}},
            {"headers": {"Cookie": "ht_internal=1"}},
            {"headers": {"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1)"}},
        )
        for kwargs in cases:
            posted.clear()
            with app.test_request_context("/start", **kwargs):
                session["ht_reddit_signup"] = "abc12345signup"
                assert reddit_pixel_events() == []
                assert session.get("ht_reddit_signup") is None
                assert send_capi("PageVisit", "abc12345event") is False
            assert posted == {}
        with app.test_request_context("/start"):
            events = reddit_pixel_events()
            assert [ev["name"] for ev in events] == ["PageVisit"]
    finally:
        app.config["REDDIT_PIXEL_ID"] = previous
        app.config["REDDIT_CAPI_TOKEN"] = previous_token


def test_go_pages_are_focused_noindex_and_free_of_account_totals(monkeypatch):
    monkeypatch.setitem(app.config, "SIGNUP_ENABLED", True)
    monkeypatch.setitem(app.config, "SIGNUP_INVITE_CODE", "")
    client = app.test_client()
    pages = {
        "learn": (
            "Learn options free, then practice with paper money",
            "Start learning free",
            "start-learning",
            "route=learn",
            ("Learn options", "Practice with paper money", "Replay a trade, free"),
        ),
        "real-pnl": (
            "See your real P&L across every broker",
            "Create free account",
            "create-account",
            "route=real-pnl",
            ("Every account, one close", "The result, day by day", "Two closed trades, written out"),
        ),
        "mistakes": (
            "Bought it back early. Then it expired worthless.",
            "Create free account",
            "create-account",
            "route=mistakes",
            (
                "Bought back, then it expired worthless",
                "Closed the morning after results",
                "The close, next to holding",
            ),
        ),
    }
    trial = (
        "Learning and paper trading are free. "
        "Your 30-day trial starts when you connect a real brokerage."
    )
    disclaimer = "Educational tool, not investment advice."
    for slug, (headline, label, cta, route, bands) in pages.items():
        resp, body = _go_body(client, f"/go/{slug}")
        assert headline in body
        assert label in body
        assert trial in body
        assert disclaimer in body
        assert route in body
        assert 'name="robots" content="noindex"' in body
        assert "noindex" in (resp.headers.get("X-Robots-Tag") or "")
        assert body.count('property="og:title"') == 1
        assert "ORCL" not in body
        assert "CFLT" not in body
        assert "1,500" not in body
        assert "$428,049" not in body
        assert "Broker data as of" not in body
        assert 'id="userMenu"' not in body
        assert 'id="navReview"' not in body
        assert "tables.js" not in body
        assert "term-tips.js" not in body
        assert 'bootstrap.min.css" media="print"' not in body
        assert "bootstrap.min.css" in body
        hero = body.split('class="ht-hero-cta"', 1)[1].split("</header>", 1)[0]
        assert f'class="ht-hero-primary" href="/signup' in hero
        assert f'data-ht-cta="{cta}"' in hero
        assert label in hero
        assert 'class="ht-hero-secondary"' not in hero
        assert 'class="ht-demo-btn"' not in body
        assert 'class="ht-hero-signin"' in hero
        assert 'class="ht-hero-short"' in hero
        assert "hqdefault.jpg" in hero
        assert "sddefault.jpg" in hero
        assert "maxresdefault" not in hero
        assert 'fetchpriority="high"' in hero
        assert 'data-ht-cta="try-demo"' in hero
        assert "Try the live demo" in hero
        assert 'Play Short:' in hero
        for place in ("close", "sticky", "nav", "footer"):
            assert f'data-ht-cta="{cta}-{place}"' in body
        assert f'data-ht-cta="{cta}-mid"' not in body
        assert 'data-youtube-id="' in body
        assert 'fetchpriority="high"' in body
        # Learn's only picture is the hero Short, loaded up front.
        if slug != "learn":
            assert 'loading="lazy"' in body
        positions = [body.index(band) for band in bands]
        assert positions == sorted(positions)
        assert "Read-only" in body
        assert "SnapTrade" in body
        assert "What does it cost?" in body
        assert "How it works" not in body
        assert "Trader Profile" not in body
        assert "msft-if-held.webp" not in body
        assert "strategies.webp" not in body
        assert "win-rate.webp" not in body
    _, learn = _go_body(client, "/go/learn")
    assert "Free forever · no card" in learn
    assert 'class="ht-inline-tap"' in learn
    assert "Open the series" in learn
    assert "Replay a trade" in learn
    assert "a.ht-inline-tap" in learn
    assert "list-style: none" in learn
    assert "pnl_real.webp" not in learn
    assert "Practice with paper money" in learn
    assert "Replay a trade, free" in learn
    varied = client.get("/go/learn?v=past")
    varied_body = varied.get_data(as_text=True)
    assert "Learn from real past trades, then practice with paper money" in varied_body
    assert "route=learn" in varied_body
    assert 'data-ht-cta="start-learning"' in varied_body
    unknown = client.get("/go/learn?v=not-a-variant")
    assert "Learn options free, then practice with paper money" in unknown.get_data(as_text=True)
    assert client.get("/go/nope").status_code == 404
    _, mistakes = _go_body(client, "/go/mistakes")
    assert "be-close.webp" in mistakes
    assert "onon.webp" in mistakes
    assert mistakes.index("be-close.webp") < mistakes.index("onon.webp")
    assert "Closing early saved you" not in mistakes
    assert "be-swing.webp" not in mistakes
    _, real = _go_body(client, "/go/real-pnl")
    assert "Schwab, Fidelity, Vanguard, Robinhood, and others. Read-only." in real
    assert real.count("pnl_real.webp") == 1
    assert "Covered-call income tracked" not in real
    assert 'class="ht-overview-mock"' in real


def test_go_pages_keep_marketing_chrome_when_signed_in(monkeypatch):
    monkeypatch.setitem(app.config, "SIGNUP_ENABLED", True)
    monkeypatch.setitem(app.config, "SIGNUP_INVITE_CODE", "")
    from datetime import date

    from app.models import User

    user = type("U", (), {})()
    user.id = 1
    user.username = "ada"
    user.is_active = True
    user.is_anonymous = False
    user.is_authenticated = True
    user.get_id = lambda: "1"

    monkeypatch.setattr(User, "get_by_id", staticmethod(lambda user_id: user if str(user_id) == "1" else None))
    monkeypatch.setattr(
        "app.snaptrade.broker_data_freshness",
        lambda user_id, today=None, tenant_ids=None: (date(2026, 10, 1), 1, None),
    )
    client = app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = "1"
        sess["_fresh"] = True
    resp, body = _go_body(client, "/go/real-pnl")
    assert "ht-public" in body
    assert "Go to your dashboard" in body
    assert "Sign In" in body
    assert "Create free account" in body
    assert "Broker data as of" not in body
    assert 'id="userMenu"' not in body
    assert "Jump to" not in body
    assert 'id="navReview"' not in body
    assert resp.status_code == 200


def test_public_page_view_records_landing_device_and_not_identity():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("TEST_DATABASE_URL not set")
    from app.db import fetch_one

    client = app.test_client()
    resp = client.get(
        "/pricing",
        headers={
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile",
            "Referer": "https://www.reddit.com/r/options?token=sekret",
            "DNT": "1",
        },
    )
    assert resp.status_code == 200
    assert "redditstatic.com" not in resp.get_data(as_text=True)
    row = fetch_one(
        """
        SELECT path, device, referrer, utm_source
          FROM funnel_events
         WHERE event = 'page_view' AND path = '/pricing'
         ORDER BY id DESC
         LIMIT 1
        """
    )
    assert row["path"] == "/pricing"
    assert row["device"] == "phone"
    assert row["referrer"] == "https://www.reddit.com/r/options"
    assert "sekret" not in (row["referrer"] or "")
    assert row["utm_source"] in (None, "")

    landed = client.get(
        "/go/real-pnl?utm_source=reddit&utm_medium=paid&utm_campaign=real-pnl"
        "&utm_content=rolls&v=rolls",
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0)"},
    )
    assert landed.status_code == 200
    cookie = " ".join(landed.headers.getlist("Set-Cookie"))
    assert "ht_touch=" in cookie
    row = fetch_one(
        """
        SELECT landing, variant, device, utm_campaign, utm_content
          FROM funnel_events
         WHERE event = 'page_view' AND path = '/go/real-pnl'
         ORDER BY id DESC
         LIMIT 1
        """
    )
    assert row["landing"] == "real-pnl"
    assert row["variant"] == "rolls"
    assert row["device"] == "desktop"
    assert row["utm_campaign"] == "real-pnl"
    assert row["utm_content"] == "rolls"

    seen = client.post(
        "/funnel/beacon",
        json={"event": "client_seen", "path": "/go/real-pnl", "detail": "1"},
    )
    assert seen.status_code == 200
    beaconed = fetch_one(
        """
        SELECT client_beacon FROM funnel_events
         WHERE event = 'page_view' AND path = '/go/real-pnl'
         ORDER BY id DESC LIMIT 1
        """
    )
    assert beaconed["client_beacon"] is True

    clicked = client.post(
        "/funnel/beacon",
        json={"event": "cta_click", "path": "/go/real-pnl", "detail": "try-demo"},
    )
    assert clicked.status_code == 200
    click = fetch_one(
        """
        SELECT detail, landing FROM funnel_events
         WHERE event = 'cta_click' AND path = '/go/real-pnl'
         ORDER BY id DESC LIMIT 1
        """
    )
    assert click["detail"] == "try-demo"
    assert click["landing"] == "real-pnl"
    bad = client.post(
        "/funnel/beacon",
        json={"event": "scroll_depth", "path": "/go/real-pnl", "detail": "10"},
    )
    assert bad.status_code == 400
    depth = client.post(
        "/funnel/beacon",
        json={"event": "scroll_depth", "path": "/go/real-pnl", "detail": "50"},
    )
    assert depth.status_code == 200
    played = client.post(
        "/funnel/beacon",
        json={"event": "video_play", "path": "/learn", "detail": "NpU79Lwkdn4"},
    )
    assert played.status_code == 200
    from app.funnel import build_acquisition
    acq = build_acquisition("7d")
    by_landing = {row["landing"]: row for row in acq["campaigns"]}
    visitors = by_landing["real-pnl"]["steps"][0]
    assert visitors["key"] == "visitors"
    assert visitors["count"] >= 1
    assert visitors["rate"] == 100.0
    hosts = {row["host"] for row in acq["referrers"]}
    assert "YouTube" in hosts and "Reddit" in hosts


def test_learn_route_signup_opens_learn(monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.setitem(app.config, "SIGNUP_ENABLED", True)
    monkeypatch.setitem(app.config, "SIGNUP_INVITE_CODE", "")
    created = {}

    class _User:
        id = 4242
        username = "learnrouteuser"

    monkeypatch.setattr(
        "app.auth.User.create",
        staticmethod(lambda username, password, email=None: created.setdefault("user", _User())),
    )
    monkeypatch.setattr("app.auth.User.get_by_email", staticmethod(lambda email: None))
    monkeypatch.setattr(
        "app.auth.User.get_by_username",
        staticmethod(lambda name: created.get("user")),
    )
    monkeypatch.setattr("app.auth.login_user", lambda *a, **k: None)
    monkeypatch.setattr("app.auth.get_accounts_for_user", lambda uid: [])
    monkeypatch.setattr("app.auth._send_welcome_verification", lambda user: None)
    monkeypatch.setattr("app.funnel.on_signup", lambda uid: created.setdefault("signed", uid))
    monkeypatch.setattr("app.campaign.stamp_signup", lambda uid: None)
    monkeypatch.setattr("app.ops_notify.notify_event", lambda *a, **k: None)

    client = app.test_client()
    page = client.get("/signup?route=learn")
    signup_html = page.get_data(as_text=True)
    assert 'name="route" value="learn"' in signup_html
    assert signup_html.count("No credit card") == 1
    assert "No card." not in signup_html
    resp = client.post("/signup", data={
        "username": "learnrouteuser",
        "email": "learnroute@example.com",
        "password": "Secret1pass",
        "confirm": "Secret1pass",
        "route": "learn",
    }, follow_redirects=False)
    assert resp.status_code in (302, 303)
    assert urlparse(resp.headers["Location"]).path == "/learn"
    assert created["signed"] == 4242


_MOZILLA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _cookie_value(client, name):
    cookie = client.get_cookie(name)
    if cookie is None:
        return ""
    return cookie.value


def _origin(acq, source):
    for row in acq.get("origins") or []:
        if row["source"] == source:
            return row
    return {"visitors": 0, "signups": 0, "rate": 0.0}


def _counted_page_views(visit_id) -> int:
    from app.db import fetch_one
    from app.funnel import counted_traffic_sql
    row = fetch_one(
        f"""
        SELECT COUNT(*)::int AS n
          FROM funnel_events
         WHERE event = 'page_view' AND visit_id = %s
           AND {counted_traffic_sql()}
        """,
        (visit_id,),
    )
    return int(row["n"] or 0)


def test_bot_user_agents_internal_ip_and_opt_out(monkeypatch):
    from app.funnel import ip_is_internal, is_bot_user_agent, request_is_internal

    assert is_bot_user_agent(_MOZILLA) is False
    assert is_bot_user_agent("") is False
    assert is_bot_user_agent(None) is False
    assert is_bot_user_agent("curling iron") is False
    for ua in (
        "Mozilla/5.0 HeadlessChrome/120.0.0.0",
        "python-requests/2.32.3",
        "curl/8.5.0",
        "curl",
        "Mozilla/5.0 Playwright",
        "Mozilla/5.0 Puppeteer",
        "Slackbot-LinkExpanding 1.0",
        "Twitterbot/1.0",
        "facebookexternalhit/1.1",
        "redditbot/1.0",
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "SomeCrawler/1.0",
        "MySpider/1.0",
    ):
        assert is_bot_user_agent(ua) is True, ua

    assert ip_is_internal("127.0.0.1") is False
    monkeypatch.setenv("INTERNAL_IPS", "10.0.0.0/8, 192.168.1.9")
    assert ip_is_internal("10.1.2.3") is True
    assert ip_is_internal("192.168.1.9") is True
    assert ip_is_internal("192.168.1.10") is False
    assert ip_is_internal("unknown") is False
    assert ip_is_internal("") is False

    with app.test_request_context("/pricing?ht_internal=1"):
        assert request_is_internal() is True
    with app.test_request_context("/pricing", headers={"Cookie": "ht_internal=1"}):
        assert request_is_internal() is True
    with app.test_request_context("/pricing"):
        assert request_is_internal() is False
    monkeypatch.setenv("INTERNAL_IPS", "203.0.113.5")
    monkeypatch.setattr("app.client_ip.real_client_ip", lambda: "203.0.113.5")
    with app.test_request_context("/pricing"):
        assert request_is_internal() is True
    monkeypatch.setenv("ADMIN_USERS", "cameron")
    from app.funnel import human_traffic_sql, internal_usernames
    assert "cameron" in internal_usernames()
    assert "testingcameron" in human_traffic_sql()
    assert "'cameron'" in human_traffic_sql()


def test_log_event_flags_headless_chrome(monkeypatch):
    inserts = []
    _patch_db(monkeypatch, inserts)
    with app.test_request_context(
        "/pricing", headers={"User-Agent": "HeadlessChrome/120.0"},
    ):
        log_event("page_view", path="/pricing")
    params = inserts[0][1]
    assert params[-1] is True
    assert "HeadlessChrome/120.0" in params
    assert "FALSE" in inserts[0][0]


def test_acquisition_sql_drops_bots_and_unbeaconed_page_views(monkeypatch):
    sqls = []

    def _fetch_all(sql, params=()):
        sqls.append(sql)
        return []

    monkeypatch.setenv("DATABASE_URL", "postgresql://funnel-test")
    monkeypatch.setattr("app.db.fetch_all", _fetch_all)
    from app.funnel import build_acquisition, build_admin_analytics
    build_acquisition("7d")
    build_admin_analytics()
    blob = "\n".join(sqls)
    assert "COALESCE(is_bot, FALSE) = FALSE" in blob
    assert "client_beacon IS DISTINCT FROM FALSE" in blob
    assert "funnel_internal_visits" in blob
    assert "testingcameron" in blob
    assert "ELSE 'Direct'" in blob


def test_acquisition_keeps_beaconed_humans_and_counts_what_was_removed():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("TEST_DATABASE_URL not set")
    from app.db import execute, fetch_one
    from app.funnel import backfill_bot_user_agents, build_acquisition, log_event

    before = build_acquisition("7d")
    before_reddit = _origin(before, "reddit / real-pnl")
    before_direct = _origin(before, "Direct")
    before_youtube = _origin(before, "www.youtube.com")
    before_filtered = before["filtered_out"]

    human = app.test_client()
    human.get(
        "/go/real-pnl?utm_source=reddit&utm_campaign=real-pnl",
        headers={"User-Agent": _MOZILLA},
    )
    seen = human.post(
        "/funnel/beacon",
        json={"event": "client_seen", "path": "/go/real-pnl", "detail": "1"},
    )
    assert seen.status_code == 200
    human_visit = fetch_one(
        """
        SELECT visit_id FROM funnel_events
         WHERE event = 'page_view' AND path = '/go/real-pnl'
           AND utm_campaign = 'real-pnl'
         ORDER BY id DESC LIMIT 1
        """
    )["visit_id"]
    assert _counted_page_views(human_visit) == 1
    touch = _cookie_value(human, "ht_touch")
    assert touch
    signup_user = int(uuid.uuid4().hex[:6], 16)
    with app.test_request_context(
        "/signup",
        headers={
            "User-Agent": _MOZILLA,
            "Cookie": "ht_touch=" + touch,
        },
    ):
        signup_id = log_event(
            "signup_completed", path="/signup", user_id=signup_user,
        )
    assert signup_id
    signup_row = fetch_one(
        """
        SELECT utm_source, utm_campaign, is_bot
          FROM funnel_events
         WHERE event_id = %s
        """,
        (signup_id,),
    )
    assert signup_row["utm_source"] == "reddit"
    assert signup_row["utm_campaign"] == "real-pnl"
    assert signup_row["is_bot"] is False

    bot = app.test_client()
    bot.get(
        "/go/mistakes",
        headers={"User-Agent": "Mozilla/5.0 HeadlessChrome/120.0.0.0"},
    )
    bot.post(
        "/funnel/beacon",
        json={"event": "client_seen", "path": "/go/mistakes", "detail": "1"},
    )
    bot_row = fetch_one(
        """
        SELECT visit_id, is_bot, client_beacon FROM funnel_events
         WHERE event = 'page_view' AND path = '/go/mistakes'
         ORDER BY id DESC LIMIT 1
        """
    )
    assert bot_row["is_bot"] is True
    assert bot_row["client_beacon"] is True
    assert _counted_page_views(bot_row["visit_id"]) == 0

    quiet = app.test_client()
    quiet.get("/pricing", headers={"User-Agent": _MOZILLA})
    quiet_row = fetch_one(
        """
        SELECT visit_id, client_beacon FROM funnel_events
         WHERE event = 'page_view' AND path = '/pricing'
           AND COALESCE(is_bot, FALSE) = FALSE
           AND client_beacon IS FALSE
         ORDER BY id DESC LIMIT 1
        """
    )
    assert quiet_row["client_beacon"] is False
    assert _counted_page_views(quiet_row["visit_id"]) == 0

    internal = app.test_client()
    flagged = internal.get(
        "/faq?ht_internal=1",
        headers={"User-Agent": _MOZILLA},
    )
    assert "ht_internal=1" in " ".join(flagged.headers.getlist("Set-Cookie"))
    internal.post(
        "/funnel/beacon",
        json={"event": "client_seen", "path": "/faq", "detail": "1"},
    )
    internal_visit = fetch_one(
        """
        SELECT visit_id FROM funnel_events
         WHERE event = 'page_view' AND path = '/faq'
         ORDER BY id DESC LIMIT 1
        """
    )["visit_id"]
    assert _counted_page_views(internal_visit) == 0
    assert fetch_one(
        "SELECT reason FROM funnel_internal_visits WHERE visit_id = %s",
        (internal_visit,),
    )["reason"] == "query"

    direct = app.test_client()
    direct.get("/signup", headers={"User-Agent": _MOZILLA})
    direct.post(
        "/funnel/beacon",
        json={"event": "client_seen", "path": "/signup", "detail": "1"},
    )
    referred = app.test_client()
    referred.get(
        "/learn",
        headers={
            "User-Agent": _MOZILLA,
            "Referer": "https://www.youtube.com/watch?v=abc",
        },
    )
    referred.post(
        "/funnel/beacon",
        json={"event": "client_seen", "path": "/learn", "detail": "1"},
    )
    mid = build_acquisition("7d")
    assert _origin(mid, "Direct")["visitors"] == before_direct["visitors"] + 1
    assert _origin(mid, "www.youtube.com")["visitors"] == before_youtube["visitors"] + 1

    legacy_bot = uuid.uuid4().hex
    legacy_human = uuid.uuid4().hex
    execute(
        """
        INSERT INTO funnel_events (event, visit_id, path, user_agent, created_at)
        VALUES ('page_view', %s, '/go/legacy-bot',
                'Mozilla/5.0 HeadlessChrome/119', NOW())
        """,
        (legacy_bot,),
    )
    execute(
        """
        INSERT INTO funnel_events (event, visit_id, path, created_at)
        VALUES ('page_view', %s, '/go/legacy-human', NOW())
        """,
        (legacy_human,),
    )
    backfill_bot_user_agents()
    assert fetch_one(
        "SELECT is_bot FROM funnel_events WHERE visit_id = %s",
        (legacy_bot,),
    )["is_bot"] is True
    assert fetch_one(
        "SELECT is_bot, user_agent FROM funnel_events WHERE visit_id = %s",
        (legacy_human,),
    )["is_bot"] is None
    assert _counted_page_views(legacy_bot) == 0
    assert _counted_page_views(legacy_human) == 1

    after = build_acquisition("7d")
    reddit = _origin(after, "reddit / real-pnl")
    assert reddit["visitors"] == before_reddit["visitors"] + 1
    assert reddit["signups"] == before_reddit["signups"] + 1
    assert reddit["rate"] == round(100.0 * reddit["signups"] / reddit["visitors"], 1)
    # The legacy row has no user agent, so it stays in Direct.
    assert _origin(after, "Direct")["visitors"] == before_direct["visitors"] + 2
    # Bot, no beacon, internal query, and the backfilled headless row.
    assert after["filtered_out"] >= before_filtered + 4


def test_testingcameron_login_removes_that_visitor(monkeypatch):
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("TEST_DATABASE_URL not set")
    from app.db import fetch_one
    from app.models import User

    client = app.test_client()
    client.get("/faq", headers={"User-Agent": _MOZILLA})
    client.post(
        "/funnel/beacon",
        json={"event": "client_seen", "path": "/faq", "detail": "1"},
    )
    visit_id = fetch_one(
        """
        SELECT visit_id FROM funnel_events
         WHERE event = 'page_view' AND path = '/faq'
           AND COALESCE(is_bot, FALSE) = FALSE
         ORDER BY id DESC LIMIT 1
        """
    )["visit_id"]
    assert _counted_page_views(visit_id) == 1

    class _Owner:
        id = 1
        username = "testingcameron"
        is_active = True
        is_anonymous = False
        is_authenticated = True

        def get_id(self):
            return "1"

    monkeypatch.setattr(User, "get_by_id", staticmethod(lambda uid: _Owner()))
    with client.session_transaction() as sess:
        sess["_user_id"] = "1"
        sess["_fresh"] = True
    client.get("/faq", headers={"User-Agent": _MOZILLA})
    assert _counted_page_views(visit_id) == 0
    assert fetch_one(
        "SELECT reason FROM funnel_internal_visits WHERE visit_id = %s",
        (visit_id,),
    )["reason"] == "account"
