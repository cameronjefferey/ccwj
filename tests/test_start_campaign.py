"""Campaign landing /start — copy, attribution cookie, funnel math."""

from flask import g, session

from app import app
from app.campaign import (
    clean_utm,
    decode_cookie,
    encode_cookie,
    filter_funnel_events,
    merge_attribution,
    parse_utms,
    reddit_pixel_context,
    stamp_signup,
    summarize_funnel,
)


def _client():
    return app.test_client()


def test_start_explains_the_mirror_and_the_honest_offer(monkeypatch):
    monkeypatch.setattr("app.campaign.record_event", lambda *a, **k: None)
    resp = _client().get(
        "/start?utm_source=reddit&utm_campaign=mirror-v1&utm_content=score"
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Your broker shows the number" in body
    assert "This shows the trades behind it" in body
    assert "Closed one early" in body
    assert "30 days" in body
    assert "No credit card" in body
    assert "Read-only" in body
    assert "5 years" not in body
    assert "five years" not in body.lower()
    assert 'href="/start/go/signup?utm_source=reddit' in body
    assert "utm_content=score" in body
    assert "ht_acq=" in (resp.headers.get("Set-Cookie") or "")
    assert 'name="robots" content="noindex"' in body


def test_start_refresh_does_not_log_a_second_visit(monkeypatch):
    events = []
    monkeypatch.setattr(
        "app.campaign.record_event",
        lambda *a, **k: events.append(a[0]),
    )
    client = _client()
    qs = "/start?utm_source=reddit&utm_campaign=mirror-v1&utm_content=score"
    assert client.get(qs).status_code == 200
    assert events == ["visit"]
    assert client.get(qs).status_code == 200
    assert events == ["visit"]


def test_new_creative_starts_a_new_visit(monkeypatch):
    events = []
    monkeypatch.setattr(
        "app.campaign.record_event",
        lambda *a, **k: events.append(a[0]),
    )
    client = _client()
    client.get("/start?utm_source=reddit&utm_campaign=mirror-v1&utm_content=score")
    client.get("/start?utm_source=reddit&utm_campaign=mirror-v1&utm_content=you")
    assert events == ["visit", "visit"]


def test_logged_in_start_goes_to_overview(monkeypatch):
    from app.models import User

    class _User:
        is_active = True
        is_anonymous = False
        is_authenticated = True
        id = 1
        username = "alice"

        def get_id(self):
            return "1"

    monkeypatch.setattr(User, "get_by_id", staticmethod(lambda uid: _User()))
    client = _client()
    with client.session_transaction() as sess:
        sess["_user_id"] = "1"
        sess["_fresh"] = True
    resp = client.get("/start")
    assert resp.status_code == 302
    assert "/overview" in (resp.headers.get("Location") or "")


def test_signup_click_is_logged_and_redirects(monkeypatch):
    events = []
    monkeypatch.setattr(
        "app.campaign.record_event",
        lambda *a, **k: events.append(a[0]),
    )
    resp = _client().get("/start/go/signup?utm_source=reddit&utm_content=book")
    assert resp.status_code == 302
    assert (resp.headers.get("Location") or "").endswith("/signup")
    assert events == ["visit", "signup_click"]


def test_demo_click_redirects_without_following(monkeypatch):
    monkeypatch.setattr("app.campaign.record_event", lambda *a, **k: None)
    resp = _client().get("/start/go/demo")
    assert resp.status_code == 302
    assert "/demo/start" in (resp.headers.get("Location") or "")


def test_unknown_destination_is_404():
    assert _client().get("/start/go/pricing").status_code == 404


def test_record_event_writes_visit_id_into_session_id(monkeypatch):
    """An earlier local table made session_id NOT NULL. New rows fill both."""
    captured = []

    def _execute(sql, params=()):
        captured.append((sql, tuple(params)))

    monkeypatch.setattr("app.db.execute", _execute)
    from app.campaign import record_event
    record_event("visit", {
        "visit_id": "c" * 32,
        "utm_source": "reddit",
        "utm_campaign": "mirror-v1",
        "utm_content": "score",
    })
    sql, params = captured[0]
    assert "session_id" in sql
    assert params[1] == "c" * 32
    assert params[2] == "c" * 32


def test_utm_values_are_a_tight_token():
    assert clean_utm("mirror-v1") == "mirror-v1"
    assert clean_utm(" score ") == "score"
    assert clean_utm("bad value") == ""
    assert clean_utm("https://evil.example") == ""
    assert parse_utms({"utm_source": "reddit", "utm_campaign": "bad value"}) == {
        "utm_source": "reddit",
        "utm_campaign": "",
        "utm_content": "",
    }


def test_cookie_roundtrip_and_fresh_visit_on_creative_change():
    attr = {
        "visit_id": "a" * 32,
        "utm_source": "reddit",
        "utm_campaign": "mirror-v1",
        "utm_content": "score",
        "visit_logged": True,
    }
    decoded = decode_cookie(encode_cookie(attr, visit_logged=True))
    assert decoded["visit_id"] == "a" * 32
    assert decoded["visit_logged"] is True
    assert decode_cookie("nope") is None

    same, log_again = merge_attribution(
        decoded, {"utm_source": "reddit", "utm_campaign": "mirror-v1", "utm_content": "score"},
    )
    assert log_again is False
    assert same["visit_id"] == "a" * 32

    nxt, log_new = merge_attribution(
        decoded, {"utm_source": "reddit", "utm_campaign": "mirror-v1", "utm_content": "you"},
    )
    assert log_new is True
    assert nxt["visit_id"] != "a" * 32
    assert nxt["utm_content"] == "you"


def test_funnel_counts_connects_separately_from_signups():
    events = [
        {"event": "visit", "visit_id": "v1", "user_id": None,
         "utm_campaign": "mirror-v1", "utm_content": "score"},
        {"event": "visit", "visit_id": "v1", "user_id": None,
         "utm_campaign": "mirror-v1", "utm_content": "score"},
        {"event": "signup_click", "visit_id": "v1", "user_id": None,
         "utm_campaign": "mirror-v1", "utm_content": "score"},
        {"event": "signup", "visit_id": "v1", "user_id": 7,
         "utm_campaign": "mirror-v1", "utm_content": "score"},
        {"event": "visit", "visit_id": "v2", "user_id": None,
         "utm_campaign": "mirror-v1", "utm_content": "you"},
        {"event": "signup", "visit_id": "v2", "user_id": 8,
         "utm_campaign": "mirror-v1", "utm_content": "you"},
        {"event": "demo_click", "visit_id": "v2", "user_id": None,
         "utm_campaign": "mirror-v1", "utm_content": "you"},
    ]
    rows = summarize_funnel(events, connected_user_ids={7})
    by_creative = {row["creative"]: row for row in rows}
    score = by_creative["score"]
    assert score["visits"] == 1  # same visit id twice is one landing
    assert score["signup_clicks"] == 1
    assert score["signups"] == 1
    assert score["connected"] == 1
    assert score["signup_rate"] == 100.0
    assert score["connect_rate"] == 100.0
    you = by_creative["you"]
    assert you["signups"] == 1
    assert you["connected"] == 0
    assert you["connect_rate"] == 0.0
    assert you["demo_clicks"] == 1

    only_you = filter_funnel_events(events, creative="you")
    assert {ev["utm_content"] for ev in only_you} == {"you"}


def test_stamp_signup_writes_the_cookie_onto_the_user(monkeypatch):
    updates = []
    events = []

    def _execute(sql, params=()):
        updates.append((sql, tuple(params)))

    monkeypatch.setattr("app.db.execute", _execute)
    monkeypatch.setattr(
        "app.campaign.record_event",
        lambda *a, **k: events.append((a, k)),
    )
    raw = encode_cookie({
        "visit_id": "b" * 32,
        "utm_source": "reddit",
        "utm_campaign": "mirror-v1",
        "utm_content": "book",
    }, visit_logged=True)
    with app.test_request_context("/", headers={"Cookie": f"ht_acq={raw}"}):
        stamp_signup(42)
        assert session.get("ht_reddit_signup") is True
    sql, params = updates[0]
    assert "acquisition_source" in sql
    assert params == ("reddit", "mirror-v1", "book", 42)
    assert events[0][0][0] == "signup"
    assert events[0][1]["user_id"] == 42


def test_reddit_pixel_fires_pagevisit_and_signup_then_clears():
    previous = app.config.get("REDDIT_PIXEL_ID")
    app.config["REDDIT_PIXEL_ID"] = "t2_testpixel"
    try:
        with app.test_request_context("/start"):
            g.campaign_pixel_event = "PageVisit"
            ctx = reddit_pixel_context()
            assert ctx["reddit_pixel_event"] == "PageVisit"
            assert ctx["reddit_pixel_id"] == "t2_testpixel"
        with app.test_request_context("/get-started"):
            session["ht_reddit_signup"] = True
            ctx = reddit_pixel_context()
            assert ctx["reddit_pixel_event"] == "SignUp"
            assert session.get("ht_reddit_signup") is None
        app.config["REDDIT_PIXEL_ID"] = "not a pixel"
        with app.test_request_context("/start"):
            g.campaign_pixel_event = "PageVisit"
            assert reddit_pixel_context()["reddit_pixel_id"] == ""
    finally:
        app.config["REDDIT_PIXEL_ID"] = previous
