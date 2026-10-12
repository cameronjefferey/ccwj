"""In-app page views and the admin per-user timeline."""

import os
import re
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from app import app as flask_app
from app.funnel import (
    ACTIVITY_EVENT_LIMIT,
    EVENTS,
    _activity_event_sql,
    _observe,
    format_pt,
    group_activity_sessions,
    log_event,
    viewer_is_hidden_from_admins,
)


def _html_response(status=200, content_type="text/html; charset=utf-8", location=""):
    class _Response:
        def __init__(self):
            self.status_code = status
            self.headers = {}
            self.mimetype = (content_type or "").split(";")[0]
            if content_type:
                self.headers["Content-Type"] = content_type
            if location:
                self.headers["Location"] = location

    return _Response()


class _Account:
    def __init__(self, uid, username):
        self.is_authenticated = True
        self.id = uid
        self.username = username


def _capture(monkeypatch):
    calls = []

    def _log(event, **kwargs):
        calls.append((event, kwargs))
        return "event-id"

    monkeypatch.setattr("app.funnel.log_event", _log)
    return calls


def _watch(monkeypatch, username="alice", uid=42, path="/overview", headers=None):
    monkeypatch.setattr(
        "flask_login.current_user", _Account(uid, username),
    )
    monkeypatch.setattr("app.funnel.viewer_is_hidden_from_admins", lambda _uid: False)
    return flask_app.test_request_context(path, headers=headers or {})


def test_app_page_view_is_not_a_reddit_milestone():
    spec = EVENTS["app_page_view"]
    assert spec["first"] is False
    assert spec["reddit"] is None
    assert "app_page_view" not in {
        key for key, _label in __import__("app.funnel", fromlist=["FUNNEL_STEPS"]).FUNNEL_STEPS
    }


def test_app_page_view_logged_for_signed_in_html_navigation(monkeypatch):
    calls = _capture(monkeypatch)
    with _watch(monkeypatch, path="/overview?tenants=sekret-scope"):
        _observe(_html_response())
    logged = [item for item in calls if item[0] == "app_page_view"]
    assert len(logged) == 1
    assert logged[0][1]["path"] == "/overview"
    assert "sekret-scope" not in logged[0][1]["path"]
    assert logged[0][1]["referrer"] == ""


@pytest.mark.parametrize(
    "username,path,headers,response_type,session_extra",
    [
        ("cameron", "/overview", None, "text/html", None),
        ("demo", "/overview", None, "text/html", None),
        ("alice", "/static/app.css", None, "text/html", None),
        ("alice", "/api/nav/symbols", None, "application/json", None),
        ("alice", "/overview", {"X-Requested-With": "XMLHttpRequest"}, "text/html", None),
        ("alice", "/overview", {"HX-Request": "true"}, "text/html", None),
        ("alice", "/overview", {"X-HT-Full": "1"}, "text/html", None),
        ("alice", "/overview", {"DNT": "1"}, "text/html", None),
        ("alice", "/healthz", None, "text/html", None),
        ("alice", "/funnel/beacon", None, "text/html", None),
        ("alice", "/admin/users", None, "text/html", None),
        ("alice", "/overview/below", None, "text/html", None),
    ],
)
def test_app_page_view_skipped_for_excluded_requests(
    monkeypatch, username, path, headers, response_type, session_extra,
):
    calls = _capture(monkeypatch)
    with _watch(monkeypatch, username=username, path=path, headers=headers):
        from flask import session
        if session_extra:
            session.update(session_extra)
        _observe(_html_response(content_type=response_type))
    assert [item for item in calls if item[0] == "app_page_view"] == []


def test_app_page_view_skipped_while_impersonating(monkeypatch):
    calls = _capture(monkeypatch)
    with _watch(monkeypatch):
        from flask import session
        session["_impersonator_id"] = 7
        _observe(_html_response())
    assert [item for item in calls if item[0] == "app_page_view"] == []


def test_app_page_view_skipped_for_private_user(monkeypatch):
    calls = _capture(monkeypatch)
    monkeypatch.setattr("app.funnel.viewer_is_hidden_from_admins", lambda _uid: True)
    monkeypatch.setattr("flask_login.current_user", _Account(42, "alice"))
    with flask_app.test_request_context("/overview"):
        _observe(_html_response())
    assert [item for item in calls if item[0] == "app_page_view"] == []


def test_app_page_view_skipped_when_privacy_list_cannot_be_read(monkeypatch):
    calls = _capture(monkeypatch)
    monkeypatch.setattr("app.funnel._hidden_user_ids", lambda: None)
    monkeypatch.setattr("flask_login.current_user", _Account(42, "alice"))
    assert viewer_is_hidden_from_admins(42) is True
    with flask_app.test_request_context("/overview"):
        _observe(_html_response())
    assert [item for item in calls if item[0] == "app_page_view"] == []


def test_app_page_view_skipped_for_redirect_and_json(monkeypatch):
    calls = _capture(monkeypatch)
    with _watch(monkeypatch):
        _observe(_html_response(status=302, location="/login"))
    with _watch(monkeypatch):
        _observe(_html_response(content_type="application/json"))
    assert [item for item in calls if item[0] == "app_page_view"] == []


def test_app_page_view_dedupes_rapid_reloads(monkeypatch):
    inserts = []
    monkeypatch.setenv("DATABASE_URL", "postgresql://funnel-test")

    def _execute(sql, params=()):
        inserts.append((sql, tuple(params)))

    state = {"row": None}

    def _fetch_one(sql, params=()):
        if "interval" in sql:
            assert params[-2] == "10"
            assert params[-1] == " minutes"
        return state["row"]

    monkeypatch.setattr("app.db.execute", _execute)
    monkeypatch.setattr("app.db.fetch_one", _fetch_one)
    monkeypatch.setattr("app.funnel.send_capi", lambda *a, **k: (_ for _ in ()).throw(AssertionError("reddit")))
    with flask_app.test_request_context("/overview"):
        assert log_event("app_page_view", user_id=42, path="/overview")
    assert inserts and inserts[0][1][0] == "app_page_view"
    state["row"] = {"?column?": 1}
    inserts.clear()
    with flask_app.test_request_context("/overview"):
        assert log_event("app_page_view", user_id=42, path="/overview") is None
    assert inserts == []


def test_activity_query_is_not_a_per_row_exists():
    sql = _activity_event_sql().lower()
    assert "exists" not in sql
    assert "user_id is null" in sql
    assert str(ACTIVITY_EVENT_LIMIT + 1) in sql


def test_sessions_follow_time_then_split_on_gap_and_visit():
    start = datetime(2026, 10, 12, 19, 0, tzinfo=timezone.utc)
    events = [
        {"created_at": start, "visit_id": "a" * 32, "event": "page_view"},
        {"created_at": start + timedelta(minutes=5), "visit_id": "a" * 32, "event": "app_page_view"},
        {"created_at": start + timedelta(minutes=50), "visit_id": "a" * 32, "event": "app_page_view"},
        {"created_at": start + timedelta(minutes=51), "visit_id": "b" * 32, "event": "page_view"},
    ]
    sessions = group_activity_sessions(events)
    assert [len(group["events"]) for group in sessions] == [2, 1, 1]
    assert format_pt(start) == "Oct 12, 12:00p PT"


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _create_user(conn, username: str) -> int:
    from werkzeug.security import generate_password_hash

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (username, password_hash, email) "
            "VALUES (%s, %s, %s) RETURNING id",
            (username, generate_password_hash("testpass123"), f"{username}@example.com"),
        )
        row = cur.fetchone()
    conn.commit()
    return int(row["id"])


def _login(http, username: str):
    resp = http.post(
        "/login",
        data={"username": username, "password": "testpass123"},
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303)


def _logout(http):
    http.post("/logout", follow_redirects=False)


@pytest.fixture
def activity_world(app, db_conn, monkeypatch):
    operator = _unique("act_op")
    person = _unique("act_person")
    stranger = _unique("act_stranger")
    private = _unique("act_private")
    monkeypatch.setenv("ADMIN_USERS", operator)
    operator_id = _create_user(db_conn, operator)
    person_id = _create_user(db_conn, person)
    stranger_id = _create_user(db_conn, stranger)
    private_id = _create_user(db_conn, private)
    signed = datetime(2026, 10, 2, 15, 0, tzinfo=timezone.utc)
    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO user_profiles (user_id, created_at)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET created_at = EXCLUDED.created_at
            """,
            (person_id, signed),
        )
        cur.execute(
            """
            UPDATE users
               SET acquisition_source = 'zz-source',
                   acquisition_campaign = 'zz-campaign',
                   acquisition_landing = 'zz-land',
                   acquisition_lt_source = 'zz-lt-source',
                   acquisition_lt_campaign = 'zz-lt-campaign',
                   acquisition_lt_landing = 'zz-lt-land'
             WHERE id = %s
            """,
            (person_id,),
        )
    db_conn.commit()
    from app.admin_privacy import add_hidden_user
    ok, info = add_hidden_user(private_id, operator_id)
    assert ok, info
    http = app.test_client()
    state = {
        "http": http,
        "operator": operator,
        "operator_id": operator_id,
        "person": person,
        "person_id": person_id,
        "stranger": stranger,
        "stranger_id": stranger_id,
        "private": private,
        "private_id": private_id,
        "visits": [],
    }
    try:
        yield state
    finally:
        _logout(http)
        with db_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM funnel_events WHERE user_id = ANY(%s) OR visit_id = ANY(%s)",
                (
                    [operator_id, person_id, stranger_id, private_id],
                    state["visits"] or ["none"],
                ),
            )
            cur.execute(
                "DELETE FROM users WHERE id = ANY(%s)",
                ([operator_id, person_id, stranger_id, private_id],),
            )
        db_conn.commit()


def _insert_event(conn, **kwargs):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO funnel_events
                (created_at, event, visit_id, user_id, path, detail,
                 user_agent, is_bot, client_beacon)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, FALSE)
            """,
            (
                kwargs["created_at"],
                kwargs["event"],
                kwargs.get("visit_id"),
                kwargs.get("user_id"),
                kwargs.get("path"),
                kwargs.get("detail"),
                kwargs.get("user_agent"),
                kwargs.get("is_bot"),
            ),
        )
    conn.commit()


def test_signed_in_navigation_inserts_one_app_page_view(app, db_conn, monkeypatch):
    from flask_login import login_user

    from app.funnel import clear_hidden_user_cache
    from app.models import User

    username = _unique("act_live")
    user_id = _create_user(db_conn, username)
    clear_hidden_user_cache()
    monkeypatch.delenv("ADMIN_USERS", raising=False)
    try:
        with app.test_request_context("/positions?tenant=sekret-tenant"):
            login_user(User.get_by_id(user_id))
            _observe(_html_response())
            _observe(_html_response())
        from app.db import fetch_all
        rows = fetch_all(
            """
            SELECT event, path, user_id, visit_id
            FROM funnel_events
            WHERE user_id = %s AND event = 'app_page_view'
            """,
            (user_id,),
        )
        assert len(rows) == 1
        assert rows[0]["path"] == "/positions"
        assert "sekret" not in (rows[0]["path"] or "")
        assert rows[0]["visit_id"]
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM funnel_events WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        db_conn.commit()


def test_activity_page_renders_timeline_in_order(activity_world, db_conn):
    person_id = activity_world["person_id"]
    stranger_id = activity_world["stranger_id"]
    visit_a = uuid.uuid4().hex
    visit_b = uuid.uuid4().hex
    activity_world["visits"].extend([visit_a, visit_b])
    start = datetime(2026, 10, 12, 19, 0, tzinfo=timezone.utc)
    rows = [
        dict(created_at=start, event="page_view", visit_id=visit_a, path="/pricing"),
        dict(created_at=start + timedelta(minutes=1), event="scroll_depth",
             visit_id=visit_a, path="/pricing", detail="75"),
        dict(created_at=start + timedelta(minutes=2), event="cta_click",
             visit_id=visit_a, path="/pricing", detail="cta-hero-zz"),
        dict(created_at=start + timedelta(minutes=3), event="signup_completed",
             visit_id=visit_a, user_id=person_id, path="/signup"),
        dict(created_at=start + timedelta(minutes=4), event="app_page_view",
             visit_id=visit_a, user_id=person_id, path="/overview"),
        dict(created_at=start + timedelta(minutes=5), event="time_on_page",
             visit_id=visit_a, user_id=person_id, path="/overview", detail="60"),
        dict(created_at=start + timedelta(minutes=6), event="page_view",
             visit_id=visit_a, path="/bot-only-zz",
             user_agent="Mozilla/5.0 (compatible; Googlebot/2.1)", is_bot=True),
        dict(created_at=start + timedelta(minutes=119), event="page_view",
             visit_id=visit_b, path="/go/real-pnl"),
        dict(created_at=start + timedelta(minutes=120), event="app_page_view",
             visit_id=visit_b, user_id=person_id, path="/positions"),
        dict(created_at=start + timedelta(minutes=121), event="app_page_view",
             visit_id=visit_b, user_id=stranger_id, path="/other-user-secret"),
        dict(created_at=start, event="page_view", visit_id=uuid.uuid4().hex,
             path="/unrelated-visit-zz"),
    ]
    for row in rows:
        _insert_event(db_conn, **row)

    http = activity_world["http"]
    _login(http, activity_world["operator"])
    page = http.get(f"/admin/users/{person_id}/activity")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    found = re.findall(
        r'data-event="([^"]*)" data-path="([^"]*)" data-detail="([^"]*)"',
        body,
    )
    assert found == [
        ("page_view", "/pricing", ""),
        ("scroll_depth", "/pricing", "75%"),
        ("cta_click", "/pricing", "cta-hero-zz"),
        ("signup_completed", "/signup", ""),
        ("app_page_view", "/overview", ""),
        ("time_on_page", "/overview", "1m"),
        ("page_view", "/go/real-pnl", ""),
        ("app_page_view", "/positions", ""),
    ]
    assert body.index("12:00p") < body.index("1:59p")
    assert "Oct 12, 12:00p PT" in body
    assert "Oct 12, 2:00p PT" in body
    assert "Oct 2, 2026" in body
    assert "zz-source · zz-campaign · zz-land" in body
    assert "zz-lt-source · zz-lt-campaign · zz-lt-land" in body
    assert "<strong>4</strong> pages viewed" in body
    assert "/bot-only-zz" not in body
    assert "/other-user-secret" not in body
    assert "/unrelated-visit-zz" not in body
    assert "Googlebot" not in body
    story = body.split('<div class="ua-top">', 1)[-1]
    assert "balance" not in story.lower()
    assert "P&amp;L" not in story and "P&L" not in story

    users = http.get("/admin/users")
    users_body = users.get_data(as_text=True)
    assert f"/admin/users/{person_id}/activity" in users_body
    assert f"/admin/users/{activity_world['private_id']}/activity" not in users_body


def test_private_activity_404s_and_list_failure_404s(activity_world, monkeypatch):
    http = activity_world["http"]
    _login(http, activity_world["operator"])
    hidden = http.get(f"/admin/users/{activity_world['private_id']}/activity")
    assert hidden.status_code == 404
    assert activity_world["private"] not in hidden.get_data(as_text=True)

    monkeypatch.setattr(
        "app.admin_privacy.hidden_snapshot",
        lambda: {"available": False, "user_ids": frozenset(), "tenant_ids": frozenset()},
    )
    closed = http.get(f"/admin/users/{activity_world['person_id']}/activity")
    assert closed.status_code == 404


def test_non_admin_cannot_open_activity(activity_world):
    http = activity_world["http"]
    anon = http.get(f"/admin/users/{activity_world['person_id']}/activity")
    assert anon.status_code in (302, 303)
    assert "/login" in (anon.headers.get("Location") or "")

    _login(http, activity_world["stranger"])
    denied = http.get(f"/admin/users/{activity_world['person_id']}/activity")
    assert denied.status_code == 404
    _logout(http)


def test_user_created_index_exists(app, db_conn):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_indexes WHERE indexname = 'idx_funnel_events_user_created'"
        )
        row = cur.fetchone()
    assert row
