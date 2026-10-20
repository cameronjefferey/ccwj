"""Privacy mode masks identity and account balances at the data layer.

Trade-level money formatting is a different function and stays numeric.
The mask is an admin recording tool. A stored flag does not mask a
non-admin, and a non-admin cannot turn it on.
"""

import os
import uuid

import pytest

from app.money import fmt_money
from app.privacy import (
    PRIVACY_MASK,
    format_privacy_balance,
    format_privacy_signed,
    mask_account_label,
    mask_secret,
    shown_account,
    slots_from_rows,
    sort_masked_account_choices,
)


def test_slots_are_stable_by_tenant_id():
    rows = [
        {"tenant_id": "snaptrade:bbb", "display_nickname": "Sara", "account_name": "Schwab Account"},
        {"tenant_id": "snaptrade:aaa", "display_nickname": "Roth", "account_name": "Schwab Account"},
    ]
    by_tid, by_name = slots_from_rows(rows)
    assert by_tid == {"snaptrade:aaa": "Account 1", "snaptrade:bbb": "Account 2"}
    # Colliding broker labels keep the first slot. tenant_id still disambiguates.
    assert by_name["Roth"] == "Account 1"
    assert by_name["Sara"] == "Account 2"
    assert by_name["Schwab Account"] == "Account 1"


def test_privacy_mask_keeps_the_word_paper(monkeypatch):
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: True)
    monkeypatch.setattr(
        "app.privacy.viewer_slots",
        lambda: (
            {"snaptrade:real": "Account 1", "snaptrade:paper": "Account 5"},
            {"Schwab Account": "Account 1", "Paper": "Account 5", "Testing": "Account 5"},
        ),
    )
    monkeypatch.setattr("app.privacy.viewer_paper_tenant_ids", lambda: {"snaptrade:paper"})
    assert shown_account("Schwab Account", "snaptrade:real") == "Account 1"
    assert shown_account("Paper", "snaptrade:paper") == "Account 5 · Paper"
    assert shown_account("Testing · Paper", "snaptrade:paper") == "Account 5 · Paper"
    # Nickname without the word still picks it up from the tenant.
    assert shown_account("Testing", "snaptrade:paper") == "Account 5 · Paper"
    assert shown_account("Paper", "snaptrade:paper") != "Account 5"


def test_mask_is_a_noop_when_privacy_is_off():
    by_tid = {"snaptrade:aaa": "Account 1"}
    by_name = {"Roth": "Account 1"}
    assert mask_account_label("Roth", "snaptrade:aaa", enabled=False, by_tid=by_tid, by_name=by_name) is None
    assert shown_account("Roth", "snaptrade:aaa") == "Roth"
    assert mask_secret("ada@example.com", enabled=False) == "ada@example.com"
    assert format_privacy_balance(19999, enabled=False) == "$19,999"
    assert format_privacy_signed(-250, enabled=False) == "-$250"


def test_account_numbers_and_balances_mask_when_on():
    by_tid, by_name = slots_from_rows([
        {"tenant_id": "snaptrade:aaa", "display_nickname": "Roth", "account_name": "Schwab Account"},
        {"tenant_id": "snaptrade:bbb", "display_nickname": "Sara", "account_name": "Schwab Account"},
    ])
    assert mask_account_label("Schwab Account", "snaptrade:bbb", enabled=True, by_tid=by_tid, by_name=by_name) == "Account 2"
    assert mask_account_label("Roth", None, enabled=True, by_tid=by_tid, by_name=by_name) == "Account 1"
    assert mask_secret("ada@example.com", enabled=True) == PRIVACY_MASK
    assert mask_secret("Schwab", enabled=True) == PRIVACY_MASK
    assert mask_secret("••••1234", enabled=True) == PRIVACY_MASK
    assert mask_secret("", enabled=True) == ""
    assert format_privacy_balance(482331, enabled=True) == PRIVACY_MASK
    assert format_privacy_balance(None, enabled=True) == "—"
    assert format_privacy_signed(1200, enabled=True) == PRIVACY_MASK


def test_masked_lists_sort_by_account_number_not_nickname():
    choices = [
        {"tenant_id": "snaptrade:m", "label": "Zulu"},
        {"tenant_id": "snaptrade:z", "label": "Alpha"},
        {"tenant_id": "snaptrade:a", "label": "Mid"},
    ]
    by_tid, by_name = slots_from_rows([
        {"tenant_id": "snaptrade:z", "display_nickname": "Alpha"},
        {"tenant_id": "snaptrade:a", "display_nickname": "Mid"},
        {"tenant_id": "snaptrade:m", "display_nickname": "Zulu"},
        {"tenant_id": "snaptrade:ten", "display_nickname": "Ten"},
    ])
    # tenant_id order: a, m, ten, z → Account 1, 2, 3, 4
    assert by_tid["snaptrade:a"] == "Account 1"
    assert by_tid["snaptrade:ten"] == "Account 3"
    ordered = sort_masked_account_choices(
        choices + [{"tenant_id": "snaptrade:ten", "label": "Ten"}],
        enabled=True,
        by_tid=by_tid,
        by_name=by_name,
    )
    assert [c["tenant_id"] for c in ordered] == [
        "snaptrade:a",
        "snaptrade:m",
        "snaptrade:ten",
        "snaptrade:z",
    ]
    # Privacy off keeps the nickname order the picker already built.
    assert sort_masked_account_choices(choices, enabled=False) == choices


def test_trade_money_formatter_is_not_masked():
    assert fmt_money(-3333, decimals=0) != PRIVACY_MASK
    assert "3,333" in fmt_money(-3333, decimals=0)


def test_jinja_account_label_uses_privacy_slots(monkeypatch):
    import os
    os.environ.setdefault("HAPPYTRADER_SKIP_DB_INIT", "1")
    from app import app

    by_tid = {"snaptrade:aaa": "Account 1"}
    by_name = {"Roth IRA": "Account 1"}
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: True)
    monkeypatch.setattr("app.privacy.viewer_slots", lambda: (by_tid, by_name))
    with app.test_request_context("/positions"):
        label = app.jinja_env.filters["account_label"]
        hide = app.jinja_env.filters["privacy_hide"]
        balance = app.jinja_env.filters["privacy_balance"]
        assert label("Roth IRA", "snaptrade:aaa") == "Account 1"
        assert hide("trader@example.com") == PRIVACY_MASK
        assert balance(250000) == PRIVACY_MASK
        rendered = app.jinja_env.from_string(
            "{{ name | account_label(tid) }}|{{ 88000 | privacy_balance }}|{{ money(pnl, 0) }}"
        ).render(name="Roth IRA", tid="snaptrade:aaa", pnl=-3333.2)
    assert rendered.startswith("Account 1|••••|")
    assert "3,333" in rendered
    assert "Roth" not in rendered


def test_story_review_masks_account_labels(monkeypatch):
    """Position review chip, card line, and chart tooltip share one rewrite."""
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: True)
    monkeypatch.setattr(
        "app.privacy.shown_account",
        lambda name, tenant_id=None: "Account 1" if tenant_id == "snaptrade:aaa" else "Account 2",
    )
    from app.position_story import _mask_story_account_labels

    items = [{
        "type": "day",
        "headlines": ["Sold the $50 call — Roth IRA."],
        "option_cards": [{"account": "Roth IRA", "title": "Sold", "lane": "options"}],
        "share_cards": [{"account": "Roth IRA", "title": "Trimmed", "lane": "shares"}],
    }]
    markers = [{
        "d": "2026-01-02",
        "k": "sell",
        "t": ["Sold the $50 call — Roth IRA."],
        "tips": [{
            "label": "Sold 1 × $50 call · Jan 16",
            "amt": "+$120",
            "sign": 1,
            "kind": "sell",
            "acct": "Roth IRA",
        }],
    }]
    stats = {"accounts": ["Roth IRA"]}
    _mask_story_account_labels(
        items, markers, stats, {"Roth IRA": "snaptrade:aaa"},
    )
    assert stats["accounts"] == ["Account 1"]
    assert items[0]["option_cards"][0]["account"] == "Account 1"
    assert items[0]["share_cards"][0]["account"] == "Account 1"
    assert "Roth" not in items[0]["headlines"][0]
    assert "Account 1" in markers[0]["t"][0]
    assert "Roth" not in markers[0]["t"][0]
    assert markers[0]["tips"][0]["acct"] == "Account 1"
    assert "Roth" not in markers[0]["tips"][0]["label"]


def test_story_review_keeps_names_when_privacy_is_off(monkeypatch):
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: False)
    from app.position_story import _mask_story_account_labels

    items = [{
        "headlines": ["Sold the $50 call — Roth IRA."],
        "option_cards": [{"account": "Roth IRA"}],
        "share_cards": [],
    }]
    markers = [{
        "t": ["Sold the $50 call — Roth IRA."],
        "tips": [{"label": "Sold 1 × $50 call", "acct": "Roth IRA"}],
    }]
    stats = {"accounts": ["Roth IRA"]}
    _mask_story_account_labels(
        items, markers, stats, {"Roth IRA": "snaptrade:aaa"},
    )
    assert stats["accounts"] == ["Roth IRA"]
    assert items[0]["option_cards"][0]["account"] == "Roth IRA"
    assert "Roth IRA" in markers[0]["t"][0]
    assert markers[0]["tips"][0]["acct"] == "Roth IRA"


def test_screen_mask_control_is_admin_menu_only():
    """End users have no toggle. Admins get it inside the Admin menu.

    Private-from-admins stays its own form and does not post the screen mask.
    """
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    profile = (root / "app/templates/profile.html").read_text()
    admin = (root / "app/templates/_admin_privacy_settings.html").read_text()
    nav = (root / "app/templates/base.html").read_text()
    assert "set_privacy_mode_route" not in profile
    assert 'id="settings-privacy"' not in profile
    assert "Privacy mode" not in profile
    account = nav.split('id="userMenu"', 1)[1].split("</ul>", 1)[0]
    assert "Hide account names on screen" not in account
    assert "set_privacy_mode_route" not in account
    admin_menu = nav.split('id="navAdmin"', 1)[1].split("</ul>", 1)[0]
    assert "Hide account names on screen" in admin_menu
    assert "set_privacy_mode_route" in admin_menu
    assert "{% if is_admin_user %}" in nav.split('id="navAdmin"', 1)[0][-500:]
    assert 'name="action" value="set_admin_privacy"' in admin
    assert "set_privacy_mode" not in admin
    assert "does not change what you see" in admin
    assert "Privacy mode" not in nav
    templates = root / "app/templates"
    for path in templates.rglob("*.html"):
        text = path.read_text()
        if "set_privacy_mode_route" in text:
            assert path.name == "base.html"
        if "Hide account names on screen masks" in text or "can mask them when you turn it on" in text:
            raise AssertionError(path.name)


class _Viewer:
    is_authenticated = True

    def __init__(self, username, user_id):
        self.username = username
        self.id = user_id


def test_stored_flag_does_not_mask_a_non_admin(monkeypatch):
    monkeypatch.setenv("ADMIN_USERS", "zzoperator")
    monkeypatch.setattr("flask_login.current_user", _Viewer("zztrader", 4))
    monkeypatch.setattr("app._viewer_profile", lambda: {"privacy_mode": True})
    from app import app
    from app.privacy import privacy_mode_on

    with app.test_request_context("/profile"):
        assert privacy_mode_on() is False


def test_stored_flag_masks_an_admin(monkeypatch):
    monkeypatch.setenv("ADMIN_USERS", "zzoperator")
    monkeypatch.setattr("flask_login.current_user", _Viewer("zzoperator", 9))
    monkeypatch.setattr("app._viewer_profile", lambda: {"privacy_mode": True})
    from app import app
    from app.privacy import privacy_mode_on

    with app.test_request_context("/profile"):
        assert privacy_mode_on() is True


def test_admin_with_flag_off_is_not_masked(monkeypatch):
    monkeypatch.setenv("ADMIN_USERS", "zzoperator")
    monkeypatch.setattr("flask_login.current_user", _Viewer("zzoperator", 9))
    monkeypatch.setattr("app._viewer_profile", lambda: {"privacy_mode": False})
    from app import app
    from app.privacy import privacy_mode_on

    with app.test_request_context("/profile"):
        assert privacy_mode_on() is False


def test_demo_login_is_never_masked(monkeypatch):
    monkeypatch.setenv("ADMIN_USERS", "demo,zzoperator")
    monkeypatch.setattr("flask_login.current_user", _Viewer("demo", 1))
    monkeypatch.setattr("app._viewer_profile", lambda: {"privacy_mode": True})
    from app import app
    from app.privacy import privacy_mode_on

    with app.test_request_context("/overview"):
        assert privacy_mode_on() is False


def test_set_privacy_mode_refuses_a_non_admin(monkeypatch):
    calls = []
    monkeypatch.setenv("ADMIN_USERS", "zzoperator")
    monkeypatch.setattr("flask_login.current_user", _Viewer("zztrader", 4))

    def _write(*args, **kwargs):
        calls.append((args, kwargs))
        return True

    monkeypatch.setattr("app.models.update_user_profile", _write)
    from app import app
    from app.privacy import set_privacy_mode

    with app.test_request_context("/privacy-mode", method="POST"):
        assert set_privacy_mode(True) is False
    assert calls == []


def test_set_privacy_mode_writes_for_an_admin(monkeypatch):
    calls = []
    monkeypatch.setenv("ADMIN_USERS", "zzoperator")
    monkeypatch.setattr("flask_login.current_user", _Viewer("zzoperator", 9))

    def _write(*args, **kwargs):
        calls.append((args, kwargs))
        return True

    monkeypatch.setattr("app.models.update_user_profile", _write)
    from app import app
    from app.privacy import privacy_mode_on, set_privacy_mode

    with app.test_request_context("/privacy-mode", method="POST"):
        assert set_privacy_mode(True) is True
        assert privacy_mode_on() is True
    assert calls == [((9,), {"privacy_mode": True})]


def test_profile_update_strips_screen_mask_unless_admin_updates_self(monkeypatch):
    sqls = []
    monkeypatch.setenv("ADMIN_USERS", "zzoperator")
    monkeypatch.setattr("app.models.ensure_user_profile", lambda user_id: None)
    monkeypatch.setattr("app.models._forget_shell", lambda user_id: None)
    monkeypatch.setattr("app.models.execute", lambda sql, params=(): sqls.append(sql))
    from app import app
    from app.models import update_user_profile

    with app.test_request_context("/profile"):
        monkeypatch.setattr("flask_login.current_user", _Viewer("zztrader", 4))
        assert update_user_profile(4, display_name="Ada", privacy_mode=True) is True
        monkeypatch.setattr("flask_login.current_user", _Viewer("zzoperator", 9))
        assert update_user_profile(9, privacy_mode=True) is True
        assert update_user_profile(4, privacy_mode=True) is True
    assert "privacy_mode" not in sqls[0]
    assert "display_name" in sqls[0]
    assert "privacy_mode" in sqls[1]
    assert len(sqls) == 2


def _unique(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _create_user(conn, username):
    from werkzeug.security import generate_password_hash

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (username, password_hash, email) "
            "VALUES (%s, %s, %s) RETURNING id",
            (username, generate_password_hash("testpass123"), f"{username}@example.com"),
        )
        user_id = int(cur.fetchone()["id"])
    conn.commit()
    return user_id


def _plant_account(conn, user_id, nickname):
    from app.models import get_or_create_broker_tenant

    account_name = "ZZBROKER-ACCT-7712"
    tenant_id = get_or_create_broker_tenant(
        user_id, "snaptrade", uuid.uuid4().hex, account_name,
    )
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE broker_tenants SET display_nickname = %s "
            "WHERE user_id = %s AND tenant_id = %s",
            (nickname, user_id, tenant_id),
        )
        cur.execute(
            """
            INSERT INTO snaptrade_accounts
                (user_id, snaptrade_account_id, tenant_id, broker_slug,
                 account_name, display_nickname)
            VALUES (%s, %s, %s, 'schwab', %s, %s)
            """,
            (user_id, uuid.uuid4().hex, tenant_id, account_name, nickname),
        )
    conn.commit()
    return tenant_id


def _plant_privacy_flag(user_id, enabled):
    from app.db import execute
    from app.models import _forget_shell, ensure_user_profile

    ensure_user_profile(user_id)
    execute(
        "UPDATE user_profiles SET privacy_mode = %s WHERE user_id = %s",
        (bool(enabled), user_id),
    )
    _forget_shell(user_id)


def _stored_flag(user_id):
    from app.db import fetch_one

    row = fetch_one(
        "SELECT privacy_mode FROM user_profiles WHERE user_id = %s",
        (user_id,),
    )
    return bool(row and row["privacy_mode"])


def _login(http, username):
    resp = http.post(
        "/login",
        data={"username": username, "password": "testpass123"},
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303)


def _account_menu(html):
    start = html.find('id="userMenu"')
    assert start != -1
    return html[start:html.find("</ul>", start)]


def _admin_menu(html):
    start = html.find('id="navAdmin"')
    if start == -1:
        return ""
    return html[start:html.find("</ul>", start)]


@pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL not set; skipping DB-dependent tests",
)
def test_non_admin_sees_real_names_and_cannot_enable_the_mask(app, db_conn, monkeypatch):
    """A stored true flag does not mask, and POST /privacy-mode is rejected."""
    username = _unique("mask_user")
    nickname = "ZZMASK-NICK-7712"
    monkeypatch.setenv("ADMIN_USERS", "zzoperator_unused")
    user_id = _create_user(db_conn, username)
    try:
        _plant_account(db_conn, user_id, nickname)
        _plant_privacy_flag(user_id, True)
        assert _stored_flag(user_id) is True

        http = app.test_client()
        _login(http, username)
        account = http.get("/profile?tab=account")
        body = account.get_data(as_text=True)
        assert account.status_code == 200
        assert nickname in body
        assert "Account 1" not in body
        assert "Hide account names on screen" not in body
        assert 'action="/privacy-mode"' not in body
        menu = _account_menu(body)
        assert "Hide account names on screen" not in menu
        assert _admin_menu(body) == ""

        prefs = http.get("/profile?tab=preferences").get_data(as_text=True)
        assert "Hide account names on screen" not in prefs
        assert 'id="settings-privacy"' not in prefs
        assert 'action="/privacy-mode"' not in prefs

        refused = http.post(
            "/privacy-mode",
            data={"enabled": "1", "next": "/profile?tab=account"},
            follow_redirects=False,
        )
        assert refused.status_code == 403
        assert _stored_flag(user_id) is True
        again = http.get("/profile?tab=account").get_data(as_text=True)
        assert nickname in again
        assert "Account 1" not in again
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        db_conn.commit()


@pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL not set; skipping DB-dependent tests",
)
def test_admin_can_toggle_the_screen_mask(app, db_conn, monkeypatch):
    username = _unique("mask_admin")
    nickname = "ZZMASK-ADMIN-7712"
    monkeypatch.setenv("ADMIN_USERS", username)
    user_id = _create_user(db_conn, username)
    try:
        _plant_account(db_conn, user_id, nickname)
        _plant_privacy_flag(user_id, False)

        http = app.test_client()
        _login(http, username)
        before = http.get("/profile?tab=account").get_data(as_text=True)
        assert nickname in before
        assert "Account 1" not in before
        assert "Hide account names on screen" not in _account_menu(before)
        admin_menu = _admin_menu(before)
        assert "Hide account names on screen" in admin_menu
        assert 'action="/privacy-mode"' in admin_menu
        prefs = http.get("/profile?tab=preferences").get_data(as_text=True)
        assert 'id="settings-privacy"' not in prefs

        turned_on = http.post(
            "/privacy-mode",
            data={"enabled": "1", "next": "/profile?tab=account"},
            follow_redirects=True,
        )
        assert turned_on.status_code == 200
        assert _stored_flag(user_id) is True
        masked = turned_on.get_data(as_text=True)
        assert nickname not in masked
        assert "Account 1" in masked
        assert "Hide account names on screen is on" in _admin_menu(masked)

        turned_off = http.post(
            "/privacy-mode",
            data={"enabled": "0", "next": "/profile?tab=account"},
            follow_redirects=True,
        )
        assert turned_off.status_code == 200
        assert _stored_flag(user_id) is False
        clear = turned_off.get_data(as_text=True)
        assert nickname in clear
        assert "Account 1" not in clear
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        db_conn.commit()


@pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL not set; skipping DB-dependent tests",
)
def test_startup_clears_screen_mask_for_non_admins(db_conn, monkeypatch):
    admin_name = _unique("mask_keep")
    other_name = _unique("mask_clear")
    monkeypatch.setenv("ADMIN_USERS", admin_name)
    admin_id = _create_user(db_conn, admin_name)
    other_id = _create_user(db_conn, other_name)
    try:
        _plant_privacy_flag(admin_id, True)
        _plant_privacy_flag(other_id, True)
        from app.models import _clear_non_admin_screen_mask

        _clear_non_admin_screen_mask()
        assert _stored_flag(admin_id) is True
        assert _stored_flag(other_id) is False
    finally:
        with db_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM users WHERE id = ANY(%s)",
                ([admin_id, other_id],),
            )
        db_conn.commit()
