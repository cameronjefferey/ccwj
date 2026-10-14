"""Private-from-admin: admins cannot see a listed user's book.

The gate lives in the shared tenant helpers (``tenant_sql_and(None)`` and
``filter_df_by_tenant_ids(..., None)``) plus ``_tenants_for_scope`` for an
explicit URL. These tests do not read BigQuery. A hidden ``?tenant=``
must 404 before a warehouse client is built.
"""
import os
import uuid

import pandas as pd
import pytest
from werkzeug.exceptions import NotFound

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL not set; skipping DB-dependent tests",
)

SECRET_ACCOUNT = "ZZSECRET-ACCT-8841"
SECRET_SYMBOL = "ZZQX"
SECRET_MONEY = "$4,821.50"
SHARED_LABEL = "ZZSHARED-LABEL-8841"


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _create_user(conn, username: str, email: str | None = None) -> int:
    from werkzeug.security import generate_password_hash

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (username, password_hash, email) "
            "VALUES (%s, %s, %s) RETURNING id",
            (
                username,
                generate_password_hash("testpass123"),
                email or f"{username}@example.com",
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return int(row["id"])


def _link_tenant(user_id: int, account_name: str) -> str:
    from app.models import get_or_create_broker_tenant

    return get_or_create_broker_tenant(
        user_id, "snaptrade", uuid.uuid4().hex, account_name,
    )


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
def world(app, db_conn, monkeypatch):
    """Operator admin, a hidden friend, a visible sibling, an exempt admin."""
    operator = _unique("priv_op")
    friend = _unique("priv_friend")
    visible = _unique("priv_vis")
    exempt = _unique("priv_exempt")
    friend_email = f"{friend}@example.com"
    monkeypatch.setenv("ADMIN_USERS", f"{operator},{exempt}")

    operator_id = _create_user(db_conn, operator)
    friend_id = _create_user(db_conn, friend, friend_email)
    visible_id = _create_user(db_conn, visible)
    exempt_id = _create_user(db_conn, exempt)

    friend_tid = _link_tenant(friend_id, SECRET_ACCOUNT)
    visible_tid = _link_tenant(visible_id, SHARED_LABEL)
    _link_tenant(friend_id, SHARED_LABEL)
    exempt_tid = _link_tenant(exempt_id, "ZZEXEMPT-ADMIN-ACCT")

    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO user_accounts (user_id, account_name) VALUES (%s, %s)",
            (friend_id, SECRET_ACCOUNT),
        )
        cur.execute(
            """
            INSERT INTO snaptrade_accounts
                (user_id, snaptrade_account_id, tenant_id, broker_slug,
                 account_name, display_nickname, account_number_masked)
            VALUES (%s, %s, %s, 'schwab', %s, %s, '••••8841')
            """,
            (friend_id, uuid.uuid4().hex, friend_tid, SECRET_ACCOUNT, SECRET_ACCOUNT),
        )
        cur.execute(
            """
            INSERT INTO feedback (user_id, username, body, page_path)
            VALUES (%s, %s, 'the chart looks cramped', %s)
            RETURNING id
            """,
            (friend_id, friend, f"/position/{SECRET_SYMBOL}"),
        )
        feedback_id = cur.fetchone()["id"]
        cur.execute(
            """
            INSERT INTO usage_events (user_id, endpoint, path, status_code)
            VALUES (%s, 'position_detail', %s, 200)
            RETURNING id
            """,
            (friend_id, f"/position/{SECRET_SYMBOL}"),
        )
        usage_id = cur.fetchone()["id"]
    db_conn.commit()

    from app.admin_privacy import add_hidden_user

    ok, info = add_hidden_user(friend_id, operator_id)
    assert ok, info
    # An admin's own book stays visible even if a row is forced in.
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO admin_hidden_users (user_id, added_by) VALUES (%s, %s) "
            "ON CONFLICT (user_id) DO NOTHING",
            (exempt_id, operator_id),
        )
    db_conn.commit()

    http = app.test_client()
    state = {
        "app": app,
        "http": http,
        "operator": operator,
        "operator_id": operator_id,
        "friend": friend,
        "friend_id": friend_id,
        "friend_email": friend_email,
        "friend_tid": friend_tid,
        "visible": visible,
        "visible_id": visible_id,
        "visible_tid": visible_tid,
        "exempt": exempt,
        "exempt_id": exempt_id,
        "exempt_tid": exempt_tid,
        "feedback_id": feedback_id,
        "usage_id": usage_id,
        "user_ids": [operator_id, friend_id, visible_id, exempt_id],
    }
    try:
        yield state
    finally:
        _logout(http)
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM feedback WHERE id = %s", (feedback_id,))
            cur.execute("DELETE FROM usage_events WHERE id = %s", (usage_id,))
            cur.execute(
                "DELETE FROM users WHERE id = ANY(%s)",
                (state["user_ids"],),
            )
        db_conn.commit()


def _as(app, user_id):
    from flask_login import login_user
    from app.models import User

    user = User.get_by_id(user_id)
    login_user(user)
    return user


AUDITED_PATHS = [
    "/positions",
    "/overview",
    "/weekly-review",
    "/today",
    "/strategies",
    "/strategies?view=fit",
    "/sectors",
    "/accounts",
    "/accounts?view=value",
    "/position/" + SECRET_SYMBOL,
    "/api/position/" + SECRET_SYMBOL + "/peek",
    "/api/nav/symbols",
    "/insights",
    "/story",
    "/earnings",
    "/daily-review/day/2020-01-02",
    "/overview/below",
]


def _with_tenant(path: str, tenant_id: str) -> str:
    join = "&" if "?" in path else "?"
    return f"{path}{join}tenant={tenant_id}"


def test_shared_sql_and_frame_drop_hidden_tenant(world):
    from app.tenant_scope import filter_df_by_tenant_ids, tenant_sql_and

    app = world["app"]
    frame = pd.DataFrame([
        {
            "tenant_id": world["friend_tid"],
            "account": SECRET_ACCOUNT,
            "symbol": SECRET_SYMBOL,
            "total_pnl": 4821.50,
        },
        {
            "tenant_id": world["visible_tid"],
            "account": SHARED_LABEL,
            "symbol": "SPY",
            "total_pnl": 10.0,
        },
        {
            "tenant_id": world["exempt_tid"],
            "account": "ZZEXEMPT-ADMIN-ACCT",
            "symbol": "QQQ",
            "total_pnl": 3.0,
        },
    ])
    with app.test_request_context("/positions"):
        _as(app, world["operator_id"])
        sql = tenant_sql_and(None)
        assert "NOT IN" in sql
        assert world["friend_tid"] in sql
        assert world["exempt_tid"] not in sql
        kept = filter_df_by_tenant_ids(frame, None)
    assert world["friend_tid"] not in set(kept["tenant_id"])
    assert SECRET_ACCOUNT not in set(kept["account"])
    assert SECRET_SYMBOL not in set(kept["symbol"])
    assert world["visible_tid"] in set(kept["tenant_id"])
    assert world["exempt_tid"] in set(kept["tenant_id"])


def test_friends_own_scope_is_unchanged(world):
    from app.routes import _tenants_for_scope
    from app.tenant_scope import filter_df_by_tenant_ids, tenant_sql_and

    app = world["app"]
    frame = pd.DataFrame([
        {
            "tenant_id": world["friend_tid"],
            "account": SECRET_ACCOUNT,
            "symbol": SECRET_SYMBOL,
            "total_pnl": 4821.50,
        },
    ])
    with app.test_request_context("/positions"):
        _as(app, world["friend_id"])
        scope = _tenants_for_scope("")
        assert world["friend_tid"] in scope
        sql = tenant_sql_and(scope)
        assert world["friend_tid"] in sql
        assert "NOT IN" not in sql
        kept = filter_df_by_tenant_ids(frame, scope)
    assert list(kept["account"]) == [SECRET_ACCOUNT]
    assert list(kept["symbol"]) == [SECRET_SYMBOL]


def test_direct_tenant_url_is_blocked(world):
    from app.routes import _tenants_for_scope

    app = world["app"]
    http = world["http"]
    _login(http, world["operator"])
    tid = world["friend_tid"]

    with app.test_request_context(f"/positions?tenant={tid}"):
        _as(app, world["operator_id"])
        with pytest.raises(NotFound):
            _tenants_for_scope("")
    with app.test_request_context(f"/positions?tenants={tid},{world['visible_tid']}"):
        _as(app, world["operator_id"])
        with pytest.raises(NotFound):
            _tenants_for_scope("")
    with app.test_request_context("/positions"):
        _as(app, world["operator_id"])
        with pytest.raises(NotFound):
            _tenants_for_scope(SECRET_ACCOUNT)
        kept = _tenants_for_scope(SHARED_LABEL)
        assert world["visible_tid"] in kept
        assert world["friend_tid"] not in kept

    for path in AUDITED_PATHS:
        resp = http.get(_with_tenant(path, tid), follow_redirects=False)
        assert resp.status_code == 404, path
        body = resp.get_data(as_text=True)
        assert SECRET_ACCOUNT not in body
        assert SECRET_MONEY not in body
        assert SECRET_SYMBOL not in body or path.endswith(SECRET_SYMBOL) or "/position/" in path
        # The symbol may appear in the request path echo of a generic 404.
        # It must not appear as account data. The secret account name is the
        # check that matters for those URLs.
        assert "total_pnl" not in body


def test_admin_cannot_remove_only_the_user_can(world):
    from app.admin_privacy import (
        is_hidden_from_admins,
        remove_hidden_user,
    )

    app = world["app"]
    http = world["http"]
    assert remove_hidden_user(world["friend_id"], world["operator_id"]) is False
    assert is_hidden_from_admins(world["friend_id"])

    with app.test_request_context("/profile"):
        from flask import session
        session["_impersonator_id"] = world["operator_id"]
        assert remove_hidden_user(world["friend_id"], world["friend_id"]) is False
    assert is_hidden_from_admins(world["friend_id"])

    _login(http, world["operator"])
    resp = http.post(
        "/admin/private",
        data={"username": world["friend"], "remove": "1", "enabled": "0"},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert is_hidden_from_admins(world["friend_id"])
    turned = http.post(
        "/profile",
        data={"action": "set_admin_privacy", "enabled": "0"},
        follow_redirects=True,
    )
    assert turned.status_code == 200
    assert is_hidden_from_admins(world["friend_id"])
    _logout(http)

    _login(http, world["friend"])
    off = http.post(
        "/profile",
        data={"action": "set_admin_privacy", "enabled": "0"},
        follow_redirects=True,
    )
    assert off.status_code == 200
    assert not is_hidden_from_admins(world["friend_id"])
    on = http.post(
        "/profile",
        data={"action": "set_admin_privacy", "enabled": "1"},
        follow_redirects=True,
    )
    assert on.status_code == 200
    assert is_hidden_from_admins(world["friend_id"])
    _logout(http)


def test_admin_add_page_shows_username_and_no_money(world):
    from app.admin_privacy import format_privacy_since, hidden_record

    http = world["http"]
    _login(http, world["operator"])
    page = http.get("/admin/private")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    record = hidden_record(world["friend_id"])
    assert f"@{world['friend']}" in body
    assert format_privacy_since(record["added_at"]) in body
    assert SECRET_ACCOUNT not in body
    assert SECRET_SYMBOL not in body
    assert SECRET_MONEY not in body
    assert "••••8841" not in body
    assert ">Remove<" not in body
    assert 'name="remove"' not in body
    assert "does not show accounts, balances, or trades" in body
    assert "Invited, currently off" not in body

    added = http.post(
        "/admin/private",
        data={"username": world["friend_email"]},
        follow_redirects=True,
    )
    assert added.status_code == 200
    assert SECRET_ACCOUNT not in added.get_data(as_text=True)

    users = http.get("/admin/users")
    users_body = users.get_data(as_text=True)
    assert world["friend"] in users_body
    assert SECRET_ACCOUNT not in users_body
    assert "Private" in users_body

    audit = http.get(
        f"/admin/audit?account={SECRET_ACCOUNT}&symbol={SECRET_SYMBOL}",
    )
    assert audit.status_code == 404

    feedback = http.get("/admin/feedback")
    fb = feedback.get_data(as_text=True)
    assert f"/position/{SECRET_SYMBOL}" not in fb
    assert "the chart looks cramped" in fb

    overview = http.get("/admin")
    assert f"/position/{SECRET_SYMBOL}" not in overview.get_data(as_text=True)

    impersonate = http.get(f"/admin/impersonate/{world['friend']}")
    assert impersonate.status_code == 404
    preview = http.get(
        f"/admin/digest-preview?user={world['friend']}&week=2026-10-05",
    )
    assert preview.status_code == 404
    assert SECRET_ACCOUNT not in preview.get_data(as_text=True)

    stranger = world["http"]
    _logout(stranger)
    _login(stranger, world["visible"])
    denied = stranger.get("/admin/private")
    assert denied.status_code == 404
    _logout(stranger)


def test_friend_settings_proof_and_own_account_still_shown(world):
    from app.admin_privacy import format_privacy_since, hidden_record

    http = world["http"]
    _login(http, world["friend"])
    page = http.get("/profile")
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    since = format_privacy_since(hidden_record(world["friend_id"])["added_at"])
    assert "Private from HappyTrader admins: On" in body
    assert f"since {since}" in body
    assert "Admins can't see your accounts, balances or trades." in body
    assert "This is a block inside the app. It is not encryption." in body
    assert "Turn off" in body
    assert SECRET_ACCOUNT in body
    _logout(http)


def test_refuses_demo_and_admin_targets(world):
    from app.admin_privacy import add_hidden_user
    from app.models import User

    ok, msg = add_hidden_user(world["operator_id"], world["operator_id"])
    assert not ok
    assert "Admin" in msg
    ok, msg = add_hidden_user(world["exempt_id"], world["operator_id"])
    assert not ok

    demo = User.get_by_username("demo")
    created = False
    if demo is None:
        from werkzeug.security import generate_password_hash
        from app.db import execute_returning
        row = execute_returning(
            "INSERT INTO users (username, password_hash) VALUES ('demo', %s) "
            "RETURNING id",
            (generate_password_hash("testpass123"),),
        )
        demo_id = int(row["id"])
        created = True
    else:
        demo_id = int(demo.id)
    try:
        ok, msg = add_hidden_user(demo_id, world["operator_id"])
        assert not ok
        assert "demo" in msg.lower()
    finally:
        if created:
            from app.db import execute
            execute("DELETE FROM users WHERE id = %s", (demo_id,))


def test_audited_modules_share_the_tenant_predicate():
    import app.accounts_page as accounts_page
    import app.earnings_page as earnings_page
    import app.insights as insights
    import app.position_detail as position_detail
    import app.positions_page as positions_page
    import app.routes as routes
    import app.sectors_page as sectors_page
    import app.strategies as strategies
    import app.strategy_fit as strategy_fit
    import app.symbols_page as symbols_page
    import app.tenant_scope as tenant_scope
    import app.trader_story as trader_story
    import app.wealth as wealth
    import app.weekly_review as weekly_review

    assert routes._tenant_sql_and is tenant_scope.tenant_sql_and
    assert routes._filter_df_by_tenant_ids is tenant_scope.filter_df_by_tenant_ids
    for mod in (
        positions_page, sectors_page, accounts_page, strategies, weekly_review,
        insights, position_detail, wealth, trader_story, earnings_page,
        symbols_page, strategy_fit,
    ):
        assert mod._tenant_sql_and is tenant_scope.tenant_sql_and, mod.__name__


def test_ops_ping_drops_hidden_account_id(world, monkeypatch):
    from app.ops_notify import notify_event

    captured = {}

    def _capture(kind, text, username=None):
        captured["text"] = text
        return True

    monkeypatch.setattr("app.ops_notify.notify", _capture)
    notify_event(
        "broken",
        user_id=world["friend_id"],
        username=world["friend"],
        account_id=SECRET_ACCOUNT,
    )
    assert SECRET_ACCOUNT not in captured["text"]
    assert world["friend"] in captured["text"]


def test_unscoped_admin_read_fails_closed_when_hidden_list_errors(world, monkeypatch):
    """A Postgres error must not turn an admin read into every account."""
    from flask import get_flashed_messages

    from app.admin_privacy import PRIVACY_LIST_UNAVAILABLE, hidden_snapshot
    from app.demo_guard import DEMO_TENANT_ID
    from app.routes import _tenants_for_scope
    from app.tenant_scope import filter_df_by_tenant_ids, tenant_sql_and

    from app.db import execute

    operator_tid = _link_tenant(world["operator_id"], "ZZOPERATOR-OWN-8841")
    execute(
        "INSERT INTO user_accounts (user_id, account_name) VALUES (%s, %s)",
        (world["operator_id"], "ZZOPERATOR-OWN-8841"),
    )
    execute(
        "INSERT INTO user_accounts (user_id, account_name) VALUES (%s, %s)",
        (world["visible_id"], SHARED_LABEL),
    )

    def _boom():
        raise RuntimeError("postgres down")

    monkeypatch.setattr("app.admin_privacy._load_hidden_snapshot", _boom)
    app = world["app"]
    frame = pd.DataFrame([
        {"tenant_id": world["friend_tid"], "account": SECRET_ACCOUNT, "symbol": SECRET_SYMBOL},
        {"tenant_id": world["visible_tid"], "account": SHARED_LABEL, "symbol": "SPY"},
        {"tenant_id": operator_tid, "account": "ZZOPERATOR-OWN-8841", "symbol": "QQQ"},
        {"tenant_id": DEMO_TENANT_ID, "account": "Demo Account", "symbol": "DIA"},
    ])
    with app.test_request_context("/positions"):
        _as(app, world["operator_id"])
        snap = hidden_snapshot()
        assert snap["available"] is False
        # The failure stays fail-closed for the rest of this request.
        # It must not be remembered as "nobody is hidden".
        assert hidden_snapshot()["available"] is False
        sql = tenant_sql_and(None)
        assert sql != ""
        assert "NOT IN" not in sql
        assert world["friend_tid"] not in sql
        assert world["visible_tid"] not in sql
        assert operator_tid in sql
        assert DEMO_TENANT_ID in sql
        kept = filter_df_by_tenant_ids(frame, None)
        assert set(kept["tenant_id"]) == {operator_tid, DEMO_TENANT_ID}
        assert SECRET_ACCOUNT not in set(kept["account"])
        assert SECRET_SYMBOL not in set(kept["symbol"])
        missing = pd.DataFrame({"x": [1, 2]})
        assert filter_df_by_tenant_ids(missing, None).empty
        scope = _tenants_for_scope("")
        assert scope is not None
        assert operator_tid in scope
        assert DEMO_TENANT_ID in scope
        assert world["friend_tid"] not in scope
        assert world["visible_tid"] not in scope
        assert PRIVACY_LIST_UNAVAILABLE in get_flashed_messages()

        # A caller's own explicit tenant list does not consult the outage.
        friend_sql = tenant_sql_and([world["friend_tid"]])
        assert friend_sql == f"AND tenant_id IN ('{world['friend_tid']}')"
        friend_kept = filter_df_by_tenant_ids(frame, [world["friend_tid"]])
        assert list(friend_kept["tenant_id"]) == [world["friend_tid"]]

    with app.test_request_context(f"/positions?tenant={world['friend_tid']}"):
        _as(app, world["operator_id"])
        with pytest.raises(NotFound):
            _tenants_for_scope("")
        assert PRIVACY_LIST_UNAVAILABLE in get_flashed_messages()

    with app.test_request_context("/positions"):
        _as(app, world["friend_id"])
        own = _tenants_for_scope("")
        assert world["friend_tid"] in own
        own_sql = tenant_sql_and(own)
        assert world["friend_tid"] in own_sql
        assert "1 = 0" not in own_sql
        own_kept = filter_df_by_tenant_ids(frame, own)
        assert SECRET_ACCOUNT in set(own_kept["account"])

    http = world["http"]
    _login(http, world["operator"])
    users = http.get("/admin/users")
    body = users.get_data(as_text=True)
    assert users.status_code == 200
    assert PRIVACY_LIST_UNAVAILABLE in body
    assert SECRET_ACCOUNT not in body
    assert SHARED_LABEL not in body
    assert "ZZOPERATOR-OWN-8841" in body
    private = http.get("/admin/private")
    private_body = private.get_data(as_text=True)
    assert PRIVACY_LIST_UNAVAILABLE in private_body
    assert "No one is on this list." not in private_body
    assert SECRET_ACCOUNT not in private_body
    audit = http.get(f"/admin/audit?account={SECRET_ACCOUNT}&symbol={SECRET_SYMBOL}")
    assert audit.status_code == 404
    assert PRIVACY_LIST_UNAVAILABLE in audit.get_data(as_text=True)
    assert SECRET_MONEY not in audit.get_data(as_text=True)
    _logout(http)

    # The outage is not cached into the next request as an empty success.
    monkeypatch.undo()
    with app.test_request_context("/positions"):
        _as(app, world["operator_id"])
        recovered = hidden_snapshot()
        assert recovered["available"] is True
        assert world["friend_tid"] in recovered["tenant_ids"]
        recovered_sql = tenant_sql_and(None)
        assert world["friend_tid"] in recovered_sql
        assert "NOT IN" in recovered_sql


def test_add_updates_the_hidden_list_immediately(world, db_conn):
    """An add in this request is visible before the next request starts.

    The list is cached on flask.g only. Add clears that cache, so the
    next read in the same request includes the new tenant. The warehouse
    cache key is the SQL predicate, which changes with the new id.
    A later request loads from Postgres again; nothing process-wide
    keeps the previous set.
    """
    from app.admin_privacy import add_hidden_user, hidden_snapshot
    from app.tenant_scope import tenant_sql_and

    other = _unique("priv_fresh")
    other_id = _create_user(db_conn, other)
    world["user_ids"].append(other_id)
    other_tid = _link_tenant(other_id, "ZZFRESH-ACCT-8841")
    app = world["app"]
    with app.test_request_context("/positions"):
        _as(app, world["operator_id"])
        before = hidden_snapshot()
        assert before["available"] is True
        assert other_tid not in before["tenant_ids"]
        assert world["friend_tid"] in before["tenant_ids"]
        ok, _info = add_hidden_user(other_id, world["operator_id"])
        assert ok
        after = hidden_snapshot()
        assert other_tid in after["tenant_ids"]
        sql = tenant_sql_and(None)
        assert other_tid in sql
        assert "NOT IN" in sql
    with app.test_request_context("/positions"):
        _as(app, world["operator_id"])
        again = hidden_snapshot()
        assert again["available"] is True
        assert other_tid in again["tenant_ids"]
        assert other_tid in tenant_sql_and(None)


def test_non_invited_user_sees_no_card_and_cannot_enable(world):
    """A user an admin never added has no card and cannot hide themselves."""
    from app.admin_privacy import NOT_INVITED, hidden_record, is_hidden_from_admins

    http = world["http"]
    _login(http, world["visible"])
    for path in ("/profile", "/profile?tab=preferences"):
        page = http.get(path)
        assert page.status_code == 200
        body = page.get_data(as_text=True)
        assert "Private from HappyTrader admins" not in body
        assert "set_admin_privacy" not in body
        assert 'id="admin-privacy"' not in body
    posted = http.post(
        "/profile",
        data={"action": "set_admin_privacy", "enabled": "1"},
        follow_redirects=True,
    )
    body = posted.get_data(as_text=True)
    assert posted.status_code == 200
    assert NOT_INVITED in body
    assert "Private from HappyTrader admins is on" not in body
    assert "set_admin_privacy" not in body
    assert hidden_record(world["visible_id"]) is None
    assert not is_hidden_from_admins(world["visible_id"])
    ignored = http.post(
        "/profile",
        data={"action": "set_admin_privacy", "enabled": "0"},
        follow_redirects=True,
    )
    assert NOT_INVITED in ignored.get_data(as_text=True)
    assert hidden_record(world["visible_id"]) is None
    _logout(http)


def test_invited_user_keeps_the_card_after_turning_privacy_off(world):
    """On, then off, the card stays. They can turn it back on themselves."""
    from app.admin_privacy import hidden_record, is_hidden_from_admins

    http = world["http"]
    _login(http, world["friend"])
    on_page = http.get("/profile")
    on_body = on_page.get_data(as_text=True)
    assert "Private from HappyTrader admins: On" in on_body
    assert "Turn off" in on_body
    prefs = http.get("/profile?tab=preferences")
    assert "Private from HappyTrader admins: On" in prefs.get_data(as_text=True)

    off = http.post(
        "/profile",
        data={"action": "set_admin_privacy", "enabled": "0"},
        follow_redirects=True,
    )
    off_body = off.get_data(as_text=True)
    assert off.status_code == 200
    assert "Private from HappyTrader admins is off." in off_body
    assert "Private from HappyTrader admins: Off." in off_body
    assert "An admin added you to private accounts." in off_body
    assert 'id="admin-privacy"' in off_body
    assert "Turn on" in off_body
    assert not is_hidden_from_admins(world["friend_id"])
    record = hidden_record(world["friend_id"])
    assert record is not None
    assert not record["active"]
    assert record["opted_out_at"] is not None

    again = http.post(
        "/profile",
        data={"action": "set_admin_privacy", "enabled": "1"},
        follow_redirects=True,
    )
    again_body = again.get_data(as_text=True)
    assert "Private from HappyTrader admins: On" in again_body
    assert "Turn off" in again_body
    assert is_hidden_from_admins(world["friend_id"])
    restored = hidden_record(world["friend_id"])
    assert restored["active"]
    assert restored["opted_out_at"] is None
    _logout(http)


def test_filtering_applies_only_while_privacy_is_on(world):
    """An invite with privacy off is not dropped from admin reads."""
    from app.admin_privacy import (
        hidden_record,
        is_hidden_from_admins,
        list_hidden_users,
        list_opted_out_invites,
        redact_from_admin,
        remove_hidden_user,
    )
    from app.tenant_scope import tenant_sql_and

    app = world["app"]
    http = world["http"]
    assert remove_hidden_user(world["friend_id"], world["friend_id"])
    assert not is_hidden_from_admins(world["friend_id"])
    assert hidden_record(world["friend_id"]) is not None

    with app.test_request_context("/positions"):
        _as(app, world["operator_id"])
        sql = tenant_sql_and(None)
        assert world["friend_tid"] not in sql
        assert redact_from_admin(world["friend_id"], world["friend"]) is False

    active_names = {row["username"] for row in list_hidden_users()}
    off_names = {row["username"] for row in list_opted_out_invites()}
    assert world["friend"] not in active_names
    assert world["friend"] in off_names

    _login(http, world["operator"])
    users = http.get("/admin/users")
    assert SECRET_ACCOUNT in users.get_data(as_text=True)
    private = http.get("/admin/private")
    private_body = private.get_data(as_text=True)
    assert "Invited, currently off" in private_body
    assert f"@{world['friend']}" in private_body
    assert SECRET_ACCOUNT not in private_body
    assert SECRET_SYMBOL not in private_body
    assert SECRET_MONEY not in private_body
    assert "••••8841" not in private_body

    # Adding them again turns privacy back on. The "since" date refreshes.
    before = hidden_record(world["friend_id"])["added_at"]
    added = http.post(
        "/admin/private",
        data={"username": world["friend"]},
        follow_redirects=True,
    )
    assert added.status_code == 200
    assert is_hidden_from_admins(world["friend_id"])
    after = hidden_record(world["friend_id"])
    assert after["active"]
    assert after["opted_out_at"] is None
    assert after["added_at"] >= before
    added_body = added.get_data(as_text=True)
    assert "Invited, currently off" not in added_body
    assert SECRET_ACCOUNT not in added_body
    _logout(http)

    with app.test_request_context("/positions"):
        _as(app, world["operator_id"])
        sql = tenant_sql_and(None)
        assert world["friend_tid"] in sql
        assert "NOT IN" in sql
        assert redact_from_admin(world["friend_id"], world["friend"]) is True

    _login(http, world["operator"])
    hidden_again = http.get("/admin/users")
    assert SECRET_ACCOUNT not in hidden_again.get_data(as_text=True)
    _logout(http)


def test_admin_readd_of_active_user_keeps_the_since_date(world):
    from app.admin_privacy import add_hidden_user, hidden_record

    before = hidden_record(world["friend_id"])["added_at"]
    ok, info = add_hidden_user(world["friend_id"], world["operator_id"])
    assert ok
    assert info == world["friend"]
    after = hidden_record(world["friend_id"])
    assert after["active"]
    assert after["added_at"] == before
    assert after["opted_out_at"] is None


def _notice_chunk(body: str, kind: str) -> str:
    """The form-card notice, not the Bootstrap toast (that stays hidden)."""
    marker = f'class="ap-notice {kind}"'
    assert marker in body
    start = body.index(marker)
    return body[start:start + 800]


def test_fresh_signup_without_broker_can_be_added(world, db_conn):
    """A just-signed-up user can be added, and the row survives the redirect.

    No broker account and no verified email. The admin can use the stored
    username, an @handle, a different case, or the email. The result is
    printed in the form card. Settings then shows the card On.
    """
    from app.db import fetch_one

    http = world["http"]
    signups = []
    for _label in ("exact", "handle", "mixed", "mail"):
        name = "Qq" + uuid.uuid4().hex[:10]
        email = f"{name}@Example.com"
        uid = _create_user(db_conn, name, email)
        world["user_ids"].append(uid)
        verified = fetch_one(
            "SELECT email_verified_at FROM users WHERE id = %s",
            (uid,),
        )
        assert verified["email_verified_at"] is None
        tenants = fetch_one(
            "SELECT COUNT(*) AS n FROM broker_tenants WHERE user_id = %s",
            (uid,),
        )
        assert int(tenants["n"]) == 0
        signups.append({"name": name, "email": email, "id": uid})

    _login(http, world["operator"])
    form = http.get("/admin/private").get_data(as_text=True)
    assert 'method="POST"' in form
    assert 'class="ap-form"' in form
    assert 'name="username"' in form
    assert 'action="/admin/private"' in form

    typed = [
        signups[0]["name"],
        "@" + signups[1]["name"],
        signups[2]["name"].swapcase(),
        signups[3]["email"].lower(),
    ]
    assert typed[2] != signups[2]["name"]
    assert typed[3] != signups[3]["email"]
    for person, raw in zip(signups, typed):
        added = http.post(
            "/admin/private",
            data={"username": raw},
            follow_redirects=True,
        )
        assert added.status_code == 200
        body = added.get_data(as_text=True)
        chunk = _notice_chunk(body, "ok")
        assert "is private from admins" in chunk
        assert f"@{person['name']}" in chunk
        assert f"@{person['name']}" in body
        # A new connection. A request that rolled back on close is empty.
        saved = fetch_one(
            "SELECT active, opted_out_at FROM admin_hidden_users "
            "WHERE user_id = %s",
            (person["id"],),
        )
        assert saved is not None
        assert saved["active"] is True
        assert saved["opted_out_at"] is None

    blank = http.post(
        "/admin/private",
        data={"username": "   "},
        follow_redirects=True,
    )
    assert "Enter a username or email." in _notice_chunk(
        blank.get_data(as_text=True), "bad",
    )
    missing = http.post(
        "/admin/private",
        data={"username": "nosuch_" + uuid.uuid4().hex},
        follow_redirects=True,
    )
    assert "No user with that username or email." in _notice_chunk(
        missing.get_data(as_text=True), "bad",
    )
    _logout(http)

    for person in signups:
        _login(http, person["name"])
        profile = http.get("/profile")
        assert profile.status_code == 200
        assert "Private from HappyTrader admins: On" in profile.get_data(
            as_text=True,
        )
        _logout(http)


class _HtmlResponse:
    def __init__(self):
        self.status_code = 200
        self.headers = {"Content-Type": "text/html; charset=utf-8"}
        self.mimetype = "text/html"


def _signup_fresh(http, username: str, email: str):
    """POST /signup with zero broker accounts. Returns the new user id."""
    resp = http.post(
        "/signup",
        data={
            "username": username,
            "password": "testpass123",
            "email": email,
        },
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303), resp.get_data(as_text=True)[:800]
    location = resp.headers.get("Location") or ""
    assert "/get-started" in location, location
    from app.models import User
    user = User.get_by_username(username)
    assert user is not None
    return int(user.id)


def _attach_vanguard(db_conn, user_id: int, tenant_id: str, account_name: str, *, broken: bool):
    """Mirror what a SnapTrade connect writes after the privacy invite."""
    broken_at = "NOW()" if broken else "NULL"
    error = f"{account_name}-SYNC-ERR" if broken else None
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO user_accounts (user_id, account_name) VALUES (%s, %s)",
            (user_id, account_name),
        )
        cur.execute(
            f"""
            INSERT INTO snaptrade_accounts
                (user_id, snaptrade_account_id, tenant_id, broker_slug,
                 account_name, display_nickname, account_number_masked,
                 connection_broken_at, last_sync_error)
            VALUES (%s, %s, %s, 'vanguard', %s, %s, '••••late', {broken_at}, %s)
            """,
            (
                user_id,
                uuid.uuid4().hex,
                tenant_id,
                account_name,
                account_name,
                error,
            ),
        )
    db_conn.commit()


def test_tenants_linked_after_privacy_add_stay_hidden(world, db_conn):
    """Signup, invite with zero accounts, then link brokerages.

    The hide list stores a user id. Tenant ids are joined from
    ``broker_tenants`` on each read, so a Vanguard account created
    after /admin/private — and a second account after that — is
    excluded the same way as one that existed at add time.
    """
    from app.admin_privacy import (
        format_privacy_since,
        hidden_record,
        hidden_snapshot,
        is_hidden_from_admins,
    )
    from app import _shell_account_scope
    from app.funnel import _observe, clear_hidden_user_cache, viewer_is_hidden_from_admins
    from app.routes import _tenants_for_scope
    from app.tenant_scope import filter_df_by_tenant_ids, tenant_sql_and

    app = world["app"]
    http = world["http"]
    late_name = _unique("priv_late")
    late_email = f"{late_name}@example.com"
    mail_name = _unique("priv_mail")
    mail_email = f"{mail_name}@example.com"
    first_account = "ZZLATE-VANGUARD-8841"
    second_account = "ZZLATE-SECOND-8841"
    mail_account = "ZZEMAIL-VANGUARD-8841"
    late_symbol = "ZZ" + uuid.uuid4().hex[:6].upper()
    late_note = f"late vanguard note {uuid.uuid4().hex[:8]}"

    late_id = _signup_fresh(http, late_name, late_email)
    world["user_ids"].append(late_id)
    started = http.get("/get-started")
    assert started.status_code == 200
    bare = http.get("/profile")
    assert bare.status_code == 200
    assert "Private from HappyTrader admins" not in bare.get_data(as_text=True)
    _logout(http)

    clear_hidden_user_cache()
    assert viewer_is_hidden_from_admins(late_id) is False

    _login(http, world["operator"])
    added = http.post(
        "/admin/private",
        data={"username": late_name},
        follow_redirects=True,
    )
    assert added.status_code == 200
    assert f"@{late_name} is private from admins" in added.get_data(as_text=True)
    assert is_hidden_from_admins(late_id)
    # The add drops the process-local page-view cache filled above.
    assert viewer_is_hidden_from_admins(late_id) is True
    _logout(http)

    _login(http, late_name)
    early = http.get("/profile")
    early_body = early.get_data(as_text=True)
    since = format_privacy_since(hidden_record(late_id)["added_at"])
    assert "Private from HappyTrader admins: On" in early_body
    assert f"since {since}" in early_body
    assert first_account not in early_body
    prefs = http.get("/profile?tab=preferences")
    assert f"since {since}" in prefs.get_data(as_text=True)
    _logout(http)

    mail_id = _signup_fresh(http, mail_name, mail_email)
    world["user_ids"].append(mail_id)
    _logout(http)
    _login(http, world["operator"])
    by_email = http.post(
        "/admin/private",
        data={"username": mail_email},
        follow_redirects=True,
    )
    assert by_email.status_code == 200
    assert f"@{mail_name} is private from admins" in by_email.get_data(as_text=True)
    assert is_hidden_from_admins(mail_id)
    _logout(http)

    with app.test_request_context("/positions"):
        _as(app, world["operator_id"])
        before_tenants = set(hidden_snapshot()["tenant_ids"])
        assert late_id in hidden_snapshot()["user_ids"]
        assert mail_id in hidden_snapshot()["user_ids"]

    first_tid = _link_tenant(late_id, first_account)
    mail_tid = _link_tenant(mail_id, mail_account)
    _attach_vanguard(db_conn, late_id, first_tid, first_account, broken=True)
    _attach_vanguard(db_conn, mail_id, mail_tid, mail_account, broken=False)
    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO feedback (user_id, username, body, page_path)
            VALUES (%s, %s, %s, %s)
            """,
            (late_id, late_name, late_note, f"/position/{late_symbol}"),
        )
        cur.execute(
            """
            INSERT INTO usage_events (user_id, endpoint, path, status_code)
            VALUES (%s, 'position_detail', %s, 200)
            """,
            (late_id, f"/position/{late_symbol}"),
        )
    db_conn.commit()

    frame = pd.DataFrame([
        {"tenant_id": first_tid, "account": first_account, "symbol": late_symbol},
        {"tenant_id": mail_tid, "account": mail_account, "symbol": "SPY"},
        {
            "tenant_id": world["visible_tid"],
            "account": SHARED_LABEL,
            "symbol": "QQQ",
        },
    ])
    with app.test_request_context("/positions"):
        _as(app, world["operator_id"])
        after = hidden_snapshot()
        assert after["available"] is True
        assert first_tid in after["tenant_ids"]
        assert mail_tid in after["tenant_ids"]
        assert first_tid not in before_tenants
        assert mail_tid not in before_tenants
        sql = tenant_sql_and(None)
        assert "NOT IN" in sql
        assert first_tid in sql
        assert mail_tid in sql
        kept = filter_df_by_tenant_ids(frame, None)
        assert first_account not in set(kept["account"])
        assert mail_account not in set(kept["account"])
        assert world["visible_tid"] in set(kept["tenant_id"])
        # An unscoped admin read stays open and drops the private tenants
        # in SQL. Naming the account label is the 404.
        assert _tenants_for_scope("") is None
        with pytest.raises(NotFound):
            _tenants_for_scope(first_account)
    with app.test_request_context(f"/positions?tenant={first_tid}"):
        _as(app, world["operator_id"])
        with pytest.raises(NotFound):
            _tenants_for_scope("")
    with app.test_request_context(
        f"/strategies?tenants={first_tid},{world['visible_tid']}"
    ):
        _as(app, world["operator_id"])
        with pytest.raises(NotFound):
            _tenants_for_scope("")

    with app.test_request_context("/positions"):
        _as(app, late_id)
        own = _tenants_for_scope("")
        assert first_tid in own
        own_sql = tenant_sql_and(own)
        assert first_tid in own_sql
        assert "NOT IN" not in own_sql
        own_kept = filter_df_by_tenant_ids(frame, own)
        assert first_account in set(own_kept["account"])
        friend_choices = _shell_account_scope()[5]
        # The header picker lists this user's own tenants. A label with a
        # digit run is shown as "Unnamed account"; the tenant id is the check.
        assert first_tid in {row["tenant_id"] for row in friend_choices}

    with app.test_request_context("/overview"):
        _as(app, world["operator_id"])
        admin_choices = _shell_account_scope()[5]
        assert first_tid not in {row["tenant_id"] for row in admin_choices}
        assert mail_tid not in {row["tenant_id"] for row in admin_choices}

    # A second brokerage, linked after the first one was already hidden.
    second_tid = _link_tenant(late_id, second_account)
    _attach_vanguard(db_conn, late_id, second_tid, second_account, broken=False)
    with app.test_request_context("/positions"):
        _as(app, world["operator_id"])
        snap = hidden_snapshot()
        assert second_tid in snap["tenant_ids"]
        assert second_tid in tenant_sql_and(None)
        with pytest.raises(NotFound):
            _tenants_for_scope(second_account)
    with app.test_request_context(f"/positions?tenant={second_tid}"):
        _as(app, world["operator_id"])
        with pytest.raises(NotFound):
            _tenants_for_scope("")
    with app.test_request_context("/positions"):
        _as(app, late_id)
        assert second_tid in _tenants_for_scope("")

    _login(http, world["operator"])
    for path in AUDITED_PATHS:
        resp = http.get(_with_tenant(path, first_tid), follow_redirects=False)
        assert resp.status_code == 404, path
        body = resp.get_data(as_text=True)
        assert first_account not in body
        assert second_account not in body
        assert mail_account not in body
    both = http.get(
        f"/positions?tenants={first_tid},{second_tid}",
        follow_redirects=False,
    )
    assert both.status_code == 404
    assert first_account not in both.get_data(as_text=True)
    labeled = http.get(
        f"/positions?account={first_account}",
        follow_redirects=False,
    )
    # 404 before any warehouse read. The typed account label is echoed
    # in the page's own URL, so the status is the check.
    assert labeled.status_code == 404
    assert "total_pnl" not in labeled.get_data(as_text=True)

    users = http.get("/admin/users")
    users_body = users.get_data(as_text=True)
    assert users.status_code == 200
    assert late_name in users_body
    assert mail_name in users_body
    assert "Private" in users_body
    assert first_account not in users_body
    assert second_account not in users_body
    assert mail_account not in users_body
    assert f"/admin/users/{late_id}/activity" not in users_body
    assert f"/admin/users/{mail_id}/activity" not in users_body
    assert f"/admin/impersonate/{late_name}" not in users_body
    assert f"/admin/impersonate/{mail_name}" not in users_body

    activity = http.get(f"/admin/users/{late_id}/activity")
    assert activity.status_code == 404
    assert late_name not in activity.get_data(as_text=True)
    assert first_account not in activity.get_data(as_text=True)
    mail_activity = http.get(f"/admin/users/{mail_id}/activity")
    assert mail_activity.status_code == 404

    impersonate = http.get(f"/admin/impersonate/{late_name}")
    assert impersonate.status_code == 404
    preview = http.get(
        f"/admin/digest-preview?user={late_name}&week=2026-10-05",
    )
    assert preview.status_code == 404
    assert first_account not in preview.get_data(as_text=True)

    private = http.get("/admin/private")
    private_body = private.get_data(as_text=True)
    assert f"@{late_name}" in private_body
    assert f"@{mail_name}" in private_body
    assert since in private_body
    assert first_account not in private_body
    assert second_account not in private_body
    assert mail_account not in private_body
    assert "does not show accounts, balances, or trades" in private_body

    overview = http.get("/admin")
    overview_body = overview.get_data(as_text=True)
    assert overview.status_code == 200
    assert first_account not in overview_body
    assert f"{first_account}-SYNC-ERR" not in overview_body
    assert f"/position/{late_symbol}" not in overview_body
    assert late_symbol not in overview_body
    assert f"/admin/impersonate/{late_name}" not in overview_body
    assert "Private" in overview_body

    feedback = http.get("/admin/feedback")
    fb = feedback.get_data(as_text=True)
    assert late_note in fb
    assert f"/position/{late_symbol}" not in fb

    analytics = http.get("/admin/analytics")
    assert analytics.status_code == 200
    analytics_body = analytics.get_data(as_text=True)
    assert first_account not in analytics_body
    assert mail_account not in analytics_body
    assert late_symbol not in analytics_body
    assert late_name not in analytics_body
    assert mail_name not in analytics_body
    assert late_email not in analytics_body
    _logout(http)

    _login(http, late_name)
    page = http.get("/profile")
    body = page.get_data(as_text=True)
    assert page.status_code == 200
    assert "Private from HappyTrader admins: On" in body
    assert f"since {since}" in body
    assert first_account in body
    assert second_account in body
    assert mail_account not in body
    _logout(http)

    # Page views from before the invite (signup, get-started) are
    # expected. The skip starts once privacy is on.
    with db_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM funnel_events WHERE user_id = ANY(%s) AND event = 'app_page_view'",
            ([late_id, mail_id],),
        )
    db_conn.commit()
    clear_hidden_user_cache()
    with app.test_request_context("/overview"):
        _as(app, world["visible_id"])
        _observe(_HtmlResponse())
    with app.test_request_context("/positions"):
        _as(app, late_id)
        _observe(_HtmlResponse())
    with app.test_request_context("/strategies"):
        _as(app, mail_id)
        _observe(_HtmlResponse())
    from app.db import fetch_all
    visible_rows = fetch_all(
        """
        SELECT event FROM funnel_events
        WHERE user_id = %s AND event = 'app_page_view'
        """,
        (world["visible_id"],),
    )
    late_rows = fetch_all(
        """
        SELECT event FROM funnel_events
        WHERE user_id = %s AND event = 'app_page_view'
        """,
        (late_id,),
    )
    mail_rows = fetch_all(
        """
        SELECT event FROM funnel_events
        WHERE user_id = %s AND event = 'app_page_view'
        """,
        (mail_id,),
    )
    assert len(visible_rows) == 1
    assert late_rows == []
    assert mail_rows == []
    with db_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM funnel_events WHERE user_id = ANY(%s)",
            ([world["visible_id"], late_id, mail_id],),
        )
        cur.execute(
            "DELETE FROM feedback WHERE user_id = ANY(%s) OR body = %s",
            ([late_id, mail_id], late_note),
        )
        cur.execute(
            "DELETE FROM usage_events WHERE user_id = ANY(%s)",
            ([late_id, mail_id],),
        )
    db_conn.commit()
