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
