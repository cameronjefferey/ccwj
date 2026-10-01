"""Demo sessions must not 500 when a route casts the Flask-Login id.

The public demo identity is ``demo-session:<token>``, not a Postgres
``users.id``. ``int()`` on that string is a ValueError. Reads return
empty. Writes refuse before any insert.
"""
import importlib
import uuid

import pandas as pd
import pytest
from flask_login import login_user


_WAREHOUSE_COLUMNS = (
    "tenant_id", "account", "symbol", "strategy", "status",
    "num_winners", "num_losers", "total_return", "realized_pnl",
    "unrealized_pnl", "total_premium_received", "total_premium_paid",
    "num_individual_trades", "num_trade_groups", "total_pnl",
    "total_dividend_income", "dividend_count", "win_rate",
    "avg_pnl_per_trade", "avg_days_in_trade", "subsector", "sector",
    "company_name", "date", "trade_date", "open_date", "close_date",
    "leg_id", "display_leg_num", "user_id", "quantity", "price",
    "amount", "fees", "market_value", "current_price", "cost_basis",
    "instrument_type", "action", "underlying_symbol",
    "account_value", "delta_1d", "dollar_impact", "close_price",
    "pct_change",
)

_BQ_MODULES = (
    "app.marketing",
    "app.earnings_page",
    "app.accounts_page",
    "app.symbols_page",
    "app.positions_page",
    "app.wealth",
    "app.admin",
    "app.sectors_page",
    "app.insights",
    "app.strategy_fit",
    "app.trader_story",
    "app.share_card",
    "app.position_detail",
    "app.strategy_fit_insights",
    "app.strategies",
    "app.first_look",
    "app.weekly_review",
    "app.routes",
    "app.bigquery_client",
    "app.held_chart",
    "app.pnl_charts",
)


def _empty_warehouse_df():
    return pd.DataFrame({c: pd.Series(dtype="float64") if c in {
        "num_winners", "num_losers", "total_return", "realized_pnl",
        "unrealized_pnl", "total_premium_received", "total_premium_paid",
        "num_individual_trades", "num_trade_groups", "total_pnl",
        "total_dividend_income", "dividend_count", "win_rate",
        "avg_pnl_per_trade", "avg_days_in_trade", "quantity", "price",
        "amount", "fees", "market_value", "current_price", "cost_basis",
        "leg_id", "display_leg_num", "account_value", "delta_1d",
        "dollar_impact", "close_price", "pct_change",
    } else "object" for c in _WAREHOUSE_COLUMNS})


class _StubJob:
    def to_dataframe(self):
        return _empty_warehouse_df()

    def result(self):
        return []

    def __iter__(self):
        return iter(())


class _StubClient:
    def query(self, *_a, **_k):
        return _StubJob()


def _install_warehouse(monkeypatch):
    def _df(*_a, **_k):
        return _empty_warehouse_df()

    def _client():
        return _StubClient()

    monkeypatch.setattr("app.query_cache.cached_query_df", _df)
    monkeypatch.setattr("app.bigquery_client.get_bigquery_client", _client)
    for name in _BQ_MODULES:
        mod = importlib.import_module(name)
        if hasattr(mod, "get_bigquery_client"):
            monkeypatch.setattr(mod, "get_bigquery_client", _client)
        if hasattr(mod, "cached_query_df"):
            monkeypatch.setattr(mod, "cached_query_df", _df)


def test_numeric_user_id_rejects_demo_session():
    from app.demo_guard import is_demo_session_id, numeric_user_id

    assert is_demo_session_id("demo-session:abc")
    assert numeric_user_id("demo-session:abc") is None
    assert numeric_user_id(7) == 7
    assert numeric_user_id("9") == 9
    assert numeric_user_id(True) is None
    assert numeric_user_id(False) is None
    assert numeric_user_id(None) is None
    assert numeric_user_id("") is None
    assert numeric_user_id("  ") is None
    assert numeric_user_id(9.0) == 9
    assert numeric_user_id(9.5) is None


def test_user_scoped_sql_fails_closed_for_demo_session():
    from app.routes import _filter_df_by_user, _user_scoped_and, _user_scoped_filter

    sql = _user_scoped_filter("demo-session:abc", ["Demo Account"])
    assert "1 = 0" in sql
    assert "demo-session" not in sql
    assert "user_id = " not in sql
    and_sql = _user_scoped_and("demo-session:abc", ["Demo Account"])
    assert "1 = 0" in and_sql
    assert "demo-session" not in and_sql

    frame = pd.DataFrame({
        "user_id": [1, None],
        "account": ["Demo Account", "Demo Account"],
    })
    out = _filter_df_by_user(frame, "demo-session:abc", ["Demo Account"])
    assert out.empty


def test_demo_tag_and_broker_reads_are_empty():
    from app import models

    demo = "demo-session:route-sweep"
    assert models.get_leg_tags_for_symbol(demo, "MU") == []
    assert models.get_all_leg_tags_for_user(demo) == []
    assert models.get_distinct_tags_for_user(demo) == []
    assert models.get_broker_accounts_for_user(demo) == []
    assert models.get_broker_account_ids_for_user(demo) == []
    assert models.get_broken_broker_tenants(demo) == []
    assert models.add_position_leg_tag(
        demo, "demo:demo-account", "MU", "2026-01-01", "ef",
    ) is None
    assert models.remove_position_leg_tag(
        demo, "demo:demo-account", "MU", "2026-01-01", "ef",
    ) is False
    with pytest.raises(ValueError):
        models.get_or_create_broker_tenant(demo, "manual", "manual:x", "IRA")
    with pytest.raises(ValueError):
        models.create_account_group(demo, "Family")
    assert models.delete_account_group(demo, 1) is False
    assert models.update_broker_tenant_display_nickname(demo, "demo:demo-account", "X") is False


def _start_demo(app, monkeypatch):
    monkeypatch.setitem(app.config, "WTF_CSRF_ENABLED", False)
    monkeypatch.delenv("TURNSTILE_SITE_KEY", raising=False)
    monkeypatch.delenv("TURNSTILE_SECRET_KEY", raising=False)
    client = app.test_client()
    resp = client.post("/demo/start", follow_redirects=False)
    assert resp.status_code == 302
    with client.session_transaction() as sess:
        uid = str(sess.get("_user_id") or "")
    assert uid.startswith("demo-session:")
    return client, uid


_DEMO_GETS = (
    "/overview?_full=1",
    "/overview/below",
    "/daily-review?_full=1",
    "/weekly-review?_full=1",
    "/today?_full=1",
    "/positions?_full=1",
    "/position/MU?_full=1",
    "/position/NOK?_full=1",
    "/position/JPM?_full=1",
    "/position/MSFT?_full=1",
    "/position/MU?_full=1&leg=1",
    "/api/position/MU/peek",
    "/strategies",
    "/strategies?view=fit",
    "/strategy-fit",
    "/sectors",
    "/industries",
    "/insights",
    "/story",
    "/accounts?_full=1",
    "/accounts/breakdown",
    "/accounts?view=value",
    "/wealth",
    "/profile",
    "/settings",
    "/share/card.png",
    "/api/nav/symbols",
    "/api/sync/overview-ready",
    "/api/github/workflow-status",
    "/daily-review/day/2026-09-30",
    "/earnings",
    "/upload",
    "/get-started",
    "/snaptrade/accounts",
    "/symbols",
    "/first-look",
)


def test_demo_session_routes_do_not_500(app, monkeypatch):
    _install_warehouse(monkeypatch)
    called = {}
    import app.models as models

    real_symbol = models.get_leg_tags_for_symbol
    real_distinct = models.get_distinct_tags_for_user
    real_all = models.get_all_leg_tags_for_user

    def _symbol(user_id, symbol, tenant_ids=None):
        called.setdefault("symbol", []).append((str(user_id), str(symbol)))
        return real_symbol(user_id, symbol, tenant_ids)

    def _distinct(user_id):
        called.setdefault("distinct", []).append(str(user_id))
        return real_distinct(user_id)

    def _all(user_id, tenant_ids=None):
        called.setdefault("all", []).append(str(user_id))
        return real_all(user_id, tenant_ids)

    monkeypatch.setattr(models, "get_leg_tags_for_symbol", _symbol)
    monkeypatch.setattr(models, "get_distinct_tags_for_user", _distinct)
    monkeypatch.setattr(models, "get_all_leg_tags_for_user", _all)

    client, uid = _start_demo(app, monkeypatch)
    headers = {"X-HT-Full": "1"}
    failures = []
    for path in _DEMO_GETS:
        resp = client.get(path, headers=headers)
        if resp.status_code >= 500:
            body = resp.get_data(as_text=True)[:400]
            failures.append(f"{path} -> {resp.status_code} {body}")
    assert not failures, "demo GET 500s:\n" + "\n".join(failures)

    symbol_hits = called.get("symbol") or []
    assert any(
        user_id == uid and symbol == "MU" for user_id, symbol in symbol_hits
    ), symbol_hits
    assert uid in (called.get("distinct") or [])

    ask = client.post(
        "/insights/ask",
        json={"question": "What did I do?"},
        headers={"Accept": "application/json", **headers},
    )
    assert ask.status_code < 500
    tag = client.post(
        "/position/MU/tags",
        data={"tag": "ef", "leg_open_date": "2026-01-01", "tenant_id": "demo:demo-account"},
        headers=headers,
    )
    assert tag.status_code < 500
    chart = client.post(
        "/position/MU/chart-read",
        data={"digest": "abc"},
        headers=headers,
    )
    assert chart.status_code < 500


def test_overview_ready_has_its_own_per_minute_limit(app, monkeypatch):
    _install_warehouse(monkeypatch)
    monkeypatch.setitem(app.config, "RATELIMIT_ENABLED", True)
    from app.models import User

    username = f"ready_{uuid.uuid4().hex[:10]}"
    User.create(username, "correct-horse-battery")
    user = User.get_by_username(username)
    client = app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = str(user.id)
        sess["_fresh"] = True

    codes = [
        client.get("/api/sync/overview-ready").status_code
        for _ in range(31)
    ]
    assert codes[0] == 200
    assert 429 in codes
    assert codes.index(429) >= 30
