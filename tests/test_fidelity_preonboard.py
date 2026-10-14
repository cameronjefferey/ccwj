"""Fidelity pre-onboard fixtures. Example numbers only. No warehouse rows."""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from unittest.mock import patch

os.environ.setdefault("HAPPYTRADER_SKIP_DB_INIT", "1")

import pandas as pd

from app.early_broker import broker_key, is_modeled_broker
from app.linked_accounts import sync_status_label
from app.snaptrade_normalize import (
    activities_to_history_df,
    balances_to_balance_df,
    cash_equivalent_symbols,
    orders_to_history_df,
    positions_to_current_df,
)

TENANT = "snaptrade:fidelity-fixture"
ACCOUNT = "Fidelity ••••5678"


def _act(atype, symbol, **extra):
    row = {
        "type": atype,
        "symbol": symbol,
        "units": extra.pop("units", 1),
        "price": extra.pop("price", 1),
        "amount": extra.pop("amount", 1),
        "trade_date": extra.pop("trade_date", "2026-10-01"),
        "fee": extra.pop("fee", 0),
    }
    row.update(extra)
    return row


def test_fidelity_account_label_routes_on_the_first_word():
    from app import snaptrade as snap

    assert snap._stable_account_name("FIDELITY", "12345678") == "Fidelity ••••5678"
    assert snap._stable_account_name("Fidelity", "ABCD") == "Fidelity Account"
    labeled = snap._stable_account_name("Fidelity Investments", "99887766")
    assert labeled.split()[0] == "Fidelity"
    assert labeled.endswith("••••7766")
    netbenefits = snap._stable_account_name("Fidelity NetBenefits", "11112222")
    assert netbenefits.split()[0] == "Fidelity"
    assert broker_key("Fidelity") == "fidelity"
    assert broker_key("FIDELITY INVESTMENTS") == "fidelity"
    assert broker_key("Fidelity NetBenefits") == "fidelity"
    assert is_modeled_broker("Fidelity") is True
    # Not a Fidelity* first token. Do not alias this without a live string.
    assert broker_key("National Financial Services") == "national"
    assert is_modeled_broker("National Financial Services") is False


def test_fidelity_core_sweep_is_not_an_equity_lot_when_flagged():
    """Example numbers. Cash already includes the core position.

    Stock market value 5800, stock cost 5000, SPAXX 4200 at $1.
    Derived account total is 10000, not 14200. Unrealized is 5000,
    not 800.
    """
    spaxx = {
        "symbol": "SPAXX",
        "units": 4200,
        "price": 1,
        "market_value": 4200,
        "cost_basis": 4200,
        "cash_equivalent": True,
        "description": "Fidelity Government Money Market",
    }
    stock = {
        "symbol": {"symbol": "AAPL", "raw_symbol": "AAPL", "description": "Apple"},
        "units": 20,
        "price": 290,
        "market_value": 5800,
        "cost_basis": 5000,
        "cash_equivalent": False,
    }
    assert cash_equivalent_symbols([spaxx, stock]) == {"SPAXX"}
    current = positions_to_current_df(
        [spaxx, stock], account_name=ACCOUNT, user_id=21, tenant_id=TENANT,
    )
    by_symbol = {row["Symbol"]: row for _, row in current.iterrows()}
    assert by_symbol["SPAXX"]["security_type"] == "Cash and Money Market"
    assert by_symbol["AAPL"]["security_type"] == "Equity"

    balances = balances_to_balance_df(
        account_summary={},
        balances=[{"cash": 4200}],
        positions=[spaxx, stock],
        account_name=ACCOUNT,
        user_id=21,
        tenant_id=TENANT,
    )
    total = balances.loc[balances["row_type"] == "account_total"].iloc[0]
    assert float(total["market_value"]) == 10000
    assert float(total["cost_basis"]) == 5000
    assert float(total["unrealized_pnl"]) == 5000

    reported = balances_to_balance_df(
        account_summary={"balance": {"total": {"amount": 10000}}},
        balances=[{"cash": 4200}],
        positions=[spaxx, stock],
        account_name=ACCOUNT,
        user_id=21,
        tenant_id=TENANT,
    )
    reported_total = reported.loc[reported["row_type"] == "account_total"].iloc[0]
    assert float(reported_total["market_value"]) == 10000
    assert float(reported_total["unrealized_pnl"]) == 5000


def test_fidelity_sweep_buy_is_kept_when_the_flag_is_false():
    """Example: $1000 settlement fund with cash_equivalent false stays Equity."""
    pos = {
        "symbol": {"raw_symbol": "SPAXX", "symbol": "SPAXX"},
        "units": 1000,
        "price": 1,
        "market_value": 1000,
        "cost_basis": 1000,
        "cash_equivalent": False,
    }
    assert cash_equivalent_symbols([pos]) == set()
    current = positions_to_current_df(
        [pos], account_name=ACCOUNT, user_id=21, tenant_id=TENANT,
    )
    assert current.iloc[0]["security_type"] == "Equity"
    history = activities_to_history_df(
        [_act("BUY", "SPAXX", units=1000, price=1, amount=1000)],
        account_name=ACCOUNT,
        user_id=21,
        tenant_id=TENANT,
        cash_equivalent_symbols=cash_equivalent_symbols([pos]),
    )
    assert list(history["Action"]) == ["Buy"]
    assert history.iloc[0]["Symbol"] == "SPAXX"


def test_fidelity_activity_shapes_string_object_fund_option_and_sweep():
    """Synthetic SnapTrade activities. Dollars are made up."""
    sweeps = {"SPAXX", "FDRXX"}
    option = {
        "underlying_symbol": {"symbol": "AAPL", "raw_symbol": "AAPL"},
        "expiration_date": "2026-11-20",
        "strike_price": 200,
        "option_type": "CALL",
    }
    activities = [
        _act("BUY", "SPAXX", units=4200, price=1, amount=4200),
        _act("SELL", {"raw_symbol": "FDRXX", "symbol": "FDRXX"}, units=100, price=1, amount=100),
        _act(
            "BUY",
            {"symbol": "FXAIX", "raw_symbol": "FXAIX", "description": "Fidelity 500 Index"},
            units=12.25, price=200, amount=2450,
        ),
        _act("DIVIDEND", "SPAXX", units=4200, price=1, amount=4.20),
        _act("REI", "SPAXX", units=4, price=1, amount=4),
        _act("REI", "FXAIX", units=1, price=200, amount=200),
        _act("INTEREST", None, units=0, price=0, amount=3.50),
        _act("FEE", None, units=0, price=0, amount=1.25),
        _act("CONTRIBUTION", None, units=0, price=0, amount=500),
        _act("WITHDRAWAL", None, units=0, price=0, amount=40),
        _act("TRANSFER", "AAPL", units=10, price=0, amount=0),
        _act("DISTRIBUTION", None, units=0, price=0, amount=40),
        _act("EXTERNAL_ASSET_TRANSFER_IN", "AAPL", units=10, price=0, amount=0),
        {
            "type": "OPTIONASSIGNMENT",
            "symbol": None,
            "option_symbol": option,
            "units": 1,
            "price": 0,
            "amount": 0,
            "trade_date": "2026-10-16",
            "fee": 0,
        },
        {
            "type": "OPTIONEXERCISE",
            "symbol": None,
            "option_symbol": option,
            "units": 1,
            "price": 0,
            "amount": 0,
            "trade_date": "2026-10-16",
            "fee": 0,
        },
        {
            "type": "OPTIONEXPIRATION",
            "symbol": None,
            "option_symbol": option,
            "units": 1,
            "price": 0,
            "amount": 0,
            "trade_date": "2026-11-20",
            "fee": 0,
        },
    ]
    df = activities_to_history_df(
        activities,
        account_name=ACCOUNT,
        user_id=21,
        tenant_id=TENANT,
        cash_equivalent_symbols=sweeps,
    )
    actions = list(df["Action"])
    symbols = list(df["Symbol"])
    assert "SPAXX" not in symbols or set(df.loc[df["Symbol"] == "SPAXX", "Action"]) <= {
        "Cash Dividend",
    }
    assert ("Buy", "SPAXX") not in list(zip(actions, symbols))
    assert ("Sell", "FDRXX") not in list(zip(actions, symbols))
    assert ("Buy", "FXAIX") in list(zip(actions, symbols))
    spaxx_rows = df[df["Symbol"] == "SPAXX"]
    assert list(spaxx_rows["Action"]) == ["Cash Dividend", "Cash Dividend"]
    fx_buys = df[(df["Symbol"] == "FXAIX") & (df["Action"] == "Buy")]
    assert len(fx_buys) == 2  # the share buy plus the reinvestment companion
    assert "Credit Interest" in actions
    assert "ADR Mgmt Fee" in actions
    assert "Deposit" in actions
    assert "Withdrawal" in actions
    assert "Assigned" in actions
    assert "Exchange or Exercise" in actions
    assert "Expired" in actions
    assert df["tenant_id"].eq(TENANT).all()
    assert df["Account"].eq(ACCOUNT).all()


def test_fidelity_orders_skip_a_flagged_sweep_and_keep_a_string_ticker():
    orders = [
        {
            "status": "EXECUTED",
            "action": "BUY",
            "universal_symbol": "SPAXX",
            "time_executed": "2026-10-01T15:00:00Z",
            "filled_quantity": 100,
            "execution_price": 1,
        },
        {
            "status": "EXECUTED",
            "action": "BUY",
            "universal_symbol": {"raw_symbol": "FXAIX", "symbol": "FXAIX", "description": "Fidelity 500 Index"},
            "time_executed": "2026-10-01T15:05:00Z",
            "filled_quantity": 2,
            "execution_price": 200,
        },
    ]
    df = orders_to_history_df(
        orders,
        account_name=ACCOUNT,
        user_id=21,
        tenant_id=TENANT,
        cash_equivalent_symbols={"SPAXX"},
    )
    assert list(df["Symbol"]) == ["FXAIX"]
    assert list(df["Action"]) == ["Buy"]


def test_fidelity_mutual_fund_fractional_and_option_snapshot():
    """units 12 and fractional_units 0.25 is 12.25. units 40 and
    fractional_units 1.44 stays 40. A call is an option, not a fund.
    """
    rows = positions_to_current_df(
        [
            {
                "symbol": {"symbol": "FXAIX", "raw_symbol": "FXAIX", "description": "Fidelity 500 Index"},
                "units": 12,
                "fractional_units": 0.25,
                "price": 200,
                "average_purchase_price": 180,
                "cash_equivalent": False,
            },
            {
                "symbol": "FDRXX",
                "units": 40,
                "fractional_units": 1.44,
                "price": 1,
                "cash_equivalent": True,
            },
            {
                "symbol": None,
                "option_symbol": {
                    "underlying_symbol": {"symbol": "AAPL", "raw_symbol": "AAPL"},
                    "expiration_date": "2026-11-20",
                    "strike_price": 200,
                    "option_type": "CALL",
                },
                "units": 1,
                "price": 3.5,
                "average_purchase_price": 320,
            },
        ],
        account_name=ACCOUNT,
        user_id=21,
        tenant_id=TENANT,
    )
    by_type = {row["security_type"]: row for _, row in rows.iterrows()}
    fund = rows[rows["Symbol"] == "FXAIX"].iloc[0]
    assert fund["security_type"] == "Equity"
    assert float(fund["Quantity"]) == 12.25
    sweep = rows[rows["Symbol"] == "FDRXX"].iloc[0]
    assert sweep["security_type"] == "Cash and Money Market"
    assert float(sweep["Quantity"]) == 40
    assert by_type["Option"]["security_type"] == "Option"
    assert "AAPL" in by_type["Option"]["Symbol"]


def test_fidelity_nested_core_symbol_is_collected_from_the_position():
    pos = {
        "symbol": {"symbol": {"raw_symbol": "FDRXX", "symbol": "FDRXX", "description": "Cash reserves"}},
        "units": 50,
        "price": 1,
        "cash_equivalent": True,
    }
    assert "FDRXX" in cash_equivalent_symbols([pos])


def test_new_fidelity_connection_does_not_treat_an_old_holdings_clock_as_a_stall(monkeypatch):
    from app import snaptrade as snap

    summary = {
        "sync_status": {
            "holdings": {"last_successful_sync": "2024-06-28T18:42:46.561Z"}
        }
    }
    monkeypatch.setattr(snap, "_ensure_snaptrade_tenant_id", lambda **k: TENANT)
    monkeypatch.setattr(snap, "_brokerage_authorization_disabled", lambda *a, **k: False)
    monkeypatch.setattr(snap, "_fetch_activities", lambda *a, **k: [])
    monkeypatch.setattr(snap, "_fetch_recent_orders", lambda *a, **k: [])
    monkeypatch.setattr(snap, "_fetch_account_orders", lambda *a, **k: [])
    monkeypatch.setattr(snap, "_fetch_positions", lambda *a, **k: [])
    monkeypatch.setattr(snap, "_fetch_option_holdings", lambda *a, **k: [])
    monkeypatch.setattr(snap, "_fetch_balances", lambda *a, **k: [])
    monkeypatch.setattr(snap, "_fetch_account_summary", lambda *a, **k: summary)
    monkeypatch.setattr(snap, "record_snaptrade_account_identity", lambda *a, **k: None)
    monkeypatch.setattr(snap, "SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS", 4)
    sentinel = RuntimeError("reached normalize")
    monkeypatch.setattr(
        snap, "activities_to_history_df",
        lambda *a, **k: (_ for _ in ()).throw(sentinel),
    )
    acc_row = {
        "snaptrade_account_id": "fid",
        "account_name": ACCOUNT,
        "broker_slug": "fidelity",
        "created_at": datetime(2026, 10, 14, 15, 0, tzinfo=timezone.utc),
    }
    try:
        snap._run_sync(
            21, object(),
            snap={"snaptrade_user_id": "u", "snaptrade_secret": "s"},
            acc_row=acc_row,
            lookback_days=60,
        )
    except RuntimeError as exc:
        assert exc is sentinel
    else:
        raise AssertionError("stale gate raised before normalize")


def test_fidelity_holdings_unavailable_and_a_real_stall_stay_distinct():
    from app import snaptrade as snap

    unavailable = {
        "sync_status": {
            "holdings": {
                "last_successful_sync": "2024-06-28T18:42:46.561Z",
                "holdings_unavailable": True,
            }
        }
    }
    assert snap._holdings_stale_days(unavailable, today=date(2026, 10, 14)) is None
    young = {
        "snaptrade_account_id": "fid",
        "account_name": ACCOUNT,
        "broker_slug": "fidelity",
        "connection_broken_at": datetime(2026, 10, 14, 16, 0, tzinfo=timezone.utc),
        "created_at": datetime(2026, 10, 14, 15, 0, tzinfo=timezone.utc),
        "last_sync_error": (
            "connection_broken:sync_status.holdings.last_successful_sync[stale]"
        ),
    }
    assert snap._connection_attention(young, today=date(2026, 10, 14)) is None
    assert sync_status_label(young) == "Sync will retry"
    real = {
        "snaptrade_account_id": "fid",
        "account_name": ACCOUNT,
        "broker_slug": "fidelity",
        "connection_broken_at": datetime(2026, 6, 19, tzinfo=timezone.utc),
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "holdings_last_successful_sync": date(2026, 6, 12),
        "last_sync_error": (
            "connection_broken:sync_status.holdings.last_successful_sync[stale]"
        ),
    }
    att = snap._connection_attention(real, today=date(2026, 6, 19))
    assert att is not None
    waiting = {
        "account_name": ACCOUNT,
        "broker_slug": "fidelity",
        "connection_broken_at": None,
        "first_sync_completed": False,
    }
    assert sync_status_label(waiting) == "Waiting for the first sync"
    assert "Reconnect needed" not in (sync_status_label(waiting) or "")


def test_run_sync_drops_a_flagged_fidelity_sweep_before_the_seed(monkeypatch):
    from app import snaptrade as snap

    fresh = date.today().strftime("%Y-%m-%dT12:00:00Z")
    monkeypatch.setattr(snap, "_ensure_snaptrade_tenant_id", lambda **k: TENANT)
    monkeypatch.setattr(snap, "_brokerage_authorization_disabled", lambda *a, **k: False)
    monkeypatch.setattr(snap, "_fetch_activities", lambda *a, **k: [
        _act("BUY", "SPAXX", units=100, price=1, amount=100),
        _act("DIVIDEND", "SPAXX", units=100, price=1, amount=1),
        _act("BUY", "FXAIX", units=2, price=200, amount=400),
    ])
    monkeypatch.setattr(snap, "_fetch_recent_orders", lambda *a, **k: [])
    monkeypatch.setattr(snap, "_fetch_account_orders", lambda *a, **k: [])
    monkeypatch.setattr(snap, "_fetch_positions", lambda *a, **k: [{
        "symbol": "SPAXX",
        "units": 100,
        "price": 1,
        "market_value": 100,
        "cost_basis": 100,
        "cash_equivalent": True,
    }])
    monkeypatch.setattr(snap, "_fetch_option_holdings", lambda *a, **k: [])
    monkeypatch.setattr(snap, "_fetch_balances", lambda *a, **k: [{"cash": 100}])
    monkeypatch.setattr(snap, "_fetch_account_summary", lambda *a, **k: {
        "sync_status": {"holdings": {"last_successful_sync": fresh}},
        "balance": {"total": {"amount": 500}},
    })
    monkeypatch.setattr(snap, "record_snaptrade_account_identity", lambda *a, **k: None)
    result = snap._run_sync(
        21, object(),
        snap={"snaptrade_user_id": "u", "snaptrade_secret": "s"},
        acc_row={
            "snaptrade_account_id": "fid",
            "account_name": ACCOUNT,
            "broker_slug": "fidelity",
            "tenant_id": TENANT,
        },
        lookback_days=60,
        defer_push=True,
    )
    history = result["history_df"]
    assert ("Buy", "SPAXX") not in list(zip(history["Action"], history["Symbol"]))
    assert ("Cash Dividend", "SPAXX") in list(zip(history["Action"], history["Symbol"]))
    assert ("Buy", "FXAIX") in list(zip(history["Action"], history["Symbol"]))
    current = result["current_df"]
    sweep = current[current["Symbol"] == "SPAXX"].iloc[0]
    assert sweep["security_type"] == "Cash and Money Market"


class _User:
    is_authenticated = True
    is_active = True
    is_anonymous = False
    id = 21
    username = "fidelity-new"

    def get_id(self):
        return "21"


class _EmptyJob:
    def to_dataframe(self, **_kwargs):
        return pd.DataFrame()


class _EmptyClient:
    def query(self, *_args, **_kwargs):
        return _EmptyJob()


def test_empty_fidelity_book_renders_positions_overview_and_strategies():
    from app import app
    import app.positions_page as positions_page
    import app.strategies as strategies
    import app.weekly_review as weekly_review

    previous = (
        app.config.get("TESTING"),
        app.config.get("PROPAGATE_EXCEPTIONS"),
    )
    app.config["TESTING"] = True
    app.config["PROPAGATE_EXCEPTIONS"] = True
    client = _EmptyClient()
    try:
        with patch.object(positions_page, "current_user", _User()), \
             patch("flask_login.utils._get_user", lambda: _User()), \
             patch.object(positions_page, "_redirect_if_no_accounts", lambda: None), \
             patch.object(positions_page, "_user_account_list", lambda: [ACCOUNT]), \
             patch.object(positions_page, "_tenants_for_scope", lambda *_a, **_k: [TENANT]), \
             patch.object(positions_page, "is_admin", lambda _u: False), \
             patch.object(positions_page, "get_bigquery_client", lambda: client):
            positions = app.test_client().get("/positions")
        assert positions.status_code == 200, positions.data[:400]
        positions_html = positions.get_data(as_text=True)
        assert "Something went wrong" not in positions_html
        assert "No positions to show yet" in positions_html

        with patch.object(strategies, "current_user", _User()), \
             patch("flask_login.utils._get_user", lambda: _User()), \
             patch("app.routes._redirect_if_no_accounts", lambda: None), \
             patch.object(strategies, "_user_account_list", lambda: [ACCOUNT]), \
             patch.object(strategies, "_tenants_for_scope", lambda *_a, **_k: [TENANT]), \
             patch.object(strategies, "get_bigquery_client", lambda: client):
            strategy = app.test_client().get("/strategies")
        assert strategy.status_code == 200, strategy.data[:500]
        strategy_html = strategy.get_data(as_text=True)
        assert "Something went wrong" not in strategy_html
        assert "Strategies" in strategy_html

        with patch.object(weekly_review, "current_user", _User()), \
             patch("flask_login.utils._get_user", lambda: _User()), \
             patch("app.routes._redirect_if_no_accounts", lambda: None), \
             patch.object(weekly_review, "_user_account_list", lambda: [ACCOUNT]), \
             patch.object(weekly_review, "_tenants_for_scope", lambda *_a, **_k: [TENANT]), \
             patch.object(weekly_review, "get_user_profile", lambda _uid: {"timezone": "America/New_York"}), \
             patch.object(weekly_review, "get_bigquery_client", lambda: client):
            overview = app.test_client().get("/overview")
        assert overview.status_code == 200, overview.data[:800]
        overview_html = overview.get_data(as_text=True)
        assert "Something went wrong" not in overview_html
        assert 'id="ov-happened"' in overview_html
    finally:
        app.config["TESTING"] = previous[0]
        app.config["PROPAGATE_EXCEPTIONS"] = previous[1]


def test_empty_fidelity_settings_waits_for_the_first_sync(monkeypatch):
    from app import app
    from app.models import User
    from tests.test_onboarding_settings_audit import _login, _stub_profile_reads

    account = {
        "snaptrade_account_id": "fid",
        "tenant_id": TENANT,
        "account_name": ACCOUNT,
        "display_nickname": "Fidelity IRA",
        "broker_slug": "Fidelity",
        "connection_broken_at": None,
        "first_sync_completed": False,
        "last_sync_error": None,
    }
    _stub_profile_reads(monkeypatch, [account])
    monkeypatch.setattr(
        "app.profile_page.get_broker_tenants_for_user",
        lambda uid: [{"tenant_id": TENANT, "account_name": ACCOUNT}],
    )
    client = app.test_client()
    user = _login(client, monkeypatch, username="fidelity-new")
    assert isinstance(user, User) or user.username == "fidelity-new"
    html = client.get("/profile").get_data(as_text=True)
    assert "Waiting for the first sync" in html
    assert "Reconnect needed" not in html
    assert "Something went wrong" not in html
