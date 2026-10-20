"""Empower readiness fixtures. Example numbers only. No warehouse rows.

The shared SnapTrade normalizer already maps the activity types Schwab
and Vanguard use. Empower-specific gaps stay dropped: a fund exchange
or a share transfer is not rewritten into a Buy, and a holding with no
broker cost does not borrow the market price as its basis.
"""
from __future__ import annotations

import os

os.environ.setdefault("HAPPYTRADER_SKIP_DB_INIT", "1")

from app.early_broker import broker_key, display_name, is_modeled_broker
from app.snaptrade_normalize import (
    activities_to_history_df,
    balances_to_balance_df,
    positions_to_current_df,
)

TENANT = "snaptrade:empower-fixture"
ACCOUNT = "Empower Retirement ••••4242"


def _act(atype, symbol="", **extra):
    row = {
        "type": atype,
        "symbol": symbol,
        "units": extra.pop("units", 1),
        "price": extra.pop("price", 10),
        "amount": extra.pop("amount", 10),
        "trade_date": extra.pop("trade_date", "2026-10-01"),
        "fee": extra.pop("fee", 0),
        "description": extra.pop("description", ""),
    }
    row.update(extra)
    return row


def test_empower_account_label_routes_on_the_first_word():
    from app import snaptrade as snap

    assert snap._stable_account_name("EMPOWER", "12345678") == "Empower ••••5678"
    assert snap._stable_account_name("Empower", "ABCD") == "Empower Account"
    labeled = snap._stable_account_name("Empower Retirement", "99887766")
    assert labeled.split()[0] == "Empower"
    assert labeled.endswith("••••7766")
    assert broker_key("Empower") == "empower"
    assert broker_key("EMPOWER RETIREMENT") == "empower"
    assert broker_key("Empower-Retirement") == "empower"
    assert is_modeled_broker("Empower") is True
    assert is_modeled_broker("Empower Retirement") is True
    assert display_name("empower") == "Empower"
    # Warehouse slug is the first whitespace token of the display label.
    # A hyphenated institution is one token and is not aliased here.
    hyphen = snap._stable_account_name("Empower-Retirement", "11112222")
    assert hyphen.split()[0] == "Empower-Retirement"


def test_unknown_exchange_is_dropped_instead_of_a_buy():
    history = activities_to_history_df(
        [_act("EXCHANGE", "SYNTH", units=12, price=25, amount=-300)],
        account_name=ACCOUNT,
        user_id=7,
        tenant_id=TENANT,
    )
    assert history.empty


def test_share_transfer_is_dropped_instead_of_an_opening_buy():
    history = activities_to_history_df(
        [_act("TRANSFER", "SYNTH", units=40, price=15, amount=0)],
        account_name=ACCOUNT,
        user_id=7,
        tenant_id=TENANT,
    )
    assert history.empty


def test_contribution_is_a_positive_deposit():
    history = activities_to_history_df(
        [_act("CONTRIBUTION", "", units=0, price=0, amount=-250)],
        account_name=ACCOUNT,
        user_id=7,
        tenant_id=TENANT,
    )
    assert len(history) == 1
    row = history.iloc[0]
    assert row["Action"] == "Deposit"
    assert float(row["Amount"]) == 250
    assert row["tenant_id"] == TENANT
    assert str(row["Account"]).split()[0] == "Empower"


def test_equity_buy_stays_a_buy():
    history = activities_to_history_df(
        [_act("BUY", "SYNTH", units=3, price=20, amount=-60, fee=1)],
        account_name=ACCOUNT,
        user_id=7,
        tenant_id=TENANT,
    )
    assert list(history["Action"]) == ["Buy"]
    assert float(history.iloc[0]["Amount"]) < 0


def test_holding_without_broker_cost_does_not_invent_a_basis():
    """Example numbers. Quote is 40. No cost field, so cost stays 0.

    Unrealized is left blank. 0 minus a made-up basis is not a loss,
    and no history row is synthesized for the shares.
    """
    position = {
        "symbol": {"symbol": "SYNTH", "description": "Synthetic Fund"},
        "units": 10,
        "price": 40,
        "market_value": 400,
    }
    current = positions_to_current_df(
        [position], account_name=ACCOUNT, user_id=7, tenant_id=TENANT,
    )
    assert len(current) == 1
    row = current.iloc[0]
    assert row["Symbol"] == "SYNTH"
    assert row["security_type"] == "Equity"
    assert float(row["cost_bases"]) == 0
    assert float(row["market_value"]) == 400
    assert row["gain_or_loss_dollat"] == ""

    history = activities_to_history_df(
        [], account_name=ACCOUNT, user_id=7, tenant_id=TENANT,
    )
    assert history.empty


def test_reported_account_total_is_not_rebuilt_from_holdings():
    """Example numbers. SnapTrade total is 1000. Holdings sum to 400.

    The balance row keeps the reported total. It does not add the
    holding again.
    """
    position = {
        "symbol": "SYNTH",
        "units": 10,
        "price": 40,
        "market_value": 400,
    }
    balances = balances_to_balance_df(
        account_summary={"balance": {"total": {"amount": 1000}}},
        balances=[{"cash": 600}],
        positions=[position],
        account_name=ACCOUNT,
        user_id=7,
        tenant_id=TENANT,
    )
    total = balances.loc[balances["row_type"] == "account_total"].iloc[0]
    cash = balances.loc[balances["row_type"] == "cash"].iloc[0]
    assert float(total["market_value"]) == 1000
    assert float(cash["market_value"]) == 600
    assert total["tenant_id"] == TENANT
