"""SnapTrade normalize layer — unit tests.

These tests pin every SnapTrade contract translation: action vocabulary
mapping, OSI option-symbol formatting, sign conventions on history,
qty * Price == market_value invariant on positions, and the cash +
account_total balance row shape.

NO network calls, NO Postgres. Pure-data tests so they run in <1s.
"""
from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
import pytest

from app.snaptrade_normalize import (
    SNAPTRADE_ACTIVITY_TO_ACTION,
    activities_to_history_df,
    balances_to_balance_df,
    merge_holdings_with_all_positions,
    orders_to_history_df,
    position_holding_key,
    positions_to_current_df,
    snaptrade_symbol_to_osi,
    symbols_already_in_cash,
)
from app.upload import (
    BALANCE_SEED_COLUMNS,
    CURRENT_SEED_COLUMNS,
    HISTORY_SEED_COLUMNS,
)

TENANT_SNAPTRADE = "snaptrade:bed78305-a764-4c4d-b4c7-fe59e391f661"


# ---------------------------------------------------------------------------
# Action vocabulary — every key must round-trip through the existing
# stg_history.sql action CASE statement (we assert against the canonical
# CSV-export labels stg_history understands).
# ---------------------------------------------------------------------------


def test_action_map_emits_only_strings_or_none():
    """Type contract: every value is either a str (legal CSV-export
    label) or None (deliberately drop this activity)."""
    for k, v in SNAPTRADE_ACTIVITY_TO_ACTION.items():
        assert isinstance(k, str) and k == k.upper(), f"{k!r} should be UPPERCASE"
        assert v is None or isinstance(v, str), f"{k}: {v!r} not str|None"


def test_action_map_covers_every_stg_history_label_we_emit():
    """Every CSV-export label we emit (from the action map AND from
    ``_resolve_option_action``) must appear in stg_history.sql's
    ``case lower(trim(action))`` block. If you add a new mapping
    value, double-check stg_history maps it too — a typo here silently
    buckets every event under ``other``."""
    from app.snaptrade_normalize import _resolve_option_action

    expected = {
        "Buy", "Sell",
        "Buy to Open", "Buy to Close", "Sell to Open", "Sell to Close",
        "Expired", "Assigned", "Exchange or Exercise",
        "Cash Dividend", "Qualified Dividend",
        "Credit Interest", "Margin Interest", "ADR Mgmt Fee",
        # External cash movements folded to action='cash_transfer'.
        "Deposit", "Withdrawal",
    }
    emitted = {v for v in SNAPTRADE_ACTIVITY_TO_ACTION.values() if v is not None}
    # Pull every option action the resolver can possibly produce.
    for canonical in ("Buy", "Sell"):
        for desc in ("buy to open this", "sell to close this", "ambiguous text"):
            emitted.add(_resolve_option_action(canonical, desc))
    missing = expected - emitted
    assert not missing, f"action map missing labels: {missing}"


def test_action_map_captures_unambiguous_cash_movements():
    """DEPOSIT / WITHDRAWAL / CONTRIBUTION are the trader's own money in/out.
    They aren't trades, but they DO move account value, so we capture them
    as a dedicated ``Cash Transfer``-family label (stg_history folds them to
    ``action = 'cash_transfer'``) to power the "exclude deposits &
    withdrawals" toggle. Deposits/contributions read as cash IN, withdrawals
    as cash OUT."""
    assert SNAPTRADE_ACTIVITY_TO_ACTION["DEPOSIT"] == "Deposit"
    assert SNAPTRADE_ACTIVITY_TO_ACTION["CONTRIBUTION"] == "Deposit"
    assert SNAPTRADE_ACTIVITY_TO_ACTION["WITHDRAWAL"] == "Withdrawal"
    assert SNAPTRADE_ACTIVITY_TO_ACTION["INTERNAL_CASH_TRANSFER_IN"] == "Deposit"
    assert SNAPTRADE_ACTIVITY_TO_ACTION["INTERNAL_CASH_TRANSFER_OUT"] == "Withdrawal"


def test_action_map_still_drops_ambiguous_or_share_movements():
    """TRANSFER and share JOURNAL stay off this map. SPLIT is handled
    out-of-band by current_position_stock_price.py + stg_split_events.
    DISTRIBUTION and cash-only JOURNAL are resolved at read time: a
    DISTRIBUTION with a ticker is a dividend, and one without is a
    withdrawal. They are not given a single map label."""
    for cash_kind in ("TRANSFER", "JOURNAL", "DISTRIBUTION", "SPLIT", "STOCKSPLIT"):
        assert SNAPTRADE_ACTIVITY_TO_ACTION[cash_kind] is None


# ---------------------------------------------------------------------------
# OSI option-symbol formatting — output MUST match the regex anchor in
# stg_history.sql: ``r'(\d{6}[CP]\d{8})'``. A 21-char total with 6-char
# left-padded underlying.
# ---------------------------------------------------------------------------


def test_snaptrade_symbol_to_osi_call():
    osi = snaptrade_symbol_to_osi({
        "option_symbol": {
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-01-19",
            "strike_price": 150,
            "option_type": "CALL",
        }
    })
    assert osi == "AAPL  260119C00150000"
    assert len(osi) == 21


def test_snaptrade_symbol_to_osi_put_with_decimal_strike():
    osi = snaptrade_symbol_to_osi({
        "option_symbol": {
            "underlying_symbol": "SPY",
            "expiration_date": "2026-12-18",
            "strike_price": 487.5,
            "option_type": "PUT",
        }
    })
    assert osi == "SPY   261218P00487500"


def test_snaptrade_symbol_to_osi_handles_iso_datetime():
    osi = snaptrade_symbol_to_osi({
        "option_symbol": {
            "underlying_symbol": "QQQ",
            "expiration_date": "2026-06-21T00:00:00Z",
            "strike_price": 400,
            "option_type": "C",  # short form
        }
    })
    assert osi == "QQQ   260621C00400000"


def test_snaptrade_symbol_to_osi_truncates_long_underlying():
    """OSI is fixed at 6 chars for the underlying; longer tickers get
    truncated. (Real US tickers cap at 5; defensive nonetheless.)"""
    osi = snaptrade_symbol_to_osi({
        "option_symbol": {
            "underlying_symbol": "BERKSHIRE",
            "expiration_date": "2026-01-19",
            "strike_price": 100,
            "option_type": "CALL",
        }
    })
    assert osi.startswith("BERKSH")
    assert len(osi) == 21


def test_snaptrade_symbol_to_osi_falls_back_for_equity():
    """Equity rows return the bare ticker (no option_symbol)."""
    osi = snaptrade_symbol_to_osi({
        "raw_symbol": "JEPI",
    })
    assert osi == "JEPI"


def test_snaptrade_symbol_to_osi_handles_nested_symbol_object():
    """SnapTrade some endpoints return ``{"symbol": {"raw_symbol": ...}}``."""
    osi = snaptrade_symbol_to_osi({
        "symbol": {"raw_symbol": "TSLA"}
    })
    assert osi == "TSLA"


def test_snaptrade_symbol_to_osi_returns_empty_for_unknown_shape():
    assert snaptrade_symbol_to_osi(None) == ""
    assert snaptrade_symbol_to_osi({}) == ""


# ---------------------------------------------------------------------------
# activities_to_history_df — sign convention, schema shape, drop rules
# ---------------------------------------------------------------------------


def _buy_activity(symbol="JEPI", units=10, price=55, amount=-550, trade_date="2026-05-11"):
    return {
        "type": "BUY",
        "symbol": {"raw_symbol": symbol, "description": symbol},
        "units": units,
        "price": price,
        "amount": amount,
        "trade_date": trade_date,
        "fee": 0,
    }


def _sell_activity(symbol="JEPI", units=5, price=60, amount=300, trade_date="2026-05-12"):
    return {
        "type": "SELL",
        "symbol": {"raw_symbol": symbol, "description": symbol},
        "units": units,
        "price": price,
        "amount": amount,
        "trade_date": trade_date,
        "fee": 0,
    }


def test_history_df_has_canonical_seed_columns():
    df = activities_to_history_df(
        [_buy_activity()], account_name="Sara Investment", user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert list(df.columns) == HISTORY_SEED_COLUMNS


def test_history_df_stamps_tenant_id_on_every_row():
    df = activities_to_history_df(
        [_buy_activity()], account_name="Sara Investment", user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert (df["tenant_id"] == TENANT_SNAPTRADE).all()


def test_history_df_stamps_account_and_user_id_on_every_row():
    """broker-sync-safety invariant: every row in a history seed must
    carry ``account_name`` AND ``user_id`` so the merge boundary's
    tenant scope works. Empty user_id would fall into the legacy
    leniency branch."""
    df = activities_to_history_df(
        [_buy_activity(), _sell_activity()],
        account_name="Sara Investment",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert (df["Account"] == "Sara Investment").all()
    assert (df["user_id"] == 9).all()


def test_history_df_signs_buy_negative_and_sell_positive():
    """The sign convention enforced here mirrors stg_history.sql's
    ``amount_signed`` CTE so two re-syncs of the same trade dedup
    cleanly. A Buy must always be negative cash; a Sell positive."""
    df = activities_to_history_df(
        [_buy_activity(amount=-550), _sell_activity(amount=300)],
        account_name="Sara Investment",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    buy_amount = df.loc[df["Action"] == "Buy", "Amount"].iloc[0]
    sell_amount = df.loc[df["Action"] == "Sell", "Amount"].iloc[0]
    assert buy_amount == -550
    assert sell_amount == 300


def test_history_df_re_signs_buy_when_broker_reports_positive_amount():
    """If a broker via SnapTrade ships a buy with a positive amount
    (some Vanguard quirks), our normalizer must re-sign it negative
    before the seed merge so the second sync doesn't duplicate the row."""
    a = _buy_activity(amount=550)  # WRONG sign from upstream
    df = activities_to_history_df([a], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert df.loc[0, "Amount"] == -550


def test_history_df_drops_unknown_activity_types():
    activities = [
        _buy_activity(),
        {"type": "ALIEN_TRANSFER_FROM_MARS", "units": 1, "price": 1, "amount": 1},
    ]
    df = activities_to_history_df(activities, account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert len(df) == 1
    assert df.iloc[0]["Action"] == "Buy"


def test_history_df_captures_deposits_and_withdrawals_with_signed_amount():
    """Deposits/withdrawals now land in the seed as Deposit/Withdrawal rows
    (folded to ``cash_transfer`` in stg_history) with a sign-correct Amount:
    deposits cash IN (+), withdrawals cash OUT (−). Share-movement/ambiguous
    types (JOURNAL with no cash) still drop silently."""
    activities = [
        _buy_activity(),
        {"type": "DEPOSIT", "amount": 1000, "trade_date": "2026-05-11"},
        {"type": "WITHDRAWAL", "amount": 500, "trade_date": "2026-05-11"},
        {"type": "JOURNAL", "amount": 0, "trade_date": "2026-05-11"},
    ]
    df = activities_to_history_df(activities, account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    # buy + deposit + withdrawal survive; journal is dropped.
    assert len(df) == 3
    dep = df[df["Action"] == "Deposit"]
    wd = df[df["Action"] == "Withdrawal"]
    assert len(dep) == 1 and len(wd) == 1
    assert float(dep.iloc[0]["Amount"]) == 1000.0
    # Withdrawal is cash OUT regardless of the broker-reported sign.
    assert float(wd.iloc[0]["Amount"]) == -500.0


def test_history_df_cash_only_journal_maps_to_deposit():
    df = activities_to_history_df(
        [{"type": "JOURNAL", "amount": 500, "trade_date": "2025-01-06",
          "description": "JOURNAL FRM ...852"}],
        account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE,
    )
    assert len(df) == 1
    assert df.iloc[0]["Action"] == "Deposit"
    assert float(df.iloc[0]["Amount"]) == 500.0


def test_history_df_share_journal_still_drops():
    df = activities_to_history_df(
        [{"type": "JOURNAL", "amount": 500, "units": 10, "trade_date": "2025-01-06",
          "symbol": {"symbol": "IYW", "raw_symbol": "IYW"}}],
        account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE,
    )
    assert df.empty


def test_history_df_withdrawal_negative_even_if_broker_reports_positive():
    """We never trust the broker's signed amount for direction — a
    withdrawal reported as +500 must still be −500 (cash out)."""
    df = activities_to_history_df(
        [{"type": "WITHDRAWAL", "amount": 500, "trade_date": "2026-05-11"}],
        account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE,
    )
    assert float(df.iloc[0]["Amount"]) == -500.0


def test_history_df_contribution_maps_to_deposit_cash_in():
    df = activities_to_history_df(
        [{"type": "CONTRIBUTION", "amount": 250, "trade_date": "2026-05-11"}],
        account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE,
    )
    assert df.iloc[0]["Action"] == "Deposit"
    assert float(df.iloc[0]["Amount"]) == 250.0


def test_history_df_internal_cash_transfer_maps_to_deposit_and_withdrawal():
    """Cash moving between the trader's own accounts is a deposit on the
    receiving side and a withdrawal on the sending side. Dropping these
    as unknown types made the exclude-transfers toggle a no-op."""
    df = activities_to_history_df(
        [
            {"type": "INTERNAL_CASH_TRANSFER_IN", "amount": 200, "trade_date": "2026-05-11"},
            {"type": "INTERNAL_CASH_TRANSFER_OUT", "amount": 200, "trade_date": "2026-05-11"},
        ],
        account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE,
    )
    assert len(df) == 2
    dep = df[df["Action"] == "Deposit"]
    wd = df[df["Action"] == "Withdrawal"]
    assert len(dep) == 1 and len(wd) == 1
    assert float(dep.iloc[0]["Amount"]) == 200.0
    assert float(wd.iloc[0]["Amount"]) == -200.0


def _fund_symbol(ticker="FUNDX"):
    return {
        "raw_symbol": ticker,
        "symbol": ticker,
        "description": "Example open-end fund",
        "type": {"code": "oef", "description": "Open-Ended Fund"},
    }


def test_history_df_string_symbol_is_a_ticker():
    """A bare-string ticker must not raise, and must keep the symbol."""
    df = activities_to_history_df(
        [{
            "type": "BUY",
            "symbol": "FUNDX",
            "units": 2,
            "price": 50,
            "amount": -100,
            "trade_date": "2026-05-11",
        }],
        account_name="Vanguard Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert len(df) == 1
    assert df.iloc[0]["Symbol"] == "FUNDX"
    assert df.iloc[0]["Action"] == "Buy"
    assert float(df.iloc[0]["Amount"]) == -100


def test_history_df_rei_matching_cash_emits_dividend_and_buy():
    """Example numbers, not a customer book.

    A reinvestment of 2 shares at $50 is $100 of dividend income and a
    $100 share purchase. The dividend row stays Cash Dividend so a
    later sync dedups against rows already stored that way.
    """
    df = activities_to_history_df(
        [{
            "type": "REI",
            "symbol": _fund_symbol(),
            "units": 2,
            "price": 50,
            "amount": 100,
            "trade_date": "2026-05-11",
            "description": "Dividend reinvestment",
        }],
        account_name="Vanguard Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert list(df["Action"]) == ["Cash Dividend", "Buy"]
    div, buy = df.iloc[0], df.iloc[1]
    assert div["Symbol"] == "FUNDX"
    assert float(div["Quantity"]) == 2
    assert float(div["Price"]) == 50
    assert float(div["Amount"]) == 100
    assert buy["Symbol"] == "FUNDX"
    assert float(buy["Quantity"]) == 2
    assert float(buy["Price"]) == 50
    assert float(buy["Amount"]) == -100
    assert buy["fees_and_comm"] == ""


def test_history_df_rei_share_balance_does_not_invent_a_buy():
    """Example numbers. units=400 is the lot that received $10, not
    400 shares bought at $50. That must stay a dividend only.
    """
    df = activities_to_history_df(
        [{
            "type": "REI",
            "symbol": "FUNDX",
            "units": 400,
            "price": 50,
            "amount": 10,
            "trade_date": "2026-05-11",
        }],
        account_name="Vanguard Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert list(df["Action"]) == ["Cash Dividend"]
    assert float(df.iloc[0]["Amount"]) == 10


def test_history_df_plain_dividend_does_not_emit_a_buy():
    """DIVIDEND units are the shares that received the cash."""
    df = activities_to_history_df(
        [{
            "type": "DIVIDEND",
            "symbol": "FUNDX",
            "units": 400,
            "price": 50,
            "amount": 10,
            "trade_date": "2026-05-11",
        }],
        account_name="Vanguard Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert list(df["Action"]) == ["Cash Dividend"]


def test_history_df_rei_skips_companion_when_same_day_buy_exists():
    """Example numbers. REI plus a BUY of the same 2 shares is one lot."""
    df = activities_to_history_df(
        [
            {
                "type": "REI",
                "symbol": "FUNDX",
                "units": 2,
                "price": 50,
                "amount": 100,
                "trade_date": "2026-05-11",
            },
            {
                "type": "BUY",
                "symbol": "FUNDX",
                "units": 2,
                "price": 50.01,
                "amount": -100,
                "trade_date": "2026-05-11",
            },
        ],
        account_name="Vanguard Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    buys = df[df["Action"] == "Buy"]
    divs = df[df["Action"] == "Cash Dividend"]
    assert len(divs) == 1
    assert len(buys) == 1
    assert float(buys.iloc[0]["Amount"]) == -100


def test_history_df_substitute_dividend_is_income():
    df = activities_to_history_df(
        [{
            "type": "SUBSTITUTE_DIVIDEND",
            "symbol": "FUNDX",
            "amount": 15,
            "trade_date": "2026-05-11",
        }],
        account_name="Vanguard Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert df.iloc[0]["Action"] == "Cash Dividend"
    assert float(df.iloc[0]["Amount"]) == 15


def test_history_df_fee_is_cash_out():
    df = activities_to_history_df(
        [{
            "type": "FEE",
            "amount": 3.5,
            "trade_date": "2026-05-11",
            "description": "Account fee",
        }],
        account_name="Vanguard Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert df.iloc[0]["Action"] == "ADR Mgmt Fee"
    assert float(df.iloc[0]["Amount"]) == -3.5


def test_history_df_occ_string_option_symbol():
    df = activities_to_history_df(
        [{
            "type": "SELL",
            "description": "Sell to open",
            "option_type": "SELL_TO_OPEN",
            "symbol": None,
            "option_symbol": "FUNDX 260618C00100000",
            "units": 1,
            "price": 1.25,
            "amount": 125,
            "trade_date": "2026-05-11",
        }],
        account_name="Vanguard Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert len(df) == 1
    assert df.iloc[0]["Action"] == "Sell to Open"
    assert df.iloc[0]["Symbol"] == "FUNDX 260618C00100000"


def test_history_df_emits_osi_for_options():
    """Option activities emit the canonical OSI string in Symbol.
    SnapTrade collapses every option open/close to BUY/SELL on the
    activities feed; we use the broker's description to disambiguate
    open vs close."""
    act = {
        "type": "SELL",
        "description": "SELL TO OPEN AAPL 06/21/26 150 CALL",
        "symbol": {
            "raw_symbol": "AAPL",
            "option_symbol": {
                "underlying_symbol": "AAPL",
                "expiration_date": "2026-06-21",
                "strike_price": 150,
                "option_type": "CALL",
            },
        },
        "units": 1,
        "price": 3.50,
        "amount": 350,
        "trade_date": "2026-05-11",
        "fee": 0,
    }
    df = activities_to_history_df([act], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert df.iloc[0]["Symbol"] == "AAPL  260621C00150000"
    assert df.iloc[0]["Action"] == "Sell to Open"


def test_history_df_handles_top_level_option_symbol_with_null_symbol():
    """Schwab via SnapTrade ships OPTION activities with ``symbol: null``
    and the structured contract at the ACTIVITY top level
    (``act["option_symbol"]``), with ``underlying_symbol`` itself a nested
    object. Pre-fix this misclassified every option as empty-symbol equity
    and the entire option lane vanished from the warehouse (hundreds of
    contracts per account). Regression for the real payload shape."""
    act = {
        "id": "a9bfbecd",
        "symbol": None,
        "option_symbol": {
            "ticker": "PLTR  260508C00147000",
            "strike_price": 147.0,
            "expiration_date": "2026-05-08",
            "underlying_symbol": {"symbol": "PLTR", "raw_symbol": "PLTR"},
            "option_type": "CALL",
        },
        "type": "OPTIONEXPIRATION",
        "description": "CALL PALANTIR TECHNOLOGI$147 EXP 05/08/26",
        "units": 2.0,
        "price": 0.0,
        "amount": 0.0,
        "trade_date": "2026-05-11",
        "fee": 0.0,
    }
    df = activities_to_history_df([act], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert len(df) == 1
    assert df.iloc[0]["Symbol"] == "PLTR  260508C00147000"
    assert df.iloc[0]["Action"] == "Expired"


def test_history_df_prefers_explicit_option_type_over_description():
    """SnapTrade's activity-level ``option_type`` field is authoritative
    for open/close. Schwab descriptions carry NO open/close hint
    (``"CALL ORACLE CORP $200 EXP 06/18/26"``), so a SELL_TO_CLOSE must be
    tagged "Sell to Close" from the field, NOT defaulted to "Sell to Open"
    by description parsing. Regression for the ORCL round-trip that showed
    as a phantom open Naked Call worth $126K."""
    act = {
        "type": "SELL",
        "option_type": "SELL_TO_CLOSE",
        "description": "CALL ORACLE CORP $200 EXP 06/18/26",
        "symbol": None,
        "option_symbol": {
            "ticker": "ORCL  260618C00200000",
            "strike_price": 200.0,
            "expiration_date": "2026-06-18",
            "underlying_symbol": {"symbol": "ORCL"},
            "option_type": "CALL",
        },
        "units": -30,
        "price": 46.85,
        "amount": 140527.14,
        "trade_date": "2026-06-02",
        "fee": 0,
    }
    df = activities_to_history_df([act], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert df.iloc[0]["Symbol"] == "ORCL  260618C00200000"
    assert df.iloc[0]["Action"] == "Sell to Close"


def test_history_df_resolves_option_close_from_description():
    """A SELL canonical type with a "Buy to Close" description must
    NOT be tagged as Sell to Open. (Confused trade direction shows
    up as wrong premium attribution downstream.)"""
    act = {
        "type": "BUY",
        "description": "BUY TO CLOSE TSLA 12/19/25 200 CALL",
        "symbol": {
            "raw_symbol": "TSLA",
            "option_symbol": {
                "underlying_symbol": "TSLA",
                "expiration_date": "2025-12-19",
                "strike_price": 200,
                "option_type": "CALL",
            },
        },
        "units": 1,
        "price": 1.20,
        "amount": -120,
        "trade_date": "2026-05-11",
        "fee": 0,
    }
    df = activities_to_history_df([act], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert df.iloc[0]["Action"] == "Buy to Close"


def test_history_df_defaults_options_to_open_when_description_silent():
    """When the broker description doesn't contain "to close" / "to
    open", we default to OPEN. Documented limitation — at least the
    event lands in stg_history's option lane, vs being silently
    dropped or routed through the equity lane."""
    act = {
        "type": "BUY",
        "description": "OPTION ASSIGNMENT — INTRADAY ROLL",  # ambiguous
        "symbol": {
            "raw_symbol": "AAPL",
            "option_symbol": {
                "underlying_symbol": "AAPL",
                "expiration_date": "2026-06-21",
                "strike_price": 150,
                "option_type": "CALL",
            },
        },
        "units": 1,
        "price": 3.50,
        "amount": -350,
        "trade_date": "2026-05-11",
        "fee": 0,
    }
    df = activities_to_history_df([act], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert df.iloc[0]["Action"] == "Buy to Open"


def test_history_df_returns_canonical_columns_when_empty():
    df = activities_to_history_df([], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert list(df.columns) == HISTORY_SEED_COLUMNS
    assert len(df) == 0


# ---------------------------------------------------------------------------
# positions_to_current_df — qty * Price == market_value invariant
# ---------------------------------------------------------------------------


def test_current_df_has_canonical_seed_columns():
    df = positions_to_current_df(
        [{"symbol": {"raw_symbol": "JEPI"}, "units": 100, "market_value": 5500, "cost_basis": 5300}],
        account_name="Sara Investment", user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert list(df.columns) == CURRENT_SEED_COLUMNS


def test_current_df_equity_price_satisfies_qty_times_price_invariant():
    """The structural invariant ``int_enriched_current_equity_price_consistent.sql``
    requires ``abs(qty * current_price - market_value) <= $0.01`` for
    every equity row. Our derive-Price-from-MV/qty rule must produce
    a Price that satisfies this even when SnapTrade ships a stale
    per-share field."""
    pos = {
        "symbol": {"raw_symbol": "JEPI"},
        "units": 100,
        "market_value": 5500.50,
        "cost_basis": 5300,
        "price": 9999.99,  # broker-reported nonsense — must NOT win
    }
    df = positions_to_current_df([pos], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    qty = float(df.iloc[0]["Quantity"])
    seed_price = float(df.iloc[0]["Price"])
    market_value = float(df.iloc[0]["market_value"])
    assert abs(qty * seed_price - market_value) <= 0.01


def test_current_df_options_keep_per_share_price():
    """Options use per-share-of-underlying premium for the Price column
    (matches the CSV-export semantic that stg_current already
    consumes)."""
    pos = {
        "symbol": {
            "raw_symbol": "AAPL",
            "option_symbol": {
                "underlying_symbol": "AAPL",
                "expiration_date": "2026-06-21",
                "strike_price": 150,
                "option_type": "CALL",
            },
        },
        "units": 1,
        "price": 3.50,
        "market_value": 350,
        "cost_basis": 250,
    }
    df = positions_to_current_df([pos], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert df.iloc[0]["Price"] == 3.50
    assert df.iloc[0]["security_type"] == "Option"


def test_current_df_option_holding_price_per_share_cost_per_contract():
    """Open option holdings from ``list_option_holdings`` ship ``price``
    PER SHARE but ``average_purchase_price`` PER CONTRACT (SnapTrade
    OptionsPosition schema), and NO market_value/cost_basis. So the 100x
    contract multiplier applies to the per-share price when deriving
    market_value, but NOT to the already-per-contract cost basis:

        market_value = |units| * price * 100
        cost_basis   = |units| * average_purchase_price   (no extra ×100)

    Regression for the 2026-07-13 SEI bug: a 10x $70C with a per-contract
    cost of $920.663 (=$9.20663/share) was double-multiplied to a
    $920,663 cost basis → -$913,563 phantom unrealized loss. Here the LITE
    LEAP's per-contract cost is $23,780.66 (=$237.8066/share) for one
    contract."""
    holding = {
        "symbol": {
            "description": "",
            "option_symbol": {
                "ticker": "LITE  261120C01100000",
                "strike_price": 1100.0,
                "expiration_date": "2026-11-20",
                "underlying_symbol": {"symbol": "LITE", "raw_symbol": "LITE"},
                "option_type": "CALL",
            },
        },
        "price": 180.85,               # per SHARE
        "units": 1.0,                  # contracts
        "average_purchase_price": 23780.66,   # per CONTRACT
    }
    df = positions_to_current_df([holding], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    row = df.iloc[0]
    assert row["Symbol"] == "LITE  261120C01100000"
    assert row["security_type"] == "Option"
    assert float(row["Price"]) == 180.85
    # market_value applies ×100 to the per-share price.
    assert abs(float(row["market_value"]) - 18085.0) < 0.01
    # cost_basis is |units| * per-contract cost — NO extra ×100.
    assert abs(float(row["cost_bases"]) - 23780.66) < 0.01
    # Long option: unrealized = market_value - cost_basis.
    assert abs(float(row["gain_or_loss_dollat"]) - (18085.0 - 23780.66)) < 0.01


def test_current_df_option_cost_basis_not_double_multiplied():
    """Direct regression for the 2026-07-13 SEI ×100 cost-basis bug.
    ``average_purchase_price`` is PER CONTRACT, so a 10-contract call with
    a $920.663/contract cost must snapshot a $9,206.63 cost basis — NOT
    $920,663 (which produced -$913,563 phantom unrealized)."""
    holding = {
        "symbol": {
            "option_symbol": {
                "ticker": "SEI   260821C00070000",
                "strike_price": 70.0,
                "expiration_date": "2026-08-21",
                "underlying_symbol": {"symbol": "SEI"},
                "option_type": "CALL",
            },
        },
        "price": 7.1,                        # per SHARE
        "units": 10.0,                       # contracts
        "average_purchase_price": 920.663,   # per CONTRACT ($9.20663/share)
    }
    df = positions_to_current_df([holding], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    row = df.iloc[0]
    assert abs(float(row["market_value"]) - 7100.0) < 0.01
    assert abs(float(row["cost_bases"]) - 9206.63) < 0.01
    assert abs(float(row["gain_or_loss_dollat"]) - (7100.0 - 9206.63)) < 0.01


def test_current_df_short_option_holding_nets_premium_received():
    """A short option holding (negative units) carries a positive
    cost_basis (premium received) and negative market_value (cost to buy
    back); unrealized P&L nets to market_value + cost_basis.
    ``average_purchase_price`` is PER CONTRACT ($148.66 = $1.4866/share)."""
    holding = {
        "symbol": {
            "option_symbol": {
                "ticker": "NVDA  260508C00210000",
                "strike_price": 210.0,
                "expiration_date": "2026-05-08",
                "underlying_symbol": {"symbol": "NVDA"},
                "option_type": "CALL",
            },
        },
        "price": 0.55,                       # per SHARE
        "units": -1.0,                       # contracts (short)
        "average_purchase_price": 148.66,    # per CONTRACT
    }
    df = positions_to_current_df([holding], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    row = df.iloc[0]
    assert abs(float(row["market_value"]) - (-55.0)) < 0.01
    assert abs(float(row["cost_bases"]) - 148.66) < 0.01
    # Short: unrealized = market_value + cost_basis = -55 + 148.66.
    assert abs(float(row["gain_or_loss_dollat"]) - 93.66) < 0.01


def test_current_df_falls_back_to_broker_price_when_market_value_missing():
    """If the broker's snapshot is mid-render (mv=0) we accept their
    per-share field as a last resort. Better than dropping the row."""
    pos = {
        "symbol": {"raw_symbol": "JEPI"},
        "units": 100,
        "market_value": 0,
        "cost_basis": 5300,
        "price": 53.00,
    }
    df = positions_to_current_df([pos], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert float(df.iloc[0]["Price"]) == 53.00


def test_current_df_null_nav_does_not_book_cost_as_a_loss():
    """Example numbers. A missing NAV is not a $0 quote.

    Market value stays 0 so the close-based price ladder can fill it.
    Unrealized stays blank instead of market_value − cost.
    """
    pos = {
        "symbol": _fund_symbol("FUNDX"),
        "units": 10,
        "price": None,
        "market_value": None,
        "average_purchase_price": 40,
        "cash_equivalent": False,
    }
    df = positions_to_current_df(
        [pos], account_name="Vanguard Account", user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    row = df.iloc[0]
    assert row["Symbol"] == "FUNDX"
    assert row["security_type"] == "Equity"
    assert float(row["Quantity"]) == 10
    assert float(row["market_value"]) == 0
    assert float(row["cost_bases"]) == 400
    assert row["gain_or_loss_dollat"] == ""


def test_current_df_cash_equivalent_is_a_sweep_not_a_holding():
    """Example numbers. A settlement fund SnapTrade already counts in
    cash must not also be an equity lot.
    """
    pos = {
        "symbol": {"raw_symbol": "CASHX", "symbol": "CASHX", "description": "Settlement fund"},
        "units": 1000,
        "price": 1,
        "cash_equivalent": True,
    }
    df = positions_to_current_df(
        [pos], account_name="Vanguard Account", user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    row = df.iloc[0]
    assert row["security_type"] == "Cash and Money Market"
    assert float(row["Quantity"]) == 1000
    assert float(row["market_value"]) == 1000
    assert abs(float(row["Quantity"]) * float(row["Price"]) - float(row["market_value"])) <= 0.01


def test_current_df_ignores_deprecated_fractional_units():
    """``fractional_units`` is deprecated. Only ``units`` is stored.

    Example: units 12 and fractional_units 0.25 stores 12, not 12.25.
    units 40 and fractional_units 1.44 stores 40. A null units does
    not fall through to the deprecated field. Alpaca sends the same
    number in both fields and stays that number.
    """
    pos = {
        "symbol": "FUNDX",
        "units": 12,
        "fractional_units": 0.25,
        "price": 80,
        "average_purchase_price": 70,
    }
    df = positions_to_current_df(
        [pos], account_name="Vanguard Account", user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    row = df.iloc[0]
    assert float(row["Quantity"]) == 12
    assert abs(float(row["market_value"]) - 12 * 80) <= 0.01
    assert abs(float(row["cost_bases"]) - 12 * 70) <= 0.01

    doubled = {
        "symbol": "CASHX",
        "units": 40,
        "fractional_units": 1.44,
        "price": 1,
        "average_purchase_price": 1,
    }
    doubled_df = positions_to_current_df(
        [doubled], account_name="Vanguard Account", user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert float(doubled_df.iloc[0]["Quantity"]) == 40

    missing_units = {
        "symbol": "FUNDX",
        "units": None,
        "fractional_units": 0.25,
        "price": 80,
    }
    missing_df = positions_to_current_df(
        [missing_units], account_name="Vanguard Account", user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert float(missing_df.iloc[0]["Quantity"]) == 0


def test_distribution_with_a_ticker_is_income_and_without_is_a_withdrawal():
    """Example dollars. A $40 fund distribution (ticker set) is a dividend.
    A $40 cash-out with no ticker is a withdrawal. Neither is dropped.
    """
    df = activities_to_history_df(
        [
            {
                "type": "DISTRIBUTION",
                "symbol": "FUNDX",
                "units": 10,
                "price": 4,
                "amount": 40,
                "trade_date": "2026-04-01",
            },
            {
                "type": "DISTRIBUTION",
                "symbol": None,
                "units": 0,
                "price": 0,
                "amount": 40,
                "trade_date": "2026-04-02",
            },
        ],
        account_name="Vanguard Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert list(df["Action"]) == ["Cash Dividend", "Withdrawal"]
    assert list(df["Symbol"]) == ["FUNDX", ""]
    assert float(df.iloc[0]["Amount"]) == 40
    assert float(df.iloc[1]["Amount"]) == -40
    assert list(df["Action"]).count("Buy") == 0


def test_share_transfer_in_stays_off_the_tape_and_opening_cost_uses_broker_basis():
    """A share transfer has no cash price, so it is not written as a fill.

    A still-held deficit is priced at the broker's cost per share
    (int_opening_balances). Example: 10 shares are still held and the
    broker cost on those shares is $500, so the inferred opening cash
    is -$500.
    """
    df = activities_to_history_df(
        [
            {
                "type": "TRANSFER",
                "symbol": "FUNDX",
                "units": 10,
                "price": 50,
                "amount": 0,
                "trade_date": "2026-03-02",
            },
            {
                "type": "EXTERNAL_ASSET_TRANSFER_IN",
                "symbol": "FUNDX",
                "units": 10,
                "price": 50,
                "amount": 0,
                "trade_date": "2026-03-02",
            },
        ],
        account_name="Vanguard Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert df.empty
    sql = (
        Path(__file__).resolve().parents[1]
        / "dbt/models/intermediate/int_opening_balances.sql"
    ).read_text()
    assert "broker_cost_basis" in sql
    assert "-n.opening_qty * safe_divide(n.current_cost_basis, n.current_qty)" in sql
    opening_qty, current_qty, current_cost_basis = 10, 10, 500
    assert -opening_qty * (current_cost_basis / current_qty) == -500


def _money_market(symbol, market_value, *, flagged=False, price=1.0):
    return {
        "symbol": symbol,
        "units": market_value,
        "price": price,
        "market_value": market_value,
        "average_purchase_price": 1.0,
        "cost_basis": market_value,
        "cash_equivalent": flagged,
    }


def test_unflagged_money_market_is_not_a_holding_when_cash_already_includes_it():
    """Example: cash $5,200 already includes a $1,000 settlement fund.
    Stock market value is $8,000. Account total $13,200 matches cash
    plus the stock, so the fund is not a second holding.
    """
    fund = _money_market("CASHX", 1000, flagged=False)
    stock = {
        "symbol": "STOCK",
        "units": 80,
        "price": 100,
        "market_value": 8000,
        "cost_basis": 7000,
        "cash_equivalent": False,
    }
    positions = [fund, stock]
    balances = [{"cash": 5200}]
    summary = {"balance": {"total": {"amount": 13200}}}
    inside = symbols_already_in_cash(
        positions, balances=balances, account_summary=summary,
    )
    assert inside == {"CASHX"}
    current = positions_to_current_df(
        positions, account_name="Vanguard Account", user_id=9,
        tenant_id=TENANT_SNAPTRADE, already_in_cash=inside,
    )
    kinds = dict(zip(current["Symbol"], current["security_type"]))
    assert kinds["CASHX"] == "Cash and Money Market"
    assert kinds["STOCK"] == "Equity"
    bal = balances_to_balance_df(
        account_summary=summary, balances=balances, positions=positions,
        account_name="Vanguard Account", user_id=9,
        tenant_id=TENANT_SNAPTRADE, already_in_cash=inside,
    )
    total = bal[bal["row_type"] == "account_total"].iloc[0]
    assert float(total["market_value"]) == 13200
    assert float(total["cost_basis"]) == 7000


def test_unflagged_money_market_stays_a_holding_when_cash_does_not_include_it():
    """Example: cash $4,200 does not include the $1,000 fund. Stock is
    $8,000. Account total $13,200 only matches if the fund is a holding.
    """
    fund = _money_market("CASHX", 1000, flagged=False)
    stock = {
        "symbol": "STOCK",
        "units": 80,
        "price": 100,
        "market_value": 8000,
        "cost_basis": 7000,
        "cash_equivalent": False,
    }
    positions = [fund, stock]
    balances = [{"cash": 4200}]
    summary = {"balance": {"total": {"amount": 13200}}}
    inside = symbols_already_in_cash(
        positions, balances=balances, account_summary=summary,
    )
    assert inside == set()
    current = positions_to_current_df(
        positions, account_name="Vanguard Account", user_id=9,
        tenant_id=TENANT_SNAPTRADE, already_in_cash=inside,
    )
    kinds = dict(zip(current["Symbol"], current["security_type"]))
    assert kinds["CASHX"] == "Equity"
    assert kinds["STOCK"] == "Equity"
    bal = balances_to_balance_df(
        account_summary=summary, balances=balances, positions=positions,
        account_name="Vanguard Account", user_id=9,
        tenant_id=TENANT_SNAPTRADE, already_in_cash=inside,
    )
    total = bal[bal["row_type"] == "account_total"].iloc[0]
    assert float(total["cost_basis"]) == 8000


def test_unflagged_money_market_stays_a_holding_when_the_total_is_missing():
    """Without an account total we cannot prove the cash balance includes
    the fund, so the fund stays a holding.
    """
    fund = _money_market("CASHX", 1000, flagged=False)
    inside = symbols_already_in_cash(
        [fund], balances=[{"cash": 5200}], account_summary={},
    )
    assert inside == set()
    current = positions_to_current_df(
        [fund], account_name="Vanguard Account", user_id=9,
        tenant_id=TENANT_SNAPTRADE, already_in_cash=inside,
    )
    assert current.iloc[0]["security_type"] == "Equity"


def test_priced_mutual_fund_is_not_treated_as_cash():
    """A fund at a market price is not a money-market sweep, even when
    the account total happens to match cash alone.
    """
    fund = {
        "symbol": "FUNDX",
        "units": 10,
        "price": 80,
        "market_value": 800,
        "cost_basis": 700,
        "cash_equivalent": False,
    }
    inside = symbols_already_in_cash(
        [fund],
        balances=[{"cash": 1000}],
        account_summary={"balance": {"total": {"amount": 1000}}},
    )
    assert inside == set()


def test_buy_of_a_fund_already_in_cash_is_not_an_equity_lot():
    """Example: a $1,000 sweep buy is skipped. A $4 dividend on it stays."""
    df = activities_to_history_df(
        [
            {
                "type": "BUY",
                "symbol": "CASHX",
                "units": 1000,
                "price": 1,
                "amount": -1000,
                "trade_date": "2026-04-01",
            },
            {
                "type": "DIVIDEND",
                "symbol": "CASHX",
                "units": 1000,
                "amount": 4,
                "trade_date": "2026-04-02",
            },
        ],
        account_name="Vanguard Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
        cash_equivalent_symbols={"CASHX"},
    )
    assert list(df["Action"]) == ["Cash Dividend"]
    assert float(df.iloc[0]["Amount"]) == 4


def test_current_df_string_occ_option_holding():
    pos = {
        "symbol": None,
        "option_symbol": "FUNDX 260618C00100000",
        "units": 1,
        "price": 1.25,
        "average_purchase_price": 150,
    }
    df = positions_to_current_df(
        [pos], account_name="Vanguard Account", user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    row = df.iloc[0]
    assert row["security_type"] == "Option"
    assert row["Symbol"] == "FUNDX 260618C00100000"
    # price is per share; market value is price × 100 × contracts.
    assert abs(float(row["market_value"]) - 125) <= 0.01
    # average_purchase_price is per contract, not ×100 again.
    assert abs(float(row["cost_bases"]) - 150) <= 0.01


def test_current_df_alpaca_snaptrade_real_payload_shape():
    """Pins the EXACT field shape SnapTrade returns for an Alpaca
    position (May 2026, snaptrade-python-sdk 11.0.193). The actual
    keys are ['symbol', 'price', 'open_pnl', 'fractional_units',
    'currency', 'units', 'average_purchase_price', 'cash_equivalent',
    'tax_lots'] — there is NO 'market_value' and NO 'cost_basis' key.

    The first SnapTrade sync for happycameron's Alpaca paper account
    shipped 4 positions with market_value=0 and unrealized_pnl=-100%
    on every row because the normalizer naively read those missing
    keys. This test pins the derived values so that regression cannot
    happen again silently.
    """
    # Real position object, simplified, taken verbatim from the live
    # SnapTrade response on 2026-05-14 for the JPM holding.
    pos = {
        "symbol": {
            "symbol": {
                "raw_symbol": "JPM",
                "description": "JPMorgan Chase & Co.",
            },
        },
        "price": 301.05,
        "open_pnl": -0.07,
        "fractional_units": 1.0,
        "units": 1.0,
        "average_purchase_price": 301.12,
        "cash_equivalent": False,
    }
    df = positions_to_current_df([pos], account_name="Alpaca Paper Account", user_id=2, tenant_id=TENANT_SNAPTRADE)
    assert len(df) == 1
    row = df.iloc[0]

    qty = float(row["Quantity"])
    seed_price = float(row["Price"])
    mv = float(row["market_value"])
    cb = float(row["cost_bases"])
    gl = float(row["gain_or_loss_dollat"])

    assert qty == 1.0
    assert mv == pytest.approx(301.05, abs=0.01), \
        f"market_value must derive from units * price (got {mv})"
    assert cb == pytest.approx(301.12, abs=0.01), \
        f"cost_basis must derive from average_purchase_price * units (got {cb})"
    assert gl == pytest.approx(-0.07, abs=0.01), \
        f"unrealized_pnl must come from open_pnl directly (got {gl})"
    assert abs(qty * seed_price - mv) <= 0.01, \
        "qty * Price == market_value invariant must hold"


def test_current_df_skips_rows_with_no_symbol():
    """A position object missing both raw_symbol and option_symbol is
    unusable downstream; drop it instead of writing a blank Symbol row
    that would crash stg_current's regex parsers."""
    df = positions_to_current_df(
        [{"symbol": {}, "units": 100, "market_value": 5500}],
        account_name="X", user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert len(df) == 0


def test_current_df_stamps_account_and_user_id():
    pos = {
        "symbol": {"raw_symbol": "JEPI"},
        "units": 100,
        "market_value": 5500,
        "cost_basis": 5300,
    }
    df = positions_to_current_df([pos], account_name="Sara Investment", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert df.iloc[0]["Account"] == "Sara Investment"
    assert df.iloc[0]["user_id"] == 9
    assert df.iloc[0]["tenant_id"] == TENANT_SNAPTRADE


# ---------------------------------------------------------------------------
# balances_to_balance_df — cash + account_total rows
# ---------------------------------------------------------------------------


def test_balance_df_emits_two_rows_for_funded_account():
    """A funded account always produces exactly cash + account_total
    rows (snapshot_account_balances_daily relies on this grain)."""
    df = balances_to_balance_df(
        account_summary={"balance": {"total": {"amount": 10000}}},
        balances=[{"cash": 1500}],
        positions=[
            {"market_value": 5500, "cost_basis": 5300},
            {"market_value": 3000, "cost_basis": 2700},
        ],
        account_name="Sara Investment",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert list(df.columns) == BALANCE_SEED_COLUMNS
    assert len(df) == 2
    assert set(df["row_type"]) == {"cash", "account_total"}


def test_balance_df_returns_empty_when_summary_has_no_balance_signal():
    """Missing summary data must not fabricate a zero-valued account."""
    df = balances_to_balance_df(
        account_summary={},
        balances=[{"cash": 0}],
        positions=[],
        account_name="X",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert len(df) == 0


def test_balance_df_emits_rows_for_explicit_zero_balance_account():
    """A broker-confirmed $0 account is real, not missing data.

    Keeping its tenant-stamped balance rows lets a seed write clear any prior
    non-zero balance and lets multi-account onboarding count the empty account
    as queryable instead of waiting forever.
    """
    df = balances_to_balance_df(
        account_summary={"balance": {"total": {"amount": 0}}},
        balances=[{"cash": 0}],
        positions=[],
        account_name="Empty Brokerage Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )

    assert len(df) == 2
    assert set(df["row_type"]) == {"cash", "account_total"}
    assert set(df["tenant_id"]) == {TENANT_SNAPTRADE}
    assert (df["market_value"] == 0).all()


def test_balance_df_derives_total_when_summary_missing_it():
    """SnapTrade's per-broker balance shape varies; some brokers report
    cash + per-position values but no summary total. Derive the total
    from cash + sum(position market_value) so downstream KPIs aren't
    blank."""
    df = balances_to_balance_df(
        account_summary={},
        balances=[{"cash": 500}],
        positions=[{"market_value": 4500, "cost_basis": 4000}],
        account_name="X",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    total_row = df[df["row_type"] == "account_total"].iloc[0]
    assert total_row["market_value"] == 5000


def test_balance_df_stamps_account_and_user_id_on_both_rows():
    df = balances_to_balance_df(
        account_summary={"balance": {"total": {"amount": 10000}}},
        balances=[{"cash": 1500}],
        positions=[{"market_value": 8500, "cost_basis": 8000}],
        account_name="Sara Investment",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert (df["account"] == "Sara Investment").all()
    assert (df["user_id"] == 9).all()


def test_balance_df_unrealized_pnl_matches_total_minus_cost_basis():
    """The unrealized_pnl column on the account_total row drives
    accuracy of the open-positions-only KPI; arithmetic must be
    exactly ``total - sum(cost_basis)``."""
    df = balances_to_balance_df(
        account_summary={"balance": {"total": {"amount": 10000}}},
        balances=[{"cash": 1500}],
        positions=[{"market_value": 8500, "cost_basis": 8000}],
        account_name="X",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    total_row = df[df["row_type"] == "account_total"].iloc[0]
    assert float(total_row["unrealized_pnl"]) == 2000  # 10000 - 8000


# ---------------------------------------------------------------------------
# orders_to_history_df — real-time fallback for the activity-feed lag
# ---------------------------------------------------------------------------


# The real Alpaca-via-SnapTrade order payload (verbatim from the
# 2026-05-14 NVDA repro). Keep this as a fixture so multiple tests can
# point at the exact shape that exposed the original lag bug.
_ALPACA_ORDER_NVDA_BUY = {
    "brokerage_order_id": "order-nvda-buy",
    "status": "EXECUTED",
    "universal_symbol": {
        "raw_symbol": "NVDA",
        "description": "NVIDIA Corporation",
    },
    "option_symbol": None,
    "action": "BUY",
    "total_quantity": "98.000000000000000000",
    "filled_quantity": "98.000000000000000000",
    "execution_price": "234.0264290000",
    "time_executed": "2026-05-14T18:03:57.838061Z",
    "order_type": "Market",
}

_ALPACA_ORDER_NVDA_SELL = {
    "brokerage_order_id": "order-nvda-sell",
    "status": "EXECUTED",
    "universal_symbol": {
        "raw_symbol": "NVDA",
        "description": "NVIDIA Corporation",
    },
    "option_symbol": None,
    "action": "SELL",
    "total_quantity": "98.000000000000000000",
    "filled_quantity": "98.000000000000000000",
    "execution_price": "234.2200000000",
    "time_executed": "2026-05-14T18:06:46.743218Z",
}


def test_orders_df_has_canonical_history_columns():
    df = orders_to_history_df(
        [_ALPACA_ORDER_NVDA_BUY], account_name="X", user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert list(df.columns) == HISTORY_SEED_COLUMNS


def test_orders_df_real_alpaca_payload_buy_yields_negative_amount():
    """Pin the exact Alpaca-via-SnapTrade order shape (May 2026,
    snaptrade-python-sdk 11.0.193). A BUY must emit ``Action="Buy"``,
    ``Amount`` negative (cash out), and ``Quantity * Price ≈ |Amount|``
    so the cross-source dedup with activities lines up."""
    df = orders_to_history_df(
        [_ALPACA_ORDER_NVDA_BUY], account_name="Alpaca Paper Account", user_id=6,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert len(df) == 1
    row = df.iloc[0]

    assert row["Account"] == "Alpaca Paper Account"
    assert row["user_id"] == 6
    assert row["Action"] == "Buy"
    assert row["Symbol"] == "NVDA"
    assert row["Description"] == "NVIDIA Corporation"
    assert float(row["Quantity"]) == pytest.approx(98.0)
    assert float(row["Price"]) == pytest.approx(234.0264290, abs=1e-6)
    # 98 * 234.026429 = 22934.589... ; sign = negative for Buy (cash out)
    assert float(row["Amount"]) < 0
    assert abs(float(row["Quantity"]) * float(row["Price"]) + float(row["Amount"])) <= 0.01


def test_orders_df_crypto_pair_uses_same_base_symbol_as_activities():
    """Coinbase order/activity symbol drift must not duplicate one fill."""
    order = {
        **_ALPACA_ORDER_NVDA_BUY,
        "universal_symbol": {
            "raw_symbol": "BTC-USD",
            "description": "Bitcoin / U.S. Dollar",
        },
        "filled_quantity": "0.01",
        "total_quantity": "0.01",
        "execution_price": "60000",
    }
    df = orders_to_history_df(
        [order], account_name="Coinbase Account", user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert df.iloc[0]["Symbol"] == "BTC"


def test_orders_df_sell_yields_positive_amount():
    """A SELL is cash IN; sign convention matches activities so the
    cross-source dedup keys agree."""
    df = orders_to_history_df([_ALPACA_ORDER_NVDA_SELL], account_name="X", user_id=6, tenant_id=TENANT_SNAPTRADE)
    row = df.iloc[0]
    assert row["Action"] == "Sell"
    assert float(row["Amount"]) > 0


def test_orders_df_drops_non_executed_statuses():
    """Pending / cancelled / rejected / expired orders are NOT trades.
    Activities will never report them as fills; writing them would
    create phantom history rows that the user has to investigate."""
    base = dict(_ALPACA_ORDER_NVDA_BUY)
    rows_in = []
    for status in ("PENDING", "CANCELLED", "REJECTED", "EXPIRED", ""):
        d = dict(base, brokerage_order_id=f"x-{status}", status=status)
        rows_in.append(d)
    df = orders_to_history_df(rows_in, account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert len(df) == 0


# Real Schwab-via-SnapTrade recent_orders option shape (verbatim from the
# 2026-07-08 Cameron Investment condor + FBK STO + PENG BTC). Schwab ships the
# EXPLICIT open/close in the record-level ``action`` (underscore form), and the
# contract in a flat ``option_symbol`` dict with no ``universal_symbol``.
def _schwab_option_order(action, ticker, otype, strike, exp, qty, price):
    under = ticker.split()[0]
    return {
        "brokerage_order_id": f"o-{ticker}-{action}",
        "status": "EXECUTED",
        "universal_symbol": None,
        "option_symbol": {
            "ticker": ticker,
            "option_type": otype,
            "strike_price": strike,
            "expiration_date": exp,
            "underlying_symbol": {"symbol": under, "raw_symbol": under},
        },
        "action": action,
        "total_quantity": f"{qty}.000000000000000000",
        "filled_quantity": f"{qty}.000000000000000000",
        "execution_price": f"{price}",
        "time_executed": "2026-07-08T18:30:00.000000Z",
    }


def test_orders_df_defers_bare_buy_sell_options():
    """A broker that ships only bare BUY/SELL for an option (Alpaca paper)
    gives us NO open/close signal, so we still defer to the activities feed —
    guessing open vs close corrupts the contract lifecycle."""
    options_order = dict(
        _ALPACA_ORDER_NVDA_BUY,
        action="BUY",
        option_symbol={
            "underlying_symbol": {"symbol": "NVDA", "raw_symbol": "NVDA"},
            "expiration_date": "2026-06-21",
            "strike_price": 230,
            "option_type": "CALL",
        },
    )
    df = orders_to_history_df([options_order], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert len(df) == 0


def test_orders_df_emits_schwab_option_with_explicit_open_close():
    """Schwab ships BUY_OPEN/SELL_OPEN/BUY_CLOSE on recent_orders — zero
    guessing. Each maps to the open/close-aware Action, the Symbol is the
    canonical OSI (matches the positions snapshot), and the Amount carries the
    100x contract multiplier with the correct cash-direction sign."""
    orders = [
        _schwab_option_order("SELL_OPEN", "FBK   260717C00060000", "CALL", 60.0, "2026-07-17", 3, "0.01"),
        _schwab_option_order("BUY_CLOSE", "PENG  260717C00080000", "CALL", 80.0, "2026-07-17", 2, "4.90"),
        _schwab_option_order("BUY_OPEN", "DAL   260717P00083000", "PUT", 83.0, "2026-07-17", 10, "1.74"),
    ]
    df = orders_to_history_df(orders, account_name="Schwab Account", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert len(df) == 3
    by_sym = {r["Symbol"]: r for _, r in df.iterrows()}

    sto = by_sym["FBK   260717C00060000"]
    assert sto["Action"] == "Sell to Open"
    assert float(sto["Quantity"]) == pytest.approx(3.0)
    assert float(sto["Amount"]) == pytest.approx(3.0)  # 3 * 100 * 0.01, cash IN

    btc = by_sym["PENG  260717C00080000"]
    assert btc["Action"] == "Buy to Close"
    assert float(btc["Amount"]) == pytest.approx(-980.0)  # 2 * 100 * 4.90, cash OUT

    bto = by_sym["DAL   260717P00083000"]
    assert bto["Action"] == "Buy to Open"
    assert float(bto["Amount"]) == pytest.approx(-1740.0)  # 10 * 100 * 1.74


def test_orders_df_accepts_strict_enum_to_form():
    """The ActionStrictWithOptions enum uses the ``_TO_`` form; accept it too
    so a broker on that vocabulary also fast-paths without guessing."""
    order = _schwab_option_order("SELL_TO_CLOSE", "AAPL  260117C00150000", "CALL", 150.0, "2026-01-17", 1, "2.50")
    df = orders_to_history_df([order], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert len(df) == 1
    assert df.iloc[0]["Action"] == "Sell to Close"
    assert float(df.iloc[0]["Amount"]) == pytest.approx(250.0)  # cash IN


def test_orders_option_dedup_key_matches_activities_row():
    """The whole point of adding options to the fast path: when the slower
    activities feed catches up, the two rows MUST collapse. The cross-source
    dedup key is (Date, Action, Symbol, Quantity, Price) — assert the orders
    row and the activities row for the SAME fill agree on all five."""
    orders_df = orders_to_history_df(
        [_schwab_option_order("SELL_OPEN", "FBK   260717C00060000", "CALL", 60.0, "2026-07-17", 3, "0.7")],
        account_name="Schwab Account", user_id=9, tenant_id=TENANT_SNAPTRADE,
    )
    # Same fill as it arrives later via the activities feed (option_symbol at
    # the activity top level, explicit option_type, per-share price 0.7).
    activity = {
        "type": "SELL",
        "option_type": "SELL_TO_OPEN",
        "option_symbol": {
            "ticker": "FBK   260717C00060000",
            "option_type": "CALL",
            "strike_price": 60.0,
            "expiration_date": "2026-07-17",
            "underlying_symbol": {"symbol": "FBK", "raw_symbol": "FBK"},
        },
        "description": "SOLD TO OPEN FBK 07/17/2026 60.00 C",
        "trade_date": "2026-07-08",
        "units": 3,
        "price": 0.7,
        "amount": 210.0,
    }
    activities_df = activities_to_history_df(
        [activity], account_name="Schwab Account", user_id=9, tenant_id=TENANT_SNAPTRADE,
    )
    key = ["Date", "Action", "Symbol", "Quantity", "Price"]
    o = {k: str(orders_df.iloc[0][k]) for k in key}
    a = {k: str(activities_df.iloc[0][k]) for k in key}
    assert o == a, f"dedup key mismatch:\n orders={o}\n activities={a}"


def test_orders_df_empty_input_returns_canonical_empty_df():
    df = orders_to_history_df([], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert list(df.columns) == HISTORY_SEED_COLUMNS
    assert len(df) == 0


def test_orders_df_skips_zero_quantity_or_zero_price_orders():
    """Defensive: missing fill data → can't construct an Amount.
    Activities will eventually carry the truth."""
    dud_qty = dict(_ALPACA_ORDER_NVDA_BUY, filled_quantity="0", total_quantity="0")
    dud_price = dict(_ALPACA_ORDER_NVDA_BUY, execution_price="0")
    df = orders_to_history_df([dud_qty, dud_price], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert len(df) == 0


def test_orders_df_emits_each_leg_of_an_executed_spread():
    """A same-day SPXW spread is one order with two option legs and no
    top-level option_symbol. Each executed leg is a provisional fill.
    The parent net price is not the per-leg premium."""
    order = {
        "brokerage_order_id": "spxw-spread",
        "status": "EXECUTED",
        "time_executed": "2026-10-01T18:00:00Z",
        "execution_price": "1.45",
        "legs": [
            {
                "leg_id": "short",
                "action": "SELL_TO_OPEN",
                "filled_quantity": "10",
                "execution_price": "7.77",
                "instrument": {
                    "symbol": "SPXW  261001C07650000",
                    "asset_type": "OPTION",
                },
            },
            {
                "leg_id": "long",
                "action": "BUY_TO_OPEN",
                "filled_quantity": "10",
                "execution_price": "6.32",
                "instrument": {
                    "symbol": "SPXW  261001C07655000",
                    "asset_type": "OPTION",
                },
            },
        ],
    }
    df = orders_to_history_df(
        [order], account_name="Schwab", user_id=9, tenant_id=TENANT_SNAPTRADE,
    )
    assert len(df) == 2
    by_action = {row["Action"]: row for _, row in df.iterrows()}
    short = by_action["Sell to Open"]
    long = by_action["Buy to Open"]
    assert float(short["Quantity"]) == pytest.approx(10)
    assert float(short["Price"]) == pytest.approx(7.77)
    assert float(short["Amount"]) == pytest.approx(7770.0)
    assert float(long["Amount"]) == pytest.approx(-6320.0)
    assert "7650" in short["Symbol"] or "07650000" in short["Symbol"]
    assert "7655" in long["Symbol"] or "07655000" in long["Symbol"]


def test_orders_df_uses_filled_quantity_for_partial_fills():
    """Partial fill: 100 placed, 60 filled, rest cancelled. Emit the
    real fill (60) not the placed quantity (100)."""
    partial = dict(
        _ALPACA_ORDER_NVDA_BUY,
        total_quantity="100",
        filled_quantity="60",
    )
    df = orders_to_history_df([partial], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert float(df.iloc[0]["Quantity"]) == 60.0


def test_orders_df_minimal_description_for_dedup_with_activities():
    """The dedup contract: orders-source emits Description = symbol's
    company name (thin), so when activities catches up with a richer
    Description (the broker's wording), the cross-source dedup in
    upload._dedup_history_rows prefers the activities row.

    This test pins the contract: Description must be just the symbol
    description, NOT a synthesized "Bought 98 NVDA at ..." string."""
    df = orders_to_history_df([_ALPACA_ORDER_NVDA_BUY], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    desc = df.iloc[0]["Description"]
    assert desc == "NVIDIA Corporation"
    # NOT a richer synthesized description
    assert "98" not in desc and "Bought" not in desc and "@" not in desc


def test_orders_string_universal_symbol_is_a_ticker_not_a_crash():
    """A bare-string ``universal_symbol`` (not the documented object) must
    not raise. That AttributeError used to fail the whole account inside
    ``_run_sync`` — the shape to expect from an unmodeled broker payload.
    """
    order = {
        "status": "EXECUTED",
        "action": "BUY",
        "universal_symbol": "VTI",
        "option_symbol": None,
        "filled_quantity": "3",
        "execution_price": "250.10",
        "time_executed": "2026-10-12T23:05:00Z",
    }
    df = orders_to_history_df(
        [order], account_name="Vanguard Account", user_id=21,
        tenant_id="snaptrade:vanguard",
    )
    assert list(df["Symbol"]) == ["VTI"]
    assert list(df["Action"]) == ["Buy"]
    assert list(df["Description"]) == ["VTI"]


def test_orders_unknown_action_with_string_symbol_does_not_raise():
    order = {
        "status": "EXECUTED",
        "action": "REINVEST",
        "universal_symbol": "VTI",
        "filled_quantity": "1",
        "execution_price": "250",
        "time_executed": "2026-10-12T23:05:00Z",
    }
    df = orders_to_history_df(
        [order], account_name="Vanguard Account", user_id=21,
        tenant_id="snaptrade:vanguard",
    )
    assert df.empty


def test_orders_null_symbol_is_skipped():
    order = {
        "status": "EXECUTED",
        "action": "BUY",
        "universal_symbol": None,
        "filled_quantity": "1",
        "execution_price": "10",
        "time_executed": "2026-10-12T23:05:00Z",
    }
    df = orders_to_history_df(
        [order], account_name="Vanguard Account", user_id=21,
        tenant_id="snaptrade:vanguard",
    )
    assert df.empty


# ---------------------------------------------------------------------------
# Crypto classification (Coinbase via SnapTrade)
# ---------------------------------------------------------------------------
# Coinbase ships positions as crypto. SnapTrade's UniversalSymbol either
# carries ``type.code = 'crypto'`` (when the broker exposes it) or omits
# the type entirely (older Coinbase responses). We classify in both
# cases — type code preferred, symbol whitelist as fallback — so the
# resulting seed row carries ``security_type='Cryptocurrency'`` and
# downstream models surface the position under the ``Crypto`` strategy
# label.


def _coinbase_btc_position(*, with_type_code: bool = True) -> dict:
    """Builds a SnapTrade-shape position payload for BTC on Coinbase.

    Mirrors the field shape we've seen from SnapTrade in May 2026
    (snaptrade-python-sdk 11.0.193 + Coinbase): ``symbol`` is a nested
    UniversalSymbol with ``symbol.symbol.raw_symbol='BTC'`` and an
    optional ``symbol.symbol.type`` block.
    """
    inner = {
        "raw_symbol": "BTC",
        "symbol": "BTC",
        "description": "Bitcoin",
    }
    if with_type_code:
        inner["type"] = {"code": "crypto", "description": "Cryptocurrency"}
    return {
        "symbol": {"symbol": inner, "description": "Bitcoin"},
        "units": 0.0045777,
        "price": 79125.5718,
        "average_purchase_price": 34931.2,
        "open_pnl": 202.27,
    }


def test_positions_df_crypto_with_type_code_marks_security_type():
    """When SnapTrade ships ``symbol.symbol.type.code = 'crypto'`` we
    must write ``security_type='Cryptocurrency'`` into the seed so
    stg_current and stg_crypto_symbols can both recognize it. Pre-fix
    every non-option position got ``security_type='Equity'`` — which
    silently fused BTC into the Buy and Hold strategy alongside SPY."""
    df = positions_to_current_df(
        [_coinbase_btc_position(with_type_code=True)],
        account_name="Coinbase Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert len(df) == 1
    assert df.iloc[0]["security_type"] == "Cryptocurrency"
    assert df.iloc[0]["Symbol"] == "BTC"


def test_positions_df_crypto_without_type_code_falls_back_to_whitelist():
    """Older Coinbase responses omitted the ``type`` block. The
    symbol-whitelist fallback (CRYPTO_SYMBOLS in app/upload.py) must
    still catch BTC / ETH / etc. so the marker doesn't depend on
    SnapTrade's per-broker normalization quirks."""
    df = positions_to_current_df(
        [_coinbase_btc_position(with_type_code=False)],
        account_name="Coinbase Account",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert df.iloc[0]["security_type"] == "Cryptocurrency"


def test_positions_df_unknown_non_option_still_equity():
    """A regression guard: a generic equity (no type.code, not on the
    crypto whitelist) must NOT get misclassified as Cryptocurrency. If
    the fallback got too aggressive, every equity row from SnapTrade
    would land in the seed as Cryptocurrency and break stg_current's
    instrument_type mapping."""
    spy_position = {
        "symbol": {
            "symbol": {"raw_symbol": "SPY", "symbol": "SPY", "description": "SPDR S&P 500"},
            "description": "SPDR S&P 500",
        },
        "units": 100,
        "price": 510.55,
        "average_purchase_price": 400.0,
        "open_pnl": 11055.0,
    }
    df = positions_to_current_df([spy_position], account_name="X", user_id=6, tenant_id=TENANT_SNAPTRADE)
    assert df.iloc[0]["security_type"] == "Equity"


def test_positions_df_whitelist_ticker_with_equity_type_block_is_equity():
    """Ticker-collision regression (2026-07-14): ``SEI`` is on the crypto
    whitelist (the Sei token) AND is Solaris Energy Infrastructure, a real
    NYSE equity. A user holding Solaris on Schwab (equity-only broker) had
    the share tagged ``Cryptocurrency`` purely from the whitelist, which
    rendered a bogus "Crypto" strategy row on an equity + long-call
    position.

    SnapTrade ships the share with an explicit non-crypto ``type`` block
    (``code='cs'`` / 'Common Stock'). The broker's own instrument type MUST
    win over the ticker whitelist — otherwise every collision ticker (LINK,
    COMP, UNI, W, …) mis-tags a legitimate equity as crypto."""
    sei_equity = {
        "symbol": {
            "symbol": {
                "raw_symbol": "SEI",
                "symbol": "SEI",
                "description": "Solaris Energy Infrastructure Inc.",
                "type": {"code": "cs", "description": "Common Stock"},
            },
            "description": "Solaris Energy Infrastructure Inc.",
        },
        "units": 1,
        "price": 67.11,
        "average_purchase_price": 70.39,
        "open_pnl": -3.28,
    }
    df = positions_to_current_df(
        [sei_equity], account_name="Schwab Account", user_id=9, tenant_id=TENANT_SNAPTRADE
    )
    assert df.iloc[0]["Symbol"] == "SEI"
    assert df.iloc[0]["security_type"] == "Equity"


def test_positions_df_whitelist_ticker_with_crypto_type_block_stays_crypto():
    """The mirror of the collision guard: a whitelisted ticker that the
    broker DOES report as crypto (Coinbase SEI token, ``type.code='crypto'``)
    must still be marked Cryptocurrency. The broker signal wins in BOTH
    directions."""
    sei_token = {
        "symbol": {
            "symbol": {
                "raw_symbol": "SEI",
                "symbol": "SEI",
                "description": "Sei",
                "type": {"code": "crypto", "description": "Cryptocurrency"},
            },
            "description": "Sei",
        },
        "units": 100,
        "price": 0.42,
        "average_purchase_price": 0.5,
        "open_pnl": -8.0,
    }
    df = positions_to_current_df(
        [sei_token], account_name="Coinbase Account", user_id=9, tenant_id=TENANT_SNAPTRADE
    )
    assert df.iloc[0]["security_type"] == "Cryptocurrency"


def test_crypto_symbols_whitelist_matches_dbt_seed():
    """The runtime CRYPTO_SYMBOLS frozenset in app/upload.py mirrors
    the dbt seed dbt/seeds/crypto_symbols.csv. They MUST stay in sync —
    if they drift, the Flask page and the BigQuery strategy
    classification will disagree on whether a position is crypto."""
    import csv
    from pathlib import Path

    from app.upload import CRYPTO_SYMBOLS

    seed_path = (
        Path(__file__).resolve().parents[1]
        / "dbt"
        / "seeds"
        / "crypto_symbols.csv"
    )
    with seed_path.open() as fh:
        reader = csv.DictReader(fh)
        seed_symbols = {row["symbol"].strip().upper() for row in reader if row.get("symbol")}
    assert seed_symbols == set(CRYPTO_SYMBOLS), (
        "dbt/seeds/crypto_symbols.csv and app/upload.py:CRYPTO_SYMBOLS drifted. "
        f"Only in seed: {sorted(seed_symbols - set(CRYPTO_SYMBOLS))}. "
        f"Only in runtime: {sorted(set(CRYPTO_SYMBOLS) - seed_symbols)}."
    )


def test_is_crypto_symbol_helper_is_case_insensitive_and_strips():
    """The ``is_crypto_symbol`` helper accepts mixed case / whitespace
    because the seed comes through pandas / BQ with various
    normalizations. False on empty / None / non-whitelisted ticker."""
    from app.upload import is_crypto_symbol
    assert is_crypto_symbol("BTC") is True
    assert is_crypto_symbol("btc") is True
    assert is_crypto_symbol("  Eth  ") is True
    assert is_crypto_symbol("PLTR") is False
    assert is_crypto_symbol("") is False
    assert is_crypto_symbol(None) is False  # type: ignore[arg-type]


def test_positions_df_ambiguous_ticker_without_type_stays_equity():
    """SNX is both Synthetix and TD SYNNEX. A missing type code used to
    stamp Cryptocurrency from the whitelist. Ambiguous tickers fail
    toward the stock; BTC still falls back to crypto."""
    snx = {
        "symbol": {
            "symbol": {
                "raw_symbol": "SNX",
                "symbol": "SNX",
                "description": "TD SYNNEX",
            },
            "description": "TD SYNNEX",
        },
        "units": 20,
        "price": 148.0,
        "average_purchase_price": 140.0,
        "open_pnl": 160.0,
    }
    df = positions_to_current_df(
        [snx], account_name="Alpaca Paper Account", user_id=1, tenant_id=TENANT_SNAPTRADE
    )
    assert df.iloc[0]["Symbol"] == "SNX"
    assert df.iloc[0]["security_type"] == "Equity"


def test_holding_is_crypto_does_not_tag_the_stock():
    from app.upload import holding_is_crypto, symbol_defaults_to_crypto

    assert symbol_defaults_to_crypto("SNX") is False
    assert symbol_defaults_to_crypto("BTC") is True
    assert holding_is_crypto("SNX", "Cryptocurrency", "TD SYNNEX Corporation") is False
    assert holding_is_crypto("SNX", "Cryptocurrency", "TD SYNNEX") is False
    assert holding_is_crypto("SNX", "", "TD SYNNEX Corporation") is False
    assert holding_is_crypto("SNX", "Cryptocurrency", "Synthetix") is True
    assert holding_is_crypto("BTC") is True
    assert holding_is_crypto("AAPL", "Equity", "Apple Inc") is False


# ---------------------------------------------------------------------------
# positions/all — symbol-less identifiers, kind, bond percent-of-face
# ---------------------------------------------------------------------------


def _current(positions):
    return positions_to_current_df(
        positions, account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE,
    )


def test_symbol_less_holding_uses_cusip_before_figi_and_description():
    pos = {
        "symbol": {"description": "Treasury note"},
        "cusip": "912810TM0",
        "figi_code": "BBG000TEST01",
        "units": 10,
        "price": 98,
        "instrument": {"kind": "bond"},
    }
    df = _current([pos])
    assert list(df["Symbol"]) == ["912810TM0"]
    assert df.iloc[0]["security_type"] == "Bond"
    assert "Treasury note" in df.iloc[0]["Description"]


def test_symbol_less_holding_uses_figi_when_cusip_is_absent():
    pos = {
        "figi_code": "BBG000TEST01",
        "description": "Agency bond",
        "units": 4,
        "price": 99,
        "instrument": {"kind": "bond", "description": "Agency bond"},
    }
    df = _current([pos])
    assert list(df["Symbol"]) == ["BBG000TEST01"]
    assert df.iloc[0]["security_type"] == "Bond"


def test_symbol_less_holding_uses_figi_nested_on_the_instrument():
    pos = {
        "instrument": {
            "kind": "bond",
            "description": "Municipal bond",
            "figi_instrument": {"figi_code": "BBG000TEST02"},
        },
        "units": "2",
        "price": "101.5",
    }
    df = _current([pos])
    assert list(df["Symbol"]) == ["BBG000TEST02"]
    assert abs(float(df.iloc[0]["market_value"]) - (2 * 101.5 * 10)) < 0.01


def test_description_key_is_stable_and_ignores_local_id():
    pos = {
        "local_id": "912810TM0",
        "instrument": {
            "kind": "bond",
            "description": "US Treasury Note 2.5% 02/15/2045",
        },
        "units": 10,
        "price": 98,
    }
    first = _current([pos])
    second = _current([dict(pos)])
    symbol = first.iloc[0]["Symbol"]
    assert symbol == second.iloc[0]["Symbol"]
    assert symbol.startswith("DESC-")
    assert "912810TM0" not in symbol
    assert first.iloc[0]["Description"] == "US Treasury Note 2.5% 02/15/2045"
    assert position_holding_key(pos) == symbol


def test_local_id_alone_does_not_become_a_holding():
    df = _current([{
        "local_id": "ABCDEF123",
        "units": 1,
        "price": 99,
    }])
    assert df.empty


def test_bond_price_without_market_value_is_percent_of_face():
    """10 bonds quoted at 98 are $9,800, not $980. Seed Price is dollars
    per bond so quantity times price still equals market value."""
    pos = {
        "symbol": {
            "raw_symbol": "912810TM0",
            "type": {"code": "bnd", "description": "Bond"},
        },
        "units": 10,
        "price": 98,
        "average_purchase_price": 97,
    }
    row = _current([pos]).iloc[0]
    assert row["security_type"] == "Bond"
    assert abs(float(row["market_value"]) - 9800) < 0.01
    assert abs(float(row["Price"]) - 980) < 0.01
    assert abs(float(row["Quantity"]) * float(row["Price"]) - 9800) <= 0.01
    assert abs(float(row["cost_bases"]) - 9700) < 0.01


def test_bond_market_value_is_not_rescaled():
    pos = {
        "cusip": "912810TM0",
        "units": 10,
        "price": 98,
        "market_value": 9750,
        "instrument": {"kind": "bond"},
    }
    row = _current([pos]).iloc[0]
    assert row["security_type"] == "Bond"
    assert abs(float(row["market_value"]) - 9750) < 0.01
    assert abs(float(row["Price"]) - 975) < 0.01


def test_v2_bond_cost_basis_quote_is_percent_and_dollar_total_is_not():
    quote = {
        "instrument": {"kind": "bond", "cusip": "912810TM1", "description": "Note"},
        "units": "10",
        "price": "98",
        "cost_basis": "97.5",
    }
    quote_row = _current([quote]).iloc[0]
    assert abs(float(quote_row["market_value"]) - 9800) < 0.01
    assert abs(float(quote_row["cost_bases"]) - 9750) < 0.01

    dollar = {
        "symbol": {"raw_symbol": "912810TM2", "type": {"code": "fixed_income"}},
        "units": 10,
        "price": 98,
        "cost_basis": 9500,
    }
    dollar_row = _current([dollar]).iloc[0]
    assert dollar_row["security_type"] == "Bond"
    assert abs(float(dollar_row["market_value"]) - 9800) < 0.01
    assert abs(float(dollar_row["cost_bases"]) - 9500) < 0.01


def test_equity_etf_mutual_fund_and_option_rows_are_unchanged():
    """Schwab equity, a bond ETF typed ``et``, a Fidelity mutual fund,
    and an option keep today's security type and valuation."""
    equity = {
        "symbol": {"raw_symbol": "JEPI", "type": {"code": "cs", "description": "Common Stock"}},
        "units": 100,
        "market_value": 5500.50,
        "cost_basis": 5300,
        "price": 9999.99,
    }
    etf = {
        "symbol": {"raw_symbol": "TBOND", "type": {"code": "et", "description": "ETF"}},
        "units": 10,
        "price": 98,
    }
    fund = {
        "symbol": {
            "raw_symbol": "FXAIX",
            "type": {"code": "oef", "description": "Open End Fund"},
        },
        "units": 12,
        "price": 200,
        "market_value": 2400,
    }
    option = {
        "symbol": {
            "raw_symbol": "AAPL",
            "option_symbol": {
                "underlying_symbol": "AAPL",
                "expiration_date": "2026-06-21",
                "strike_price": 150,
                "option_type": "CALL",
            },
        },
        "units": 1,
        "price": 3.50,
        "market_value": 350,
        "cost_basis": 250,
    }
    df = _current([equity, etf, fund, option])
    by_symbol = {row["Symbol"]: row for _, row in df.iterrows()}
    assert by_symbol["JEPI"]["security_type"] == "Equity"
    assert abs(float(by_symbol["JEPI"]["market_value"]) - 5500.50) < 0.01
    assert abs(float(by_symbol["JEPI"]["Quantity"]) * float(by_symbol["JEPI"]["Price"]) - 5500.50) <= 0.01
    assert by_symbol["TBOND"]["security_type"] == "Equity"
    assert abs(float(by_symbol["TBOND"]["market_value"]) - 980) < 0.01
    assert by_symbol["FXAIX"]["security_type"] == "Equity"
    assert abs(float(by_symbol["FXAIX"]["market_value"]) - 2400) < 0.01
    assert by_symbol["AAPL  260621C00150000"]["security_type"] == "Option"
    assert float(by_symbol["AAPL  260621C00150000"]["Price"]) == 3.50
    assert abs(float(by_symbol["AAPL  260621C00150000"]["market_value"]) - 350) < 0.01


def test_positions_all_future_kind_is_preserved_and_not_priced_as_a_bond():
    pos = {
        "instrument": {"kind": "future", "symbol": "ESZ6", "raw_symbol": "ESZ6"},
        "units": "1",
        "price": "5000",
    }
    row = _current([pos]).iloc[0]
    assert row["Symbol"] == "ESZ6"
    assert row["security_type"] == "Future"
    assert abs(float(row["market_value"]) - 5000) < 0.01


def test_merge_keeps_classic_equity_and_appends_only_a_new_bond():
    classic = [{
        "symbol": {"raw_symbol": "JEPI", "type": {"code": "et"}},
        "units": 100,
        "market_value": 5500.50,
        "price": 55,
        "cost_basis": 5300,
    }]
    option = {
        "symbol": {
            "option_symbol": {
                "ticker": "LITE  261120C01100000",
                "strike_price": 1100.0,
                "expiration_date": "2026-11-20",
                "underlying_symbol": {"symbol": "LITE", "raw_symbol": "LITE"},
                "option_type": "CALL",
            },
        },
        "price": 180.85,
        "units": 1.0,
        "average_purchase_price": 23780.66,
    }
    all_positions = [
        {
            "instrument": {"kind": "stock", "symbol": "JEPI", "raw_symbol": "JEPI"},
            "units": "1",
            "price": "1",
        },
        {
            "instrument": {
                "kind": "option",
                "symbol": "LITE  261120C01100000",
                "raw_symbol": "LITE261120C01100000",
            },
            "units": "9",
            "price": "1",
        },
        {
            "instrument": {
                "kind": "bond",
                "description": "Treasury note",
                "cusip": "912810TM0",
            },
            "units": "10",
            "price": "98",
        },
    ]
    merged = merge_holdings_with_all_positions([classic[0], option], all_positions)
    df = _current(merged)
    assert list(df["Symbol"]) == [
        "JEPI",
        "LITE  261120C01100000",
        "912810TM0",
    ]
    jepi = df[df["Symbol"] == "JEPI"].iloc[0]
    assert jepi["security_type"] == "Equity"
    assert abs(float(jepi["market_value"]) - 5500.50) < 0.01
    assert float(jepi["Quantity"]) == 100
    lite = df[df["Symbol"] == "LITE  261120C01100000"].iloc[0]
    assert lite["security_type"] == "Option"
    assert float(lite["Price"]) == 180.85
    assert abs(float(lite["market_value"]) - 18085.0) < 0.01
    bond = df[df["Symbol"] == "912810TM0"].iloc[0]
    assert bond["security_type"] == "Bond"
    assert abs(float(bond["market_value"]) - 9800) < 0.01
