"""SnapTrade normalize layer — unit tests.

These tests pin every SnapTrade contract translation: action vocabulary
mapping, OSI option-symbol formatting, sign conventions on history,
qty * Price == market_value invariant on positions, and the cash +
account_total balance row shape.

NO network calls, NO Postgres. Pure-data tests so they run in <1s.
"""
from __future__ import annotations

import math

import pandas as pd
import pytest

from app.snaptrade_normalize import (
    SNAPTRADE_ACTIVITY_TO_ACTION,
    activities_to_history_df,
    balances_to_balance_df,
    orders_to_history_df,
    positions_to_current_df,
    snaptrade_symbol_to_osi,
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
    }
    emitted = {v for v in SNAPTRADE_ACTIVITY_TO_ACTION.values() if v is not None}
    # Pull every option action the resolver can possibly produce.
    for canonical in ("Buy", "Sell"):
        for desc in ("buy to open this", "sell to close this", "ambiguous text"):
            emitted.add(_resolve_option_action(canonical, desc))
    missing = expected - emitted
    assert not missing, f"action map missing labels: {missing}"


def test_action_map_drops_cash_movement_explicitly():
    """Deposit/withdrawal/transfer/journal/split must map to None —
    not a silent KeyError, not "Other". stg_history would bucket them
    as `other` and our P&L surfaces would show them as cash
    adjustments that look like trades. Splits in particular are
    handled out-of-band by current_position_stock_price.py +
    stg_split_events; including them here would double-count."""
    for cash_kind in ("DEPOSIT", "WITHDRAWAL", "TRANSFER", "JOURNAL", "SPLIT", "STOCKSPLIT"):
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


def test_history_df_drops_deposit_withdrawal_journal_silently():
    """Cash movements that explicitly map to None must NOT land in the
    seed (they aren't trades). They produce zero rows — not "Unknown"
    and not "Other"."""
    activities = [
        _buy_activity(),
        {"type": "DEPOSIT", "amount": 1000, "trade_date": "2026-05-11"},
        {"type": "WITHDRAWAL", "amount": -500, "trade_date": "2026-05-11"},
        {"type": "JOURNAL", "amount": 0, "trade_date": "2026-05-11"},
    ]
    df = activities_to_history_df(activities, account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert len(df) == 1


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


def test_current_df_option_holding_applies_100x_contract_multiplier():
    """Open option holdings from ``list_option_holdings`` ship a per-share
    ``price`` / ``average_purchase_price`` and NO market_value/cost_basis.
    The derived totals must apply the 100x contract multiplier so an open
    leg snapshots at its real dollar value, not 1/100th. Regression for the
    LITE LEAP that showed in trade history but never as a current position
    (SnapTrade serves options from a separate endpoint)."""
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
        "price": 180.85,
        "units": 1.0,
        "average_purchase_price": 237.8066,
    }
    df = positions_to_current_df([holding], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    row = df.iloc[0]
    assert row["Symbol"] == "LITE  261120C01100000"
    assert row["security_type"] == "Option"
    assert float(row["Price"]) == 180.85
    assert abs(float(row["market_value"]) - 18085.0) < 0.01
    assert abs(float(row["cost_bases"]) - 23780.66) < 0.01
    # Long option: unrealized = market_value - cost_basis.
    assert abs(float(row["gain_or_loss_dollat"]) - (18085.0 - 23780.66)) < 0.01


def test_current_df_short_option_holding_nets_premium_received():
    """A short option holding (negative units) carries a positive
    cost_basis (premium received) and negative market_value (cost to buy
    back); unrealized P&L nets to market_value + cost_basis."""
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
        "price": 0.55,
        "units": -1.0,
        "average_purchase_price": 1.4866,
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


def test_balance_df_returns_empty_for_zero_balance_account():
    """Brand-new accounts with no cash and no positions should produce
    zero rows so stg_account_balances doesn't index a phantom tenant."""
    df = balances_to_balance_df(
        account_summary={},
        balances=[{"cash": 0}],
        positions=[],
        account_name="X",
        user_id=9,
        tenant_id=TENANT_SNAPTRADE,
    )
    assert len(df) == 0


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


def test_orders_df_skips_options_orders():
    """Orders endpoint doesn't carry the broker-text description we
    need to disambiguate Buy-to-Open vs Buy-to-Close. Skip options;
    they will arrive via activities (which carries the description)."""
    options_order = dict(
        _ALPACA_ORDER_NVDA_BUY,
        option_symbol={
            "underlying_symbol": "NVDA",
            "expiration_date": "2026-06-21",
            "strike_price": 230,
            "option_type": "CALL",
        },
    )
    df = orders_to_history_df([options_order], account_name="X", user_id=9, tenant_id=TENANT_SNAPTRADE)
    assert len(df) == 0


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
