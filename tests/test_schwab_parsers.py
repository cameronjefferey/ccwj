"""
Tests for the Schwab trader-v1 TRADE parser. The previous parser read the
legacy `transactionItem` shape and silently produced 632 rows of
Action='Other' with empty symbols — tests here guard against that regression.
"""
from app.schwab import (
    _schwab_action_from_effect,
    _schwab_asset_type_to_security_type,
    _schwab_collapse_wash_pairs,
    _schwab_trade_rows,
)


def test_action_from_effect_option_opening_positive_amount_is_buy_to_open():
    assert (
        _schwab_action_from_effect("OPTION", 1, "OPENING")
        == "Buy to Open"
    )


def test_action_from_effect_option_opening_negative_amount_is_sell_to_open():
    assert (
        _schwab_action_from_effect("OPTION", -2, "OPENING")
        == "Sell to Open"
    )


def test_action_from_effect_option_closing_negative_amount_is_sell_to_close():
    assert (
        _schwab_action_from_effect("OPTION", -1, "CLOSING")
        == "Sell to Close"
    )


def test_action_from_effect_equity_positive_amount_is_buy():
    assert _schwab_action_from_effect("EQUITY", 100, "") == "Buy"


def test_action_from_effect_equity_negative_amount_is_sell():
    assert _schwab_action_from_effect("EQUITY", -100, "") == "Sell"


def test_asset_type_maps_collective_investment_to_etf_label():
    assert (
        _schwab_asset_type_to_security_type("COLLECTIVE_INVESTMENT")
        == "ETFs & Closed End Funds"
    )


def test_trade_rows_v1_shape_equity_buy():
    tx = {
        "type": "TRADE",
        "tradeDate": "2026-04-15T13:30:00Z",
        "netAmount": -500.0,
        "transferItems": [
            {
                "instrument": {
                    "symbol": "AAPL",
                    "description": "APPLE INC",
                    "assetType": "EQUITY",
                },
                "amount": 10,
                "price": 50.0,
                "cost": -500.0,
                "positionEffect": "OPENING",
            },
            {"feeType": "COMMISSION", "amount": -0.65, "cost": -0.65},
        ],
    }
    rows = _schwab_trade_rows(tx)
    assert len(rows) == 1
    r = rows[0]
    assert r["action"] == "Buy"
    assert r["symbol"] == "AAPL"
    assert r["description"] == "APPLE INC"
    assert r["quantity"] == 10
    assert r["price"] == 50.0
    assert r["amount"] == -500.0
    assert r["transaction_date"] == "04/15/2026"


def test_trade_rows_v1_shape_option_sell_to_open():
    tx = {
        "type": "TRADE",
        "tradeDate": "2026-04-17",
        "netAmount": 523.16,
        "transferItems": [
            {
                "instrument": {
                    "symbol": "QTUM  260515C00131000",
                    "description": "CALL ETF SER SOLUTIONS",
                    "assetType": "OPTION",
                    "putCall": "CALL",
                },
                "amount": -2,
                "price": 2.62,
                "cost": 524.0,
                "positionEffect": "OPENING",
            }
        ],
    }
    rows = _schwab_trade_rows(tx)
    assert len(rows) == 1
    r = rows[0]
    assert r["action"] == "Sell to Open"
    assert r["quantity"] == 2
    assert r["price"] == 2.62


def test_trade_rows_skips_non_trade_and_fee_only():
    assert _schwab_trade_rows({"type": "DIVIDEND_OR_INTEREST"}) == []
    assert (
        _schwab_trade_rows(
            {
                "type": "TRADE",
                "transferItems": [
                    {"feeType": "COMMISSION", "amount": -0.65, "cost": -0.65}
                ],
            }
        )
        == []
    )


def test_trade_rows_collapses_covered_call_assignment_wash_pair():
    """
    A real Schwab covered-call assignment returned a TRADE payload with THREE
    LUNR equity transferItems (-1500, +1500, -1500) plus an option leg. The
    old parser wrote Sell + Buy + Sell, doubling cost basis and gain/loss on
    the position page. We now group by (symbol, price, asset_type) and sum
    signed amounts/costs so the wash pair drops, leaving one net Sell for the
    equity and an "Assigned" row for the option.
    """
    tx = {
        "type": "TRADE",
        "transactionSubType": "OA",
        "tradeDate": "2025-01-27T13:30:00Z",
        "netAmount": 34499.75,
        "transferItems": [
            {
                "instrument": {
                    "symbol": "LUNR",
                    "description": "INTUITIVE MACHS INC CLASS A",
                    "assetType": "EQUITY",
                },
                "amount": -1500,
                "price": 23.0,
                "cost": 34500.0,
            },
            {
                "instrument": {
                    "symbol": "LUNR",
                    "description": "INTUITIVE MACHS INC CLASS A",
                    "assetType": "EQUITY",
                },
                "amount": 1500,
                "price": 23.0,
                "cost": -34500.0,
            },
            {
                "instrument": {
                    "symbol": "LUNR",
                    "description": "INTUITIVE MACHS INC CLASS A",
                    "assetType": "EQUITY",
                },
                "amount": -1500,
                "price": 23.0,
                "cost": 34500.0,
            },
            {
                "instrument": {
                    "symbol": "LUNR  250124C00023000",
                    "description": "INTUITIVE MACHS INC 01/24/2025 $23 Call",
                    "assetType": "OPTION",
                    "putCall": "CALL",
                },
                "amount": -15,
                "price": 0.0,
                "cost": 0.0,
                "positionEffect": "CLOSING",
            },
        ],
    }
    rows = _schwab_trade_rows(tx)
    assert len(rows) == 2, rows
    equity_rows = [r for r in rows if r["symbol"] == "LUNR"]
    option_rows = [r for r in rows if r["symbol"].startswith("LUNR ")]
    assert len(equity_rows) == 1
    assert len(option_rows) == 1
    eq = equity_rows[0]
    assert eq["action"] == "Sell"
    assert eq["quantity"] == 1500
    assert eq["price"] == 23.0
    assert eq["amount"] == 34500.0
    opt = option_rows[0]
    assert opt["action"] == "Assigned"
    assert opt["quantity"] == 15


def test_trade_rows_option_exercise_emits_exchange_or_exercise():
    tx = {
        "type": "TRADE",
        "transactionSubType": "OE",
        "tradeDate": "2025-03-10",
        "transferItems": [
            {
                "instrument": {
                    "symbol": "AAPL  250321C00150000",
                    "assetType": "OPTION",
                },
                "amount": -2,
                "price": 0.0,
                "cost": 0.0,
                "positionEffect": "CLOSING",
            }
        ],
    }
    rows = _schwab_trade_rows(tx)
    assert len(rows) == 1
    assert rows[0]["action"] == "Exchange or Exercise"


def test_trade_rows_option_expiration_emits_expired():
    tx = {
        "type": "TRADE",
        "transactionSubType": "EXPIRATION",
        "tradeDate": "2025-04-18",
        "transferItems": [
            {
                "instrument": {
                    "symbol": "SPY   250418C00600000",
                    "assetType": "OPTION",
                },
                "amount": -1,
                "price": 0.0,
                "cost": 0.0,
                "positionEffect": "CLOSING",
            }
        ],
    }
    rows = _schwab_trade_rows(tx)
    assert len(rows) == 1
    assert rows[0]["action"] == "Expired"


def test_collapse_wash_pairs_drops_buy_keeps_sell():
    rows = [
        {"transaction_date": "01/27/2025", "action": "Sell", "symbol": "LUNR", "description": "LUNR", "quantity": 1500, "price": 23.0, "fees": "", "amount": 34500.0},
        {"transaction_date": "01/27/2025", "action": "Buy",  "symbol": "LUNR", "description": "LUNR", "quantity": 1500, "price": 23.0, "fees": "", "amount": -34500.0},
    ]
    out = _schwab_collapse_wash_pairs(rows)
    assert len(out) == 1
    assert out[0]["action"] == "Sell"
    assert out[0]["amount"] == 34500.0


def test_collapse_wash_pairs_leaves_unmatched_pair_alone():
    rows = [
        {"transaction_date": "01/27/2025", "action": "Sell", "symbol": "LUNR", "description": "LUNR", "quantity": 1500, "price": 23.0, "fees": "", "amount": 34500.0},
        {"transaction_date": "01/27/2025", "action": "Buy",  "symbol": "LUNR", "description": "LUNR", "quantity": 1500, "price": 22.0, "fees": "", "amount": -33000.0},
    ]
    out = _schwab_collapse_wash_pairs(rows)
    assert len(out) == 2


def test_collapse_wash_pairs_does_not_drop_real_scalp_with_different_prices():
    rows = [
        {"transaction_date": "03/01/2025", "action": "Buy",  "symbol": "AAPL", "description": "AAPL", "quantity": 100, "price": 150.0, "fees": "", "amount": -15000.0},
        {"transaction_date": "03/01/2025", "action": "Sell", "symbol": "AAPL", "description": "AAPL", "quantity": 100, "price": 151.0, "fees": "", "amount": 15100.0},
    ]
    out = _schwab_collapse_wash_pairs(rows)
    assert len(out) == 2


def test_collapse_wash_pairs_handles_multiple_symbols_same_day():
    rows = [
        {"transaction_date": "04/06/2026", "action": "Buy",  "symbol": "RKLB", "description": "RKLB", "quantity": 100, "price": 63.0, "fees": "", "amount": -6300.0},
        {"transaction_date": "04/06/2026", "action": "Sell", "symbol": "RKLB", "description": "RKLB", "quantity": 100, "price": 63.0, "fees": "", "amount": 6300.0},
        {"transaction_date": "04/06/2026", "action": "Buy",  "symbol": "CRWV", "description": "CRWV", "quantity": 100, "price": 78.0, "fees": "", "amount": -7800.0},
        {"transaction_date": "04/06/2026", "action": "Sell", "symbol": "CRWV", "description": "CRWV", "quantity": 100, "price": 78.0, "fees": "", "amount": 7800.0},
    ]
    out = _schwab_collapse_wash_pairs(rows)
    assert len(out) == 2
    actions = sorted(r["action"] for r in out)
    assert actions == ["Sell", "Sell"]


def test_trade_rows_legacy_transactionitem_fallback_still_parsed():
    tx = {
        "type": "TRADE",
        "transactionDate": "2024-01-02",
        "netAmount": -100.0,
        "transactionItem": {
            "instrument": {"symbol": "XYZ", "assetType": "EQUITY"},
            "amount": 5,
            "price": 20.0,
            "cost": -100.0,
        },
    }
    rows = _schwab_trade_rows(tx)
    assert len(rows) == 1
    assert rows[0]["symbol"] == "XYZ"
    assert rows[0]["action"] == "Buy"
