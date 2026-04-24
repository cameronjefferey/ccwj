"""
Tests for the Schwab trader-v1 TRADE parser. The previous parser read the
legacy `transactionItem` shape and silently produced 632 rows of
Action='Other' with empty symbols — tests here guard against that regression.
"""
from app.schwab import (
    _schwab_action_from_effect,
    _schwab_asset_type_to_security_type,
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
