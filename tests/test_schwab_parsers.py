"""
Tests for the Schwab trader-v1 TRADE parser. The previous parser read the
legacy `transactionItem` shape and silently produced 632 rows of
Action='Other' with empty symbols — tests here guard against that regression.
"""
from app.schwab import (
    _is_schwab_refresh_token_invalid,
    _schwab_action_from_effect,
    _schwab_asset_type_to_security_type,
    _schwab_cash_event_rows,
    _schwab_classify_cash_event_action,
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


# --------------------------------------------------------------------------- #
# Dividend / interest sync — DIVIDEND_OR_INTEREST and friends were silently
# dropped by the old `_schwab_trade_rows` guard. JEPI / JEPQ holders saw $0
# dividends on /position even though the broker was reporting them. These tests
# guard against that regression.
# --------------------------------------------------------------------------- #


def test_classify_qualified_dividend_from_description():
    inst = {"description": "JPMorgan Equity Premium Income ETF"}
    assert (
        _schwab_classify_cash_event_action(
            {"description": "QUALIFIED DIVIDEND~JEPI"}, inst
        )
        == "Qualified Dividend"
    )


def test_classify_cash_dividend_default():
    assert (
        _schwab_classify_cash_event_action({"description": "CASH DIVIDEND"}, None)
        == "Cash Dividend"
    )


def test_classify_margin_interest():
    assert (
        _schwab_classify_cash_event_action(
            {"description": "MARGIN INTEREST 11/26-12/29"}, None
        )
        == "Margin Interest"
    )


def test_classify_bank_interest():
    assert (
        _schwab_classify_cash_event_action(
            {"description": "BANK INT ...523 SCHWAB BANK"}, None
        )
        == "Bank Interest"
    )


def test_classify_credit_interest():
    assert (
        _schwab_classify_cash_event_action(
            {"description": "SCHWAB1 INT 11/26-12/29"}, None
        )
        == "Credit Interest"
    )


def test_classify_subtype_fallback_for_qd():
    # Description empty — fall back to subType code
    assert (
        _schwab_classify_cash_event_action({"transactionSubType": "QD"}, None)
        == "Qualified Dividend"
    )


def test_classify_unknown_event_returns_empty_string():
    # JOURNAL / MEMORANDUM / ACH should not be classified as dividends
    assert _schwab_classify_cash_event_action({"description": "JOURNAL ENTRY"}, None) == ""
    assert _schwab_classify_cash_event_action({}, None) == ""


def test_cash_event_rows_qualified_dividend_jepi():
    """
    Real Schwab DIVIDEND_OR_INTEREST shape for a JEPI qualified dividend.
    The dividend leg carries the symbol; the cash-equivalent leg is a
    bookkeeping mirror that must be ignored to avoid a duplicate row.
    """
    tx = {
        "type": "DIVIDEND_OR_INTEREST",
        "transactionSubType": "QD",
        "tradeDate": "2024-08-01T00:00:00+0000",
        "description": "QUALIFIED DIVIDEND~JEPI",
        "netAmount": 142.55,
        "transferItems": [
            {
                "amount": 142.55,
                "instrument": {
                    "symbol": "JEPI",
                    "assetType": "COLLECTIVE_INVESTMENT",
                    "description": "JPMorgan Equity Premium Income ETF",
                },
            },
            {
                "amount": -142.55,
                "instrument": {"assetType": "CASH_EQUIVALENT"},
            },
        ],
    }
    rows = _schwab_cash_event_rows(tx)
    assert len(rows) == 1, rows
    r = rows[0]
    assert r["action"] == "Qualified Dividend"
    assert r["symbol"] == "JEPI"
    assert r["amount"] == 142.55
    assert r["quantity"] == ""
    assert r["price"] == ""


def test_cash_event_rows_cash_dividend_with_only_equity_leg():
    tx = {
        "type": "DIVIDEND_OR_INTEREST",
        "subType": "CD",
        "transactionDate": "2024-09-15T00:00:00+0000",
        "description": "CASH DIVIDEND",
        "transferItems": [
            {
                "amount": 80.00,
                "instrument": {
                    "symbol": "JEPQ",
                    "assetType": "COLLECTIVE_INVESTMENT",
                },
            },
        ],
    }
    rows = _schwab_cash_event_rows(tx)
    assert len(rows) == 1
    assert rows[0]["action"] == "Cash Dividend"
    assert rows[0]["symbol"] == "JEPQ"
    assert rows[0]["amount"] == 80.00


def test_cash_event_rows_margin_interest_no_instrument_leg():
    """
    Margin interest typically arrives with no instrument at all (just a
    netAmount and description). The fallback path should still emit a row.
    """
    tx = {
        "type": "DIVIDEND_OR_INTEREST",
        "transactionDate": "2024-08-30T00:00:00+0000",
        "description": "MARGIN INTEREST 07/30-08/29",
        "netAmount": -18.95,
        "transferItems": [],
    }
    rows = _schwab_cash_event_rows(tx)
    assert len(rows) == 1
    assert rows[0]["action"] == "Margin Interest"
    assert rows[0]["symbol"] == ""
    assert rows[0]["amount"] == -18.95


def test_cash_event_rows_skips_unrelated_receive_and_deliver():
    """
    RECEIVE_AND_DELIVER also covers stock splits / mergers / journal entries.
    Without a dividend-shaped description or subType we should not synthesize
    a fake dividend row — Schwab's daily snapshot reflects those flows.
    """
    tx = {
        "type": "RECEIVE_AND_DELIVER",
        "transactionSubType": "SPLIT",
        "description": "MANDATORY REVERSE STOCK SPLIT",
        "transferItems": [
            {
                "amount": -100,
                "instrument": {"symbol": "RVSN", "assetType": "EQUITY"},
            },
            {
                "amount": 1,
                "instrument": {"symbol": "RVSN", "assetType": "EQUITY"},
            },
        ],
    }
    assert _schwab_cash_event_rows(tx) == []


def test_cash_event_rows_skips_pure_trade_payload():
    # Sanity: dividend extractor must not emit anything for a TRADE row.
    tx = {
        "type": "TRADE",
        "transferItems": [
            {
                "amount": -100,
                "instrument": {"symbol": "JEPI", "assetType": "EQUITY"},
                "price": 60.0,
            }
        ],
    }
    assert _schwab_cash_event_rows(tx) == []


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


# ---------------------------------------------------------------------------
# Refresh-token-invalid detector.
#
# The detector flips schwab_connections.refresh_token_invalid_at so the
# in-app "Reconnect Schwab" banner fires. A miss here means the cron
# keeps logging refresh failures and the user has no in-product signal
# their tokens died.
#
# Real production logs have shipped four distinct spellings; this test
# pins every one of them. See ~/.cursor/skills/broker-sync-safety/SKILL.md
# 2026-05-21 for the regression that motivated the additions of
# `invalid_grant` and the literal Schwab error_description.
# ---------------------------------------------------------------------------


def test_refresh_token_invalid_matches_modern_invalid_grant_error():
    """Real Schwab API response (May 2026 cron logs):

        unsupported_token_type: 400 Bad Request:
        "{"error_description":"Refresh token is invalid, expired or
        revoked","error":"invalid_grant"}"

    Both ``invalid_grant`` and the literal description must trigger
    the banner — they appear together but matching either is enough.
    """
    exc = Exception(
        'unsupported_token_type: 400 Bad Request: '
        '"{"error_description":"Refresh token is invalid, expired or '
        'revoked","error":"invalid_grant"}"'
    )
    assert _is_schwab_refresh_token_invalid(exc) is True


def test_refresh_token_invalid_matches_bare_invalid_grant_string():
    """Defensive: any future Schwab response shape that strips the
    description but keeps the OAuth ``invalid_grant`` error code is
    still detected. Refresh is the only ``/oauth/token`` grant we do.
    """
    assert _is_schwab_refresh_token_invalid(Exception("invalid_grant")) is True


def test_refresh_token_invalid_matches_legacy_refresh_token_authentication_error():
    """Older Schwab response code, still in the matcher to survive a
    Schwab rollback of the error-code format change."""
    assert _is_schwab_refresh_token_invalid(
        Exception("refresh_token_authentication_error: 400 Bad Request")
    ) is True


def test_refresh_token_invalid_matches_legacy_failed_refresh_token_authentication():
    """Older schwab-py exception phrasing — keep matching it so we
    don't regress users whose tokens die during a rollback."""
    assert _is_schwab_refresh_token_invalid(
        Exception("Failed refresh token authentication")
    ) is True


def test_refresh_token_invalid_does_not_trip_on_unrelated_5xx():
    """A transient Schwab 500 / connect timeout is NOT a refresh-token
    problem and must not flip the banner — those recover on the next
    cron run."""
    assert _is_schwab_refresh_token_invalid(
        Exception("500 Internal Server Error")
    ) is False
    assert _is_schwab_refresh_token_invalid(
        Exception("ReadTimeout")
    ) is False


def test_refresh_token_invalid_does_not_trip_on_bare_401():
    """A bare 401 is usually a stale account_hash that
    ``_sync_account_hash_from_numbers`` recovers from on retry — do
    not flip the connection to ``refresh_token_invalid`` for it."""
    assert _is_schwab_refresh_token_invalid(
        Exception("401 Unauthorized: Token validation failed")
    ) is False
