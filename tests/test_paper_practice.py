"""Alpaca Paper practice ticket.

The read-only Connection Portal must stay read-only. The practice
session is a separate login: Alpaca Paper, connectionType=trade, one
BUY_TO_OPEN contract. No order is sent without the confirm ticket.
"""
from __future__ import annotations

import os

os.environ.setdefault("HAPPYTRADER_SKIP_DB_INIT", "1")

from datetime import date

from app.paper_practice import (
    SENT_KEY,
    TICKET_KEY,
    contract_cost,
    expiry_choices,
    limit_from_quote,
    occ_symbol,
    option_root,
    practice_receipt,
    practice_sentence,
    strike_choices,
    trade_views,
)
from app.snaptrade import (
    option_quote_request_path,
    practice_portal_login_kwargs,
    read_only_portal_login_kwargs,
    single_leg_buy_to_open,
)


_SNAP = {"snaptrade_user_id": "user-1", "snaptrade_secret": "secret-1"}


def test_read_only_portal_does_not_request_trading():
    kwargs = read_only_portal_login_kwargs(_SNAP, "https://app.example/callback")
    assert kwargs["user_id"] == "user-1"
    assert kwargs["custom_redirect"] == "https://app.example/callback"
    assert "broker" not in kwargs
    assert "connection_type" not in kwargs
    assert "immediate_redirect" not in kwargs

    reconnect = read_only_portal_login_kwargs(
        _SNAP, "https://app.example/callback", reconnect="auth-1",
    )
    assert reconnect["reconnect"] == "auth-1"
    assert "connection_type" not in reconnect
    assert "broker" not in reconnect


def test_practice_portal_is_alpaca_paper_trade_only():
    kwargs = practice_portal_login_kwargs(_SNAP, "https://app.example/callback")
    assert kwargs["broker"] == "ALPACA-PAPER"
    assert kwargs["connection_type"] == "trade"
    assert kwargs["immediate_redirect"] is True
    assert kwargs["custom_redirect"] == "https://app.example/callback"
    assert "reconnect" not in kwargs


def test_option_quote_encodes_occ_spaces_as_plus():
    # SnapTrade rejects a signature that encodes those spaces as %20.
    path = option_quote_request_path(
        "user-1", "secret-1", "acct-1",
        "SPY   260930C00769000", "client-1", 1700000000,
    )
    assert path.startswith("/accounts/acct-1/quotes/options?")
    assert "symbol=SPY+++260930C00769000" in path
    assert "%20" not in path


def test_occ_symbol_is_21_characters():
    symbol = occ_symbol("AAPL", date(2026, 10, 16), "call", 230)
    assert symbol == "AAPL  261016C00230000"
    assert len(symbol) == 21
    put = occ_symbol("SPY", date(2026, 10, 2), "put", 565)
    assert put == "SPY   261002P00565000"
    assert len(put) == 21
    spx = occ_symbol(option_root("SPX"), date(2026, 9, 30), "call", 7600)
    assert spx == "SPXW  260930C07600000"
    assert len(spx) == 21


def test_expiries_match_what_each_symbol_lists():
    # Tuesday, Sep 29, 2026, session still open. SPY, QQQ, and SPX
    # all expire that day.
    spy = expiry_choices(date(2026, 9, 29), "SPY", session_open=True)
    assert spy["soon"]["date"] == date(2026, 9, 29)
    assert spy["soon"]["label"] == "Daily"
    assert spy["weekly"]["date"] == date(2026, 10, 2)
    spx = expiry_choices(date(2026, 9, 29), "SPX", session_open=True)
    assert spx["soon"]["label"] == "Daily"
    assert spx["soon"]["date"] == date(2026, 9, 29)

    # Friday after the close. The next daily is Monday.
    after = expiry_choices(date(2026, 10, 2), "SPX", session_open=False)
    assert after["soon"]["date"] == date(2026, 10, 5)
    assert after["soon"]["label"] == "Daily"
    assert after["weekly"]["date"] == date(2026, 10, 9)

    strikes = strike_choices(228.4, 5)
    assert strikes == {"below": 225, "at": 230, "above": 235}


def test_sentence_is_the_trade_they_picked():
    text = practice_sentence("SPY", "call", 760, "at", date(2026, 10, 2))
    assert text.startswith("Buy 1 SPY call at $760, about the latest close.")
    assert "One contract is 100 shares." in text
    assert "right to buy the shares" in text
    assert "recommendation" not in text.lower()
    spx = practice_sentence("SPX", "put", 7600, "at", date(2026, 9, 30))
    assert "pays cash" in spx
    assert "$100 per point" in spx
    assert "100 shares" in spx
    assert "not 100 shares" in spx
    from app.paper_practice import spx_level_from_spy
    assert spx_level_from_spy(759.89) == 7598.9


def test_quote_triplet_puts_the_mid_between_bid_and_ask():
    from app.paper_practice import quote_triplet
    assert quote_triplet(0.05, 0.06) == {"bid": "0.05", "mid": "0.055", "ask": "0.06"}
    thin = quote_triplet(0, 0.01)
    assert thin["bid"] is None
    assert thin["mid"] is None
    assert thin["ask"] == "0.01"


def test_beginner_readout_says_where_the_stock_had_to_go():
    from datetime import date
    from app.paper_practice import beginner_trade_sentence
    text = beginner_trade_sentence({
        "direction": "Bought",
        "contracts_bought_to_open": 1,
        "premium_paid": -18,
        "option_strike": 770,
        "option_type": "C",
        "underlying_symbol": "SPY",
        "option_expiry": date(2026, 9, 30),
        "status": "Closed",
        "finish_price": 766.44,
        "net_cash_flow": -18,
    })
    assert "You made money if SPY finished above $770.18" in text
    assert "SPY finished at $766.44" in text
    assert "Therefore the call expired, and the $18 is gone" in text


def test_chain_strikes_sit_around_the_price():
    from app.paper_practice import chain_strikes
    assert chain_strikes(768.4, 1, width=2) == [766, 767, 768, 769, 770]
    assert chain_strikes(7688, 5, width=1) == [7685, 7690, 7695]


def test_brokerage_view_is_a_chain_header_not_a_lesson():
    occ = occ_symbol("SPY", date(2026, 9, 30), "call", 770)
    views = trade_views(
        "SPY", "call", 770, "above", date(2026, 9, 30),
        "$0.60", "$60.00", occ, False,
    )
    assert "right to buy the shares" in views["beginner"]["headline"]
    assert views["brokerage"]["headline"] == "SPY  30 SEP 26"
    assert "right to buy" not in views["brokerage"]["headline"]
    brokerage = {row["label"]: row["value"] for row in views["brokerage"]["rows"]}
    assert brokerage["Order"] == "Buy to open 1 Call"
    assert brokerage["Bid"] == "—"
    assert brokerage["Mid"] == "—"
    assert brokerage["Ask"] == "—"
    assert brokerage["Limit"] == "$0.60"
    assert "Mid is halfway" in views["brokerage"]["note"]


def test_closed_market_review_uses_the_last_price():
    from app.paper_practice import quote_from_chain_row, review_limit

    quote = quote_from_chain_row(0, 0, last=1.154, previous=0.90)
    limit, note, err = review_limit(None, quote, "SPY", session_open=False)
    assert err is None
    assert limit == "1.15"
    assert note == "last price at close"
    # A missing last trade uses the previous close.
    previous = quote_from_chain_row(0, 0, last=None, previous=2.4)
    limit, note, err = review_limit(None, previous, "SPY", session_open=False)
    assert limit == "2.40" and note == "last price at close" and err is None
    # Nothing to price is a closed-market message, not a retry prompt.
    empty = quote_from_chain_row(0, 0, last=float("nan"), previous=None)
    limit, note, err = review_limit(None, empty, "SPY", session_open=False)
    assert limit is None and note is None
    assert "last price" in err
    assert "Pick it again in a moment" not in err


def test_closed_market_prices_weekly_and_multi_week_from_the_last_trade(monkeypatch):
    """After the close, bid and ask are 0 and yfinance has no previousClose.

    That shape still prices the daily, the weekly, and the multi-week ticket.
    """
    from types import SimpleNamespace

    from app import app
    from app import paper_practice as practice

    monkeypatch.setattr(practice, "regular_session_open", lambda now=None: False)
    monkeypatch.setattr(practice, "_chain_from_cache", lambda *_a, **_k: None)
    monkeypatch.setattr(practice, "current_user", SimpleNamespace(id=1))
    monkeypatch.setattr(practice, "quote_option_premium", lambda *_a, **_k: None)

    def fake_chain(_symbol, _expiry, strikes):
        return [
            {
                "strike": strike,
                "call": practice.quote_from_chain_row(0, 0, last=6.57),
                "put": practice.quote_from_chain_row(0, 0, last=4.2),
            }
            for strike in strikes
        ]

    monkeypatch.setattr(practice, "load_option_chain", fake_chain)
    account = {"snaptrade_account_id": "learner-acct"}
    with app.test_request_context("/practice"):
        for tenor in ("soon", "weekly", "multi"):
            ticket, err = practice._build_ticket(
                {"symbol": "SPY", "side": "call", "tenor": tenor, "distance": "at"},
                {"SPY": 760.0},
                account,
                today=date(2026, 10, 2),
            )
            assert err is None, (tenor, err)
            assert ticket["tenor"] == tenor
            assert ticket["limit_price"] == "6.57"
            assert ticket["price_note"] == "This limit is the last price at close."


def test_open_session_review_still_requires_a_live_quote():
    from app.paper_practice import quote_from_chain_row, review_limit

    quote = quote_from_chain_row(1.10, 1.20, last=1.15, previous=1.00)
    limit, note, err = review_limit(None, quote, "SPY", session_open=True)
    assert limit is None and note is None
    assert "Pick it again in a moment" in err
    limit, note, err = review_limit(1.16, quote, "SPY", session_open=True)
    assert err is None and note is None
    assert limit == "1.15"


def test_limit_matches_the_quoted_premium_in_cents():
    assert limit_from_quote(1.154) == "1.15"
    assert limit_from_quote(1.155) == "1.16"
    assert limit_from_quote(None) is None
    assert limit_from_quote(0) is None
    assert contract_cost("1.16") == __import__("decimal").Decimal("116.00")


def test_single_leg_order_is_buy_to_open_one_contract():
    order = single_leg_buy_to_open("AAPL  261016C00230000", "1.16")
    assert order["order_type"] == "LIMIT"
    assert order["limit_price"] == "1.16"
    assert order["price_effect"] == "DEBIT"
    assert order["legs"] == [{
        "instrument": {
            "symbol": "AAPL  261016C00230000",
            "instrument_type": "OPTION",
        },
        "action": "BUY_TO_OPEN",
        "units": 1,
    }]


def test_pick_page_uses_lesson_words(monkeypatch):
    from app import app
    from app.models import User
    from app import paper_practice as practice

    class _SessionUser:
        is_active = True
        is_anonymous = False
        id = 7
        username = "ada"

        @property
        def is_authenticated(self):
            return True

        def get_id(self):
            return "7"

    user = _SessionUser()
    monkeypatch.setattr(
        User, "get_by_id", staticmethod(lambda user_id: user if str(user_id) == "7" else None),
    )
    monkeypatch.setattr(practice, "latest_spots", lambda: {
        "SPY": 759.89, "QQQ": 706.90, "SPX": 7598.9,
    })
    monkeypatch.setattr(practice, "snaptrade_enabled", lambda: True)
    monkeypatch.setattr(practice, "alpaca_paper_trade_account", lambda user_id: {
        "snaptrade_account_id": "learner-acct",
        "tenant_id": "snaptrade:learner",
        "account_name": "Practice",
        "row": {},
    })
    monkeypatch.setattr(practice, "market_today", lambda now=None: date(2026, 9, 29))
    monkeypatch.setattr(practice, "_session_open", lambda now=None: True)

    client = app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = "7"
        sess["_fresh"] = True
    html = client.get("/practice").get_data(as_text=True)
    assert "Practice a trade" in html
    assert "Paper money" in html
    assert "not a recommendation" in html
    assert "Call · right to buy" in html
    assert "Beginner" in html and "Intermediate" in html and "Brokerage" in html
    assert "Put · right to sell" in html
    assert "One contract is 100 shares" in html
    assert "Review this paper trade" in html
    filled = client.get(
        "/practice?symbol=SPY&side=put&tenor=weekly&distance=above"
    ).get_data(as_text=True)
    assert "Filled in from the lesson" in filled
    assert 'name="side" value="put" checked' in filled
    assert 'name="tenor" value="weekly" checked' in filled
    assert 'name="distance" value="above" checked' in filled
    assert 'name="tenor" value="soon" checked' not in filled
    assert "BUY_TO_OPEN" not in html
    assert "ALPACA-PAPER" not in html
    assert "SPY" in html and "QQQ" in html and "SPX" in html
    assert "AAPL" not in html
    assert 'data-soon="Daily · Sep 29"' in html
    assert ">Daily · Sep 29<" in html
    assert 'data-contract="SPX pays cash. One contract is $100 per point, not 100 shares."' in html
    assert "Weekly" in html
    assert "Multi-week" in html
    assert "In 3 months" not in html
    assert "Below" in html


def test_placed_trade_is_the_thing_to_look_at_until_the_value_arrives(monkeypatch):
    from app import app
    from app.models import User
    from app import paper_practice as practice
    from flask import render_template, session

    class _SessionUser:
        is_active = True
        is_anonymous = False
        id = 7
        username = "ada"

        @property
        def is_authenticated(self):
            return True

        def get_id(self):
            return "7"

    user = _SessionUser()
    monkeypatch.setattr(
        User, "get_by_id", staticmethod(lambda user_id: user if str(user_id) == "7" else None),
    )
    monkeypatch.setattr(practice, "latest_spots", lambda: {})
    receipt = practice_receipt({
        "sentence": "You are buying a call on SPY.",
        "symbol": "SPY",
        "side": "call",
        "strike": 770,
        "expiry": "2026-09-30",
        "expiry_label": "Daily",
        "limit_label": "$0.60",
        "cost_label": "$60.00",
        "cash_settled": False,
    })
    assert receipt["expiry_label"] == "Daily · Sep 30"
    assert receipt["strike_label"] == "$770"

    client = app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = "7"
        sess["_fresh"] = True
        sess[SENT_KEY] = receipt
    html = client.get("/practice?placed=1").get_data(as_text=True)
    assert "Here's your trade" in html
    assert "You are buying a call on SPY." in html
    assert "$60.00" in html
    assert "was sent to the paper account" in html
    assert "No data found" not in html

    with app.test_request_context("/position/SPY"):
        session[SENT_KEY] = receipt
        empty = render_template(
            "position_detail.html",
            symbol="SPY",
            tabs=None,
            invariant_warning=None,
            viewer_is_admin=False,
            opening_balances=None,
            accounts=[],
            account_toggles=None,
            account_groups=None,
            symbol_company=None,
            symbol_subsector=None,
            symbol_next_earnings=None,
            symbol_sector=None,
            kpis=None,
            error=None,
            title="SPY",
        )
    assert "Here's your trade" in empty
    assert "You are buying a call on SPY." in empty
    assert "No data found" not in empty


def test_intro_is_for_learning_and_links_to_options_101(monkeypatch):
    from app import app
    from app.models import User
    from app import paper_practice as practice

    class _SessionUser:
        is_active = True
        is_anonymous = False
        id = 7
        username = "ada"

        @property
        def is_authenticated(self):
            return True

        def get_id(self):
            return "7"

    user = _SessionUser()
    monkeypatch.setattr(
        User, "get_by_id", staticmethod(lambda user_id: user if str(user_id) == "7" else None),
    )
    monkeypatch.setattr(practice, "snaptrade_enabled", lambda: True)
    monkeypatch.setattr(practice, "alpaca_paper_trade_account", lambda user_id: None)

    client = app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = "7"
        sess["_fresh"] = True
    html = client.get("/practice").get_data(as_text=True)
    assert "here to learn" in html
    assert "buying power" in html
    assert "$100,000" not in html
    assert 'href="/learn"' in html
    assert "Learn Options 101" in html
    assert "Alpaca" not in html
    assert "limit premium" not in html
    assert "and the premium" not in html
    assert "what it costs" in html


def test_unquoted_ticket_does_not_send_an_order(monkeypatch):
    from flask import session
    from flask_login import login_user

    from app import app
    from app.paper_practice import SENT_KEY, TICKET_KEY, paper_practice_place

    sent = []
    monkeypatch.setattr(
        "app.paper_practice.place_single_leg_option_order",
        lambda *a, **k: sent.append(1),
    )
    monkeypatch.setattr(
        "app.paper_practice.alpaca_paper_trade_account",
        lambda user_id: None,
    )

    class _Viewer:
        is_authenticated = True
        is_active = True
        is_anonymous = False
        id = 7

        def get_id(self):
            return "7"

    with app.test_request_context(
        "/practice/place", method="POST", data={"nonce": "look-1"},
    ):
        login_user(_Viewer())
        session[TICKET_KEY] = {
            "nonce": "look-1",
            "look": True,
            "sentence": "Buy 1 SPY call at $764, about today's price.",
            "side": "call",
            "symbol": "SPY",
        }
        resp = paper_practice_place.__wrapped__()
        saved = session.get(SENT_KEY)

    assert resp.status_code == 302
    assert sent == []
    assert saved is None


def test_place_without_confirm_does_not_send(monkeypatch):
    from flask import session
    from flask_login import login_user

    from app import app
    from app.paper_practice import paper_practice_place

    sent = []
    monkeypatch.setattr(
        "app.paper_practice.place_single_leg_option_order",
        lambda *a, **k: sent.append(1),
    )

    class _Viewer:
        is_authenticated = True
        is_active = True
        is_anonymous = False
        id = 7

        def get_id(self):
            return "7"

    with app.test_request_context("/practice/place", method="POST", data={"nonce": "nope"}):
        login_user(_Viewer())
        resp = paper_practice_place.__wrapped__()
        assert TICKET_KEY not in session

    assert resp.status_code == 302
    assert sent == []


def test_trade_account_ignores_read_only_and_the_demo_mirror(monkeypatch):
    from app import snaptrade as snap

    class _Resp:
        def __init__(self, body):
            self.body = body

    class _Connections:
        def list_brokerage_authorizations(self, **kwargs):
            return _Resp([
                {
                    "id": "read-auth",
                    "type": "read",
                    "brokerage": {"slug": "ALPACA-PAPER", "name": "Alpaca Paper"},
                },
                {
                    "id": "trade-auth",
                    "type": "trade",
                    "disabled": False,
                    "brokerage": {"slug": "ALPACA-PAPER", "name": "Alpaca Paper"},
                },
            ])

        def list_brokerage_authorization_accounts(self, **kwargs):
            assert kwargs["authorization_id"] == "trade-auth"
            return _Resp([
                {"id": "demo-acct"},
                {"id": "learner-acct"},
            ])

    class _Client:
        connections = _Connections()

    monkeypatch.setattr(
        snap, "get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "u", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(snap, "_get_snaptrade_client", lambda: _Client())
    monkeypatch.setattr(
        snap, "get_snaptrade_accounts",
        lambda uid: [
            {
                "snaptrade_account_id": "demo-acct",
                "tenant_id": "demo:demo-account",
                "account_name": "Demo Account",
            },
            {
                "snaptrade_account_id": "learner-acct",
                "tenant_id": "snaptrade:learner",
                "account_name": "Alpaca Paper",
                "display_nickname": "Practice",
            },
        ],
    )

    found = snap.alpaca_paper_trade_account(7)
    assert found["snaptrade_account_id"] == "learner-acct"
    assert found["tenant_id"] == "snaptrade:learner"
    assert found["account_name"] == "Practice"


def test_confirmed_place_sends_the_shown_limit_and_does_not_refresh(monkeypatch):
    from flask import session
    from flask_login import login_user

    from app import app
    from app import snaptrade as snap
    from app.paper_practice import TICKET_KEY, paper_practice_place

    class _Resp:
        def __init__(self, body):
            self.body = body

    calls = []

    class _Trading:
        def place_mleg_order(self, **kwargs):
            calls.append(kwargs)
            return _Resp({"brokerage_order_id": "ord-1", "status": "ACCEPTED"})

    class _Client:
        trading = _Trading()

    syncs = []

    class _Viewer:
        is_authenticated = True
        is_active = True
        is_anonymous = False
        id = 7

        def get_id(self):
            return "7"

    monkeypatch.setattr(
        snap, "get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "u", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(snap, "_get_snaptrade_client", lambda: _Client())
    monkeypatch.setattr(
        "app.paper_practice.alpaca_paper_trade_account",
        lambda user_id: {
            "snaptrade_account_id": "learner-acct",
            "tenant_id": "snaptrade:learner",
            "account_name": "Practice",
            "row": {"snaptrade_account_id": "learner-acct"},
        },
    )
    monkeypatch.setattr(
        "app.paper_practice.queue_account_read_sync",
        lambda user_id, row: syncs.append((user_id, row)),
    )

    ticket = {
        "nonce": "nonce-1",
        "occ": "AAPL  261016C00230000",
        "limit_price": "1.16",
        "cost": "116.00",
        "sentence": "Buy 1 AAPL call at $230, about today's price.",
        "side": "call",
        "symbol": "AAPL",
        "strike": 230,
        "expiry": "2026-10-16",
        "expiry_label": "Weekly",
        "limit_label": "$1.16",
        "cost_label": "$116.00",
        "account_id": "learner-acct",
    }
    with app.test_request_context(
        "/practice/place", method="POST", data={"nonce": "nonce-1"},
    ):
        login_user(_Viewer())
        session[TICKET_KEY] = ticket
        resp = paper_practice_place.__wrapped__()
        assert TICKET_KEY not in session

    assert resp.status_code == 302
    assert calls[0]["order_type"] == "LIMIT"
    assert calls[0]["limit_price"] == "1.16"
    assert calls[0]["legs"][0]["action"] == "BUY_TO_OPEN"
    assert calls[0]["legs"][0]["units"] == 1
    assert calls[0]["legs"][0]["instrument"]["symbol"] == "AAPL  261016C00230000"
    assert calls[0]["account_id"] == "learner-acct"
    assert "force_refresh" not in calls[0]
    assert syncs == [(7, {"snaptrade_account_id": "learner-acct"})]


def test_practice_callback_returns_to_the_ticket(monkeypatch):
    import types

    from flask import session

    from app import app
    from app import snaptrade as snap

    monkeypatch.setattr(snap, "current_user", types.SimpleNamespace(id=7))
    monkeypatch.setattr(
        snap, "get_snaptrade_user",
        lambda uid: {"snaptrade_user_id": "u", "snaptrade_secret": "s"},
    )
    monkeypatch.setattr(snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(
        snap, "_list_snaptrade_accounts",
        lambda client, creds: ([{
            "id": "paper-acc",
            "institution_name": "Alpaca Paper",
            "number": "1111",
            "brokerage_authorization": "auth-paper",
        }], True),
    )

    def _accounts(uid):
        if not _accounts.calls:
            _accounts.calls += 1
            return []
        return [{
            "snaptrade_account_id": "paper-acc",
            "first_sync_completed": False,
            "account_name": "Alpaca Paper",
        }]

    _accounts.calls = 0
    monkeypatch.setattr(snap, "get_snaptrade_accounts", _accounts)
    monkeypatch.setattr(
        snap, "_ensure_snaptrade_tenant_id", lambda **kwargs: "snaptrade:paper-acc",
    )
    monkeypatch.setattr(
        snap, "get_broker_tenant",
        lambda tid: {"account_name": "Alpaca Paper", "display_nickname": None},
    )
    monkeypatch.setattr(snap, "upsert_snaptrade_account", lambda *a, **k: None)
    monkeypatch.setattr(snap, "clear_snaptrade_connection_broken", lambda *a, **k: None)
    monkeypatch.setattr(snap, "add_account_for_user", lambda *a, **k: None)
    monkeypatch.setattr(
        snap, "set_snaptrade_brokerage_authorization_id", lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "app.early_broker.maybe_stamp_early_broker_cohort", lambda *a, **k: None,
    )
    kicked = []
    monkeypatch.setattr(snap, "_kick_post_connect_sync", lambda uid: kicked.append(uid))

    with app.test_request_context("/snaptrade/callback"):
        session["snaptrade_callback_user_id"] = 7
        session["snaptrade_practice_return"] = "1"
        resp = snap.snaptrade_callback.__wrapped__()
        flashes = session.get("_flashes") or []

    assert resp.status_code == 302
    assert (resp.location or "").rstrip("/").endswith("/practice")
    assert kicked == [7]
    messages = " ".join(str(item) for item in flashes)
    assert "Paper account connected" in messages
    assert "Connected 1 account" not in messages
