"""Live-audit fixes for paper practice: real book, orders, ticks, simple nav."""
from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("HAPPYTRADER_SKIP_DB_INIT", "1")

from app.paper_accounts import (
    drop_paper_from_mixed_book,
    is_paper_row,
    paper_display_label,
    position_link_symbol,
)
from app.paper_practice import (
    _readout_symbol,
    beginner_readouts,
    limit_from_quote,
    option_tick,
    order_status_bucket,
    practice_sentence,
    round_to_tick,
    trade_views,
)


def _row(name, tid="snaptrade:paper"):
    return {
        "tenant_id": tid,
        "account_name": name,
        "broker_label": name,
        "display_nickname": None,
        "broker_slug": "snaptrade",
    }


def test_alpaca_paper_row_is_paper_and_live_alpaca_is_not():
    paper = _row("Alpaca Paper Account")
    live = _row("Alpaca Account", "snaptrade:live")
    schwab = _row("Schwab Account", "snaptrade:schwab")
    assert is_paper_row(paper)
    assert not is_paper_row(live)
    assert not is_paper_row(schwab)
    assert paper_display_label(paper) == "Paper"
    assert paper_display_label(live) is None


def test_share_card_scope_drops_paper_unless_the_request_is_paper():
    import app.share_card as share_card
    from app import app

    rows = [
        {"tenant_id": "snaptrade:real", "account_name": "Schwab Account"},
        {"tenant_id": "snaptrade:paper", "account_name": "Alpaca Paper Account"},
    ]
    with app.test_request_context("/share/card.png"):
        with patch("app.share_card._owned_tenant_ids", return_value=["snaptrade:real", "snaptrade:paper"]), \
             patch("app.models.get_broker_tenants_for_user", return_value=rows):
            assert share_card._share_scope_ids(9) == ["snaptrade:real"]
    with app.test_request_context("/share/card.png?tenants=snaptrade:paper"):
        with patch("app.share_card._owned_tenant_ids", return_value=["snaptrade:real", "snaptrade:paper"]), \
             patch("app.models.get_broker_tenants_for_user", return_value=rows):
            assert share_card._share_scope_ids(9) == ["snaptrade:paper"]


def test_mixed_book_drops_paper_and_paper_only_stays():
    rows = {
        "snaptrade:real": _row("Schwab Account", "snaptrade:real"),
        "snaptrade:paper": _row("Alpaca Paper Account"),
    }
    assert drop_paper_from_mixed_book(
        ["snaptrade:real", "snaptrade:paper"], rows,
    ) == ["snaptrade:real"]
    assert drop_paper_from_mixed_book(["snaptrade:paper"], rows) == ["snaptrade:paper"]
    assert drop_paper_from_mixed_book(None, rows) is None


def test_scope_excludes_paper_unless_the_scope_is_paper():
    import app.routes as routes

    owned = [
        {"tenant_id": "snaptrade:real", "account_name": "Schwab Account"},
        {"tenant_id": "snaptrade:paper", "account_name": "Alpaca Paper Account"},
    ]
    user = SimpleNamespace(id=9, username="cam", is_authenticated=True)
    from app import app

    with app.test_request_context("/overview"):
        with patch.object(routes, "current_user", user), \
             patch.object(routes, "is_admin", lambda u: False), \
             patch.object(routes, "get_broker_tenants_for_user", lambda uid: owned):
            assert routes._tenants_for_scope("") == ["snaptrade:real"]

    with app.test_request_context("/overview?tenants=snaptrade:paper"):
        with patch.object(routes, "current_user", user), \
             patch.object(routes, "is_admin", lambda u: False), \
             patch.object(routes, "get_broker_tenants_for_user", lambda uid: owned):
            assert routes._tenants_for_scope("") == ["snaptrade:paper"]


def test_ticks_and_spx_link():
    assert limit_from_quote(1.154) == "1.15"
    assert limit_from_quote(1.155) == "1.16"
    assert option_tick("SPX", Decimal("2.90")) == Decimal("0.05")
    assert option_tick("SPXW", Decimal("3")) == Decimal("0.10")
    assert option_tick("AAPL", Decimal("2.90")) == Decimal("0.05")
    assert option_tick("AAPL", Decimal("3.10")) == Decimal("0.10")
    assert round_to_tick(Decimal("3.14"), "SPX") == Decimal("3.10")
    assert round_to_tick(Decimal("2.97"), "SPX") == Decimal("2.95")
    assert limit_from_quote("1.17", "IWM") == "1.15"
    assert position_link_symbol("SPX") == "SPXW"
    assert position_link_symbol("spy") == "SPY"
    assert _readout_symbol("brk.b") == "BRK.B"
    assert _readout_symbol("tqq1") == "TQQ1"


def test_copy_is_literal():
    views = trade_views(
        "SPY", "call", 500, "at", date(2026, 10, 2),
        "$3.50", "$350.00", "SPY   261002C00500000", False,
        as_of_label="the Sep 29 close",
    )
    note = views["brokerage"]["note"]
    assert "highest price a buyer is paying" in note
    assert "lowest price a seller will take" in note
    assert "what a seller will take" not in note
    assert "$3.50 x 100 = $350.00" in views["intermediate"]["note"]
    assert "premium for one contract" not in views["intermediate"]["note"]
    assert "Price per share" in {row["label"] for row in views["beginner"]["rows"]}
    assert "Total cost" in {row["label"] for row in views["brokerage"]["rows"]}
    text = practice_sentence("SPY", "call", 500, "at", date(2026, 10, 2), as_of_label="the Sep 29 close")
    assert "the Sep 29 close" in text
    assert "today's price" not in text
    spx = trade_views(
        "SPX", "call", 5000, "at", date(2026, 10, 2),
        "$3.50", "$350.00", "SPXW  261002C05000000", True,
    )
    assert "cash-settled" in spx["brokerage"]["note"]
    placed = views["beginner"]["note"]
    assert "was sent" not in placed
    from app.paper_practice import practice_receipt
    receipt = practice_receipt({
        "sentence": text,
        "symbol": "SPX",
        "side": "call",
        "strike": 5000,
        "expiry": "2026-10-02",
        "expiry_label": "Daily",
        "limit_label": "$3.50",
        "cost_label": "$350.00",
        "cash_settled": True,
        "views": spx,
    })
    assert receipt["link_symbol"] == "SPXW"
    assert "was sent" in receipt["views"]["beginner"]["note"]


def test_cancel_is_paper_only_and_demo_is_blocked():
    from app import app
    from flask import session
    from flask_login import login_user

    cancelled = []

    class _Viewer:
        is_authenticated = True
        is_active = True
        is_anonymous = False
        id = 7
        username = "ada"

        def get_id(self):
            return "7"

    with app.test_request_context(
        "/practice/orders/cancel",
        method="POST",
        data={"brokerage_order_id": "ord-1", "account_id": "attacker-account"},
    ):
        login_user(_Viewer())
        session["paper_practice_orders"] = [{
            "brokerage_order_id": "ord-1",
            "status": "open",
            "symbol": "SPY",
        }]
        with patch("app.paper_practice.alpaca_paper_trade_account", return_value=None), \
             patch("app.paper_practice.cancel_account_order", side_effect=lambda *a, **k: cancelled.append(a)):
            from app.paper_practice import paper_practice_cancel
            paper_practice_cancel.__wrapped__()
    assert cancelled == []

    with app.test_request_context(
        "/practice/orders/cancel",
        method="POST",
        data={"brokerage_order_id": "ord-9"},
    ):
        login_user(_Viewer())
        with patch("app.paper_practice.demo_block_writes", return_value=("blocked", 403)), \
             patch("app.paper_practice.cancel_account_order", side_effect=lambda *a, **k: cancelled.append(1)):
            from app.paper_practice import paper_practice_cancel
            resp = paper_practice_cancel.__wrapped__()
    assert resp[1] == 403
    assert cancelled == []

    account = {"snaptrade_account_id": "paper-acct", "row": {}}
    with app.test_request_context(
        "/practice/orders/cancel",
        method="POST",
        data={"brokerage_order_id": "ord-2", "account_id": "attacker-account"},
    ):
        login_user(_Viewer())
        session["paper_practice_orders"] = []
        with patch("app.paper_practice.alpaca_paper_trade_account", return_value=account), \
             patch("app.paper_practice.list_account_recent_orders", return_value=[
                 {"brokerage_order_id": "ord-2", "status": "PENDING", "option_symbol": {"underlying_symbol": "SPY"}},
             ]), \
             patch("app.paper_practice.cancel_account_order", side_effect=lambda *a, **k: cancelled.append(a)) as cancel:
            from app.paper_practice import paper_practice_cancel
            paper_practice_cancel.__wrapped__()
            cancel.assert_called_once()
            assert cancel.call_args.args[1] == "paper-acct"
            assert cancel.call_args.args[2] == "ord-2"


def test_order_status_buckets():
    assert order_status_bucket("EXECUTED") == "filled"
    assert order_status_bucket("CANCELLED") == "cancelled"
    assert order_status_bucket("CANCELED") == "cancelled"
    assert order_status_bucket("REJECTED") == "rejected"
    assert order_status_bucket("EXPIRED") == "expired"
    assert order_status_bucket("PENDING") == "open"
    assert order_status_bucket("SOMETHING_ELSE") == "unknown"


def test_beginner_readout_degrades_when_the_db_is_down():
    with patch("app.paper_practice._remember_voice", side_effect=RuntimeError("db down")):
        assert beginner_readouts(["snaptrade:paper"], "BRK.B") == []


def test_simple_nav_hides_advanced_items():
    from app import app
    from app.models import User

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
    client = app.test_client()
    with patch.object(User, "get_by_id", staticmethod(lambda user_id: user if str(user_id) == "7" else None)), \
         patch("app.paper_accounts.viewer_flags", return_value=(True, True)), \
         patch("app.paper_practice.snaptrade_enabled", return_value=False), \
         patch("app.paper_practice.latest_spots", return_value={}):
        with client.session_transaction() as sess:
            sess["_user_id"] = "7"
            sess["_fresh"] = True
        html = client.get("/practice").get_data(as_text=True)
    assert ">Practice<" in html
    assert ">Learn<" in html
    assert ">Overview<" in html
    assert ">Positions<" in html
    assert "Trader Profile" not in html
    assert "AI Insights" not in html
    assert ">Strategies<" not in html


def test_learn_links_to_practice():
    from app import app

    html = app.test_client().get("/learn").get_data(as_text=True)
    assert 'href="/practice"' in html
    assert "Practice a trade" in html
