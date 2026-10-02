"""Brokerage status on /practice: labels, poll cache, fill sync."""
from unittest.mock import patch

import pytest

from app.paper_practice import (
    ORDERS_KEY,
    SENT_KEY,
    TICKET_KEY,
    _last_order_at,
    brokerage_status_label,
    order_status_bucket,
    paper_fill_notices,
    practice_receipt,
    reset_order_status_state,
)


def _ticket(**extra):
    base = {
        "nonce": "nonce-1",
        "occ": "SPY   261016C00500000",
        "limit_price": "1.16",
        "cost": "116.00",
        "sentence": "You are buying a call on SPY.",
        "side": "call",
        "symbol": "SPY",
        "strike": 500,
        "expiry": "2026-10-16",
        "expiry_label": "Weekly",
        "limit_label": "$1.16",
        "cost_label": "$116.00",
        "account_id": "learner-acct",
    }
    base.update(extra)
    return base


def _viewer():
    class _Viewer:
        is_authenticated = True
        is_active = True
        is_anonymous = False
        id = 7

        def get_id(self):
            return "7"

    return _Viewer()


def _account():
    return {
        "snaptrade_account_id": "learner-acct",
        "tenant_id": "snaptrade:learner",
        "account_name": "Practice",
        "row": {"snaptrade_account_id": "learner-acct"},
    }


@pytest.fixture(autouse=True)
def _clean_status_state():
    reset_order_status_state()
    _last_order_at.clear()
    yield
    reset_order_status_state()
    _last_order_at.clear()


def test_brokerage_status_words():
    assert order_status_bucket("PENDING") == "open"
    assert order_status_bucket("CANCELED") == "cancelled"
    assert brokerage_status_label("ACCEPTED", session_open=True) == "Accepted"
    assert brokerage_status_label("PENDING", session_open=True) == "Pending"
    assert brokerage_status_label("QUEUED", session_open=True) == "Queued for next session"
    assert brokerage_status_label("ACCEPTED", session_open=False) == "Queued for next session"
    assert brokerage_status_label("PENDING", session_open=False) == "Queued for next session"
    assert brokerage_status_label("EXECUTED", execution_price="1.16") == "Filled at $1.16"
    assert brokerage_status_label("EXECUTED") == "Filled"
    assert brokerage_status_label(
        "REJECTED", reject_reason="insufficient buying power",
    ) == "Rejected: insufficient buying power"
    assert brokerage_status_label("CANCELED") == "Canceled"
    assert brokerage_status_label("CANCEL_PENDING") == "Cancel requested"


def test_receipt_uses_the_place_response():
    accepted = practice_receipt(
        _ticket(), {"status": "ACCEPTED", "brokerage_order_id": "ord-1"},
    )
    assert accepted["status"] == "open"
    assert accepted["status_label"] in {"Accepted", "Queued for next session"}
    assert accepted["brokerage_order_id"] == "ord-1"

    filled = practice_receipt(
        _ticket(),
        {"status": "EXECUTED", "brokerage_order_id": "ord-2", "execution_price": "1.16"},
    )
    assert filled["status"] == "filled"
    assert filled["status_label"] == "Filled at $1.16"

    rejected = practice_receipt(
        _ticket(),
        {
            "status": "REJECTED",
            "brokerage_order_id": "ord-3",
            "rejection_reason": "insufficient buying power",
        },
    )
    assert rejected["status"] == "rejected"
    assert rejected["status_label"] == "Rejected: insufficient buying power"


def test_confirmed_place_labels_accepted_and_syncs_once(monkeypatch):
    from flask import session
    from flask_login import login_user

    from app import app
    from app.paper_practice import paper_practice_place

    syncs = []
    monkeypatch.setattr("app.paper_practice.regular_session_open", lambda now=None: True)
    monkeypatch.setattr("app.paper_practice.alpaca_paper_trade_account", lambda user_id: _account())
    monkeypatch.setattr(
        "app.paper_practice.place_single_leg_option_order",
        lambda *args, **kwargs: {"brokerage_order_id": "ord-1", "status": "ACCEPTED"},
    )
    monkeypatch.setattr(
        "app.paper_practice.queue_account_read_sync",
        lambda user_id, row: syncs.append((user_id, row)),
    )
    with app.test_request_context("/practice/place", method="POST", data={"nonce": "nonce-1"}):
        login_user(_viewer())
        session[TICKET_KEY] = _ticket()
        resp = paper_practice_place.__wrapped__()
        assert resp.status_code == 302
        assert "placed=1" in resp.location
        assert session[SENT_KEY]["status_label"] == "Accepted"
        assert session[SENT_KEY]["status"] == "open"
    assert syncs == [(7, {"snaptrade_account_id": "learner-acct"})]


def test_rejected_receipt_does_not_promise_a_position():
    from flask import render_template

    from app import app

    rejected = practice_receipt(
        _ticket(),
        {
            "brokerage_order_id": "ord-r",
            "status": "REJECTED",
            "rejection_reason": "insufficient buying power",
        },
    )
    filled = practice_receipt(
        _ticket(),
        {
            "brokerage_order_id": "ord-f",
            "status": "EXECUTED",
            "execution_price": "1.16",
        },
    )
    with app.test_request_context("/practice?placed=1"):
        rejected_html = render_template("_paper_trade_receipt.html", sent=rejected)
        filled_html = render_template("_paper_trade_receipt.html", sent=filled)
    assert "Rejected: insufficient buying power" in rejected_html
    assert "before the fill syncs" not in rejected_html
    assert "before the fill syncs" in filled_html


def test_rejected_place_confirms_without_syncing(monkeypatch):
    from flask import session
    from flask_login import login_user

    from app import app
    from app.paper_practice import paper_practice_place

    syncs = []
    monkeypatch.setattr("app.paper_practice.alpaca_paper_trade_account", lambda user_id: _account())
    monkeypatch.setattr(
        "app.paper_practice.place_single_leg_option_order",
        lambda *args, **kwargs: {
            "brokerage_order_id": "ord-r",
            "status": "REJECTED",
            "rejection_reason": "insufficient buying power",
        },
    )
    monkeypatch.setattr(
        "app.paper_practice.queue_account_read_sync",
        lambda user_id, row: syncs.append(row),
    )
    with app.test_request_context("/practice/place", method="POST", data={"nonce": "nonce-1"}):
        login_user(_viewer())
        session[TICKET_KEY] = _ticket()
        resp = paper_practice_place.__wrapped__()
        assert resp.status_code == 302
        assert "placed=1" in resp.location
        assert session[SENT_KEY]["status"] == "rejected"
        assert session[SENT_KEY]["status_label"] == "Rejected: insufficient buying power"
    assert syncs == []


def test_filled_place_syncs_once(monkeypatch):
    from flask import session
    from flask_login import login_user

    from app import app
    from app.paper_practice import maybe_sync_filled_order, paper_practice_place

    syncs = []
    monkeypatch.setattr("app.paper_practice.alpaca_paper_trade_account", lambda user_id: _account())
    monkeypatch.setattr(
        "app.paper_practice.place_single_leg_option_order",
        lambda *args, **kwargs: {
            "brokerage_order_id": "ord-f",
            "status": "EXECUTED",
            "execution_price": "1.16",
        },
    )
    monkeypatch.setattr(
        "app.paper_practice.queue_account_read_sync",
        lambda user_id, row: syncs.append(user_id),
    )
    with app.test_request_context("/practice/place", method="POST", data={"nonce": "nonce-1"}):
        login_user(_viewer())
        session[TICKET_KEY] = _ticket()
        paper_practice_place.__wrapped__()
        assert session[SENT_KEY]["status_label"] == "Filled at $1.16"
        again = maybe_sync_filled_order(7, _account(), "ord-f")
    assert syncs == [7]
    assert again is False


def test_place_rate_limit_stays_on_confirm(monkeypatch):
    from flask import session
    from flask_login import login_user

    from app import app
    from app.paper_practice import paper_practice_place

    monkeypatch.setattr("app.paper_practice.alpaca_paper_trade_account", lambda user_id: _account())
    monkeypatch.setattr(
        "app.paper_practice.place_single_leg_option_order",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("429 Too Many Requests")),
    )
    monkeypatch.setattr(
        "app.paper_practice.queue_account_read_sync",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("synced")),
    )
    with app.test_request_context("/practice/place", method="POST", data={"nonce": "nonce-1"}):
        login_user(_viewer())
        session[TICKET_KEY] = _ticket()
        resp = paper_practice_place.__wrapped__()
        assert "confirm=1" in resp.location
        from flask import get_flashed_messages
        messages = get_flashed_messages()
    assert any("busy" in message for message in messages)
    assert all("429" not in message for message in messages)


def test_status_poll_is_cached_and_rate_limit_is_friendly(monkeypatch):
    from flask_login import login_user

    from app import app
    from app.paper_practice import _status_cache, paper_practice_order_status

    calls = []

    def _list(user_id, account_id, raise_on_error=False):
        calls.append(1)
        if len(calls) > 1:
            raise RuntimeError("429 Too Many Requests")
        return [{
            "brokerage_order_id": "ae608d51-1111-2222-3333-444444444444",
            "status": "ACCEPTED",
            "option_symbol": {"underlying_symbol": "SPY"},
            "total_quantity": "1.000000000000000000",
            "limit_price": "6.23",
        }]

    monkeypatch.setattr("app.paper_practice.regular_session_open", lambda now=None: True)
    monkeypatch.setattr("app.paper_practice.alpaca_paper_trade_account", lambda user_id: _account())
    monkeypatch.setattr("app.paper_practice.list_account_recent_orders", _list)
    monkeypatch.setattr("app.paper_practice.queue_account_read_sync", lambda *args, **kwargs: None)

    with app.test_request_context("/practice/orders/status"):
        login_user(_viewer())
        first = paper_practice_order_status.__wrapped__()
        second = paper_practice_order_status.__wrapped__()
    assert len(calls) == 1
    body = first.get_json()
    assert body["orders"][0]["status_label"] == "Accepted"
    assert body["orders"][0]["quantity_label"] == "1 contract"
    assert body["orders"][0]["limit_label"] == "$6.23"
    assert body["orders"][0]["detail"] == "1 contract · Limit $6.23"
    assert body["poll_after_ms"] == 15000
    from app.paper_practice import _STATUS_CACHE_SECONDS
    assert _STATUS_CACHE_SECONDS >= 15
    assert "snaptrade_account_id" not in body["orders"][0]
    assert second.get_json()["orders"][0]["status"] == "open"

    for entry in _status_cache.values():
        entry["at"] = 0
    with app.test_request_context("/practice/orders/status"):
        login_user(_viewer())
        limited = paper_practice_order_status.__wrapped__()
    payload = limited.get_json()
    assert "busy" in payload["error"]
    assert "429" not in payload["error"]
    assert payload["orders"][0]["status_label"] == "Accepted"
    assert payload["poll_after_ms"] == 15000


def test_only_a_new_fill_syncs_the_paper_account(monkeypatch):
    from flask import session

    from app import app
    from app.paper_practice import merged_paper_orders

    syncs = []
    monkeypatch.setattr("app.paper_practice.alpaca_paper_trade_account", lambda user_id: _account())
    monkeypatch.setattr(
        "app.paper_practice.queue_account_read_sync",
        lambda user_id, row: syncs.append(user_id),
    )
    old = [{
        "brokerage_order_id": "old-fill",
        "status": "EXECUTED",
        "execution_price": "2.00",
        "option_symbol": {"underlying_symbol": "QQQ"},
    }]
    with app.test_request_context("/practice"):
        session[ORDERS_KEY] = []
        monkeypatch.setattr("app.paper_practice.list_account_recent_orders", lambda *a, **k: old)
        merged_paper_orders(7)
    assert syncs == []

    fresh = [{
        "brokerage_order_id": "ord-f",
        "status": "EXECUTED",
        "execution_price": "1.16",
        "option_symbol": {"underlying_symbol": "SPY"},
    }]
    with app.test_request_context("/practice"):
        session[ORDERS_KEY] = [{
            "brokerage_order_id": "ord-f",
            "status": "open",
            "status_label": "Accepted",
            "symbol": "SPY",
            "snaptrade_account_id": "learner-acct",
        }]
        monkeypatch.setattr("app.paper_practice.list_account_recent_orders", lambda *a, **k: fresh)
        shown = merged_paper_orders(7)
        assert shown[0]["status"] == "filled"
        assert shown[0]["status_label"] == "Filled at $1.16"
        merged_paper_orders(7)
    assert syncs == [7]


def test_open_orders_panel_and_poll_script(monkeypatch):
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
    monkeypatch.setattr(
        User, "get_by_id", staticmethod(lambda user_id: user if str(user_id) == "7" else None),
    )
    monkeypatch.setattr("app.paper_practice.latest_spots", lambda: {})
    monkeypatch.setattr(
        "app.paper_practice.merged_paper_orders",
        lambda user_id, **kwargs: [{
            "brokerage_order_id": "ord-1",
            "status": "open",
            "status_label": "Accepted",
            "symbol": "SPY",
            "link_symbol": "SPY",
            "sentence": "You are buying a call on SPY.",
            "detail": "1 contract · Limit $6.23",
            "cancelable": True,
        }],
    )
    client = app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = "7"
        sess["_fresh"] = True
    html = client.get("/practice").get_data(as_text=True)
    assert "Open orders" in html
    assert "Accepted" in html
    assert "1 contract" in html
    assert "Limit $6.23" in html
    assert ">Cancel<" in html
    assert "/practice/orders/status" in html
    assert "paper-orders.js" in html
    script = open("app/static/js/paper-orders.js", encoding="utf-8").read()
    assert "return 3000;" not in script
    assert "return 15000" in script
    assert "return 30000" in script
    assert "8 * 60 * 1000" in script
    assert "visibilityState" in script
    assert 'data-order-status="open"' in script
    assert "if (hasOpen()) schedule(15000)" in script


def test_paper_fill_notice_skips_symbols_already_listed(monkeypatch):
    from flask import session

    from app import app

    syncs = []
    monkeypatch.setattr(
        "app.snaptrade.get_snaptrade_accounts",
        lambda user_id: [{"snaptrade_account_id": "learner-acct"}],
    )
    monkeypatch.setattr(
        "app.paper_practice.queue_account_read_sync",
        lambda user_id, row: syncs.append(row["snaptrade_account_id"]),
    )
    with app.test_request_context("/positions"):
        session[ORDERS_KEY] = [
            {
                "status": "filled",
                "symbol": "SPY",
                "link_symbol": "SPY",
                "brokerage_order_id": "ord-f",
                "snaptrade_account_id": "learner-acct",
            },
            {
                "status": "filled",
                "symbol": "QQQ",
                "link_symbol": "QQQ",
                "brokerage_order_id": "ord-q",
                "snaptrade_account_id": "learner-acct",
            },
        ]
        notices = paper_fill_notices(7, ["QQQ"])
        paper_fill_notices(7, ["QQQ"])
    assert [item["symbol"] for item in notices] == ["SPY"]
    assert notices[0]["sentence"] == "SPY filled on the paper account. Syncing that account now."
    assert syncs == ["learner-acct"]


def test_positions_names_a_filled_paper_trade_without_a_pnl_row():
    from flask import render_template

    from app import app
    from app.positions_page import ERROR_DEFAULTS

    ctx = dict(ERROR_DEFAULTS)
    ctx["paper_fills"] = [{
        "symbol": "SPY",
        "sentence": "SPY filled on the paper account. Syncing that account now.",
    }]
    with app.test_request_context("/positions"):
        html = render_template("positions.html", **ctx)
    assert "SPY filled on the paper account. Syncing that account now." in html
    assert 'href="/position/SPY"' in html
