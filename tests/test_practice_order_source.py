"""Practice lists every order status, not only fills from the last 24 hours.

SnapTrade's recent-orders call defaults to executed orders. An open paper
order never showed up, and cancel refused because that same list was empty.
"""
from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("HAPPYTRADER_SKIP_DB_INIT", "1")

from app.paper_practice import normalize_broker_order, order_status_bucket
from app.snaptrade import _fetch_recent_orders, list_account_recent_orders

_SNAP = {"snaptrade_user_id": "su", "snaptrade_secret": "ss"}

_OPEN = {
    "brokerage_order_id": "open-1",
    "status": "PENDING",
    "option_symbol": {"underlying_symbol": "SPY"},
}
_CANCELLED = {
    "brokerage_order_id": "can-1",
    "status": "CANCELLED",
    "option_symbol": {"underlying_symbol": "QQQ"},
}
_FILLED = {
    "brokerage_order_id": "fill-1",
    "status": "EXECUTED",
    "option_symbol": {"underlying_symbol": "IWM"},
}
_REJECTED = {
    "brokerage_order_id": "rej-1",
    "status": "REJECTED",
    "option_symbol": {"underlying_symbol": "AAPL"},
}
# Same id as the open order, with a stale executed status on the older feed.
_STALE_OPEN = {
    "brokerage_order_id": "open-1",
    "status": "EXECUTED",
    "option_symbol": {"underlying_symbol": "SPY"},
}


def _client(recent, older, *, fail_older=False):
    state = {"recent": [], "orders": [], "cancel": []}

    class _Info:
        @staticmethod
        def get_user_account_recent_orders(**kwargs):
            state["recent"].append(kwargs)
            if kwargs.get("only_executed") is False:
                return {"orders": list(recent)}
            return {"orders": []}

        @staticmethod
        def get_user_account_orders(**kwargs):
            state["orders"].append(kwargs)
            if fail_older:
                raise RuntimeError("account orders down")
            return list(older)

    class _Trading:
        @staticmethod
        def cancel_user_account_order(**kwargs):
            state["cancel"].append(kwargs)
            return {"status": "CANCELLED"}

    client = SimpleNamespace(
        account_information=_Info,
        trading=_Trading,
    )
    return client, state


def test_recent_orders_default_omits_only_executed():
    seen = []

    class _Info:
        @staticmethod
        def get_user_account_recent_orders(**kwargs):
            seen.append(kwargs)
            return {"orders": []}

    client = SimpleNamespace(account_information=_Info)
    assert _fetch_recent_orders(client, "u", "s", "acc") == []
    assert "only_executed" not in seen[0]

    assert _fetch_recent_orders(client, "u", "s", "acc", only_executed=False) == []
    assert seen[1]["only_executed"] is False


def test_list_includes_open_cancelled_filled_and_rejected():
    client, state = _client(
        [_OPEN],
        [_CANCELLED, _FILLED, _REJECTED, _STALE_OPEN],
    )
    with patch("app.snaptrade.get_snaptrade_user", return_value=_SNAP), \
         patch("app.snaptrade._get_snaptrade_client", return_value=client):
        rows = list_account_recent_orders(7, "paper-acct")

    assert state["recent"][0]["only_executed"] is False
    assert state["recent"][0]["account_id"] == "paper-acct"
    assert state["orders"][0]["state"] == "all"
    assert state["orders"][0]["days"] == 30
    ids = [row["brokerage_order_id"] for row in rows]
    assert ids == ["open-1", "can-1", "fill-1", "rej-1"]
    # The 24-hour row wins over the older feed's copy of the same id.
    assert rows[0]["status"] == "PENDING"
    buckets = [order_status_bucket(row["status"]) for row in rows]
    assert buckets == ["open", "cancelled", "filled", "rejected"]
    shown = [normalize_broker_order(row) for row in rows]
    assert [item["status"] for item in shown] == buckets
    assert shown[0]["cancelable"] is True
    assert shown[1]["cancelable"] is False


def test_accepted_option_shows_quantity_and_limit_from_the_leg():
    """A multi-leg order keeps the contract count on the leg, not the parent."""
    shown = normalize_broker_order({
        "brokerage_order_id": "ae608d51-aaaa-bbbb-cccc-ddddeeeeffff",
        "status": "ACCEPTED",
        "limit_price": "6.22",
        "option_symbol": {"underlying_symbol": "SPY"},
        "legs": [{
            "total_quantity": "1",
            "instrument": {"instrument_type": "OPTION", "symbol": "SPY   261002C00763000"},
        }],
    })
    assert shown["status"] == "open"
    assert shown["symbol"] == "SPY"
    assert shown["sentence"] == ""
    assert shown["quantity_label"] == "1 contract"
    assert shown["limit_label"] == "$6.22"
    assert shown["detail"] == "1 contract · Limit $6.22"


def test_older_feed_failure_keeps_the_recent_open_order():
    client, _state = _client([_OPEN], [], fail_older=True)
    with patch("app.snaptrade.get_snaptrade_user", return_value=_SNAP), \
         patch("app.snaptrade._get_snaptrade_client", return_value=client):
        rows = list_account_recent_orders(7, "paper-acct")
    assert [row["brokerage_order_id"] for row in rows] == ["open-1"]
    assert rows[0]["status"] == "PENDING"


def test_cancel_accepts_an_open_order_from_the_corrected_list():
    """Cancel must see the open order through the real list, not a patched one."""
    from app import app
    from flask import session
    from flask_login import login_user

    class _Viewer:
        is_authenticated = True
        is_active = True
        is_anonymous = False
        id = 7
        username = "ada"

        def get_id(self):
            return "7"

    client, state = _client(
        [_OPEN],
        [_CANCELLED, _FILLED, _REJECTED],
    )
    account = {"snaptrade_account_id": "paper-acct", "row": {}}
    with app.test_request_context(
        "/practice/orders/cancel",
        method="POST",
        data={"brokerage_order_id": "open-1", "account_id": "attacker-account"},
    ):
        login_user(_Viewer())
        session["paper_practice_orders"] = []
        with patch("app.paper_practice.alpaca_paper_trade_account", return_value=account), \
             patch("app.snaptrade.get_snaptrade_user", return_value=_SNAP), \
             patch("app.snaptrade._get_snaptrade_client", return_value=client):
            from app.paper_practice import paper_practice_cancel
            paper_practice_cancel.__wrapped__()
        flashes = " ".join(msg for _cat, msg in session.get("_flashes") or [])
        assert "not an open order" not in flashes
        assert state["cancel"]
        assert state["cancel"][0]["account_id"] == "paper-acct"
        assert state["cancel"][0]["brokerage_order_id"] == "open-1"
        assert all(call.get("only_executed") is False for call in state["recent"])
        assert all(call.get("state") == "all" for call in state["orders"])
        stored = session["paper_practice_orders"]
        assert stored[0]["brokerage_order_id"] == "open-1"
        assert stored[0]["cancelable"] is False
