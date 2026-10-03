"""Snapshot-only expiry rows recover quantity, open date, and kept %."""

from datetime import date

from app.position_detail import repair_snapshot_option_outcomes


def test_be_estimated_row_uses_the_sept_28_opening_fill():
    # The legs row was the snapshot contract: qty 0, opened on the expiry,
    # kept % blank. The fill that opened it is Sep 28.
    outcome = {
        "type": "option",
        "trade_symbol": "BE    261002C00297500",
        "direction": "Sold",
        "close_type": "Settled at expiry (est.)",
        "open_date": "2026-10-02",
        "close_date": "2026-10-02",
        "option_expiry": "2026-10-02",
        "quantity": 0,
        "premium_received": 0,
        "premium_paid": 0,
        "pnl": 430.0,
        "cost": 0,
        "proceeds": 0,
        "return_pct": None,
        "tenant_id": "snaptrade:be",
        "account": "Schwab Account",
    }
    fills = [{
        "trade_symbol": "BE 10/02/2026 297.50 C",
        "action": "option_sell_to_open",
        "trade_date": "2026-09-28",
        "quantity": 2,
        "amount": 430.0,
        "tenant_id": "snaptrade:be",
        "account": "Schwab Account",
    }]
    repaired = repair_snapshot_option_outcomes([outcome], fills)
    assert len(repaired) == 1
    row = repaired[0]
    assert row["quantity"] == 2
    assert row["quantity_display"] == "2"
    assert row["open_date"] == "2026-09-28"
    assert row["premium_received"] == 430.0
    assert row["return_pct"] == 100.0


def test_qty_already_set_still_uses_the_sept_28_open_and_kept_pct():
    # #209 put the snapshot quantity on the row. The open date stayed the
    # expiry and kept % stayed blank, because that open date was not equal
    # to a null close date and option_expiry was not on the legs query.
    outcome = {
        "type": "option",
        "trade_symbol": "BE    261002C00297500",
        "direction": "Sold",
        "close_type": "Settled at expiry (est.)",
        "open_date": "2026-10-02",
        "close_date": "",
        "option_expiry": "2026-10-02",
        "option_strike": 297.5,
        "option_type": "C",
        "underlying": "BE",
        "quantity": 2,
        "quantity_display": "2",
        "premium_received": 0,
        "premium_paid": 0,
        "pnl": 430.0,
        "cost": 0,
        "proceeds": 0,
        "return_pct": None,
        "tenant_id": "snaptrade:be",
        "account": "Schwab Account",
    }
    fills = [{
        "trade_symbol": "BE 10/02/2026 297.50 C",
        "action": "option_sell_to_open",
        "trade_date": "2026-09-28",
        "quantity": 2,
        "amount": 430.0,
        "symbol": "BE",
        "option_expiry": "2026-10-02",
        "option_strike": 297.5,
        "option_type": "C",
        "tenant_id": "snaptrade:be",
        "account": "Schwab Account",
    }]
    repaired = repair_snapshot_option_outcomes([outcome], fills)
    assert len(repaired) == 1
    row = repaired[0]
    assert row["quantity"] == 2
    assert row["open_date"] == "2026-09-28"
    assert row["premium_received"] == 430.0
    assert row["return_pct"] == 100.0


def test_unparsed_symbol_matches_the_fill_on_expiry_strike_and_type():
    outcome = {
        "type": "option",
        "trade_symbol": "CALL BE OCT 02 26 297.5",
        "direction": "Sold",
        "close_type": "Settled at expiry (est.)",
        "open_date": "2026-10-02",
        "close_date": "2026-10-03",
        "option_expiry": "2026-10-02",
        "option_strike": 297.5,
        "option_type": "Call",
        "underlying": "BE",
        "quantity": 2,
        "premium_received": 0,
        "premium_paid": 0,
        "pnl": 430.0,
        "return_pct": None,
        "tenant_id": "snaptrade:be",
    }
    fills = [{
        "trade_symbol": "not a contract",
        "action": "option_sell_to_open",
        "trade_date": "2026-09-28",
        "quantity": 2,
        "price": 2.15,
        "amount": 0,
        "symbol": "BE",
        "option_expiry": "2026-10-02",
        "option_strike": 297.5,
        "option_type": "C",
        "tenant_id": "snaptrade:be",
    }]
    repaired = repair_snapshot_option_outcomes([outcome], fills)
    row = repaired[0]
    assert row["open_date"] == "2026-09-28"
    assert row["premium_received"] == 430.0
    assert row["return_pct"] == 100.0


def test_priced_history_row_wins_over_a_qty_snapshot_duplicate():
    history = {
        "type": "option",
        "trade_symbol": "BE 10/02/2026 297.50 C",
        "direction": "Sold",
        "open_date": "2026-09-28",
        "close_date": "2026-10-02",
        "option_expiry": "2026-10-02",
        "quantity": 2,
        "premium_received": 430.0,
        "pnl": 430.0,
        "return_pct": 100.0,
        "tenant_id": "snaptrade:be",
        "underlying": "BE",
    }
    snapshot = {
        "type": "option",
        "trade_symbol": "BE    261002C00297500",
        "direction": "Sold",
        "close_type": "Settled at expiry (est.)",
        "open_date": "2026-10-02",
        "close_date": "2026-10-02",
        "option_expiry": "2026-10-02",
        "quantity": 2,
        "premium_received": 0,
        "premium_paid": 0,
        "pnl": 430.0,
        "return_pct": None,
        "tenant_id": "snaptrade:be",
        "underlying": "BE",
    }
    repaired = repair_snapshot_option_outcomes([history, snapshot], [])
    assert len(repaired) == 1
    assert repaired[0]["trade_symbol"] == "BE 10/02/2026 297.50 C"
    assert repaired[0]["open_date"] == "2026-09-28"
    assert repaired[0]["return_pct"] == 100.0


def test_empty_snapshot_row_is_dropped_when_history_is_present():
    history = {
        "type": "option",
        "trade_symbol": "BE 10/02/2026 297.50 C",
        "direction": "Sold",
        "open_date": "2026-09-28",
        "close_date": "2026-10-02",
        "option_expiry": "2026-10-02",
        "quantity": 2,
        "premium_received": 430.0,
        "pnl": 430.0,
        "tenant_id": "snaptrade:be",
    }
    snapshot = {
        **history,
        "trade_symbol": "BE    261002C00297500",
        "open_date": "2026-10-02",
        "quantity": 0,
        "premium_received": 0,
        "return_pct": None,
    }
    repaired = repair_snapshot_option_outcomes([history, snapshot], [])
    assert len(repaired) == 1
    assert repaired[0]["trade_symbol"] == "BE 10/02/2026 297.50 C"
    assert repaired[0]["open_date"] == "2026-09-28"


def test_utc_snapshot_open_cannot_be_after_the_expiry(monkeypatch):
    # Warehouse current_date() is UTC. After 8pm ET on Oct 2 the snapshot
    # date is Oct 3 while the contract closed on the expiry, Oct 2.
    monkeypatch.setattr(
        "app.position_detail._market_today", lambda: date(2026, 10, 3),
    )
    outcome = {
        "type": "option",
        "trade_symbol": "BE    261002C00297500",
        "direction": "Sold",
        "close_type": "Settled at expiry (est.)",
        "open_date": "2026-10-03",
        "close_date": "2026-10-02",
        "option_expiry": "2026-10-02",
        "quantity": 2,
        "premium_received": 430.0,
        "premium_paid": 0,
        "pnl": 430.0,
        "return_pct": 100.0,
        "tenant_id": "snaptrade:be",
    }
    repaired = repair_snapshot_option_outcomes([outcome], [])
    assert repaired[0]["open_date"] == "2026-10-02"
    assert repaired[0]["days_held"] == 0
