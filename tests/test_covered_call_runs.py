"""Covered-call and wheel runs (app/covered_call_runs.py).

The position page groups a share lot with the calls written against it.
These tests use the same shape as the RKLB cycle described for the
feature: 100 shares bought at $69, five weekly calls that expire for
$931, then a $63 call assigned for $75 of premium and a $600 share loss.
Net of the whole run is +$406 when the fills have no fees. The whole-run
net subtracts broker fees; each call's amount stays the premium.
"""

from datetime import date

import pandas as pd

from app import app
from app.covered_call_runs import build_covered_call_runs


def _occ(root, expiry, cp, strike):
    strike8 = int(round(strike * 1000))
    return f"{root} {expiry:%y%m%d}{cp}{strike8:08d}"


def _row(when, action, instrument, symbol, qty, price, amount, **extra):
    row = {
        "trade_date": when,
        "action": action,
        "instrument_type": instrument,
        "trade_symbol": symbol,
        "quantity": qty,
        "price": price,
        "amount": amount,
        "fees": extra.pop("fees", 0.0),
        "account": extra.pop("account", "Schwab"),
        "tenant_id": extra.pop("tenant_id", "snaptrade:acct"),
    }
    row.update(extra)
    return row


def _frame(rows):
    return pd.DataFrame(rows)


def _rklb_cycle():
    """Analog of the RKLB covered-call cycle, on a stand-in symbol.

    100 shares at $69. Five expired weeklies keep $931. The $63 call
    expiring Apr 2, 2026 is assigned: option +$75, shares −$600.
    """
    rows = [
        _row(date(2026, 2, 20), "equity_buy", "Equity", "RKLB", 100, 69.0, -6900.0),
    ]
    weeklies = [
        (date(2026, 2, 23), date(2026, 2, 27), 70.0, 2.00, 200.0),
        (date(2026, 3, 2), date(2026, 3, 6), 68.0, 1.80, 180.0),
        (date(2026, 3, 9), date(2026, 3, 13), 67.0, 1.75, 175.0),
        (date(2026, 3, 16), date(2026, 3, 20), 66.0, 1.90, 190.0),
        (date(2026, 3, 23), date(2026, 3, 27), 65.0, 1.86, 186.0),
    ]
    for opened, expiry, strike, price, credit in weeklies:
        rows.append(_row(
            opened, "option_sell_to_open", "Call",
            _occ("RKLB", expiry, "C", strike), 1, price, credit,
        ))
    rows.append(_row(
        date(2026, 3, 30), "option_sell_to_open", "Call",
        _occ("RKLB", date(2026, 4, 2), "C", 63), 1, 0.75, 75.0,
    ))
    rows.append(_row(
        date(2026, 4, 2), "option_assigned", "Call",
        _occ("RKLB", date(2026, 4, 2), "C", 63), 1, None, 0.0,
    ))
    rows.append(_row(
        date(2026, 4, 2), "equity_sell", "Equity", "RKLB", 100, 63.0, 6300.0,
    ))
    return _frame(rows)


def test_rklb_analog_one_run_nets_406():
    runs = build_covered_call_runs(_rklb_cycle(), as_of=date(2026, 4, 3))
    assert len(runs) == 1
    run = runs[0]
    assert run["kind"] == "covered_call"
    assert run["status"] == "closed"
    assert run["share_sentence"] == "100 shares bought at $69. Called away at $63."
    assert run["premium_total"] == 1006.0
    assert run["share_pnl"] == -600.0
    assert run["net"] == 406.0
    assert run["premium_label"] == "Calls net"
    assert run["call_count"] == 6
    assert run["call_count_label"] == "6 calls"
    assert run["net_label"] == "Whole run"

    outcomes = [c["outcome"] for c in run["calls"]]
    assert outcomes == ["expired", "expired", "expired", "expired", "expired", "assigned"]
    groups = run["outcome_groups"]
    assert [g["outcome"] for g in groups] == ["expired", "assigned"]
    assert groups[0]["count_label"] == "5 calls"
    assert groups[0]["net"] == 931.0
    assert groups[1]["count_label"] == "1 call"
    assert groups[1]["net"] == 75.0
    assert [c["expiry"] for c in groups[0]["calls"]] == [
        "2026-02-27", "2026-03-06", "2026-03-13", "2026-03-20", "2026-03-27",
    ]
    assert run["calls"][4]["running_premium"] == 931.0
    assert run["calls"][5]["premium"] == 75.0
    assert run["calls"][5]["running_premium"] == 1006.0
    assert run["calls"][5]["title"] == "$63 call"
    assert run["calls"][5]["expiry_label"] == "Apr 2, 2026"


def test_explicit_expiry_matches_inferred_expiry():
    rows = [
        _row(date(2026, 2, 20), "equity_buy", "Equity", "RKLB", 100, 69.0, -6900.0),
        _row(
            date(2026, 2, 23), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 2, 27), "C", 70), 1, 2.00, 200.0,
        ),
        _row(
            date(2026, 2, 27), "option_expired", "Call",
            _occ("RKLB", date(2026, 2, 27), "C", 70), 1, 0.0, 0.0,
        ),
    ]
    runs = build_covered_call_runs(_frame(rows), as_of=date(2026, 3, 1))
    assert len(runs) == 1
    assert runs[0]["calls"][0]["outcome"] == "expired"
    assert runs[0]["calls"][0]["premium"] == 200.0
    assert runs[0]["status"] == "open"
    assert runs[0]["net_label"] == "Whole run so far"


def test_two_runs_when_shares_go_flat_and_are_bought_again():
    rows = [
        _row(date(2026, 1, 5), "equity_buy", "Equity", "RKLB", 100, 69.0, -6900.0),
        _row(
            date(2026, 1, 6), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 1, 16), "C", 70), 1, 1.00, 100.0,
        ),
        _row(
            date(2026, 1, 16), "option_assigned", "Call",
            _occ("RKLB", date(2026, 1, 16), "C", 70), 1, None, 0.0,
        ),
        _row(date(2026, 1, 16), "equity_sell", "Equity", "RKLB", 100, 70.0, 7000.0),
        _row(date(2026, 3, 2), "equity_buy", "Equity", "RKLB", 100, 40.0, -4000.0),
        _row(
            date(2026, 3, 3), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 3, 20), "C", 45), 1, 0.50, 50.0,
        ),
    ]
    current = pd.DataFrame([{
        "instrument_type": "Equity",
        "quantity": 100,
        "current_price": 45.0,
        "tenant_id": "snaptrade:acct",
        "account": "Schwab",
    }])
    runs = build_covered_call_runs(
        _frame(rows), current_df=current, as_of=date(2026, 3, 10),
    )
    assert len(runs) == 2
    closed, open_run = runs
    assert closed["status"] == "closed"
    assert closed["share_pnl"] == 100.0  # (70 - 69) * 100
    assert closed["premium_total"] == 100.0
    assert closed["net"] == 200.0
    assert open_run["status"] == "open"
    assert open_run["share_pnl"] == 500.0  # (45 - 40) * 100
    assert open_run["premium_total"] == 50.0
    assert open_run["net"] == 550.0
    assert open_run["calls"][0]["outcome"] == "open"
    assert "not locked in" not in open_run["share_sentence"]
    assert open_run["has_open_call"] is True
    assert runs[0]["label"].startswith("Run 1")
    assert runs[1]["label"].startswith("Run 2")


def test_partial_sale_stays_one_open_run():
    rows = [
        _row(date(2026, 1, 5), "equity_buy", "Equity", "RKLB", 200, 50.0, -10000.0),
        _row(
            date(2026, 1, 6), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 1, 16), "C", 55), 1, 1.00, 100.0,
        ),
        _row(date(2026, 1, 20), "equity_sell", "Equity", "RKLB", 100, 60.0, 6000.0),
    ]
    current = pd.DataFrame([{
        "instrument_type": "Equity",
        "quantity": 100,
        "current_price": 58.0,
        "tenant_id": "snaptrade:acct",
        "account": "Schwab",
    }])
    runs = build_covered_call_runs(
        _frame(rows), current_df=current, as_of=date(2026, 1, 21),
    )
    assert len(runs) == 1
    run = runs[0]
    assert run["status"] == "open"
    # Sold 100 at 60 (realized +1,000) and still hold 100 marked at 58 (+800).
    assert run["share_pnl"] == 1800.0
    assert run["premium_total"] == 100.0
    assert run["net"] == 1900.0
    assert run["share_sentence"] == (
        "200 shares bought at $50. Sold 100 shares at $60. 100 shares still held."
    )
    assert run["calls"][0]["partial"] is False


def test_partial_coverage_when_more_contracts_than_shares():
    rows = [
        _row(date(2026, 1, 5), "equity_buy", "Equity", "RKLB", 100, 50.0, -5000.0),
        _row(
            date(2026, 1, 6), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 2, 20), "C", 55), 2, 1.00, 200.0,
        ),
    ]
    runs = build_covered_call_runs(_frame(rows), as_of=date(2026, 1, 10))
    assert len(runs) == 1
    call = runs[0]["calls"][0]
    assert call["partial"] is True
    assert "Partly covered" in call["partial_note"]
    assert call["premium"] == 200.0
    assert "2 contracts" in call["title"]


def test_wheel_put_assignment_leads_into_shares():
    rows = [
        _row(
            date(2026, 6, 3), "option_sell_to_open", "Put",
            _occ("F", date(2026, 6, 21), "P", 12), 1, 0.60, 60.0,
        ),
        _row(
            date(2026, 6, 21), "option_assigned", "Put",
            _occ("F", date(2026, 6, 21), "P", 12), 1, None, 0.0,
        ),
        _row(date(2026, 6, 21), "equity_buy", "Equity", "F", 100, 12.0, -1200.0),
        _row(
            date(2026, 6, 24), "option_sell_to_open", "Call",
            _occ("F", date(2026, 7, 19), "C", 13), 1, 0.45, 45.0,
        ),
        _row(
            date(2026, 7, 19), "option_assigned", "Call",
            _occ("F", date(2026, 7, 19), "C", 13), 1, None, 0.0,
        ),
        _row(date(2026, 7, 19), "equity_sell", "Equity", "F", 100, 13.0, 1300.0),
    ]
    runs = build_covered_call_runs(_frame(rows), as_of=date(2026, 7, 20))
    assert len(runs) == 1
    run = runs[0]
    assert run["kind"] == "wheel"
    assert run["label"] == "Wheel"
    assert [c["outcome"] for c in run["calls"]] == ["assigned", "assigned"]
    assert [c["side"] for c in run["calls"]] == ["put", "call"]
    assert run["premium_total"] == 105.0
    assert run["share_pnl"] == 100.0
    assert run["net"] == 205.0
    assert run["share_sentence"] == "100 shares from the $12 put. Called away at $13."
    assert run["calls"][0]["running_premium"] == 60.0
    assert run["calls"][1]["running_premium"] == 105.0


def test_roll_is_its_own_outcome_and_the_new_call_is_a_new_row():
    rows = [
        _row(date(2026, 1, 2), "equity_buy", "Equity", "RKLB", 100, 50.0, -5000.0),
        _row(
            date(2026, 1, 2), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 1, 17), "C", 55), 1, 1.00, 100.0,
        ),
        _row(
            date(2026, 1, 10), "option_buy_to_close", "Call",
            _occ("RKLB", date(2026, 1, 17), "C", 55), 1, 0.20, -20.0,
        ),
        _row(
            date(2026, 1, 10), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 1, 24), "C", 60), 1, 0.90, 90.0,
        ),
    ]
    current = pd.DataFrame([{
        "instrument_type": "Equity",
        "quantity": 100,
        "current_price": 50.0,
        "tenant_id": "snaptrade:acct",
        "account": "Schwab",
    }])
    runs = build_covered_call_runs(
        _frame(rows), current_df=current, as_of=date(2026, 1, 25),
    )
    assert len(runs) == 1
    run = runs[0]
    assert [c["outcome"] for c in run["calls"]] == ["rolled", "expired"]
    assert [c["premium"] for c in run["calls"]] == [80.0, 90.0]
    assert [c["running_premium"] for c in run["calls"]] == [80.0, 170.0]
    assert [g["outcome"] for g in run["outcome_groups"]] == ["expired", "rolled"]
    assert [g["net"] for g in run["outcome_groups"]] == [90.0, 80.0]
    assert run["share_pnl"] == 0.0
    assert run["net"] == 170.0


def test_buy_to_close_without_a_new_call_is_closed():
    rows = [
        _row(date(2026, 1, 2), "equity_buy", "Equity", "RKLB", 100, 50.0, -5000.0),
        _row(
            date(2026, 1, 2), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 2, 20), "C", 55), 1, 1.00, 100.0,
        ),
        _row(
            date(2026, 1, 10), "option_buy_to_close", "Call",
            _occ("RKLB", date(2026, 2, 20), "C", 55), 1, 0.40, -40.0,
        ),
    ]
    runs = build_covered_call_runs(_frame(rows), as_of=date(2026, 1, 11))
    assert runs[0]["calls"][0]["outcome"] == "closed"
    assert runs[0]["calls"][0]["premium"] == 60.0


def test_fees_are_not_subtracted_from_premium():
    rows = [
        _row(date(2026, 1, 2), "equity_buy", "Equity", "RKLB", 100, 50.0, -5001.3, fees=1.3),
        _row(
            date(2026, 1, 2), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 1, 17), "C", 55), 1, 1.00, 99.0, fees=1.0,
        ),
    ]
    runs = build_covered_call_runs(_frame(rows), as_of=date(2026, 1, 4))
    assert runs[0]["calls"][0]["premium"] == 100.0
    assert runs[0]["fees"] == 2.3
    assert runs[0]["net"] == round(runs[0]["premium_total"] + runs[0]["share_pnl"] - runs[0]["fees"], 2)
    assert "after fees" in runs[0]["net_label"]
    # Share result uses the fill price, not the fee-netted cash amount.
    assert runs[0]["share_sentence"].startswith("100 shares bought at $50.")


def test_naked_call_while_flat_is_not_a_run():
    rows = [
        _row(
            date(2026, 1, 2), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 1, 17), "C", 55), 1, 1.00, 100.0,
        ),
        _row(
            date(2026, 1, 17), "option_expired", "Call",
            _occ("RKLB", date(2026, 1, 17), "C", 55), 1, 0.0, 0.0,
        ),
    ]
    assert build_covered_call_runs(_frame(rows), as_of=date(2026, 1, 20)) == []


def test_call_between_two_runs_stays_out():
    rows = [
        _row(date(2026, 1, 2), "equity_buy", "Equity", "RKLB", 100, 50.0, -5000.0),
        _row(
            date(2026, 1, 2), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 1, 17), "C", 55), 1, 1.00, 100.0,
        ),
        _row(date(2026, 1, 20), "equity_sell", "Equity", "RKLB", 100, 52.0, 5200.0),
        _row(
            date(2026, 2, 1), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 2, 20), "C", 60), 1, 1.00, 100.0,
        ),
        _row(date(2026, 3, 1), "equity_buy", "Equity", "RKLB", 100, 40.0, -4000.0),
        _row(
            date(2026, 3, 2), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 3, 20), "C", 45), 1, 0.80, 80.0,
        ),
    ]
    runs = build_covered_call_runs(_frame(rows), as_of=date(2026, 3, 5))
    assert len(runs) == 2
    assert len(runs[0]["calls"]) == 1
    assert runs[0]["calls"][0]["premium"] == 100.0
    assert len(runs[1]["calls"]) == 1
    assert runs[1]["calls"][0]["premium"] == 80.0


def test_tenants_do_not_share_a_run():
    rows = []
    for tenant, price in (("snaptrade:a", 10.0), ("snaptrade:b", 20.0)):
        rows.append(_row(
            date(2026, 1, 2), "equity_buy", "Equity", "RKLB", 100, price, -price * 100,
            tenant_id=tenant, account="Schwab Account",
        ))
        rows.append(_row(
            date(2026, 1, 3), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 1, 17), "C", price + 1), 1, 1.00, 100.0,
            tenant_id=tenant, account="Schwab Account",
        ))
    runs = build_covered_call_runs(
        _frame(rows),
        as_of=date(2026, 1, 4),
        label_map={"snaptrade:a": "Ira", "snaptrade:b": "Taxable"},
    )
    assert len(runs) == 2
    assert {r["tenant_id"] for r in runs} == {"snaptrade:a", "snaptrade:b"}
    assert all(r["show_account"] for r in runs)
    assert {r["account_label"] for r in runs} == {"Ira", "Taxable"}
    # Each lot keeps its own cost. They must not average into one $15 entry.
    sentences = {r["share_sentence"] for r in runs}
    assert "100 shares bought at $10. Still holding them." in sentences
    assert "100 shares bought at $20. Still holding them." in sentences


def test_split_adjusts_the_share_lot_before_the_later_sell():
    rows = [
        _row(date(2026, 1, 2), "equity_buy", "Equity", "XLU", 100, 20.0, -2000.0),
        _row(
            date(2026, 1, 12), "option_sell_to_open", "Call",
            _occ("XLU", date(2026, 1, 17), "C", 12), 1, 0.50, 50.0,
        ),
        _row(date(2026, 2, 2), "equity_sell", "Equity", "XLU", 200, 12.0, 2400.0),
    ]
    splits = pd.DataFrame([{
        "symbol": "XLU",
        "split_date": date(2026, 1, 10),
        "split_ratio": 2.0,
    }])
    runs = build_covered_call_runs(
        _frame(rows), splits_df=splits, as_of=date(2026, 2, 3),
    )
    assert len(runs) == 1
    # 100 shares at $20 become 200 after the split, sold at $12.
    # Share result = 200 * 12 - 100 * 20 = +400. Premium +50. Net +450.
    assert runs[0]["status"] == "closed"
    assert runs[0]["share_pnl"] == 400.0
    assert runs[0]["premium_total"] == 50.0
    assert runs[0]["net"] == 450.0
    assert runs[0]["share_sentence"] == "200 shares bought at $10. Sold at $12."
    assert runs[0]["price_note"] == "Prices are adjusted for the stock split."


def test_closed_run_states_split_adjusted_prices():
    """SCHD-shaped: the buy is pre-split, the sale is post-split."""
    rows = [
        _row(date(2024, 6, 3), "equity_buy", "Equity", "SCHD", 100, 82.40, -8240.0),
        _row(
            date(2024, 6, 10), "option_sell_to_open", "Call",
            _occ("SCHD", date(2024, 6, 21), "C", 28), 1, 0.40, 40.0,
        ),
        _row(date(2024, 11, 4), "equity_sell", "Equity", "SCHD", 300, 28.33, 8499.0),
    ]
    splits = pd.DataFrame([{
        "symbol": "SCHD",
        "split_date": date(2024, 10, 11),
        "split_ratio": 3.0,
    }])
    runs = build_covered_call_runs(
        _frame(rows), splits_df=splits, as_of=date(2024, 11, 5),
    )
    assert len(runs) == 1
    assert runs[0]["share_sentence"] == "300 shares bought at $27.47. Sold at $28.33."
    assert runs[0]["price_note"] == "Prices are adjusted for the stock split."
    assert "82.40" not in runs[0]["share_sentence"]


def test_synthetic_opening_balance_restores_pre_history_covered_call():
    call = _occ("RKLB", date(2026, 1, 17), "C", 15)
    rows = [
        _row(
            date(2026, 1, 2), "option_sell_to_open", "Call",
            call, 1, 1.0, 100.0,
        ),
        _row(
            date(2026, 1, 17), "option_expired", "Call",
            call, 1, 0.0, 0.0,
        ),
    ]
    opening = pd.DataFrame([{
        "tenant_id": "snaptrade:acct",
        "account": "Schwab",
        "symbol": "RKLB",
        "opening_date": date(2025, 12, 31),
        "opening_qty": 100.0,
        "est_amount": -1000.0,
        "price_source": "broker_cost_basis",
    }])
    current = pd.DataFrame([{
        "instrument_type": "Equity",
        "quantity": 100.0,
        "current_price": 12.0,
        "tenant_id": "snaptrade:acct",
        "account": "Schwab",
    }])
    runs = build_covered_call_runs(
        _frame(rows), current_df=current, opening_df=opening,
        as_of=date(2026, 1, 20),
    )
    assert len(runs) == 1
    assert runs[0]["status"] == "open"
    assert runs[0]["calls"][0]["outcome"] == "expired"
    assert runs[0]["share_pnl"] == 200.0
    assert runs[0]["premium_total"] == 100.0
    assert runs[0]["net"] == 300.0


def test_synthetic_opening_quantity_is_not_double_adjusted_for_splits():
    call = _occ("XLU", date(2026, 1, 17), "C", 12)
    rows = [
        _row(
            date(2026, 1, 12), "option_sell_to_open", "Call",
            call, 1, 0.50, 50.0,
        ),
        _row(
            date(2026, 1, 17), "option_expired", "Call",
            call, 1, 0.0, 0.0,
        ),
        _row(date(2026, 2, 2), "equity_sell", "Equity", "XLU", 200, 12.0, 2400.0),
    ]
    opening = pd.DataFrame([{
        "tenant_id": "snaptrade:acct",
        "account": "Schwab",
        "symbol": "XLU",
        "opening_date": date(2026, 1, 2),
        # Warehouse opening quantities are already in today's units.
        "opening_qty": 200.0,
        "est_amount": -2000.0,
        "price_source": "market_close",
    }])
    splits = pd.DataFrame([{
        "symbol": "XLU",
        "split_date": date(2026, 1, 10),
        "split_ratio": 2.0,
    }])
    runs = build_covered_call_runs(
        _frame(rows), opening_df=opening, splits_df=splits,
        as_of=date(2026, 2, 3),
    )
    assert len(runs) == 1
    assert runs[0]["status"] == "closed"
    assert runs[0]["share_pnl"] == 400.0
    assert runs[0]["premium_total"] == 50.0
    assert runs[0]["net"] == 450.0


def test_long_put_exercise_does_not_invent_a_wheel():
    rows = [
        _row(
            date(2026, 1, 2), "option_buy_to_open", "Put",
            _occ("RKLB", date(2026, 1, 17), "P", 12), 1, 0.40, -40.0,
        ),
        _row(
            date(2026, 1, 17), "option_exercised", "Put",
            _occ("RKLB", date(2026, 1, 17), "P", 12), 1, None, 0.0,
        ),
        _row(date(2026, 1, 17), "equity_sell", "Equity", "RKLB", 100, 12.0, 1200.0),
    ]
    assert build_covered_call_runs(_frame(rows), as_of=date(2026, 1, 20)) == []


def test_outcome_groups_sort_by_net_and_keep_date_order():
    """Expired, closed, assigned, and rolled land in one run.

    Group nets sort descending. Calls inside a group stay in open-date
    order, including when a later expired call is the one with the
    bigger credit.
    """
    rows = [
        _row(date(2026, 1, 2), "equity_buy", "Equity", "RKLB", 100, 50.0, -5000.0),
        _row(
            date(2026, 1, 5), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 2, 6), "C", 60), 1, 0.40, 40.0,
        ),
        _row(
            date(2026, 1, 6), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 3, 20), "C", 65), 1, 1.00, 100.0,
        ),
        _row(
            date(2026, 1, 20), "option_buy_to_close", "Call",
            _occ("RKLB", date(2026, 3, 20), "C", 65), 1, 0.30, -30.0,
        ),
        _row(
            date(2026, 2, 2), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 2, 20), "C", 70), 1, 0.90, 90.0,
        ),
        _row(
            date(2026, 3, 2), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 3, 21), "C", 55), 1, 2.00, 200.0,
        ),
        _row(
            date(2026, 3, 9), "option_buy_to_close", "Call",
            _occ("RKLB", date(2026, 3, 21), "C", 55), 1, 2.60, -260.0,
        ),
        _row(
            date(2026, 3, 9), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 4, 17), "C", 80), 1, 0.50, 50.0,
        ),
        _row(
            date(2026, 4, 1), "option_sell_to_open", "Call",
            _occ("RKLB", date(2026, 4, 18), "C", 52), 1, 0.30, 30.0,
        ),
        _row(
            date(2026, 4, 18), "option_assigned", "Call",
            _occ("RKLB", date(2026, 4, 18), "C", 52), 1, None, 0.0,
        ),
        _row(date(2026, 4, 18), "equity_sell", "Equity", "RKLB", 100, 52.0, 5200.0),
    ]
    runs = build_covered_call_runs(_frame(rows), as_of=date(2026, 4, 19))
    assert len(runs) == 1
    run = runs[0]
    groups = run["outcome_groups"]
    assert [g["outcome"] for g in groups] == ["expired", "closed", "assigned", "rolled"]
    assert [g["net"] for g in groups] == [180.0, 70.0, 30.0, -60.0]
    assert [g["count_label"] for g in groups] == ["3 calls", "1 call", "1 call", "1 call"]
    assert [c["title"] for c in groups[0]["calls"]] == ["$60 call", "$70 call", "$80 call"]
    assert sum(g["net"] for g in groups) == run["premium_total"] == 220.0
    assert sum(g["count"] for g in groups) == run["call_count"] == 6
    assert run["call_count_label"] == "6 calls"
    assert run["premium_label"] == "Calls net"

    with app.app_context():
        html = app.jinja_env.get_template("_covered_call_runs.html").render(
            covered_call_runs=runs,
            symbol="RKLB",
        )
    assert "Calls net" in html
    assert ">Net<" in html
    assert "Running premium" not in html
    assert "Premium" not in html
    assert "3 calls" in html
    assert "+$180.00" in html
    assert "-$60.00" in html
    order = [html.find(f'ht-run-pill-{name}') for name in (
        "expired", "closed", "assigned", "rolled",
    )]
    assert order == sorted(order)
    assert 'aria-expanded="false"' in html
    assert html.count('aria-expanded="false"') == 5
    assert 'aria-controls="ht-run-1-groups"' in html
    assert 'id="ht-run-1-groups"' in html
    assert 'aria-controls="ht-run-1-g-1"' in html
    assert 'id="ht-run-1-g-1"' in html
    assert "<details" in html
    assert " open>" not in html
    assert " open " not in html


def test_template_renders_the_run_numbers():
    runs = build_covered_call_runs(_rklb_cycle(), as_of=date(2026, 4, 3))
    with app.app_context():
        html = app.jinja_env.get_template("_covered_call_runs.html").render(
            covered_call_runs=runs,
            symbol="RKLB",
        )
    assert "Covered call runs" in html
    assert "100 shares bought at $69" in html
    assert "Called away at $63" in html
    assert "+$931.00" in html
    assert "+$75.00" in html
    assert "-$600.00" in html
    assert "+$406.00" in html
    assert "Expired" in html
    assert "Assigned" in html
    assert "The whole-run net includes broker fees" in html
    assert "Whole run" in html
    assert "Calls net" in html
    assert ">Net<" in html
    assert "Running premium" not in html
    assert "Premium" not in html
    assert 'aria-expanded="false"' in html
    assert " open>" not in html
    assert 'class="ht-run"' in html


def test_template_masks_multi_account_run_label_in_privacy_mode(monkeypatch):
    runs = build_covered_call_runs(
        _rklb_cycle(), as_of=date(2026, 4, 3),
        label_map={"snaptrade:acct": "Family IRA"},
    )
    # Account labels only render when the position spans multiple tenants.
    runs[0]["show_account"] = True
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: True)
    monkeypatch.setattr(
        "app.privacy.viewer_slots",
        lambda: (
            {"snaptrade:acct": "Account 2"},
            {"Family IRA": "Account 2"},
        ),
    )
    with app.test_request_context("/position/RKLB"):
        html = app.jinja_env.get_template("_covered_call_runs.html").render(
            covered_call_runs=runs,
            symbol="RKLB",
        )
    assert "Account 2" in html
    assert "Family IRA" not in html
