"""Covered-call and wheel runs (app/covered_call_runs.py).

The position page groups a share lot with the calls written against it.
These tests use the same shape as the RKLB cycle described for the
feature: 100 shares bought at $69, five weekly calls that expire for
$931, then a $63 call assigned for $75 of premium and a $600 share loss.
Net of the whole run is +$406. Fees stay out of the math.
"""

from datetime import date

import pandas as pd

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
    assert run["premium_label"] == "Calls"
    assert run["net_label"] == "Together"
    assert run["summary"] == "Sold 6 calls, Feb 20 – Apr 2"

    outcomes = [c["outcome"] for c in run["calls"]]
    assert outcomes == ["expired", "expired", "expired", "expired", "expired", "assigned"]
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
    assert runs[0]["net_label"] == "Together"
    assert runs[0]["premium_label"] == "Calls"


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


def test_template_renders_the_run_numbers():
    from pathlib import Path
    from app import app

    runs = build_covered_call_runs(_rklb_cycle(), as_of=date(2026, 4, 3))
    with app.app_context():
        html = app.jinja_env.get_template("_covered_call_runs.html").render(
            covered_call_runs=runs,
            symbol="RKLB",
        )
    assert "Covered call runs" in html
    assert "100 shares bought at $69" in html
    assert "Called away at $63" in html
    assert "+$200.00" in html
    assert "+$75.00" in html
    assert "-$600.00" in html
    assert "+$406.00" in html
    assert "Expired" in html
    assert "Assigned" in html
    assert "Broker fees are not included" in html
    assert "Together" in html
    assert "+$1,006.00" in html
    assert "Calls" in html
    assert "Shares" in html
    assert "Whole run" not in html
    assert "Premium kept" not in html
    assert "Running premium" not in html
    assert "ht-together-tip" in html
    assert 'class="ht-run"' in html
    assert "account_label(run.tenant_id)" in Path("app/templates/_covered_call_runs.html").read_text()
