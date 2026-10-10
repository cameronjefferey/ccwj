"""All-in cost per share: holding cycle, rolls, and spread grouping."""

from pathlib import Path

from app.all_in_cost import all_in_cost

ROOT = Path(__file__).resolve().parents[1]


def _fill(**kwargs):
    row = {
        "tenant_id": "snaptrade:ddog",
        "symbol": "DDOG",
        "fees": 0,
    }
    row.update(kwargs)
    return row


def _ddog_puts():
    """4.75 credit, rolled for a net 3.57, then assigned at $275."""
    return [
        # An earlier put that expired while flat. It is not this cycle.
        _fill(
            trade_date="2026-09-01",
            action="option_sell_to_open",
            option_type="put",
            option_strike=100,
            option_expiry="2026-09-18",
            quantity=1,
            price=1.00,
        ),
        _fill(
            trade_date="2026-09-18",
            action="option_expired",
            option_type="put",
            option_strike=100,
            option_expiry="2026-09-18",
            quantity=1,
            price=0,
        ),
        _fill(
            trade_date="2026-10-02",
            action="option_sell_to_open",
            option_type="put",
            option_strike=275,
            option_expiry="2026-10-09",
            quantity=1,
            price=4.75,
        ),
        _fill(
            trade_date="2026-10-09",
            action="option_buy_to_close",
            option_type="put",
            option_strike=275,
            option_expiry="2026-10-09",
            quantity=1,
            price=1.18,
        ),
        _fill(
            trade_date="2026-10-09",
            action="option_sell_to_open",
            option_type="put",
            option_strike=275,
            option_expiry="2026-10-16",
            quantity=1,
            price=4.75,
        ),
        _fill(
            trade_date="2026-10-16",
            action="option_assigned",
            option_type="put",
            option_strike=275,
            option_expiry="2026-10-16",
            quantity=1,
            price=0,
        ),
        # Schwab also posts the share buy. It must not double the shares.
        _fill(
            trade_date="2026-10-16",
            action="equity_buy",
            quantity=100,
            price=275,
        ),
    ]


def test_ddog_roll_then_assignment_is_266_68():
    result = all_in_cost(
        _ddog_puts(), shares=100, cost_basis=27500, symbol="DDOG",
    )
    assert result["replay_shares"] == 100
    assert result["cycle_start"] == "2026-10-02"
    assert result["equity_per_share"] == 275.00
    assert result["all_in_per_share"] == 266.68
    assert result["net_premium"] == 832.00
    rolls = [group for group in result["groups"] if group["kind"] == "roll"]
    assert len(rolls) == 1
    assert rolls[0]["cash"] == 357.00
    assert rolls[0]["bucket"] == "puts_before_assignment"
    opens = [group for group in result["groups"] if group["kind"] == "option"]
    assert len(opens) == 1
    assert opens[0]["cash"] == 475.00
    assert result["buckets"]["puts_before_assignment"] == 832.00
    assert result["buckets"]["calls"] == 0
    puts = result["lines"][0]
    assert puts["label"] == "Puts before assignment"
    assert puts["per_share"] == 8.32
    assert puts["credit"] is True
    ids = [fill_id for group in result["groups"] for fill_id in group["fill_ids"]]
    assert len(ids) == len(set(ids))
    assert result["show_all_in"] is True
    assert result["open_options"] is False


def test_ddog_covered_call_lowers_all_in_and_stays_open():
    fills = _ddog_puts() + [
        _fill(
            trade_date="2026-10-20",
            action="option_sell_to_open",
            option_type="call",
            option_strike=290,
            option_expiry="2026-10-30",
            quantity=1,
            price=2.00,
        ),
    ]
    result = all_in_cost(fills, shares=100, cost_basis=27500, symbol="DDOG")
    assert result["equity_per_share"] == 275.00
    assert result["all_in_per_share"] == 264.68
    assert result["net_premium"] == 1032.00
    calls = next(line for line in result["lines"] if line["bucket"] == "calls")
    puts = next(line for line in result["lines"] if line["bucket"] == "puts_before_assignment")
    assert calls["per_share"] == 2.00
    assert puts["per_share"] == 8.32
    assert result["open_options"] is True
    assert "not final" in result["note"]
    assert "not final" in result["hover"]


def test_covered_call_only_position():
    fills = [
        _fill(
            symbol="XYZ",
            trade_date="2026-03-02",
            action="equity_buy",
            quantity=100,
            price=50,
        ),
        _fill(
            symbol="XYZ",
            trade_date="2026-03-03",
            action="option_sell_to_open",
            option_type="call",
            option_strike=55,
            option_expiry="2026-03-20",
            quantity=1,
            price=2.00,
        ),
    ]
    result = all_in_cost(fills, shares=100, cost_basis=5000, symbol="XYZ")
    assert result["equity_per_share"] == 50.00
    assert result["all_in_per_share"] == 48.00
    assert result["buckets"]["puts_before_assignment"] == 0
    assert result["buckets"]["calls"] == 200.00
    assert result["lines"][0]["label"] == "Calls"
    assert result["lines"][0]["per_share"] == 2.00
    assert result["show_all_in"] is True


def test_no_option_activity_omits_the_second_number():
    fills = [
        _fill(
            symbol="ABC",
            trade_date="2026-01-05",
            action="equity_buy",
            quantity=100,
            price=40,
        ),
    ]
    result = all_in_cost(fills, shares=100, cost_basis=4000, symbol="ABC")
    assert result["equity_per_share"] == 40.00
    assert result["all_in_per_share"] == 40.00
    assert result["show_all_in"] is False
    assert result["groups"] == []
    assert result["has_option_activity"] is False
    assert result["open_options"] is False
    assert result["note"] == ""


def test_cycle_resets_when_shares_go_to_zero():
    first_cycle = [
        _fill(symbol="QQQ", trade_date="2026-02-02", action="equity_buy", quantity=100, price=100),
        _fill(
            symbol="QQQ",
            trade_date="2026-02-03",
            action="option_sell_to_open",
            option_type="call",
            option_strike=110,
            option_expiry="2026-02-20",
            quantity=1,
            price=3.00,
        ),
        _fill(symbol="QQQ", trade_date="2026-02-18", action="equity_sell", quantity=100, price=108),
        _fill(symbol="QQQ", trade_date="2026-04-01", action="equity_buy", quantity=100, price=80),
    ]
    flat_then_new = all_in_cost(
        first_cycle, shares=100, cost_basis=8000, symbol="QQQ",
    )
    assert flat_then_new["cycle_start"] == "2026-04-01"
    assert flat_then_new["all_in_per_share"] == 80.00
    assert flat_then_new["equity_per_share"] == 80.00
    assert flat_then_new["show_all_in"] is False
    assert flat_then_new["net_premium"] == 0
    assert flat_then_new["open_options"] is False
    assert flat_then_new["replay_shares"] == 100

    with_new_call = first_cycle + [
        _fill(
            symbol="QQQ",
            trade_date="2026-04-02",
            action="option_sell_to_open",
            option_type="call",
            option_strike=90,
            option_expiry="2026-04-17",
            quantity=1,
            price=1.00,
        ),
    ]
    reopened = all_in_cost(with_new_call, shares=100, cost_basis=8000, symbol="QQQ")
    assert reopened["all_in_per_share"] == 79.00
    assert reopened["net_premium"] == 100.00
    assert reopened["buckets"]["calls"] == 100.00
    assert reopened["open_options"] is True


def test_spread_is_one_group_and_cash_is_not_double_counted():
    fills = [
        _fill(symbol="SPY", trade_date="2026-05-01", action="equity_buy", quantity=100, price=100),
        _fill(
            symbol="SPY",
            trade_date="2026-05-04",
            action="option_sell_to_open",
            option_type="call",
            option_strike=110,
            option_expiry="2026-05-15",
            quantity=1,
            price=3.00,
        ),
        _fill(
            symbol="SPY",
            trade_date="2026-05-04",
            action="option_buy_to_open",
            option_type="call",
            option_strike=120,
            option_expiry="2026-05-15",
            quantity=1,
            price=1.00,
        ),
    ]
    result = all_in_cost(fills, shares=100, cost_basis=10000, symbol="SPY")
    assert len(result["groups"]) == 1
    group = result["groups"][0]
    assert group["kind"] == "spread"
    assert group["cash"] == 200.00
    assert group["bucket"] == "calls"
    assert len(group["fill_ids"]) == len(set(group["fill_ids"])) == 2
    assert sum(item["cash"] for item in result["groups"]) == 200.00
    assert result["all_in_per_share"] == 98.00
    assert result["equity_per_share"] == 100.00
    assert result["net_premium"] == 200.00


def test_fees_reduce_a_credit_once():
    puts = [
        _fill(
            trade_date="2026-10-02",
            action="option_sell_to_open",
            option_type="put",
            option_strike=275,
            option_expiry="2026-10-16",
            quantity=1,
            price=4.75,
            fees=0.65,
        ),
        _fill(
            trade_date="2026-10-16",
            action="option_assigned",
            option_type="put",
            option_strike=275,
            option_expiry="2026-10-16",
            quantity=1,
            price=0,
        ),
        _fill(trade_date="2026-10-16", action="equity_buy", quantity=100, price=275),
    ]
    gross_fee = all_in_cost(puts, shares=100, cost_basis=27500, symbol="DDOG")
    assert gross_fee["net_premium"] == 474.35
    assert gross_fee["all_in_per_share"] == 270.26
    assert round(gross_fee["equity_per_share"] - gross_fee["all_in_per_share"], 2) == 4.74

    already_net = [
        dict(puts[0], amount=474.35),
        puts[1],
        puts[2],
    ]
    net_fee = all_in_cost(already_net, shares=100, cost_basis=27500, symbol="DDOG")
    assert net_fee["net_premium"] == 474.35
    assert net_fee["all_in_per_share"] == 270.26


def test_assignment_without_a_duplicate_share_row_still_counts_the_put():
    fills = [
        _fill(
            trade_date="2026-10-02",
            action="option_sell_to_open",
            option_type="put",
            option_strike=275,
            option_expiry="2026-10-16",
            quantity=1,
            price=4.75,
        ),
        _fill(trade_date="2026-10-16", action="equity_buy", quantity=100, price=275),
    ]
    result = all_in_cost(fills, shares=100, cost_basis=27500, symbol="DDOG")
    assert result["replay_shares"] == 100
    assert result["cycle_start"] == "2026-10-02"
    assert result["all_in_per_share"] == 270.25


def test_demo_tenant_uses_the_same_formula():
    fills = []
    for row in _ddog_puts():
        copied = dict(row)
        copied["tenant_id"] = "demo:demo-account"
        fills.append(copied)
    result = all_in_cost(
        fills,
        shares=100,
        cost_basis=27500,
        symbol="DDOG",
        tenant_id="demo:demo-account",
    )
    assert result["all_in_per_share"] == 266.68
    assert result["tenant_id"] == "demo:demo-account"


def test_flat_position_returns_none():
    assert all_in_cost(_ddog_puts(), shares=0, cost_basis=27500) is None


def _open_ddog_csp():
    """The DDOG put chain, still open: no assignment and no shares."""
    return [
        fill for fill in _ddog_puts()
        if fill["action"] not in {"option_assigned", "equity_buy"}
    ]


def test_open_csp_with_no_shares_is_if_assigned():
    result = all_in_cost(_open_ddog_csp(), shares=0, symbol="DDOG")
    assert result["if_assigned"] is True
    assert result["equity_per_share"] is None
    assert result["all_in_per_share"] == 266.68
    assert result["net_premium"] == 832.00
    assert result["show_all_in"] is True
    assert result["open_options"] is True
    assert "not a cost of shares" in result["note"].lower()
    strike = next(line for line in result["lines"] if line["label"] == "Strike")
    assert strike["per_share"] == 275.00
    puts = next(line for line in result["lines"] if line["label"] == "Puts before assignment")
    assert puts["per_share"] == 8.32
    assert puts["credit"] is True
    # The earlier $100 put expired while flat. It is not this cycle.
    assert result["buckets"]["puts_before_assignment"] == 832.00


def test_open_put_spread_is_not_an_if_assigned_cost():
    fills = [
        _fill(
            trade_date="2026-10-02",
            action="option_sell_to_open",
            option_type="put",
            option_strike=275,
            option_expiry="2026-10-16",
            quantity=1,
            price=4.75,
        ),
        _fill(
            trade_date="2026-10-02",
            action="option_buy_to_open",
            option_type="put",
            option_strike=265,
            option_expiry="2026-10-16",
            quantity=1,
            price=2.00,
        ),
    ]
    assert all_in_cost(fills, shares=0, symbol="DDOG") is None


def test_covered_and_partially_covered_use_the_shares_held():
    """150 shares and two short calls: one covered, one only partly covered.

    Broker cost basis is missing. The buy in the tape is the cost. Premium
    is divided by all 150 shares, not by the 100 a single call covers.
    """
    from app.all_in_cost import _stamp_rows

    fills = [
        _fill(symbol="NVDA", tenant_id="snaptrade:nvda", trade_date="2026-03-02",
              action="equity_buy", quantity=150, price=40),
        _fill(symbol="NVDA", tenant_id="snaptrade:nvda", trade_date="2026-03-03",
              action="option_sell_to_open", option_type="call", option_strike=45,
              option_expiry="2026-03-20", quantity=1, price=1.50),
        _fill(symbol="NVDA", tenant_id="snaptrade:nvda", trade_date="2026-03-03",
              action="option_sell_to_open", option_type="call", option_strike=50,
              option_expiry="2026-04-17", quantity=1, price=1.50),
    ]
    result = all_in_cost(fills, shares=150, cost_basis=0, symbol="NVDA",
                         tenant_id="snaptrade:nvda")
    assert result["equity_per_share"] == 40.00
    assert result["all_in_per_share"] == 38.00
    assert result["net_premium"] == 300.00
    assert result["buckets"]["calls"] == 300.00
    assert result["show_all_in"] is True

    rows = [
        {"tenant_id": "snaptrade:nvda", "symbol": "NVDA", "strategies": "Covered Call"},
        {"tenant_id": "snaptrade:nvda", "symbol": "NVDA", "strategies": "Partially Covered Call"},
    ]
    holdings = [{
        "tenant_id": "snaptrade:nvda",
        "symbol": "NVDA",
        "instrument_type": "Equity",
        "quantity": 150,
        "cost_basis": 0,
    }]
    _stamp_rows(rows, holdings, fills, [])
    assert rows[0]["equity_per_share"] == 40.00
    assert rows[1]["equity_per_share"] == 40.00
    assert rows[0]["all_in_per_share"] == rows[1]["all_in_per_share"] == 38.00


def test_put_spread_loss_before_shares_does_not_raise_cost():
    fills = [
        _fill(symbol="BE", tenant_id="snaptrade:be", trade_date="2026-04-01",
              action="option_sell_to_open", option_type="put", option_strike=140,
              option_expiry="2026-04-17", quantity=1, price=2.00),
        _fill(symbol="BE", tenant_id="snaptrade:be", trade_date="2026-04-01",
              action="option_buy_to_open", option_type="put", option_strike=130,
              option_expiry="2026-04-17", quantity=1, price=8.00),
        _fill(symbol="BE", tenant_id="snaptrade:be", trade_date="2026-04-17",
              action="option_expired", option_type="put", option_strike=140,
              option_expiry="2026-04-17", quantity=1, price=0),
        _fill(symbol="BE", tenant_id="snaptrade:be", trade_date="2026-04-17",
              action="option_expired", option_type="put", option_strike=130,
              option_expiry="2026-04-17", quantity=1, price=0),
        _fill(symbol="BE", tenant_id="snaptrade:be", trade_date="2026-06-01",
              action="equity_buy", quantity=100, price=132.70),
    ]
    result = all_in_cost(fills, shares=100, cost_basis=13270, symbol="BE")
    assert result["equity_per_share"] == 132.70
    assert result["all_in_per_share"] == 132.70
    assert result["show_all_in"] is False
    assert result["buckets"]["put_spreads"] == 0
    assert result["net_premium"] == 0


def test_put_spread_loss_during_the_cycle_raises_all_in():
    """A debit in this holding cycle raises all-in. It is named once."""
    fills = [
        _fill(symbol="BE", trade_date="2026-06-01", action="equity_buy",
              quantity=100, price=132.70),
        _fill(symbol="BE", trade_date="2026-06-15", action="option_sell_to_open",
              option_type="put", option_strike=120, option_expiry="2026-07-17",
              quantity=1, price=4.00),
        _fill(symbol="BE", trade_date="2026-06-15", action="option_buy_to_open",
              option_type="put", option_strike=100, option_expiry="2026-07-17",
              quantity=1, price=50.00),
    ]
    result = all_in_cost(fills, shares=100, cost_basis=13270, symbol="BE")
    assert result["equity_per_share"] == 132.70
    assert result["net_premium"] == -4600.00
    assert result["all_in_per_share"] == 178.70
    assert result["show_all_in"] is True
    assert len(result["groups"]) == 1
    assert result["groups"][0]["kind"] == "spread"
    assert result["groups"][0]["cash"] == -4600.00
    line = result["lines"][0]
    assert line["label"] == "Put spreads"
    assert line["credit"] is False
    assert line["per_share"] == 46.00
    assert "Put spreads +$46.00" in result["hover"]


def test_put_spread_opened_and_closed_during_the_cycle_is_one_bucket():
    """The close is the same spread, so the loss is one Put spreads line."""
    fills = [
        _fill(symbol="BE", trade_date="2026-06-01", action="equity_buy",
              quantity=100, price=132.70),
        _fill(symbol="BE", trade_date="2026-06-15", action="option_sell_to_open",
              option_type="put", option_strike=120, option_expiry="2026-07-17",
              quantity=1, price=4.00),
        _fill(symbol="BE", trade_date="2026-06-15", action="option_buy_to_open",
              option_type="put", option_strike=100, option_expiry="2026-07-17",
              quantity=1, price=8.00),
        _fill(symbol="BE", trade_date="2026-07-01", action="option_buy_to_close",
              option_type="put", option_strike=120, option_expiry="2026-07-17",
              quantity=1, price=10.00),
        _fill(symbol="BE", trade_date="2026-07-01", action="option_sell_to_close",
              option_type="put", option_strike=100, option_expiry="2026-07-17",
              quantity=1, price=2.00),
    ]
    result = all_in_cost(fills, shares=100, cost_basis=13270, symbol="BE")
    # Open debit $400, close debit $800, net loss $1,200 = $12.00/share.
    assert result["equity_per_share"] == 132.70
    assert result["net_premium"] == -1200.00
    assert result["all_in_per_share"] == 144.70
    assert result["buckets"]["put_spreads"] == -1200.00
    assert result["buckets"]["other"] == 0
    assert len(result["groups"]) == 1
    assert result["lines"][0]["label"] == "Put spreads"
    assert result["lines"][0]["per_share"] == 12.00
    assert result["lines"][0]["credit"] is False


def test_symbol_rows_for_several_strategies_share_one_cost():
    from app.all_in_cost import _stamp_rows

    fills = [
        _fill(symbol="QBTS", tenant_id="snaptrade:qbts", trade_date="2026-01-05",
              action="equity_buy", quantity=200, price=8),
        _fill(symbol="QBTS", tenant_id="snaptrade:qbts", trade_date="2026-02-02",
              action="option_sell_to_open", option_type="call", option_strike=12,
              option_expiry="2026-02-20", quantity=1, price=0.40),
        _fill(symbol="QBTS", tenant_id="snaptrade:qbts", trade_date="2026-02-02",
              action="option_buy_to_open", option_type="call", option_strike=15,
              option_expiry="2026-03-20", quantity=1, price=1.00),
    ]
    rows = [
        {"tenant_id": "snaptrade:qbts", "symbol": "QBTS", "strategy": "Buy and Hold"},
        {"tenant_id": "snaptrade:qbts", "symbol": "QBTS", "strategy": "Covered Call"},
        {"tenant_id": "snaptrade:qbts", "symbol": "QBTS", "strategy": "Long Call"},
    ]
    holdings = [{
        "tenant_id": "snaptrade:qbts",
        "symbol": "QBTS",
        "instrument_type": "Equity",
        "quantity": 200,
        "cost_basis": 0,
    }]
    _stamp_rows(rows, holdings, fills, [])
    assert {row["equity_per_share"] for row in rows} == {8.00}
    # Call credit $40 and long-call debit $100, net -$60 on 200 shares = -$0.30.
    assert {row["all_in_per_share"] for row in rows} == {8.30}
    assert all(row["show_all_in"] for row in rows)


def test_missing_broker_basis_uses_the_opening_estimate():
    result = all_in_cost(
        [],
        shares=100,
        cost_basis=0,
        opening_basis=5730,
        symbol="JEPQ",
    )
    assert result["equity_per_share"] == 57.30
    assert result["basis_estimated"] is True
    assert result["show_all_in"] is False
    assert "did not report a cost basis" in result["hover"]


def test_snapshot_open_call_counts_when_the_sale_is_not_in_the_tape():
    fills = [
        _fill(symbol="JEPQ", trade_date="2026-05-01", action="equity_buy",
              quantity=100, price=57.30),
    ]
    snapshot = [{
        "tenant_id": "snaptrade:jepq",
        "symbol": "JEPQ",
        "instrument_type": "Call",
        "option_type": "call",
        "option_strike": 60,
        "option_expiry": "2026-11-20",
        "quantity": -1,
        "cost_basis": 200,
    }]
    result = all_in_cost(
        fills, shares=100, cost_basis=5730, symbol="JEPQ",
        snapshot_options=snapshot, tenant_id="snaptrade:jepq",
    )
    assert result["equity_per_share"] == 57.30
    assert result["all_in_per_share"] == 55.30
    assert result["open_options"] is True
    assert result["show_all_in"] is True
    assert result["net_premium"] == 200.00


def test_assigned_shares_with_no_priced_buy_still_have_a_cost():
    """NVDA: covered calls, broker basis missing, shares delivered at $0.

    Schwab posts the assigned shares as a buy with a blank price and no
    separate assignment row. The open short put's strike is the cost.
    Partial coverage uses every share held, and every strategy row of
    that account shares it.
    """
    from app.all_in_cost import _stamp_rows

    fills = [
        _fill(symbol="NVDA", tenant_id="snaptrade:nvda", trade_date="2026-05-01",
              action="option_sell_to_open", option_type="put", option_strike=180,
              option_expiry="2026-05-15", quantity=1, price=4.00),
        _fill(symbol="NVDA", tenant_id="snaptrade:nvda", trade_date="2026-05-15",
              action="equity_buy", quantity=100, price=0),
        _fill(symbol="NVDA", tenant_id="snaptrade:nvda", trade_date="2026-06-01",
              action="equity_buy", quantity=50, price=190),
        _fill(symbol="NVDA", tenant_id="snaptrade:nvda", trade_date="2026-06-02",
              action="option_sell_to_open", option_type="call", option_strike=200,
              option_expiry="2026-06-19", quantity=1, price=3.00),
        _fill(symbol="NVDA", tenant_id="snaptrade:nvda", trade_date="2026-06-02",
              action="option_sell_to_open", option_type="call", option_strike=220,
              option_expiry="2026-07-17", quantity=1, price=2.00),
    ]
    result = all_in_cost(
        fills, shares=150, cost_basis=0, symbol="NVDA", tenant_id="snaptrade:nvda",
    )
    # 100 shares at the 180 strike, 50 bought at 190.
    # Put credit $400 plus $500 of call premium.
    assert result is not None
    assert result["equity_per_share"] == 183.33
    assert result["all_in_per_share"] == 177.33
    assert result["buckets"]["calls"] == 500.00
    assert result["show_all_in"] is True

    rows = [
        {"tenant_id": "snaptrade:nvda", "symbol": "NVDA",
         "strategies": "Cash-Secured Put, Covered Call, Partially Covered Call"},
        {"tenant_id": "snaptrade:nvda", "symbol": "NVDA", "strategies": "Put Spread"},
    ]
    holdings = [{
        "tenant_id": "snaptrade:nvda",
        "symbol": "NVDA",
        "instrument_type": "Equity",
        "quantity": 150,
        "cost_basis": 0,
    }]
    _stamp_rows(rows, holdings, fills, [])
    assert rows[0]["equity_per_share"] == 183.33
    assert rows[1]["equity_per_share"] == 183.33
    assert rows[0]["show_all_in"] is True


def test_assignment_symbol_prices_shares_when_the_strike_column_is_blank():
    """No equity buy at all. The assignment row only has the contract symbol."""
    fills = [
        _fill(symbol="NVDA", tenant_id="snaptrade:nvda", trade_date="2026-05-01",
              action="option_sell_to_open", quantity=1, price=4.00,
              trade_symbol="NVDA 05/15/2026 180.00 P"),
        _fill(symbol="NVDA", tenant_id="snaptrade:nvda", trade_date="2026-05-15",
              action="option_assigned", quantity=1, price=0,
              trade_symbol="NVDA 05/15/2026 180.00 P"),
        _fill(symbol="NVDA", tenant_id="snaptrade:nvda", trade_date="2026-06-02",
              action="option_sell_to_open", quantity=1, price=3.00,
              trade_symbol="NVDA 06/19/2026 200.00 C"),
    ]
    result = all_in_cost(
        fills, shares=100, cost_basis=0, symbol="NVDA", tenant_id="snaptrade:nvda",
    )
    assert result["equity_per_share"] == 180.00
    assert result["all_in_per_share"] == 173.00
    assert result["buckets"]["calls"] == 300.00


def test_open_call_sold_before_this_lot_still_counts():
    """JEPQ: cost is the broker basis, the covered call was sold earlier and is still open.

    The tape has the sale, so the snapshot must not be required, and the
    sale is before the share purchase that starts this cycle.
    """
    fills = [
        _fill(symbol="JEPQ", tenant_id="snaptrade:jepq", trade_date="2025-11-03",
              action="option_sell_to_open", option_type="call", option_strike=60,
              option_expiry="2026-12-18", quantity=1, price=2.00,
              trade_symbol="JEPQ 12/18/2026 60.00 C"),
        _fill(symbol="JEPQ", tenant_id="snaptrade:jepq", trade_date="2026-05-01",
              action="equity_buy", quantity=100, price=57.30),
    ]
    result = all_in_cost(
        fills, shares=100, cost_basis=5730, symbol="JEPQ", tenant_id="snaptrade:jepq",
    )
    assert result["equity_per_share"] == 57.30
    assert result["all_in_per_share"] == 55.30
    assert result["open_options"] is True
    assert result["show_all_in"] is True
    assert result["net_premium"] == 200.00


def test_closed_tape_call_does_not_hide_or_double_the_snapshot_open():
    """Same contract expired on the tape, still short on the snapshot.

    Matching the trade symbol used to skip the snapshot. Counting the
    expired credit as well as the snapshot premium would double it.
    """
    fills = [
        _fill(symbol="JEPQ", tenant_id="snaptrade:jepq", trade_date="2026-04-01",
              action="option_sell_to_open", option_type="call", option_strike=60,
              option_expiry="2026-11-20", quantity=1, price=1.50,
              trade_symbol="JEPQ 11/20/2026 60.00 C"),
        _fill(symbol="JEPQ", tenant_id="snaptrade:jepq", trade_date="2026-04-17",
              action="option_expired", option_type="call", option_strike=60,
              option_expiry="2026-11-20", quantity=1, price=0,
              trade_symbol="JEPQ 11/20/2026 60.00 C"),
        _fill(symbol="JEPQ", tenant_id="snaptrade:jepq", trade_date="2026-05-01",
              action="equity_buy", quantity=100, price=57.30),
    ]
    snapshot = [{
        "tenant_id": "snaptrade:jepq",
        "symbol": "JEPQ",
        "instrument_type": "Call",
        "option_type": "call",
        "option_strike": 60,
        "option_expiry": "2026-11-20",
        "quantity": -1,
        "cost_basis": 200,
        "trade_symbol": "JEPQ 11/20/2026 60.00 C",
    }]
    result = all_in_cost(
        fills, shares=100, cost_basis=5730, symbol="JEPQ",
        snapshot_options=snapshot, tenant_id="snaptrade:jepq",
    )
    assert result["equity_per_share"] == 57.30
    assert result["all_in_per_share"] == 55.30
    assert result["open_options"] is True
    assert result["show_all_in"] is True
    assert result["net_premium"] == 200.00


def test_closed_call_before_the_shares_does_not_change_all_in():
    fills = [
        _fill(symbol="JEPQ", trade_date="2025-11-03", action="option_sell_to_open",
              option_type="call", option_strike=60, option_expiry="2025-11-21",
              quantity=1, price=2.00, trade_symbol="JEPQ 11/21/2025 60.00 C"),
        _fill(symbol="JEPQ", trade_date="2025-11-21", action="option_expired",
              option_type="call", option_strike=60, option_expiry="2025-11-21",
              quantity=1, price=0, trade_symbol="JEPQ 11/21/2025 60.00 C"),
        _fill(symbol="JEPQ", trade_date="2026-05-01", action="equity_buy",
              quantity=100, price=57.30),
    ]
    result = all_in_cost(fills, shares=100, cost_basis=5730, symbol="JEPQ")
    assert result["equity_per_share"] == 57.30
    assert result["show_all_in"] is False
    assert result["net_premium"] == 0


def test_put_spread_is_not_labeled_calls_and_sits_beside_real_calls():
    """BE: put legs whose column says Call, plus a real covered-call credit.

    The contract symbol is a put. The debit is Put spreads, the credit
    is Calls, and the signs stay opposite.
    """
    fills = [
        _fill(symbol="BE", tenant_id="snaptrade:be", trade_date="2026-06-01",
              action="equity_buy", quantity=100, price=132.70),
        _fill(symbol="BE", tenant_id="snaptrade:be", trade_date="2026-06-15",
              action="option_sell_to_open", option_type="call", quantity=1, price=4.00,
              trade_symbol="BE 07/17/2026 120.00 P"),
        _fill(symbol="BE", tenant_id="snaptrade:be", trade_date="2026-06-15",
              action="option_buy_to_open", option_type="call", quantity=1, price=49.96,
              trade_symbol="BE 07/17/2026 100.00 P"),
        _fill(symbol="BE", tenant_id="snaptrade:be", trade_date="2026-06-20",
              action="option_sell_to_open", option_type="call", quantity=1, price=1.50,
              trade_symbol="BE 08/21/2026 160.00 C"),
    ]
    result = all_in_cost(
        fills, shares=100, cost_basis=13270, symbol="BE", tenant_id="snaptrade:be",
    )
    assert result["equity_per_share"] == 132.70
    # Put debit $4,596, call credit $150. All-in = 132.70 + 45.96 - 1.50.
    assert result["buckets"]["put_spreads"] == -4596.00
    assert result["buckets"]["calls"] == 150.00
    assert result["all_in_per_share"] == 177.16
    labels = {line["label"]: line for line in result["lines"]}
    assert labels["Put spreads"]["per_share"] == 45.96
    assert labels["Put spreads"]["credit"] is False
    assert labels["Calls"]["per_share"] == 1.50
    assert labels["Calls"]["credit"] is True
    assert "Put spreads +$45.96" in result["hover"]
    assert "Calls −$1.50" in result["hover"]


def test_put_spread_with_only_the_underlying_symbol_is_still_a_put_spread():
    fills = [
        _fill(symbol="BE", trade_date="2026-06-01", action="equity_buy",
              quantity=100, price=132.70, trade_symbol="BE"),
        _fill(symbol="BE", trade_date="2026-06-15", action="option_sell_to_open",
              option_type="put", option_strike=120, option_expiry="2026-07-17",
              quantity=1, price=4.00, trade_symbol="BE"),
        _fill(symbol="BE", trade_date="2026-06-15", action="option_buy_to_open",
              option_type="put", option_strike=100, option_expiry="2026-07-17",
              quantity=1, price=8.00, trade_symbol="BE"),
    ]
    result = all_in_cost(fills, shares=100, cost_basis=13270, symbol="BE")
    assert result["groups"][0]["kind"] == "spread"
    assert result["groups"][0]["bucket"] == "put_spreads"
    assert result["lines"][0]["label"] == "Put spreads"


def test_long_call_with_no_shares_stays_blank():
    fills = [
        _fill(action="option_buy_to_open", option_type="call", option_strike=200,
              option_expiry="2026-12-18", quantity=1, price=5.00),
    ]
    assert all_in_cost(fills, shares=0, symbol="DDOG") is None


def test_templates_fold_all_in_into_the_existing_header_and_table():
    detail = (ROOT / "app/templates/position_detail.html").read_text()
    positions = (ROOT / "app/templates/positions.html").read_text()
    assert "Cost/share" in detail
    assert "All-in" in detail
    assert "pd-allin-pop" in detail
    hero = detail.split('class="review-hero pos-hero"', 1)[1].split("pos-total", 1)[0]
    assert "pd-allin" in hero
    assert "pos-cost-col" in positions
    assert 'class="pos-costline"' in positions
    assert "term('Cost')" in positions
    assert "term('Cost/share')" not in positions
    assert "term('All-in')" in positions
    assert "If assigned" in detail
    assert "If assigned" in positions
    wide = positions.split("@media (min-width: 1280px)", 1)[1].split("@media", 1)[0]
    assert "#symbolTable th:nth-child(8), #symbolTable td:nth-child(8) { width: 8%; }" in wide
    assert 'colspan="14"' in positions
    assert 'colspan="12"' not in positions.split('id="symbolTable"', 1)[1].split("positionsTable", 1)[0]
