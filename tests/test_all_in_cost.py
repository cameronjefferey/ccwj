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
    wide = positions.split("@media (min-width: 1280px)", 1)[1].split("@media", 1)[0]
    assert "#symbolTable th:nth-child(8), #symbolTable td:nth-child(8) { width: 8%; }" in wide
    assert 'colspan="14"' in positions
    assert 'colspan="12"' not in positions.split('id="symbolTable"', 1)[1].split("positionsTable", 1)[0]
