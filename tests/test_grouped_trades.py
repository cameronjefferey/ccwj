"""Strategies, Sectors, and Fit count a spread or a lot as one trade.

Dollars stay on the rows the warehouse already summed. These tests pin
the counts, the win rate, and the best and worst pick against the legs.
"""

import pandas as pd

from app.grouped_trades import (
    attach_sectors,
    best_and_worst,
    counts_by,
    format_record_span,
    group_book_trades,
    months_from_units,
    overlay_month_counts,
    plain_label,
    stamp_grouped_counts,
)
from app.sector_labels import classify_symbol
from app.strategies import dte_breakdown


def occ(root, yymmdd, cp, strike):
    return f"{root:<6}{yymmdd}{cp}{int(round(float(strike) * 1000)):08d}"


def leg(**overrides):
    row = {
        "tenant_id": "snaptrade:1",
        "account": "Ira",
        "symbol": "AMD",
        "status": "Closed",
        "trade_group_type": "option_contract",
        "open_date": "2025-01-02",
        "close_date": "2025-01-17",
        "quantity": 1,
        "days_in_trade": 15,
        "dte_bucket": "8-30 DTE",
    }
    row.update(overrides)
    return row


def _counted(units):
    rows = [unit for unit in units if unit.get("counts_as_trade")]
    winners = sum(unit["num_winners"] for unit in rows)
    losers = sum(unit["num_losers"] for unit in rows)
    return rows, winners, losers


def test_call_spread_is_one_trade_and_fees_stay_in_the_net():
    units = group_book_trades([
        leg(
            strategy="Call Spread", direction="Sold",
            trade_symbol=occ("AMD", "250117", "C", 100), total_pnl=300,
        ),
        leg(
            strategy="Call Spread", direction="Bought",
            trade_symbol=occ("AMD", "250117", "C", 105), total_pnl=-50,
        ),
    ])
    counted, winners, losers = _counted(units)
    assert len(counted) == 1
    assert counted[0]["strategy"] == "Call Spread"
    assert counted[0]["pnl"] == 250
    assert winners == 1 and losers == 0
    best, worst = best_and_worst(units, strategy="Call Spread")
    assert best["pnl"] == 250
    assert best["pnl"] != 300


def test_two_same_day_verticals_at_different_strikes_stay_two_trades():
    units = group_book_trades([
        leg(strategy="Call Spread", direction="Sold",
            trade_symbol=occ("AMD", "250117", "C", 100), total_pnl=80),
        leg(strategy="Call Spread", direction="Bought",
            trade_symbol=occ("AMD", "250117", "C", 105), total_pnl=-20),
        leg(strategy="Call Spread", direction="Sold",
            trade_symbol=occ("AMD", "250117", "C", 110), total_pnl=-40),
        leg(strategy="Call Spread", direction="Bought",
            trade_symbol=occ("AMD", "250117", "C", 115), total_pnl=10),
    ])
    counted, winners, losers = _counted(units)
    assert len(counted) == 2
    assert sorted(unit["pnl"] for unit in counted) == [-30, 60]
    assert winners == 1 and losers == 1


def test_iron_condor_is_one_trade_with_the_net_of_all_four_legs():
    units = group_book_trades([
        leg(strategy="Iron Condor", direction="Sold", symbol="SPX",
            trade_symbol=occ("SPXW", "250117", "C", 6000), total_pnl=200),
        leg(strategy="Iron Condor", direction="Bought", symbol="SPX",
            trade_symbol=occ("SPXW", "250117", "C", 6010), total_pnl=-40),
        leg(strategy="Iron Condor", direction="Sold", symbol="SPX",
            trade_symbol=occ("SPXW", "250117", "P", 5900), total_pnl=180),
        leg(strategy="Iron Condor", direction="Bought", symbol="SPX",
            trade_symbol=occ("SPXW", "250117", "P", 5890), total_pnl=-30),
    ])
    counted, winners, losers = _counted(units)
    assert len(counted) == 1
    assert counted[0]["strategy"] == "Iron Condor"
    assert counted[0]["pnl"] == 310
    assert winners == 1 and losers == 0
    months = months_from_units(units)
    assert int(months.iloc[0]["trades_closed"]) == 1
    assert int(months.iloc[0]["num_winners"]) == 1


def test_grouped_month_counts_do_not_move_mart_dollars():
    mart = pd.DataFrame([
        {
            "strategy": "Call Spread",
            "month_start": "2025-01-01",
            "trades_closed": 1,
            "num_winners": 1,
            "num_losers": 0,
            "total_pnl": 100,
            "win_rate_pct": 100,
            "avg_pnl": 100,
        },
        {
            "strategy": "Call Spread",
            "month_start": "2025-02-01",
            "trades_closed": 1,
            "num_winners": 0,
            "num_losers": 1,
            "total_pnl": -25,
            "win_rate_pct": 0,
            "avg_pnl": -25,
        },
    ])
    grouped = pd.DataFrame([{
        "strategy": "Call Spread",
        "month_start": "2025-02-01",
        "trades_closed": 1,
        "num_winners": 1,
        "num_losers": 0,
        "total_pnl": 75,
    }])
    overlaid = overlay_month_counts(mart, grouped)
    january = overlaid.iloc[0]
    february = overlaid.iloc[1]
    assert january["total_pnl"] == 100
    assert january["trades_closed"] == 0
    assert pd.isna(january["avg_pnl"])
    assert february["total_pnl"] == -25
    assert february["trades_closed"] == 1
    assert february["num_winners"] == 1


def test_strangle_pairs_the_call_and_the_put():
    units = group_book_trades([
        leg(strategy="Strangle", direction="Sold",
            trade_symbol=occ("AMD", "250117", "C", 110), total_pnl=90),
        leg(strategy="Strangle", direction="Sold",
            trade_symbol=occ("AMD", "250117", "P", 90), total_pnl=70),
    ])
    counted, winners, _losers = _counted(units)
    assert len(counted) == 1
    assert counted[0]["strategy"] == "Strangle"
    assert counted[0]["pnl"] == 160
    assert winners == 1


def test_diagonal_pairs_only_rows_already_labeled_diagonal():
    paired = group_book_trades([
        leg(strategy="Diagonal Call Spread", direction="Sold",
            trade_symbol=occ("AMD", "250117", "C", 100), total_pnl=120,
            option_expiry="2025-01-17"),
        leg(strategy="Diagonal Call Spread", direction="Bought",
            trade_symbol=occ("AMD", "250221", "C", 95), total_pnl=-80,
            option_expiry="2025-02-21", open_date="2025-01-02"),
    ])
    counted, _winners, _losers = _counted(paired)
    assert len(counted) == 1
    assert counted[0]["pnl"] == 40
    assert counted[0]["strategy"] == "Diagonal Call Spread"

    separate = group_book_trades([
        leg(strategy="Diagonal Call Spread", direction="Sold",
            trade_symbol=occ("AMD", "250117", "C", 100), total_pnl=120),
        leg(strategy="Long Call", direction="Bought",
            trade_symbol=occ("AMD", "250221", "C", 95), total_pnl=-80,
            option_expiry="2025-02-21"),
    ])
    names = sorted(unit["strategy"] for unit in separate if unit["counts_as_trade"])
    assert names == ["Diagonal Call Spread", "Long Call"]


def test_equity_session_is_one_lot_even_when_the_row_counts_fills():
    units = group_book_trades([
        {
            "tenant_id": "snaptrade:1",
            "account": "Ira",
            "symbol": "AAPL",
            "strategy": "Covered Call",
            "trade_group_type": "equity_session",
            "status": "Closed",
            "total_pnl": 400,
            "num_trades": 7,
            "open_date": "2024-01-02",
            "close_date": "2024-06-01",
            "days_in_trade": 150,
        },
        leg(strategy="Covered Call", direction="Sold", symbol="AAPL",
            trade_symbol=occ("AAPL", "240621", "C", 180), total_pnl=90),
    ])
    counted, _winners, _losers = _counted(units)
    assert len(counted) == 2
    assert {unit["strategy"] for unit in counted} == {"Covered Call"}
    assert sum(unit["pnl"] for unit in counted) == 490


def test_assignment_keeps_the_put_and_the_shares_as_two_strategies():
    units = group_book_trades([
        leg(strategy="Cash-Secured Put", direction="Sold", symbol="SOFI",
            trade_symbol=occ("SOFI", "250117", "P", 15), total_pnl=200,
            close_date="2025-01-17"),
        {
            "tenant_id": "snaptrade:1",
            "account": "Ira",
            "symbol": "SOFI",
            "strategy": "Wheel",
            "trade_group_type": "equity_session",
            "status": "Closed",
            "total_pnl": -50,
            "open_date": "2025-01-17",
            "close_date": "2025-03-01",
        },
    ])
    counted, _winners, losers = _counted(units)
    assert len(counted) == 2
    assert {unit["strategy"] for unit in counted} == {"Cash-Secured Put", "Wheel"}
    assert sum(unit["pnl"] for unit in counted) == 150
    assert losers == 1


def test_worthless_expiry_is_a_trade_and_not_a_loss():
    units = group_book_trades([
        leg(strategy="Naked Call", direction="Sold",
            trade_symbol=occ("AMD", "250117", "C", 200), total_pnl=0),
        leg(strategy="Naked Call", direction="Sold",
            trade_symbol=occ("AMD", "250117", "C", 210), total_pnl=80),
    ])
    counted, winners, losers = _counted(units)
    assert len(counted) == 2
    assert winners == 1 and losers == 0
    decided = winners + losers
    assert winners / decided == 1


def test_same_contract_opened_twice_is_two_lots_whose_results_sum():
    symbol = occ("MU", "250117", "C", 120)
    parent = leg(
        strategy="Naked Call", direction="Sold", symbol="MU",
        trade_symbol=symbol, quantity=2, total_pnl=150,
    )
    fills = [
        {"tenant_id": "snaptrade:1", "trade_symbol": symbol,
         "action": "option_sell_to_open", "quantity": 1, "price": 3.0,
         "amount": 300, "trade_date": "2025-01-02"},
        {"tenant_id": "snaptrade:1", "trade_symbol": symbol,
         "action": "option_sell_to_open", "quantity": 1, "price": 2.0,
         "amount": 200, "trade_date": "2025-01-03"},
        {"tenant_id": "snaptrade:1", "trade_symbol": symbol,
         "action": "option_buy_to_close", "quantity": 1, "price": 2.0,
         "amount": -200, "trade_date": "2025-01-08"},
        {"tenant_id": "snaptrade:1", "trade_symbol": symbol,
         "action": "option_buy_to_close", "quantity": 1, "price": 1.5,
         "amount": -150, "trade_date": "2025-01-09"},
    ]
    units = group_book_trades([parent], fills)
    counted, winners, losers = _counted(units)
    assert len(counted) == 2
    assert sorted(unit["pnl"] for unit in counted) == [50, 100]
    assert sum(unit["pnl"] for unit in counted) == 150
    assert winners + losers == 2


def test_settlement_pending_index_spread_is_not_a_win_or_a_loss():
    units = group_book_trades([
        leg(strategy="Call Spread", direction="Sold", symbol="SPX",
            status="Settlement pending",
            trade_symbol=occ("SPXW", "250117", "C", 6000), total_pnl=500),
        leg(strategy="Call Spread", direction="Bought", symbol="SPX",
            status="Settlement pending",
            trade_symbol=occ("SPXW", "250117", "C", 6010), total_pnl=-100),
    ])
    assert units
    assert all(not unit["counts_as_trade"] for unit in units)
    assert all(unit["num_winners"] == 0 and unit["num_losers"] == 0 for unit in units)
    assert counts_by(units, ("strategy",)) == {}


def test_a_roll_is_the_close_and_the_new_contract():
    units = group_book_trades([
        leg(strategy="Naked Put", direction="Sold",
            trade_symbol=occ("AMD", "250117", "P", 100), total_pnl=-40,
            close_date="2025-01-10"),
        leg(strategy="Naked Put", direction="Sold",
            trade_symbol=occ("AMD", "250221", "P", 95), total_pnl=25,
            status="Open", open_date="2025-01-10", close_date=""),
    ])
    counted, winners, losers = _counted(units)
    assert len(counted) == 2
    assert winners == 0 and losers == 1
    assert {unit["status"] for unit in counted} == {"Closed", "Open"}


def test_empty_book_and_a_single_trade():
    assert group_book_trades([]) == []
    units = group_book_trades([
        leg(strategy="Long Put", direction="Bought",
            trade_symbol=occ("QQQ", "250117", "P", 480), symbol="QQQ",
            total_pnl=-15),
    ])
    counted, winners, losers = _counted(units)
    assert len(counted) == 1
    assert counted[0]["pnl"] == -15
    assert winners == 0 and losers == 1
    best, worst = best_and_worst(units)
    assert best["label"] == worst["label"]
    assert worst["pnl"] == -15


def test_stamp_replaces_fill_counts_and_leaves_dollars():
    spread = group_book_trades([
        leg(strategy="Put Spread", direction="Sold",
            trade_symbol=occ("IWM", "250117", "P", 200), symbol="IWM",
            total_pnl=40),
        leg(strategy="Put Spread", direction="Bought",
            trade_symbol=occ("IWM", "250117", "P", 195), symbol="IWM",
            total_pnl=-10),
    ])
    frame = pd.DataFrame([{
        "tenant_id": "snaptrade:1",
        "strategy": "Put Spread",
        "num_individual_trades": 6,
        "num_winners": 2,
        "num_losers": 2,
        "win_rate": 0.5,
        "total_pnl": 999.0,
    }])
    stamped = stamp_grouped_counts(frame, spread, ("tenant_id", "strategy"))
    assert float(stamped.iloc[0]["total_pnl"]) == 999.0
    assert int(stamped.iloc[0]["num_individual_trades"]) == 1
    assert int(stamped.iloc[0]["num_winners"]) == 1
    assert int(stamped.iloc[0]["num_losers"]) == 0


def test_dte_breakdown_counts_a_spread_once():
    frame = pd.DataFrame([
        leg(strategy="Call Spread", direction="Sold",
            trade_symbol=occ("AMD", "250117", "C", 100), total_pnl=30,
            dte_bucket="0-7 DTE"),
        leg(strategy="Call Spread", direction="Bought",
            trade_symbol=occ("AMD", "250117", "C", 105), total_pnl=-10,
            dte_bucket="0-7 DTE"),
    ])
    rows = {row["dte_bucket"]: row for row in dte_breakdown(frame)}
    assert rows["0-7 DTE"]["num_trades"] == 1
    assert rows["0-7 DTE"]["total_pnl"] == 20
    assert rows["0-7 DTE"]["win_rate_pct"] == 100.0


def test_best_and_worst_use_the_sector_on_the_grouped_trade():
    units = group_book_trades([
        leg(strategy="Buy and Hold", symbol="AAPL", total_pnl=50,
            trade_group_type="equity_session", trade_symbol=""),
        leg(strategy="Buy and Hold", symbol="XOM", total_pnl=-20,
            trade_group_type="equity_session", trade_symbol=""),
    ])
    labeled = pd.DataFrame([
        {"symbol": "AAPL", "sector": "Technology", "subsector": "Hardware"},
        {"symbol": "XOM", "sector": "Energy", "subsector": "Oil"},
    ])
    attach_sectors(units, labeled)
    best, worst = best_and_worst(units, sector="Technology")
    assert best["symbol"] == "AAPL"
    assert worst["symbol"] == "AAPL"
    energy_best, energy_worst = best_and_worst(units, sector="Energy")
    assert energy_worst["pnl"] == -20
    assert energy_best["symbol"] == "XOM"


def test_plain_labels_and_the_record_span():
    assert plain_label("0-7 DTE") == "0–7 days"
    assert plain_label("ITM") == "In the money"
    assert plain_label("Technology") == "Technology"
    assert format_record_span("2024-01-02", "2026-10-08") == "Jan 2, 2024 – Oct 8, 2026"
    assert format_record_span(None, None) == ""
    assert classify_symbol("VIX", None, None) == ("Index", "Index")
