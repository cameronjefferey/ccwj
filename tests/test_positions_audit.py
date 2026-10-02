"""Positions list and position-detail audit (items 1–4 and 9–12).

Warehouse rules are pinned as SQL text so they hold without a BigQuery
build. Dollar display and the story engine are pinned in Python.
"""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from app.outcome_units import (
    adjusted_contract_qty,
    adjusted_contract_strike,
    apply_spread_collected,
    closing_without_an_open,
    net_collected,
    outcome_win_loss,
)

CONTRACTS = Path("dbt/models/intermediate/int_option_contracts.sql").read_text()
CLASSIFICATION = Path("dbt/models/intermediate/int_strategy_classification.sql").read_text()
LEGS = Path("dbt/models/intermediate/int_position_legs.sql").read_text()
WRITEOFFS = Path("dbt/models/intermediate/int_closed_equity_legs.sql").read_text()
SUMMARY = Path("dbt/models/marts/positions_summary.sql").read_text()
PAGE = Path("app/templates/position_detail.html").read_text()
POSITIONS = Path("app/templates/positions.html").read_text()


def test_orphan_closes_are_opened_before_history_and_not_realized():
    assert "opened_before_history" in CONTRACTS
    assert "<= 1e-9" in CONTRACTS
    assert "when coalesce(opened_before_history, false) then 0.0" in CONTRACTS
    assert "where not coalesce(opened_before_history, false)" in CLASSIFICATION
    assert "where not coalesce(opened_before_history, false)" in LEGS
    # Both opening quantities at 0 must not take the Sold branch.
    assert "No opening fill" in CONTRACTS
    orphans = closing_without_an_open([
        {"action": "option_buy_to_close", "trade_symbol": "KLAC  250117C00800000"},
        {"action": "option_sell_to_open", "trade_symbol": "AMD   250117C00100000"},
        {"action": "option_buy_to_close", "trade_symbol": "AMD   250117C00100000"},
        {"action": "option_expired", "trade_symbol": "NVO   250117C00050000"},
    ])
    assert orphans == ["KLAC  250117C00800000", "NVO   250117C00050000"]


def test_split_adjusted_contract_maps_strike_and_qty():
    # SCHD 3:1: 85C / 3 = 28.333..., 1 contract becomes 3.
    assert adjusted_contract_strike(85, 3, 1) == pytest.approx(28.333333, rel=1e-6)
    assert adjusted_contract_qty(1, 3, 1) == pytest.approx(3)
    assert "split_links" in CONTRACTS
    assert "int_split_factors" in CONTRACTS
    assert "(sac.total_buy_qty - sac.total_sell_qty) < 1" in WRITEOFFS
    assert "cumulative_split_factor" in WRITEOFFS


def test_missing_broker_shares_close_the_covered_call_run():
    from app.covered_call_runs import build_covered_call_runs

    rows = pd.DataFrame([
        {
            "trade_date": date(2026, 1, 2), "action": "equity_buy",
            "instrument_type": "Equity", "trade_symbol": "PLTR",
            "quantity": 100, "price": 148.21, "amount": -14821.0, "fees": 0.0,
            "account": "Cameron Investment", "tenant_id": "snaptrade:cam",
        },
        {
            "trade_date": date(2026, 1, 3), "action": "option_sell_to_open",
            "instrument_type": "Call", "trade_symbol": "PLTR  260220C00160000",
            "quantity": 1, "price": 2.0, "amount": 200.0, "fees": 0.0,
            "account": "Cameron Investment", "tenant_id": "snaptrade:cam",
        },
    ])
    current = pd.DataFrame([{
        "symbol": "PLTR", "quantity": 0, "current_price": 20.0,
        "tenant_id": "snaptrade:cam", "account": "Cameron Investment",
    }])
    runs = build_covered_call_runs(rows, current_df=current, as_of=date(2026, 2, 1))
    assert len(runs) == 1
    assert runs[0]["status"] == "closed"
    assert runs[0]["missing_close"] is True
    assert "sale or transfer missing from broker history" in runs[0]["share_sentence"].lower()
    assert runs[0]["share_pnl"] == 0.0


def test_directional_tile_is_buy_to_open_only_including_expiry():
    from app.position_story import build_position_story, compose_story_summary

    df = pd.DataFrame([
        # Bought-back short. Its loss stays out of the directional tile.
        {"trade_date": date(2024, 1, 2), "action": "option_sell_to_open",
         "instrument_type": "Call", "trade_symbol": "NOW  240216C00080000",
         "quantity": 1, "price": 2.0, "amount": 200.0, "account": "Schwab",
         "tenant_id": None},
        {"trade_date": date(2024, 1, 10), "action": "option_buy_to_close",
         "instrument_type": "Call", "trade_symbol": "NOW  240216C00080000",
         "quantity": 1, "price": 5.0, "amount": -500.0, "account": "Schwab",
         "tenant_id": None},
        # Long call that expires worthless is directional.
        {"trade_date": date(2024, 2, 1), "action": "option_buy_to_open",
         "instrument_type": "Call", "trade_symbol": "NOW  240316C00090000",
         "quantity": 1, "price": 4.0, "amount": -400.0, "account": "Schwab",
         "tenant_id": None},
        {"trade_date": date(2024, 3, 16), "action": "option_expired",
         "instrument_type": "Call", "trade_symbol": "NOW  240316C00090000",
         "quantity": 1, "price": 0.0, "amount": 0.0, "account": "Schwab",
         "tenant_id": None},
    ])
    _, _, stats = build_position_story(df, None)
    assert stats["long_losses"] == 1
    assert stats["long_loss_total"] == pytest.approx(400)
    assert stats["long_wins"] == 0
    assert stats["contract_losses"] >= 1
    tiles = compose_story_summary(stats)["tiles"]
    directional = next(t for t in tiles if t["label"] == "Directional")
    assert directional["value"] == "-$400"
    assert "premium" not in directional["sub"]


def test_long_option_exercised_itm_is_directional_not_expired():
    from app.position_story import build_position_story

    df = pd.DataFrame([
        {"trade_date": date(2024, 1, 2), "action": "option_buy_to_open",
         "instrument_type": "Call", "trade_symbol": "AMD  240216C00100000",
         "quantity": 1, "price": 5.0, "amount": -500.0, "account": "Schwab",
         "tenant_id": None},
        {"trade_date": date(2024, 2, 16), "action": "option_exercised",
         "instrument_type": "Call", "trade_symbol": "AMD  240216C00100000",
         "quantity": 1, "price": None, "amount": 0.0, "account": "Schwab",
         "tenant_id": None},
        {"trade_date": date(2024, 2, 16), "action": "equity_buy",
         "instrument_type": "Equity", "trade_symbol": "AMD",
         "quantity": 100, "price": 100.0, "amount": -10000.0, "account": "Schwab",
         "tenant_id": None},
    ])
    items, _, stats = build_position_story(df, None)
    text = " | ".join(
        h for it in items if it["type"] == "day" for h in it["headlines"]
    )
    assert "Exercised the $100 call" in text
    assert "expired worthless" not in text
    assert stats["long_losses"] == 1
    assert stats["long_loss_total"] == pytest.approx(500)


def test_spread_collected_is_net_credit_and_one_outcome():
    assert net_collected(16523, 11536.63, "Call Spread") == pytest.approx(4986.37)
    assert net_collected(200, 50, "Covered Call") == 200
    assert net_collected(100, 150, "Put Spread") == 0
    rows = [
        {"strategy": "Call Spread", "status": "Closed", "total_pnl": 40,
         "tenant_id": "t", "account": "a", "user_id": 1, "symbol": "VICR",
         "open_date": "2024-01-02", "option_expiry": "2024-02-16"},
        {"strategy": "Call Spread", "status": "Closed", "total_pnl": -10,
         "tenant_id": "t", "account": "a", "user_id": 1, "symbol": "VICR",
         "open_date": "2024-01-02", "option_expiry": "2024-02-16"},
        {"strategy": "Call Spread", "status": "Closed", "total_pnl": 0,
         "tenant_id": "t", "account": "a", "user_id": 1, "symbol": "VICR",
         "open_date": "2024-03-01", "option_expiry": "2024-04-19"},
        {"strategy": "Naked Call", "status": "Closed", "total_pnl": 5,
         "open_date": "2024-01-02", "option_expiry": "2024-02-16"},
    ]
    assert outcome_win_loss(rows) == (2, 0)
    assert "outcome_units" in SUMMARY
    assert "round(unit_pnl, 2) != 0" in SUMMARY
    frame = pd.DataFrame([
        {"strategy": "Iron Condor", "total_premium_received": 800,
         "total_premium_paid": -300},
        {"strategy": "Covered Call", "total_premium_received": 100,
         "total_premium_paid": 0},
    ])
    out = apply_spread_collected(frame)
    assert out.loc[0, "total_premium_received"] == 500
    assert out.loc[1, "total_premium_received"] == 100


def test_exercised_short_is_labeled_assigned():
    assert "exercised_label = 'Assigned'" in PAGE
    assert "o.close_type == 'Exercised' and o.direction == 'Sold'" in PAGE
    assert ">Assigned<" in PAGE or "'Assigned'" in PAGE


def test_zero_pnl_is_neither_win_nor_loss():
    from app.position_detail import _rollup_int_strategy_to_summary_shape

    assert "when round(oc.total_pnl, 2) > 0 then true" in CLASSIFICATION
    assert "else null" in CLASSIFICATION
    assert outcome_win_loss([
        {"strategy": "Long Call", "status": "Closed", "total_pnl": 0},
        {"strategy": "Long Call", "status": "Closed", "total_pnl": 0.004},
        {"strategy": "Long Call", "status": "Closed", "total_pnl": -12},
    ]) == (0, 1)
    rolled = _rollup_int_strategy_to_summary_shape(pd.DataFrame([
        {
            "account": "A", "symbol": "NOW", "strategy": "Long Call",
            "status": "Closed", "total_pnl": 0.0, "realized_pnl": 0.0,
            "unrealized_pnl": 0.0, "num_trades": 1, "is_winner": None,
            "premium_received": 0, "premium_paid": 100, "days_in_trade": 5,
        },
        {
            "account": "A", "symbol": "NOW", "strategy": "Long Call",
            "status": "Closed", "total_pnl": -10.0, "realized_pnl": -10.0,
            "unrealized_pnl": 0.0, "num_trades": 1, "is_winner": False,
            "premium_received": 0, "premium_paid": 50, "days_in_trade": 4,
        },
    ]))
    assert int(rolled.iloc[0]["num_winners"]) == 0
    assert int(rolled.iloc[0]["num_losers"]) == 1


def test_covered_call_run_net_includes_fees():
    from app.covered_call_runs import build_covered_call_runs

    rows = pd.DataFrame([
        {
            "trade_date": date(2026, 1, 2), "action": "equity_buy",
            "instrument_type": "Equity", "trade_symbol": "RKLB",
            "quantity": 100, "price": 50.0, "amount": -5001.3, "fees": 1.3,
            "account": "Schwab", "tenant_id": "snaptrade:acct",
        },
        {
            "trade_date": date(2026, 1, 2), "action": "option_sell_to_open",
            "instrument_type": "Call", "trade_symbol": "RKLB  260117C00055000",
            "quantity": 1, "price": 1.0, "amount": 99.0, "fees": 1.0,
            "account": "Schwab", "tenant_id": "snaptrade:acct",
        },
    ])
    runs = build_covered_call_runs(rows, as_of=date(2026, 1, 4))
    assert runs[0]["calls"][0]["premium"] == 100.0
    assert runs[0]["fees"] == pytest.approx(2.3)
    assert runs[0]["net"] == pytest.approx(97.7)


def test_premium_collected_tile_and_fills_label():
    assert "(kpis.premium_collected or 0) > 0" in PAGE
    assert ">Fills</th>" in POSITIONS or ">Fills</a>" in POSITIONS
    assert "incl. dividends" in POSITIONS
    assert "open rows" in POSITIONS


def test_paper_receipt_carries_tenant_for_the_link():
    from app.paper_practice import practice_receipt

    receipt = practice_receipt({
        "sentence": "You are buying a call on AMD.",
        "symbol": "AMD",
        "side": "call",
        "strike": 100,
        "expiry": "2026-09-30",
        "expiry_label": "Daily",
        "limit_label": "$1.00",
        "cost_label": "$100.00",
    }, tenant_id="snaptrade:paper")
    assert receipt["tenant_id"] == "snaptrade:paper"
    page = Path("app/templates/paper_practice.html").read_text()
    assert "tenants=sent.tenant_id" in page
    assert "tenants=order.tenant_id" in page
