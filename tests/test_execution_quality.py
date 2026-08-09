"""Execution review (app/execution_quality.py).

Pins the aggregation + phrasing of int_option_exit_quality rows into the
Trader Profile card, the per-symbol mirror sentences, and the day-row
verdict notes — plus the data-sufficiency gates (the "after X days"
promise). All synthetic frames; no BigQuery.
"""

from datetime import date

import pandas as pd

from app.execution_quality import (
    MIN_GRADED_PROFILE,
    exit_notes,
    summarize_execution,
    symbol_execution_sentences,
)
from app.position_story import build_position_story


def _row(**kw):
    base = {
        "tenant_id": "snaptrade:abc",
        "account": "Schwab Account",
        "symbol": "SOFI",
        "trade_symbol": "SOFI 250620C00007000",
        "option_type": "C",
        "option_strike": 7.0,
        "option_expiry": date(2025, 6, 20),
        "direction": "Sold",
        "open_date": date(2025, 5, 1),
        "close_date": date(2025, 6, 1),
        "close_type": "Closed",
        "days_held": 31,
        "dte_at_close": 19,
        "contracts": 1.0,
        "premium_received": 100.0,
        "cost_to_close": -45.0,
        "proceeds_from_close": 0.0,
        "realized_pnl": 55.0,
        "underlying_close_at_expiry": 6.10,
        "intrinsic_at_expiry": 0.0,
        "expiry_settlement_value": 0.0,
        "expired_worthless": True,
        "gradeable_early_close": True,
        "early_close_vs_expiry_delta": -45.0,
        "was_rolled": False,
        "roll_new_strike": None,
        "roll_new_expiry": None,
        "net_roll_credit": None,
        "peak_unrealized_pnl": None,
        "snapshot_count": 0,
        "snapshot_density": 0.0,
        "data_reliable": False,
        "pnl_given_back": 0.0,
        "giveback_pct": 0.0,
    }
    base.update(kw)
    return base


# ── Sufficiency gates ─────────────────────────────────────────────────────

def test_profile_gate_below_threshold_returns_none():
    df = pd.DataFrame([_row() for _ in range(MIN_GRADED_PROFILE - 1)])
    assert summarize_execution(df) is None


def test_profile_gate_empty_and_missing_frames():
    assert summarize_execution(pd.DataFrame()) is None
    assert summarize_execution(None) is None
    assert symbol_execution_sentences(pd.DataFrame()) == []
    assert exit_notes(None) == {}


def test_symbol_gate_needs_two_graded():
    df = pd.DataFrame([_row()])
    assert symbol_execution_sentences(df) == []


# ── Profile card ─────────────────────────────────────────────────────────

def _profile_df():
    rows = []
    # 3 short buybacks on contracts that expired worthless (cost money).
    for i in range(3):
        rows.append(_row(trade_symbol=f"SOFI 250620C0000700{i}",
                         early_close_vs_expiry_delta=-40.0))
    # 1 short buyback that dodged an ITM finish (saved money).
    rows.append(_row(trade_symbol="SOFI 250620C00008000",
                     expired_worthless=False, intrinsic_at_expiry=8.2,
                     expiry_settlement_value=820.0,
                     early_close_vs_expiry_delta=820.0))
    # 2 rolls: one never tested, one that sidestepped settlement value.
    rows.append(_row(trade_symbol="RKLB 250321C00030000", symbol="RKLB",
                     was_rolled=True, early_close_vs_expiry_delta=-25.0))
    rows.append(_row(trade_symbol="RKLB 250418C00028000", symbol="RKLB",
                     was_rolled=True, expired_worthless=False,
                     intrinsic_at_expiry=4.0, expiry_settlement_value=400.0,
                     early_close_vs_expiry_delta=400.0))
    # 2 long sales, net ahead of expiry.
    rows.append(_row(trade_symbol="ORCL 260618C00200000", symbol="ORCL",
                     direction="Bought", premium_received=0.0,
                     cost_to_close=0.0, proceeds_from_close=900.0,
                     realized_pnl=500.0,
                     early_close_vs_expiry_delta=900.0))
    rows.append(_row(trade_symbol="ORCL 260618C00210000", symbol="ORCL",
                     direction="Bought", premium_received=0.0,
                     cost_to_close=0.0, proceeds_from_close=100.0,
                     realized_pnl=-50.0, expired_worthless=False,
                     intrinsic_at_expiry=3.0,
                     early_close_vs_expiry_delta=-200.0))
    # 2 held to worthless expiry (discipline chip).
    rows.append(_row(trade_symbol="JEPI 250516C00060000", symbol="JEPI",
                     close_type="Expired", gradeable_early_close=False,
                     early_close_vs_expiry_delta=None, realized_pnl=120.0))
    rows.append(_row(trade_symbol="JEPI 250620C00061000", symbol="JEPI",
                     close_type="ExpiredOTM", gradeable_early_close=False,
                     early_close_vs_expiry_delta=None, realized_pnl=95.0))
    return pd.DataFrame(rows)


def test_profile_summary_buckets_and_copy():
    out = summarize_execution(_profile_df())
    assert out is not None
    assert out["n_graded"] == 8
    text = " ".join(out["sentences"])
    # Early buybacks: 4 shorts, 3 worthless, $120 given up, $820 avoided.
    assert "bought back 4 short contracts before expiry" in text
    assert "3 of them went on to expire worthless" in text
    assert "$120" in text and "$820" in text
    # Rolls: 1 of 2 never tested; $400 sidestepped.
    assert "2 strikes you rolled away from" in text
    assert "$400" in text
    # Longs: 1 of 2 beat expiry, net +$700.
    assert "selling early beat holding to expiry in 1 of them" in text
    assert "$700" in text
    # Marks record still accumulating (no data_reliable rows).
    assert "still accumulating" in text

    labels = {c["label"]: c["value"] for c in out["chips"]}
    assert labels["Contracts graded"] == "8"
    assert labels["Rolls never tested"] == "1 of 2"
    assert "$215" in labels["Held to worthless expiry"]  # 120 + 95 kept

    # Examples sorted by |delta|; biggest first (ORCL +$900).
    assert out["examples"][0]["symbol"] == "ORCL"
    assert len(out["examples"]) == 3


def test_profile_marks_sentence_when_coverage_reliable():
    rows = [_row(trade_symbol=f"T{i}", data_reliable=True,
                 peak_unrealized_pnl=100.0, realized_pnl=80.0)
            for i in range(5)]
    out = summarize_execution(pd.DataFrame(rows))
    text = " ".join(out["sentences"])
    assert "captured a median 80% of the best exit" in text
    labels = {c["label"]: c["value"] for c in out["chips"]}
    assert labels["Median peak capture"] == "80%"


# ── Per-symbol mirror sentences ──────────────────────────────────────────

def test_symbol_sentences_net_giveup():
    df = pd.DataFrame([
        _row(early_close_vs_expiry_delta=-45.0),
        _row(trade_symbol="SOFI 250620C00007500",
             early_close_vs_expiry_delta=-30.0),
    ])
    out = symbol_execution_sentences(df)
    assert len(out) == 1
    assert "2 of the contracts you closed early here have a known expiry outcome" in out[0]
    assert "2 went on to expire worthless" in out[0]
    assert "gave up $75" in out[0]


def test_symbol_sentences_roll_line():
    df = pd.DataFrame([
        _row(was_rolled=True, early_close_vs_expiry_delta=-25.0),
        _row(trade_symbol="SOFI 250620C00007500", was_rolled=True,
             early_close_vs_expiry_delta=-30.0),
    ])
    out = symbol_execution_sentences(df)
    assert any("2 of the 2 strikes you rolled away from were never tested"
               in s for s in out)


# ── Day-row verdict notes ────────────────────────────────────────────────

def test_exit_notes_variants():
    df = pd.DataFrame([
        _row(),  # short buyback, expired worthless, -45
        _row(trade_symbol="SOFI 250620C00008000", expired_worthless=False,
             intrinsic_at_expiry=8.2, early_close_vs_expiry_delta=820.0),
        _row(trade_symbol="RKLB 250321C00030000", was_rolled=True,
             early_close_vs_expiry_delta=-25.0),
        _row(trade_symbol="ORCL 260618C00200000", direction="Bought",
             expired_worthless=False, intrinsic_at_expiry=3.0,
             early_close_vs_expiry_delta=-200.0),
        # Below the noise floor: no note.
        _row(trade_symbol="TINY 250620C00001000",
             early_close_vs_expiry_delta=-5.0),
    ])
    notes = exit_notes(df)
    assert "expired worthless — the early close gave up $45" in notes["SOFI 250620C00007000"]
    assert "closing early avoided $820" in notes["SOFI 250620C00008000"]
    assert "never tested" in notes["RKLB 250321C00030000"]
    assert "worth $200 more than the exit price" in notes["ORCL 260618C00200000"]
    assert "TINY 250620C00001000" not in notes


def test_exit_note_renders_on_completing_close_in_story():
    trades = pd.DataFrame([
        {"trade_date": date(2025, 5, 1), "action": "option_sell_to_open",
         "instrument_type": "Call", "trade_symbol": "SOFI 250620C00007000",
         "quantity": 1, "price": 1.0, "amount": 100.0,
         "account": "Schwab", "tenant_id": None},
        {"trade_date": date(2025, 6, 1), "action": "option_buy_to_close",
         "instrument_type": "Call", "trade_symbol": "SOFI 250620C00007000",
         "quantity": 1, "price": 0.45, "amount": -45.0,
         "account": "Schwab", "tenant_id": None},
    ])
    notes = exit_notes(pd.DataFrame([_row()]))
    items, _, _ = build_position_story(trades, None, exit_notes=notes)
    close_day = [i for i in items
                 if i["type"] == "day" and i["date_iso"] == "2025-06-01"][0]
    joined = " ".join(close_day["headlines"])
    assert "Bought back the $7 call" in joined
    assert "The record after the fact" in joined
    # The open day carries no verdict.
    open_day = [i for i in items
                if i["type"] == "day" and i["date_iso"] == "2025-05-01"][0]
    assert "record after the fact" not in " ".join(open_day["headlines"])
