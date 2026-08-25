"""Execution review (app/execution_quality.py).

Pins the aggregation + phrasing of int_option_exit_quality rows into the
Trader Profile card, the per-symbol mirror sentences, and the day-row
verdict notes — plus the data-sufficiency gates (the "after X days"
promise). All synthetic frames; no BigQuery.
"""

from datetime import date, timedelta

import pandas as pd

from app.execution_quality import (
    MIN_GRADED_PROFILE,
    MIN_TREND_RECENT,
    TREND_WINDOW_DAYS,
    execution_trend,
    exit_notes,
    open_option_record,
    summarize_execution,
    symbol_execution_sentences,
    verdicts_landed,
    verdicts_pending,
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
    """Takeaway-first shape: headline (net of all graded deltas) +
    scannable findings rows — one bold number per row, detail in prose."""
    out = summarize_execution(_profile_df())
    assert out is not None
    assert out["n_graded"] == 8

    # Headline: -120 + 820 - 25 + 400 + 900 - 200 = +$1,775 net.
    assert out["headline"]["value"] == "+$1,775"
    assert out["headline"]["tone"] == "pos"
    assert "8 contracts graded" in out["headline"]["sub"]

    f = {x["label"]: x for x in out["findings"]}
    # Longs lead: net +$700, beat holding in 1 of 2.
    assert list(f)[0] == "Long exits"
    assert f["Long exits"]["value"] == "+$700"
    assert "beat holding in 1" in f["Long exits"]["detail"]
    # Shorts: 3 of 4 worthless anyway; dodged $820 on the 1 ITM finish.
    assert f["Short buybacks"]["value"] == "+$700"  # -120 + 820
    assert "3 of 4 buybacks" in f["Short buybacks"]["detail"]
    assert "$820" in f["Short buybacks"]["detail"]
    # Rolls: 1 of 2 never tested; the other sidestepped $400.
    assert f["Rolls never tested"]["value"] == "1 of 2"
    assert "$400" in f["Rolls never tested"]["detail"]
    # Expiry discipline: $215 kept across 2 contracts.
    assert f["Held to expiry"]["value"] == "$215 kept"
    # Marks still accumulating (no data_reliable rows) → note, no finding.
    assert "Peak capture" not in f
    assert "still accumulating" in out["pending_note"]

    # Examples sorted by |delta|; biggest first (ORCL +$900).
    assert out["examples"][0]["symbol"] == "ORCL"
    assert len(out["examples"]) == 3


def test_profile_marks_finding_when_coverage_reliable():
    rows = [_row(trade_symbol=f"T{i}", data_reliable=True,
                 peak_unrealized_pnl=100.0, realized_pnl=80.0)
            for i in range(5)]
    out = summarize_execution(pd.DataFrame(rows))
    f = {x["label"]: x for x in out["findings"]}
    assert f["Peak capture"]["value"] == "80% median"
    assert "5 winners" in f["Peak capture"]["detail"]
    assert out["pending_note"] is None


# ── Per-symbol mirror sentences ──────────────────────────────────────────

def test_symbol_sentences_net_giveup():
    df = pd.DataFrame([
        _row(early_close_vs_expiry_delta=-45.0),
        _row(trade_symbol="SOFI 250620C00007500",
             early_close_vs_expiry_delta=-30.0),
    ])
    out = symbol_execution_sentences(df)
    assert len(out) == 1
    assert "2 of 2 expired worthless anyway" in out[0]
    assert "gave up $75" in out[0]


def test_symbol_sentences_roll_line():
    df = pd.DataFrame([
        _row(was_rolled=True, early_close_vs_expiry_delta=-25.0),
        _row(trade_symbol="SOFI 250620C00007500", was_rolled=True,
             early_close_vs_expiry_delta=-30.0),
    ])
    out = symbol_execution_sentences(df)
    assert any("2 of 2 rolls were never tested" in s for s in out)


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
    assert "expired worthless — the early close gave up $45" in \
        notes[("snaptrade:abc", "SOFI 250620C00007000")]
    assert "closing early avoided $820" in \
        notes[("snaptrade:abc", "SOFI 250620C00008000")]
    assert "never tested" in \
        notes[("snaptrade:abc", "RKLB 250321C00030000")]
    assert "worth $200 more than the exit price" in \
        notes[("snaptrade:abc", "ORCL 260618C00200000")]
    assert ("snaptrade:abc", "TINY 250620C00001000") not in notes


# ── Rolling self-comparison ──────────────────────────────────────────────

_TODAY = date(2026, 8, 9)


def _prepped(rows):
    """Run rows through the module's own _prep (dates → date objects)."""
    from app.execution_quality import _prep
    df = _prep(pd.DataFrame(rows))
    return df[df["gradeable_early_close"]
              & df["early_close_vs_expiry_delta"].notna()]


def test_trend_requires_recent_and_baseline_samples():
    recent_day = _TODAY - timedelta(days=10)
    old_day = _TODAY - timedelta(days=TREND_WINDOW_DAYS + 30)
    # All recent, no baseline → None.
    rows = [_row(trade_symbol=f"R{i}", close_date=recent_day)
            for i in range(MIN_TREND_RECENT)]
    assert execution_trend(_prepped(rows), today=_TODAY) is None
    # All old, no recent → None.
    rows = [_row(trade_symbol=f"O{i}", close_date=old_day)
            for i in range(MIN_TREND_RECENT)]
    assert execution_trend(_prepped(rows), today=_TODAY) is None


def test_trend_compares_recent_avg_to_baseline_avg():
    recent_day = _TODAY - timedelta(days=10)
    old_day = _TODAY - timedelta(days=TREND_WINDOW_DAYS + 30)
    rows = (
        # Recent: avg -$30/contract.
        [_row(trade_symbol=f"R{i}", close_date=recent_day,
              early_close_vs_expiry_delta=-30.0) for i in range(3)]
        # Baseline: avg -$80/contract.
        + [_row(trade_symbol=f"O{i}", close_date=old_day,
                early_close_vs_expiry_delta=-80.0) for i in range(3)]
    )
    out = execution_trend(_prepped(rows), today=_TODAY)
    assert out is not None
    assert out["recent_avg"] == -30.0
    assert out["baseline_avg"] == -80.0
    assert out["n_recent"] == 3


def test_trend_finding_reaches_profile_card():
    recent_day = _TODAY - timedelta(days=10)
    old_day = _TODAY - timedelta(days=TREND_WINDOW_DAYS + 30)
    rows = (
        [_row(trade_symbol=f"R{i}", close_date=recent_day,
              early_close_vs_expiry_delta=25.0) for i in range(3)]
        + [_row(trade_symbol=f"O{i}", close_date=old_day,
                early_close_vs_expiry_delta=-60.0) for i in range(3)]
    )
    out = summarize_execution(pd.DataFrame(rows), today=_TODAY)
    assert out is not None
    f = {x["label"]: x for x in out["findings"]}
    trend = f[f"Last {TREND_WINDOW_DAYS} days"]
    assert trend["value"] == "+$25/exit"
    assert "vs \u2212$60/exit before the window" in trend["detail"]


# ── Verdict maturation (Daily Review) ────────────────────────────────────

def test_verdicts_landed_windows_on_expiry_date():
    in_window = _row(trade_symbol="A", option_expiry=_TODAY - timedelta(days=2))
    out_window = _row(trade_symbol="B", symbol="RKLB",
                      option_expiry=_TODAY - timedelta(days=30))
    future = _row(trade_symbol="C", symbol="ORCL",
                  option_expiry=_TODAY + timedelta(days=5),
                  gradeable_early_close=False,
                  early_close_vs_expiry_delta=None,
                  intrinsic_at_expiry=None, expired_worthless=None)
    df = pd.DataFrame([in_window, out_window, future])
    landed = verdicts_landed(df, _TODAY - timedelta(days=6), _TODAY)
    assert [v["symbol"] for v in landed] == ["SOFI"]
    # sentence = self-contained prose for the email; action = short line
    # for the page (the delta renders in its own column there).
    assert "expired worthless" in landed[0]["sentence"]
    assert landed[0]["action"] == \
        "Bought back the $7 call; it expired worthless anyway."
    assert landed[0]["delta"] == -45.0


def test_verdicts_landed_sorted_by_magnitude():
    df = pd.DataFrame([
        _row(trade_symbol="A", option_expiry=_TODAY,
             early_close_vs_expiry_delta=-30.0),
        _row(trade_symbol="B", symbol="RKLB", option_expiry=_TODAY,
             expired_worthless=False, intrinsic_at_expiry=8.2,
             early_close_vs_expiry_delta=820.0),
    ])
    landed = verdicts_landed(df, _TODAY - timedelta(days=6), _TODAY)
    assert [v["symbol"] for v in landed] == ["RKLB", "SOFI"]
    assert "closing early avoided $820" in landed[0]["sentence"]


def test_verdicts_pending_counts_and_next():
    df = pd.DataFrame([
        # Early close, expiry still ahead → pending.
        _row(trade_symbol="A", option_expiry=_TODAY + timedelta(days=12),
             gradeable_early_close=False, early_close_vs_expiry_delta=None),
        _row(trade_symbol="B", symbol="RKLB",
             option_expiry=_TODAY + timedelta(days=5),
             gradeable_early_close=False, early_close_vs_expiry_delta=None),
        # Already matured → not pending.
        _row(trade_symbol="C", symbol="ORCL",
             option_expiry=_TODAY - timedelta(days=3)),
        # Expiry-type close → never pending.
        _row(trade_symbol="D", symbol="JEPI", close_type="Expired",
             option_expiry=_TODAY + timedelta(days=5)),
    ])
    out = verdicts_pending(df, _TODAY)
    assert out["n"] == 2
    assert out["next_symbol"] == "RKLB"
    assert out["next_short_label"] == "$7 call"
    assert out["items"][0]["days_away"] == 5


def test_verdicts_empty_frames():
    assert verdicts_landed(pd.DataFrame(), _TODAY, _TODAY) == []
    assert verdicts_pending(None, _TODAY) is None


def test_verdicts_landed_groups_put_spread_as_one_net():
    """VICR-shaped: short $210 put + long $190 put, same tenant/expiry.
    Showing ±$28k and ±$24k independently is the misleading feed; the
    trader closed one spread, so the verdict is the net vs holding both."""
    df = pd.DataFrame([
        _row(symbol="VICR", trade_symbol="VICR  260821P00210000",
             option_type="P", option_strike=210.0, direction="Sold",
             option_expiry=_TODAY, expired_worthless=False,
             early_close_vs_expiry_delta=-28187.0),
        _row(symbol="VICR", trade_symbol="VICR  260821P00190000",
             option_type="P", option_strike=190.0, direction="Bought",
             option_expiry=_TODAY, expired_worthless=False,
             early_close_vs_expiry_delta=24293.0),
        _row(symbol="FN", trade_symbol="FN    260821C00600000",
             option_type="C", option_strike=600.0, direction="Bought",
             option_expiry=_TODAY, expired_worthless=False,
             early_close_vs_expiry_delta=26993.0),
    ])
    landed = verdicts_landed(df, _TODAY - timedelta(days=6), _TODAY)
    assert [v["symbol"] for v in landed] == ["FN", "VICR"]
    vicr = landed[1]
    assert vicr["delta"] == -3894.0
    assert vicr["structure"] == "Put Spread"
    assert vicr["action"] == "Closed the $190 / $210 put spread."
    assert "worse than holding both legs" in vicr["sentence"]
    assert "$3,894" in vicr["sentence"]
    fn = landed[0]
    assert fn["structure"] is None
    assert "Sold the $600 call" in fn["action"]


def test_verdicts_landed_does_not_fuse_two_tenants():
    df = pd.DataFrame([
        _row(tenant_id="snaptrade:one", symbol="VICR",
             trade_symbol="VICR  260821P00210000", option_type="P",
             option_strike=210.0, direction="Sold", option_expiry=_TODAY,
             early_close_vs_expiry_delta=-100.0),
        _row(tenant_id="snaptrade:two", symbol="VICR",
             trade_symbol="VICR  260821P00190000", option_type="P",
             option_strike=190.0, direction="Bought", option_expiry=_TODAY,
             expired_worthless=False, early_close_vs_expiry_delta=80.0),
    ])
    landed = verdicts_landed(df, _TODAY - timedelta(days=6), _TODAY)
    assert len(landed) == 2
    assert all(v["structure"] is None for v in landed)


def test_verdicts_pending_counts_a_spread_as_one():
    df = pd.DataFrame([
        _row(symbol="VICR", trade_symbol="VICR  261120P00210000",
             option_type="P", option_strike=210.0, direction="Sold",
             option_expiry=_TODAY + timedelta(days=12),
             gradeable_early_close=False, early_close_vs_expiry_delta=None),
        _row(symbol="VICR", trade_symbol="VICR  261120P00190000",
             option_type="P", option_strike=190.0, direction="Bought",
             option_expiry=_TODAY + timedelta(days=12),
             gradeable_early_close=False, early_close_vs_expiry_delta=None),
        _row(symbol="LITE", trade_symbol="LITE  261120C01100000",
             option_type="C", option_strike=1100.0, direction="Sold",
             option_expiry=_TODAY + timedelta(days=5),
             gradeable_early_close=False, early_close_vs_expiry_delta=None),
    ])
    out = verdicts_pending(df, _TODAY)
    assert out["n"] == 2
    assert out["next_symbol"] == "LITE"
    assert out["next_short_label"] == "$1100 call"
    vicr = [i for i in out["items"] if i["symbol"] == "VICR"][0]
    assert vicr["short_label"] == "$190 / $210 put spread"


def test_two_same_direction_puts_stay_independent():
    """Two cash-secured puts, same expiry, no long leg — not a spread."""
    df = pd.DataFrame([
        _row(symbol="VICR", trade_symbol="VICR  260821P00210000",
             option_type="P", option_strike=210.0, direction="Sold",
             option_expiry=_TODAY, early_close_vs_expiry_delta=-40.0),
        _row(symbol="VICR", trade_symbol="VICR  260821P00190000",
             option_type="P", option_strike=190.0, direction="Sold",
             option_expiry=_TODAY, early_close_vs_expiry_delta=-30.0),
    ])
    landed = verdicts_landed(df, _TODAY - timedelta(days=6), _TODAY)
    assert len(landed) == 2
    assert all(v["structure"] is None for v in landed)


# ── Live open-option record ──────────────────────────────────────────────

def _open_row(**kw):
    base = {
        "tenant_id": "snaptrade:abc",
        "account": "Schwab Account",
        "symbol": "SOFI",
        "trade_symbol": "SOFI 260918C00007000",
        "option_type": "C",
        "option_strike": 7.0,
        "option_expiry": _TODAY + timedelta(days=40),
        "direction": "Sold",
        "open_date": _TODAY - timedelta(days=20),
        "contracts_sold_to_open": 1.0,
        "contracts_bought_to_open": 0.0,
        "premium_received": 200.0,
        "premium_paid": 0.0,
        "current_market_value": -50.0,
        "current_unrealized_pnl": 150.0,
    }
    base.update(kw)
    return base


def test_open_record_short_premium_capture():
    out = open_option_record(pd.DataFrame([_open_row()]), _TODAY)
    assert out is not None and len(out["shorts"]) == 1
    s = out["shorts"][0]
    assert s["captured_pct"] == 75  # 150 of 200 premium
    assert s["days_left"] == 40
    assert s["premium"] == 200.0
    assert not out["longs"]


def test_open_record_long_mark_vs_paid():
    row = _open_row(direction="Bought", premium_received=0.0,
                    premium_paid=-400.0, current_market_value=500.0,
                    current_unrealized_pnl=100.0)
    out = open_option_record(pd.DataFrame([row]), _TODAY)
    assert out is not None and len(out["longs"]) == 1
    l = out["longs"][0]
    assert l["paid"] == 400.0
    assert l["mark"] == 500.0
    assert l["change_pct"] == 25


def test_open_record_skips_unmarked_and_past_expiry():
    never_snapshotted = _open_row(trade_symbol="X",
                                  current_market_value=0.0,
                                  current_unrealized_pnl=0.0)
    stale_expiry = _open_row(trade_symbol="Y",
                             option_expiry=_TODAY - timedelta(days=2))
    assert open_option_record(
        pd.DataFrame([never_snapshotted, stale_expiry]), _TODAY) is None
    assert open_option_record(pd.DataFrame(), _TODAY) is None
    assert open_option_record(None, _TODAY) is None


def test_exit_note_renders_on_completing_close_in_story():
    trades = pd.DataFrame([
        {"trade_date": date(2025, 5, 1), "action": "option_sell_to_open",
         "instrument_type": "Call", "trade_symbol": "SOFI 250620C00007000",
         "quantity": 1, "price": 1.0, "amount": 100.0,
         "account": "Schwab", "tenant_id": "snaptrade:abc"},
        {"trade_date": date(2025, 6, 1), "action": "option_buy_to_close",
         "instrument_type": "Call", "trade_symbol": "SOFI 250620C00007000",
         "quantity": 1, "price": 0.45, "amount": -45.0,
         "account": "Schwab", "tenant_id": "snaptrade:abc"},
    ])
    notes = exit_notes(pd.DataFrame([_row()]))
    items, _, _ = build_position_story(trades, None, exit_notes=notes)
    close_day = [i for i in items
                 if i["type"] == "day" and i["date_iso"] == "2025-06-01"][0]
    joined = " ".join(close_day["headlines"])
    assert "Bought back the $7 call" in joined
    assert "After the fact" in joined
    # The open day carries no verdict.
    open_day = [i for i in items
                if i["type"] == "day" and i["date_iso"] == "2025-05-01"][0]
    assert "After the fact" not in " ".join(open_day["headlines"])


def test_exit_notes_stay_with_their_tenant_for_same_contract():
    """Two accounts can hold the same OCC symbol; verdicts must not cross."""
    trade_symbol = "SOFI 250620C00007000"
    trades = pd.DataFrame([
        {"trade_date": date(2025, 5, 1), "action": "option_sell_to_open",
         "instrument_type": "Call", "trade_symbol": trade_symbol,
         "quantity": 1, "price": 1.0, "amount": 100.0,
         "account": "Schwab Account", "tenant_id": "snaptrade:one"},
        {"trade_date": date(2025, 5, 2), "action": "option_sell_to_open",
         "instrument_type": "Call", "trade_symbol": trade_symbol,
         "quantity": 1, "price": 2.0, "amount": 200.0,
         "account": "Schwab Account", "tenant_id": "snaptrade:two"},
        {"trade_date": date(2025, 6, 1), "action": "option_buy_to_close",
         "instrument_type": "Call", "trade_symbol": trade_symbol,
         "quantity": 1, "price": 0.45, "amount": -45.0,
         "account": "Schwab Account", "tenant_id": "snaptrade:one"},
        {"trade_date": date(2025, 6, 2), "action": "option_buy_to_close",
         "instrument_type": "Call", "trade_symbol": trade_symbol,
         "quantity": 1, "price": 0.20, "amount": -20.0,
         "account": "Schwab Account", "tenant_id": "snaptrade:two"},
    ])
    verdicts = pd.DataFrame([
        _row(tenant_id="snaptrade:one", account="Schwab Account",
             trade_symbol=trade_symbol, expired_worthless=True,
             early_close_vs_expiry_delta=-45.0),
        _row(tenant_id="snaptrade:two", account="Schwab Account",
             trade_symbol=trade_symbol, expired_worthless=False,
             intrinsic_at_expiry=8.2, early_close_vs_expiry_delta=820.0),
    ])

    items, _, _ = build_position_story(
        trades, None, exit_notes=exit_notes(verdicts))
    first_close = next(
        i for i in items
        if i["type"] == "day" and i["date_iso"] == "2025-06-01")
    second_close = next(
        i for i in items
        if i["type"] == "day" and i["date_iso"] == "2025-06-02")
    first_text = " ".join(first_close["headlines"])
    second_text = " ".join(second_close["headlines"])

    assert "expired worthless" in first_text
    assert "closing early avoided $820" not in first_text
    assert "closing early avoided $820" in second_text
    assert "expired worthless" not in second_text
