"""Position story engine (app/position_story.py).

Pins the semantic maneuver detection — rolls, wheels, covered calls,
kept premium, scale-in/out — and the interlude narration built from the
daily-mark chart series. All synthetic frames; no BigQuery.
"""

from datetime import date

import pandas as pd

from app.position_story import build_position_story, parse_occ


def _trades(rows):
    """rows: date/action/type/symbol/qty/price/amount[/account/tenant_id]."""
    return pd.DataFrame([
        {
            "trade_date": r[0], "action": r[1], "instrument_type": r[2],
            "trade_symbol": r[3], "quantity": r[4], "price": r[5],
            "amount": r[6], "account": r[7] if len(r) > 7 else "Schwab",
            "tenant_id": r[8] if len(r) > 8 else None,
        }
        for r in rows
    ])


def _headlines(items):
    out = []
    for it in items:
        if it["type"] == "day":
            out.extend(it["headlines"])
    return " | ".join(out)


def test_parse_occ():
    occ = parse_occ("RKLB 250117C00037000")
    assert occ == {"expiry": date(2025, 1, 17), "option_type": "call", "strike": 37.0}
    assert parse_occ("RKLB") is None
    assert parse_occ(None) is None
    assert parse_occ("BRK B 250117P00300500")["strike"] == 300.5


def test_roll_up_and_out_detected():
    df = _trades([
        (date(2024, 11, 1), "option_sell_to_open", "Call", "RKLB 241115C00011000", 1, 0.52, 52.0),
        (date(2024, 11, 12), "option_buy_to_close", "Call", "RKLB 241115C00011000", 1, 2.10, -210.0),
        (date(2024, 11, 12), "option_sell_to_open", "Call", "RKLB 241220C00013000", 1, 2.80, 280.0),
    ])
    items, markers, stats = build_position_story(df, None)
    text = _headlines(items)
    assert "Rolled the short $11 call up and out" in text
    assert "$11 Nov 15 → $13 Dec 20" in text
    assert "collecting a net $70 credit" in text
    # The roll day is one chapter with one headline, not two fills narrated
    # separately.
    roll_day = [i for i in items if i["type"] == "day" and i["date_iso"] == "2024-11-12"][0]
    assert len(roll_day["headlines"]) == 1
    assert len(roll_day["events"]) == 2  # raw fills still listed underneath


def test_wheel_full_cycle_narrated():
    df = _trades([
        # No shares -> short put = wheel opening.
        (date(2024, 6, 3), "option_sell_to_open", "Put", "F 240621P00012000", 1, 0.60, 60.0),
        # Assigned: option row + mechanical equity fill at the strike.
        (date(2024, 6, 21), "option_assigned", "Put", "F 240621P00012000", 1, None, 0.0),
        (date(2024, 6, 21), "equity_buy", "Equity", "F", 100, 12.0, -1200.0),
        # Covered call against the assigned shares.
        (date(2024, 6, 24), "option_sell_to_open", "Call", "F 240719C00013000", 1, 0.45, 45.0),
        # Called away: option row + mechanical equity sell.
        (date(2024, 7, 19), "option_assigned", "Call", "F 240719C00013000", 1, None, 0.0),
        (date(2024, 7, 19), "equity_sell", "Equity", "F", 100, 13.0, 1300.0),
    ])
    items, _, _stats = build_position_story(df, None)
    text = _headlines(items)
    assert "Opened a wheel" in text
    assert "wheel turns" in text
    assert "$60 of premium already collected" in text
    assert "covered call" in text
    assert "called away at $13" in text
    assert "Wheel complete: $105 of premium collected" in text
    # The mechanical equity fills are swallowed by the assignment sentences.
    assert "Started the stock position" not in text
    assert "Sold the last" not in text


def test_short_put_expiry_keeps_premium():
    df = _trades([
        (date(2024, 3, 1), "option_sell_to_open", "Put", "T 240315P00016000", 2, 1.15, 230.0),
        (date(2024, 3, 15), "option_expired", "Put", "T 240315P00016000", 2, None, 0.0),
    ])
    items, _, _stats = build_position_story(df, None)
    text = _headlines(items)
    assert "expired worthless" in text
    assert "kept the full $230 premium" in text


def test_long_option_expiry_loses_premium():
    df = _trades([
        (date(2024, 3, 1), "option_buy_to_open", "Call", "T 240315C00020000", 1, 0.80, -80.0),
        (date(2024, 3, 15), "option_expired", "Call", "T 240315C00020000", 1, None, 0.0),
    ])
    items, _, _stats = build_position_story(df, None)
    text = _headlines(items)
    assert "bullish bet" in text
    assert "$80 paid for it is gone" in text


def test_equity_lifecycle_phrasing():
    df = _trades([
        (date(2024, 1, 2), "equity_buy", "Equity", "AAPL", 100, 10.37, -1037.0),
        (date(2024, 1, 9), "equity_buy", "Equity", "AAPL", 50, 11.00, -550.0),
        (date(2024, 1, 16), "equity_sell", "Equity", "AAPL", 50, 13.50, 675.0),
        (date(2024, 1, 23), "equity_sell", "Equity", "AAPL", 100, 14.00, 1400.0),
    ])
    items, _, _stats = build_position_story(df, None)
    text = _headlines(items)
    assert "Started the stock position: 100 shares at $10.37" in text
    assert "Added 50 shares at $11.00 — now holding 150" in text
    assert "Trimmed 50 shares at $13.50 — 100 still working" in text
    assert "Sold the last 100 shares at $14.00 — flat on the stock again" in text


def test_dividends_and_marker_alignment():
    df = _trades([
        (date(2024, 5, 1), "equity_buy", "Equity", "JEPI", 200, 55.0, -11000.0),
    ])
    div = pd.DataFrame([
        {"trade_date": "2024-06-03", "amount": 54.12},
    ])
    items, markers, stats = build_position_story(df, div)
    text = _headlines(items)
    assert "Collected $54.12 in dividends just for holding" in text
    days = [i for i in items if i["type"] == "day"]
    assert [d["date_iso"] for d in days] == ["2024-05-01", "2024-06-03"]
    assert [m["d"] for m in markers] == ["2024-05-01", "2024-06-03"]
    assert all(m["t"] for m in markers)  # tooltips lead with the headlines
    assert days[1]["kind"] == "income"


def test_interlude_from_daily_marks():
    df = _trades([
        (date(2024, 1, 2), "option_sell_to_open", "Call", "X 240315C00020000", 1, 3.00, 300.0),
        (date(2024, 2, 20), "option_buy_to_close", "Call", "X 240315C00020000", 1, 0.50, -50.0),
    ])
    # Daily chart: options decay steadily in the seller's favor.
    dates = [f"2024-01-{d:02d}" for d in range(2, 32)] + [f"2024-02-{d:02d}" for d in range(1, 21)]
    n = len(dates)
    total = [round(i * 300.0 / (n - 1), 2) for i in range(n)]
    chart = {"dates": dates, "total": total, "options": total, "underlying_price": []}
    items, _, _stats = build_position_story(df, None, chart)
    interludes = [i for i in items if i["type"] == "interlude"]
    assert len(interludes) == 1
    assert "Time decay" in interludes[0]["text"]
    assert "in your favor" in interludes[0]["text"]
    # Interlude sits between the two chapters.
    kinds = [i["type"] for i in items]
    assert kinds == ["day", "interlude", "day"]


def test_no_interlude_for_small_moves_or_short_gaps():
    df = _trades([
        (date(2024, 1, 2), "equity_buy", "Equity", "X", 10, 10.0, -100.0),
        (date(2024, 1, 10), "equity_sell", "Equity", "X", 10, 11.0, 110.0),
    ])
    dates = [f"2024-01-{d:02d}" for d in range(2, 11)]
    chart = {"dates": dates, "total": [0, 1, 2, 3, 4, 5, 6, 7, 8], "options": [], "underlying_price": []}
    items, _, _stats = build_position_story(df, None, chart)
    assert all(i["type"] == "day" for i in items)


def test_multi_account_sentences_are_tagged():
    df = _trades([
        (date(2024, 1, 2), "equity_buy", "Equity", "X", 10, 10.0, -100.0, "Cameron 401k"),
        (date(2024, 1, 3), "equity_buy", "Equity", "X", 10, 10.0, -100.0, "Sara IRA"),
    ])
    items, _, _stats = build_position_story(df, None)
    text = _headlines(items)
    assert "— Cameron 401k." in text
    assert "— Sara IRA." in text


def test_colliding_account_labels_keep_tenant_state_separate():
    label = "Schwab Account"
    tenant_a = "snaptrade:tenant-a"
    tenant_b = "snaptrade:tenant-b"
    df = _trades([
        # Tenant A owns the shares and writes one genuinely covered call.
        (date(2024, 1, 2), "equity_buy", "Equity", "X", 100, 10.0, -1000.0, label, tenant_a),
        (date(2024, 1, 3), "option_sell_to_open", "Call", "X 240216C00011000", 1, 0.50, 50.0, label, tenant_a),
        # Closing A while opening B on the same day is not a roll: these are
        # different physical accounts despite their identical broker labels.
        (date(2024, 2, 1), "option_buy_to_close", "Call", "X 240216C00011000", 1, 0.80, -80.0, label, tenant_a),
        (date(2024, 2, 1), "option_sell_to_open", "Call", "X 240419C00013000", 1, 1.10, 110.0, label, tenant_b),
        # Tenant B still owns no shares, so another call there is also naked.
        (date(2024, 2, 2), "option_sell_to_open", "Call", "X 240419C00014000", 1, 0.60, 60.0, label, tenant_b),
    ])

    _, _, stats = build_position_story(df, None)

    assert stats["rolls"] == 0
    assert stats["covered_calls"] == 1


def test_break_interlude_for_long_flat_gap():
    df = _trades([
        (date(2024, 1, 2), "equity_buy", "Equity", "X", 100, 10.0, -1000.0),
        (date(2024, 2, 1), "equity_sell", "Equity", "X", 100, 12.0, 1200.0),
        # 8 months flat, then re-entry.
        (date(2024, 10, 1), "equity_buy", "Equity", "X", 50, 15.0, -750.0),
    ])
    items, _, _stats = build_position_story(df, None)
    interludes = [i for i in items if i["type"] == "interlude"]
    assert len(interludes) == 1
    assert "flat and out of the name" in interludes[0]["text"]
    assert "October 2024" in interludes[0]["text"]


def test_no_break_interlude_while_position_open():
    df = _trades([
        (date(2024, 1, 2), "equity_buy", "Equity", "X", 100, 10.0, -1000.0),
        # Long silence but still holding: not a "walked away" break.
        (date(2024, 10, 1), "equity_sell", "Equity", "X", 100, 15.0, 1500.0),
    ])
    items, _, _stats = build_position_story(df, None)
    assert all(i["type"] == "day" for i in items)


def test_split_is_narrated_and_fixes_share_units():
    df = _trades([
        # 100 pre-split shares, then a 3:1 split, then sell 300 post-split.
        (date(2024, 9, 2), "equity_buy", "Equity", "SCHD", 100, 82.38, -8238.0),
        (date(2024, 11, 1), "equity_sell", "Equity", "SCHD", 300, 28.33, 8499.0),
    ])
    splits = pd.DataFrame([
        {"symbol": "SCHD", "split_date": "2024-10-10", "split_ratio": 3.0},
    ])
    items, markers, stats = build_position_story(df, None, splits_df=splits)
    text = _headlines(items)
    assert "3-for-1 split: your 100 shares became 300" in text
    # Post-split sell of 300 closes the position — NOT an oversell.
    assert "more than you held" not in text
    assert "Sold the last 300 shares" in text
    # The split is its own chapter with a chart marker.
    split_days = [i for i in items if i["type"] == "day" and i["date_iso"] == "2024-10-10"]
    assert len(split_days) == 1 and split_days[0]["kind"] == "lifecycle"
    assert any(m["d"] == "2024-10-10" for m in markers)


def test_split_while_flat_is_silent():
    df = _trades([
        (date(2024, 9, 2), "equity_buy", "Equity", "X", 100, 10.0, -1000.0),
        (date(2024, 9, 20), "equity_sell", "Equity", "X", 100, 11.0, 1100.0),
        (date(2024, 12, 1), "equity_buy", "Equity", "X", 50, 4.0, -200.0),
    ])
    splits = pd.DataFrame([
        {"symbol": "X", "split_date": "2024-10-15", "split_ratio": 3.0},
    ])
    items, _, _stats = build_position_story(df, None, splits_df=splits)
    assert "split" not in _headlines(items)
    assert all(i["date_iso"] != "2024-10-15" for i in items if i["type"] == "day")


def test_drip_fills_aggregate_to_one_sentence():
    df = _trades([
        (date(2024, 5, 1), "equity_buy", "Equity", "SCHD", 100, 80.0, -8000.0),
        # Broker ships a 0-qty stub plus the real fractional fill.
        (date(2024, 6, 3), "dividend_reinvest", "Equity", "SCHD", 0, None, 0.0),
        (date(2024, 6, 3), "dividend_reinvest", "Equity", "SCHD", 0.8926, 82.0, -73.19),
    ])
    items, _, _stats = build_position_story(df, None)
    text = _headlines(items)
    assert text.count("Dividend reinvested") == 1
    assert "0.89 more share" in text


# ── The mirror ───────────────────────────────────────────────────────────

from app.position_story import compose_mirror  # noqa: E402


def test_stats_record_the_maneuvers_the_story_narrates():
    df = _trades([
        (date(2024, 6, 3), "option_sell_to_open", "Put", "F 240621P00012000", 1, 0.60, 60.0),
        (date(2024, 6, 21), "option_assigned", "Put", "F 240621P00012000", 1, None, 0.0),
        (date(2024, 6, 21), "equity_buy", "Equity", "F", 100, 12.0, -1200.0),
        (date(2024, 6, 24), "option_sell_to_open", "Call", "F 240719C00013000", 1, 0.45, 45.0),
        (date(2024, 7, 1), "option_buy_to_close", "Call", "F 240719C00013000", 1, 0.10, -10.0),
        (date(2024, 7, 2), "option_sell_to_open", "Call", "F 240802C00013500", 1, 0.50, 50.0),
        (date(2024, 8, 2), "option_expired", "Call", "F 240802C00013500", 1, None, 0.0),
    ])
    _, _, stats = build_position_story(df, None)
    assert stats["wheels_opened"] == 1
    assert stats["assignments"] == 1
    assert stats["puts_sold"] == 1
    assert stats["covered_calls"] == 2
    assert stats["premium_collected"] == 155.0
    assert stats["expired_kept"] == 1
    assert stats["expired_premium"] == 50.0
    assert stats["contract_wins"] == 2  # buyback win + expiry keep
    assert stats["chapters"] == 6
    assert stats["span_days"] == 60


def test_compose_mirror_reads_like_a_reflection():
    df = _trades([
        (date(2024, 1, 2), "equity_buy", "Equity", "RKLB", 1000, 19.71, -19710.0),
        (date(2024, 1, 3), "option_sell_to_open", "Call", "RKLB 240216C00022000", 10, 0.47, 473.0),
        (date(2024, 2, 16), "option_expired", "Call", "RKLB 240216C00022000", 10, None, 0.0),
        (date(2024, 3, 1), "option_sell_to_open", "Call", "RKLB 240419C00025000", 10, 0.60, 600.0),
        (date(2024, 4, 19), "option_expired", "Call", "RKLB 240419C00025000", 10, None, 0.0),
    ])
    _, _, stats = build_position_story(df, None)
    mirror = compose_mirror(stats, "RKLB", book_rank=2, book_size=94)
    text = " ".join(mirror)
    assert text.startswith("Your RKLB story:")
    assert "income engine" in text
    assert "$1,073 of option premium" in text
    assert "#2 of 94 symbols" in text
    # Cognitive-noise cap: shape + at most 2 behaviors + book rank.
    assert len(mirror) <= 4


def test_compose_mirror_empty_for_empty_story():
    _, _, stats = build_position_story(pd.DataFrame(), None)
    assert compose_mirror(stats, "X") == []
    assert compose_mirror(None, "X") == []


def test_roll_open_leg_counts_as_premium_collected():
    # Identity/eras consistency: the /story eras sum STO credits straight
    # from fills, so the fingerprint must count a roll's open leg too.
    df = _trades([
        (date(2024, 1, 2), "equity_buy", "Equity", "F", 100, 12.0, -1200.0),
        (date(2024, 1, 3), "option_sell_to_open", "Call", "F 240216C00013000", 1, 0.50, 50.0),
        (date(2024, 2, 1), "option_buy_to_close", "Call", "F 240216C00013000", 1, 0.80, -80.0),
        (date(2024, 2, 1), "option_sell_to_open", "Call", "F 240419C00014000", 1, 1.10, 110.0),
    ])
    _, _, stats = build_position_story(df, None)
    assert stats["rolls"] == 1
    assert stats["premium_collected"] == 160.0  # 50 STO + 110 roll open
