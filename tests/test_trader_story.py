"""The trader novel (/story, app/trader_story.py): book building,
style classification, and novel composition from synthetic fills."""

from datetime import date

import pandas as pd

from app.trader_story import (
    build_book,
    classify_style,
    compose_novel,
    _build_eras,
    _build_scoreboard,
    _build_standouts,
    _busiest_day,
    _open_held_symbol_count,
)


def _trades(rows):
    """rows: (date, symbol, action, instrument_type, trade_symbol, qty, price, amount)"""
    return pd.DataFrame(
        [
            {
                "account": "Test Account",
                "tenant_id": "snaptrade:t1",
                "symbol": sym,
                "trade_date": pd.Timestamp(d),
                "action": action,
                "instrument_type": itype,
                "trade_symbol": tsym,
                "quantity": qty,
                "price": price,
                "amount": amount,
            }
            for d, sym, action, itype, tsym, qty, price, amount in rows
        ]
    )


def _summary(rows):
    """rows: (symbol, total_return, has_open_leg)"""
    return pd.DataFrame(
        [
            {"tenant_id": "snaptrade:t1", "symbol": s, "total_return": r,
             "has_open_leg": o}
            for s, r, o in rows
        ]
    )


_BOOK_TRADES = _trades([
    # AAA — income: covered calls against shares, expiry kept.
    (date(2024, 1, 2), "AAA", "equity_buy", "Equity", "AAA", 100, 50.0, -5000.0),
    (date(2024, 1, 3), "AAA", "option_sell_to_open", "Call", "AAA 240216C00055000", 1, 1.0, 100.0),
    (date(2024, 2, 16), "AAA", "option_expired", "Call", "AAA 240216C00055000", 1, None, 0.0),
    # BBB — directional: long call bought and sold at a profit.
    (date(2024, 3, 4), "BBB", "option_buy_to_open", "Call", "BBB 240621C00100000", 1, 5.0, -500.0),
    (date(2024, 5, 1), "BBB", "option_sell_to_close", "Call", "BBB 240621C00100000", 1, 8.0, 800.0),
    # CCC — pure stock, built with adds.
    (date(2025, 1, 6), "CCC", "equity_buy", "Equity", "CCC", 10, 20.0, -200.0),
    (date(2025, 2, 3), "CCC", "equity_buy", "Equity", "CCC", 10, 21.0, -210.0),
    (date(2025, 3, 3), "CCC", "equity_buy", "Equity", "CCC", 10, 22.0, -220.0),
])

_BOOK_SUMMARY = _summary([
    ("AAA", 600.0, 1),
    ("BBB", 300.0, 0),
    ("CCC", -50.0, 1),
])


def _book():
    return build_book(_BOOK_TRADES, None, None, _BOOK_SUMMARY)


def test_build_book_one_entry_per_symbol_with_fingerprint_and_pnl():
    book = _book()
    assert set(book) == {"AAA", "BBB", "CCC"}
    assert book["AAA"]["stats"]["covered_calls"] == 1
    assert book["AAA"]["stats"]["expired_kept"] == 1
    assert book["AAA"]["pnl"] == 600.0
    assert book["AAA"]["open"] is True
    assert book["BBB"]["stats"]["long_opens"] == 1
    assert book["BBB"]["stats"]["contract_wins"] == 1
    assert book["BBB"]["open"] is False
    assert book["CCC"]["stats"]["adds"] == 2  # first buy is the open


def test_classify_style():
    book = _book()
    assert classify_style(book["AAA"]["stats"]) == "income"
    assert classify_style(book["BBB"]["stats"]) == "directional"
    assert classify_style(book["CCC"]["stats"]) == "stock"


def test_scoreboard_groups_by_style_with_green_counts():
    rows = {r["style"]: r for r in _build_scoreboard(_book())}
    assert rows["income"]["symbols"] == 1
    assert rows["income"]["green"] == 1
    assert rows["income"]["best_symbol"] == "AAA"
    assert rows["stock"]["green"] == 0
    assert rows["stock"]["pnl"] == -50.0


def test_standouts_best_seller_and_toughest_link_distinct_symbols():
    cards = _build_standouts(_book())
    by_label = {c["label"]: c for c in cards}
    assert by_label["Top performer"]["symbol"] == "AAA"
    assert by_label["Top performer"]["pnl_text"].startswith("+")
    assert by_label["Largest loss"]["symbol"] == "CCC"
    # No symbol appears on two cards.
    syms = [c["symbol"] for c in cards]
    assert len(syms) == len(set(syms))


def test_eras_one_row_per_year_with_new_names_and_premium():
    eras = _build_eras(_BOOK_TRADES)
    assert [e["year"] for e in eras] == [2024, 2025]
    assert eras[0]["title"] == "First year of activity"
    assert "2 new symbols" in eras[0]["line"]
    assert "$100 premium collected" in eras[0]["line"]
    assert "$500 placed on long options" in eras[0]["line"]
    assert "1 new symbol" in eras[1]["line"]


def test_eras_and_busiest_day_ignore_drip_reinvestments():
    """Monthly JEPI DRIPs must not inflate fill counts or invent a busiest day."""
    drips = _trades([
        (date(2024, 6, 3), "JEPI", "dividend_reinvest", "Equity", "JEPI", 0.12, 56.0, -6.72)
        for _ in range(6)
    ])
    mixed = pd.concat([_BOOK_TRADES, drips], ignore_index=True)
    eras = _build_eras(mixed)
    assert [e["year"] for e in eras] == [2024, 2025]
    assert "6 fills" not in eras[0]["line"]
    assert _busiest_day(drips) is None
    assert _busiest_day(mixed) is None  # still < 5 placed fills on any day


def test_busiest_day_requires_a_real_cluster():
    assert _busiest_day(_BOOK_TRADES) is None  # max 1 fill/day here

    burst = _trades([
        (date(2024, 6, 3), s, "equity_buy", "Equity", s, 1, 1.0, -1.0)
        for s in ("A", "B", "C", "D", "E", "F")
    ])
    fact = _busiest_day(burst)
    assert fact["value"] == "Jun 3, 2024"
    assert fact["detail"] == "6 fills across 6 symbols"


def test_compose_novel_shape():
    novel = compose_novel(_book(), _BOOK_TRADES)
    assert novel["hero_counts"]["stories"] == 3
    assert novel["hero_counts"]["open_stories"] == 2
    assert novel["hero_counts"]["since"] == "January 2024"
    # Takeaway-first profile: one identity headline + fact rows, no prose
    # list, no chip strip repeating the same numbers.
    profile = novel["profile"]
    assert profile["headline"]
    labels = [f["label"] for f in profile["facts"]]
    assert "Income book" in labels
    for f in profile["facts"]:
        assert set(f) == {"label", "value", "tone", "detail"}
    assert "identity" not in novel and "chips" not in novel
    assert len(novel["eras"]) == 2
    assert novel["scoreboard"]


def test_compose_novel_empty_book():
    assert compose_novel({}, pd.DataFrame()) is None


def test_open_held_symbol_count_is_unique_open_symbols():
    df = pd.DataFrame([
        {"has_open_leg": 1, "symbol": "AAPL"},
        {"has_open_leg": 1, "symbol": "aapl"},
        {"has_open_leg": 1, "symbol": "MSFT"},
        {"has_open_leg": 0, "symbol": "TSLA"},
    ])
    assert _open_held_symbol_count(df) == 2
    assert _open_held_symbol_count(pd.DataFrame()) == 0
    assert _open_held_symbol_count(None) == 0
    status_df = pd.DataFrame([
        {"status": "Open", "symbol": "NVDA"},
        {"status": "Closed", "symbol": "AMD"},
    ])
    assert _open_held_symbol_count(status_df) == 1
