"""Spellings of one option must stay one contract.

CHECK 12 failed after compact OCC roots became the underlying: the
estimated-fee matcher keys that root, so an open on one spelling was
dropped as the shadow of a statement open on another. The close stayed
behind with no cost basis. Classification and the coach both omit
that fragment. A genuine pre-window close, with no open on any
spelling, stays out of both.

Synthetic symbols only. The warehouse guard is
dbt/tests/no_close_only_spelling_of_an_opened_contract.sql.
"""
from pathlib import Path

from app.option_formatting import canonical_occ_trade_symbol

ROOT = Path(__file__).resolve().parents[1]

COMPACT = "ABCD240119C00150000"
PADDED = "ABCD  240119C00150000"
LONG_FORM = "ABCD 01/19/2024 150.00 C"
CANONICAL = "ABCD240119C00150000"


def _is_opened_before_history(fills):
    """Same flag as direction_lookup in int_option_contracts."""
    opened = sum(
        qty for action, qty in fills
        if action in ("option_sell_to_open", "option_buy_to_open")
    )
    closed = sum(
        qty for action, qty in fills
        if action in (
            "option_buy_to_close", "option_sell_to_close",
            "option_assigned", "option_exercised", "option_expired",
        )
    )
    return opened <= 1e-9 and closed > 1e-9


def test_macro_runs_before_the_estimated_fee_match():
    history = (ROOT / "dbt/models/staging/stg_history.sql").read_text()
    current = (ROOT / "dbt/models/staging/stg_current.sql").read_text()
    marks = (ROOT / "dbt/models/intermediate/int_option_marks_daily.sql").read_text()
    macro = (ROOT / "dbt/macros/canonical_option_trade_symbol.sql").read_text()
    assert "canonical_option_trade_symbol(" in history
    assert "canonical_option_trade_symbol(" in current
    # The SCD2 snapshot retains the pre-canonicalization spellings. Its
    # reader must migrate those historical ids or they no longer join to
    # the canonical contracts.
    assert marks.count("canonical_option_trade_symbol(") >= 2
    # The CTE, not the comment that names it. The rewrite has to land
    # before the shadow match so both spellings share one contract id.
    assert history.find("canonical_option_trade_symbol(") < history.find("est_fee_matched as (")
    assert "format_date('%y%m%d'" in macro
    assert "round(" in macro and "* 1000" in macro


def test_spellings_share_one_trade_symbol():
    assert canonical_occ_trade_symbol(COMPACT) == CANONICAL
    assert canonical_occ_trade_symbol(PADDED) == CANONICAL
    assert canonical_occ_trade_symbol(LONG_FORM) == CANONICAL
    assert canonical_occ_trade_symbol("ABCD 01/19/2024 150.50 C") == "ABCD240119C00150500"
    assert canonical_occ_trade_symbol("AB.CD 01/16/2026 450.00 C") == "AB.CD260116C00450000"


def test_equities_and_other_strikes_stay_distinct():
    assert canonical_occ_trade_symbol("ABCD") == "ABCD"
    assert canonical_occ_trade_symbol("AB.CD") == "AB.CD"
    assert (
        canonical_occ_trade_symbol("ABCD 01/19/2024 155.00 C")
        != canonical_occ_trade_symbol(LONG_FORM)
    )


def test_cross_spelling_round_trip_is_not_a_close_only_fragment():
    """Estimated open on compact OCC, statement open on long form, close
    on the compact spelling. Dropping the estimate before unifying leaves
    a close with no cost basis. Unifying first keeps the close with the
    statement open."""
    rows = [
        {"symbol": COMPACT, "action": "option_sell_to_open", "est": True},
        {"symbol": LONG_FORM, "action": "option_sell_to_open", "est": False},
        {"symbol": COMPACT, "action": "option_buy_to_close", "est": False},
    ]

    # Regression shape: the estimate is removed while spellings are still
    # distinct. The compact symbol is only a close.
    by_raw = {}
    for row in rows:
        if row["est"]:
            continue
        by_raw.setdefault(row["symbol"], []).append((row["action"], 1))
    assert _is_opened_before_history(by_raw[COMPACT])
    assert not _is_opened_before_history(by_raw[LONG_FORM])

    # Fix shape: one trade_symbol, then the duplicate estimate drops.
    statement_opens = {
        canonical_occ_trade_symbol(row["symbol"])
        for row in rows
        if not row["est"] and row["action"] == "option_sell_to_open"
    }
    kept = [
        row for row in rows
        if not (
            row["est"]
            and canonical_occ_trade_symbol(row["symbol"]) in statement_opens
        )
    ]
    grouped = {}
    for row in kept:
        grouped.setdefault(canonical_occ_trade_symbol(row["symbol"]), []).append(
            (row["action"], 1)
        )
    assert list(grouped) == [CANONICAL]
    assert not _is_opened_before_history(grouped[CANONICAL])


def _collapse_blank_price(rows):
    """One row per check-1 grain after the symbol rewrite.

    Mirrors ``blank_price_ranked`` in stg_history: a blank price with a
    real symbol collapses; a blank symbol never does. Priced rows are
    not this path.
    """
    kept = []
    seen = set()
    for row in rows:
        symbol = canonical_occ_trade_symbol(row["symbol"])
        if row["price"] is not None or not symbol:
            kept.append(row)
            continue
        grain = (
            row["tenant"],
            row["day"],
            row["action"],
            symbol,
            row["qty"],
            row["amount"],
        )
        if grain in seen:
            continue
        seen.add(grain)
        kept.append({**row, "symbol": symbol})
    return kept


def test_blank_price_spellings_of_one_settlement_collapse():
    """Two spellings of an expired contract, and of an exercised
    contract, are one settlement each. Two blank-symbol events on the
    same day stay two events. A different strike stays its own row."""
    rows = [
        {
            "tenant": "t", "day": "2024-01-19", "action": "option_expired",
            "symbol": "ABCD  240119C00150000", "qty": 2, "price": None, "amount": 0,
        },
        {
            "tenant": "t", "day": "2024-01-19", "action": "option_expired",
            "symbol": "ABCD 01/19/2024 150.00 C", "qty": 2, "price": None, "amount": 0,
        },
        {
            "tenant": "t", "day": "2024-02-16", "action": "option_exercised",
            "symbol": "WXYZ240216P00045000", "qty": 1, "price": None, "amount": 0,
        },
        {
            "tenant": "t", "day": "2024-02-16", "action": "option_exercised",
            "symbol": "WXYZ 02/16/2024 45.00 P", "qty": 1, "price": None, "amount": 0,
        },
        {
            "tenant": "t", "day": "2024-03-15", "action": "option_expired",
            "symbol": "", "qty": 1, "price": None, "amount": 0,
        },
        {
            "tenant": "t", "day": "2024-03-15", "action": "option_expired",
            "symbol": "", "qty": 3, "price": None, "amount": 0,
        },
        {
            "tenant": "t", "day": "2024-01-19", "action": "option_expired",
            "symbol": "ABCD 01/19/2024 155.00 C", "qty": 2, "price": None, "amount": 0,
        },
    ]
    kept = _collapse_blank_price(rows)
    symbols = [canonical_occ_trade_symbol(row["symbol"]) for row in kept]
    assert symbols.count(CANONICAL) == 1
    assert symbols.count("WXYZ240216P00045000") == 1
    assert symbols.count("ABCD240119C00155000") == 1
    assert sum(1 for row in kept if row["symbol"] == "") == 2
    assert len(kept) == 5


def test_blank_price_rank_is_in_stg_history_and_blank_symbols_stay():
    history = (ROOT / "dbt/models/staging/stg_history.sql").read_text()
    assert "blank_price_ranked as (" in history
    assert "(trade_symbol is null or price is null)" not in history
    ranked = history.split("blank_price_ranked as (", 1)[1].split("history_rows as (", 1)[0]
    assert "d.trade_symbol" in ranked
    assert "cast(d.quantity as string)" in ranked
    assert "cast(d.amount as string)" in ranked
    assert "d.price is null" in ranked
    passthrough = history.split("history_rows as (", 1)[1]
    # The last branch is the only one that still keeps a row with no symbol.
    last = passthrough.rsplit("union all", 1)[-1]
    assert "trade_symbol is null" in last
    assert "price is null" not in last


def test_demo_book_keys_on_the_compact_contract_id():
    macro = (ROOT / "dbt/macros/canonical_option_trade_symbol.sql").read_text()
    assert "macro canonical_option_symbol_text" in macro
    for rel in (
        "dbt/models/staging/demo/stg_demo_current.sql",
        "dbt/models/staging/demo/stg_demo_history.sql",
        "dbt/tests/demo_temp_current_keeps_mirror.sql",
    ):
        assert "canonical_option_symbol_text(" in (ROOT / rel).read_text()
    # Long form and padded OCC are one id, so a demo sum on that id
    # keeps both lots. A different strike does not.
    long_form = "ABCD 01/19/2024 150.00 C"
    padded = "ABCD  240119C00150000"
    other = "ABCD 01/19/2024 155.00 C"
    assert canonical_occ_trade_symbol(long_form) == canonical_occ_trade_symbol(padded)
    assert canonical_occ_trade_symbol(other) != canonical_occ_trade_symbol(long_form)
