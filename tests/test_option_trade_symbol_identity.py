"""Spellings of one option must stay one contract.

CHECK 12 failed after compact OCC roots became the underlying: the
estimated-fee matcher keys that root, so an open on one spelling was
dropped as the shadow of a statement open on another. The close stayed
behind with no cost basis. Classification omits that fragment. The
coach still counts it as Other Option.

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
    macro = (ROOT / "dbt/macros/canonical_option_trade_symbol.sql").read_text()
    assert "canonical_option_trade_symbol(" in history
    assert "canonical_option_trade_symbol(" in current
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
