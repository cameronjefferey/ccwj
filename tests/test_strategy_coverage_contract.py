"""Pin covered-call coverage to the 3-day buy-write window (Sep 2026 / CCJ).

Current holdings do not flip the label. A sold call is Covered Call only
when the fill ledger shows enough shares at write or within 3 days
(synthetic opening-balance fills count at write — they are pre-window
shares, dated "day before first fill" as a sort key).
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLASSIFICATION_SQL = (
    ROOT / "dbt/models/intermediate/int_strategy_classification.sql"
).read_text()


def _coverage_at_write(*, open_date, fills, lookahead_days=3):
    """Mirrors coverage_at_write.coverage_qty.

    fills: list of (trade_date, signed_qty, is_synthetic_opening)
    Dates are datetime.date or comparable.
    """
    from datetime import timedelta

    cutoff = open_date + timedelta(days=lookahead_days)
    at_open = 0.0
    at_lookahead = 0.0
    for trade_date, qty, synthetic in fills:
        if synthetic or trade_date <= open_date:
            at_open += qty
        if synthetic or trade_date <= cutoff:
            at_lookahead += qty
    return max(at_open, at_lookahead)


def _label_sold_call(coverage_qty, required_shares=100.0, shares_per_contract=100.0):
    if coverage_qty + 1e-6 >= required_shares:
        return "Covered Call"
    if coverage_qty + 1e-6 >= shares_per_contract:
        return "Partially Covered Call"
    return "Naked Call"


def test_buy_write_within_three_days_is_covered():
    from datetime import date

    write = date(2026, 6, 1)
    fills = [(date(2026, 6, 3), 100.0, False)]
    assert _label_sold_call(_coverage_at_write(open_date=write, fills=fills)) == (
        "Covered Call"
    )


def test_stock_bought_after_three_days_stays_naked():
    from datetime import date

    write = date(2026, 6, 1)
    fills = [(date(2026, 6, 10), 100.0, False)]
    assert _label_sold_call(_coverage_at_write(open_date=write, fills=fills)) == (
        "Naked Call"
    )


def test_current_holdings_do_not_cover_a_naked_write():
    from datetime import date

    write = date(2026, 6, 1)
    # 100 shares held now, but the only fill is after the 3-day window.
    fills = [(date(2026, 8, 1), 100.0, False)]
    assert _label_sold_call(_coverage_at_write(open_date=write, fills=fills)) == (
        "Naked Call"
    )


def test_shares_sold_before_write_are_naked():
    from datetime import date

    write = date(2026, 6, 1)
    fills = [
        (date(2026, 1, 1), 100.0, False),
        (date(2026, 5, 1), -100.0, False),
    ]
    assert _label_sold_call(_coverage_at_write(open_date=write, fills=fills)) == (
        "Naked Call"
    )


def test_synthetic_opening_counts_even_when_dated_after_write():
    from datetime import date

    write = date(2026, 6, 1)
    # Opening balance dated day-before-first-fill (Aug), but it stands
    # for pre-window shares that were held at the June write.
    fills = [(date(2026, 8, 14), 100.0, True)]
    assert _label_sold_call(_coverage_at_write(open_date=write, fills=fills)) == (
        "Covered Call"
    )


def test_partial_coverage_at_write():
    from datetime import date

    write = date(2026, 6, 1)
    fills = [(date(2026, 6, 1), 100.0, False)]
    qty = _coverage_at_write(open_date=write, fills=fills)
    assert _label_sold_call(qty, required_shares=200.0) == "Partially Covered Call"


def test_classification_sql_is_write_window_only():
    assert "coverage_at_write as (" in CLASSIFICATION_SQL
    assert "left join coverage_at_write cov" in CLASSIFICATION_SQL
    assert "option_coverage as (" not in CLASSIFICATION_SQL
    assert "ledger_equity_qty as (" not in CLASSIFICATION_SQL
    assert "snapshot_equity_qty as (" not in CLASSIFICATION_SQL
    assert "write_covered_calls_on_session as (" in CLASSIFICATION_SQL
    assert "num_write_covered_sold_calls" in CLASSIFICATION_SQL
    write_block = CLASSIFICATION_SQL.split("coverage_at_write as (")[1].split(
        "diagonal_cover as ("
    )[0]
    assert "left join {{ ref('int_equity_fills') }}" in write_block
    assert "is_synthetic_opening" in write_block
    assert "\n    join {{ ref('int_equity_fills') }}" not in write_block


def test_dbt_invariant_is_write_date_only():
    path = ROOT / "dbt/tests/covered_call_has_coverage_at_write.sql"
    sql = path.read_text()
    assert "is_synthetic_opening" in sql
    assert "shares held now" not in sql.lower()
    assert not (ROOT / "dbt/tests/no_open_naked_call_when_shares_cover.sql").exists()
