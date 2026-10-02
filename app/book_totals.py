"""One all-time book total for Positions and Strategies.

Both heroes read the same identity off ``positions_summary``:

    total = realized + unrealized + dividends

``total_return`` already includes attributed dividends. ``realized_pnl``
and ``unrealized_pnl`` are trade-only, so a hero that sums those two and
then shows ``total_return`` cannot add up. The strategy mart also drops
rows whose strategy is blank; Positions keeps them. The shared total is
the sum of the positions frame (blank strategy included).

Whole-dollar heroes round each component, then park a $1 remainder on
the largest absolute component so the figures on the page still add up.
"""
from __future__ import annotations

import pandas as pd


def _num(value) -> float:
    try:
        if value is None or pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _col_sum(df: pd.DataFrame, *names: str) -> float:
    for name in names:
        if name in df.columns:
            return float(pd.to_numeric(df[name], errors="coerce").fillna(0).sum())
    return 0.0


def reconcile_book(realized, unrealized, dividends) -> dict:
    """Whole-dollar realized, unrealized, dividends, and total.

    ``total`` is the rounded sum of the raw components. Each displayed
    component is rounded on its own, then the difference (usually $0 or
    $1) is applied to the component with the largest absolute value.
    """
    raw = {
        "realized": _num(realized),
        "unrealized": _num(unrealized),
        "dividends": _num(dividends),
    }
    total_raw = raw["realized"] + raw["unrealized"] + raw["dividends"]
    shown = {key: int(round(val)) for key, val in raw.items()}
    total = int(round(total_raw))
    drift = total - sum(shown.values())
    if drift:
        key = max(shown, key=lambda name: (abs(shown[name]), name))
        shown[key] += drift
    return {
        "realized": shown["realized"],
        "unrealized": shown["unrealized"],
        "dividends": shown["dividends"],
        "total": total,
        "raw_realized": raw["realized"],
        "raw_unrealized": raw["unrealized"],
        "raw_dividends": raw["dividends"],
        "raw_total": total_raw,
    }


def book_totals_from_frame(df) -> dict:
    """Shared hero from a ``positions_summary``-shaped frame.

    Blank strategy rows stay in the sum. ``dividend_income`` is accepted
    as an alias of ``total_dividend_income`` (the strategy mart's name).
    """
    if df is None or getattr(df, "empty", True):
        return reconcile_book(0, 0, 0)
    return reconcile_book(
        _col_sum(df, "realized_pnl"),
        _col_sum(df, "unrealized_pnl"),
        _col_sum(df, "total_dividend_income", "dividend_income"),
    )


def hero_book(positions_df=None, strategy_row=None) -> dict:
    """All-accounts book, or one strategy row, on the same identity.

    Positions always passes the filtered positions frame. Strategies
    passes that same frame for the all-strategies hero, and the focused
    strategy's own realized / unrealized / dividends when one card is open.
    """
    if strategy_row is not None:
        return reconcile_book(
            strategy_row.get("realized_pnl"),
            strategy_row.get("unrealized_pnl"),
            strategy_row.get("dividend_income", strategy_row.get("total_dividend_income")),
        )
    return book_totals_from_frame(positions_df)
