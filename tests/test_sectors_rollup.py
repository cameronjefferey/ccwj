"""Sector header counts must match the cards."""

import pandas as pd

from app.sectors_page import _sector_rollups


def _row(sector, subsector, symbol, account="Ira", pnl=10.0):
    return {
        "account": account,
        "symbol": symbol,
        "strategy": "Buy and Hold",
        "total_pnl": pnl,
        "realized_pnl": pnl,
        "unrealized_pnl": 0.0,
        "total_premium_received": 0.0,
        "total_dividend_income": 0.0,
        "total_return": pnl,
        "num_individual_trades": 1,
        "num_winners": 1 if pnl > 0 else 0,
        "num_losers": 0 if pnl > 0 else 1,
        "sector": sector,
        "subsector": subsector,
    }


def test_subsector_kpi_matches_the_sum_of_the_cards():
    """Audit: header said 55 subsectors while the cards summed to 56.

    ``nunique()`` on the subsector name counts "Unknown" once even when it
    sits under two sectors. The header has to count the pairs the cards list.
    """
    df = pd.DataFrame([
        _row("Technology", "Semiconductors", "NVDA"),
        _row("Technology", "Unknown", "XYZ"),
        _row("Unknown", "Unknown", "ABC", pnl=-5),
        _row("Energy", "Oil & Gas", "XOM", account="Sara"),
        _row("Energy", "Oil & Gas", "CVX"),
    ])
    roll = _sector_rollups(df)
    card_sum = sum(int(s["num_subsectors"]) for s in roll["sector_rows"])
    distinct_names = int(df["subsector"].nunique())

    assert distinct_names == 3  # Semiconductors, Unknown, Oil & Gas
    assert card_sum == 4  # Unknown is listed under both Technology and Unknown
    assert roll["kpis"]["num_subsectors"] == card_sum
    assert roll["kpis"]["num_subsectors"] == len(roll["subsector_rows"])
    assert roll["kpis"]["num_subsectors"] != distinct_names


def test_strategy_fit_matrix_scroll_and_metric_contract():
    """The fit matrix must scroll in-view and retarget totals with the metric."""
    from pathlib import Path

    html = Path("app/templates/strategy_fit.html").read_text()
    assert "fit-table-wrap" in html
    assert "overflow: auto" in html
    assert "position: sticky; left: 0" in html
    assert "position: sticky; right: 0" in html
    assert 'id="fitScrollHint"' in html
    assert "function renderTotal" in html
    assert "td.total-cell[data-total-pnl]" in html
    assert "data-num-trades" in html
    assert "data-win-rate" in html
    strategies = Path("app/templates/strategies.html").read_text()
    assert "population_label" in strategies
    assert "focus_strategy.num_symbols }} symbol" not in strategies
