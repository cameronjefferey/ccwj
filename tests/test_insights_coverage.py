"""Insights exit-timing coverage uses the marks window, not lifetime closed."""
import pandas as pd

from app.insights import _exit_coverage, _strategy_exit_rows


def _row(**kwargs):
    base = {
        "account": "Schwab Account",
        "strategy": "Covered Call",
        "total_closed": 74,
        "eligible_closed": 12,
        "reliable_contracts": 6,
        "avg_giveback_pct": 0.0,
        "avg_days_held_past_peak": 0.5,
        "total_pnl_given_back": 5440.62,
        "avg_pct_premium_captured": 80.0,
    }
    base.update(kwargs)
    return base


def test_coverage_uses_eligible_not_lifetime():
    df = pd.DataFrame([
        _row(reliable_contracts=6, eligible_closed=12, total_closed=74),
        _row(strategy="Covered Call", reliable_contracts=4,
             eligible_closed=10, total_closed=101),
        _row(strategy="Long Call", reliable_contracts=2,
             eligible_closed=8, total_closed=232),
    ])
    reliable, eligible, pct = _exit_coverage(df)
    assert reliable == 12
    assert eligible == 30
    assert pct == 40
    # Lifetime 407 would have been 3% — the bug on the Insights card.


def test_coverage_falls_back_to_total_closed_without_eligible_col():
    df = pd.DataFrame([
        {"reliable_contracts": 12, "total_closed": 407},
    ])
    reliable, eligible, pct = _exit_coverage(df)
    assert (reliable, eligible, pct) == (12, 407, 3)


def test_coverage_empty():
    assert _exit_coverage(pd.DataFrame()) == (0, 0, 0)


def test_strategy_rows_collapse_duplicate_labels_and_use_window_denom():
    signals = pd.DataFrame([
        _row(reliable_contracts=6, avg_giveback_pct=0.0,
             avg_days_held_past_peak=0.5, total_pnl_given_back=5441),
        _row(reliable_contracts=4, avg_giveback_pct=95.0,
             avg_days_held_past_peak=3.0, total_pnl_given_back=4379),
        _row(strategy="Long Call", reliable_contracts=1,
             avg_giveback_pct=0.0, total_pnl_given_back=13767),
    ])
    coverage = pd.DataFrame([
        {"strategy": "Covered Call", "eligible_closed": 12, "reliable_contracts": 6},
        {"strategy": "Covered Call", "eligible_closed": 10, "reliable_contracts": 4},
        {"strategy": "Long Call", "eligible_closed": 8, "reliable_contracts": 1},
    ])
    rows = _strategy_exit_rows(signals, coverage)
    assert [r["strategy"] for r in rows] == ["Covered Call"]
    cc = rows[0]
    assert cc["trades"] == 10
    assert cc["total_closed"] == 22
    assert cc["pnl_given_back"] == 9820
    # Weighted giveback: (0*6 + 95*4) / 10 = 38
    assert round(cc["giveback_pct"]) == 38


def test_strategy_rows_skip_sparse_without_coverage_frame():
    signals = pd.DataFrame([
        _row(reliable_contracts=2, total_closed=76),
    ])
    assert _strategy_exit_rows(signals) == []
