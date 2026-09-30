"""The public demo's AI analysis is composed from the current book.

The old path inserted one essay on first boot (covered calls on AAPL /
NVDA / META, dated the insert day) and then never replaced it. Demo
writes are blocked, so production kept serving April 2026 copy about a
book the demo account does not trade.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from app.demo_analysis import compose_demo_analysis


def _book():
    return pd.DataFrame([
        {
            "symbol": "MU",
            "strategy": "Call Spread",
            "total_return": 1200.0,
            "realized_pnl": 800.0,
            "unrealized_pnl": 400.0,
            "total_dividend_income": 0.0,
            "num_individual_trades": 4,
            "num_winners": 1,
            "num_losers": 0,
            "first_trade_date": "2026-08-08",
            "last_trade_date": "2026-09-12",
        },
        {
            "symbol": "NVDA",
            "strategy": "Strangle",
            "total_return": -300.0,
            "realized_pnl": -300.0,
            "unrealized_pnl": 0.0,
            "total_dividend_income": 0.0,
            "num_individual_trades": 2,
            "num_winners": 0,
            "num_losers": 1,
            "first_trade_date": "2026-08-20",
            "last_trade_date": "2026-09-01",
        },
        {
            "symbol": "SPY",
            "strategy": "Buy and Hold",
            "total_return": 50.0,
            "realized_pnl": 0.0,
            "unrealized_pnl": 50.0,
            "total_dividend_income": 1.25,
            "num_individual_trades": 1,
            "num_winners": 0,
            "num_losers": 0,
            "first_trade_date": "2026-08-08",
            "last_trade_date": "2026-08-08",
        },
    ])


def test_compose_demo_analysis_names_the_actual_book():
    when = datetime(2026, 9, 28, 12, 0, 0)
    summary, body, generated = compose_demo_analysis(
        _book(),
        coaching_data={
            "recent_exits": [{
                "symbol": "MU",
                "strategy": "Call Spread",
                "actual_pnl": 800,
                "close_date": "2026-09-12",
            }],
        },
        generated_at=when,
    )
    assert generated == when
    text = summary + "\n" + body
    assert "Call Spread" in text
    assert "Strangle" in text
    assert "Buy and Hold" in text
    assert "MU" in text
    assert "2026-08-08" in text
    assert "What This Demo Shows" not in text
    assert "Mirror Score" not in text
    assert "Upload your own data" not in text
    assert "PMCC" not in text
    assert "Poor Man's Covered Call" not in text
    # NVDA is in this fixture, so naming it is correct. AAPL / META are not.
    assert "AAPL" not in text
    assert "META" not in text
    assert "MU (Call Spread) on 2026-09-12" in text


def test_empty_demo_book_is_not_the_april_essay():
    summary, body, _when = compose_demo_analysis(pd.DataFrame())
    text = summary + "\n" + body
    assert "no positions" in summary.lower()
    assert "What This Demo Shows" not in text
    assert "covered call" not in text.lower()
    assert "AAPL" not in text


def test_demo_seed_no_longer_embeds_the_stale_essay():
    models = Path(__file__).resolve().parents[1].joinpath("app/models.py").read_text()
    assert "What This Demo Shows" not in models
    assert "Upload your own data" not in models


def test_surface_css_for_the_demo_bugs():
    root = Path(__file__).resolve().parents[1]
    insights = (root / "app/templates/insights.html").read_text()
    positions = (root / "app/templates/positions.html").read_text()
    fit = (root / "app/templates/strategy_fit.html").read_text()
    assert "rgba(255,255,255,.92)" not in insights
    assert "var(--ht-surface" in insights
    assert "W / L" not in positions
    assert ">W/L<" in positions
    assert "body:has(#positionsTable) .ht-page { max-width: none; }" in positions
    # Laptop widths fit both Positions tables inside the card. A max-content
    # table with its scrollbar at the bottom of the 75vh sticky box left
    # Premium / W/L clipped at 1280x800.
    assert "@media (min-width: 768px) and (max-width: 1366px)" in positions
    assert positions.count("table-layout: fixed") >= 2
    assert "fit-col-short" in fit
    assert "fit-corner-short" in fit
    assert "max-width: 480px" in fit
