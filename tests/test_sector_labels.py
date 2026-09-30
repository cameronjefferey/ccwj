"""Unmapped sectors display as Unclassified and sort last."""

import pandas as pd

from app.sector_labels import (
    UNCLASSIFIED,
    apply_sector_labels,
    canonical_sector_param,
    classify_symbol,
    is_unclassified,
    sort_unclassified_last,
)


def test_etf_and_crypto_fill_a_missing_sector_without_overriding_a_real_one():
    assert classify_symbol("JEPI", "Unknown", "Unknown") == ("Equity Income", "Equity Income")
    assert classify_symbol("QQQ", None, None) == ("Technology", "Nasdaq-100")
    assert classify_symbol("XLK", "", "")[0] == "Technology"
    assert classify_symbol("BTC", "Unknown", "Unknown") == ("Crypto", "Crypto")
    # A warehouse sector wins even when the ticker is in the fallback map.
    assert classify_symbol("SPY", "Financial Services", "Banks") == (
        "Financial Services",
        "Banks",
    )
    assert classify_symbol("AAPL", "Technology", "Unknown") == ("Technology", UNCLASSIFIED)
    assert classify_symbol("DWAC", "Unknown", None) == (UNCLASSIFIED, UNCLASSIFIED)


def test_apply_sector_labels_and_sort_last():
    df = pd.DataFrame([
        {"symbol": "JEPI", "sector": "Unknown", "subsector": "Unknown"},
        {"symbol": "AAPL", "sector": "Technology", "subsector": "Consumer Electronics"},
        {"symbol": "ZZZ", "sector": None, "subsector": None},
    ])
    out = apply_sector_labels(df)
    assert out.loc[0, "sector"] == "Equity Income"
    assert out.loc[1, "sector"] == "Technology"
    assert out.loc[2, "sector"] == UNCLASSIFIED
    labels = sort_unclassified_last(["Unclassified", "Energy", "Technology", "Unknown"])
    assert labels[-2:] == ["Unclassified", "Unknown"]
    assert labels[0] == "Energy"
    assert is_unclassified("Unknown")
    assert is_unclassified(UNCLASSIFIED)
    assert not is_unclassified("Technology")


def test_unknown_bookmark_selects_the_unclassified_bucket():
    assert canonical_sector_param("Unknown") == UNCLASSIFIED
    assert canonical_sector_param("") == ""
    assert canonical_sector_param("Energy") == "Energy"


def test_sector_cards_sort_unclassified_last():
    from app.sectors_page import _sector_rollups

    df = pd.DataFrame([
        {
            "account": "Ira", "symbol": "ZZZ", "strategy": "Buy and Hold",
            "total_pnl": 5000, "realized_pnl": 5000, "unrealized_pnl": 0,
            "total_premium_received": 0, "total_dividend_income": 0,
            "total_return": 5000, "num_individual_trades": 2,
            "num_winners": 1, "num_losers": 1,
            "sector": "Unknown", "subsector": "Unknown",
        },
        {
            "account": "Ira", "symbol": "AAPL", "strategy": "Buy and Hold",
            "total_pnl": 10, "realized_pnl": 10, "unrealized_pnl": 0,
            "total_premium_received": 0, "total_dividend_income": 0,
            "total_return": 10, "num_individual_trades": 1,
            "num_winners": 1, "num_losers": 0,
            "sector": "Technology", "subsector": "Consumer Electronics",
        },
    ])
    roll = _sector_rollups(df)
    names = [row["sector"] for row in roll["sector_rows"]]
    assert names[-1] == UNCLASSIFIED
    assert "Unknown" not in names
    assert names[0] == "Technology"


def test_discovery_lab_copy_is_plain():
    from pathlib import Path

    insights = Path("app/insights.py").read_text()
    template = Path("app/templates/insights.html").read_text()
    banned = [
        "curves other platforms",
        "broker blotter",
        "almost anyone",
        "Retail risk tools",
        "no other retail tool",
        "Evidence-backed, not adjectives",
    ]
    blob = insights + template
    for phrase in banned:
        assert phrase not in blob
