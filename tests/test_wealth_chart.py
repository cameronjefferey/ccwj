"""Wealth chart helpers — collapse duplicate mart rows before groupby-sum."""

import pandas as pd

from app.wealth import _build_chart_payload, _collapse_wealth_daily_duplicate_grain


def test_collapse_keeps_populated_user_id_over_null_twins():
    """Stage 0/1 tenancy leniency can emit NULL + populated user_id rows for the
    same account/day — summing them in chart groupby inflated values ~2×."""
    ts = pd.Timestamp("2026-05-11")
    base = {
        "account": "Emmory Investment",
        "date": ts,
        "account_value": 15940.0,
        "cash_value": 28.0,
        "equity_value": 15912.0,
        "option_value": 0.0,
    }
    df = pd.DataFrame(
        [{**base, "user_id": None}, {**base, "user_id": 9}],
    )
    collapsed = _collapse_wealth_daily_duplicate_grain(df)
    assert len(collapsed) == 1
    assert pd.notna(collapsed["user_id"].iloc[0])


def test_build_chart_payload_not_doubled_after_collapse_vs_raw_duplicate():
    ts = pd.Timestamp("2026-05-07")
    base = {
        "account": "A",
        "date": ts,
        "account_value": 100.0,
        "cash_value": 10.0,
        "equity_value": 90.0,
        "option_value": 0.0,
        "user_id": None,
    }
    dup = pd.DataFrame(
        [
            dict(base),
            {**base, "user_id": 1},
        ]
    )
    raw_chart = _build_chart_payload(dup)
    collapsed = _collapse_wealth_daily_duplicate_grain(dup)
    fixed_chart = _build_chart_payload(collapsed)
    assert raw_chart["account_value"][0] == 200.0
    assert fixed_chart["account_value"][0] == 100.0


def test_build_chart_payload_sums_multiple_accounts_same_date_after_collapse():
    """Different accounts same day remain additive (combined view)."""
    d = pd.Timestamp("2026-05-07")
    df = pd.DataFrame(
        [
            {
                "account": "A",
                "user_id": 1,
                "date": d,
                "account_value": 100,
                "cash_value": 0,
                "equity_value": 100,
                "option_value": 0,
            },
            {
                "account": "B",
                "user_id": 1,
                "date": d,
                "account_value": 50,
                "cash_value": 10,
                "equity_value": 40,
                "option_value": 0,
            },
        ]
    )
    out = _build_chart_payload(_collapse_wealth_daily_duplicate_grain(df))
    assert str(out["dates"][0]).startswith("2026-05-07")
    assert out["account_value"] == [150.0]

