"""Wealth chart helpers — collapse duplicate mart rows before groupby-sum."""

from datetime import date

import pandas as pd

from app.wealth import (
    _build_chart_payload,
    _build_income_panel,
    _build_summary,
    _collapse_wealth_daily_duplicate_grain,
    _slice_wealth_to_range,
)


def _deposit_scenario():
    """4-day single account: +100 market, then a $5,000 deposit, then +100
    market. Raw value climbs $5,200; true trading/income gain is only $200.
    """
    rows = []
    spec = [
        ("2026-01-01", 1000.0, 0.0),
        ("2026-01-02", 1100.0, 0.0),
        ("2026-01-03", 6100.0, 5000.0),  # $5k deposit lands
        ("2026-01-04", 6200.0, 5000.0),
    ]
    for d, av, cum_dep in spec:
        rows.append({
            "tenant_id": "snaptrade:abc",
            "account": "A",
            "user_id": 9,
            "date": pd.Timestamp(d),
            "account_value": av,
            "cash_value": 0.0,
            "equity_value": av,
            "option_value": 0.0,
            "cumulative_net_deposits": cum_dep,
            "cumulative_dividends": 0.0,
            "cumulative_interest_net": 0.0,
            "cumulative_fees": 0.0,
        })
    return pd.DataFrame(rows)


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


# ---------------------------------------------------------------------------
# Deposit / withdrawal exclusion
# ---------------------------------------------------------------------------

def test_chart_payload_emits_deposit_adjusted_line():
    out = _build_chart_payload(_deposit_scenario(), exclude_transfers=True)
    # Raw account value climbs through the $5k deposit.
    assert out["account_value"] == [1000.0, 1100.0, 6100.0, 6200.0]
    # Deposit-adjusted line strips the $5k step (rebased to window start).
    assert out["account_value_ex_transfers"] == [1000.0, 1100.0, 1100.0, 1200.0]
    # Net deposits rebased to 0 on day one, jumping on the deposit day.
    assert out["net_deposits"] == [0.0, 0.0, 5000.0, 5000.0]
    assert out["has_transfers"] is True
    assert out["exclude_transfers"] is True


def test_chart_payload_no_transfer_column_is_graceful_noop():
    df = _deposit_scenario().drop(columns=["cumulative_net_deposits"])
    out = _build_chart_payload(df, exclude_transfers=True)
    # Adjusted line == raw when there's no transfer data to remove.
    assert out["account_value_ex_transfers"] == out["account_value"]
    assert out["net_deposits"] == [0.0, 0.0, 0.0, 0.0]
    assert out["has_transfers"] is False


def test_summary_change_in_range_excludes_deposits_when_toggled():
    df = _deposit_scenario()
    raw = _build_summary(df, exclude_transfers=False)
    adj = _build_summary(df, exclude_transfers=True)
    # Raw picks up the whole $5,200 climb; adjusted isolates the $200 gain.
    assert raw["change_in_range"]["abs"] == 5200.0
    assert adj["change_in_range"]["abs"] == 200.0
    # Point-in-time value is the real balance regardless of the toggle.
    assert raw["account_value"] == adj["account_value"] == 6200.0
    assert adj["net_deposits_in_range"] == 5000.0
    assert adj["has_transfers"] is True


def test_summary_no_transfer_column_matches_legacy_behavior():
    df = _deposit_scenario().drop(columns=["cumulative_net_deposits"])
    adj = _build_summary(df, exclude_transfers=True)
    # Without transfer data, excluding does nothing: full $5,200 shows.
    assert adj["change_in_range"]["abs"] == 5200.0
    assert adj["net_deposits_in_range"] == 0.0
    assert adj["has_transfers"] is False


def test_income_panel_reports_net_deposits_in_window():
    panel = _build_income_panel(_deposit_scenario())
    assert panel["net_deposits"] == 5000.0
    assert panel["has_transfers"] is True


def _value_curve(start, days, start_val=16335.40, daily_delta=-12.04):
    """Evenly spaced snapshots so lookback math is calendar-simple."""
    rows = []
    for i in range(days):
        d = pd.Timestamp(start) + pd.Timedelta(days=i)
        av = start_val + i * daily_delta
        rows.append({
            "tenant_id": "snaptrade:abc",
            "account": "Emmory",
            "user_id": 9,
            "date": d,
            "account_value": av,
            "cash_value": 28.0,
            "equity_value": av - 28.0,
            "option_value": 0.0,
            "cumulative_net_deposits": 0.0,
            "cumulative_dividends": 0.0,
            "cumulative_interest_net": 0.0,
            "cumulative_fees": 0.0,
        })
    return pd.DataFrame(rows)


def test_vs_90d_falls_back_to_first_snapshot_when_history_is_shorter():
    """Emmory-class: snapshots start ~80 days ago, so today-90d has no row."""
    df = _value_curve("2026-06-08", 80)
    out = _build_summary(df)
    assert out["change_90d"] is not None
    assert out["change_90d_since"] is not None
    assert "Jun" in out["change_90d_since"]
    assert out["change_90d"]["abs"] == out["change_in_range"]["abs"]


def test_vs_90d_uses_true_90d_row_when_history_is_long_enough():
    df = _value_curve("2026-04-01", 150, start_val=17000.0, daily_delta=-5.0)
    out = _build_summary(df)
    assert out["change_90d_since"] is None
    latest = df.iloc[-1]["account_value"]
    target = pd.Timestamp(df.iloc[-1]["date"]) - pd.Timedelta(days=90)
    ref = df.loc[df["date"] <= target].iloc[-1]["account_value"]
    assert out["change_90d"]["abs"] == round(float(latest) - float(ref), 2)


def test_30d_window_does_not_blank_the_90d_card():
    """Pre-fix, range was applied in SQL so a 30d view always said
    'Not enough history' for vs 90d."""
    full = _value_curve("2026-04-01", 150, start_val=17000.0, daily_delta=-5.0)
    end = full.iloc[-1]["date"].date()
    start = end - pd.Timedelta(days=30)
    window = _slice_wealth_to_range(full, start, end)
    out = _build_summary(window, lookback_df=full)
    assert out["change_90d"] is not None
    assert out["change_90d_since"] is None
    # Change-in-range is the 30d window, not the 90d lookback.
    assert out["change_in_range"]["abs"] != out["change_90d"]["abs"]


def test_slice_keeps_only_the_requested_window():
    df = _value_curve("2026-06-01", 60)
    sliced = _slice_wealth_to_range(df, date(2026, 7, 1), date(2026, 7, 31))
    assert sliced["date"].min() >= pd.Timestamp("2026-07-01")
    assert sliced["date"].max() <= pd.Timestamp("2026-07-31")
    assert len(sliced) == 30

