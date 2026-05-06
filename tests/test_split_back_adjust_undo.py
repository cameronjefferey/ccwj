"""Pin the split back-adjust undo logic from current_position_stock_price.py.

yfinance returns historical close prices retroactively scaled to today's
share basis. For a 1-for-30 reverse split, pre-split closes are
multiplied by 30; two consecutive 1-for-30 reverse splits multiply by
900. Our share ledger is keyed off raw broker tickets in stg_history,
so back-adjusted closes against raw shares produce nonsense P&L (a
real bug shipped a $17M phantom equity peak on /accounts: 8000 raw
RVSN shares marked against a $2,214 back-adjusted close instead of
the actual ~$2.46 trading price for that day).

The fix multiplies each historical close by the product of split
ratios for splits whose effective date is after that close — undoing
yfinance's adjustment exactly.

These tests use synthetic split / price data so they don't reach
out to Yahoo; they pin the math, not the upstream data.
"""
from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest


def _undo_split_back_adjust(closes: pd.Series, splits: pd.Series) -> pd.Series:
    """Mirror the in-pipeline transform.

    Implemented as a standalone helper inside the test so we can pin
    the math even though the production version lives inline in a
    standalone script (``current_position_stock_price.py``).
    """
    out = closes.astype(float).copy()
    if splits is None or len(splits) == 0:
        return out
    hist_dates = out.index
    for sd, ratio in zip(list(splits.index), [float(r) for r in splits.values]):
        if not (ratio > 0):
            continue
        mask = hist_dates < sd
        if mask.any():
            out.loc[mask] = out.loc[mask] * ratio
    return out


def _ts(d: dt.date) -> pd.Timestamp:
    return pd.Timestamp(d)


class TestSplitBackAdjustUndo:
    def test_no_splits_is_identity(self):
        closes = pd.Series(
            [100.0, 101.0, 102.0],
            index=pd.DatetimeIndex(
                [_ts(dt.date(2024, 1, 1)), _ts(dt.date(2024, 1, 2)), _ts(dt.date(2024, 1, 3))]
            ),
        )
        splits = pd.Series([], index=pd.DatetimeIndex([]), dtype=float)
        out = _undo_split_back_adjust(closes, splits)
        pd.testing.assert_series_equal(out, closes.astype(float))

    def test_post_split_dates_are_unchanged(self):
        # A historical close on or after the split date is in
        # post-split units already; no adjustment needed.
        closes = pd.Series(
            [10.0],
            index=pd.DatetimeIndex([_ts(dt.date(2026, 3, 1))]),
        )
        splits = pd.Series(
            [0.033333],
            index=pd.DatetimeIndex([_ts(dt.date(2026, 2, 3))]),
        )
        out = _undo_split_back_adjust(closes, splits)
        assert out.iloc[0] == pytest.approx(10.0)

    def test_single_reverse_split_undoes(self):
        # A 1-for-30 reverse split (ratio=1/30) inflates pre-split closes
        # by 30x. Multiplying the inflated close by the ratio recovers
        # the actual price.
        closes = pd.Series(
            [60.0],
            index=pd.DatetimeIndex([_ts(dt.date(2025, 6, 1))]),
        )
        splits = pd.Series(
            [1.0 / 30.0],
            index=pd.DatetimeIndex([_ts(dt.date(2026, 2, 3))]),
        )
        out = _undo_split_back_adjust(closes, splits)
        assert out.iloc[0] == pytest.approx(2.0, rel=1e-6)

    def test_two_consecutive_reverse_splits_compound(self):
        # The RVSN case: TWO 1-for-30 reverse splits (Feb 2026) → 900x
        # back-adjustment. yfinance reports $2,214; actual close ~$2.46.
        closes = pd.Series(
            [2214.0],
            index=pd.DatetimeIndex([_ts(dt.date(2024, 12, 30))]),
        )
        splits = pd.Series(
            [1.0 / 30.0, 1.0 / 30.0],
            index=pd.DatetimeIndex(
                [_ts(dt.date(2026, 2, 3)), _ts(dt.date(2026, 2, 4))]
            ),
        )
        out = _undo_split_back_adjust(closes, splits)
        assert out.iloc[0] == pytest.approx(2.46, rel=1e-3)

    def test_split_in_middle_only_affects_earlier_dates(self):
        # Three close prices, with a split between the second and third.
        # The split should only adjust the first two.
        closes = pd.Series(
            [60.0, 90.0, 5.0],
            index=pd.DatetimeIndex(
                [
                    _ts(dt.date(2024, 1, 1)),
                    _ts(dt.date(2025, 1, 1)),
                    _ts(dt.date(2026, 6, 1)),
                ]
            ),
        )
        splits = pd.Series(
            [1.0 / 30.0],
            index=pd.DatetimeIndex([_ts(dt.date(2026, 2, 3))]),
        )
        out = _undo_split_back_adjust(closes, splits)
        assert out.iloc[0] == pytest.approx(2.0, rel=1e-6)
        assert out.iloc[1] == pytest.approx(3.0, rel=1e-6)
        # Post-split row untouched
        assert out.iloc[2] == pytest.approx(5.0)

    def test_forward_split_ratio_greater_than_one_scales_down(self):
        # 4-for-1 forward split has ratio=4; pre-split closes were
        # back-adjusted to 1/4 of the actual price. Multiplying by 4
        # recovers the actual price.
        closes = pd.Series(
            [25.0],
            index=pd.DatetimeIndex([_ts(dt.date(2024, 1, 1))]),
        )
        splits = pd.Series(
            [4.0],
            index=pd.DatetimeIndex([_ts(dt.date(2025, 1, 1))]),
        )
        out = _undo_split_back_adjust(closes, splits)
        assert out.iloc[0] == pytest.approx(100.0)

    def test_zero_or_negative_ratio_is_skipped(self):
        # Defensive: malformed splits feed shouldn't crash the pipeline.
        closes = pd.Series(
            [10.0],
            index=pd.DatetimeIndex([_ts(dt.date(2024, 1, 1))]),
        )
        splits = pd.Series(
            [0.0, -1.0, 2.0],
            index=pd.DatetimeIndex(
                [
                    _ts(dt.date(2025, 1, 1)),
                    _ts(dt.date(2025, 2, 1)),
                    _ts(dt.date(2025, 3, 1)),
                ]
            ),
        )
        out = _undo_split_back_adjust(closes, splits)
        # Only the 2.0 split should apply.
        assert out.iloc[0] == pytest.approx(20.0)
