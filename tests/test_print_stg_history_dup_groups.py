"""Warehouse dump must accept NULL trade_date (pandas NaT → BQ NULL)."""
import importlib.util
from datetime import date, datetime
from pathlib import Path

import pandas as pd

_SPEC = importlib.util.spec_from_file_location(
    "print_stg_history_dup_groups",
    Path(__file__).resolve().parents[1] / "scripts" / "print_stg_history_dup_groups.py",
)
_dump = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_dump)
_as_bq_date = _dump._as_bq_date


def test_as_bq_date_maps_nat_to_none():
    assert _as_bq_date(pd.NaT) is None
    assert _as_bq_date("NaT") is None
    assert _as_bq_date(None) is None


def test_as_bq_date_keeps_real_dates():
    assert _as_bq_date(date(2024, 5, 14)) == date(2024, 5, 14)
    assert _as_bq_date(datetime(2024, 5, 14, 20, 30)) == date(2024, 5, 14)
    assert _as_bq_date(pd.Timestamp("2024-05-14")) == date(2024, 5, 14)
