"""Unit tests for scripts/refresh_earnings_calendar.py.

The loader used to return an empty row when Earnings Date was missing,
so ETF calendars (JEPI/JEPQ/SPY) never persisted their Ex-Dividend Date.
These tests pin the fetch seam: persist whatever Ticker.calendar has.
"""
from datetime import date, datetime, timezone

import pandas as pd

from scripts import refresh_earnings_calendar as cal


class FakeTicker:
    def __init__(self, calendar):
        self.calendar = calendar


class BoomTicker:
    def __init__(self, *_args, **_kwargs):
        raise RuntimeError("yfinance 429")


def _fetch(calendar):
    return cal._fetch_one("JEPI", ticker_factory=lambda _: FakeTicker(calendar))


def test_earnings_only_fills_earnings_fields():
    row = _fetch({"Earnings Date": [date(2026, 7, 30), date(2026, 8, 3)]})
    assert row["symbol"] == "JEPI"
    assert row["next_earnings_date"] == date(2026, 7, 30)
    assert row["earnings_window_start"] == date(2026, 7, 30)
    assert row["earnings_window_end"] == date(2026, 8, 3)
    assert row["next_ex_div_date"] is None
    assert row["next_dividend_pay_date"] is None


def test_ex_div_only_etf_still_persists_ex_div():
    """JEPI-shaped: Ex-Dividend Date, no Earnings Date. Used to be empty."""
    row = _fetch({
        "Ex-Dividend Date": date(2026, 9, 2),
        "Dividend Date": date(2026, 9, 5),
    })
    assert row["next_ex_div_date"] == date(2026, 9, 2)
    assert row["next_dividend_pay_date"] == date(2026, 9, 5)
    assert row["next_earnings_date"] is None
    assert row["earnings_window_start"] is None
    assert row["earnings_window_end"] is None


def test_both_earnings_and_ex_div():
    row = _fetch({
        "Earnings Date": [date(2026, 10, 15)],
        "Ex-Dividend Date": date(2026, 9, 2),
        "Dividend Date": date(2026, 9, 5),
    })
    assert row["next_earnings_date"] == date(2026, 10, 15)
    assert row["next_ex_div_date"] == date(2026, 9, 2)
    assert row["next_dividend_pay_date"] == date(2026, 9, 5)


def test_camelcase_ex_div_keys():
    row = _fetch({
        "exDividendDate": date(2026, 9, 2),
        "dividendDate": date(2026, 9, 5),
    })
    assert row["next_ex_div_date"] == date(2026, 9, 2)
    assert row["next_dividend_pay_date"] == date(2026, 9, 5)


def test_empty_calendar_is_all_null():
    row = _fetch({})
    assert row["next_earnings_date"] is None
    assert row["next_ex_div_date"] is None
    assert row["next_dividend_pay_date"] is None
    assert row["symbol"] == "JEPI"


def test_yfinance_error_is_still_a_row():
    row = cal._fetch_one("JEPI", ticker_factory=BoomTicker)
    assert row["symbol"] == "JEPI"
    assert row["next_earnings_date"] is None
    assert row["next_ex_div_date"] is None


def test_unix_timestamp_ex_div():
    ts = datetime(2026, 9, 2, tzinfo=timezone.utc).timestamp()
    row = _fetch({"Ex-Dividend Date": ts})
    assert row["next_ex_div_date"] == date(2026, 9, 2)


def test_list_of_ex_div_dates_uses_earliest():
    row = _fetch({"Ex-Dividend Date": [date(2026, 9, 10), date(2026, 9, 2)]})
    assert row["next_ex_div_date"] == date(2026, 9, 2)


def test_dataframe_calendar_shape():
    frame = pd.DataFrame(
        {0: [date(2026, 9, 2), date(2026, 9, 5)]},
        index=["Ex-Dividend Date", "Dividend Date"],
    )
    row = _fetch(frame)
    assert row["next_ex_div_date"] == date(2026, 9, 2)
    assert row["next_dividend_pay_date"] == date(2026, 9, 5)


def test_email_ex_divs_sql_stays_in_sync_with_calendar():
    from app.email_digests_cli import _EX_DIVS_SQL

    assert "stg_ex_div_calendar" in _EX_DIVS_SQL
    assert "ex_div_source" in _EX_DIVS_SQL
    assert "LEFT JOIN" in _EX_DIVS_SQL
    assert "CEIL(" in _EX_DIVS_SQL
