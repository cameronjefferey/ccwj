"""
Refresh earnings_calendar: pull next-earnings AND next-ex-div dates from
yfinance for every ticker that shows up in stg_history (excluding crypto),
and load into ccwj-dbt.analytics.earnings_calendar.

yfinance exposes both via Ticker(...).calendar — a dict shaped like:
  {
    'Earnings Date': [datetime.date(2026, 7, 30)]   # 1-2 dates (windowed)
                or [datetime.date(...), datetime.date(...)],
    'Earnings High': 1.99, 'Earnings Low': 1.83, ...
    'Dividend Date': datetime.date(...),
    'Ex-Dividend Date': datetime.date(...),
  }

ETFs / indices often have an Ex-Dividend Date and no earnings (JEPI/JEPQ/
SPY). We used to return an empty row when Earnings Date was missing, so
the calendar's real future ex-div never landed in the warehouse and Daily
Review guessed from last+median spacing — which is how JEPI vanished
next to JEPQ. Persist whatever the calendar has; NULL the rest.

A row is always written (negative cache) so we know a ticker was tried.

Mirrors the operational pattern of scripts/refresh_symbol_metadata.py:
  - Read distinct symbols out of BigQuery
  - Hit yfinance per symbol with a small sleep between calls
  - WRITE_TRUNCATE the result table

Symbol-only (no account / user_id) — safe to share across tenants. Joins
to per-user holdings happen downstream in dbt / Flask.

Run locally:    python scripts/refresh_earnings_calendar.py
Run in CI:      called from .github/workflows/bigquery_update.yml
"""

from __future__ import annotations

import csv
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
from google.cloud import bigquery

TABLE_ID = "ccwj-dbt.analytics.earnings_calendar"
SLEEP_BETWEEN_CALLS_SEC = 0.2


def _load_crypto_symbols() -> set[str]:
    """Load crypto tickers from the seed so we don't hammer yfinance with
    'BTC' / 'ETH' / 'USDC' lookups that always come back empty."""
    seed_path = Path(__file__).resolve().parent.parent / "dbt" / "seeds" / "crypto_symbols.csv"
    if not seed_path.exists():
        return set()
    out: set[str] = set()
    with seed_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = (row.get("symbol") or "").strip().upper()
            if sym:
                out.add(sym)
    return out


def _distinct_symbols(client: bigquery.Client) -> list[str]:
    """All underlyings any user has ever traded. Upper/trim so 'aapl' /
    ' AAPL ' don't show up as separate rows."""
    sql = """
        SELECT DISTINCT UPPER(TRIM(underlying_symbol)) AS symbol
        FROM `ccwj-dbt.analytics.stg_history`
        WHERE underlying_symbol IS NOT NULL
          AND TRIM(underlying_symbol) != ''
    """
    rows = client.query(sql).result()
    return sorted({r["symbol"] for r in rows if r["symbol"]})


def _coerce_date(val) -> date | None:
    """yfinance hands back datetime.date already, but be defensive against
    datetime / Timestamp / string / unix-seconds drift across versions."""
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, (int, float)) and val > 0:
        try:
            return datetime.fromtimestamp(val, tz=timezone.utc).date()
        except (OverflowError, OSError, ValueError):
            return None
    try:
        return pd.Timestamp(val).date()
    except Exception:  # noqa: BLE001
        return None


def _first_date(raw) -> date | None:
    """Calendar values are a date, a list of dates, or a Timestamp."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    if not isinstance(raw, (list, tuple)):
        raw = [raw]
    coerced = [d for d in (_coerce_date(d) for d in raw) if d is not None]
    return min(coerced) if coerced else None


def _as_calendar_dict(cal) -> dict:
    if cal is None:
        return {}
    if isinstance(cal, dict):
        return cal
    if isinstance(cal, pd.DataFrame):
        out = {}
        for idx, row in cal.iterrows():
            out[str(idx)] = row.iloc[0] if len(row) else None
        return out
    return {}


def _empty_row(symbol: str) -> dict:
    return {
        "symbol": symbol,
        "next_earnings_date": None,
        "earnings_window_start": None,
        "earnings_window_end": None,
        "next_ex_div_date": None,
        "next_dividend_pay_date": None,
        "fetched_at": datetime.now(timezone.utc),
    }


def _fetch_one(symbol: str, ticker_factory=None) -> dict:
    """Hit yfinance for one ticker. Always returns a row dict (with NULL
    date fields for unknowns / errors) so the negative cache is preserved
    and a single bad ticker can't kill the whole run.

    ``ticker_factory`` is a test seam (same shape as refresh_symbol_metadata).
    """
    factory = ticker_factory or yf.Ticker
    empty = _empty_row(symbol)
    try:
        cal = _as_calendar_dict(factory(symbol).calendar)
    except Exception as exc:  # noqa: BLE001 — yfinance raises a wide range
        print(f"  ! {symbol}: yfinance error: {exc}")
        return empty

    if not cal:
        return empty

    earnings_dates = cal.get("Earnings Date") or []
    if not isinstance(earnings_dates, (list, tuple)):
        earnings_dates = [earnings_dates]
    coerced = [d for d in (_coerce_date(d) for d in earnings_dates) if d is not None]

    empty["next_ex_div_date"] = _first_date(
        cal.get("Ex-Dividend Date") or cal.get("exDividendDate")
    )
    empty["next_dividend_pay_date"] = _first_date(
        cal.get("Dividend Date") or cal.get("dividendDate")
    )

    if not coerced:
        return empty

    empty["next_earnings_date"] = min(coerced)
    empty["earnings_window_start"] = min(coerced)
    empty["earnings_window_end"] = max(coerced)
    return empty


def main() -> None:
    client = bigquery.Client()
    crypto = _load_crypto_symbols()
    all_symbols = _distinct_symbols(client)
    # SPY/QQQ used to be skipped because they have no earnings. They DO
    # have ex-div dates, and tenants hold them, so they stay in the fetch.
    symbols = [s for s in all_symbols if s not in crypto]
    skipped = len(all_symbols) - len(symbols)
    print(f"Fetching earnings/ex-div calendar for {len(symbols)} symbols "
          f"(skipped {skipped} crypto)...")

    rows: list[dict] = []
    for i, sym in enumerate(symbols, start=1):
        rows.append(_fetch_one(sym))
        if i % 25 == 0:
            print(f"  ... {i}/{len(symbols)}")
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    if not rows:
        print("No calendar rows fetched. Bailing out without overwriting the table.")
        return

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["symbol"], keep="last")

    schema = [
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("next_earnings_date", "DATE"),
        bigquery.SchemaField("earnings_window_start", "DATE"),
        bigquery.SchemaField("earnings_window_end", "DATE"),
        bigquery.SchemaField("next_ex_div_date", "DATE"),
        bigquery.SchemaField("next_dividend_pay_date", "DATE"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP"),
    ]
    job = client.load_table_from_dataframe(
        df,
        TABLE_ID,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        ),
    )
    job.result()
    with_earn = int(df["next_earnings_date"].notna().sum())
    with_div = int(df["next_ex_div_date"].notna().sum())
    print(f"Loaded {len(df)} rows into {TABLE_ID} "
          f"({with_earn} earnings, {with_div} ex-div).")


if __name__ == "__main__":
    main()
