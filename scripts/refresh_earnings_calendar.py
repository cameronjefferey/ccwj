"""
Refresh earnings_calendar: pull next-earnings dates from yfinance for every
ticker that shows up in stg_history (excluding crypto symbols and benchmarks
that don't report earnings), and load into ccwj-dbt.analytics.earnings_calendar.

yfinance exposes earnings via Ticker(...).calendar — a dict shaped like:
  {
    'Earnings Date': [datetime.date(2026, 7, 30)]   # 1-2 dates (windowed)
                or [datetime.date(...), datetime.date(...)],
    'Earnings High': 1.99, 'Earnings Low': 1.83, ...
    'Dividend Date': ..., 'Ex-Dividend Date': ...,
  }

ETFs / indices / crypto return {} (no fundamentals). We persist a row even
for those (as a negative cache, NULL date) so we have visibility into what
was tried without scanning logs.

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

# Symbols that yfinance categorically has no earnings calendar for. We skip
# them outright to avoid 404 spam in CI logs. Benchmark ETFs included
# because every tenant gets SPY/QQQ minted into stg_history by the price
# loader.
BENCHMARK_TICKERS = {"SPY", "QQQ"}


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
    datetime / Timestamp / string drift across versions."""
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    try:
        return pd.Timestamp(val).date()
    except Exception:  # noqa: BLE001
        return None


def _fetch_one(symbol: str) -> dict:
    """Hit yfinance for one ticker. Always returns a row dict (with NULL
    date fields for unknowns / errors) so the negative cache is preserved
    and a single bad ticker can't kill the whole run."""
    fetched_at = datetime.now(timezone.utc)
    empty = {
        "symbol": symbol,
        "next_earnings_date": None,
        "earnings_window_start": None,
        "earnings_window_end": None,
        "fetched_at": fetched_at,
    }
    try:
        cal = yf.Ticker(symbol).calendar or {}
    except Exception as exc:  # noqa: BLE001 — yfinance raises a wide range
        print(f"  ! {symbol}: yfinance error: {exc}")
        return empty

    if not isinstance(cal, dict) or not cal:
        return empty

    earnings_dates = cal.get("Earnings Date") or []
    if not isinstance(earnings_dates, (list, tuple)):
        earnings_dates = [earnings_dates]
    coerced = [d for d in (_coerce_date(d) for d in earnings_dates) if d is not None]
    if not coerced:
        return empty

    return {
        "symbol": symbol,
        "next_earnings_date": min(coerced),
        "earnings_window_start": min(coerced),
        "earnings_window_end": max(coerced),
        "fetched_at": fetched_at,
    }


def main() -> None:
    client = bigquery.Client()
    crypto = _load_crypto_symbols()
    all_symbols = _distinct_symbols(client)
    symbols = [s for s in all_symbols if s not in crypto and s not in BENCHMARK_TICKERS]
    skipped = len(all_symbols) - len(symbols)
    print(f"Fetching earnings for {len(symbols)} symbols (skipped {skipped} crypto/benchmark)...")

    rows: list[dict] = []
    for i, sym in enumerate(symbols, start=1):
        rows.append(_fetch_one(sym))
        if i % 25 == 0:
            print(f"  ... {i}/{len(symbols)}")
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    if not rows:
        print("No earnings fetched. Bailing out without overwriting the table.")
        return

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["symbol"], keep="last")

    schema = [
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("next_earnings_date", "DATE"),
        bigquery.SchemaField("earnings_window_start", "DATE"),
        bigquery.SchemaField("earnings_window_end", "DATE"),
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
    with_dates = int(df["next_earnings_date"].notna().sum())
    print(f"Loaded {len(df)} rows into {TABLE_ID} ({with_dates} with earnings dates).")


if __name__ == "__main__":
    main()
