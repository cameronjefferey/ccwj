"""
Refresh symbol_metadata: pull sector / industry / market cap / company name
from yfinance for every ticker that shows up in stg_history (plus SPY/QQQ
benchmarks), and load it into ccwj-dbt.analytics.symbol_metadata.

Mirrors the operational pattern of current_position_stock_price.py:
  - Read distinct symbols out of BigQuery
  - Hit a free public API (yfinance)
  - WRITE_TRUNCATE the result table

The table is symbol-only (no account column), so it is safe to share across
all tenants. Joins happen downstream in dbt.

Run locally:    python scripts/refresh_symbol_metadata.py
Run in CI:      called from .github/workflows/bigquery_update.yml
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
from google.cloud import bigquery

TABLE_ID = "ccwj-dbt.analytics.symbol_metadata"
BENCHMARK_TICKERS = ["SPY", "QQQ"]
SLEEP_BETWEEN_CALLS_SEC = 0.2  # be polite to Yahoo


def _distinct_symbols(client: bigquery.Client) -> list[str]:
    """All underlyings any user has ever traded, plus benchmarks. Upper/trim
    so 'aapl' / ' AAPL ' don't show up as separate rows."""
    sql = """
        SELECT DISTINCT UPPER(TRIM(underlying_symbol)) AS symbol
        FROM `ccwj-dbt.analytics.stg_history`
        WHERE underlying_symbol IS NOT NULL
          AND TRIM(underlying_symbol) != ''
    """
    rows = client.query(sql).result()
    symbols = {r["symbol"] for r in rows if r["symbol"]}
    symbols.update(BENCHMARK_TICKERS)
    return sorted(symbols)


def _fetch_one(symbol: str) -> dict | None:
    """Hit yfinance for one ticker. Returns None on any error so a single
    bad ticker can't kill the whole run."""
    try:
        info = yf.Ticker(symbol).info or {}
    except Exception as exc:  # noqa: BLE001 — yfinance raises a wide range
        print(f"  ! {symbol}: yfinance error: {exc}")
        return None

    sector = info.get("sector")
    industry = info.get("industry")
    industry_disp = info.get("industryDisp") or industry
    if not sector and not industry:
        # No metadata at all — likely a delisted / OTC / synthetic symbol.
        # Still record the row so we know we tried, with Unknown placeholders.
        print(f"  - {symbol}: no sector/industry from yfinance")

    return {
        "symbol": symbol,
        "sector": sector or "Unknown",
        "industry": industry or "Unknown",
        "industry_group": industry_disp or industry or "Unknown",
        "country": info.get("country") or "Unknown",
        "market_cap": int(info["marketCap"]) if info.get("marketCap") else None,
        "long_name": info.get("longName") or info.get("shortName") or symbol,
        "fetched_at": datetime.now(timezone.utc),
    }


def main() -> None:
    client = bigquery.Client()
    symbols = _distinct_symbols(client)
    print(f"Fetching metadata for {len(symbols)} symbols...")

    rows: list[dict] = []
    for i, sym in enumerate(symbols, start=1):
        row = _fetch_one(sym)
        if row:
            rows.append(row)
        if i % 25 == 0:
            print(f"  ... {i}/{len(symbols)}")
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    if not rows:
        print("No metadata fetched. Bailing out without overwriting the table.")
        return

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["symbol"], keep="last")

    schema = [
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sector", "STRING"),
        bigquery.SchemaField("industry", "STRING"),
        bigquery.SchemaField("industry_group", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("market_cap", "INT64"),
        bigquery.SchemaField("long_name", "STRING"),
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
    print(f"Loaded {len(df)} rows into {TABLE_ID}.")


if __name__ == "__main__":
    main()
