"""
Refresh symbol_metadata: pull sector / subsector / market cap / company
name from yfinance for every ticker that shows up in stg_history (plus
SPY/QQQ benchmarks), and load it into ccwj-dbt.analytics.symbol_metadata.

We use the finance-standard hierarchy "sector → subsector". yfinance
exposes company tickers under the keys 'sector' and 'industry' (and a
friendlier 'industryDisp' display string), but everywhere else in this
codebase they're called sector / subsector — including the BigQuery table
this script writes. yfinance keys → BigQuery columns:
  - info['sector']                       → sector
  - info['industryDisp'] or ['industry'] → subsector

ETFs and mutual funds often do not populate those company fields. For
funds, fall back to yfinance's fund-specific sector allocations:
  - funds_data.sector_weightings max key → sector
  - fund_overview['categoryName']        → subsector

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

TABLE_ID = "ccwj-dbt.analytics.symbol_metadata"
BENCHMARK_TICKERS = ["SPY", "QQQ"]
SLEEP_BETWEEN_CALLS_SEC = 0.2  # be polite to Yahoo

YF_SECTOR_LABELS = {
    "basic_materials": "Basic Materials",
    "communication_services": "Communication Services",
    "consumer_cyclical": "Consumer Cyclical",
    "consumer_defensive": "Consumer Defensive",
    "energy": "Energy",
    "financial_services": "Financial Services",
    "healthcare": "Healthcare",
    "industrials": "Industrials",
    "realestate": "Real Estate",
    "real_estate": "Real Estate",
    "technology": "Technology",
    "utilities": "Utilities",
}


def _distinct_symbols(client) -> list[str]:
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


def _clean_text(value) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def _normalize_yf_sector_key(key) -> str | None:
    text = _clean_text(key)
    if not text:
        return None

    normalized = (
        text.lower()
        .replace("&", "and")
        .replace("-", "_")
        .replace(" ", "_")
    )
    return YF_SECTOR_LABELS.get(
        normalized,
        " ".join(part.capitalize() for part in normalized.split("_") if part),
    )


def _numeric_weight(value) -> float | None:
    if isinstance(value, dict):
        value = value.get("raw")

    try:
        weight = float(value)
    except (TypeError, ValueError):
        return None

    return weight if weight > 0 else None


def _dominant_fund_sector(sector_weightings) -> str | None:
    if not isinstance(sector_weightings, dict):
        return None

    weighted_sectors = []
    for key, value in sector_weightings.items():
        label = _normalize_yf_sector_key(key)
        weight = _numeric_weight(value)
        if label and weight is not None:
            weighted_sectors.append((weight, label))

    if not weighted_sectors:
        return None

    return max(weighted_sectors, key=lambda item: (item[0], item[1]))[1]


def _fund_metadata(ticker, info: dict) -> tuple[str | None, str | None]:
    symbol = getattr(ticker, "ticker", "ticker")
    try:
        funds_data = ticker.funds_data
    except Exception as exc:  # noqa: BLE001 — yfinance fund lookups are broad
        print(f"  - {symbol}: no fund metadata from yfinance: {exc}")
        return None, _clean_text(info.get("category"))

    sector = None
    try:
        sector = _dominant_fund_sector(funds_data.sector_weightings)
    except Exception as exc:  # noqa: BLE001 — one bad module should not kill refresh
        print(f"  - {symbol}: no fund sector weightings from yfinance: {exc}")

    category = _clean_text(info.get("category"))
    try:
        fund_overview = funds_data.fund_overview or {}
        category = (
            category
            or _clean_text(fund_overview.get("categoryName"))
            or _clean_text(fund_overview.get("legalType"))
        )
    except Exception as exc:  # noqa: BLE001
        print(f"  - {symbol}: no fund category from yfinance: {exc}")

    return sector, category


def _fetch_one(symbol: str, ticker_factory=None) -> dict | None:
    """Hit yfinance for one ticker. Returns None on any error so a single
    bad ticker can't kill the whole run."""
    if ticker_factory is None:
        import yfinance as yf

        ticker_factory = yf.Ticker

    try:
        ticker = ticker_factory(symbol)
        info = ticker.info or {}
    except Exception as exc:  # noqa: BLE001 — yfinance raises a wide range
        print(f"  ! {symbol}: yfinance error: {exc}")
        return None

    sector = _clean_text(info.get("sector"))
    # Prefer yfinance's display string ('industryDisp') over the slug
    # ('industry'); it's the same hierarchy level but the human-readable
    # version (e.g. "Software—Application" vs "software-application").
    subsector = _clean_text(info.get("industryDisp")) or _clean_text(info.get("industry"))

    if not sector or not subsector:
        fund_sector, fund_subsector = _fund_metadata(ticker, info)
        sector = sector or fund_sector
        subsector = subsector or fund_subsector

    if not sector and not subsector:
        # No metadata at all — likely a delisted / OTC / synthetic symbol.
        # Still record the row so we know we tried, with Unknown placeholders.
        print(f"  - {symbol}: no sector/subsector from yfinance")

    return {
        "symbol": symbol,
        "sector": sector or "Unknown",
        "subsector": subsector or "Unknown",
        "country": info.get("country") or "Unknown",
        "market_cap": int(info["marketCap"]) if info.get("marketCap") else None,
        "long_name": info.get("longName") or info.get("shortName") or symbol,
        "fetched_at": datetime.now(timezone.utc),
    }


def main() -> None:
    import pandas as pd
    from google.cloud import bigquery

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
        bigquery.SchemaField("subsector", "STRING"),
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
