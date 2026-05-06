import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from google.cloud import bigquery

# Step 1: Initialize BigQuery client
client = bigquery.Client()

# Step 2: Query BigQuery to get all traded underlyings (equity AND option positions)
# so we also have stock prices for symbols held only as options.
query = """
    SELECT account, user_id, underlying_symbol AS symbol, MIN(trade_date) AS position_open_date
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE underlying_symbol IS NOT NULL
    GROUP BY 1, 2, 3
"""
positions = client.query(query).result()
positions_df = pd.DataFrame([dict(row.items()) for row in positions])

# Step 2b: Add benchmark tickers (SPY, QQQ) so weekly review can skip live yfinance.
# We mint one benchmark row per (account, user_id) so prices propagate to every
# tenant — see docs/USER_ID_TENANCY.md.
BENCHMARK_TICKERS = ["SPY", "QQQ"]
benchmark_start = date(date.today().year, 1, 1)
benchmark_rows = []
if not positions_df.empty:
    tenant_pairs = positions_df[["account", "user_id"]].drop_duplicates().to_records(index=False)
else:
    tenant_pairs = [("_benchmark", None)]
for account, user_id in tenant_pairs:
    for sym in BENCHMARK_TICKERS:
        already_present = not positions_df.empty and (
            (positions_df["symbol"] == sym)
            & (positions_df["account"] == account)
            & (positions_df["user_id"].astype("object") == user_id)
        ).any()
        if not already_present:
            benchmark_rows.append({
                "account": account,
                "user_id": user_id,
                "symbol": sym,
                "position_open_date": benchmark_start,
            })
if benchmark_rows:
    positions_df = pd.concat(
        [positions_df, pd.DataFrame(benchmark_rows)], ignore_index=True
    )

# Step 3: Collect daily price & dividend data
today = date.today()
# yfinance's end is exclusive, so use tomorrow to include today's close price
end_date = (today + timedelta(days=1)).isoformat()
all_data = []

for _, row in positions_df.iterrows():
    account = row["account"]
    user_id = row["user_id"] if "user_id" in row else None
    symbol = row["symbol"]
    start_date = row["position_open_date"].isoformat()

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(start=start_date, end=end_date)

        # Undo yfinance's split back-adjustment.
        #
        # yfinance always returns historical close prices retroactively
        # adjusted for splits — pre-split prices are scaled so that they
        # are directly comparable, on a per-current-share basis, to
        # post-split prices. For a 1-for-30 reverse split this means
        # pre-split closes are *multiplied* by 30 (because 1 new share
        # represents 30 old shares); two consecutive 1-for-30 reverse
        # splits inflate pre-split closes by 900×.
        #
        # That convention is the wrong default for us because our share
        # ledger is built from raw broker tickets (``stg_history``), not
        # from current-shares. Marking 8000 raw RVSN shares against a
        # back-adjusted $2,214 close produced a $17.7M phantom equity
        # peak on the /accounts chart (real peak ~$10K). The user never
        # held shares at $2,214 — that's just yfinance reverse-engineering
        # 2026 splits onto 2024 prices.
        #
        # Fix: multiply each historical close by the product of split
        # ratios for splits that occur AFTER its date. For RVSN on
        # 2024-12-30 the future ratios are 0.033333 × 0.033333 ≈
        # 0.001111 → raw close $2.46. For dates after all known splits
        # (or for symbols with no splits) the factor is 1 and we keep
        # whatever yfinance returned. Dividends from yfinance are not
        # consumed downstream (mart_daily_pnl reads them from
        # stg_history), so we leave the Dividends column alone.
        try:
            splits = ticker.splits
        except Exception:
            splits = None
        if splits is not None and len(splits) and not hist.empty:
            split_dates = list(splits.index)
            split_ratios = [float(r) for r in splits.values]
            close = hist["Close"].astype(float).copy()
            hist_dates = hist.index
            for sd, ratio in zip(split_dates, split_ratios):
                if not (ratio > 0):
                    continue
                mask = hist_dates < sd
                if mask.any():
                    close.loc[mask] = close.loc[mask] * ratio
            hist["Close"] = close

        hist = hist[["Close", "Dividends"]].reset_index()
        hist["account"] = account
        hist["user_id"] = user_id
        hist["symbol"] = symbol
        hist.rename(columns={"Date": "date", "Close": "close_price", "Dividends": "dividend"}, inplace=True)

        # Ensure we're only keeping data from on/after position_open_date
        hist = hist[hist["date"].dt.date >= row["position_open_date"]]

        all_data.append(hist)
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")

# Step 4: Upload results to BigQuery
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    # user_id round-trips through pandas as object/Int64; coerce to nullable Int64
    # so BigQuery loads it cleanly as INT64 NULLABLE alongside legacy NULL rows.
    final_df["user_id"] = pd.to_numeric(final_df["user_id"], errors="coerce").astype("Int64")
    final_df = final_df[["account", "user_id", "symbol", "date", "close_price", "dividend"]]

    table_id = "ccwj-dbt.analytics.daily_position_performance"
    final_df = final_df.drop_duplicates()
    job = client.load_table_from_dataframe(
        final_df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()  # Waits for the job to complete
    print("Upload successful.")
else:
    print("No data to upload.")
