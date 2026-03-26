import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from google.cloud import bigquery

# Step 1: Initialize BigQuery client
client = bigquery.Client()

# Step 2: Query BigQuery to get all traded underlyings (equity AND option positions)
# so we also have stock prices for symbols held only as options.
query = """
    SELECT account, underlying_symbol AS symbol, MIN(trade_date) AS position_open_date
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE underlying_symbol IS NOT NULL
    GROUP BY 1, 2
"""
positions = client.query(query).result()
positions_df = pd.DataFrame([dict(row.items()) for row in positions])

# Step 2b: Add benchmark tickers (SPY, QQQ) so weekly review can skip live yfinance
BENCHMARK_TICKERS = ["SPY", "QQQ"]
benchmark_start = date(date.today().year, 1, 1)
benchmark_rows = []
accounts_seen = set(positions_df["account"].unique()) if not positions_df.empty else set()
benchmark_account = list(accounts_seen)[0] if accounts_seen else "_benchmark"
for sym in BENCHMARK_TICKERS:
    already_present = not positions_df.empty and (
        (positions_df["symbol"] == sym) & (positions_df["account"] == benchmark_account)
    ).any()
    if not already_present:
        benchmark_rows.append({
            "account": benchmark_account,
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
    symbol = row["symbol"]
    start_date = row["position_open_date"].isoformat()

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(start=start_date, end=end_date)
        hist = hist[["Close", "Dividends"]].reset_index()
        hist["account"] = account
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
    final_df = final_df[["account", "symbol", "date", "close_price", "dividend"]]

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
