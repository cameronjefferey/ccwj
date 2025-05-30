import pandas as pd
import yfinance as yf
from datetime import date
from google.cloud import bigquery

# Step 1: Initialize BigQuery client
client = bigquery.Client()

# Step 2: Query BigQuery to get current positions
query = """
    SELECT account, symbol, position_open_date
    FROM `ccwj-dbt.analytics.positions_with_open_dates`
"""
positions = client.query(query).result()
positions_df = pd.DataFrame([dict(row.items()) for row in positions])

# Step 3: Collect daily price & dividend data
today = date.today().isoformat()
all_data = []

for _, row in positions_df.iterrows():
    account = row["account"]
    symbol = row["symbol"]
    start_date = row["position_open_date"].isoformat()

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(start=start_date, end=today)
        hist = hist[["Close", "Dividends"]].reset_index()
        hist["account"] = account
        hist["symbol"] = symbol
        hist.rename(columns={"Date": "date", "Close": "close_price", "Dividends": "dividend"}, inplace=True)
        all_data.append(hist)
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")

# Step 4: Upload results to BigQuery
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df = final_df[["account", "symbol", "date", "close_price", "dividend"]]

    table_id = "ccwj-dbt.analytics.daily_position_performance"
    job = client.load_table_from_dataframe(
        final_df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()  # Waits for the job to complete
    print("Upload successful.")
else:
    print("No data to upload.")
