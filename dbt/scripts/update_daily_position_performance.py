from datetime import date
import pandas as pd
import yfinance as yf
from utils.bigquery_client import get_bigquery_client, query_bigquery
from pandas_gbq import to_gbq

# Setup
client = get_bigquery_client()

# Step 1: Query BigQuery for (account, symbol, position_open_date)
positions = query_bigquery(client, "positions_with_open_dates.sql")
positions_df = pd.DataFrame([dict(row.items()) for row in positions])

# Step 2: Fetch daily price/dividend for each symbol
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

# Step 3: Combine and upload
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df = final_df[["account", "symbol", "date", "close_price", "dividend"]]
    
    # Upload to BigQuery
    to_gbq(
        dataframe=final_df,
        destination_table="analytics.daily_position_performance",
        project_id="ccwj-dbt",
        if_exists="replace"  # or "append" if doing this daily
    )
else:
    print("No data retrieved.")
