def model(dbt, session):
    import pandas as pd
    import yfinance as yf
    from datetime import date

    # Configure the model
    dbt.config(
        materialized="table",
        schema="analytics"
    )

    # Reference the positions_with_open_dates model
    positions_df = dbt.ref("positions_with_open_dates").to_pandas()

    # Initialize an empty list to collect data
    all_data = []

    # Get today's date
    today = date.today().isoformat()

    # Iterate over each position to fetch historical data
    for _, row in positions_df.iterrows():
        account = row["account"]
        symbol = row["symbol"]
        start_date = row["position_open_date"].isoformat()

        try:
            # Fetch historical data from Yahoo Finance
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=start_date, end=today)
            hist = hist[["Close", "Dividends"]].reset_index()
            hist["account"] = account
            hist["symbol"] = symbol
            hist.rename(columns={"Date": "date", "Close": "close_price", "Dividends": "dividend"}, inplace=True)
            all_data.append(hist)
        except Exception as e:
            dbt.log(f"Error fetching data for {symbol}: {e}")

    # Combine all data into a single DataFrame
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df[["account", "symbol", "date", "close_price", "dividend"]]
    else:
        # Return an empty DataFrame with the expected schema
        final_df = pd.DataFrame(columns=["account", "symbol", "date", "close_price", "dividend"])

    return final_df
