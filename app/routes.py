# app/routes.py

from flask import render_template, request
from app import app
from app.bigquery_client import get_bigquery_client
from app.utils import read_sql_file
import pandas as pd

@app.route('/')
@app.route('/index')
def index():
    user = {'username': 'Miguel'}
    posts = [
        {"author": {"username": "John"}, "body": "Beautiful day in Portland!"},
        {"author": {"username": "Susan"}, "body": "The Avengers movie was so cool!"}
    ]
    return render_template('index.html', title='Home', user=user, posts=posts)

@app.route("/ping")
def ping():
    return "✅ Flask app is alive"

@app.route("/positions")
def positions():
    client = get_bigquery_client()

    # --- 1) Read account filter from query params ---
    selected_account = request.args.get("account")  # None or a string

    # --- 2) Load current trades ---
    trades_query = read_sql_file("positions_current_position_trades.sql")
    trades_df = client.query(trades_query).to_dataframe()
    trades_df["transaction_date"] = pd.to_datetime(trades_df["transaction_date"])

    # get full list of accounts for dropdown
    accounts = sorted(trades_df["account"].dropna().unique())

    # apply account filter if provided
    if selected_account:
        trades_df = trades_df[trades_df["account"] == selected_account]

    # build trades_data per symbol & record earliest trade date
    trades_data = {}
    earliest_date = {}
    for symbol, grp in trades_df.groupby("symbol"):
        grp_sorted = grp.sort_values("transaction_date", ascending=False)
        trades_data[symbol] = grp_sorted.to_dict(orient="records")
        # record the first (earliest) transaction date as string
        earliest_date[symbol] = grp["transaction_date"].min().strftime("%Y-%m-%d")

    # --- 3) Load daily performance data ---
    chart_query = read_sql_file("positions_daily_performance.sql")
    chart_df = client.query(chart_query).to_dataframe()
    chart_df["day"] = pd.to_datetime(chart_df["day"])

    # apply account filter on chart_df if provided
    if selected_account:
        chart_df = chart_df[chart_df["account"] == selected_account]

    # --- 4) Build chart_data per symbol, trimming early dates ---
    chart_data = {}
    for symbol, start_str in earliest_date.items():
        df_sym = chart_df[chart_df["symbol"] == symbol].copy()
        if df_sym.empty:
            continue
        # keep only dates on or after the first trade
        start_dt = pd.to_datetime(start_str)
        df_sym = df_sym[df_sym["day"] >= start_dt]
        df_sym.sort_values("day", inplace=True)

        # serialize for JS
        chart_data[symbol] = [
            {
                "day": row["day"].strftime("%Y-%m-%d"),
                "security_type": row["security_type"],
                "gain_or_loss": row["gain_or_loss"]
            }
            for _, row in df_sym.iterrows()
        ]

    return render_template(
        "positions.html",
        charts=chart_data,
        trades=trades_data,
        accounts=accounts,
        selected_account=selected_account
    )
