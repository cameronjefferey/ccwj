from flask import render_template, request
from app import app
from app.bigquery_client import get_bigquery_client, query_bigquery
from collections import defaultdict
from app.utils import read_sql_file
from datetime import datetime

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

    # Load daily performance data
    chart_query = read_sql_file("positions_daily_performance.sql")
    df = client.query(chart_query).to_dataframe()
    df['day'] = df['day'].astype(str)

    # Build per-symbol chart data with only relevant days
    chart_data = {}
    symbols = df['symbol'].unique()

    for symbol in symbols:
        symbol_df = df[df['symbol'] == symbol].copy()
        symbol_df.sort_values("day", inplace=True)
        rows = [
            {
                "day": row["day"],
                "security_type": row["security_type"],
                "gain_or_loss": row["gain_or_loss"]
            }
            for _, row in symbol_df.iterrows()
        ]
        chart_data[symbol] = rows

    # Load and group current position trades
    trades_query = read_sql_file("positions_current_position_trades.sql")
    trades_df = client.query(trades_query).to_dataframe()
    trades_df['transaction_date'] = trades_df['transaction_date'].astype(str)

    trades_data = {}
    for symbol in trades_df['symbol'].unique():
        symbol_trades = trades_df[trades_df['symbol'] == symbol].copy()
        symbol_trades.sort_values("transaction_date", ascending=False, inplace=True)
        trades_data[symbol] = [
            row._asdict() for row in symbol_trades.itertuples(index=False)
        ]

    return render_template("positions.html", charts=chart_data, trades=trades_data)
