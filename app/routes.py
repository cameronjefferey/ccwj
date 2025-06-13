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
    query = read_sql_file("positions_daily_performance.sql")
    df = client.query(query).to_dataframe()

    df['day'] = df['day'].astype(str)
    symbols = df['symbol'].unique()
    sec_types = ['Stock', 'Options Sold']

    chart_data = {}
    for symbol in symbols:
        symbol_df = df[df['symbol'] == symbol]
        symbol_days = sorted(symbol_df['day'].unique())
        chart_rows = []
        for day in symbol_days:
            for sec_type in sec_types:
                match = symbol_df[(symbol_df['day'] == day) & (symbol_df['security_type'] == sec_type)]
                gain = float(match['gain_or_loss'].values[0]) if not match.empty else None
                if gain is not None:
                    chart_rows.append({
                        "day": day,
                        "security_type": sec_type,
                        "gain_or_loss": gain
                    })
        chart_data[symbol] = chart_rows

    return render_template("positions.html", charts=chart_data, security_types=sec_types)