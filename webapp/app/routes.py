from flask import render_template, request
from app import app
from app.bigquery_client import get_bigquery_client, query_bigquery

@app.route('/')
@app.route('/index')
def index():
    user = {'username': 'Miguel'}
    posts = [
        {"author": {"username": "John"}, "body": "Beautiful day in Portland!"},
        {"author": {"username": "Susan"}, "body": "The Avengers movie was so cool!"}
    ]
    return render_template('index.html', title='Home', user=user, posts=posts)

@app.route('/accounts')
def accounts():
    client = get_bigquery_client()
    
    # Get filters from URL parameters
    selected_account = request.args.get('account')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # Provide default dates if none are selected
    if not start_date:
        start_date = "2024-01-01"  # Default start date
    if not end_date:
        end_date = "2025-12-31"  # Default end date

    queries = {
        "account_dashboard_total": {
            "file": "account_dashboard_total.sql",
            "fields": ["account", "market_value"]
        },
        "account_gains": {
            "file": "account_gains.sql",
            "fields": ["account", "unrealized_gain_or_loss", "realized_gain_or_loss"]
        },
        "account_current_portfolio": {
            "file": "account_current_portfolio.sql",
            "fields": ["account", "symbol", "strategy", "number_of_shares", "number_of_options", "position_value"]
        }
    }

    results = {}
    accounts = set()  # To store all unique account names

    for key, query_info in queries.items():
        query_results = query_bigquery(client, query_info["file"], start_date, end_date)
        fields = query_info["fields"]

        data = [tuple(getattr(row, field) for field in fields) for row in query_results]
        
        # Extract all unique accounts
        accounts.update([row[0] for row in data])

        # If an account filter is applied, only keep the matching account
        if selected_account:
            data = [row for row in data if row[0] == selected_account]

        results[key] = data

    return render_template('accounts.html', data=results, accounts=sorted(accounts), selected_account=selected_account, start_date=start_date, end_date=end_date)

