from flask import render_template, flash, redirect, url_for, request
from app import app
from app.bigquery_client import get_bigquery_client, query_bigquery
from google.cloud import bigquery

@app.route('/')
@app.route('/index')
def index():
    user = {'username': 'Miguel'}
    posts = [
        {
            'author': {'username': 'John'},
            'body': 'Beautiful day in Portland!'
        },
        {
            'author': {'username': 'Susan'},
            'body': 'The Avengers movie was so cool!'
        }
    ]
    return render_template('index.html', title='Home', user=user, posts=posts)

@app.route('/accounts')
def accounts():
    client = get_bigquery_client()
    
    # Get the selected account from the URL parameters (e.g., ?account=XYZ)
    selected_account = request.args.get('account')

    queries = {
        "account_dashboard_total": {
            "file": "account_dashboard_total.sql",
            "fields": ["account", "market_value"]
        },
        "account_gains": {
            "file": "account_gains.sql",
            "fields": ["account", "unrealized_gain_or_loss", "realized_gain_or_loss"]
        }
    }

    results = {}
    accounts = set()  # To store all unique account names

    for key, query_info in queries.items():
        query_results = query_bigquery(client, query_info["file"])
        fields = query_info["fields"]

        # Convert query results dynamically based on field count
        data = [tuple(getattr(row, field) for field in fields) for row in query_results]

        # Extract all unique accounts
        accounts.update(row[0] for row in data)

        # If a filter is applied, only keep the matching account
        if selected_account:
            data = [row for row in data if row[0] == selected_account]

        results[key] = data

    return render_template('accounts.html', data=results, accounts=sorted(accounts), selected_account=selected_account)