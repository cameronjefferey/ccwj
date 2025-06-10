from flask import render_template, request
from app import app
from app.bigquery_client import get_bigquery_client, query_bigquery
from collections import defaultdict

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

@app.route('/strategy')
def strategy():
    client = get_bigquery_client()

    selected_account = request.args.get('account')
    selected_symbol = request.args.get('symbol')
    selected_strategy = request.args.get('strategy')

    queries = {
        "strategy_option_strategy_outcome": {
            "file": "strategy_option_strategy_outcome.sql",
            "fields": ["account", "symbol", "strategy", "strategy_outcome"]
        },
        "strategy_option_position_strategy_outcome": {
            "file": "strategy_option_position_strategy_outcome.sql",
            "fields": ["account", "symbol", "trade_symbol", "trade_outcome", "strategy"]
        }
    }

    results = {}
    accounts = set()
    symbols = set()
    strategies = set()

    for key, query_info in queries.items():
        query_results = query_bigquery(client, query_info["file"])
        fields = query_info["fields"]

        data = [tuple(getattr(row, field, None) for field in fields) for row in query_results]

        # Register filters
        if "account" in fields:
            accounts.update([row[0] for row in data if row[0]])

        if "symbol" in fields:
            symbol_index = fields.index("symbol")
            symbols.update([row[symbol_index] for row in data if row[symbol_index]])

        if "strategy" in fields:
            strategy_index = fields.index("strategy")
            strategies.update([row[strategy_index] for row in data if row[strategy_index]])

        # Apply filters
        if selected_account:
            data = [row for row in data if row[0] == selected_account]
        if selected_symbol and "symbol" in fields:
            data = [row for row in data if row[symbol_index] == selected_symbol]
        if selected_strategy and "strategy" in fields:
            data = [row for row in data if row[strategy_index] == selected_strategy]

        # Drop symbol only for the strategy_option_strategy_outcome table
        

        if key == "strategy_option_strategy_outcome":
            # Group by (account, strategy) and sum strategy_outcome
            grouped = defaultdict(float)
            for row in data:
                account = row[0]
                strategy = row[2]
                outcome = float(row[3]) if row[3] is not None else 0
                grouped[(account, strategy)] += outcome

            display_fields = ["account", "strategy", "strategy_outcome"]
            display_data = [(acct, strat, outcome) for (acct, strat), outcome in grouped.items()]

        else:
            display_fields = fields
            display_data = data

        results[key] = {
            "headers": display_fields,
            "rows": display_data,
            "raw": data
        }

    return render_template(
        "strategy.html",
        data=results,
        accounts=sorted(accounts),
        symbols=sorted(symbols),
        strategies=sorted(strategies),
        selected_account=selected_account,
        selected_symbol=selected_symbol,
        selected_strategy=selected_strategy
    )
