# HappyTrader

A portfolio analytics platform for options traders. Automatically classifies trading strategies (Covered Calls, Cash-Secured Puts, Wheels, Spreads, etc.) and tracks performance metrics across accounts and symbols.

## Tech Stack

- **Data Pipeline:** dbt + Google BigQuery
- **Web App:** Flask + Bootstrap 5 + Chart.js
- **Language:** Python 3.11, SQL (BigQuery)

## Features

- **Strategy auto-detection** — Classifies trades into Covered Call, Cash-Secured Put, Wheel, Buy and Hold, Long/Short options, Spreads, and more based on trade history
- **Performance metrics** — Total P&L, realized/unrealized breakdown, win rate, average return, duration, premium collected, dividend income
- **Multi-account** — Filter and compare across brokerage accounts
- **Positions dashboard** — KPI cards, strategy P&L chart, sortable/searchable data table with color-coded badges

## Project Structure

```
app/
├── routes.py                  # Flask routes (/, /positions, /ping)
├── bigquery_client.py         # BigQuery client (OAuth + service account)
├── templates/
│   ├── base.html              # Base layout (dark nav, Bootstrap 5)
│   ├── index.html             # Landing page
│   └── positions.html         # Positions dashboard
└── utils.py                   # Helper functions

dbt/
├── models/
│   ├── staging/
│   │   ├── stg_history.sql    # Clean/normalize historical trades
│   │   └── stg_current.sql    # Clean/normalize current positions
│   ├── intermediate/
│   │   ├── int_equity_sessions.sql          # Equity buy/sell lifecycle tracking
│   │   ├── int_option_contracts.sql         # Option contract P&L aggregation
│   │   ├── int_dividends.sql                # Dividend income by symbol
│   │   └── int_strategy_classification.sql  # Strategy tagging (CC, CSP, Wheel, etc.)
│   └── marts/
│       └── positions_summary.sql            # Final dashboard-ready summary
├── seeds/
│   ├── 0417_history.csv       # Historical trades (7,500+ rows)
│   └── 0417_current.csv       # Current open positions
└── dbt_project.yml
```

## Data Flow

```
Seeds (CSV)
  → Staging (clean/parse)
    → Intermediate (sessions, contracts, strategy classification)
      → Mart (positions_summary)
        → Flask dashboard (BigQuery query → Bootstrap UI)
```

## Setup

### Prerequisites

- Python 3.11+
- Google Cloud account with BigQuery enabled
- `gcloud` CLI

### Install

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### BigQuery Auth

```bash
gcloud auth application-default login
```

### Run dbt

```bash
cd dbt
dbt seed    # Load CSVs into BigQuery
dbt build   # Build all models
```

### Run the App

```bash
python -m flask run
```

Open [http://127.0.0.1:5000/positions](http://127.0.0.1:5000/positions)

## Routes

| Route | Description |
|-------|-------------|
| `/` | Landing page |
| `/positions` | Positions dashboard (filterable by account, strategy, status) |
| `/ping` | Health check |

## Strategy Classification

| Strategy | Detection Logic |
|----------|----------------|
| Covered Call | Sold call while holding underlying equity |
| Cash-Secured Put | Sold put without holding equity |
| Wheel | Put assigned → equity acquired → (optional covered calls) |
| Buy and Hold | Equity position with no associated options |
| Call/Put Spread | Bought + sold same type, same expiry, different strikes |
| Long Call/Put | Bought option standalone |
| Protective Put | Bought put while holding equity |
| Naked Call | Sold call without equity |
