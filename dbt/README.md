# 📈 HappyTrader Web Dashboard

A Flask-based portfolio analytics app integrating BigQuery, Chart.js, and dynamic equity/trade data for active traders.

---

## 🔧 Tech Stack

* **Backend Framework:** Flask (Python)
* **Frontend:** HTML + Bootstrap 5 (Jinja templating)
* **Data Layer:** Google BigQuery (via Python BigQuery client)
* **Charting:** Chart.js (via embedded JavaScript)
* **Data Sources:**

  * Internal SQL files for trades and performance
  * External data via yfinance (for earnings date)

---

## 🗂️ Features Overview

### 📊 Positions Dashboard

* Dynamic accordion UI (per stock symbol)
* Includes:

  * Account filter (dropdown)
  * Equity summary table (symbol, security type, gain/loss)
  * Trade history table
  * Interactive daily performance chart

### ⚙️ Chart Engine

* Chart.js with:

  * Smooth lines (tension 0.4)
  * Security type grouping (Stock, Option, Dividend, etc.)
  * Custom color map

### 📁 SQL-Driven Data Model

* Modular SQL files loaded dynamically:

  * `positions_current_position_trades.sql`
  * `positions_daily_performance.sql`
  * `positions_current_equity_trades.sql`
* Fully filterable by account
* Supports joins, filters, and date logic

### 📈 Earnings Model (Optional)

* Uses `yfinance` to fetch upcoming earnings dates
* Generates: `{ symbol, next_earnings_date }`
* Output: display, CSV, or join to other models

---

## 🔐 Best Practices & Design Patterns

* Clear separation of concerns
* Modular, reusable SQL
* Jinja templating for server-side rendering
* Secure rendering with Jinja filters
* REST-style routes and input filtering

---

## 🚀 Deployment & Tooling

* Runs on Flask/Gunicorn stack
* Compatible with local and cloud BigQuery auth
* No ORM required — runs pure SQL

---

## 📚 Project Structure

```
app/
├── routes.py                   # Flask routes
├── templates/
│   ├── base.html               # Bootstrap base template
│   └── positions.html          # Main dashboard
├── static/                     # Custom CSS/JS (optional)
├── utils.py                    # Helper functions (e.g., SQL reader)
├── bigquery_client.py         # BQ client factory
├── *.sql                       # Modular queries
```

---

## 💡 Skills Demonstrated

* Python Flask app development
* Jinja + Bootstrap dynamic UIs
* Google BigQuery SQL modeling
* JavaScript chart rendering with Chart.js
* Data filtering, transformation, and JSON serialization
* Optional API integration (e.g. yfinance)

---

## 🌐 Route Endpoints

* `/positions` — main dashboard
* `/ping` — health check
* `/index` — landing page

---

## 📎 Resume Reference

**Project:** HappyTrader Web Dashboard
**Role:** Full-Stack Developer
**Stack:** Flask · BigQuery · Chart.js · Python · Jinja2 · SQL
**Live Demo / GitHub:** *\[Add your link here]*
