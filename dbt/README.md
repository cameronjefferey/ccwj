# HappyTrader dbt Models

dbt project that transforms raw brokerage trade data into strategy-classified position summaries.

## Model Layers

### Staging (views)
- `stg_history` — Normalizes historical trades from `trade_history` (manual upload and Schwab sync both write here) plus `demo_history`. Parses dates, actions, option symbols, instrument types.
- `stg_current` — Cleans current positions from `current_positions` (same — one seed for both sources) plus `demo_current`. Filters cash/totals, parses option symbols, casts numerics.

### Intermediate (tables)
- `int_equity_sessions` — Detects equity position lifecycles using running share count. Session = one continuous holding period.
- `int_option_contracts` — Groups trades by option contract. Computes premiums, close type, direction, P&L.
- `int_dividends` — Aggregates dividend income by account and symbol.
- `int_strategy_classification` — Tags every trade group with a strategy (Covered Call, CSP, Wheel, Spread, Buy and Hold, etc.)

### Marts (tables)
- `positions_summary` — One row per (account, symbol, strategy) with total P&L, win rate, avg return, duration, premium, dividends, and total return.

## Raw tenant data (dbt SOURCE, not seeds)

Tenant trade data lives in the **BigQuery raw dataset**
`ccwj-dbt.analytics_raw` (dbt source `raw_broker`), written directly by
the app's sync/upload writers via `app/seed_store.py` (Aug 2026
migration — the old `dbt/seeds/*.csv` git-as-database flow is retired;
git history of those CSVs is the archive):

| Table | Description |
|-------|-------------|
| `trade_history` | All historical/closed trades — manual upload **and** broker sync merge here (per-tenant append + dedupe) |
| `current_positions` | Current open positions snapshot — writers replace per tenant |
| `account_balances` | Cash + account_total rows for equity snapshots. Written by any broker connector; no equivalent from manual uploads. |

All columns are STRING; the extra `_row_seq` INT64 column preserves the
app's write order (never selected by staging). Local dev builds read
`analytics_raw_dev` via `DBT_RAW_DATASET` (set by `scripts/dev-refresh.sh`).

## Seeds (static/demo data only)

| File | Description |
|------|-------------|
| `demo_history.csv` | Demo user history |
| `demo_current.csv` | Demo user current positions |
| `cflt_prices.csv` | Optional price seed |
| `crypto_symbols.csv` | Curated crypto symbol whitelist |

## Usage

```bash
dbt seed    # Load static CSVs
dbt build   # Build all models
```
