# HappyTrader dbt Models

dbt project that transforms raw brokerage trade data into strategy-classified position summaries.

## Model Layers

### Staging (views)
- `stg_history` — Normalizes historical trades (manual `trade_history` ∪ Schwab `schwab_transactions` via `stg_trade_history_seed_union`). Parses dates, actions, option symbols, instrument types.
- `stg_current` — Cleans current positions (`current_positions` ∪ Schwab via `stg_positions_seed_union`). Filters cash/totals, parses option symbols, casts numerics.

### Intermediate (tables)
- `int_equity_sessions` — Detects equity position lifecycles using running share count. Session = one continuous holding period.
- `int_option_contracts` — Groups trades by option contract. Computes premiums, close type, direction, P&L.
- `int_dividends` — Aggregates dividend income by account and symbol.
- `int_strategy_classification` — Tags every trade group with a strategy (Covered Call, CSP, Wheel, Spread, Buy and Hold, etc.)

### Marts (tables)
- `positions_summary` — One row per (account, symbol, strategy) with total P&L, win rate, avg return, duration, premium, dividends, and total return.

## Seeds

| File | Description |
|------|-------------|
| `trade_history.csv` | All historical/closed trades (per-account merge on upload) |
| `current_positions.csv` | Current open positions snapshot (per-account replace on upload) |
| `demo_history.csv` | Demo user history |
| `demo_current.csv` | Demo user current positions |
| `cflt_prices.csv` | Optional price seed |
| `schwab_open_positions.csv` | Schwab API open positions (native columns; merged in dbt) |
| `schwab_account_balances.csv` | Schwab cash + account_total rows for equity snapshots |
| `schwab_transactions.csv` | Schwab API trades (last sync window; native columns) |

## Usage

```bash
dbt seed    # Load CSVs
dbt build   # Build all models
```
