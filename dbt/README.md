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

## Seeds

| File | Description |
|------|-------------|
| `trade_history.csv` | All historical/closed trades — manual upload **and** Schwab sync merge here (per-account append + dedupe) |
| `current_positions.csv` | Current open positions snapshot — manual upload **and** Schwab sync replace per account |
| `demo_history.csv` | Demo user history |
| `demo_current.csv` | Demo user current positions |
| `cflt_prices.csv` | Optional price seed |
| `schwab_account_balances.csv` | Schwab cash + account_total rows for equity snapshots (Schwab-only; no equivalent from manual uploads) |

## Usage

```bash
dbt seed    # Load CSVs
dbt build   # Build all models
```
