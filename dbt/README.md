# HappyTrader dbt Models

dbt project that transforms raw brokerage trade data into strategy-classified position summaries.

## Model Layers

### Staging (views)
- `stg_history` — Normalizes 7,500+ historical trades. Parses dates, actions, option symbols, instrument types.
- `stg_current` — Cleans current positions snapshot. Filters cash/totals, parses option symbols, casts numerics.

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
| `0417_history.csv` | All historical/closed trades |
| `0417_current.csv` | Current open positions snapshot |

## Usage

```bash
dbt seed    # Load CSVs
dbt build   # Build all models
```
