#!/usr/bin/env bash
# Full local refresh pipeline — mirrors what GitHub Actions does after an upload.
#
# Order:
#   1. git pull       — bring down seed CSVs committed by the web app upload
#   2. dbt build      — rebuild everything downstream of trade_history + current_positions
#   3. python prices  — fetch end-of-day stock prices via yfinance → BigQuery
#   4. dbt build      — rebuild only models downstream of stg_daily_prices
#
# Usage:
#   ./refresh.sh            — full pipeline
#   ./refresh.sh --prices   — skip git pull + pass 1 (prices + downstream only)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBT_DIR="$SCRIPT_DIR/dbt"

# Activate the venv (dbt + app deps)
# shellcheck disable=SC1091
source "$DBT_DIR/.venv/bin/activate"

if [[ "${1:-}" != "--prices" ]]; then
  echo "==> Step 1: git pull (fetch latest uploaded seed CSVs)"
  cd "$SCRIPT_DIR"
  git pull

  echo "==> Step 2: dbt build (seeds + trade history + current positions downstream)"
  cd "$DBT_DIR"
  dbt build --select "+stg_history+" "+stg_current+" "+stg_account_balances+"
  cd "$SCRIPT_DIR"
fi

echo "==> Step 3: fetch daily stock prices"
python "$SCRIPT_DIR/current_position_stock_price.py"

echo "==> Step 4: dbt build (prices downstream only)"
cd "$DBT_DIR"
dbt build --select "stg_daily_prices+"

echo "==> Done."
