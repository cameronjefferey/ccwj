-- Latest account balances
SELECT * FROM `ccwj-dbt.analytics.snapshot_account_balances_daily` WHERE dbt_valid_to IS NULL;

-- Latest option positions
SELECT * FROM `ccwj-dbt.analytics.snapshot_options_market_values_daily` WHERE trade_symbol = 'PLTR 01/15/2027 120.00 C' AND dbt_valid_to IS NULL;