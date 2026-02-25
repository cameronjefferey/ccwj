{{
    config(
        materialized='view'
    )
}}

/*
    Daily stock close prices from the yfinance pipeline
    (current_position_stock_price.py → BigQuery).

    One row per (account, symbol, trading_day).
*/

select
    trim(account)                      as account,
    trim(symbol)                       as symbol,
    cast(date as date)                 as date,
    cast(close_price as float64)       as close_price,
    coalesce(cast(dividend as float64), 0) as dividend
from {{ source('external', 'daily_position_performance') }}
where date is not null
  and symbol is not null
