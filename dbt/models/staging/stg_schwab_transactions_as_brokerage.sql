{{
    config(
        materialized='view'
    )
}}

/*
  Map Schwab API-shaped transactions (schwab_transactions seed) into the
  trade_history.csv column layout for stg_history.
*/

select
    trim(account) as Account,
    trim(transaction_date) as Date,
    trim(action) as Action,
    trim(symbol) as Symbol,
    trim(description) as Description,
    cast(safe_cast(quantity as float64) as string) as Quantity,
    cast(safe_cast(price as float64) as string) as Price,
    trim(coalesce(cast(fees as string), '')) as fees_and_comm,
    trim(coalesce(cast(amount as string), '')) as Amount
from {{ ref('schwab_transactions') }}
where trim(coalesce(account, '')) != ''
