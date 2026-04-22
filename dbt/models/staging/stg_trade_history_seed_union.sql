{{
    config(
        materialized='view'
    )
}}

/*
  Union export trade_history with Schwab transactions. Normalize the export
  side to STRING so types match the Schwab branch (BQ often infers INT64 /
  FLOAT64 from CSV while Schwab outputs STRING).
*/

select
    cast(Account as string) as Account,
    cast(Date as string) as Date,
    cast(Action as string) as Action,
    cast(Symbol as string) as Symbol,
    cast(Description as string) as Description,
    cast(Quantity as string) as Quantity,
    cast(Price as string) as Price,
    cast(fees_and_comm as string) as fees_and_comm,
    cast(Amount as string) as Amount
from {{ ref('trade_history') }}
union all
select * from {{ ref('stg_schwab_transactions_as_brokerage') }}
