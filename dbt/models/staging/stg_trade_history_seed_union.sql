{{
    config(
        materialized='view'
    )
}}

select * from {{ ref('trade_history') }}
union all
select * from {{ ref('stg_schwab_transactions_as_brokerage') }}
