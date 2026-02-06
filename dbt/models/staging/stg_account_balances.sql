{{
    config(
        materialized='view'
    )
}}

/*
    Account-level balances extracted from the current positions snapshot.

    Pulls the rows that stg_current intentionally filters out:
      - Cash & Money Market balances
      - Account Total summary rows
*/

with source as (
    select * from {{ ref('0417_current') }}
),

cash_rows as (
    select
        trim(account) as account,
        'cash' as row_type,
        safe_cast(market_value as float64) as market_value,
        cast(null as float64) as cost_basis,
        cast(null as float64) as unrealized_pnl,
        cast(null as float64) as unrealized_pnl_pct,
        safe_cast(percent_of_account as float64) as percent_of_account
    from source
    where lower(trim(coalesce(security_type, ''))) = 'cash and money market'
),

account_total_rows as (
    select
        trim(account) as account,
        'account_total' as row_type,
        safe_cast(market_value as float64) as market_value,
        safe_cast(cost_bases as float64) as cost_basis,
        safe_cast(gain_or_loss_dollat as float64) as unrealized_pnl,
        safe_cast(gain_or_loss_percent as float64) as unrealized_pnl_pct,
        cast(null as float64) as percent_of_account
    from source
    where lower(trim(coalesce(symbol, ''))) = 'account total'
)

select * from cash_rows
union all
select * from account_total_rows
