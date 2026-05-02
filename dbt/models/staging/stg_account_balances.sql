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

with export_source as (
    -- Cast every column we touch to STRING so this model is resilient to the
    -- seed being empty (BigQuery re-infers column types per load; an empty
    -- current_positions seed ends up with different types than demo_current,
    -- which breaks UNION ALL). stg_current does the same via a macro.
    select
        cast(account as string) as account,
        cast(symbol as string) as symbol,
        cast(security_type as string) as security_type,
        cast(market_value as string) as market_value,
        cast(cost_bases as string) as cost_bases,
        cast(gain_or_loss_dollat as string) as gain_or_loss_dollat,
        cast(gain_or_loss_percent as string) as gain_or_loss_percent,
        cast(percent_of_account as string) as percent_of_account
    from {{ ref('current_positions') }}
    union all
    select
        cast(account as string) as account,
        cast(symbol as string) as symbol,
        cast(security_type as string) as security_type,
        cast(market_value as string) as market_value,
        cast(cost_bases as string) as cost_bases,
        cast(gain_or_loss_dollat as string) as gain_or_loss_dollat,
        cast(gain_or_loss_percent as string) as gain_or_loss_percent,
        cast(percent_of_account as string) as percent_of_account
    from {{ ref('demo_current') }}
),

-- Schwab CSV exports format dollar columns as `$1,234.56` (with $ and commas)
-- and pct columns as `5.54%`. Strip those before safe_cast so the demo seed
-- — which uses the export-style quoting — produces real numbers, not NULLs.
cash_rows as (
    select
        trim(account) as account,
        'cash' as row_type,
        safe_cast(trim(replace(replace(market_value, '$', ''), ',', '')) as float64) as market_value,
        cast(null as float64) as cost_basis,
        cast(null as float64) as unrealized_pnl,
        cast(null as float64) as unrealized_pnl_pct,
        safe_cast(trim(replace(percent_of_account, '%', '')) as float64) as percent_of_account
    from export_source
    where lower(trim(coalesce(security_type, ''))) = 'cash and money market'
),

account_total_rows as (
    select
        trim(account) as account,
        'account_total' as row_type,
        safe_cast(trim(replace(replace(market_value, '$', ''), ',', '')) as float64) as market_value,
        safe_cast(trim(replace(replace(cost_bases, '$', ''), ',', '')) as float64) as cost_basis,
        safe_cast(trim(replace(replace(gain_or_loss_dollat, '$', ''), ',', '')) as float64) as unrealized_pnl,
        safe_cast(trim(replace(gain_or_loss_percent, '%', '')) as float64) as unrealized_pnl_pct,
        cast(null as float64) as percent_of_account
    from export_source
    where lower(trim(coalesce(symbol, ''))) in ('account total', 'positions total')
),

schwab_bal_rows as (
    select
        trim(cast(account as string)) as account,
        case lower(trim(cast(row_type as string)))
            when 'cash' then 'cash'
            when 'account_total' then 'account_total'
        end as row_type,
        safe_cast(trim(replace(replace(replace(cast(market_value as string), '$', ''), ',', ''), ' ', '')) as float64) as market_value,
        safe_cast(trim(replace(replace(replace(cast(cost_basis as string), '$', ''), ',', ''), ' ', '')) as float64) as cost_basis,
        safe_cast(trim(replace(replace(replace(cast(unrealized_pnl as string), '$', ''), ',', ''), ' ', '')) as float64) as unrealized_pnl,
        safe_cast(trim(replace(replace(replace(cast(unrealized_pnl_pct as string), '%', ''), ',', ''), ' ', '')) as float64) as unrealized_pnl_pct,
        safe_cast(trim(replace(replace(cast(percent_of_account as string), '%', ''), ',', '')) as float64) as percent_of_account
    from {{ ref('schwab_account_balances') }}
    where trim(coalesce(cast(account as string), '')) != ''
      and lower(trim(coalesce(cast(row_type as string), ''))) in ('cash', 'account_total')
)

select * from cash_rows
union all
select * from account_total_rows
union all
select * from schwab_bal_rows
