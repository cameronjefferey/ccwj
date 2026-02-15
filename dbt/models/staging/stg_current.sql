{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ ref('0417_current') }}
    union all
    select * from {{ ref('demo_current') }}
),

cleaned as (
    select
        trim(account) as account,

        -- Full trade symbol
        trim(symbol) as trade_symbol,

        -- Underlying ticker
        trim(split(trim(symbol), ' ')[safe_offset(0)]) as underlying_symbol,

        -- Option components
        safe.parse_date('%m/%d/%Y', split(trim(symbol), ' ')[safe_offset(1)]) as option_expiry,
        safe_cast(split(trim(symbol), ' ')[safe_offset(2)] as float64) as option_strike,
        split(trim(symbol), ' ')[safe_offset(3)] as option_type,

        -- Instrument type
        case
            when split(trim(symbol), ' ')[safe_offset(3)] = 'C' then 'Call'
            when split(trim(symbol), ' ')[safe_offset(3)] = 'P' then 'Put'
            when lower(trim(coalesce(security_type, ''))) in ('equity', 'etfs & closed end funds') then 'Equity'
            else 'Other'
        end as instrument_type,

        trim(description) as description,
        safe_cast(quantity as float64) as quantity,
        safe_cast(price as float64) as current_price,
        safe_cast(market_value as float64) as market_value,
        safe_cast(cost_bases as float64) as cost_basis,
        safe_cast(gain_or_loss_dollat as float64) as unrealized_pnl,       -- note: typo in source column name
        safe_cast(gain_or_loss_percent as float64) as unrealized_pnl_pct,
        trim(security_type) as security_type_raw,
        trim(in_the_money) as in_the_money,
        safe_cast(dividend_yield as float64) as dividend_yield,
        safe_cast(pe_ratio as float64) as pe_ratio,
        current_date() as snapshot_date

    from source
    where lower(trim(coalesce(security_type, ''))) not in ('cash and money market', '')
      and lower(trim(coalesce(symbol, ''))) not like '%account total%'
)

select * from cleaned
