{{
    config(
        materialized='view'
    )
}}

-- Sector / industry per symbol, sourced from yfinance via
-- scripts/refresh_symbol_metadata.py. Trim/upper the symbol so joins
-- against stg_history (which already upper/trims) line up cleanly, and
-- coalesce nulls to 'Unknown' so downstream filters don't have to
-- special-case nulls everywhere.

with src as (
    select * from {{ source('external', 'symbol_metadata') }}
),

cleaned as (
    select
        upper(trim(symbol))                         as symbol,
        coalesce(nullif(trim(sector), ''),  'Unknown') as sector,
        coalesce(nullif(trim(industry), ''),'Unknown') as industry,
        coalesce(nullif(trim(industry_group), ''), industry, 'Unknown') as industry_group,
        coalesce(nullif(trim(country), ''), 'Unknown') as country,
        market_cap,
        long_name,
        fetched_at
    from src
    where symbol is not null
      and trim(symbol) != ''
),

-- One row per symbol — keep the most recently fetched if duplicates exist
deduped as (
    select * except (rn) from (
        select
            *,
            row_number() over (
                partition by symbol
                order by fetched_at desc nulls last
            ) as rn
        from cleaned
    )
    where rn = 1
)

select * from deduped
