{{
    config(
        materialized='view'
    )
}}

-- Sector / subsector per symbol, sourced from yfinance via
-- scripts/refresh_symbol_metadata.py. Trim/upper the symbol so joins
-- against stg_history (which already upper/trims) line up cleanly, and
-- coalesce nulls to 'Unknown' so downstream filters don't have to
-- special-case nulls everywhere.
--
-- "Subsector" was previously called "industry" to match yfinance's API
-- field name; we standardized on the finance-industry term "subsector"
-- (sector → subsector hierarchy). refresh_symbol_metadata.py runs
-- BEFORE the dbt build in CI (and locally) and overwrites the source
-- table with WRITE_TRUNCATE, so by the time this view is rebuilt the
-- new column name is in place.

with src as (
    select * from {{ source('external', 'symbol_metadata') }}
),

cleaned as (
    select
        upper(trim(symbol))                         as symbol,
        coalesce(nullif(trim(sector), ''),    'Unknown') as sector,
        coalesce(nullif(trim(subsector), ''), 'Unknown') as subsector,
        coalesce(nullif(trim(country), ''),   'Unknown') as country,
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
