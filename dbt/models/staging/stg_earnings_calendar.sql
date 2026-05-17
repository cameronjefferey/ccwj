{{
    config(
        materialized='view'
    )
}}

-- Next-earnings dates per symbol, sourced from yfinance via
-- scripts/refresh_earnings_calendar.py. Upper/trim the symbol so joins
-- against stg_history / stg_symbol_metadata (which upper/trim too) line
-- up cleanly.
--
-- The loader persists NULL-date rows for symbols with no earnings data
-- (ETFs, delisted, etc.) as a negative cache. We filter those out here
-- so downstream consumers can rely on next_earnings_date being non-null.
--
-- Defensive existence check (same pattern as stg_split_events): on the
-- very first deploy of the earnings pipeline, `earnings_calendar`
-- doesn't exist in BQ until the loader has run once. Without this guard
-- the CI build fails on a missing relation. Empty-shape fallback is
-- safe: downstream views simply find no upcoming earnings for any
-- holding, the UI hides the new sections, and the rest of the warehouse
-- builds normally.

{%- if execute -%}
    {%- set earnings_rel = adapter.get_relation(
            database='ccwj-dbt',
            schema='analytics',
            identifier='earnings_calendar'
        ) -%}
{%- else -%}
    {%- set earnings_rel = none -%}
{%- endif -%}

{% if earnings_rel is not none %}

with src as (
    select * from {{ source('external', 'earnings_calendar') }}
),

cleaned as (
    select
        upper(trim(symbol))                   as symbol,
        cast(next_earnings_date   as date)    as next_earnings_date,
        cast(earnings_window_start as date)   as earnings_window_start,
        cast(earnings_window_end   as date)   as earnings_window_end,
        fetched_at
    from src
    where symbol is not null
      and trim(symbol) != ''
      and next_earnings_date is not null
),

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

{% else %}

-- Empty fallback: relation doesn't exist yet (pre-loader deploy).
select
    cast(null as string)    as symbol,
    cast(null as date)      as next_earnings_date,
    cast(null as date)      as earnings_window_start,
    cast(null as date)      as earnings_window_end,
    cast(null as timestamp) as fetched_at
where false

{% endif %}
