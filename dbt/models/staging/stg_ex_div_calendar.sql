{{
    config(
        materialized='view'
    )
}}

-- Next ex-dividend (and pay) dates per symbol, sourced from yfinance
-- Ticker.calendar via scripts/refresh_earnings_calendar.py (same table
-- as earnings — the calendar payload already carries both). Upper/trim
-- the symbol so joins against holdings / stg_symbol_metadata line up.
--
-- The loader persists NULL-date rows as a negative cache. We filter
-- those out here so downstream can rely on next_ex_div_date being
-- non-null. Earnings-only rows (no dividend) stay out of this view;
-- stg_earnings_calendar still owns the earnings-date filter.
--
-- Column-existence guard: the first warehouse build after this model
-- ships may run against an earnings_calendar table that does not yet
-- have next_ex_div_date (loader hasn't written the new schema). Empty
-- fallback keeps the build green; Daily Review keeps the cadence
-- heuristic until the next loader run.

{%- if execute -%}
    {%- set cal_rel = adapter.get_relation(
            database='ccwj-dbt',
            schema='analytics',
            identifier='earnings_calendar'
        ) -%}
    {%- set has_ex_div_col = false -%}
    {%- if cal_rel is not none -%}
        {%- set col_names = adapter.get_columns_in_relation(cal_rel)
            | map(attribute='name') | map('lower') | list -%}
        {%- set has_ex_div_col = 'next_ex_div_date' in col_names -%}
    {%- endif -%}
{%- else -%}
    {%- set has_ex_div_col = false -%}
{%- endif -%}

{% if has_ex_div_col %}

with src as (
    select * from {{ source('external', 'earnings_calendar') }}
),

cleaned as (
    select
        upper(trim(symbol))                     as symbol,
        cast(next_ex_div_date        as date)   as next_ex_div_date,
        cast(next_dividend_pay_date  as date)   as next_dividend_pay_date,
        fetched_at
    from src
    where symbol is not null
      and trim(symbol) != ''
      and next_ex_div_date is not null
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

select
    cast(null as string)    as symbol,
    cast(null as date)      as next_ex_div_date,
    cast(null as date)      as next_dividend_pay_date,
    cast(null as timestamp) as fetched_at
where false

{% endif %}
