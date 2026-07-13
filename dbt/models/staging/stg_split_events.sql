{{
    config(
        materialized='view'
    )
}}

/*
    Stock-split corporate actions, sourced from yfinance via
    current_position_stock_price.py.

    One row per (symbol, split_date) when a split actually occurred.
    Symbol-grain (no account/user_id) — splits are corporate actions
    that apply identically to every tenant who held the symbol.

    split_ratio convention (yfinance native): "shares after the split
    that one pre-split share becomes". 2.0 = 2:1 forward, 0.5 = 1:2
    reverse, 0.0333 = 1:30 reverse.

    Defensive existence check: on the very first deploy of the splits
    pipeline, ``daily_split_events`` doesn't exist in BQ until the
    loader has run once. Without this guard pass-1 of the CI build
    fails on a missing relation. ``adapter.get_relation()`` returns
    ``None`` instead of erroring, and we emit an empty-shape result.

    Independent of stg_daily_prices on purpose: int_split_factors
    needs to feed int_equity_sessions / int_closed_equity_legs which
    must build in pass 1 of the CI two-pass workflow. If split data
    came from stg_daily_prices, those models would cascade into
    ``stg_daily_prices+`` and the entire warehouse would move to
    pass 2.
*/

{%- if execute -%}
    {%- set splits_rel = adapter.get_relation(
            database='ccwj-dbt',
            schema='analytics',
            identifier='daily_split_events'
        ) -%}
{%- else -%}
    {%- set splits_rel = none -%}
{%- endif -%}

{% if splits_rel is not none %}

select
    trim(symbol)               as symbol,
    cast(split_date as date)   as split_date,
    cast(split_ratio as float64) as split_ratio
from {{ source('external', 'daily_split_events') }}
where symbol is not null
  and split_date is not null
  and split_ratio is not null
  and split_ratio > 0

{% else %}

-- Empty fallback: relation doesn't exist yet (pre-loader deploy).
-- All downstream split factors will collapse to 1.0 (no adjustment),
-- which is the correct "no splits known" behavior.
-- NOTE: use `limit 0`, NOT `where false` — BigQuery rejects a WHERE on a
-- FROM-less query ("Query without FROM clause cannot have a WHERE clause").
-- A FROM-less SELECT of typed NULLs + `limit 0` returns the empty shape.
select
    cast(null as string)  as symbol,
    cast(null as date)    as split_date,
    cast(null as float64) as split_ratio
limit 0

{% endif %}
