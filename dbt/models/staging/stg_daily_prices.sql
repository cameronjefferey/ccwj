{{
    config(
        materialized='view'
    )
}}

/*
    Daily stock close prices from the yfinance pipeline
    (current_position_stock_price.py → BigQuery).

    One row per (account, user_id, symbol, trading_day). user_id is
    NULL on legacy rows written before the user_id-tenancy migration —
    see docs/USER_ID_TENANCY.md. The pipeline rebuilds this source
    table with WRITE_TRUNCATE on every cron run, so the user_id column
    appears the first time the new loader runs. Until then we use a
    defensive check so dbt build doesn't fail mid-deploy.
*/

{%- set src = source('external', 'daily_position_performance') -%}
{%- if execute -%}
    {%- set src_cols = adapter.get_columns_in_relation(src) | map(attribute='name') | list -%}
{%- else -%}
    {%- set src_cols = [] -%}
{%- endif -%}

select
    trim(account)                      as account,
    {% if 'user_id' in src_cols -%}
    safe_cast(user_id as int64)        as user_id,
    {%- else -%}
    cast(null as int64)                as user_id,
    {%- endif %}
    trim(symbol)                       as symbol,
    cast(date as date)                 as date,
    cast(close_price as float64)       as close_price,
    coalesce(cast(dividend as float64), 0) as dividend
from {{ source('external', 'daily_position_performance') }}
where date is not null
  and symbol is not null
