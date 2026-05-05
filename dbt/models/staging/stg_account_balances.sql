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

    ``user_id`` is the new tenant key (see ``docs/USER_ID_TENANCY.md``).
    Detected via ``adapter.get_columns_in_relation`` so this model keeps
    building during the deploy gap when the BQ seed table hasn't been
    rewritten with the new schema yet (e.g. dbt-bigquery's seed loader
    silently dropping the all-empty user_id column on first deploy).
*/
{%- if execute -%}
    {%- set _curr_cols = adapter.get_columns_in_relation(ref('current_positions')) | map(attribute='name') | list -%}
    {%- set _demo_cols = adapter.get_columns_in_relation(ref('demo_current')) | map(attribute='name') | list -%}
    {%- set _bal_cols  = adapter.get_columns_in_relation(ref('schwab_account_balances')) | map(attribute='name') | list -%}
{%- else -%}
    {%- set _curr_cols = [] -%}
    {%- set _demo_cols = [] -%}
    {%- set _bal_cols  = [] -%}
{%- endif -%}
{%- set _curr_user_id_expr = "cast(user_id as string)" if 'user_id' in _curr_cols else "cast(null as string)" -%}
{%- set _demo_user_id_expr = "cast(user_id as string)" if 'user_id' in _demo_cols else "cast(null as string)" -%}
{%- set _bal_user_id_expr  = "cast(user_id as string)" if 'user_id' in _bal_cols  else "cast(null as string)" -%}

with export_source as (
    -- Cast every column we touch to STRING so this model is resilient to the
    -- seed being empty (BigQuery re-infers column types per load; an empty
    -- current_positions seed ends up with different types than demo_current,
    -- which breaks UNION ALL). stg_current does the same via a macro.
    select
        cast(account as string) as account,
        {{ _curr_user_id_expr }} as user_id,
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
        {{ _demo_user_id_expr }} as user_id,
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
        safe_cast(nullif(trim(user_id), '') as int64) as user_id,
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
        safe_cast(nullif(trim(user_id), '') as int64) as user_id,
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
        safe_cast(nullif(trim({{ _bal_user_id_expr }}), '') as int64) as user_id,
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
