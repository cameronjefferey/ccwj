{{
    config(
        materialized='view'
    )
}}

/*
    Account-level balances — v2.

    Pulls cash + account-total rows from the broker-sync seed
    (``account_balances``) and from the current-positions seed (legacy
    rows). Under v2 the canonical-uid backfill is GONE — every row is
    tenant-stamped at sync time, so split-tenancy can't happen.

    Demo seed union is preserved; demo rows have tenant_id = NULL.
*/
{% if execute %}
    {%- set _curr_cols = adapter.get_columns_in_relation(ref('current_positions')) | map(attribute='name') | list -%}
    {%- set _demo_cols = adapter.get_columns_in_relation(ref('demo_current')) | map(attribute='name') | list -%}
    {%- set _bal_cols  = adapter.get_columns_in_relation(ref('account_balances')) | map(attribute='name') | list -%}
{% else %}
    {%- set _curr_cols = [] -%}
    {%- set _demo_cols = [] -%}
    {%- set _bal_cols  = [] -%}
{% endif %}
{% set _curr_user_id_expr = "cast(user_id as string)" if 'user_id' in _curr_cols else "cast(null as string)" %}
{% set _demo_user_id_expr = "cast(user_id as string)" if 'user_id' in _demo_cols else "cast(null as string)" %}
{% set _bal_user_id_expr  = "cast(user_id as string)" if 'user_id' in _bal_cols  else "cast(null as string)" %}
{% set _curr_tenant_id_expr = "cast(tenant_id as string)" if 'tenant_id' in _curr_cols else "cast(null as string)" %}
{% set _demo_tenant_id_expr = "cast(tenant_id as string)" if 'tenant_id' in _demo_cols else "cast(null as string)" %}
{% set _bal_tenant_id_expr  = "cast(tenant_id as string)" if 'tenant_id' in _bal_cols  else "cast(null as string)" %}

with export_source as (
    select
        cast(account as string) as account,
        {{ _curr_user_id_expr }} as user_id,
        {{ _curr_tenant_id_expr }} as tenant_id,
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
        {{ _demo_tenant_id_expr }} as tenant_id,
        cast(symbol as string) as symbol,
        cast(security_type as string) as security_type,
        cast(market_value as string) as market_value,
        cast(cost_bases as string) as cost_bases,
        cast(gain_or_loss_dollat as string) as gain_or_loss_dollat,
        cast(gain_or_loss_percent as string) as gain_or_loss_percent,
        cast(percent_of_account as string) as percent_of_account
    from {{ ref('demo_current') }}
),

cash_rows as (
    select
        trim(account) as account,
        safe_cast(safe_cast(nullif(trim(user_id), '') as float64) as int64) as user_id,
        nullif(trim(tenant_id), '') as tenant_id,
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
        safe_cast(safe_cast(nullif(trim(user_id), '') as float64) as int64) as user_id,
        nullif(trim(tenant_id), '') as tenant_id,
        'account_total' as row_type,
        safe_cast(trim(replace(replace(market_value, '$', ''), ',', '')) as float64) as market_value,
        safe_cast(trim(replace(replace(cost_bases, '$', ''), ',', '')) as float64) as cost_basis,
        safe_cast(trim(replace(replace(gain_or_loss_dollat, '$', ''), ',', '')) as float64) as unrealized_pnl,
        safe_cast(trim(replace(gain_or_loss_percent, '%', '')) as float64) as unrealized_pnl_pct,
        cast(null as float64) as percent_of_account
    from export_source
    where lower(trim(coalesce(symbol, ''))) in ('account total', 'positions total')
),

broker_bal_rows as (
    select
        trim(cast(account as string)) as account,
        safe_cast(safe_cast(nullif(trim({{ _bal_user_id_expr }}), '') as float64) as int64) as user_id,
        nullif(trim({{ _bal_tenant_id_expr }}), '') as tenant_id,
        case lower(trim(cast(row_type as string)))
            when 'cash' then 'cash'
            when 'account_total' then 'account_total'
        end as row_type,
        safe_cast(trim(replace(replace(replace(cast(market_value as string), '$', ''), ',', ''), ' ', '')) as float64) as market_value,
        safe_cast(trim(replace(replace(replace(cast(cost_basis as string), '$', ''), ',', ''), ' ', '')) as float64) as cost_basis,
        safe_cast(trim(replace(replace(replace(cast(unrealized_pnl as string), '$', ''), ',', ''), ' ', '')) as float64) as unrealized_pnl,
        safe_cast(trim(replace(replace(replace(cast(unrealized_pnl_pct as string), '%', ''), ',', ''), ' ', '')) as float64) as unrealized_pnl_pct,
        safe_cast(trim(replace(replace(cast(percent_of_account as string), '%', ''), ',', '')) as float64) as percent_of_account
    from {{ ref('account_balances') }}
    where trim(coalesce(cast(account as string), '')) != ''
      and lower(trim(coalesce(cast(row_type as string), ''))) in ('cash', 'account_total')
),

unioned as (
    select *, 1 as src_priority from broker_bal_rows
    union all
    select *, 2 as src_priority from cash_rows
    union all
    select *, 2 as src_priority from account_total_rows
),

-- Dedupe on (tenant_id when present, account fallback for demo, row_type).
-- v2 dedups on tenant_id; demo rows (tenant_id = NULL) collapse on
-- (account, row_type) which is fine because demo accounts are unique.
deduped as (
    select * except (src_priority)
    from unioned
    qualify row_number() over (
        partition by coalesce(tenant_id, account), row_type
        order by src_priority,
                 case when market_value is not null then 0 else 1 end
    ) = 1
)

select * from deduped
