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
-- Real-broker balance rows now arrive via the per-broker staging adapters
-- (dbt/models/staging/brokers/stg_broker_<slug>_balances), each of which
-- emits THIS broker's rows from BOTH the account_balances seed
-- (src_priority 1) and the legacy current_positions cash/total export
-- (src_priority 2). Demo rows are added separately below because demo is
-- not a broker. See stg_history.sql and dbt/macros/broker_slug_from_account.sql
-- for the add-a-brokerage procedure. The unioned/deduped logic is unchanged.
{% if execute %}
    {%- set _demo_cols = adapter.get_columns_in_relation(ref('demo_current')) | map(attribute='name') | list -%}
{% else %}
    {%- set _demo_cols = [] -%}
{% endif %}
{% set _demo_user_id_expr = "cast(user_id as string)" if 'user_id' in _demo_cols else "cast(null as string)" %}
{% set _demo_tenant_id_expr = "cast(tenant_id as string)" if 'tenant_id' in _demo_cols else "cast(null as string)" %}

with demo_export as (
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

demo_cash_rows as (
    select
        trim(account) as account,
        safe_cast(safe_cast(nullif(trim(user_id), '') as float64) as int64) as user_id,
        nullif(trim(tenant_id), '') as tenant_id,
        'cash' as row_type,
        safe_cast(trim(replace(replace(market_value, '$', ''), ',', '')) as float64) as market_value,
        cast(null as float64) as cost_basis,
        cast(null as float64) as unrealized_pnl,
        cast(null as float64) as unrealized_pnl_pct,
        safe_cast(trim(replace(percent_of_account, '%', '')) as float64) as percent_of_account,
        2 as src_priority
    from demo_export
    where lower(trim(coalesce(security_type, ''))) = 'cash and money market'
),

demo_account_total_rows as (
    select
        trim(account) as account,
        safe_cast(safe_cast(nullif(trim(user_id), '') as float64) as int64) as user_id,
        nullif(trim(tenant_id), '') as tenant_id,
        'account_total' as row_type,
        safe_cast(trim(replace(replace(market_value, '$', ''), ',', '')) as float64) as market_value,
        safe_cast(trim(replace(replace(cost_bases, '$', ''), ',', '')) as float64) as cost_basis,
        safe_cast(trim(replace(replace(gain_or_loss_dollat, '$', ''), ',', '')) as float64) as unrealized_pnl,
        safe_cast(trim(replace(gain_or_loss_percent, '%', '')) as float64) as unrealized_pnl_pct,
        cast(null as float64) as percent_of_account,
        2 as src_priority
    from demo_export
    where lower(trim(coalesce(symbol, ''))) in ('account total', 'positions total')
),

unioned as (
    select * from {{ ref('stg_broker_schwab_balances') }}
    union all
    select * from {{ ref('stg_broker_alpaca_balances') }}
    union all
    select * from {{ ref('stg_broker_fidelity_balances') }}
    union all
    select * from {{ ref('stg_broker_interactive_balances') }}
    union all
    select * from {{ ref('stg_broker_other_balances') }}
    union all
    select * from demo_cash_rows
    union all
    select * from demo_account_total_rows
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
