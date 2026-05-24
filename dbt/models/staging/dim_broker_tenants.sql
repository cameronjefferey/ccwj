{{
    config(
        materialized='table'
    )
}}

-- v2 broker-tenant dimension — see docs/V2_TENANT_KEY_DESIGN.md.
--
-- One row per ``tenant_id`` observed across the three user-tied seeds
-- (trade_history, current_positions, account_balances). Sourced from
-- the RAW seeds rather than the staging models so we don't depend on
-- the canonicalization-free v2 staging being already built.
--
-- This dim is the BigQuery analogue of the Postgres ``broker_tenants``
-- table — Flask reads ``broker_tenants`` to know "what tenant_ids does
-- this user own", BigQuery reads ``dim_broker_tenants`` to know "what
-- account_name / user_id labels go with this tenant_id" for display.
--
-- See docs/V2_TENANT_KEY_DESIGN.md for the rationale behind keying
-- on a broker-stable ``tenant_id`` instead of a Postgres SERIAL.

{% if execute %}
    {%- set _hist_cols = adapter.get_columns_in_relation(ref('trade_history')) | map(attribute='name') | list -%}
    {%- set _curr_cols = adapter.get_columns_in_relation(ref('current_positions')) | map(attribute='name') | list -%}
    {%- set _bal_cols  = adapter.get_columns_in_relation(ref('account_balances')) | map(attribute='name') | list -%}
{% else %}
    {%- set _hist_cols = [] -%}
    {%- set _curr_cols = [] -%}
    {%- set _bal_cols  = [] -%}
{% endif %}

{%- set _hist_tenant_expr = "cast(tenant_id as string)" if 'tenant_id' in _hist_cols else "cast(null as string)" -%}
{%- set _curr_tenant_expr = "cast(tenant_id as string)" if 'tenant_id' in _curr_cols else "cast(null as string)" -%}
{%- set _bal_tenant_expr  = "cast(tenant_id as string)" if 'tenant_id' in _bal_cols  else "cast(null as string)" -%}
{%- set _hist_user_id_expr = "cast(user_id as string)" if 'user_id' in _hist_cols else "cast(null as string)" -%}
{%- set _curr_user_id_expr = "cast(user_id as string)" if 'user_id' in _curr_cols else "cast(null as string)" -%}
{%- set _bal_user_id_expr  = "cast(user_id as string)" if 'user_id' in _bal_cols  else "cast(null as string)" -%}

with raw_tenants as (
    select
        {{ _hist_tenant_expr }} as tenant_id,
        {{ _hist_user_id_expr }} as user_id,
        cast(Account as string) as account_name
    from {{ ref('trade_history') }}

    union all

    select
        {{ _curr_tenant_expr }} as tenant_id,
        {{ _curr_user_id_expr }} as user_id,
        cast(Account as string) as account_name
    from {{ ref('current_positions') }}

    union all

    select
        {{ _bal_tenant_expr }} as tenant_id,
        {{ _bal_user_id_expr }} as user_id,
        cast(account as string) as account_name
    from {{ ref('account_balances') }}
),

cleaned as (
    select
        nullif(trim(tenant_id), '') as tenant_id,
        safe_cast(safe_cast(nullif(trim(user_id), '') as float64) as int64) as user_id,
        trim(account_name) as account_name
    from raw_tenants
)

select
    tenant_id,
    any_value(user_id) as user_id,
    any_value(account_name) as account_name,
    split(tenant_id, ':')[safe_offset(0)] as broker_slug,
    substr(tenant_id, length(split(tenant_id, ':')[safe_offset(0)]) + 2) as broker_uuid,
    count(*) as source_row_count
from cleaned
where tenant_id is not null
  and account_name is not null
  and account_name != ''
group by tenant_id
