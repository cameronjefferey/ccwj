{{
    config(
        materialized='view'
    )
}}

/*
    Public demo account balances.

    TEMPORARY: while var('demo_temp_seed') is true, cash is the sum of
    the temp-seed fills and account_total adds today's position marks
    from stg_current. That is the same arithmetic as the last day of
    int_demo_temp_equity_daily once closes match the seed prices.
    Removal steps: dbt/seeds/DEMO_TEMP_SEED.md.

    The else branch is the bot mirror. See stg_demo_history.sql.
    Column order/types match broker_balances_rows() / stg_account_balances's
    `unioned` CTE exactly, including the trailing `src_priority`.
*/

{% if var('demo_temp_seed', false) %}

with cash as (
    select coalesce(sum(amount), 0) as cash_value
    from {{ ref('stg_history') }}
    where tenant_id = 'demo:demo-account'
),

positions as (
    select coalesce(sum(market_value), 0) as market_value
    from {{ ref('stg_current') }}
    where tenant_id = 'demo:demo-account'
)

select
    'Demo Account'              as account,
    cast(null as int64)         as user_id,
    'demo:demo-account'         as tenant_id,
    'cash'                      as row_type,
    cash.cash_value             as market_value,
    cast(null as float64)       as cost_basis,
    cast(null as float64)       as unrealized_pnl,
    cast(null as float64)       as unrealized_pnl_pct,
    cast(null as float64)       as percent_of_account,
    1                           as src_priority
from cash

union all

select
    'Demo Account'              as account,
    cast(null as int64)         as user_id,
    'demo:demo-account'         as tenant_id,
    'account_total'             as row_type,
    cash.cash_value + positions.market_value as market_value,
    cast(null as float64)       as cost_basis,
    cast(null as float64)       as unrealized_pnl,
    cast(null as float64)       as unrealized_pnl_pct,
    cast(null as float64)       as percent_of_account,
    1                           as src_priority
from cash
cross join positions

{% else %}

select
    'Demo Account'          as account,
    cast(null as int64)     as user_id,
    'demo:demo-account'     as tenant_id,
    row_type,
    market_value,
    cost_basis,
    unrealized_pnl,
    unrealized_pnl_pct,
    percent_of_account,
    src_priority
from {{ ref('stg_broker_alpaca_balances') }}
where tenant_id = '{{ var("demo_source_tenant_id", "") }}'
  and '{{ var("demo_source_tenant_id", "") }}' != ''

{% endif %}
