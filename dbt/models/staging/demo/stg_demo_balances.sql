{{
    config(
        materialized='view'
    )
}}

/*
    Public demo account balances.

    Always the bot mirror's cash and account_total. While
    demo_temp_seed_on() is true, the seed book's cash (signed fills,
    including its opening deposit) and its position market value are
    added onto those two rows. stg_account_balances keeps one row per
    (tenant, row_type), so the sum has to happen here — two cash rows
    would drop one.

    The seed figures are read from the seed files, not from stg_history,
    so mirror fills are not added twice. Flag off = the mirror rows
    only, with zero seed dollars. See dbt/seeds/DEMO_TEMP_SEED.md.

    Column order/types match broker_balances_rows() / stg_account_balances,
    including the trailing src_priority.
*/

with mirror as (
    select
        row_type,
        market_value
    from {{ ref('stg_broker_alpaca_balances') }}
    where tenant_id = '{{ var("demo_source_tenant_id", "") }}'
      and '{{ var("demo_source_tenant_id", "") }}' != ''
)

{% if demo_temp_seed_on() %}

, seed_raw as (
    select
        lower(trim(cast(Action as string))) as action_raw,
        {{ parse_seed_number('Quantity') }} as quantity,
        {{ parse_seed_number('Price') }} as price,
        coalesce({{ parse_seed_number('fees_and_comm') }}, 0) as fees,
        coalesce({{ parse_seed_number('Amount') }}, 0) as amount_raw
    from {{ ref('demo_temp_history') }}
),

seed_cash as (
    select coalesce(sum(
        case
            when action_raw in (
                'sell to open', 'sell to close', 'buy to open', 'buy to close'
            )
             and abs(fees) > 0.005
             and quantity is not null
             and price is not null
             and abs(abs(amount_raw) - abs(quantity) * price * 100) <= 0.05
            then case
                when action_raw in ('sell to open', 'sell to close')
                    then abs(quantity) * price * 100 - abs(fees)
                else -(abs(quantity) * price * 100 + abs(fees))
            end
            when action_raw in ('buy', 'buy to open', 'buy to close')
                then -abs(amount_raw)
            when action_raw in ('sell', 'sell to open', 'sell to close')
                then abs(amount_raw)
            else amount_raw
        end
    ), 0) as cash_value
    from seed_raw
),

seed_mv as (
    select coalesce(sum({{ parse_seed_number('market_value') }}), 0) as market_value
    from {{ ref('demo_temp_current') }}
),

totals as (
    select
        coalesce(max(if(row_type = 'cash', market_value, null)), 0) as cash_value,
        coalesce(max(if(row_type = 'account_total', market_value, null)), 0) as account_total
    from mirror
)

select
    'Demo Account'              as account,
    cast(null as int64)         as user_id,
    'demo:demo-account'         as tenant_id,
    'cash'                      as row_type,
    totals.cash_value + seed_cash.cash_value as market_value,
    cast(null as float64)       as cost_basis,
    cast(null as float64)       as unrealized_pnl,
    cast(null as float64)       as unrealized_pnl_pct,
    cast(null as float64)       as percent_of_account,
    1                           as src_priority
from totals
cross join seed_cash

union all

select
    'Demo Account'              as account,
    cast(null as int64)         as user_id,
    'demo:demo-account'         as tenant_id,
    'account_total'             as row_type,
    totals.account_total + seed_cash.cash_value + seed_mv.market_value as market_value,
    cast(null as float64)       as cost_basis,
    cast(null as float64)       as unrealized_pnl,
    cast(null as float64)       as unrealized_pnl_pct,
    cast(null as float64)       as percent_of_account,
    1                           as src_priority
from totals
cross join seed_cash
cross join seed_mv

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
