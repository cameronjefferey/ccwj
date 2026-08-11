{{
    config(
        materialized='view'
    )
}}

/*
    Public demo account balances — a MIRROR of a real tenant, relabeled.
    See stg_demo_history.sql for the full rationale.

    Column order/types match broker_balances_rows() / stg_account_balances's
    `unioned` CTE exactly, including the trailing `src_priority` that the
    base model's dedup orders by. Both priority tiers are carried through
    unchanged so the demo dedupes to the authoritative account_balances row
    (priority 1) the same way the source tenant does.

    Pre-mirror the demo's cash / account_total rows were derived by
    filtering demo_current for `security_type = 'cash and money market'` and
    `symbol in ('account total','positions total')`. Sourcing them from the
    balances adapter instead is the correct post-migration path.
*/

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
