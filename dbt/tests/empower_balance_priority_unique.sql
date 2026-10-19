/*
    The balances seed (src_priority 1) has one cash row and one
    account-total row per Empower tenant. Priority 2 (legacy
    current_positions cash / total lines) may repeat the same
    row_type; stg_account_balances keeps priority 1.
*/

select
    tenant_id,
    row_type,
    count(*) as n
from {{ ref('stg_broker_empower_balances') }}
where src_priority = 1
group by tenant_id, row_type
having count(*) > 1
