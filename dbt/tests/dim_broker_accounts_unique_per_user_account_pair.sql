{{ config(severity='warn') }}
/*
    `dim_broker_accounts` should have at most one row per
    `(user_id, account_name)`. If two distinct `broker_account_id`
    values exist for the same `(user_id, account_name)`, it means the
    Postgres `broker_accounts` row was deleted and re-created (each
    SERIAL is unique). The Stage 3 filter still works correctly
    (it'd return BOTH ids for the user) but the warehouse is
    needlessly carrying drift.

    Severity is WARN — this is a hygiene check, not a security
    invariant. Stage 4 cleanup might dedupe by preferring the
    newer broker_account_id, or it might tolerate the drift forever.
*/

select
    user_id,
    account_name,
    count(*) as n,
    array_agg(broker_account_id) as ids
from {{ ref('dim_broker_accounts') }}
group by user_id, account_name
having count(*) > 1
