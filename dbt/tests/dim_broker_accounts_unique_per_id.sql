/*
    `dim_broker_accounts` MUST have at most one row per
    `broker_account_id`. The Stage 3 filter
    `WHERE broker_account_id IN (SELECT broker_account_id FROM
    dim_broker_accounts WHERE user_id = :u)` would silently produce
    duplicates if this invariant breaks.

    Note: GROUP BY broker_account_id in the model itself enforces
    this. This test is belt-and-suspenders.
*/

select
    broker_account_id,
    count(*) as n
from {{ ref('dim_broker_accounts') }}
group by broker_account_id
having count(*) > 1
