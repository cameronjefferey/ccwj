/*
    leg_id must be unique per (tenant_id, account, user_id, symbol).

    tenant_id is the canonical per-physical-account key: multiple Schwab
    accounts that share the "Schwab Account" display label (same account +
    user_id) are distinct tenants and must each get their own leg sequence.
    Keying this test on (account, user_id) alone tripped 83 false positives
    when 5 Schwab accounts fused — the grain must include tenant_id.

    The Python implementation used to assign negative session_ids by
    decrementing a counter while iterating dict groups; in edge cases
    (multiple "gap" clusters that should have been deduplicated) it
    could produce two rows with the same negative id. The SQL mart
    derives leg_id from gap_id directly, so this is impossible by
    construction — this test is the guard.
*/

select
    tenant_id,
    user_id,
    account,
    symbol,
    leg_id,
    count(*) as cnt
from {{ ref('int_position_legs') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
