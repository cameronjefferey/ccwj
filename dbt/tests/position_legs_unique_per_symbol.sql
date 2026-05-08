/*
    leg_id must be unique per (user_id, account, symbol).

    The Python implementation used to assign negative session_ids by
    decrementing a counter while iterating dict groups; in edge cases
    (multiple "gap" clusters that should have been deduplicated) it
    could produce two rows with the same negative id. The SQL mart
    derives leg_id from gap_id directly, so this is impossible by
    construction — this test is the guard.
*/

select
    user_id,
    account,
    symbol,
    leg_id,
    count(*) as cnt
from {{ ref('int_position_legs') }}
group by 1, 2, 3, 4
having count(*) > 1
