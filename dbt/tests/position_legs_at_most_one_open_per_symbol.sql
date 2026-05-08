/*
    A trader can only have ONE current chapter of activity per symbol per
    account. Two simultaneously-Open legs means the merge algorithm broke
    (or someone reverted to the anchor-by-equity-session model). The
    merged-interval algorithm guarantees this invariant by construction —
    every still-live interval extends to today, so any other interval
    that touches today gets folded into the same leg.

    See ``int_position_legs.sql`` and the conversation that prompted the
    teardown: PLTR / Cameron Investment was the visible regression
    (one LEAP + one short call rendering as two Open legs).
*/

select
    user_id,
    account,
    symbol,
    count(*) as open_leg_count
from {{ ref('int_position_legs') }}
where status = 'Open'
group by 1, 2, 3
having count(*) > 1
