/*
    A trader can only have ONE current chapter of activity per symbol per
    physical account (tenant_id). Two simultaneously-Open legs means the
    merge algorithm broke (or someone reverted to the anchor-by-equity-
    session model). The merged-interval algorithm guarantees this invariant
    by construction — every still-live interval extends to today, so any
    other interval that touches today gets folded into the same leg.

    Grain is (tenant_id, account, user_id, symbol): multiple Schwab accounts
    sharing the "Schwab Account" display label are distinct tenants and each
    legitimately holds its own Open leg for the same symbol. Keying on
    (account, user_id) alone tripped 18 false positives when 5 Schwab
    accounts fused.

    See ``int_position_legs.sql`` and the conversation that prompted the
    teardown: PLTR / Cameron Investment was the visible regression
    (one LEAP + one short call rendering as two Open legs).
*/

select
    tenant_id,
    user_id,
    account,
    symbol,
    count(*) as open_leg_count
from {{ ref('int_position_legs') }}
where status = 'Open'
group by 1, 2, 3, 4
having count(*) > 1
