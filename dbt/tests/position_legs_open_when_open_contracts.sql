/*
    Tenant-isolation–level invariant for the position legs mart.

    For every (user_id, account, symbol) that currently holds an OPEN
    option contract (per int_option_contracts.status), at least one row
    in int_position_legs must have status = 'Open'.

    Pre-int_position_legs the page constructed legs in Python only from
    CLOSED contracts; this test is the regression guard that the legs
    section can never again show 100 % closed pills while the banner
    (and unrealized P&L) say Open. See AGENTS.md "Surface change —
    holistic follow-through".
*/

with open_contracts as (
    select
        account,
        user_id,
        underlying_symbol as symbol,
        count(*) as open_contract_count
    from {{ ref('int_option_contracts') }}
    where status = 'Open'
    group by 1, 2, 3
),

open_legs as (
    select
        account,
        user_id,
        symbol,
        count(*) as open_leg_count
    from {{ ref('int_position_legs') }}
    where status = 'Open'
    group by 1, 2, 3
)

select
    oc.account,
    oc.user_id,
    oc.symbol,
    oc.open_contract_count,
    coalesce(ol.open_leg_count, 0) as open_leg_count
from open_contracts oc
left join open_legs ol
    on  ol.account = oc.account
    and ol.user_id is not distinct from oc.user_id
    and ol.symbol = oc.symbol
where coalesce(ol.open_leg_count, 0) = 0
