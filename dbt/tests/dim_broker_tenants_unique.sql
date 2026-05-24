/*
    ``dim_broker_tenants`` MUST have at most one row per ``tenant_id``.

    The dim's GROUP BY tenant_id enforces this by construction; this
    test is the belt-and-suspenders backstop in case a future refactor
    forgets the GROUP BY or splits the dim somehow. Routes that filter
    ``WHERE tenant_id IN (SELECT tenant_id FROM dim_broker_tenants
    WHERE user_id = :u)`` would silently produce duplicate rows if this
    invariant broke.
*/

select
    tenant_id,
    count(*) as n
from {{ ref('dim_broker_tenants') }}
group by tenant_id
having count(*) > 1
