/*
    Every row in every user-tied seed MUST have a populated tenant_id
    that matches the v2 format ``<broker_slug>:<broker_uuid>``.

    Under v2 (see docs/V2_TENANT_KEY_DESIGN.md) tenant_id is THE
    warehouse tenant key. NULL or malformed tenant_id means either:
        - Pre-cutover legacy data the truncation missed (ingestion bug).
        - SnapTrade sync emitting rows without the tenant_id field (a
          sync regression).
        - A manual upload not stamped through the broker_tenants table.

    Any of those breaks downstream tenancy and must trip the build.
    Error severity from day 1 — there is no "transition lenient" mode
    under v2 because the migration deliberately truncates all seeds.

    Demo seeds (demo_history, demo_current) are intentionally NOT
    covered here — they're shared sample data for the demo user, not
    tenant-tied. They flow through stg_history's union and stay
    unfiltered because their account label is hardcoded 'demo'.
*/

with combined as (
    select
        'trade_history' as src, tenant_id, count(*) as n
    from {{ ref('trade_history') }}
    group by tenant_id

    union all

    select
        'current_positions' as src, tenant_id, count(*) as n
    from {{ ref('current_positions') }}
    group by tenant_id

    union all

    select
        'account_balances' as src, tenant_id, count(*) as n
    from {{ ref('account_balances') }}
    group by tenant_id
)

select src, tenant_id, n
from combined
where tenant_id is null
   or trim(tenant_id) = ''
   or not regexp_contains(tenant_id, r'^[a-z_]+:[A-Za-z0-9:.\-]+$')
