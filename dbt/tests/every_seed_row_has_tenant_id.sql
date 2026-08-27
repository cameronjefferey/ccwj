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

    The uuid half is opaque. SnapTrade UUIDs are hex+hyphens; CSV uploads
    stamp ``manual:manual:<account name>`` and account names routinely
    contain spaces (``manual:manual:Emmory Investment``). Require
    ``<slug>:<nonempty>`` — do not restrict the uuid charset, or a
    legitimate upload fails the warehouse rebuild and the user sits on
    the processing page.

    The demo is intentionally NOT covered here. It is no longer seed data
    at all: since Aug 2026 it is a relabeled MIRROR of a real tenant
    (dbt/models/staging/demo/stg_demo_history.sql), synthesized downstream
    of these raw tables and stamped 'demo:demo-account' in the mirror
    models themselves — so it can never be the source of a NULL tenant_id
    here. The source tenant it copies IS covered, as a normal broker row.
*/

with combined as (
    select
        'trade_history' as src, tenant_id, count(*) as n
    from {{ source('raw_broker', 'trade_history') }}
    group by tenant_id

    union all

    select
        'current_positions' as src, tenant_id, count(*) as n
    from {{ source('raw_broker', 'current_positions') }}
    group by tenant_id

    union all

    select
        'account_balances' as src, tenant_id, count(*) as n
    from {{ source('raw_broker', 'account_balances') }}
    group by tenant_id
)

select src, tenant_id, n
from combined
where tenant_id is null
   or trim(tenant_id) = ''
   or not regexp_contains(tenant_id, r'^[a-z_]+:.+$')
