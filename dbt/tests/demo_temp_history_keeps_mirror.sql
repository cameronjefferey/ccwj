{{ config(enabled=demo_temp_seed_on()) }}

/*
    Flag on: Demo Account history is the bot mirror PLUS every seed
    fill. A dropped mirror row or a dropped seed row fails this.
    Disabled when DEMO_TEMP_SEED is off — that case is
    demo_temp_off_equals_mirror.sql. See dbt/seeds/DEMO_TEMP_SEED.md.
*/

with counts as (
    select
        (select count(*) from {{ ref('stg_history') }}
         where tenant_id = 'demo:demo-account') as demo_n,
        (select count(*) from {{ ref('stg_history') }}
         where tenant_id = '{{ var("demo_source_tenant_id", "") }}') as mirror_n,
        (select count(*) from {{ ref('demo_temp_history') }}) as seed_n
)

select demo_n, mirror_n, seed_n
from counts
where demo_n != mirror_n + seed_n
