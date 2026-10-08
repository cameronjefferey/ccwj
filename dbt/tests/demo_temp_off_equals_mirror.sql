{{ config(enabled=not demo_temp_seed_on()) }}

/*
    Flag off: Demo Account is the pure bot mirror. Zero extra history,
    position, or balance rows, and zero extra dollars. The seed CSVs
    are not read by this test. Enabled only when DEMO_TEMP_SEED is off.
    See dbt/seeds/DEMO_TEMP_SEED.md.
*/

with history_demo as (
    select * except (account, user_id, tenant_id)
    from {{ ref('stg_history') }}
    where tenant_id = 'demo:demo-account'
),

history_src as (
    select * except (account, user_id, tenant_id)
    from {{ ref('stg_history') }}
    where tenant_id = '{{ var("demo_source_tenant_id", "") }}'
),

current_demo as (
    select * except (account, user_id, tenant_id)
    from {{ ref('stg_current') }}
    where tenant_id = 'demo:demo-account'
),

current_src as (
    select * except (account, user_id, tenant_id)
    from {{ ref('stg_current') }}
    where tenant_id = '{{ var("demo_source_tenant_id", "") }}'
),

balance_demo as (
    select * except (account, user_id, tenant_id)
    from {{ ref('stg_account_balances') }}
    where tenant_id = 'demo:demo-account'
),

balance_src as (
    select * except (account, user_id, tenant_id)
    from {{ ref('stg_account_balances') }}
    where tenant_id = '{{ var("demo_source_tenant_id", "") }}'
),

diffs as (
    select 'history' as surface from (
        select * from history_demo
        except distinct
        select * from history_src
    ) history_extra
    union all
    select 'history' from (
        select * from history_src
        except distinct
        select * from history_demo
    ) history_missing
    union all
    select 'current' from (
        select * from current_demo
        except distinct
        select * from current_src
    ) current_extra
    union all
    select 'current' from (
        select * from current_src
        except distinct
        select * from current_demo
    ) current_missing
    union all
    select 'balances' from (
        select * from balance_demo
        except distinct
        select * from balance_src
    ) balance_extra
    union all
    select 'balances' from (
        select * from balance_src
        except distinct
        select * from balance_demo
    ) balance_missing
)

select surface, count(*) as n
from diffs
group by 1

union all

select 'history_count', 1
from (select 1) as one_row
where (select count(*) from history_demo) != (select count(*) from history_src)

union all

select 'current_count', 1
from (select 1) as one_row
where (select count(*) from current_demo) != (select count(*) from current_src)

union all

select 'balance_count', 1
from (select 1) as one_row
where (select count(*) from balance_demo) != (select count(*) from balance_src)
