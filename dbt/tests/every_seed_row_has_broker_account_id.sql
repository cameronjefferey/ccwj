{{ config(severity='warn') }}
/*
    Stage 4 contract test (warning-severity during the staged migration).

    Every row in the three primary seeds (`trade_history`, `current_positions`,
    `account_balances`) SHOULD have a non-null `broker_account_id`. NULLs
    indicate one of:

      - An orphan tenant (user_id in seed references a deleted Postgres
        user; Stage 1 declined to stamp to avoid an FK violation).
      - A new sync stamped through a code path that bypasses
        `app/upload.py merge_and_push_seeds()` — that path is the
        Stage 0 invariant and should always stamp broker_account_id.

    Severity is WARN during the staged migration because:
      - Stage 1 left ~7,389 rows NULL (the 20 orphan (account, user_id)
        tuples whose user_id no longer exists in Postgres).
      - These rows are invisible to Stage 3 filters (broker_account_id
        IS NULL ⇒ not in user's broker_account_ids list) which is the
        defense-in-depth guarantee we want — but they're still in the
        seed and need operator triage.

    Stage 4B will tighten to ERROR after the orphan rows have been
    resolved (delete / reassign / recreate user / quarantine), at which
    point every seed row will be guaranteed to have a stamp.

    The test surfaces a `failure_reason` so the dbt log makes the
    Stage 4B punch-list visible at every CI run.

    See docs/BROKER_ACCOUNT_ID_MIGRATION.md (Stage 1 — actual outcome
    section) for the orphan-row triage matrix.
*/

with all_seed_rows as (
    select
        'trade_history' as seed_table,
        cast(safe_cast(nullif(trim(cast(user_id as string)), '') as float64) as int64) as user_id,
        trim(cast(account as string)) as account_name,
        safe_cast(broker_account_id as int64) as broker_account_id
    from {{ ref('trade_history') }}

    union all

    select
        'current_positions' as seed_table,
        cast(safe_cast(nullif(trim(cast(user_id as string)), '') as float64) as int64) as user_id,
        trim(cast(account as string)) as account_name,
        safe_cast(broker_account_id as int64) as broker_account_id
    from {{ ref('current_positions') }}

    union all

    select
        'account_balances' as seed_table,
        cast(safe_cast(nullif(trim(cast(user_id as string)), '') as float64) as int64) as user_id,
        trim(cast(account as string)) as account_name,
        safe_cast(broker_account_id as int64) as broker_account_id
    from {{ ref('account_balances') }}
),

missing as (
    select
        seed_table,
        account_name,
        user_id,
        count(*) as null_row_count
    from all_seed_rows
    where broker_account_id is null
    group by 1, 2, 3
)

select
    seed_table,
    account_name,
    user_id,
    null_row_count,
    'seed row has NULL broker_account_id — orphan tenant or sync bypass' as failure_reason
from missing
order by null_row_count desc
