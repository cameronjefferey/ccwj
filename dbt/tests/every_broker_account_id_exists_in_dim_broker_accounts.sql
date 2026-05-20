{{ config(severity='warn') }}
/*
    Stage 4 contract test (warning-severity during Stage 2/3).

    Every NON-NULL `broker_account_id` in the user-tied staging models
    must have a matching row in `dim_broker_accounts`. If any seed row
    carries a `broker_account_id` that the dim doesn't know about, the
    Flask filter `WHERE broker_account_id IN (...)` cannot match it
    and the row becomes silently invisible.

    Severity is WARN (not error) during the staged migration because:
      - During Stage 2 deploy gap, marts may not yet propagate the
        column.
      - During Stage 1 → Stage 4 transition, orphan rows with NULL
        broker_account_id intentionally don't have dim entries.

    Stage 4B will tighten to ERROR after the orphan rows have been
    resolved (deleted, reassigned, or recreated) and every emitted
    seed row has a stamp.

    See docs/BROKER_ACCOUNT_ID_MIGRATION.md (Stage 1 — actual outcome
    section) for the orphan-row triage discussion.
*/

with seed_ids as (
    select distinct broker_account_id
    from {{ ref('stg_history') }}
    where broker_account_id is not null

    union distinct

    select distinct broker_account_id
    from {{ ref('stg_current') }}
    where broker_account_id is not null

    union distinct

    select distinct broker_account_id
    from {{ ref('stg_account_balances') }}
    where broker_account_id is not null
),

dim_ids as (
    select distinct broker_account_id
    from {{ ref('dim_broker_accounts') }}
)

select
    s.broker_account_id,
    'broker_account_id present in seed but missing from dim_broker_accounts' as failure_reason
from seed_ids s
left join dim_ids d using (broker_account_id)
where d.broker_account_id is null
