{{ config(severity='warn') }}
/*
    Stage 2 contract test (warning-severity during the migration).

    For every `(account, user_id)` tuple in the user-tied staging
    models, there must be AT MOST ONE non-null `broker_account_id`.
    Two distinct `broker_account_id` values stamped on the same
    `(account, user_id)` tuple would indicate either:
      - Drift: the Postgres `broker_accounts` row was deleted and
        re-created with a new SERIAL, then the script re-stamped under
        the new id while leaving older rows under the old id.
      - Multi-broker overlap: the same account label appears under
        both Schwab and SnapTrade for the same user (shouldn't happen
        — Schwab's masked label is broker-prefixed; SnapTrade's is too).

    This is the invariant that lets the eventual Stage 4 filter flip
    use `broker_account_id` alone as the tenant key — if it's
    unambiguous per `(account, user_id)`, the legacy and new filters
    AGREE for every row.

    Stage 4 will tighten to ERROR.
*/

with combined as (
    select account, user_id, broker_account_id
    from {{ ref('stg_history') }}
    where broker_account_id is not null

    union all

    select account, user_id, broker_account_id
    from {{ ref('stg_current') }}
    where broker_account_id is not null

    union all

    select account, user_id, broker_account_id
    from {{ ref('stg_account_balances') }}
    where broker_account_id is not null
)

select
    account,
    user_id,
    count(distinct broker_account_id) as distinct_broker_account_ids,
    array_agg(distinct broker_account_id) as ids,
    'multiple broker_account_id values for one (account, user_id) — drift or multi-broker overlap' as failure_reason
from combined
group by account, user_id
having count(distinct broker_account_id) > 1
