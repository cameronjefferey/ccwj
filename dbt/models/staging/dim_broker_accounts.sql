/*
    Stable per-(user, broker, broker-handle) tenant key for the
    warehouse. Replaces (account_name, user_id) as the join key in
    Stage 3 of the broker_account_id migration
    (see docs/BROKER_ACCOUNT_ID_MIGRATION.md).

    SOURCED FROM THE RAW SEEDS, NOT FROM STAGING.

    This is critical. The staging models run the
    `stg_canonical_account_owner` CTE which REWRITES user_id when an
    account has been double-stamped across multiple user_ids (the
    orphan-tenancy "B" case — see
    `.cursor/rules/position-detail-orphan-tenancy-reconciliation.mdc`).
    That rewrite picks the user_id with the most recent trade activity,
    falling back to the higher uid on ties. In production we have
    accounts whose ORIGINAL Stage 1 broker_account_id stamp is under
    one user_id (the live Postgres one) but whose canonical-owner
    rewrite reassigns the row to a DELETED user_id (which had more
    recent dummy activity). If we derived this dim from staging, the
    Stage 3 filter `WHERE broker_account_id IN (... live user's ids)`
    wouldn't match anything — because the dim would report the
    broker_account_id as belonging to the deleted user.

    Reading the seeds directly preserves the (broker_account_id ↔
    user_id) pairing that the Stage 0/1 writers established — which
    IS the Postgres truth, by construction. Stage 4 will replace this
    with a proper Postgres → BQ export of `broker_accounts`; until
    then, the seed is the next-best stable source.

    Rows with NULL broker_account_id are excluded — they're the
    orphan rows (investment1 legacy + deleted-user data) and have no
    place in the dim.
*/

-- depends_on: {{ ref('trade_history') }}
-- depends_on: {{ ref('current_positions') }}
-- depends_on: {{ ref('account_balances') }}

{% if execute %}
    {%- set _hist_cols = adapter.get_columns_in_relation(ref('trade_history'))    | map(attribute='name') | list -%}
    {%- set _curr_cols = adapter.get_columns_in_relation(ref('current_positions'))| map(attribute='name') | list -%}
    {%- set _bal_cols  = adapter.get_columns_in_relation(ref('account_balances')) | map(attribute='name') | list -%}
{% else %}
    {%- set _hist_cols = [] -%}
    {%- set _curr_cols = [] -%}
    {%- set _bal_cols  = [] -%}
{% endif %}

{% if 'broker_account_id' in _hist_cols %}

with history_seed as (
    select
        safe_cast(safe_cast(nullif(trim(cast(broker_account_id as string)), '') as float64) as int64) as broker_account_id,
        safe_cast(safe_cast(nullif(trim(cast(user_id as string)), '') as float64) as int64) as user_id,
        trim(cast(account as string)) as account_name,
        safe.parse_date('%m/%d/%Y', cast(date as string)) as event_date
    from {{ ref('trade_history') }}
),

current_seed as (
    {% if 'broker_account_id' in _curr_cols %}
    select
        safe_cast(safe_cast(nullif(trim(cast(broker_account_id as string)), '') as float64) as int64) as broker_account_id,
        safe_cast(safe_cast(nullif(trim(cast(user_id as string)), '') as float64) as int64) as user_id,
        trim(cast(account as string)) as account_name,
        current_date() as event_date
    from {{ ref('current_positions') }}
    {% else %}
    select cast(null as int64) as broker_account_id,
           cast(null as int64) as user_id,
           cast(null as string) as account_name,
           cast(null as date)   as event_date
    where false
    {% endif %}
),

balances_seed as (
    {% if 'broker_account_id' in _bal_cols %}
    select
        safe_cast(safe_cast(nullif(trim(cast(broker_account_id as string)), '') as float64) as int64) as broker_account_id,
        safe_cast(safe_cast(nullif(trim(cast(user_id as string)), '') as float64) as int64) as user_id,
        trim(cast(account as string)) as account_name,
        current_date() as event_date
    from {{ ref('account_balances') }}
    {% else %}
    select cast(null as int64) as broker_account_id,
           cast(null as int64) as user_id,
           cast(null as string) as account_name,
           cast(null as date)   as event_date
    where false
    {% endif %}
),

unioned as (
    select * from history_seed
    union all
    select * from current_seed
    union all
    select * from balances_seed
),

scoped as (
    select * from unioned
    where broker_account_id is not null
      and user_id is not null
      and account_name is not null
      and account_name != ''
)

-- Group by the (broker_account_id, user_id) PAIR as stamped in the
-- seed. Each broker_account_id maps to exactly one user_id by
-- Postgres FK construction, so this is naturally unique. The pair
-- groupby (vs. broker_account_id alone) is defensive: if a future
-- script bug ever stamped two user_ids onto one broker_account_id,
-- the `dim_broker_accounts_unique_per_id` singular test surfaces it
-- and the operator can decide which row to drop.
select
    broker_account_id,
    user_id,
    any_value(account_name) as account_name,
    min(event_date) as first_seen_at,
    max(event_date) as last_seen_at,
    count(*) as source_row_count
from scoped
group by broker_account_id, user_id

{% else %}

-- Defensive empty shape while the seed deploys without broker_account_id
-- (Stage 0 schema migration hasn't been applied yet). Matches the
-- columns downstream consumers read so dbt parse stays green.
select
    cast(null as int64)  as broker_account_id,
    cast(null as int64)  as user_id,
    cast(null as string) as account_name,
    cast(null as date)   as first_seen_at,
    cast(null as date)   as last_seen_at,
    cast(null as int64)  as source_row_count
where false

{% endif %}
