{{
    config(
        materialized='view'
    )
}}

-- depends_on: {{ ref('trade_history') }}
-- depends_on: {{ ref('current_positions') }}
-- depends_on: {{ ref('account_balances') }}

/*
    Canonical owner per broker-account label.

    Resolves the "B" case of orphan tenancy: same physical broker
    account stamped across MULTIPLE non-NULL user_ids by historical
    sync runs that re-seeded under a re-registered or renumbered user
    record. See:

      .cursor/rules/position-detail-orphan-tenancy-reconciliation.mdc

    Real example (May 2026 — Cameron Investment / PLTR):
      - stg_history has 50 PLTR fills under uid=9 AND 51 fills under
        uid=13 (the 50 in u9 are identical to the first 50 in u13).
      - stg_current has every PLTR contract row stamped twice (once
        under each uid).
      - stg_account_balances has cash + account_total stamped twice.
      - Position Detail rendered every leg, every closed trade, every
        strategy row, every breakdown total **2x** — Hero -$9,878.18
        on a position whose actual total is roughly half of that.

    Pre-existing canonical_account_owner CTE in stg_history required
    `count(distinct user_id) = 1` across stg_current ∪
    stg_account_balances → silently refused to fire when the broker
    surfaces were ALSO dual-stamped, leaving every consumer doubled.

    Resolution rule: per account label, the canonical user_id is the
    uid with the most recent trade_history activity (max(trade_date)).
    Ties break by the higher uid (newer user record). NULL uids are
    excluded — they're handled by the existing "A" orphan-backfill in
    each staging model.

    Read directly from the trade_history seed (NOT stg_history) so we
    can compute canonical owner BEFORE stg_history's own backfill
    runs. stg_history's pre-backfill state is exactly the seed —
    using stg_history would just stall on the same ambiguity we're
    trying to resolve, and creates an awkward partial-cycle.

    Consumed by stg_history, stg_current, stg_account_balances —
    each rewrites its `user_id` to canonical_user_id and dedupes by
    the natural grain. Net effect: every row attributed to the stale
    uid either becomes an exact duplicate of a canonical-uid row (and
    is dropped) or survives under the canonical uid (no data loss).
*/

{% if execute %}
    {%- set _hist_cols = adapter.get_columns_in_relation(ref('trade_history'))    | map(attribute='name') | list -%}
    {%- set _curr_cols = adapter.get_columns_in_relation(ref('current_positions'))| map(attribute='name') | list -%}
    {%- set _bal_cols  = adapter.get_columns_in_relation(ref('account_balances')) | map(attribute='name') | list -%}
{% else %}
    {%- set _hist_cols = [] -%}
    {%- set _curr_cols = [] -%}
    {%- set _bal_cols  = [] -%}
{% endif %}

{% if 'user_id' in _hist_cols %}

with history_parsed as (
    select
        trim(cast(account as string)) as account,
        -- Same float-then-int cast as stg_history (seed user_ids
        -- arrive as Postgres BIGINT serialized via pandas, which
        -- emits "9.0" string form — safe_cast string→int64 rejects
        -- decimal points). See stg_history for the full write-up.
        safe_cast(safe_cast(nullif(trim(cast(user_id as string)), '') as float64) as int64) as user_id,
        safe.parse_date('%m/%d/%Y', cast(date as string)) as trade_date
    from {{ ref('trade_history') }}
    where user_id is not null
),

history_activity as (
    select
        account,
        user_id,
        max(trade_date) as last_activity,
        -- Marker so the union with snapshot-only fallbacks can
        -- prioritize history-derived rows when they exist.
        true as has_history
    from history_parsed
    where account is not null
      and trim(account) != ''
      and user_id is not null
    group by 1, 2
),

-- Snapshot-only fallback. Paper-trading accounts (Alpaca, demo
-- linkages) and freshly-linked-but-untraded brokers ship NO rows in
-- trade_history yet still appear in `current_positions` /
-- `account_balances` snapshots. When two users link the same paper
-- account they double-stamp those seeds the same way real brokers
-- double-stamp trade history. With no trade_date to disambiguate
-- "most-recently-active", we fall through to picking the highest
-- uid (newest user record by Postgres autoincrement). This is the
-- same tie-break used when two uids share a max(trade_date), so the
-- policy is consistent across all cases.
current_positions_uids as (
    {% if 'user_id' in _curr_cols %}
    select
        trim(cast(account as string)) as account,
        safe_cast(safe_cast(nullif(trim(cast(user_id as string)), '') as float64) as int64) as user_id
    from {{ ref('current_positions') }}
    where user_id is not null
      and account is not null
      and trim(cast(account as string)) != ''
    {% else %}
    select
        cast(null as string) as account,
        cast(null as int64)  as user_id
    where false
    {% endif %}
),

account_balances_uids as (
    {% if 'user_id' in _bal_cols %}
    select
        trim(cast(account as string)) as account,
        safe_cast(safe_cast(nullif(trim(cast(user_id as string)), '') as float64) as int64) as user_id
    from {{ ref('account_balances') }}
    where user_id is not null
      and account is not null
      and trim(cast(account as string)) != ''
    {% else %}
    select
        cast(null as string) as account,
        cast(null as int64)  as user_id
    where false
    {% endif %}
),

snapshot_only_uids as (
    select account, user_id from current_positions_uids
    union distinct
    select account, user_id from account_balances_uids
),

combined as (
    select account, user_id, last_activity, has_history
    from history_activity
    union all
    select
        account,
        user_id,
        cast(null as date) as last_activity,
        false              as has_history
    from snapshot_only_uids
),

ranked as (
    select
        account,
        user_id,
        last_activity,
        row_number() over (
            partition by account, user_id
            order by case when has_history then 0 else 1 end
        ) as uid_rn
    from combined
),

per_account as (
    select
        account,
        user_id,
        last_activity,
        row_number() over (
            partition by account
            order by
                -- 1. History-derived signals win over snapshot-only.
                case when last_activity is not null then 0 else 1 end,
                -- 2. Most recent activity within history-derived.
                last_activity desc nulls last,
                -- 3. Larger uid (newer user record) breaks ties.
                user_id desc nulls last
        ) as rn
    from ranked
    where uid_rn = 1
)

select
    account,
    user_id          as canonical_user_id,
    last_activity    as canonical_last_activity
from per_account
where rn = 1

{% else %}

-- Defensive empty shape while the seed deploys without user_id (the
-- column is added via the upload flow; first deploy after schema
-- change may land before the new column is in BQ). Matches the
-- columns downstream models read so dbt compile stays green.
select
    cast(null as string)  as account,
    cast(null as int64)   as canonical_user_id,
    cast(null as date)    as canonical_last_activity
where false

{% endif %}
