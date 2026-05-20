{{
    config(
        materialized='view'
    )
}}

/*
    Account-level balances extracted from the current positions snapshot.

    Pulls the rows that stg_current intentionally filters out:
      - Cash & Money Market balances
      - Account Total summary rows

    ``user_id`` is the new tenant key (see ``docs/USER_ID_TENANCY.md``).
    Detected via ``adapter.get_columns_in_relation`` so this model keeps
    building during the deploy gap when the BQ seed table hasn't been
    rewritten with the new schema yet (e.g. dbt-bigquery's seed loader
    silently dropping the all-empty user_id column on first deploy).
*/
{% if execute %}
    {%- set _curr_cols = adapter.get_columns_in_relation(ref('current_positions')) | map(attribute='name') | list -%}
    {%- set _demo_cols = adapter.get_columns_in_relation(ref('demo_current')) | map(attribute='name') | list -%}
    {%- set _bal_cols  = adapter.get_columns_in_relation(ref('account_balances')) | map(attribute='name') | list -%}
{% else %}
    {%- set _curr_cols = [] -%}
    {%- set _demo_cols = [] -%}
    {%- set _bal_cols  = [] -%}
{% endif %}
{% set _curr_user_id_expr = "cast(user_id as string)" if 'user_id' in _curr_cols else "cast(null as string)" %}
{% set _demo_user_id_expr = "cast(user_id as string)" if 'user_id' in _demo_cols else "cast(null as string)" %}
{% set _bal_user_id_expr  = "cast(user_id as string)" if 'user_id' in _bal_cols  else "cast(null as string)" %}
{# Stage 0 broker_account_id passthrough — see docs/BROKER_ACCOUNT_ID_MIGRATION.md. #}
{% set _curr_brk_id_expr = "cast(broker_account_id as string)" if 'broker_account_id' in _curr_cols else "cast(null as string)" %}
{% set _demo_brk_id_expr = "cast(broker_account_id as string)" if 'broker_account_id' in _demo_cols else "cast(null as string)" %}
{% set _bal_brk_id_expr  = "cast(broker_account_id as string)" if 'broker_account_id' in _bal_cols  else "cast(null as string)" %}

with export_source as (
    -- Cast every column we touch to STRING so this model is resilient to the
    -- seed being empty (BigQuery re-infers column types per load; an empty
    -- current_positions seed ends up with different types than demo_current,
    -- which breaks UNION ALL). stg_current does the same via a macro.
    select
        cast(account as string) as account,
        {{ _curr_user_id_expr }} as user_id,
        {{ _curr_brk_id_expr }} as broker_account_id,
        cast(symbol as string) as symbol,
        cast(security_type as string) as security_type,
        cast(market_value as string) as market_value,
        cast(cost_bases as string) as cost_bases,
        cast(gain_or_loss_dollat as string) as gain_or_loss_dollat,
        cast(gain_or_loss_percent as string) as gain_or_loss_percent,
        cast(percent_of_account as string) as percent_of_account
    from {{ ref('current_positions') }}
    union all
    select
        cast(account as string) as account,
        {{ _demo_user_id_expr }} as user_id,
        {{ _demo_brk_id_expr }} as broker_account_id,
        cast(symbol as string) as symbol,
        cast(security_type as string) as security_type,
        cast(market_value as string) as market_value,
        cast(cost_bases as string) as cost_bases,
        cast(gain_or_loss_dollat as string) as gain_or_loss_dollat,
        cast(gain_or_loss_percent as string) as gain_or_loss_percent,
        cast(percent_of_account as string) as percent_of_account
    from {{ ref('demo_current') }}
),

-- Schwab CSV exports format dollar columns as `$1,234.56` (with $ and commas)
-- and pct columns as `5.54%`. Strip those before safe_cast so the demo seed
-- — which uses the export-style quoting — produces real numbers, not NULLs.
cash_rows as (
    select
        trim(account) as account,
        -- Cast through FLOAT64: seed user_id is "9.0" string form (pandas
        -- emits Postgres BIGINT that way). safe_cast(STRING -> INT64)
        -- rejects any decimal point. See stg_history.sql for the full
        -- incident write-up.
        safe_cast(safe_cast(nullif(trim(user_id), '') as float64) as int64) as user_id,
        safe_cast(safe_cast(nullif(trim(broker_account_id), '') as float64) as int64) as broker_account_id,
        'cash' as row_type,
        safe_cast(trim(replace(replace(market_value, '$', ''), ',', '')) as float64) as market_value,
        cast(null as float64) as cost_basis,
        cast(null as float64) as unrealized_pnl,
        cast(null as float64) as unrealized_pnl_pct,
        safe_cast(trim(replace(percent_of_account, '%', '')) as float64) as percent_of_account
    from export_source
    where lower(trim(coalesce(security_type, ''))) = 'cash and money market'
),

account_total_rows as (
    select
        trim(account) as account,
        safe_cast(safe_cast(nullif(trim(user_id), '') as float64) as int64) as user_id,
        safe_cast(safe_cast(nullif(trim(broker_account_id), '') as float64) as int64) as broker_account_id,
        'account_total' as row_type,
        safe_cast(trim(replace(replace(market_value, '$', ''), ',', '')) as float64) as market_value,
        safe_cast(trim(replace(replace(cost_bases, '$', ''), ',', '')) as float64) as cost_basis,
        safe_cast(trim(replace(replace(gain_or_loss_dollat, '$', ''), ',', '')) as float64) as unrealized_pnl,
        safe_cast(trim(replace(gain_or_loss_percent, '%', '')) as float64) as unrealized_pnl_pct,
        cast(null as float64) as percent_of_account
    from export_source
    where lower(trim(coalesce(symbol, ''))) in ('account total', 'positions total')
),

broker_bal_rows as (
    select
        trim(cast(account as string)) as account,
        safe_cast(safe_cast(nullif(trim({{ _bal_user_id_expr }}), '') as float64) as int64) as user_id,
        safe_cast(safe_cast(nullif(trim({{ _bal_brk_id_expr }}), '') as float64) as int64) as broker_account_id,
        case lower(trim(cast(row_type as string)))
            when 'cash' then 'cash'
            when 'account_total' then 'account_total'
        end as row_type,
        safe_cast(trim(replace(replace(replace(cast(market_value as string), '$', ''), ',', ''), ' ', '')) as float64) as market_value,
        safe_cast(trim(replace(replace(replace(cast(cost_basis as string), '$', ''), ',', ''), ' ', '')) as float64) as cost_basis,
        safe_cast(trim(replace(replace(replace(cast(unrealized_pnl as string), '$', ''), ',', ''), ' ', '')) as float64) as unrealized_pnl,
        safe_cast(trim(replace(replace(replace(cast(unrealized_pnl_pct as string), '%', ''), ',', ''), ' ', '')) as float64) as unrealized_pnl_pct,
        safe_cast(trim(replace(replace(cast(percent_of_account as string), '%', ''), ',', '')) as float64) as percent_of_account
    from {{ ref('account_balances') }}
    where trim(coalesce(cast(account as string), '')) != ''
      and lower(trim(coalesce(cast(row_type as string), ''))) in ('cash', 'account_total')
),

-- Dedupe across the three sources. The same (account, user_id, row_type)
-- can appear in *both* current_positions (manual export seed) AND
-- account_balances (broker sync seed — Schwab native and SnapTrade both
-- write here) once a user has uploaded a CSV and then connected a broker.
-- Without this, snapshot_account_balances_daily's MERGE fails with
-- "must match at most one source row for each target row" (its
-- unique_key is (account, row_type) — see the snapshot's docstring for
-- the Stage 0/1 grain rationale). Prefer the broker row when it exists
-- (more authoritative / live-synced); else the export row.
unioned as (
    select *, 1 as src_priority from broker_bal_rows
    union all
    select *, 2 as src_priority from cash_rows
    union all
    select *, 2 as src_priority from account_total_rows
),

-- Orphan-tenant backfill — see stg_history.sql for the full incident
-- write-up. TWO failure modes, both seen in production:
--
--   (A) NULL → populated.  Account synced under user_id=NULL before
--       being linked. Existing `account_owner` CTE handles via the
--       unambiguous-uid guard.
--   (B) Stale-uid → canonical-uid.  Same account stamped twice under
--       different uids (May 2026 / Cameron Investment — both
--       account_total and cash rows duplicated under uid=9 and
--       uid=13). The `count = 1` guard refuses to fire, leaving the
--       snapshot doubled. Resolution lives in
--       `stg_canonical_account_owner`; we apply it here with
--       higher precedence than the row's own uid stamp.
--
-- After the canonical rewrite, the `deduped` step below collapses
-- exact-duplicate rows that fold onto the same canonical uid.
account_owner as (
    select
        account,
        any_value(user_id) as inferred_user_id
    from unioned
    where user_id is not null
    group by 1
    having count(distinct user_id) = 1
),

backfilled as (
    select
        u.* except(user_id),
        coalesce(co.canonical_user_id, u.user_id, ao.inferred_user_id) as user_id
    from unioned u
    left join account_owner ao using (account)
    left join {{ ref('stg_canonical_account_owner') }} co using (account)
),

deduped as (
    select * except (src_priority)
    from backfilled
    qualify row_number() over (
        partition by account, user_id, row_type
        order by src_priority,
                 -- Tie-break on a populated market_value so empty
                 -- placeholder rows lose to ones with real numbers.
                 case when market_value is not null then 0 else 1 end
    ) = 1
)

select * from deduped
