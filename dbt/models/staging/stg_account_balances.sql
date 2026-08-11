{{
    config(
        materialized='view'
    )
}}

/*
    Account-level balances — v2.

    Pulls cash + account-total rows from the broker-sync seed
    (``account_balances``) and from the current-positions seed (legacy
    rows). Under v2 the canonical-uid backfill is GONE — every row is
    tenant-stamped at sync time, so split-tenancy can't happen.

    Demo union is preserved; demo rows carry
    tenant_id = 'demo:demo-account' (matches the demo user's
    broker_tenants row from ensure_demo_user). Since Aug 2026 the demo is a
    relabeled MIRROR of a real tenant rather than fabricated seed data —
    see dbt/models/staging/demo/stg_demo_balances.sql.
*/
-- Real-broker balance rows now arrive via the per-broker staging adapters
-- (dbt/models/staging/brokers/stg_broker_<slug>_balances), each of which
-- emits THIS broker's rows from BOTH the account_balances seed
-- (src_priority 1) and the legacy current_positions cash/total export
-- (src_priority 2). Demo rows are added separately below because demo is
-- not a broker. See stg_history.sql and dbt/macros/broker_slug_from_account.sql
-- for the add-a-brokerage procedure. The unioned/deduped logic is unchanged.
with unioned as (
    select * from {{ ref('stg_broker_schwab_balances') }}
    union all
    select * from {{ ref('stg_broker_alpaca_balances') }}
    union all
    select * from {{ ref('stg_broker_fidelity_balances') }}
    union all
    select * from {{ ref('stg_broker_interactive_balances') }}
    union all
    select * from {{ ref('stg_broker_other_balances') }}
    union all
    select * from {{ ref('stg_demo_balances') }}
),

-- Dedupe on (tenant_id when present, account fallback, row_type).
-- v2 dedups on tenant_id (demo rows carry 'demo:demo-account' since
-- Aug 2026, so they dedupe on tenant_id like every other row).
deduped as (
    select * except (src_priority)
    from unioned
    qualify row_number() over (
        partition by coalesce(tenant_id, account), row_type
        order by src_priority,
                 case when market_value is not null then 0 else 1 end
    ) = 1
)

select * from deduped
