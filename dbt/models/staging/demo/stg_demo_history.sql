{{
    config(
        materialized='view'
    )
}}

/*
    Public demo history.

    TEMPORARY: while var('demo_temp_seed') is true this view is the
    made-up book in dbt/seeds/demo_temp_history.csv, stamped
    demo:demo-account. Real tenants never read that file. How to turn
    it off and go back to the bot mirror: dbt/seeds/DEMO_TEMP_SEED.md.

    The else branch is the permanent path: a MIRROR of a real tenant,
    relabeled. The demo used to be fabricated data (hand-written
    AAPL/MSFT buys in dbt/seeds/demo_history.csv, plus a synthetic
    account-value curve in int_demo_equity_daily). It became a relabeled
    copy of the EarningsFollower trading bot's Alpaca paper account.
    See var('demo_source_tenant_id') in dbt_project.yml.

    ── Why a mirror and not shared tenancy ──────────────────────────────
    Postgres ``broker_tenants.tenant_id`` is a PRIMARY KEY (app/models.py),
    so a tenant belongs to exactly ONE user; the demo user cannot simply be
    granted the bot's tenant. Mirroring keeps `demo:demo-account` a
    genuinely separate tenant, so the demo renders through the exact same
    tenant scoping as any real user and there is NO isolation carve-out to
    audit. The bot's own user still sees only its own tenant.

    ── Why the mirror reads stg_broker_alpaca_history, NOT the raw source
    stg_broker_alpaca_history drops Alpaca's duplicate activities
    partial-fill rows and repairs the missing 100x option contract
    multiplier. Mirroring `source('raw_broker', 'trade_history')` directly
    would faithfully reproduce the exact bugs that model exists to fix — a
    phantom ~-$67k unrealized loss and a ~+$14.7k cash break (see that
    model's header and broker-sync-safety 2026-07-16). Always mirror
    POST-adapter.

    user_id is emitted NULL: the demo user's numeric id is
    environment-specific (local dev and prod are separate Postgres
    databases) and under v2 user_id is informational only — isolation is on
    tenant_id.
*/

{% if var('demo_temp_seed', false) %}

select
    'Demo Account'                          as Account,
    cast(null as string)                    as user_id,
    'demo:demo-account'                     as tenant_id,
    cast(Date as string)                    as Date,
    cast(Action as string)                  as Action,
    cast(Symbol as string)                  as Symbol,
    cast(Description as string)             as Description,
    cast(Quantity as string)                as Quantity,
    cast(Price as string)                   as Price,
    cast(fees_and_comm as string)           as fees_and_comm,
    cast(Amount as string)                  as Amount
from {{ ref('demo_temp_history') }}

{% else %}

select
    'Demo Account'                          as Account,
    cast(null as string)                    as user_id,
    'demo:demo-account'                     as tenant_id,
    Date,
    Action,
    Symbol,
    Description,
    Quantity,
    Price,
    fees_and_comm,
    Amount
from {{ ref('stg_broker_alpaca_history') }}
where tenant_id = '{{ var("demo_source_tenant_id", "") }}'
  and '{{ var("demo_source_tenant_id", "") }}' != ''

{% endif %}
