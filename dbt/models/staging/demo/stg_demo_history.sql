{{
    config(
        materialized='view'
    )
}}

/*
    Public demo history — a MIRROR of a real tenant, relabeled.

    The demo used to be fabricated data (hand-written AAPL/MSFT buys in
    dbt/seeds/demo_history.csv, plus a synthetic account-value curve in
    int_demo_equity_daily). It is now a relabeled copy of the
    EarningsFollower trading bot's live Alpaca paper account, so the demo
    shows a real, continuously-updating book instead of a fiction. See
    var('demo_source_tenant_id') in dbt_project.yml.

    ── Why a mirror and not shared tenancy ──────────────────────────────
    Postgres ``broker_tenants.tenant_id`` is a PRIMARY KEY (app/models.py),
    so a tenant belongs to exactly ONE user; the demo user cannot simply be
    granted the bot's tenant. Mirroring keeps `demo:demo-account` a
    genuinely separate tenant, so the demo renders through the exact same
    tenant scoping as any real user and there is NO isolation carve-out to
    audit. The bot's own user still sees only its own tenant.

    ── Why this reads stg_broker_alpaca_history, NOT the raw source ─────
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
