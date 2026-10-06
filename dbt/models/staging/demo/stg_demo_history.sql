{{
    config(
        materialized='view'
    )
}}

/*
    Public demo history.

    Always the bot mirror (stg_broker_alpaca_history, relabeled
    demo:demo-account). While demo_temp_seed_on() is true, the made-up
    book in dbt/seeds/demo_temp_history.csv is UNIONED on top. It does
    not replace the mirror. Flag off = the mirror select only, so the
    demo tenant has zero seed rows.

    How to turn the seed off without a code edit: dbt/seeds/DEMO_TEMP_SEED.md.

    A seed fill that would land on the same stg_history dedup grain as a
    mirror fill (tenant, date, action, symbol, quantity, price@4dp) has
    its price moved by one cent and its amount scaled with it, so the
    mirror row is kept and the seed row is kept. No collision, no bump.

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

with mirror as (
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
)

{% if demo_temp_seed_on() %}

, seed_base as (
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
        cast(Amount as string)                  as Amount,
        {{ parse_seed_date('Date') }}           as _d,
        lower(trim(cast(Action as string)))     as _a,
        upper(trim(cast(Symbol as string)))     as _s,
        {{ parse_seed_number('Quantity') }}     as _q,
        {{ parse_seed_number('Price') }}        as _p,
        {{ parse_seed_number('Amount') }}       as _amt
    from {{ ref('demo_temp_history') }}
),

mirror_keys as (
    select
        {{ parse_seed_date('Date') }}           as _d,
        lower(trim(Action))                     as _a,
        upper(trim(Symbol))                     as _s,
        {{ parse_seed_number('Quantity') }}     as _q,
        round({{ parse_seed_number('Price') }}, 4) as _p
    from mirror
),

seed as (
    select
        s.Account,
        s.user_id,
        s.tenant_id,
        s.Date,
        s.Action,
        s.Symbol,
        s.Description,
        s.Quantity,
        case
            when k._d is not null and s._p is not null
                then cast(s._p + 0.01 as string)
            else s.Price
        end as Price,
        s.fees_and_comm,
        case
            when k._d is not null and s._p is not null and s._p != 0
                then cast(s._amt * (s._p + 0.01) / s._p as string)
            else s.Amount
        end as Amount
    from seed_base s
    left join mirror_keys k
        on k._d is not distinct from s._d
       and k._a is not distinct from s._a
       and k._s is not distinct from s._s
       and k._q is not distinct from s._q
       and k._p is not distinct from round(s._p, 4)
)

select * from mirror
union all
select * from seed

{% else %}

select * from mirror

{% endif %}
