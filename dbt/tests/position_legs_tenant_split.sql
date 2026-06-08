/*
    Tenant-split regression guard for the position legs mart.

    The bug this prevents (May/June 2026): connecting 5 physical Schwab
    accounts via SnapTrade — all carrying the identical display label
    ``account = "Schwab Account"`` + ``user_id = 9`` because SnapTrade
    returned no distinct masked number — fused every account's QTUM / UFO /
    IYW running-quantity series together. The intermediate/mart models
    grained on ``(account, user_id, symbol)`` instead of the canonical
    ``tenant_id`` (``snaptrade:<uuid>``), so the 5 accounts collapsed into
    one and ``position_legs_unique_per_symbol`` / ``..._at_most_one_open``
    tripped (83 / 18 violations).

    After the tenant_id re-grain, each physical account keeps its own legs.
    This test asserts the fix holds: for any ``(account, user_id, symbol)``
    that maps to MORE THAN ONE distinct ``tenant_id`` in the source position
    models, ``int_position_legs`` must surface AT LEAST as many distinct
    tenant_ids — i.e. the legs were partitioned per physical account and not
    silently merged back together.

    Data-independent on purpose: it only fires on the colliding-label case
    (n_src > 1) and so requires no hardcoded symbol or account count. When a
    user has 5 Schwab accounts holding QTUM, n_src = 5 and the test fails the
    moment a regression fuses any of them.
*/

with src_tenants as (
    select
        account,
        user_id,
        symbol,
        count(distinct tenant_id) as n_src
    from (
        select tenant_id, account, user_id, underlying_symbol as symbol
        from {{ ref('int_option_contracts') }}
        union all
        select tenant_id, account, user_id, symbol
        from {{ ref('int_equity_sessions') }}
    )
    where tenant_id is not null
    group by 1, 2, 3
),

leg_tenants as (
    select
        account,
        user_id,
        symbol,
        count(distinct tenant_id) as n_legs
    from {{ ref('int_position_legs') }}
    where tenant_id is not null
    group by 1, 2, 3
)

select
    s.account,
    s.user_id,
    s.symbol,
    s.n_src,
    coalesce(l.n_legs, 0) as n_legs
from src_tenants s
left join leg_tenants l
    using (account, user_id, symbol)
-- Only the colliding-label case matters; a single-tenant (account,
-- user_id, symbol) trivially has n_src = 1.
where s.n_src > 1
  and coalesce(l.n_legs, 0) < s.n_src
