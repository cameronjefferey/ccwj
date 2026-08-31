{#
    DRIP reinvestments are share-lot / cost-basis events, not trades the
    user placed. int_equity_sessions.num_trades must exclude them (same
    spirit as synthetic opening-balance rows).

    Per (tenant, account, user, symbol): session trade counts cannot
    exceed non-synthetic equity fills minus detected DRIP buys. If DRIPs
    are still in num_trades, this inequality fires.

    Win/loss is closed-session grain and is intentionally untouched.
#}

with drip_counts as (
    select
        tenant_id,
        account,
        user_id,
        underlying_symbol as symbol,
        count(*) as n_drip
    from {{ ref('int_drip_fills') }}
    group by 1, 2, 3, 4
),

fill_counts as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        countif(not is_synthetic_opening) as n_fills
    from {{ ref('int_equity_fills') }}
    group by 1, 2, 3, 4
),

session_counts as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        sum(num_trades) as n_trades
    from {{ ref('int_equity_sessions') }}
    group by 1, 2, 3, 4
)

select
    s.tenant_id,
    s.account,
    s.user_id,
    s.symbol,
    s.n_trades,
    f.n_fills,
    d.n_drip
from session_counts s
inner join fill_counts f
    on  (s.tenant_id is not distinct from f.tenant_id)
    and s.account = f.account
    and (s.user_id is not distinct from f.user_id)
    and s.symbol = f.symbol
inner join drip_counts d
    on  (s.tenant_id is not distinct from d.tenant_id)
    and s.account = d.account
    and (s.user_id is not distinct from d.user_id)
    and s.symbol = d.symbol
where s.n_trades > f.n_fills - d.n_drip
