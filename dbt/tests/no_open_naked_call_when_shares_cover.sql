{#
    Open short-call honesty (Sep 2026 / CCJ).

    An OPEN sold call must not be labeled Naked Call when the same
    (tenant, account, user, symbol) currently holds at least one
    contract's deliverable (~100 shares). That is a covered call —
    whether the shares arrived via a fill, a synthesized opening
    balance, a share transfer (snapshot only), or a buy more than
    3 days after the write.

    Pre-fix, coverage was write-date-only from int_equity_fills
    (INNER JOIN). A live 100-share + 1 short call then rendered as
    Covered Call (equity session, 30% overlap test) AND Naked Call
    (the contract). Real case: CCJ Sep 18 '26 $102C against 100
    shares.

    Fails (returns rows) for any open Naked Call whose current
    holdings (greater of fill-ledger running qty and broker
    snapshot) cover >= 95% of one deliverable.
#}

with naked_open as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        trade_symbol,
        strategy,
        open_date
    from {{ ref('int_strategy_classification') }}
    where trade_group_type = 'option_contract'
      and strategy = 'Naked Call'
      and status = 'Open'
      and option_type = 'C'
      and direction = 'Sold'
),

ledger_now as (
    select
        n.tenant_id,
        n.account,
        n.user_id,
        n.trade_symbol,
        coalesce(sum(f.signed_quantity), 0) as shares_held
    from naked_open n
    left join {{ ref('int_equity_fills') }} f
        on  f.account = n.account
        and (f.user_id is not distinct from n.user_id)
        and (f.tenant_id is not distinct from n.tenant_id)
        and f.symbol = n.symbol
    group by 1, 2, 3, 4
),

snapshot_now as (
    select
        n.tenant_id,
        n.account,
        n.user_id,
        n.trade_symbol,
        coalesce(sum(c.quantity), 0) as shares_held
    from naked_open n
    left join {{ ref('stg_current') }} c
        on  c.account = n.account
        and (c.user_id is not distinct from n.user_id)
        and (c.tenant_id is not distinct from n.tenant_id)
        and c.underlying_symbol = n.symbol
        and c.instrument_type = 'Equity'
    group by 1, 2, 3, 4
),

deliverable as (
    select
        n.tenant_id,
        n.account,
        n.user_id,
        n.trade_symbol,
        100.0 * coalesce(sf.cumulative_split_factor, 1.0) as shares_per_contract
    from naked_open n
    left join {{ ref('int_split_factors') }} sf
        on  sf.symbol     = n.symbol
        and sf.trade_date = n.open_date
)

select
    n.tenant_id,
    n.account,
    n.user_id,
    n.symbol,
    n.trade_symbol,
    n.strategy,
    greatest(coalesce(ln.shares_held, 0), coalesce(sn.shares_held, 0))
        as shares_now,
    d.shares_per_contract
from naked_open n
join ledger_now ln
    on  n.account = ln.account
    and (n.user_id is not distinct from ln.user_id)
    and (n.tenant_id is not distinct from ln.tenant_id)
    and n.trade_symbol = ln.trade_symbol
join snapshot_now sn
    on  n.account = sn.account
    and (n.user_id is not distinct from sn.user_id)
    and (n.tenant_id is not distinct from sn.tenant_id)
    and n.trade_symbol = sn.trade_symbol
join deliverable d
    on  n.account = d.account
    and (n.user_id is not distinct from d.user_id)
    and (n.tenant_id is not distinct from d.tenant_id)
    and n.trade_symbol = d.trade_symbol
where greatest(coalesce(ln.shares_held, 0), coalesce(sn.shares_held, 0))
      >= 0.95 * d.shares_per_contract
