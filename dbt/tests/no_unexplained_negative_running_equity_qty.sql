{#
    Opening-balance synthesis invariant (Aug 2026 classification audit F1).

    After int_opening_balances injects synthetic opening buys, the running
    share count over int_equity_fills must NEVER go materially negative for
    any (tenant, account, user, symbol) — a negative running count means
    the warehouse thinks the trader sold shares they never owned, which is
    exactly the corruption the synthesis exists to repair (pre-fix: UBER
    history netting to -800 shares, sells silently dropped from sessions,
    a "2.7-share" IYW session carrying $215K of phantom P&L).

    Exemptions:
      - symbols with any equity_sell_short fill (genuine short positions
        legitimately run negative and are excluded from synthesis), and
      - dips within -1.0 share of zero (fractional rounding in broker
        exports; matches the epsilon spirit of the 1e-9 session cut but
        allows for dust-level export noise).

    Fails (returns rows) when a symbol with no short fills still dips
    below -1.0 shares at any point — meaning the deficit math in
    int_opening_balances failed to cover it (or a new sync wrote history
    the synthesis hasn't explained). That position's sessions, coverage
    judgments, and dividends are all suspect until repaired.
#}

with fills as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        trade_date,
        action,
        signed_quantity
    from {{ ref('int_equity_fills') }}
),

shorted_symbols as (
    select distinct tenant_id, account, user_id, symbol
    from fills
    where action = 'equity_sell_short'
),

running as (
    select
        f.*,
        sum(f.signed_quantity) over (
            partition by f.tenant_id, f.account, f.user_id, f.symbol
            order by f.trade_date, f.action
            rows between unbounded preceding and current row
        ) as running_qty
    from fills f
)

select
    r.tenant_id,
    r.account,
    r.user_id,
    r.symbol,
    min(r.running_qty) as worst_running_qty
from running r
left join shorted_symbols ss
    on  r.account = ss.account
    and (r.user_id is not distinct from ss.user_id)
    and (r.tenant_id is not distinct from ss.tenant_id)
    and r.symbol = ss.symbol
where ss.symbol is null
group by 1, 2, 3, 4
having min(r.running_qty) < -1.0
