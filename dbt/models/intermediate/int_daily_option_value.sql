/*
    Daily option mark-to-market per (account, underlying_symbol).

    One row per (account, symbol, date) with:
      - option_market_value   (sum of option market_value from snapshot that day)
      - option_cost_basis     (sum of option cost_basis from snapshot that day)

    v2: under the SnapTrade-only architecture we no longer build daily
    snapshot wrappers (history loss accepted — see
    docs/V2_TENANT_KEY_DESIGN.md). Marks come from the live
    ``stg_current`` snapshot for today only. Charts that previously had
    a multi-day option line on symbol/account views fall back to
    cumulative_options_pnl from trades for historical days.
*/

with opt_snapshot as (
    select
        account,
        user_id,
        tenant_id,
        trade_symbol,
        underlying_symbol,
        market_value,
        cost_basis,
        snapshot_date
    from {{ ref('stg_current') }}
    where snapshot_date is not null
      and instrument_type in ('Call', 'Put')
)

select
    account,
    user_id,
    tenant_id,
    underlying_symbol as symbol,
    snapshot_date     as date,
    sum(market_value) as option_market_value,
    sum(cost_basis)   as option_cost_basis
from opt_snapshot
where underlying_symbol is not null and trim(underlying_symbol) != ''
group by 1, 2, 3, 4, 5
order by account, user_id, symbol, date
