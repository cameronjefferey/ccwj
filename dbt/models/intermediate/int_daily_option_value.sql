/*
    Daily option mark-to-market per (account, underlying_symbol).

    One row per (account, symbol, date) with:
      - option_market_value   (sum of option market_value from snapshot that day)
      - option_cost_basis     (sum of option cost_basis from snapshot that day)

    Built from snapshot_options_market_values_daily so that when the user
    uploads daily, the option line on symbol/account charts can move every
    day (like equity) instead of only on trade days. Before snapshots exist,
    charts fall back to cumulative_options_pnl from trades.
*/

with opt_snapshot as (
    select
        account,
        user_id,
        trade_symbol,
        underlying_symbol,
        market_value,
        cost_basis,
        snapshot_date,
        dbt_valid_from
    from {{ ref('snapshot_options_market_values_daily') }}
    where snapshot_date is not null
),

latest_per_option_day as (
    select
        account,
        user_id,
        trade_symbol,
        underlying_symbol,
        market_value,
        cost_basis,
        snapshot_date
    from (
        select
            *,
            row_number() over (
                partition by account, user_id, trade_symbol, snapshot_date
                order by dbt_valid_from desc
            ) as rn
        from opt_snapshot
    )
    where rn = 1
)

select
    account,
    user_id,
    underlying_symbol as symbol,
    snapshot_date     as date,
    sum(market_value) as option_market_value,
    sum(cost_basis)   as option_cost_basis
from latest_per_option_day
where underlying_symbol is not null and trim(underlying_symbol) != ''
group by 1, 2, 3, 4
order by 1, 2, 3, 4
