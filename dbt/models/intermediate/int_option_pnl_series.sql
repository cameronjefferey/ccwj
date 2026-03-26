{{
    config(
        materialized='view'
    )
}}
/*
    Per-contract daily P&L curve from option snapshots.

    One row per (account, trade_symbol, snapshot_date). Tracks how each
    option's unrealized P&L evolved over time — the data that makes exit
    timing analysis possible.

    For sold options, positive unrealized_pnl means the option has decayed
    in your favor (you could buy it back cheaper than you sold it).
*/

with opt_snapshot as (
    select
        account,
        trade_symbol,
        underlying_symbol,
        option_expiry,
        option_strike,
        option_type,
        quantity,
        current_price,
        market_value,
        cost_basis,
        unrealized_pnl,
        snapshot_date,
        dbt_valid_from
    from {{ ref('snapshot_options_market_values_daily') }}
    where snapshot_date is not null
),

-- Dedup to latest snapshot row per (account, trade_symbol, snapshot_date)
deduped as (
    select
        *,
        row_number() over (
            partition by account, trade_symbol, snapshot_date
            order by dbt_valid_from desc
        ) as rn
    from opt_snapshot
),

daily as (
    select
        account,
        trade_symbol,
        underlying_symbol,
        option_expiry,
        option_strike,
        option_type,
        quantity,
        current_price,
        market_value,
        cost_basis,
        unrealized_pnl,
        snapshot_date
    from deduped
    where rn = 1
),

-- Join contract metadata for direction, strategy, premium, open_date
contracts as (
    select
        account,
        trade_symbol,
        direction,
        open_date,
        close_date,
        status,
        premium_received,
        premium_paid,
        total_pnl as final_pnl
    from {{ ref('int_option_contracts') }}
),

strat as (
    select account, trade_symbol, strategy
    from {{ ref('int_strategy_classification') }}
    where trade_group_type = 'option_contract'
)

select
    d.account,
    d.trade_symbol,
    d.underlying_symbol,
    d.option_expiry,
    d.option_strike,
    d.option_type,
    d.snapshot_date,
    d.quantity,
    d.current_price,
    d.market_value,
    d.cost_basis,
    d.unrealized_pnl,

    c.direction,
    c.open_date,
    c.close_date,
    c.status as contract_status,
    c.premium_received,
    c.premium_paid,
    c.final_pnl,
    coalesce(s.strategy, 'Other Option') as strategy,

    date_diff(d.snapshot_date, c.open_date, day) as day_in_trade,

    -- Running peak P&L: the best unrealized P&L seen up to this snapshot
    max(d.unrealized_pnl) over (
        partition by d.account, d.trade_symbol
        order by d.snapshot_date
        rows between unbounded preceding and current row
    ) as peak_pnl_to_date,

    -- For sold options: what % of premium received has been captured
    case
        when c.direction = 'Sold' and abs(c.premium_received) > 0
        then round(d.unrealized_pnl / abs(c.premium_received) * 100, 1)
        else null
    end as pct_of_premium_captured

from daily d
join contracts c
    on d.account = c.account
    and d.trade_symbol = c.trade_symbol
left join strat s
    on d.account = s.account
    and d.trade_symbol = s.trade_symbol
