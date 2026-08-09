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

    Marks come from two disjoint sources (Aug 2026 rewire, same split as
    int_option_contract_daily_pnl): today from the live ``stg_current``
    snapshot; history from ``int_option_marks_daily`` (the SCD2 options
    snapshot unfolded per day, accumulating since 2026-08-04). Downstream
    coaching surfaces gate on ``data_reliable`` / ``snapshot_density``,
    so peak / giveback metrics strengthen automatically as history
    accrues instead of needing a flag day.
*/

with opt_snapshot as (
    select
        account,
        user_id,
        tenant_id,
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
    from {{ ref('stg_current') }}
    where snapshot_date is not null
      and instrument_type in ('Call', 'Put')

    union all

    select
        account,
        user_id,
        tenant_id,
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
        date as snapshot_date
    from {{ ref('int_option_marks_daily') }}
    where date < current_date()
),

daily as (
    select
        account,
        user_id,
        tenant_id,
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
    from opt_snapshot
),

contracts as (
    select
        tenant_id,
        account,
        user_id,
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
    select tenant_id, account, user_id, trade_symbol, strategy
    from {{ ref('int_strategy_classification') }}
    where trade_group_type = 'option_contract'
)

select
    d.account,
    d.user_id,
    -- v2 tenant_id passthrough (see docs/V2_TENANT_KEY_DESIGN.md).
    d.tenant_id,
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

    -- Running peak P&L: the best unrealized P&L seen up to this snapshot.
    -- Window keyed by (tenant_id, account, user_id, trade_symbol) to keep
    -- physical accounts sharing a display label apart.
    max(d.unrealized_pnl) over (
        partition by d.tenant_id, d.account, d.user_id, d.trade_symbol
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
    and (d.user_id is not distinct from c.user_id)
    and (d.tenant_id is not distinct from c.tenant_id)
    and d.trade_symbol = c.trade_symbol
left join strat s
    on d.account = s.account
    and (d.user_id is not distinct from s.user_id)
    and (d.tenant_id is not distinct from s.tenant_id)
    and d.trade_symbol = s.trade_symbol
