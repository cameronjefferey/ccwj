/*
    Current positions enriched with latest daily close prices.

    For Equity rows, overrides the (often stale) CSV snapshot price with the
    most recent close from daily_position_performance.  Recomputes market_value,
    unrealized_pnl, and unrealized_pnl_pct accordingly.

    Non-equity rows (options, etc.) pass through unchanged.
*/

with current_positions as (
    select * from {{ ref('stg_current') }}
),

latest_prices as (
    select account, symbol, close_price, date as price_date
    from (
        select
            account, symbol, close_price, date,
            row_number() over (partition by account, symbol order by date desc) as rn
        from {{ ref('stg_daily_prices') }}
    )
    where rn = 1
)

select
    cp.account,
    cp.trade_symbol,
    cp.underlying_symbol,
    cp.option_expiry,
    cp.option_strike,
    cp.option_type,
    cp.instrument_type,
    cp.description,
    cp.quantity,

    -- Price: prefer daily close for equity
    case
        when cp.instrument_type = 'Equity'
             and lp.close_price is not null
             and lp.close_price > 0
        then lp.close_price
        else cp.current_price
    end as current_price,

    -- Market value
    case
        when cp.instrument_type = 'Equity'
             and lp.close_price is not null
             and lp.close_price > 0
             and cp.quantity is not null
             and cp.quantity != 0
        then cp.quantity * lp.close_price
        else cp.market_value
    end as market_value,

    cp.cost_basis,

    -- Unrealized P&L
    case
        when cp.instrument_type = 'Equity'
             and lp.close_price is not null
             and lp.close_price > 0
             and cp.quantity is not null
             and cp.quantity != 0
             and cp.cost_basis is not null
             and cp.cost_basis != 0
        then cp.quantity * lp.close_price - cp.cost_basis
        else cp.unrealized_pnl
    end as unrealized_pnl,

    -- Unrealized P&L %
    case
        when cp.instrument_type = 'Equity'
             and lp.close_price is not null
             and lp.close_price > 0
             and cp.quantity is not null
             and cp.quantity != 0
             and cp.cost_basis is not null
             and cp.cost_basis != 0
        then 100.0 * (cp.quantity * lp.close_price - cp.cost_basis) / cp.cost_basis
        else cp.unrealized_pnl_pct
    end as unrealized_pnl_pct,

    cp.security_type_raw,
    cp.in_the_money,
    cp.dividend_yield,
    cp.pe_ratio,
    cp.snapshot_date,
    lp.price_date

from current_positions cp
left join latest_prices lp
    on cp.account = lp.account
    and cp.underlying_symbol = lp.symbol
