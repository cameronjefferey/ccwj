/*
    "If You Did Nothing" benchmark — precomputed hold P&L per position.
    Uses close prices from stg_daily_prices (yfinance pipeline) instead of
    making live API calls. One row per (account, symbol, strategy).
*/

with positions as (
    select
        account,
        symbol,
        strategy,
        status,
        total_return,
        first_trade_date,
        last_trade_date
    from {{ ref('positions_summary') }}
),

equity_cost as (
    select
        account,
        underlying_symbol as symbol,
        sum(abs(cast(amount as float64))) as equity_cost
    from {{ ref('stg_history') }}
    where instrument_type = 'Equity'
      and action = 'equity_buy'
      and cast(amount as float64) < 0
    group by 1, 2
),

entry_prices as (
    select account, symbol, close_price as entry_price
    from (
        select
            p.account, p.symbol, dp.close_price,
            row_number() over (
                partition by p.account, p.symbol
                order by dp.date asc
            ) as rn
        from positions p
        inner join {{ ref('stg_daily_prices') }} dp
            on  p.account = dp.account
            and p.symbol  = dp.symbol
            and dp.date  >= p.first_trade_date
        where dp.close_price is not null and dp.close_price > 0
    )
    where rn = 1
),

exit_prices as (
    select account, symbol, close_price as exit_price
    from (
        select
            p.account, p.symbol, dp.close_price,
            row_number() over (
                partition by p.account, p.symbol
                order by dp.date desc
            ) as rn
        from positions p
        inner join {{ ref('stg_daily_prices') }} dp
            on  p.account = dp.account
            and p.symbol  = dp.symbol
            and dp.date  <= coalesce(p.last_trade_date, current_date())
        where dp.close_price is not null and dp.close_price > 0
    )
    where rn = 1
)

select
    p.account,
    p.symbol,
    p.strategy,
    p.status,
    round(p.total_return, 2) as your_pnl,
    coalesce(ec.equity_cost, 0) as capital,
    ep.entry_price,
    xp.exit_price,
    round(safe_divide(xp.exit_price - ep.entry_price, ep.entry_price) * 100, 2)
        as hold_return_pct,
    round(
        case
            when ep.entry_price > 0 and ec.equity_cost > 0
            then ec.equity_cost * safe_divide(xp.exit_price - ep.entry_price, ep.entry_price)
        end
    , 2) as hold_pnl

from positions p
left join equity_cost ec on p.account = ec.account and p.symbol = ec.symbol
left join entry_prices ep on p.account = ep.account and p.symbol = ep.symbol
left join exit_prices xp  on p.account = xp.account and p.symbol = xp.symbol
