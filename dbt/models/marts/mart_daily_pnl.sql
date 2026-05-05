/*
    Daily P&L building blocks — pre-aggregated for chart rendering.

    One row per (account, symbol, date).  Covers every date that has either
    a trade or a daily close price from the yfinance pipeline.

    Options: when daily snapshots exist (int_daily_option_value), we expose
    option_market_value and option_cost_basis so the chart can show
    mark-to-market option P&L every day (like equity). When absent, the
    app uses cumulative_options_pnl from trade flows.

    Equity columns provide the daily buy/sell events so the presentation
    layer can compute running average-cost P&L. close_price from
    stg_daily_prices enables daily mark-to-market for equity.
*/

with trade_daily as (
    select
        account,
        user_id,
        underlying_symbol as symbol,
        trade_date as date,

        sum(case when instrument_type in ('Call', 'Put')
            then amount else 0 end)                                     as options_amount,

        sum(case when action = 'dividend'
            then amount else 0 end)                                     as dividends_amount,

        sum(case when instrument_type = 'Equity' and action = 'equity_buy'
            then abs(amount) else 0 end)                                as equity_buy_cost,

        sum(case when instrument_type = 'Equity' and action = 'equity_buy'
            then abs(coalesce(quantity, 0)) else 0 end)                 as equity_buy_qty,

        sum(case when instrument_type = 'Equity'
                      and action in ('equity_sell', 'equity_sell_short')
            then amount else 0 end)                                     as equity_sell_proceeds,

        sum(case when instrument_type = 'Equity'
                      and action in ('equity_sell', 'equity_sell_short')
            then abs(coalesce(quantity, 0)) else 0 end)                 as equity_sell_qty,

        sum(case when instrument_type not in ('Call', 'Put', 'Equity', 'Dividend')
            then amount else 0 end)                                     as other_amount

    from {{ ref('stg_history') }}
    where trade_date is not null
      and underlying_symbol is not null
    group by 1, 2, 3, 4
),

prices as (
    select account, symbol, date, close_price
    from {{ ref('stg_daily_prices') }}
),

-- Build the per-tenant date spine from rows that have user_id
-- (trade_daily, daily_option). prices have no user_id so we expand them
-- per-tenant via a join to known (account, user_id) pairs from
-- trade_daily; without that the price-only rows would produce NULL
-- user_id rows that the app filter would drop.
known_tenants as (
    select distinct account, user_id, symbol from trade_daily
    union distinct
    select distinct account, user_id, symbol from {{ ref('int_daily_option_value') }}
),

all_dates as (
    select distinct account, user_id, symbol, date from (
        select account, user_id, symbol, date from trade_daily
        union distinct
        select account, user_id, symbol, date from {{ ref('int_daily_option_value') }}
        union distinct
        select kt.account, kt.user_id, kt.symbol, p.date
        from known_tenants kt
        join prices p
            on kt.account = p.account
            and kt.symbol = p.symbol
    )
),

daily_option as (
    select account, user_id, symbol, date, option_market_value, option_cost_basis
    from {{ ref('int_daily_option_value') }}
),

joined as (
    select
        ad.account,
        ad.user_id,
        ad.symbol,
        ad.date,
        coalesce(td.options_amount, 0)        as options_amount,
        coalesce(td.dividends_amount, 0)      as dividends_amount,
        coalesce(td.equity_buy_cost, 0)       as equity_buy_cost,
        coalesce(td.equity_buy_qty, 0)        as equity_buy_qty,
        coalesce(td.equity_sell_proceeds, 0)  as equity_sell_proceeds,
        coalesce(td.equity_sell_qty, 0)       as equity_sell_qty,
        coalesce(td.other_amount, 0)          as other_amount,
        p.close_price,
        o.option_market_value,
        o.option_cost_basis,

        -- Flag rows that have at least one trade (vs price-only rows)
        case when td.date is not null then true else false end as has_trade

    from all_dates ad
    left join trade_daily td
        on ad.account = td.account
        and (ad.user_id is not distinct from td.user_id)
        and ad.symbol = td.symbol
        and ad.date = td.date
    left join prices p
        on ad.account = p.account
        and ad.symbol = p.symbol
        and ad.date = p.date
    left join daily_option o
        on ad.account = o.account
        and (ad.user_id is not distinct from o.user_id)
        and ad.symbol = o.symbol
        and ad.date = o.date
),

-- Carry forward latest snapshot option values so every date (on or
-- after first snapshot) has option P&L from snapshots. Window keyed
-- by (account, user_id, symbol) so two tenants can't share a fill.
filled as (
    select
        account,
        user_id,
        symbol,
        date,
        options_amount,
        dividends_amount,
        equity_buy_cost,
        equity_buy_qty,
        equity_sell_proceeds,
        equity_sell_qty,
        other_amount,
        last_value(close_price ignore nulls) over (
            partition by account, user_id, symbol order by date
            rows between unbounded preceding and current row
        ) as close_price,
        has_trade,
        last_value(option_market_value ignore nulls) over (
            partition by account, user_id, symbol order by date
            rows between unbounded preceding and current row
        ) as option_market_value,
        last_value(option_cost_basis ignore nulls) over (
            partition by account, user_id, symbol order by date
            rows between unbounded preceding and current row
        ) as option_cost_basis,
        sum(options_amount) over w    as cumulative_options_pnl,
        sum(dividends_amount) over w  as cumulative_dividends_pnl,
        sum(other_amount) over w      as cumulative_other_pnl
    from joined
    window w as (partition by account, user_id, symbol order by date)
)

select
    account,
    user_id,
    symbol,
    date,
    options_amount,
    dividends_amount,
    equity_buy_cost,
    equity_buy_qty,
    equity_sell_proceeds,
    equity_sell_qty,
    other_amount,
    close_price,
    has_trade,
    option_market_value,
    option_cost_basis,
    cumulative_options_pnl,
    cumulative_dividends_pnl,
    cumulative_other_pnl
from filled
order by account, user_id, symbol, date
