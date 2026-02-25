/*
    Daily P&L building blocks — pre-aggregated for chart rendering.

    One row per (account, symbol, date).  Covers every date that has either
    a trade or a daily close price from the yfinance pipeline.

    Options, dividends, and "other" components include running cumulative
    sums (trivial — just the cumulative amount).

    Equity columns provide the daily buy/sell events so the presentation
    layer can compute running average-cost P&L (which is inherently
    stateful and not practical in pure SQL without recursive CTEs).

    close_price comes from stg_daily_prices, enabling daily mark-to-market
    for equity positions.
*/

with trade_daily as (
    select
        account,
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
    group by 1, 2, 3
),

prices as (
    select account, symbol, date, close_price
    from {{ ref('stg_daily_prices') }}
),

all_dates as (
    select distinct account, symbol, date from (
        select account, symbol, date from trade_daily
        union distinct
        select account, symbol, date from prices
    )
),

joined as (
    select
        ad.account,
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

        -- Flag rows that have at least one trade (vs price-only rows)
        case when td.date is not null then true else false end as has_trade

    from all_dates ad
    left join trade_daily td
        on ad.account = td.account
        and ad.symbol = td.symbol
        and ad.date = td.date
    left join prices p
        on ad.account = p.account
        and ad.symbol = p.symbol
        and ad.date = p.date
)

select
    account,
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

    sum(options_amount) over w    as cumulative_options_pnl,
    sum(dividends_amount) over w as cumulative_dividends_pnl,
    sum(other_amount) over w     as cumulative_other_pnl

from joined
window w as (partition by account, symbol order by date)
order by account, symbol, date
