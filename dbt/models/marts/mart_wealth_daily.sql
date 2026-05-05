{{ config(materialized='table') }}

/*
    Daily wealth + cumulative trading flow per (account, date).

    Powers the /wealth page.

    Built from two sources:
      1. mart_account_equity_daily — actual snapshot account_value /
         equity_value / option_value / cash_value (truth from Schwab).
      2. stg_history — per-day options P&L cash flow + dividends.

    "Implied contributions" is computed by *identity*, not measurement:

        contributions(d) = account_value(d) − cumulative_trading_pnl(d)
                                            − cumulative_dividends(d)

    Schwab's CSV does not expose deposit/withdrawal rows in stg_history,
    so this is the only honest way to back-out money-in.  When the trader
    did not deposit anything, `contributions` will trend roughly flat.
    When a deposit happened, it shows up as a step-up.

    `gains` = account_value − contributions (the part that is *not*
    money the trader put in).  Useful for the "money in vs gains"
    stacked chart.

    Tenancy: `account` column on every row.  Apps must scope by user's
    accounts in SQL and Python.
*/

with daily_value as (
    -- Account snapshot truth (option_value already separated from equity_value)
    select
        account,
        date,
        account_value,
        equity_value,
        option_value,
        cash_value
    from {{ ref('mart_account_equity_daily') }}
),

-- Per-day options realized cash flow (sells - buys, including expirations)
options_pnl_daily as (
    select
        account,
        trade_date as date,
        sum(case when instrument_type in ('Call', 'Put') then amount else 0 end) as options_realized
    from {{ ref('stg_history') }}
    where trade_date is not null
    group by 1, 2
),

-- Per-day dividend cash flow
div_daily as (
    select
        account,
        trade_date as date,
        sum(case when action = 'dividend' then amount else 0 end) as dividends
    from {{ ref('stg_history') }}
    where trade_date is not null
    group by 1, 2
),

-- Per-day equity buy/sell cash flow.  We treat (sells − buys) as realized
-- equity P&L only AFTER summing across all days; on any single day buying
-- 100 shares is just allocation, not a loss.
equity_flow_daily as (
    select
        account,
        trade_date as date,
        sum(case when instrument_type = 'Equity'
                  and action in ('equity_sell', 'equity_sell_short')
                 then amount else 0 end)
          + sum(case when instrument_type = 'Equity' and action = 'equity_buy'
                     then amount else 0 end) as equity_net_flow
    from {{ ref('stg_history') }}
    where trade_date is not null
    group by 1, 2
),

joined as (
    select
        d.account,
        d.date,
        d.account_value,
        d.equity_value,
        d.option_value,
        d.cash_value,
        coalesce(o.options_realized, 0)   as options_realized_today,
        coalesce(dv.dividends, 0)         as dividends_today,
        coalesce(ef.equity_net_flow, 0)   as equity_net_flow_today
    from daily_value d
    left join options_pnl_daily o
        on d.account = o.account and d.date = o.date
    left join div_daily dv
        on d.account = dv.account and d.date = dv.date
    left join equity_flow_daily ef
        on d.account = ef.account and d.date = ef.date
),

-- Running totals.  We sum FROM the very first day forward — even days
-- before the first snapshot — but only emit rows where account_value
-- (the snapshot) actually exists.
with_cumulative as (
    select
        account,
        date,
        account_value,
        equity_value,
        option_value,
        cash_value,
        options_realized_today,
        dividends_today,
        equity_net_flow_today,

        sum(options_realized_today) over (
            partition by account
            order by date
            rows between unbounded preceding and current row
        ) as cumulative_options_pnl,

        sum(dividends_today) over (
            partition by account
            order by date
            rows between unbounded preceding and current row
        ) as cumulative_dividends,

        sum(equity_net_flow_today) over (
            partition by account
            order by date
            rows between unbounded preceding and current row
        ) as cumulative_equity_net_flow
    from joined
),

final as (
    select
        account,
        date,
        account_value,
        equity_value,
        option_value,
        cash_value,

        cumulative_options_pnl,
        cumulative_dividends,
        cumulative_equity_net_flow,

        -- Rough realized-trading-PnL proxy (cash from option closes
        -- + dividends + equity net cash flow).  Used to back out
        -- contributions; not a substitute for full P&L accounting.
        cumulative_options_pnl + cumulative_dividends + cumulative_equity_net_flow
            as cumulative_trading_cashflow,

        -- IMPLIED contributions: residual after subtracting trading
        -- cashflow from account_value.  When this jumps up between
        -- two days with no trades, the trader almost certainly
        -- deposited money.  When stable, no deposits.
        account_value
            - cumulative_options_pnl
            - cumulative_dividends
            - cumulative_equity_net_flow
            as implied_contributions,

        -- "gains" = the part of account_value that wasn't money the
        -- trader put in.  account_value - implied_contributions.
        cumulative_options_pnl
            + cumulative_dividends
            + cumulative_equity_net_flow
            as implied_gains
    from with_cumulative
)

select * from final
order by account, date
