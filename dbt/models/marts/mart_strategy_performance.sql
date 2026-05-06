{{ config(materialized='table') }}

/*
    Strategy-level performance — what works for you.

    One row per (account, strategy) with:
      - total_pnl       (dividend-inclusive headline number, peer to equity+options)
      - trade_only_pnl  (legacy: realized + unrealized, no dividends)
      - realized_pnl, unrealized_pnl, dividend_income
      - num_trades, num_winners, num_losers, win_rate
      - premium_received, premium_paid
      - avg_days_in_trade, first_trade_date, last_trade_date

    Strategy values include "Dividend" for buy-for-yield positions where the
    dividend income exceeds the price-appreciation P&L (see positions_summary).

    Powers the Strategies page and strategy-focused sections on Weekly Review.
    No P/L judgment — just evidence: which strategies you use and how they've performed.
*/

with base as (
    select
        account,
        user_id,
        strategy,
        sum(total_pnl)              as total_pnl,
        sum(trade_only_pnl)         as trade_only_pnl,
        sum(realized_pnl)           as realized_pnl,
        sum(unrealized_pnl)         as unrealized_pnl,
        sum(total_premium_received) as premium_received,
        sum(total_premium_paid)      as premium_paid,
        sum(num_individual_trades)  as num_trades,
        sum(num_winners)            as num_winners,
        sum(num_losers)             as num_losers,
        sum(total_dividend_income)  as dividend_income,
        sum(total_return)           as total_return,
        count(distinct symbol)      as num_symbols,
        min(first_trade_date)       as first_trade_date,
        max(last_trade_date)        as last_trade_date,
        avg(avg_days_in_trade)      as avg_days_in_trade
    from {{ ref('positions_summary') }}
    where strategy is not null and trim(strategy) != ''
    group by 1, 2, 3
),

with_win_rate as (
    select
        *,
        safe_divide(num_winners, nullif(num_winners + num_losers, 0)) as win_rate
    from base
)

select
    account,
    user_id,
    strategy,
    round(total_pnl, 2)           as total_pnl,
    round(trade_only_pnl, 2)      as trade_only_pnl,
    round(realized_pnl, 2)        as realized_pnl,
    round(unrealized_pnl, 2)      as unrealized_pnl,
    round(premium_received, 2)    as premium_received,
    round(premium_paid, 2)        as premium_paid,
    num_trades,
    num_winners,
    num_losers,
    round(win_rate, 4)            as win_rate,
    round(dividend_income, 2)    as dividend_income,
    round(total_return, 2)       as total_return,
    num_symbols,
    first_trade_date,
    last_trade_date,
    round(avg_days_in_trade, 1)  as avg_days_in_trade
from with_win_rate
order by account, total_return desc
