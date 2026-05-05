{{ config(materialized='table') }}

/*
    Strategy performance over time — one row per (account, strategy, month).

    Enables "am I getting better or worse at this strategy?" by providing
    monthly win rate, P&L, and trade counts with rolling 3-month averages
    and a simple trend signal.
*/

with closed_trades as (
    select
        account,
        user_id,
        strategy,
        date_trunc(close_date, month) as month_start,
        total_pnl,
        is_winner,
        days_in_trade,
        premium_received,
        premium_paid
    from {{ ref('int_strategy_classification') }}
    where status = 'Closed'
      and close_date is not null
      and strategy is not null
),

monthly as (
    select
        account,
        user_id,
        strategy,
        month_start,
        count(*)                              as trades_closed,
        countif(is_winner)                    as num_winners,
        countif(not is_winner)                as num_losers,
        safe_divide(
            countif(is_winner),
            nullif(count(*), 0)
        )                                     as win_rate,
        sum(total_pnl)                        as total_pnl,
        avg(total_pnl)                        as avg_pnl_per_trade,
        avg(days_in_trade)                    as avg_days_in_trade,
        sum(premium_received)                 as premium_collected,
        sum(abs(premium_paid))                as premium_paid
    from closed_trades
    group by 1, 2, 3, 4
),

with_rolling as (
    select
        *,
        avg(win_rate) over (
            partition by account, user_id, strategy
            order by month_start
            rows between 3 preceding and 1 preceding
        ) as win_rate_3m,

        avg(avg_pnl_per_trade) over (
            partition by account, user_id, strategy
            order by month_start
            rows between 3 preceding and 1 preceding
        ) as avg_pnl_3m,

        avg(trades_closed) over (
            partition by account, user_id, strategy
            order by month_start
            rows between 3 preceding and 1 preceding
        ) as avg_trades_3m,

        count(*) over (
            partition by account, user_id, strategy
            order by month_start
            rows between 3 preceding and 1 preceding
        ) as baseline_months
    from monthly
)

select
    account,
    user_id,
    strategy,
    month_start,
    trades_closed,
    num_winners,
    num_losers,
    round(win_rate * 100, 1)              as win_rate_pct,
    round(total_pnl, 2)                   as total_pnl,
    round(avg_pnl_per_trade, 2)           as avg_pnl_per_trade,
    round(avg_days_in_trade, 1)           as avg_days_in_trade,
    round(premium_collected, 2)           as premium_collected,
    round(premium_paid, 2)                as premium_paid,
    round(win_rate_3m * 100, 1)           as win_rate_3m_pct,
    round(avg_pnl_3m, 2)                 as avg_pnl_3m,
    round(avg_trades_3m, 1)              as avg_trades_3m,
    baseline_months,
    case
        when baseline_months < 2 then 'new'
        when win_rate > win_rate_3m * 1.10 then 'improving'
        when win_rate < win_rate_3m * 0.90 then 'declining'
        else 'stable'
    end as trend_signal
from with_rolling
order by account, user_id, strategy, month_start
