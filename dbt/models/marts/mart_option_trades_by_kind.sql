{{
    config(
        materialized='view'
    )
}}
/*
    Option trades grouped by kind: strategy, DTE bucket, moneyness at open, outcome.

    Use for "Option Trades by Kind" page: which strategies, at what DTE/moneyness,
    resulted in wins vs losses. One row per (account, strategy, dte_bucket, moneyness_at_open, outcome).
*/
with kinds as (
    select * from {{ ref('int_option_trade_kinds') }}
),

agg as (
    select
        account,
        user_id,
        strategy,
        dte_bucket,
        moneyness_at_open,
        outcome,
        count(*) as num_trades,
        sum(total_pnl) as total_pnl,
        sum(net_cash_flow) as net_cash_flow
    from kinds
    group by 1, 2, 3, 4, 5, 6
),

-- Strategy-level totals for win rate and ordering
strategy_totals as (
    select
        account,
        user_id,
        strategy,
        count(*) as strategy_num_trades,
        sum(total_pnl) as strategy_total_pnl,
        sum(case when outcome = 'Winner' then 1 else 0 end) as strategy_winners
    from kinds
    group by 1, 2, 3
)

select
    a.account,
    a.user_id,
    a.strategy,
    a.dte_bucket,
    a.moneyness_at_open,
    a.outcome,
    a.num_trades,
    a.total_pnl,
    a.net_cash_flow,
    round(100.0 * sum(case when a.outcome = 'Winner' then a.num_trades else 0 end)
          over (partition by a.account, a.user_id, a.strategy, a.dte_bucket, a.moneyness_at_open)
          / nullif(sum(a.num_trades) over (partition by a.account, a.user_id, a.strategy, a.dte_bucket, a.moneyness_at_open), 0), 1) as win_rate_pct,
    st.strategy_num_trades,
    st.strategy_total_pnl,
    round(100.0 * st.strategy_winners / nullif(st.strategy_num_trades, 0), 1) as strategy_win_rate_pct
from agg a
join strategy_totals st
    on a.account = st.account
    and (a.user_id is not distinct from st.user_id)
    and a.strategy = st.strategy
order by a.account, a.user_id, st.strategy_total_pnl desc nulls last, a.strategy, a.dte_bucket, a.moneyness_at_open, a.outcome
