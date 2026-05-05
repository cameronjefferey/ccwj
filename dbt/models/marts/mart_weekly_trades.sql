{{ config(materialized='table') }}

/*
    Weekly trades mart — trade-level view for Weekly Review.

    One row per trade group (from int_strategy_classification) with:
      - week_start          (ISO week of open/close)
      - account, symbol, trade_symbol, strategy
      - open_date, close_date, status
      - net_cash_flow
      - total_pnl          (lifetime P&L for the group)
      - num_trades         (0 = snapshot-only synthetic; UI should hide for "this week" open rows)
      - trade_cost         (abs(net_cash_flow))
      - current_unrealized_pnl
      - current_market_value

    For closed trades:
      - current_unrealized_pnl = 0
      - current_market_value   = 0

    For open trades:
      - current_unrealized_pnl = total_pnl
      - current_market_value   = trade_cost + total_pnl

    Weekly Review can filter this mart by week_start instead of doing
    per-request calculations in Flask.
*/

with base as (
    select
        account,
        user_id,
        symbol,
        strategy,
        trade_symbol,
        open_date,
        close_date,
        status,
        net_cash_flow,
        total_pnl,
        coalesce(num_trades, 0) as num_trades,
        date_trunc(coalesce(close_date, open_date), isoweek) as week_start
    from {{ ref('int_strategy_classification') }}
    where open_date is not null
       or close_date is not null
),

calc as (
    select
        account,
        user_id,
        symbol,
        strategy,
        trade_symbol,
        week_start,
        open_date,
        close_date,
        status,
        cast(num_trades as int64) as num_trades,
        cast(net_cash_flow as float64) as net_cash_flow,
        cast(total_pnl as float64)     as total_pnl,
        abs(cast(net_cash_flow as float64)) as trade_cost,
        case
            when status = 'Closed' then 0.0
            else cast(total_pnl as float64)
        end as current_unrealized_pnl,
        case
            when status = 'Closed' then 0.0
            else abs(cast(net_cash_flow as float64)) + cast(total_pnl as float64)
        end as current_market_value
    from base
)

select *
from calc
order by account, user_id, week_start, symbol, trade_symbol

