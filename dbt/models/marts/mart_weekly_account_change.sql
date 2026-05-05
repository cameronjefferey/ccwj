{{ config(materialized='view') }}

/*
    Weekly account change decomposition for the equity waterfall.

    One row per (account, week_start) with:
      - account_value_start
      - account_value_end
      - pnl_closed_trades       (from mart_weekly_summary)
      - pnl_dividends_other     (from mart_daily_pnl building blocks)
      - pnl_open_drift          (residual to reconcile start→end)
      - total_change
*/

with daily_equity as (
    select
        account,
        user_id,
        date,
        account_value
    from {{ ref('mart_account_equity_daily') }}
),

with_weeks as (
    select
        account,
        user_id,
        date_trunc(date, isoweek) as week_start,
        account_value
    from daily_equity
),

week_bounds as (
    select
        account,
        user_id,
        week_start,
        min_by(account_value, date) as account_value_start,
        max_by(account_value, date) as account_value_end
    from (
        select
            account,
            user_id,
            week_start,
            date,
            account_value
        from (
            select
                account,
                user_id,
                date,
                date_trunc(date, isoweek) as week_start,
                account_value
            from daily_equity
        )
    )
    group by 1, 2, 3
),

closed_trades as (
    select
        account,
        user_id,
        week_start,
        total_pnl as pnl_closed_trades
    from {{ ref('mart_weekly_summary') }}
),

dividends_other as (
    select
        account,
        user_id,
        date_trunc(date, isoweek) as week_start,
        sum(dividends_amount + other_amount) as pnl_dividends_other
    from {{ ref('mart_daily_pnl') }}
    group by 1, 2, 3
)

select
    wb.account,
    wb.user_id,
    wb.week_start,
    wb.account_value_start,
    wb.account_value_end,
    coalesce(ct.pnl_closed_trades, 0)   as pnl_closed_trades,
    coalesce(do.pnl_dividends_other, 0) as pnl_dividends_other,
    (wb.account_value_end - wb.account_value_start)
      - coalesce(ct.pnl_closed_trades, 0)
      - coalesce(do.pnl_dividends_other, 0) as pnl_open_drift,
    wb.account_value_end - wb.account_value_start as total_change
from week_bounds wb
left join closed_trades ct
  on wb.account = ct.account
 and (wb.user_id is not distinct from ct.user_id)
 and wb.week_start = ct.week_start
left join dividends_other do
  on wb.account = do.account
 and (wb.user_id is not distinct from do.user_id)
 and wb.week_start = do.week_start
order by wb.account, wb.user_id, wb.week_start

