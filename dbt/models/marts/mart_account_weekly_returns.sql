{{ config(materialized='table') }}

/*
    Weekly account returns from daily snapshots.

    One row per (account, week_start) with:
      - start_value       (account_value at the first snapshot in the ISO week)
      - end_value         (account_value at the last snapshot in the ISO week)
      - weekly_return_pct ((end_value - start_value) / start_value * 100)

    This uses mart_account_equity_daily, which is already based on dbt
    snapshots of Schwab account totals. Flask should query this mart
    directly instead of recomputing weekly returns.
*/

with daily as (
    select
        account,
        date,
        account_value
    from {{ ref('mart_account_equity_daily') }}
),

with_weeks as (
    select
        account,
        date_trunc(date, isoweek) as week_start,
        date,
        account_value
    from daily
),

week_bounds as (
    select
        account,
        week_start,
        min_by(account_value, date) as start_value,
        max_by(account_value, date) as end_value
    from (
        select
            account,
            week_start,
            date,
            account_value
        from with_weeks
    )
    group by account, week_start
)

select
    account,
    week_start,
    start_value,
    end_value,
    safe_divide(end_value - start_value, nullif(start_value, 0)) * 100 as weekly_return_pct
from week_bounds
order by account, week_start

