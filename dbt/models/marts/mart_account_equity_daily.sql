{{ config(materialized='table') }}

/*
    Daily account value from snapshots, broken into equity vs options.

    One row per (account, date) with:
      - account_value  (from snapshot_account_balances_daily account_total rows)
      - cash_value     (from snapshot_account_balances_daily cash rows)
      - option_value   (from snapshot_options_market_values_daily, summed)
      - equity_value   (account_value - cash_value - option_value)

    This relies on dbt snapshots so history is based on the actual
    Schwab exports at (ideally) end-of-day. If there are multiple
    snapshots for the same (account, row_type, snapshot_date), we keep
    only the latest by dbt_valid_from for that day.
*/

with raw as (
    select
        account,
        user_id,
        row_type,
        market_value,
        snapshot_date,
        dbt_valid_from
    from {{ ref('snapshot_account_balances_daily') }}
    where account != 'Demo Account'
),

latest_per_day as (
    select
        account,
        user_id,
        row_type,
        market_value,
        snapshot_date,
        dbt_valid_from
    from (
        select
            account,
            user_id,
            row_type,
            market_value,
            snapshot_date,
            dbt_valid_from,
            row_number() over (
                partition by account, user_id, row_type, snapshot_date
                order by dbt_valid_from desc
            ) as rn
        from raw
        where snapshot_date is not null
    )
    where rn = 1
),

option_raw as (
    select
        account,
        user_id,
        trade_symbol,
        market_value,
        snapshot_date,
        dbt_valid_from
    from {{ ref('snapshot_options_market_values_daily') }}
    where account != 'Demo Account'
),

option_latest_per_day as (
    select
        account,
        user_id,
        trade_symbol,
        market_value,
        snapshot_date,
        dbt_valid_from
    from (
        select
            account,
            user_id,
            trade_symbol,
            market_value,
            snapshot_date,
            dbt_valid_from,
            row_number() over (
                partition by account, user_id, trade_symbol, snapshot_date
                order by dbt_valid_from desc
            ) as rn
        from option_raw
        where snapshot_date is not null
    )
    where rn = 1
),

options_by_account_day as (
    select
        account,
        user_id,
        snapshot_date as date,
        sum(market_value) as option_value
    from option_latest_per_day
    group by 1, 2, 3
),

by_account_day as (
    select
        account,
        user_id,
        snapshot_date as date,
        sum(case when row_type = 'account_total' then market_value else 0 end) as account_value,
        sum(case when row_type = 'cash'          then market_value else 0 end) as cash_value
    from latest_per_day
    group by 1, 2, 3
),

snapshot_result as (
    select
        b.account,
        b.user_id,
        b.date,
        b.account_value - b.cash_value - coalesce(o.option_value, 0) as equity_value,
        coalesce(o.option_value, 0)                                  as option_value,
        b.cash_value,
        b.account_value
    from by_account_day b
    left join options_by_account_day o
      on b.account = o.account
     and (b.user_id is not distinct from o.user_id)
     and b.date    = o.date
)

select * from snapshot_result
where account_value > 0
union all
-- int_demo_equity_daily emits user_id NULL by design (the demo user_id
-- is environment-specific). The app's demo path filters by
-- ``account = 'Demo Account'`` rather than user_id — see
-- docs/USER_ID_TENANCY.md.
select account, user_id, date, equity_value, option_value, cash_value, account_value
from {{ ref('int_demo_equity_daily') }}
order by account, user_id, date

