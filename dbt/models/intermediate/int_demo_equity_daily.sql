{{
    config(
        materialized='table'
    )
}}
/*
    Synthetic daily account value for Demo Account so Weekly Review
    "Today's Snapshot" and weekly returns have a full history (no empty cells).
    One row per (Demo Account, date) from 2022-01-01 through current_date.
*/
with date_series as (
    select date_day as date
    from unnest(
        generate_date_array(date('2022-01-01'), current_date(), interval 1 day)
    ) as date_day
),

demo_daily as (
    select
        'Demo Account' as account,
        d.date,
        -- Grow from ~82k to ~185k over the period with small daily noise (deterministic)
        82000 + 103000 * (date_diff(d.date, date('2022-01-01'), day) /  nullif(date_diff(current_date(), date('2022-01-01'), day), 0))
            + mod(cast(farm_fingerprint(cast(d.date as string)) as int64), 400) - 200 as account_value_raw
    from date_series d
),

with_splits as (
    select
        account,
        date,
        account_value_raw as account_value,
        cast(account_value_raw * 0.05 as int64) as cash_value,
        cast(account_value_raw * 0.12 as int64) as option_value
    from demo_daily
)

select
    account,
    date,
    account_value - cash_value - option_value as equity_value,
    option_value,
    cash_value,
    account_value
from with_splits
order by account, date
