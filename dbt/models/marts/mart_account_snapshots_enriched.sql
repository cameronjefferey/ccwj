{{ config(materialized='table') }}

/*
    Enriched daily account snapshots.

    One row per date (aggregated across all of the user's accounts) with:
      - account_value          (sum of account_value from mart_account_equity_daily)
      - base_1d_date/value     (previous snapshot, if any)
      - delta_1d / delta_1d_pct
      - base_1w_date/value     (nearest snapshot on or before date - 7 days)
      - delta_1w / delta_1w_pct
      - base_1m_date/value     (nearest snapshot on or before date - 30 days)
      - delta_1m / delta_1m_pct

    This powers the "Today's Snapshot" tiles. Flask should read straight
    from this mart rather than recomputing deltas in Python.
*/

with daily as (
    select
        date,
        sum(account_value) as account_value
    from {{ ref('mart_account_equity_daily') }}
    group by date
),

ordered as (
    select
        date,
        account_value,
        lag(account_value) over (order by date) as prev_value,
        lag(date)          over (order by date) as prev_date
    from daily
),

one_week_base as (
    select
        d.date,
        (
            select max(d2.date)
            from daily d2
            where d2.date <= date_sub(d.date, interval 7 day)
        ) as base_1w_date
    from daily d
),

one_month_base as (
    select
        d.date,
        (
            select max(d2.date)
            from daily d2
            where d2.date <= date_sub(d.date, interval 30 day)
        ) as base_1m_date
    from daily d
),

joined as (
    select
        o.date,
        o.account_value,
        o.prev_date      as base_1d_date,
        o.prev_value     as base_1d_value,
        w.base_1w_date,
        mw.account_value as base_1w_value,
        m.base_1m_date,
        mm.account_value as base_1m_value
    from ordered o
    left join one_week_base w
      on o.date = w.date
    left join daily mw
      on w.base_1w_date = mw.date
    left join one_month_base m
      on o.date = m.date
    left join daily mm
      on m.base_1m_date = mm.date
)

select
    date,
    account_value,

    base_1d_date,
    base_1d_value,
    case when base_1d_value is not null then account_value - base_1d_value end as delta_1d,
    case
        when base_1d_value is not null and base_1d_value != 0
            then safe_divide(account_value - base_1d_value, base_1d_value) * 100
    end as delta_1d_pct,

    base_1w_date,
    base_1w_value,
    case when base_1w_value is not null then account_value - base_1w_value end as delta_1w,
    case
        when base_1w_value is not null and base_1w_value != 0
            then safe_divide(account_value - base_1w_value, base_1w_value) * 100
    end as delta_1w_pct,

    base_1m_date,
    base_1m_value,
    case when base_1m_value is not null then account_value - base_1m_value end as delta_1m,
    case
        when base_1m_value is not null and base_1m_value != 0
            then safe_divide(account_value - base_1m_value, base_1m_value) * 100
    end as delta_1m_pct

from joined
order by date

