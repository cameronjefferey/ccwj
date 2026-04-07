{{ config(materialized='table') }}

/*
    Enriched daily account snapshots — per account.

    One row per (account, date) with:
      - account_value          (from mart_account_equity_daily)
      - base_1d_date/value     (previous snapshot for this account, if any)
      - delta_1d / delta_1d_pct
      - base_1w_date/value     (nearest snapshot on or before date - 7 days)
      - delta_1w / delta_1w_pct
      - base_1m_date/value     (nearest snapshot on or before date - 30 days)
      - delta_1m / delta_1m_pct

    Flask filters by account IN (user's accounts), then aggregates
    (sum account_value by date) and computes display deltas for the
    latest date so "Today's Snapshot" respects multi-account scope.
*/

with daily as (
    select
        account,
        date,
        account_value
    from {{ ref('mart_account_equity_daily') }}
),

ordered as (
    select
        account,
        date,
        account_value,
        lag(account_value) over (partition by account order by date) as prev_value,
        lag(date)          over (partition by account order by date) as prev_date
    from daily
),

one_week_base as (
    select
        account,
        d.date,
        (
            select max(d2.date)
            from daily d2
            where d2.account = d.account
              and d2.date <= date_sub(d.date, interval 7 day)
        ) as base_1w_date
    from daily d
),

one_month_base as (
    select
        account,
        d.date,
        (
            select max(d2.date)
            from daily d2
            where d2.account = d.account
              and d2.date <= date_sub(d.date, interval 30 day)
        ) as base_1m_date
    from daily d
),

joined as (
    select
        o.account,
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
      on o.account = w.account and o.date = w.date
    left join daily mw
      on w.account = mw.account and w.base_1w_date = mw.date
    left join one_month_base m
      on o.account = m.account and o.date = m.date
    left join daily mm
      on m.account = mm.account and m.base_1m_date = mm.date
)

select
    account,
    date,
    account_value,

    base_1d_date,
    case when base_1d_value > 0 then base_1d_value end as base_1d_value,
    case when base_1d_value > 0 and account_value > 0
         then account_value - base_1d_value end as delta_1d,
    case when base_1d_value > 0 and account_value > 0
         then safe_divide(account_value - base_1d_value, base_1d_value) * 100
    end as delta_1d_pct,

    base_1w_date,
    case when base_1w_value > 0 then base_1w_value end as base_1w_value,
    case when base_1w_value > 0 and account_value > 0
         then account_value - base_1w_value end as delta_1w,
    case when base_1w_value > 0 and account_value > 0
         then safe_divide(account_value - base_1w_value, base_1w_value) * 100
    end as delta_1w_pct,

    base_1m_date,
    case when base_1m_value > 0 then base_1m_value end as base_1m_value,
    case when base_1m_value > 0 and account_value > 0
         then account_value - base_1m_value end as delta_1m,
    case when base_1m_value > 0 and account_value > 0
         then safe_divide(account_value - base_1m_value, base_1m_value) * 100
    end as delta_1m_pct

from joined
order by account, date
