{{ config(materialized='table') }}

/*
    Daily account value, broken into equity vs options vs cash.

    One row per (account, date) with:
      - account_value  (from stg_account_balances account_total rows)
      - cash_value     (from stg_account_balances cash rows)
      - option_value   (from stg_current option rows, summed)
      - equity_value   (account_value - cash_value - option_value)

    v2: under the SnapTrade-only architecture we no longer build
    daily snapshot wrappers — see docs/V2_TENANT_KEY_DESIGN.md
    (history loss accepted on cutover). The series here therefore
    starts populating from the first SnapTrade sync onward; reads
    come directly from the live ``stg_account_balances`` and
    ``stg_current`` snapshots.
*/

with bal_rows as (
    select
        account,
        user_id,
        tenant_id,
        row_type,
        market_value,
        current_date() as snapshot_date
    from {{ ref('stg_account_balances') }}
    where account != 'Demo Account'
      and row_type in ('cash', 'account_total')
),

option_rows as (
    select
        account,
        user_id,
        tenant_id,
        trade_symbol,
        market_value,
        snapshot_date
    from {{ ref('stg_current') }}
    where account != 'Demo Account'
      and instrument_type in ('Call', 'Put')
      and snapshot_date is not null
),

options_by_account_day as (
    select
        tenant_id,
        account,
        user_id,
        snapshot_date as date,
        sum(market_value) as option_value
    from option_rows
    group by 1, 2, 3, 4
),

by_account_day as (
    select
        tenant_id,
        account,
        user_id,
        snapshot_date as date,
        sum(case when row_type = 'account_total' then market_value else 0 end) as account_value,
        sum(case when row_type = 'cash'          then market_value else 0 end) as cash_value
    from bal_rows
    group by 1, 2, 3, 4
),

snapshot_result as (
    select
        b.tenant_id,
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
     and (b.tenant_id is not distinct from o.tenant_id)
     and b.date    = o.date
),

-- v2 tenant_id is carried natively from staging and is part of the grain
-- so each physical account keeps its own daily account-value series. The
-- demo source has no tenant_id; the app filters demo by account label.
all_rows as (
    select tenant_id, account, user_id, date, equity_value, option_value, cash_value, account_value
    from snapshot_result
    where account_value > 0
    union all
    -- int_demo_equity_daily emits user_id NULL by design (the demo user_id
    -- is environment-specific). The app's demo path filters by
    -- ``account = 'Demo Account'`` rather than user_id.
    select cast(null as string) as tenant_id, account, user_id, date, equity_value, option_value, cash_value, account_value
    from {{ ref('int_demo_equity_daily') }}
)

select * from all_rows f
order by f.tenant_id, f.account, f.user_id, f.date
