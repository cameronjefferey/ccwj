{{ config(materialized='table') }}

/*
    Daily account value, broken into equity vs options vs cash.

    One row per (tenant_id, account, user_id, date) with:
      - account_value  (account_total rows of the daily balances snapshot)
      - cash_value     (cash rows of the daily balances snapshot)
      - option_value   (today's live option rows from stg_current, summed)
      - equity_value   (account_value - cash_value - option_value)

    HISTORY SOURCE (June 2026 fix): the per-day ``account_value`` /
    ``cash_value`` series reads from the accumulating
    ``snapshot_account_balances_daily`` SCD2 snapshot — one value-version
    per (tenant, day the balance changed). Before this, the mart read the
    LIVE ``stg_account_balances`` stamped with ``current_date()``, which
    only ever produced ONE row per account (today). That silently starved
    every day-over-day surface: the Daily Review "vs yesterday / 1w / 1m"
    comparisons and the Daily Account Δ calendar
    (mart_account_snapshots_enriched) and the /wealth page
    (mart_wealth_daily) all had no prior day to diff against, so they
    rendered "—" / blank. The snapshot wrapper had been accumulating
    history the whole time but was never ``ref()``'d back in after the v2
    cutover ("history loss accepted") — this rewires it.

    EQUITY / OPTION SPLIT: ``option_value`` comes from the LIVE
    ``stg_current`` snapshot, which carries ``tenant_id`` so the per-tenant
    split is correct for the most recent day. The options snapshot wrapper
    (``snapshot_options_market_values_daily``) predates ``tenant_id`` and
    keys on (account, user_id) — which collides for users with several
    "Schwab Account" tenants — so it is intentionally NOT used here (a
    join on (account, user_id) would fan the combined option MV onto every
    colliding tenant row, per AGENTS.md re-grain rule). On historical days
    there is therefore no option row to match and options fold into
    ``equity_value`` (``option_value = 0``). ``account_value`` /
    ``cash_value`` — the numbers the comparisons / calendar / wealth deltas
    depend on — are fully historical and tenant-correct regardless.
*/

with bal_rows as (
    select
        account,
        -- snapshot stores legacy NULL user_id as the sentinel -1 to keep
        -- its MERGE well-defined; map it back to NULL so downstream
        -- ``is not distinct from`` joins behave as they did pre-fix.
        nullif(user_id, -1) as user_id,
        tenant_id,
        row_type,
        market_value,
        snapshot_date
    from {{ ref('snapshot_account_balances_daily') }}
    where account != 'Demo Account'
      and row_type in ('cash', 'account_total')
    -- SCD2 ``check`` strategy can record two versions on the SAME
    -- ``snapshot_date`` if a balance changed twice in one day (two
    -- syncs / uploads). Keep only the final version per
    -- (tenant, account, user, row_type, day) so the by-day SUM below
    -- doesn't double-count that day's balance.
    qualify row_number() over (
        partition by tenant_grain, coalesce(user_id, -1), row_type, snapshot_date
        order by dbt_valid_from desc
    ) = 1
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
