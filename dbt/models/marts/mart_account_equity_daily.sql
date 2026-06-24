{{ config(materialized='table') }}

/*
    Daily account value, broken into equity vs options vs cash.

    One row per (tenant_id, account, user_id, date) with:
      - account_value  (account_total rows of the daily balances snapshot,
                        with today's EQUITY sleeve repriced to the official
                        close once published — see equity_by_account_day)
      - cash_value     (cash rows of the daily balances snapshot)
      - option_value   (today's live option rows from stg_current, summed)
      - equity_value   (account_value - cash_value - option_value)

    CLOSE-BASED REPORTING (June 2026): today's broker account_total can bake
    in transient after-hours equity marks if the sync lands after the bell.
    We snap the equity sleeve to the official yfinance close once it is
    published; cash, margin, and options stay broker-reported. Historical
    days and intraday (close not yet out) are unchanged. See AGENTS.md
    "Pricing Precedence".

    HISTORY SOURCE (June 2026 fix): the per-day ``account_value`` /
    ``cash_value`` series reads from the accumulating
    ``snapshot_account_balances_daily`` SCD2 snapshot. Before this, the mart
    read the LIVE ``stg_account_balances`` stamped with ``current_date()``,
    which only ever produced ONE row per account (today). That silently
    starved every day-over-day surface: the Daily Review "vs yesterday / 1w
    / 1m" comparisons and the Daily Account Δ calendar
    (mart_account_snapshots_enriched) and the /wealth page
    (mart_wealth_daily) all had no prior day to diff against, so they
    rendered "—" / blank.

    DAILY SPINE (June 2026 fix #2): the snapshot uses the SCD2 ``check``
    strategy — it records a new version ONLY when the balance CHANGES. So a
    FLAT account (value unchanged) has exactly ONE version, stamped the day
    the value first appeared. Reading that version's ``snapshot_date`` as the
    series date froze the Daily Review on that first-seen date with $0/—
    deltas, even though the connection syncs fine daily and the value is
    current (real case June 2026: testingcameron / user_id=9 — all five
    Schwab tenants flat at their June 12 values, page stuck on June 12 while
    "Sync now" returned success every time). The fix expands each version's
    ``[dbt_valid_from, dbt_valid_to)`` interval across a daily date spine
    through ``current_date()``, forward-filling the last-known balance — so
    every calendar day has a row, "today" is always today, and a flat balance
    reads as today vs yesterday = $0 (a real comparison) instead of a frozen
    stale date. An account that genuinely stopped syncing is surfaced by the
    connection-broken banner, NOT by silently freezing the date here.

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

with bal_versions as (
    select
        account,
        -- snapshot stores legacy NULL user_id as the sentinel -1 to keep
        -- its MERGE well-defined; map it back to NULL so downstream
        -- ``is not distinct from`` joins behave as they did pre-fix.
        nullif(user_id, -1) as user_id,
        tenant_id,
        tenant_grain,
        row_type,
        market_value,
        date(dbt_valid_from) as valid_from,
        date(dbt_valid_to)   as valid_to   -- NULL for the current (open) version
    from {{ ref('snapshot_account_balances_daily') }}
    where account != 'Demo Account'
      and row_type in ('cash', 'account_total')
),

-- One calendar day per row from the earliest snapshot through today.
spine as (
    select day
    from unnest(generate_date_array(
        (select min(valid_from) from bal_versions),
        current_date()
    )) as day
),

-- Expand each SCD2 version across the spine, forward-filling the last-known
-- balance. ``[valid_from, valid_to)`` is half-open so adjacent versions never
-- both claim the boundary day; the open (current) version runs through today
-- via the +1 cap. The qualify is a safety net for a same-day double change.
bal_rows as (
    select
        v.account,
        v.user_id,
        v.tenant_id,
        v.tenant_grain,
        v.row_type,
        v.market_value,
        s.day as snapshot_date
    from bal_versions v
    join spine s
      on s.day >= v.valid_from
     and s.day <  coalesce(v.valid_to, date_add(current_date(), interval 1 day))
    qualify row_number() over (
        partition by v.tenant_grain, coalesce(v.user_id, -1), v.row_type, s.day
        order by v.valid_from desc
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

-- CLOSE-BASED REPORTING (June 2026 — see AGENTS.md "Pricing Precedence").
-- Today's account_value is the broker account_total, which (when synced
-- after the bell) bakes in the broker's transient after-hours EQUITY marks.
-- We reprice ONLY the equity sleeve to the official close:
--   account_value(today) = broker account_total
--                          - broker equity MV
--                          + close-priced equity MV
-- Cash, margin, and option_value stay broker-reported. stg_current carries
-- only today's snapshot, so this adjustment is naturally today-only; on
-- historical days both sums are 0 and account_value is unchanged. When
-- today's close is not yet published (intraday), close-priced MV == broker
-- MV per symbol, so the adjustment nets to 0 and the live broker mark
-- carries — exactly the int_enriched_current ladder.
equity_rows as (
    select
        account,
        user_id,
        tenant_id,
        underlying_symbol,
        quantity,
        market_value,
        snapshot_date
    from {{ ref('stg_current') }}
    where account != 'Demo Account'
      and instrument_type = 'Equity'
      and snapshot_date is not null
),

today_close_prices as (
    select account, symbol, close_price
    from {{ ref('stg_daily_prices') }}
    where date = current_date()
),

equity_by_account_day as (
    select
        er.tenant_id,
        er.account,
        er.user_id,
        er.snapshot_date as date,
        sum(er.market_value) as broker_equity_mv,
        sum(case
                when tc.close_price is not null and tc.close_price > 0
                then er.quantity * tc.close_price
                else er.market_value
            end) as close_equity_mv
    from equity_rows er
    left join today_close_prices tc
        on er.account = tc.account
        and er.underlying_symbol = tc.symbol
    group by 1, 2, 3, 4
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
        -- Equity sleeve is the plug so the three sleeves always sum to the
        -- (repriced) account_value. The repricing below shifts account_value
        -- by (close equity MV − broker equity MV); cash/option are untouched.
        (b.account_value
            - coalesce(e.broker_equity_mv, 0)
            + coalesce(e.close_equity_mv, 0))
            - b.cash_value
            - coalesce(o.option_value, 0)                            as equity_value,
        coalesce(o.option_value, 0)                                  as option_value,
        b.cash_value,
        -- account_value snapped to the official close for today's equity
        -- sleeve (no-op on historical days and intraday; see equity CTE).
        b.account_value
            - coalesce(e.broker_equity_mv, 0)
            + coalesce(e.close_equity_mv, 0)                         as account_value
    from by_account_day b
    left join options_by_account_day o
      on b.account = o.account
     and (b.user_id is not distinct from o.user_id)
     and (b.tenant_id is not distinct from o.tenant_id)
     and b.date    = o.date
    left join equity_by_account_day e
      on b.account = e.account
     and (b.user_id is not distinct from e.user_id)
     and (b.tenant_id is not distinct from e.tenant_id)
     and b.date    = e.date
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
