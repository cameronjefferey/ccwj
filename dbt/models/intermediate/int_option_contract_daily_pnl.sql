/*
    Daily option P&L attribution per contract.

    THE RULE (codified in AGENTS.md "Option P&L Attribution"):

        For each option contract, the chart should show:
          - $0 contribution before open_date
          - daily mark-to-market while open (snapshot exists,
            last-known value carried forward across snapshot gaps
            like weekends and sync-skip days)
          - $0 contribution while open if NO snapshot has yet
            existed for the contract (defer credit to the close
            date — DO NOT credit STO premium on the open date)
          - the FULL realized P&L on close_date (BTC, STC, expiry,
            assignment, exercise) — credit stays at that value forever
            after; we do NOT continue emitting rows post-close

    Why: cash-flow attribution (sum option fills on their fill date —
    the pre-fix behavior of ``mart_daily_pnl.cumulative_options_pnl``)
    creates a misleading time series. A short call that opens for a
    $3,000 premium and expires worthless 7 days later is correctly
    +$3,000 of realized P&L on day 7, not +$3,000 on day 1. Crediting
    on STO front-loads every short-premium trade by [open→close] days,
    which both lies about the timing AND creates false "volatility" in
    the chart (premium spike at STO, equal-and-opposite dip at BTC).

    Daily option marks are this product's unique value prop — we sync
    snapshots so the chart can show a real options leg, not just two
    cash steps. This model is what makes that promise visible.

    OUTPUT GRAIN: one row per (account, user_id, symbol, trade_symbol,
    date) for every day of the contract's open lifetime PLUS one row on
    close_date for closed contracts. Pre-snapshot days within the open
    lifetime are EMITTED with mtm=0 — they contribute nothing but their
    presence keeps the date spine dense for downstream aggregation.

    DENSE SPINE: rows for every date in [open_date, close_date) (open
    lifetime) plus close_date itself for closed contracts. Without the
    dense spine, downstream had to do a per-symbol LAST_VALUE IGNORE
    NULLS carry-forward, which got stuck after the last contract closed
    (it kept emitting the carried-forward MTM forever instead of
    snapping to 0). With the dense spine, mart_daily_pnl just SUMs and
    the result is automatically correct on every date.

    EMISSION KEY: ``is_realized_close``
        - false → MTM contribution (open lifetime, point-in-time per day)
        - true  → realized contribution (single row on close_date)

    Downstream (mart_daily_pnl) computes:

        cumulative_options_pnl(d) =
            SUM(realized_today over date <= d)         -- cumulates
        open_options_unrealized_pnl(d) =
            SUM(mtm_today at d, all open contracts)    -- point-in-time

    The chart helper reads both fields and adds them. There is NO
    third "options_amount" cash flow contribution — that would
    triple-count.
*/

with contracts as (
    select
        tenant_id,
        account,
        user_id,
        underlying_symbol as symbol,
        trade_symbol,
        open_date,
        close_date,
        status,
        net_cash_flow
    from {{ ref('int_option_contracts') }}
    where open_date is not null
),

-- Per-contract per-snapshot MTM.
--
-- Schwab convention: ``cost_basis`` is positive for BOTH shorts
-- (= premium received) and longs (= premium paid). ``market_value``
-- is negative for shorts (cost-to-close) and positive for longs
-- (current asset value). The single unified formula:
--
--     unrealized = market_value - sign(quantity) * cost_basis
--
-- collapses to:
--   - short (qty<0):  mv - (-1)*cb = mv + cb   (premium kept minus cost to close)
--   - long  (qty>0):  mv - (+1)*cb = mv - cb   (current value minus cost paid)
--
-- This matches the override in ``stg_current.cleaned`` which is the
-- SOURCE OF TRUTH for unrealized P&L sign correction.
-- ``short_aware_unrealized_pnl`` in app/upload.py is the Python
-- mirror of the same rule.
--
-- v2: under the SnapTrade-only architecture there's no daily snapshot
-- wrapper (history loss accepted on cutover — see
-- docs/V2_TENANT_KEY_DESIGN.md). Marks come from the live
-- ``stg_current`` snapshot for today only; historical days fall
-- through to $0 contribution from the spine and the realized credit
-- on close_date does the rest.
snapshots as (
    select
        tenant_id,
        account,
        user_id,
        underlying_symbol as symbol,
        trade_symbol,
        snapshot_date as date,
        case
            when quantity is null or quantity = 0
            then coalesce(market_value, 0) - coalesce(cost_basis, 0)
            when quantity < 0
            then coalesce(market_value, 0) + coalesce(cost_basis, 0)
            else coalesce(market_value, 0) - coalesce(cost_basis, 0)
        end as mtm_unrealized_pnl
    from {{ ref('stg_current') }}
    where snapshot_date is not null
      and instrument_type in ('Call', 'Put')
      and underlying_symbol is not null
      and trim(underlying_symbol) != ''
),

-- Per-contract last snapshot date. Used to cap the lifetime spine —
-- for OPEN contracts without recent snapshot activity (the typical
-- "tenant migration" pattern: position was synced under an orphan
-- (account, NULL user_id) tuple through some date, then reassigned
-- to a real user_id under a new account label; snapshot rows for the
-- old tenant stop appearing in snapshot_options_market_values_daily
-- but the orphan int_option_contracts row stays Open forever),
-- carrying forward the last-known MTM all the way to current_date()
-- propagates STALE MTM into mart_daily_pnl (real example May 2026:
-- Schwab Account NULL PLTR 270115C00120000 last snapshot 5/7,
-- contract reassigned to Cameron Investment uid=9 on 5/8 — old
-- tenant's chart kept showing -$4,235 indefinitely).
last_snapshot_per_contract as (
    select
        tenant_id,
        account,
        user_id,
        trade_symbol,
        max(date) as last_snapshot_date
    from snapshots
    group by 1, 2, 3, 4
),

-- Contracts currently in the broker's live snapshot (today). Used to
-- decide whether the lifetime spine should extend to current_date()
-- vs cap at last_snapshot_date. The split matters when the daily
-- snapshot sync lags the live snapshot:
--
--   * currently_owned = TRUE
--       The contract is still in the trader's portfolio today even
--       if snapshot_options_market_values_daily hasn't booked a
--       row for current_date() yet (Schwab sync runs nightly; live
--       fetch updates intra-day). Extend the spine to current_date()
--       so carry-forward propagates the last-known snapshot MTM
--       (e.g. 5/8 snapshot) onto 5/9, 5/10, 5/11 — that's the
--       trader's true unrealized exposure during the gap, not $0.
--
--   * currently_owned = FALSE
--       Either the contract was reassigned/migrated to another
--       tenant, or the broker connection silently dropped. No
--       carry-forward beyond last_snapshot_date — better to under-
--       report MTM than to keep crediting a position the trader
--       no longer holds in the chart's terminal value.
--
-- This is the fix for the May-2026 "PLTR LEAP shows $0 MTM on Mon
-- but -$4,235 in the int_option_contracts headline" reconciliation
-- gap. Pre-fix the spine ended on Friday's snapshot date; the chart
-- "snapped to 0" over the weekend even though the position was
-- still on the books and the int_option_contracts.total_pnl
-- (which reads cur.unrealized_pnl) said -$4,235.
currently_owned as (
    select distinct
        tenant_id,
        account,
        user_id,
        trade_symbol
    from {{ ref('stg_current') }}
    where instrument_type in ('Call', 'Put')
),

-- Dense date spine across the open lifetime of each contract:
-- [open_date, close_date) for closed contracts (close_date itself is
-- owned by the realized branch), or [open_date, last_snapshot_date]
-- for open contracts that have snapshot data, else just [open_date]
-- alone for open contracts that have NEVER been snapshotted (their
-- contribution is $0 every day under realize-on-close anyway).
-- ``generate_date_array`` is inclusive on both ends, so we subtract
-- 1 day from close_date to get the half-open interval.
contract_lifetime as (
    select
        c.tenant_id,
        c.account,
        c.user_id,
        c.symbol,
        c.trade_symbol,
        c.open_date,
        c.close_date,
        date_d as date
    from contracts c
    left join last_snapshot_per_contract ls
        on c.account = ls.account
        and (c.user_id is not distinct from ls.user_id)
        and (c.tenant_id is not distinct from ls.tenant_id)
        and c.trade_symbol = ls.trade_symbol
    left join currently_owned co
        on c.account = co.account
        and (c.user_id is not distinct from co.user_id)
        and (c.tenant_id is not distinct from co.tenant_id)
        and c.trade_symbol = co.trade_symbol
    cross join unnest(
        generate_date_array(
            c.open_date,
            case
                -- Closed: spine ends day before close_date (realized
                -- branch owns close_date itself).
                when c.close_date is not null
                    then date_sub(c.close_date, interval 1 day)
                -- Open AND still in today's broker snapshot: extend
                -- spine to current_date() so the carry-forward
                -- window function propagates the last-known MTM
                -- across the snapshot-vs-today gap. Without this,
                -- a Friday-synced position has $0 MTM on Mon/Tue
                -- and the chart "snaps to 0" while the headline
                -- KPI (which reads stg_current live) says
                -- -$4,235. See currently_owned CTE header.
                when co.trade_symbol is not null
                    then current_date()
                -- Open with snapshot history but not currently
                -- owned (tenant migration / dropped connection):
                -- cap at last_snapshot_date — better to
                -- under-report MTM than to credit a position the
                -- trader no longer holds.
                --
                -- ``greatest(last_snapshot, open_date)`` because
                -- snapshot history can predate open_date for
                -- tenant-migrated contracts. Real example May 2026:
                -- RDDT 135C synced under (Schwab Account, NULL) on
                -- 5/8, then reassigned to (Schwab Account, uid=7) on
                -- 5/12 by the orphan-id cleanup script. The new
                -- tenant's int_option_contracts row sees open_date=
                -- 5/12 (the assignment day) but the snapshot table
                -- has 5/8 rows under uid=7 (the assignment also
                -- moved historical snapshot rows). Without
                -- ``greatest``, generate_date_array(5/12, 5/8) is
                -- empty and the contract contributes nothing — even
                -- though stg_current has a fresh today snapshot. The
                -- ``greatest`` floor ensures at least one row gets
                -- generated so today's snapshot lands on the chart.
                when ls.last_snapshot_date is not null
                    then greatest(ls.last_snapshot_date, c.open_date)
                -- Open with no snapshot ever: spine is just
                -- [open_date]. Contribution is $0 (defer to close).
                else c.open_date
            end
        )
    ) as date_d
    -- Defensive: if open_date > close_date for some pathological
    -- contract (synthetic snapshot_only contracts can have close_date
    -- equal to open_date when the contract is born already-closed),
    -- generate_date_array returns empty → no spurious rows.
),

-- Carry-forward last-known snapshot value per contract per date.
--
-- This uses a UNION-then-window pattern instead of a LEFT JOIN
-- because snapshot dates can fall OUTSIDE the contract's lifetime
-- spine (e.g. tenant-migrated contracts where the snapshot table
-- has rows from BEFORE open_date — see contract_lifetime header).
-- A simple LEFT JOIN on cl.date = s.date wouldn't see those earlier
-- snapshots and carry-forward via window inside the spine alone
-- would miss them entirely. By unioning all snapshot rows into the
-- spine, the window function can carry forward snapshots from
-- before open_date through the lifetime; we then re-join the spine
-- to retain only lifetime dates. If no snapshot has been recorded
-- for this contract yet, mtm_unrealized stays NULL (pre-snapshot
-- warm-up window) and downstream COALESCE-to-0 emits a $0
-- contribution for that day.
spine_with_snapshots as (
    select
        cl.tenant_id,
        cl.account,
        cl.user_id,
        cl.symbol,
        cl.trade_symbol,
        cl.date,
        cast(null as float64) as snap_mtm,
        true as in_lifetime
    from contract_lifetime cl
    union all
    select
        s.tenant_id,
        s.account,
        s.user_id,
        s.symbol,
        s.trade_symbol,
        s.date,
        s.mtm_unrealized_pnl as snap_mtm,
        false as in_lifetime
    from snapshots s
),
spine_filled as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        trade_symbol,
        date,
        in_lifetime,
        last_value(snap_mtm ignore nulls) over (
            partition by tenant_id, account, user_id, trade_symbol
            order by date, in_lifetime  -- snapshot row first when same date
            rows between unbounded preceding and current row
        ) as mtm_unrealized_pnl
    from spine_with_snapshots
),
-- Restrict back to lifetime-only dates. Multiple rows can exist per
-- (contract, date) when both a lifetime row AND a snapshot row landed
-- on the same date; pick exactly one via row_number.
contract_daily_mtm as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        trade_symbol,
        date,
        mtm_unrealized_pnl
    from (
        select *,
               row_number() over (
                   partition by tenant_id, account, user_id, trade_symbol, date
                   order by in_lifetime desc  -- prefer the lifetime row
               ) as rn
        from spine_filled
        where in_lifetime
    )
    where rn = 1
),

open_mtm as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        trade_symbol,
        date,
        coalesce(mtm_unrealized_pnl, 0) as pnl_today,
        false as is_realized_close
    from contract_daily_mtm
),

-- Realized credit: one row per closed contract on close_date.
--
-- Uses net_cash_flow (sum of all explicit fills). For OTM expiries
-- this equals the premium received (no closing fill). For BTC closes
-- this equals premium - cost_to_close. For assignments and exercises,
-- the option's net_cash_flow excludes the underlying stock
-- transaction (which lives on the equity P&L line) — consistent with
-- the existing total_pnl semantics in int_option_contracts.
--
-- For OPEN contracts (close_date null), no row is emitted here — the
-- chart shows MTM only, and once they close in a future build, the
-- realized credit lands on close_date.
realized_close as (
    select
        c.tenant_id,
        c.account,
        c.user_id,
        c.symbol,
        c.trade_symbol,
        c.close_date as date,
        c.net_cash_flow as pnl_today,
        true as is_realized_close
    from contracts c
    where c.close_date is not null
      and c.status = 'Closed'
),

-- v2 tenant_id is carried natively from staging through the contract grain.
all_rows as (
    select * from open_mtm
    union all
    select * from realized_close
)

select * from all_rows
