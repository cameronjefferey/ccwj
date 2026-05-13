/*
    Equity position sessions.

    Tracks the lifecycle of each equity holding by computing a running share
    count and cutting a new "session" every time the position goes from 0 to
    a positive quantity.  Each session represents one continuous period of
    ownership (buy → hold → sell).

    Open sessions are enriched with current market data from stg_current.
*/

with equity_trades as (
    select
        account,
        user_id,
        underlying_symbol as symbol,
        trade_date,
        action,
        case
            when action = 'equity_buy'        then  quantity
            when action = 'equity_sell'       then -quantity
            when action = 'equity_sell_short' then -quantity
            else 0
        end as signed_quantity,
        quantity,
        amount
    from {{ ref('stg_history') }}
    where instrument_type = 'Equity'
),

-- Running share count.
-- Window partitioned by (account, user_id, symbol) so two users with the
-- same account_name + symbol never share a running quantity series.
running as (
    select
        *,
        sum(signed_quantity) over (
            partition by account, user_id, symbol
            order by trade_date, action
            rows between unbounded preceding and current row
        ) as running_qty
    from equity_trades
),

-- Previous running quantity (to detect 0 → positive transitions)
with_prev as (
    select
        *,
        coalesce(
            lag(running_qty) over (
                partition by account, user_id, symbol
                order by trade_date, action
            ),
            0
        ) as prev_running_qty
    from running
),

-- Assign session IDs: increment each time the position transitions from 0 → positive.
--
-- Float-precision-aware zero check (1e-9 share epsilon).
-- Why: signed_quantity is FLOAT64 (IEEE 754 double). A round-trip like
-- ``+1 + 0.0006 + 0.0005 + 1 + 0.0006 + 0.0006 - 0.0023 - 2`` doesn't
-- evaluate to exactly 0 — it evaluates to ``-0.000000000000000000``
-- (≈ -1e-17). The strict check ``prev_running_qty = 0`` fails, the
-- next buy doesn't start a new session, and an entire 2025 round-trip
-- + a fresh 2025-12-30 lot get fused into one session_id=1 with
-- closed-loss math even though the 12/30 lot is genuinely OPEN.
-- Real bug: Emmory Investment IYW (2026-05-13) — chart terminal
-- $-1,957 instead of $396.67. Smallest broker fractional share
-- precision is ~0.0001, so 1e-9 is 5 orders of magnitude tighter than
-- any real fractional fill could ever be. ``running_qty > 1e-9``
-- guards the same case symmetrically.
sessions as (
    select
        *,
        sum(
            case
                when abs(prev_running_qty) < 1e-9
                 and running_qty > 1e-9
                then 1
                else 0
            end
        ) over (
            partition by account, user_id, symbol
            order by trade_date, action
            rows between unbounded preceding and current row
        ) as session_id
    from with_prev
),

-- Aggregate each session (trade-derived only). We track buy_qty / sell_qty
-- separately so the closed-session P&L logic below can spot transfer-out
-- residuals (buy_qty > sell_qty AND no current holdings in this account)
-- and value them against the actually-sold shares' cost basis instead of
-- treating the missing shares as a $0-proceeds loss.
trade_session_summary as (
    select
        account,
        user_id,
        symbol,
        session_id,
        min(trade_date)  as open_date,
        max(trade_date)  as last_trade_date,
        max(running_qty) as max_quantity_held,
        sum(amount)      as net_cash_flow,   -- total cash in/out from buys and sells
        sum(case when action = 'equity_buy' then abs(amount) else 0 end)
                         as total_buy_cost,
        sum(case when action = 'equity_buy' then quantity else 0 end)
                         as total_buy_qty,
        sum(case when action in ('equity_sell','equity_sell_short')
                 then quantity else 0 end)
                         as total_sell_qty,
        count(*)         as num_trades
    from sessions
    where session_id > 0   -- exclude orphan trades outside any session (e.g. naked shorts)
    group by 1, 2, 3, 4
),

-- Per (account, user_id, symbol), how many trade sessions exist and what's
-- the highest session_id used. Snapshot-only sessions need a session_id
-- that doesn't collide with any existing trade session (e.g. trade_session
-- with id=1 followed by a fresh transferred-in lot must become id=2, not
-- a duplicate id=1 row that breaks every join keyed on session_id).
trade_sessions_by_symbol as (
    select
        account,
        user_id,
        symbol,
        max(session_id)                                as max_session_id,
        sum(total_buy_qty - total_sell_qty)            as net_open_qty_from_trades,
        sum(case
                when total_buy_qty > total_sell_qty then total_buy_cost
                else 0
            end)                                       as cost_from_open_trade_sessions
    from trade_session_summary
    group by 1, 2, 3
),

-- Equity rows in the current snapshot whose share count is NOT explained
-- by trade history. Two cases:
--
--  (a) symbol+account has no trade rows at all (Schwab-positions-only
--      sync, or pre-history holding). Whole snapshot quantity is unaccounted.
--
--  (b) symbol+account has trade rows but they net to fewer open shares
--      than the snapshot shows (e.g. a closed round-trip of 150 shares
--      followed by a transferred-in lot of 250 shares Schwab's
--      transactions API doesn't surface). Emit a snapshot session for
--      the residual so the page shows the user's actually-held lot
--      with its real cost basis from the snapshot, instead of folding
--      the snapshot's market value into the closed trade session and
--      reporting a phantom +$15K "unrealized" P&L equal to the market
--      value itself. See data-pipeline-fixes.mdc — this surfaced on
--      /position/IREN?account=Cameron+Investment for testingcameron.
snapshot_equity_sessions as (
    select
        c.account,
        c.user_id,
        c.underlying_symbol as symbol,
        coalesce(tsbs.max_session_id, 0) + 1 as session_id,
        coalesce(c.snapshot_date, current_date()) as open_date,
        coalesce(c.snapshot_date, current_date()) as last_trade_date,
        -- "Unaccounted" shares only — the trade-explained portion stays in
        -- the trade session (which keeps Open status when trade qty > 0).
        greatest(
            coalesce(abs(c.quantity), 0) - coalesce(tsbs.net_open_qty_from_trades, 0),
            0
        ) as max_quantity_held,
        -- Cost basis for the unaccounted shares: prefer the snapshot's
        -- per-share avg cost when available, otherwise pro-rata the
        -- snapshot cost basis. Trade-history's cost stays attributed to
        -- the trade session.
        -(
            coalesce(c.cost_basis, 0)
            - coalesce(tsbs.cost_from_open_trade_sessions, 0)
        ) as net_cash_flow,
        coalesce(c.cost_basis, 0) - coalesce(tsbs.cost_from_open_trade_sessions, 0)
            as total_buy_cost,
        greatest(
            coalesce(abs(c.quantity), 0) - coalesce(tsbs.net_open_qty_from_trades, 0),
            0
        ) as total_buy_qty,
        cast(0 as float64)         as total_sell_qty,
        0 as num_trades
    from {{ ref('stg_current') }} c
    left join trade_sessions_by_symbol tsbs
        on tsbs.account = c.account
        -- NULL-safe so legacy rows with user_id IS NULL still match the
        -- snapshot's NULL user_id (Stage 0 leniency); non-NULL on both
        -- sides compares strictly.
        and (tsbs.user_id is not distinct from c.user_id)
        and tsbs.symbol = c.underlying_symbol
    where c.instrument_type = 'Equity'
      and coalesce(c.quantity, 0) != 0
      and trim(coalesce(c.underlying_symbol, '')) != ''
      -- Only emit a snapshot session when trade history doesn't already
      -- explain *any* still-open shares for this (account, symbol).
      -- Two qualifying scenarios:
      --   (a) zero trade rows for this symbol+account at all (LEFT JOIN
      --       leaves tsbs NULL → coalesce = 0)  — pure Schwab-only path
      --   (b) all trade rows net to zero (full round-trip) — the
      --       snapshot represents a separately-acquired lot
      --
      -- We deliberately do NOT emit a snapshot session for the partial
      -- case (trade has some still-open shares + snapshot has more):
      -- attributing market_value to two sessions joined on the same
      -- stg_current row would double-count it. Partial mismatches are a
      -- data integrity issue better surfaced than silently patched.
      and coalesce(tsbs.net_open_qty_from_trades, 0) <= 0
),

session_summary as (
    select * from trade_session_summary
    union all
    select * from snapshot_equity_sessions
),

-- Cross-account holdings of the same (user_id, symbol). When a session
-- closes for one account but the user still holds the symbol elsewhere,
-- the missing shares were almost certainly transferred (Schwab Journal
-- entry with no symbol-bearing trade row). For those sessions, total_pnl
-- and realized_pnl below report the cost basis of the SOLD shares only,
-- not the cost basis of the transferred shares — the latter sits in the
-- destination account and counts there. Without this, JEPI / dividend ETFs
-- that get transferred between joint and individual accounts produce
-- six-figure phantom losses on the position page.
user_total_holdings as (
    select
        user_id,
        underlying_symbol as symbol,
        sum(coalesce(quantity, 0)) as shares_held_anywhere
    from {{ ref('stg_current') }}
    where instrument_type = 'Equity'
      and coalesce(quantity, 0) > 0
      and trim(coalesce(underlying_symbol, '')) != ''
    group by 1, 2
),

-- Identify the latest session per account/symbol (candidate for "Open")
latest_session as (
    select
        account,
        user_id,
        symbol,
        max(session_id) as latest_session_id
    from session_summary
    group by 1, 2, 3
),

final as (
    select
        s.account,
        s.user_id,
        s.symbol,
        s.session_id,
        s.open_date,
        s.last_trade_date,
        s.max_quantity_held,
        s.net_cash_flow,
        s.num_trades,

        -- A session is Open only when:
        --   1. it's the latest session for this (account, user_id, symbol), AND
        --   2. the symbol still appears in the current snapshot, AND
        --   3. this session itself has unsold shares (total_buy_qty > total_sell_qty).
        -- The qty guard matters when a trade-history session ran a full
        -- round-trip back to zero shares but the snapshot still shows the
        -- symbol (because a separate transferred-in / pre-history lot
        -- exists). Without it the closed round-trip used to be rebadged
        -- "Open" and inherit the snapshot's market value as fake
        -- unrealized P&L. The transferred-in lot itself becomes its own
        -- snapshot_equity_sessions row with a fresh session_id.
        case
            when ls.latest_session_id = s.session_id
                 and c.trade_symbol is not null
                 and coalesce(s.total_buy_qty, 0) > coalesce(s.total_sell_qty, 0)
            then 'Open'
            else 'Closed'
        end as status,

        -- Current market data (only meaningful for open sessions). Same
        -- guard as `status`: a closed round-trip whose symbol still has
        -- a snapshot row must NOT pick up the snapshot's market value
        -- (that belongs to the separate snapshot session).
        case
            when ls.latest_session_id = s.session_id
                 and c.trade_symbol is not null
                 and coalesce(s.total_buy_qty, 0) > coalesce(s.total_sell_qty, 0)
            then c.market_value
        end as current_market_value,

        case
            when ls.latest_session_id = s.session_id
                 and c.trade_symbol is not null
                 and coalesce(s.total_buy_qty, 0) > coalesce(s.total_sell_qty, 0)
            then c.current_price
        end as current_price,

        -- Total P&L for OPEN sessions = realized + broker_unrealized.
        --
        --   broker_unrealized = mv − cb           (snapshot, source of truth)
        --   realized          = sell_proceeds − cost_of_sold_lots
        --                     = sell_proceeds − max(0, total_buy_cost − cb_remaining)
        --
        -- Why this shape: the broker tracks per-lot cost basis under
        -- its own accounting (FIFO for Schwab by default). The cost
        -- basis remaining on the snapshot (``cb``) is for the lots
        -- still on the books. The cost basis ATTRIBUTABLE TO THE
        -- SOLD LOTS is therefore (total_buy_cost − cb), and realized
        -- = sell_proceeds − that. For DELL ••••0044 (bought 300 cheap
        -- in Dec, bought 300 expensive on 4/21, sold 300 the same day)
        -- the broker holds the 300 expensive lots as remaining
        -- (cb=$62,934) so realized = $49,500 − ($101,085 − $62,934)
        -- = $49,500 − $38,151 = $11,349 — matches the chart's
        -- running-average walk and the broker's own per-lot books.
        --
        -- The ``max(0, …)`` clamps the JPM/AMZN phantom-share case.
        -- When the broker reports MORE shares than trade history
        -- explains (transfer-ins or sync-dropped journal entries),
        -- ``cb_remaining`` includes cost for shares that never
        -- appeared as buy rows, so ``total_buy_cost − cb_remaining``
        -- goes NEGATIVE. Clamping at 0 means we DON'T credit that
        -- gap as fake realized P&L (the cost is real but tracked
        -- at the originating account if it was a transfer; we
        -- shouldn't double-count it here). Pre-fix the formula was
        -- ``net_cash_flow + market_value`` which treated the phantom
        -- shares as pure profit and inflated JPM by $30k.
        --
        -- Closed session, no transfer-out:        net_cash_flow (buys + sells)
        -- Closed session, transfer-out detected:  realized P&L on sold shares
        --                                         only (sell_proceeds − sold-share
        --                                         cost basis); transferred shares'
        --                                         cost is in the destination account.
        case
            when ls.latest_session_id = s.session_id
                 and c.trade_symbol is not null
                 and coalesce(s.total_buy_qty, 0) > coalesce(s.total_sell_qty, 0)
                then
                    -- realized = sell_proceeds − cost_of_sold_lots
                    -- sell_proceeds = net_cash_flow + total_buy_cost
                    -- cost_of_sold = max(0, total_buy_cost − cb_remaining)
                    (s.net_cash_flow + s.total_buy_cost)
                    - greatest(
                        0,
                        s.total_buy_cost - coalesce(c.cost_basis, 0)
                    )
                    -- broker unrealized
                    + coalesce(c.market_value, 0) - coalesce(c.cost_basis, 0)
            when c.trade_symbol is null
                 and coalesce(s.total_buy_qty, 0) > coalesce(s.total_sell_qty, 0)
                 and coalesce(uth.shares_held_anywhere, 0)
                     >= (coalesce(s.total_buy_qty, 0) - coalesce(s.total_sell_qty, 0))
                then -- realized on sold shares only
                     -- = sell_proceeds_total − avg_cost × sell_qty
                     -- net_cash_flow = sell_proceeds_total − total_buy_cost
                     -- so realized = net_cash_flow + total_buy_cost
                     --              − avg_cost × (buy_qty − sell_qty)
                     -- which simplifies to:
                     -- net_cash_flow + (total_buy_cost / buy_qty) × sell_qty
                     --                             − total_buy_cost × (sell_qty / buy_qty)
                     -- but we keep it explicit for readability:
                round(
                    s.net_cash_flow
                    + safe_divide(
                        s.total_buy_cost,
                        greatest(s.total_buy_qty, s.total_sell_qty)
                    ) * (s.total_buy_qty - s.total_sell_qty),
                    2
                )
            -- Orphan / duplicated buys with no sells and no holdings anywhere:
            -- emit 0 P&L instead of net_cash_flow (= -cost_basis as a phantom
            -- loss). Same reasoning as the parallel guard in
            -- int_closed_equity_legs.writeoffs — without this the strategy
            -- breakdown shows "Buy and Hold Closed -$47,639" for an account
            -- that actually owes the user nothing (cross-tenant pollution from
            -- an old test import, dup buy from a sync regression). Real
            -- transfer-outs are still caught by the prior branch (the user
            -- holds the shares somewhere). See SKILL.md (2026-05-11).
            when c.trade_symbol is null
                 and coalesce(s.total_sell_qty, 0) = 0
                 and coalesce(uth.shares_held_anywhere, 0) = 0
                then 0
            else s.net_cash_flow
        end as total_pnl,

        -- Duration in calendar days
        case
            when ls.latest_session_id = s.session_id
                 and c.trade_symbol is not null
                 and coalesce(s.total_buy_qty, 0) > coalesce(s.total_sell_qty, 0)
            then date_diff(current_date(), s.open_date, day)
            else date_diff(s.last_trade_date, s.open_date, day)
        end as days_held

    from session_summary s
    join latest_session ls
        on s.account = ls.account
        and (s.user_id is not distinct from ls.user_id)
        and s.symbol = ls.symbol
    left join {{ ref('stg_current') }} c
        on s.account = c.account
        and (s.user_id is not distinct from c.user_id)
        and s.symbol = c.underlying_symbol
        and c.instrument_type = 'Equity'
    left join user_total_holdings uth
        on (s.user_id is not distinct from uth.user_id)
        and s.symbol = uth.symbol
)

select * from final
