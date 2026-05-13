/*
    Closed equity legs — one row per equity sell event within a session.

    Complements option legs from int_strategy_classification so the
    positions table can show the full story: shares bought, shares sold
    (e.g. called away via assignment), and remaining open equity.

    Realized P&L per sell uses the session-level average buy cost:
        realized = sell_proceeds − avg_cost_per_share × qty_sold

    This is the standard average-cost method.  It is exact when all buys
    precede all sells (the common case for covered-call positions).
*/

with equity_trades as (
    select
        account,
        user_id,
        underlying_symbol as symbol,
        trade_date,
        trade_symbol,
        action,
        quantity,
        amount,
        case
            when action = 'equity_buy'        then  quantity
            when action = 'equity_sell'       then -quantity
            when action = 'equity_sell_short' then -quantity
            else 0
        end as signed_quantity
    from {{ ref('stg_history') }}
    where instrument_type = 'Equity'
),

-- Window keyed by (account, user_id, symbol) so cross-tenant rows with
-- the same account label can never share a running quantity series. See
-- docs/USER_ID_TENANCY.md.
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

-- Float-precision-aware zero check (1e-9 share epsilon).
-- See identical guard in `int_equity_sessions`.sessions for the bug
-- this prevents (Emmory IYW round-trip + new lot fused into one
-- session_id because IEEE 754 sum returned -1e-17 instead of 0).
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

session_avg_cost as (
    select
        account,
        user_id,
        symbol,
        session_id,
        min(trade_date) as session_open_date,
        sum(case when action = 'equity_buy' then abs(amount) else 0 end) as total_buy_cost,
        sum(case when action = 'equity_buy' then quantity else 0 end)    as total_buy_qty,
        sum(case when action in ('equity_sell','equity_sell_short')
                 then quantity else 0 end)                                as total_sell_qty
    from sessions
    where session_id > 0
    group by 1, 2, 3, 4
),

-- Avg cost per share. Use the LARGER of total_buy_qty and total_sell_qty as
-- the denominator so the sum of realized P&L across sell events reconciles
-- to the session's actual net cash flow (= true realized P&L) even when the
-- trade history has more sells than buys (e.g. transfers-in or pre-history
-- holdings that aren't represented as buy rows).
sell_events as (
    select
        s.account,
        s.user_id,
        s.symbol,
        s.trade_symbol,
        s.session_id,
        sac.session_open_date                                             as open_date,
        s.trade_date                                                      as close_date,
        s.quantity                                                        as sell_qty,
        s.amount                                                          as sell_proceeds,
        safe_divide(
            sac.total_buy_cost,
            greatest(sac.total_buy_qty, sac.total_sell_qty)
        ) as avg_cost_per_share,
        round(safe_divide(s.amount, s.quantity), 2)                       as sale_price_per_share,
        round(
            safe_divide(
                sac.total_buy_cost,
                greatest(sac.total_buy_qty, sac.total_sell_qty)
            ) * s.quantity,
            2
        ) as cost_basis,
        round(
            s.amount - safe_divide(
                sac.total_buy_cost,
                greatest(sac.total_buy_qty, sac.total_sell_qty)
            ) * s.quantity,
            2
        ) as realized_pnl
    from sessions s
    join session_avg_cost sac
        on  s.account    = sac.account
        and (s.user_id is not distinct from sac.user_id)
        and s.symbol     = sac.symbol
        and s.session_id = sac.session_id
    where s.action in ('equity_sell', 'equity_sell_short')
      and s.session_id > 0
),

-- Residual rows: when the trade history shows fewer shares sold than bought
-- yet the session has closed (the symbol is no longer in current holdings
-- for THIS account), the missing shares went somewhere — most often a
-- Schwab Journal entry that transferred them to another account in the same
-- portfolio. We previously labeled the residual a "Cost Written Off" loss,
-- which double-counted the cost basis on the position page (e.g. JEPI/0044:
-- bought 2,000, sold 1,000, transferred 1,000 to /4828; we'd report a
-- $54,973 fake loss instead of the actual $2,681 gain on the 1,000 sold).
--
-- The fix: cross-reference the user's CURRENT holdings of the same symbol
-- across every account. If the residual qty is plausibly explained by
-- shares held in another account (>= residual qty), suppress the writeoff —
-- the cost basis isn't lost, it's just sitting in the destination account.
-- Only emit a writeoff when no current holdings can absorb the residual
-- (genuine missing trade history: corporate action, transfer-out off the
-- platform, etc.). Sum-of-realized-per-leg then still reconciles to the
-- session's actual realized P&L on the sold shares only.
session_status as (
    select account, user_id, symbol, session_id, status, last_trade_date
    from {{ ref('int_equity_sessions') }}
),

-- Sum of currently-held shares of each (user_id, symbol) across accounts the
-- user owns. Used to decide whether a residual buy_qty − sell_qty is a
-- transfer (shares exist elsewhere) vs. genuinely missing trade history.
user_other_holdings as (
    select
        user_id,
        underlying_symbol as symbol,
        sum(coalesce(quantity, 0)) as shares_held_elsewhere
    from {{ ref('stg_current') }}
    where instrument_type = 'Equity'
      and coalesce(quantity, 0) > 0
      and trim(coalesce(underlying_symbol, '')) != ''
    group by 1, 2
),

-- Same-account snapshot of open equity qty (aggregates across NULL vs
-- populated user_id rows). When legacy session rows have user_id NULL but
-- the live lot is stamped with a populated id (or vice versa),
-- ``user_other_holdings`` alone misses intra-account holdings and we emit a
-- phantom "Cost Written Off" for shares that are still on the books (IYW
-- Dec 2025: buy-and-hold replot as -100% loss + chart vs hero split).
account_symbol_holdings as (
    select
        account,
        trim(coalesce(underlying_symbol, '')) as symbol,
        sum(abs(coalesce(quantity, 0))) as shares_held_on_account
    from {{ ref('stg_current') }}
    where instrument_type = 'Equity'
      and coalesce(quantity, 0) != 0
      and trim(coalesce(underlying_symbol, '')) != ''
    group by 1, 2
),

writeoffs as (
    select
        sac.account,
        sac.user_id,
        sac.symbol,
        sac.symbol                                                      as trade_symbol,
        sac.session_id,
        sac.session_open_date                                           as open_date,
        ss.last_trade_date                                              as close_date,
        sac.total_buy_qty - sac.total_sell_qty                          as sell_qty,
        cast(0 as float64)                                              as sell_proceeds,
        cast(0 as float64)                                              as sale_price_per_share,
        round(
            safe_divide(
                sac.total_buy_cost,
                greatest(sac.total_buy_qty, sac.total_sell_qty)
            ) * (sac.total_buy_qty - sac.total_sell_qty),
            2
        ) as cost_basis,
        round(
            -safe_divide(
                sac.total_buy_cost,
                greatest(sac.total_buy_qty, sac.total_sell_qty)
            ) * (sac.total_buy_qty - sac.total_sell_qty),
            2
        ) as realized_pnl
    from session_avg_cost sac
    join session_status ss
        on sac.account    = ss.account
        and (sac.user_id is not distinct from ss.user_id)
        and sac.symbol     = ss.symbol
        and sac.session_id = ss.session_id
    left join user_other_holdings uoh
        on (sac.user_id is not distinct from uoh.user_id)
        and sac.symbol = uoh.symbol
    left join account_symbol_holdings ash
        on sac.account = ash.account
        and sac.symbol = ash.symbol
    where ss.status = 'Closed'
      and sac.total_buy_qty > sac.total_sell_qty
      and (sac.total_buy_qty - sac.total_sell_qty) > 0
      -- Suppress writeoff when the residual is plausibly a transfer to
      -- another account the user owns. coalesce → 0 when no shares are
      -- held anywhere; that's the only case the residual truly is lost.
      and coalesce(uoh.shares_held_elsewhere, 0)
          < (sac.total_buy_qty - sac.total_sell_qty)
      -- Suppress when the symbol is still held on THIS account under any
      -- user_id stamp (same-tenant NULL vs populated split).
      and coalesce(ash.shares_held_on_account, 0)
          < (sac.total_buy_qty - sac.total_sell_qty)
      -- Defensive guard against orphan / duplicated buy rows: when trade
      -- history shows pure buys with NO sells AND the user holds zero
      -- shares of this symbol anywhere, the row is almost certainly
      -- orphan noise (cross-tenant pollution from an old test import,
      -- a duplicated buy from a sync regression, etc.) rather than a
      -- real transfer-out we want to surface as a writeoff loss. A real
      -- transfer-out path always involves at least one sell, an
      -- assignment, or a still-extant position somewhere — none of
      -- which is true here. Skipping the writeoff makes the page
      -- silently drop the phantom "Cost Written Off -$47,639" row that
      -- the May 2026 BE/Sara position-page screenshot reported.
      -- See ~/.cursor/skills/schwab-sync-safety/SKILL.md (2026-05-11).
      and not (sac.total_sell_qty = 0
               and coalesce(uoh.shares_held_elsewhere, 0) = 0)
),

all_legs as (
    select
        account, user_id, symbol, trade_symbol, session_id, open_date, close_date,
        sell_qty as quantity, sale_price_per_share, sell_proceeds,
        cost_basis, realized_pnl,
        'Closed' as status, 'Equity Sold' as description
    from sell_events
    union all
    select
        account, user_id, symbol, trade_symbol, session_id, open_date, close_date,
        sell_qty as quantity, sale_price_per_share, sell_proceeds,
        cost_basis, realized_pnl,
        'Closed' as status, 'Cost Written Off' as description
    from writeoffs
)

select * from all_legs
order by account, symbol, close_date
