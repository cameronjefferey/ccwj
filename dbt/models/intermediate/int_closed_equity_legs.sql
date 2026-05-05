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

sessions as (
    select
        *,
        sum(
            case when prev_running_qty = 0 and running_qty > 0 then 1 else 0 end
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

-- Synthetic write-off row for any session where the trade history shows
-- fewer shares sold than bought yet the session has closed (e.g. the symbol
-- is no longer in current holdings). The residual cost basis is the loss
-- caused by missing trade history (transfers, corporate actions, etc.) and
-- is needed so the sum of realized P&L per leg reconciles to the session's
-- actual net cash flow (which is what positions_summary reports).
session_status as (
    select account, user_id, symbol, session_id, status, last_trade_date
    from {{ ref('int_equity_sessions') }}
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
    where ss.status = 'Closed'
      and sac.total_buy_qty > sac.total_sell_qty
      and (sac.total_buy_qty - sac.total_sell_qty) > 0
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
