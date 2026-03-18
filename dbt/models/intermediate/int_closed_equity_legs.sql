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

running as (
    select
        *,
        sum(signed_quantity) over (
            partition by account, symbol
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
                partition by account, symbol
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
            partition by account, symbol
            order by trade_date, action
            rows between unbounded preceding and current row
        ) as session_id
    from with_prev
),

session_avg_cost as (
    select
        account,
        symbol,
        session_id,
        min(trade_date) as session_open_date,
        sum(case when action = 'equity_buy' then abs(amount) else 0 end) as total_buy_cost,
        sum(case when action = 'equity_buy' then quantity else 0 end)    as total_buy_qty
    from sessions
    where session_id > 0
    group by 1, 2, 3
),

sell_events as (
    select
        s.account,
        s.symbol,
        s.trade_symbol,
        s.session_id,
        sac.session_open_date                                             as open_date,
        s.trade_date                                                      as close_date,
        s.quantity                                                        as sell_qty,
        s.amount                                                          as sell_proceeds,
        safe_divide(sac.total_buy_cost, sac.total_buy_qty)               as avg_cost_per_share,
        round(safe_divide(s.amount, s.quantity), 2)                       as sale_price_per_share,
        round(safe_divide(sac.total_buy_cost, sac.total_buy_qty) * s.quantity, 2) as cost_basis,
        round(
            s.amount - safe_divide(sac.total_buy_cost, sac.total_buy_qty) * s.quantity,
            2
        ) as realized_pnl
    from sessions s
    join session_avg_cost sac
        on  s.account    = sac.account
        and s.symbol     = sac.symbol
        and s.session_id = sac.session_id
    where s.action in ('equity_sell', 'equity_sell_short')
      and s.session_id > 0
)

select
    account,
    symbol,
    trade_symbol,
    session_id,
    open_date,
    close_date,
    sell_qty       as quantity,
    sale_price_per_share,
    sell_proceeds,
    cost_basis,
    realized_pnl,
    'Closed'       as status,
    'Equity Sold'  as description
from sell_events
order by account, symbol, close_date
