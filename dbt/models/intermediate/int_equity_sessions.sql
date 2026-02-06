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

-- Running share count
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

-- Previous running quantity (to detect 0 → positive transitions)
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

-- Assign session IDs: increment each time the position transitions from 0 → positive
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

-- Aggregate each session
session_summary as (
    select
        account,
        symbol,
        session_id,
        min(trade_date)  as open_date,
        max(trade_date)  as last_trade_date,
        max(running_qty) as max_quantity_held,
        sum(amount)      as net_cash_flow,   -- total cash in/out from buys and sells
        count(*)         as num_trades
    from sessions
    where session_id > 0   -- exclude orphan trades outside any session (e.g. naked shorts)
    group by 1, 2, 3
),

-- Identify the latest session per account/symbol (candidate for "Open")
latest_session as (
    select
        account,
        symbol,
        max(session_id) as latest_session_id
    from session_summary
    group by 1, 2
),

final as (
    select
        s.account,
        s.symbol,
        s.session_id,
        s.open_date,
        s.last_trade_date,
        s.max_quantity_held,
        s.net_cash_flow,
        s.num_trades,

        -- A session is Open only if it's the latest one AND the symbol still appears in current holdings
        case
            when ls.latest_session_id = s.session_id
                 and c.trade_symbol is not null
            then 'Open'
            else 'Closed'
        end as status,

        -- Current market data (only meaningful for open sessions)
        case
            when ls.latest_session_id = s.session_id and c.trade_symbol is not null
            then c.market_value
        end as current_market_value,

        case
            when ls.latest_session_id = s.session_id and c.trade_symbol is not null
            then c.current_price
        end as current_price,

        -- Total P&L
        --   Closed session:  sum of all cash flows (buy amounts + sell amounts)
        --   Open session:    cash flows + current market value of remaining shares
        case
            when ls.latest_session_id = s.session_id and c.trade_symbol is not null
            then s.net_cash_flow + coalesce(c.market_value, 0)
            else s.net_cash_flow
        end as total_pnl,

        -- Duration in calendar days
        case
            when ls.latest_session_id = s.session_id and c.trade_symbol is not null
            then date_diff(current_date(), s.open_date, day)
            else date_diff(s.last_trade_date, s.open_date, day)
        end as days_held

    from session_summary s
    join latest_session ls
        on s.account = ls.account
        and s.symbol = ls.symbol
    left join {{ ref('stg_current') }} c
        on s.account = c.account
        and s.symbol = c.underlying_symbol
        and c.instrument_type = 'Equity'
)

select * from final
