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

-- Assign session IDs: increment each time the position transitions from 0 → positive
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

-- Aggregate each session (trade-derived only)
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
        count(*)         as num_trades
    from sessions
    where session_id > 0   -- exclude orphan trades outside any session (e.g. naked shorts)
    group by 1, 2, 3, 4
),

-- Equity rows in current snapshot with no equity trade history (e.g. Schwab-only)
snapshot_equity_sessions as (
    select
        c.account,
        c.user_id,
        c.underlying_symbol as symbol,
        1 as session_id,
        coalesce(c.snapshot_date, current_date()) as open_date,
        coalesce(c.snapshot_date, current_date()) as last_trade_date,
        coalesce(abs(c.quantity), 0) as max_quantity_held,
        -coalesce(c.cost_basis, 0) as net_cash_flow,
        0 as num_trades
    from {{ ref('stg_current') }} c
    where c.instrument_type = 'Equity'
      and coalesce(c.quantity, 0) != 0
      and trim(coalesce(c.underlying_symbol, '')) != ''
      and not exists (
          select 1
          from trade_session_summary t
          where t.account = c.account
            -- Match user_id with a NULL-safe comparison: both NULL is a
            -- legacy-row match (Stage 0 backfill state); both non-NULL
            -- compares strictly. Without the NULL-safe equality
            -- ``t.user_id = c.user_id`` would silently miss legacy rows
            -- and we'd double-count snapshot sessions on top of trade
            -- sessions for the same holding.
            and (t.user_id is not distinct from c.user_id)
            and t.symbol = c.underlying_symbol
      )
),

session_summary as (
    select * from trade_session_summary
    union all
    select * from snapshot_equity_sessions
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
        and (s.user_id is not distinct from ls.user_id)
        and s.symbol = ls.symbol
    left join {{ ref('stg_current') }} c
        on s.account = c.account
        and (s.user_id is not distinct from c.user_id)
        and s.symbol = c.underlying_symbol
        and c.instrument_type = 'Equity'
)

select * from final
