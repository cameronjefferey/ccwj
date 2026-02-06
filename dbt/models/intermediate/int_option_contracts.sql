/*
    Option contract lifecycle.

    Groups every trade on the same option contract (account + trade_symbol)
    into a single row with:
      - direction (Sold / Bought)
      - premiums collected / paid
      - closing info (Expired, Assigned, Closed, Exercised)
      - total P&L including unrealised component for open contracts
*/

with option_trades as (
    select
        account,
        trade_symbol,
        underlying_symbol,
        option_expiry,
        option_strike,
        option_type,
        trade_date,
        action,
        quantity,
        amount,
        fees
    from {{ ref('stg_history') }}
    where instrument_type in ('Call', 'Put')
),

-- Predominant direction per contract (for signing expired / assigned quantities)
direction_lookup as (
    select
        account,
        trade_symbol,
        sum(case when action = 'option_sell_to_open' then quantity else 0 end) as total_sto_qty,
        sum(case when action = 'option_buy_to_open'  then quantity else 0 end) as total_bto_qty,
        case
            when sum(case when action = 'option_sell_to_open' then quantity else 0 end)
              >= sum(case when action = 'option_buy_to_open'  then quantity else 0 end)
            then 'Sold'
            else 'Bought'
        end as direction
    from option_trades
    group by 1, 2
),

contract_summary as (
    select
        o.account,
        o.trade_symbol,
        o.underlying_symbol,
        max(o.option_expiry)  as option_expiry,
        max(o.option_strike)  as option_strike,
        max(o.option_type)    as option_type,
        d.direction,

        -- Dates
        min(o.trade_date)  as open_date,
        max(o.trade_date)  as close_date,

        -- Quantities
        sum(case when o.action = 'option_sell_to_open' then o.quantity else 0 end) as contracts_sold_to_open,
        sum(case when o.action = 'option_buy_to_open'  then o.quantity else 0 end) as contracts_bought_to_open,
        sum(case when o.action in (
            'option_buy_to_close', 'option_sell_to_close',
            'option_expired', 'option_assigned', 'option_exercised'
        ) then o.quantity else 0 end) as contracts_closed,

        -- Cash flows
        sum(case when o.action = 'option_sell_to_open'  then o.amount else 0 end) as premium_received,
        sum(case when o.action = 'option_buy_to_open'   then o.amount else 0 end) as premium_paid,
        sum(case when o.action = 'option_buy_to_close'  then o.amount else 0 end) as cost_to_close,
        sum(case when o.action = 'option_sell_to_close' then o.amount else 0 end) as proceeds_from_close,
        sum(o.amount) as net_cash_flow,
        sum(o.fees)   as total_fees,

        -- How the contract was closed (highest-priority terminal event wins)
        max(case
            when o.action = 'option_assigned'  then 'Assigned'
            when o.action = 'option_exercised' then 'Exercised'
            when o.action = 'option_expired'   then 'Expired'
            when o.action in ('option_buy_to_close', 'option_sell_to_close') then 'Closed'
        end) as close_type,

        count(*) as num_trades

    from option_trades o
    join direction_lookup d
        on o.account = d.account
        and o.trade_symbol = d.trade_symbol
    group by o.account, o.trade_symbol, o.underlying_symbol, d.direction
)

select
    c.*,

    -- Status
    case
        when cur.trade_symbol is not null    then 'Open'
        when c.close_type is not null        then 'Closed'
        when c.option_expiry < current_date() then 'Closed'   -- expired without explicit event
        else 'Open'
    end as status,

    -- Current market data for open contracts
    coalesce(cur.market_value, 0)    as current_market_value,
    coalesce(cur.unrealized_pnl, 0)  as current_unrealized_pnl,

    -- Total P&L  (for short options, market_value is negative = cost to buy back)
    case
        when cur.trade_symbol is not null
        then c.net_cash_flow + coalesce(cur.market_value, 0)
        else c.net_cash_flow
    end as total_pnl,

    -- Duration
    date_diff(
        case
            when cur.trade_symbol is not null then current_date()
            else coalesce(c.close_date, current_date())
        end,
        c.open_date,
        day
    ) as days_in_trade

from contract_summary c
left join {{ ref('stg_current') }} cur
    on c.account = cur.account
    and c.trade_symbol = cur.trade_symbol
    and cur.instrument_type in ('Call', 'Put')
