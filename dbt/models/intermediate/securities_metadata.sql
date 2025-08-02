with equity_metadata as (
select 
    account,
    symbol,
    session_id,
    max(quantity) as equity_quantity,
    sum(amount) as equity_gain_or_loss,
    min(transaction_date) as open_equity_date,
    max(transaction_date) as close_equity_date,
from {{ ref('position_equity_sessions') }}
group by 1,2,3


)


, trades_metadata as (
select 
    account,
    symbol,
    trade_symbol,
    security_type,
    action,
    --Quantities
    --Only need to know how many were opened since open will always equal closed
    abs(sum(case when action in {{ buy_actions() }} then quantity end)) as trade_quantity,

    --Dates
    min(case when action in {{ buy_actions() }} then transaction_date end) open_trade_date,
    coalesce(max(case when action in {{ sell_actions() }} then transaction_date end),current_date()) close_trade_date,
    
    --Amounts
    sum(case when action in {{ buy_actions() }} then amount end) open_trade_amount,
    sum(case when action in {{ sell_actions() }} then amount end) close_trade_amount,
    sum(amount) as trade_gain_or_loss,

    
from {{ ref('history_and_current_combined')}}
where 1=1
group by 1,2,3,4,5
)
select 
    trades_metadata.*,
    --Security Trade Order 
    row_number() over (partition by account, symbol, security_type order by open_trade_date, close_trade_date,open_trade_amount) as position_trade_order,

    --Equity Trade Order 
    case when security_type = 'Equity' then row_number() over (partition by account, symbol, security_type order by open_trade_date, close_trade_date,open_trade_amount) end as equity_trade_order
from trades_metadata
where 1=1