select 
    account,
    symbol,
    security_type,
    trade_symbol,

    --Quantities
    max(abs(coalesce(trade_quantity,0))) as position_quantity,

    --Dates
    min(trade_date) as open_position_date,
    max(trade_date) as close_position_date,
    
    --Amounts
    sum(trade_amount) as position_gain_or_loss,
from {{ ref('trades_metadata')}}
group by 1,2,3,4