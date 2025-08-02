select 
    account,
    symbol,
    security_type,

    --Quantities
    sum(trade_quantity) as equity_quantity,

    --Dates
    min(trade_date) as open_equity_date,
    max(trade_date) as close_equity_date,
    
    --Amounts
    sum(trade_amount) as equity_gain_or_loss,
from {{ ref('positions_metadata')}}
group by 1,2,3