select 
    account,
    symbol,
    security_type,
    trade_symbol,

    --Quantities
    sum(abs(trade_quantity/2)) as equity_quantity,

    --Dates
    min(trade_date) as open_equity_date,
    max(trade_date) as close_equity_date,
    
    --Amounts
    sum(trade_amount) as equity_gain_or_loss,
from {{ ref('trades_metadata')}}
group by 1,2,3,4