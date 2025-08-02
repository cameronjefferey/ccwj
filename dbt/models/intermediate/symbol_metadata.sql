select 
    account,
    symbol,

    --Quantities
    sum(equity_quantity) as position_quantity,

    --Dates
    min(open_equity_date) as open_position_date,
    max(close_equity_date) as close_position_date,
    
    --Amounts
    sum(equity_gain_or_loss) as position_gain_or_loss,
from {{ ref('equity_metadata')}}
group by 1,2