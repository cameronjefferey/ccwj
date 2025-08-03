select 
    account,
    symbol,
    security_type,

    --Quantities
    sum(position_quantity) as equity_quantity,

    --Dates
    min(open_position_date) as open_equity_date,
    max(close_position_date) as close_equity_date,
    
    --Amounts
    sum(position_gain_or_loss) as equity_gain_or_loss,
from {{ ref('positions_metadata')}}
group by 1,2,3