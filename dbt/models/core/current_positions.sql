select 
    account,
    symbol,
    trade_symbol,
    quantity,
    amount,
    next_position_amount,
    case 
        when is_current_position_establishement_1_0 = 1 then transaction_date
        else null 
    end as equity_position_open_date,
from {{ ref('history_and_current_combined') }}
