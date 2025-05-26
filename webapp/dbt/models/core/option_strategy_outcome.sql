select 
    account,
    strategy,
    symbol,
    sum(trade_outcome) as strategy_outcome,
from {{ ref('option_position_strategy_outcome')}}
group by 1,2,3