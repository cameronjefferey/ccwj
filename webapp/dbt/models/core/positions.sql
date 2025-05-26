with active_positions as (
select 
    symbol,
    description,
    quantity,
    day_change_dollar,
    cost_basis,
    
from {{ ref('current')}}

)

, history as (
select 
    history.*
from {{ ref('history')}}
    left join {{ ref('current')}} as c 
        on c.trade_symbol = history.trade_symbol
where c.trade_symbol is null 
)
select 
*
from active_positions