
with option_outcome as (
select 
    account,
    trade_symbol,
    sum(amount) as trade_outcome,
from {{ ref('history_and_current_combined')}}

group by 1,2
)

, trading_info as (
select 
    distinct history_and_current_combined.account,
    history_and_current_combined.symbol,
    history_and_current_combined.trade_symbol,
    coalesce(case when history_and_current_combined.security_type = 'Option' then history_and_current_combined.quantity*100 else history_and_current_combined.quantity end,0) as option_quantity,
    coalesce(equity_held_daily.cumulative_equity_quantity,0) as cumulative_equity_quantity,
    option_outcome.trade_outcome,

from {{ ref('history_and_current_combined')}}
    left join {{ ref('equity_held_daily')}}
        on history_and_current_combined.transaction_date = equity_held_daily.day
        and equity_held_daily.symbol = history_and_current_combined.symbol
    left join option_outcome 
        on history_and_current_combined.account = option_outcome.account
        and history_and_current_combined.trade_symbol = option_outcome.trade_symbol
where history_and_current_combined.action in ('sell to open','buy to open')
)
, final as (
select 
    *,
        case 
        when cumulative_equity_quantity >= option_quantity then 'Covered Call' 
        when cumulative_equity_quantity = 0 and option_quantity > 0 then 'Naked Call'
        when cumulative_equity_quantity = 0 and option_quantity < 0 then 'Naked Put'
        when cumulative_equity_quantity > 0 and option_quantity > 0 then 'Collar'
        when cumulative_equity_quantity > 0 and option_quantity < 0 then 'Protective Put'
        else 'Stock'
    end as strategy,

from trading_info 
) 
select * 
from final 