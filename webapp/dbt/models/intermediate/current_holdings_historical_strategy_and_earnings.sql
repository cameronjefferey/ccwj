select 
    calendar_symbol_dates.day,
    calendar_symbol_dates.symbol,
    current_equity_historical_earnings.equity_shares_quantity,
    current_equity_historical_earnings.historical_equity_holdings,
    current_option_historical_earnings.options_quantity,
    current_option_historical_earnings.amount,
    current_option_historical_earnings.daily_options_value,
    case 
        when coalesce(equity_shares_quantity,0) = 0 and coalesce(options_quantity,0) = 0 then 'No Position'
        when coalesce(equity_shares_quantity,0) = coalesce(options_quantity,0)*100 then 'Covered Call'
        when coalesce(equity_shares_quantity,0) = 0 and coalesce(options_quantity,0)*100 > 0 then 'Naked Calls'
        when coalesce(equity_shares_quantity,0) > 0 and coalesce(options_quantity,0)*100 = 0 then 'Buy and Hold'
        else 'Mixed'
    end as daily_strategy
from {{ ref('calendar_symbol_dates')}}
 left join {{ ref('current_equity_historical_earnings')}}
    on current_equity_historical_earnings.day = calendar_symbol_dates.day 
        and current_equity_historical_earnings.symbol = calendar_symbol_dates.symbol 
left join {{ ref('current_option_historical_earnings')}}
    on current_option_historical_earnings.day = calendar_symbol_dates.day
        and current_option_historical_earnings.symbol = calendar_symbol_dates.symbol 

where 1=1
   and calendar_symbol_dates.day BETWEEN '2024-01-01' and current_date()
   and calendar_symbol_dates.symbol = 'ASTS'