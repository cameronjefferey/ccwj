select 
    calendar_symbol_dates.day,
    calendar_symbol_dates.symbol,
    'Options' as equity_type,
    historical_options_held.amount as total,
    

from {{ ref('calendar_symbol_dates')}}
    left join {{ ref('historical_options_held')}} using (symbol,day)
    left join {{ ref('historical_equity_held')}} using (symbol,day)
UNION ALL 
select 
    calendar_symbol_dates.day,
    calendar_symbol_dates.symbol,
    'Stock' as equity_type,
    historical_equity_held.total_gain_or_loss as total,

from {{ ref('calendar_symbol_dates')}}
    left join {{ ref('historical_equity_held')}} using (symbol,day)

UNION ALL 
select 
    calendar_symbol_dates.day,
    calendar_symbol_dates.symbol,
    'Total' as equity_type,
    historical_equity_held.total_gain_or_loss + historical_options_held.amount as total,
    

from {{ ref('calendar_symbol_dates')}}
    left join {{ ref('historical_options_held')}} using (symbol,day)
    left join {{ ref('historical_equity_held')}} using (symbol,day)