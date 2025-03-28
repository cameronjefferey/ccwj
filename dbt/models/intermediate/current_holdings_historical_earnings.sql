with combined as (
select 
    'Equity' as security_type,
    current_equity_historical_earnings.day,
    current_equity_historical_earnings.symbol,
    current_equity_historical_earnings.equity_shares_quantity as security_quantity,
    current_equity_historical_earnings.historical_equity_holdings as daily_earnings,

from {{ ref('current_equity_historical_earnings')}}
UNION ALL 
select 
    'Options' as security_type,
    current_option_historical_earnings.day,
    current_option_historical_earnings.symbol,
    current_option_historical_earnings.options_quantity as security_quantity,
    current_option_historical_earnings.amount as daily_earnings,

from {{ ref('current_option_historical_earnings')}}
)
select *
from combined 
where EXTRACT(DAYOFWEEK from day) in (2,3,4,5,6)