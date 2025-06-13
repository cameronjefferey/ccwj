select 
    daily_equity_held.day,
    symbol,
    account,
    'Stock' as security_type,
    equity_gain_or_loss as gain_or_loss 
from {{ ref('daily_equity_held')}}
UNION ALL 
select 
    daily_equity_held.day,
    symbol,
    account,
    'Options Sold' as security_type,
    option_calls_sold_gain_or_loss as gain_or_loss 
from {{ ref('daily_equity_held')}}