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
    running_options_gain_or_loss as gain_or_loss 
from {{ ref('daily_equity_held')}}
UNION ALL 
select 
    daily_equity_held.day,
    symbol,
    account,
    'Dividend Paid' as security_type,
    dividends_paid as gain_or_loss 
from {{ ref('daily_equity_held')}}



UNION ALL 
select 
    daily_equity_held.day,
    symbol,
    account,
    'Total' as security_type,
    case when coalesce(equity_gain_or_loss,0) + coalesce(running_options_gain_or_loss,0) + coalesce(dividends_paid,0) = 0 then null 
    else coalesce(equity_gain_or_loss,0) + coalesce(running_options_gain_or_loss,0) + coalesce(dividends_paid,0) end as gain_or_loss 
from {{ ref('daily_equity_held')}}