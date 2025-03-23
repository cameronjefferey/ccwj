select 
    'current' as transaction_type,
    account,
    trade_symbol,
    sum(market_value) as market_value,
    sum(gain_or_loss_dollar) as gain_or_loss_dollar,

from {{ ref('current')}}
where trade_symbol = 'Account Total'
group by 1,2,3

UNION ALL 

select 
    'historical' as transaction_type,
    account,
    trade_symbol,
    sum(null) as market_value,
    sum(amount) as gain_or_loss_dollar 
from {{ ref('history')}}
group by 1,2,3
