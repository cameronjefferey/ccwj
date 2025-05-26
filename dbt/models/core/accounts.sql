select 
    'current' as transaction_type,
    account,
    trade_symbol,
    transaction_date,
    'current' as position_status,
    sum(market_value) as market_value,
    sum(gain_or_loss_dollar) as gain_or_loss_dollar,

from {{ ref('current')}}
group by 1,2,3,4,5

UNION ALL 

select 
    'historical' as transaction_type,
    history.account,
    history.trade_symbol,
    history.transaction_date,
    case 
        when c.symbol is not null then 'current'
        else 'historical'
    end as position_status,
    sum(null) as market_value,
    sum(history.amount) as gain_or_loss_dollar 
from {{ ref('history')}}
    left join {{ ref('current')}} as c 
        on history.trade_symbol = c.trade_symbol
            and history.account = c.account
group by 1,2,3,4,5
