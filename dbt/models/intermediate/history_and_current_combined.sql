with combined as (
select 
    account,
    transaction_date,
    trade_symbol,
    action,
    symbol,
    case when security_type = 'ETFs & Closed End Funds' then 'Equity' else security_type end as security_type,
    option_expiration_date,
    option_expiration_price,
    option_security_type,
    quantity,
    price,
    0 as fees_and_comm,
    market_value as amount,
    cost_basis,
    cost_per_share,

from {{ ref('current')}}

UNION ALL 

select 
    account,
    transaction_date,
    trade_symbol,
    action,
    symbol,
    security_type,
    option_expiration_date,
    option_expiration_price,
    option_security_type,
    quantity,
    price,
    fees_and_comm,
    amount,
    0 as cost_basis,
    cost_per_share,
    


from {{ ref('history')}}
)
select * 
from combined 
where 1=1
