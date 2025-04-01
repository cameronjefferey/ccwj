select 
    account,
    current_date() as transaction_date,
    sum(market_value) as market_value,
from {{ ref('accounts') }}
where trade_symbol = 'Account Total'
group by 1,2
