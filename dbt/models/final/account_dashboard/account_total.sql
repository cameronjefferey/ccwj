select 
    account,
    sum(market_value) as market_value,
from {{ ref('accounts') }}
group by 1
