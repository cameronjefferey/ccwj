with fix as (
select 
    account,
    date as old_date,
    cast(coalesce(SAFE.PARSE_DATE('%m/%d/%y',split(date," ")[0]),SAFE.PARSE_DATE('%m/%d/%Y',split(date," ")[0]))as date) as transaction_date,
    lower(action) as action,
    
    symbol as trade_symbol,
    trim(split(symbol," ")[0]) as symbol,
    split(symbol," ")[SAFE_OFFSET(1)] as option_expiration_date,
    split(symbol," ")[SAFE_OFFSET(2)] as option_expiration_price,
    split(symbol," ")[SAFE_OFFSET(3)] as option_security_type,
    case 
        when action in ('Buy','Sell') then 'Equity'
        when split(symbol," ")[SAFE_OFFSET(3)]  = 'C' then 'Option'
    end as security_type,
    case 
        when action in ('Assigned','Expired','Buy to Close','Sell to Close') then 'Close' 
        else 'Open'
    end as trade_type,
    SAFE_CAST(quantity as FLOAT64)as quantity,
    price,
    fees_and_comm,
    SAFE_CAST(amount as FLOAT64) as amount,
    SAFE_CAST(amount as FLOAT64)/nullif(SAFE_CAST(quantity as FLOAT64),0) as cost_per_share,
from {{ ref('0417_history')}}
)
select 
    *
from fix 
order by transaction_date desc 


