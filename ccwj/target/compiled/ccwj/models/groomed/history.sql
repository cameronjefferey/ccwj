with fix as (
select 
    date as old_date,
    cast(coalesce(SAFE.PARSE_DATE('%m/%d/%y',split(date," ")[0]),SAFE.PARSE_DATE('%m/%d/%Y',split(date," ")[0]))as date) as date,
    action,
    
    symbol as trade_symbol,
    split(symbol," ")[0] as symbol,
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
    quantity,
    price,
    fees_and_comm,
    amount as amount_old,
    CAST(REGEXP_REPLACE(case 
       when cast(amount as string) LIKE '($%)' THEN CONCAT('-', REPLACE(REPLACE(cast(amount as string), ')', ''), '($', ''))
       when cast(amount as string) LIKE '$%' THEN REPLACE(REPLACE(cast(amount as string), ')', ''), '$', '')
       when action in ('Expired','Assigned','Journal') then '0'
       else cast(amount as string)
    end, r'\$|,', '') as FLOAT64) as amount
from `ccwj-dbt`.`analytics`.`0417_history`
)
select 
    *
from fix 
where symbol in ('CFLT','ONON','JXN','ASTS')
order by date desc