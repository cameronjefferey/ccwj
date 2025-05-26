with flattened as (
select 
    account,
    symbol,
    transaction_date,
    coalesce(max(case when security_type = 'Equity' then quantity end),0) as number_of_shares,
    coalesce(max(case when security_type = 'Option' then quantity*100 end),0) as number_of_options,

    coalesce(max(case when security_type = 'Equity' then market_value end),0) as shares_value,
    coalesce(max(case when security_type = 'Option' then market_value end),0) as options_value,


from {{ ref('current_positions')}}
group by 1,2,3
)
select 
    account,
    symbol,
    transaction_date,
    case 
        when number_of_shares + number_of_options = 0 then 'Covered Call' 
        when number_of_shares = 0 and number_of_options > 0 then 'Naked Call'
        when number_of_shares = 0 and number_of_options < 0 then 'Naked Put'
        when number_of_shares > 0 and number_of_options > 0 then 'Collar'
        when number_of_shares > 0 and number_of_options < 0 then 'Protective Put'
        else 'Stock'
    end as strategy,
    number_of_shares,
    number_of_options,
    sum(shares_value) + sum(options_value) as position_value
from flattened
group by 1,2,3,4,5,6
