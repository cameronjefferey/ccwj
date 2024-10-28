with current_holdings as (
select 
   distinct symbol
from {{ ref('current')}}
)
, adding_quantities as (
select 
    date,
    action,
    symbol,
    security_type,
    trade_type,
    trade_symbol,
    quantity,
    amount,
    row_number() over (partition by trade_symbol order by date) as trade_order,
    case when security_type = 'Equity' then amount else sum(amount) over (partition by trade_symbol order by date) end as trade_outcome,
    coalesce(sum(case when action = 'Buy' then quantity when action = 'Sell' then -quantity end) over (partition by symbol order by date,security_type asc),0) as quantity_at_trade_date
from {{ ref('history')}} 
    join current_holdings using (symbol)
)
, final as (
select 
    symbol,
    security_type,
    trade_symbol,
    action,
    trade_order,
    amount,
    trade_outcome,
    quantity,
    quantity_at_trade_date,
    case 
        when security_type = 'Equity' then 'Equity'
        when security_type = 'Option' 
            and quantity*100 <= quantity_at_trade_date then 'Covered Call'
        when security_type = 'Option'
            and quantity_at_trade_date = 0 then 'Naked Options'
        else 'Other'
    end as trading_strategy
from adding_quantities
where not (security_type = 'Option' and trade_order = 1)
)
, test_final as (
select * from final 
)
select 
    symbol,
    trading_strategy,
    sum(trade_outcome)
from final 
group by 1,2
