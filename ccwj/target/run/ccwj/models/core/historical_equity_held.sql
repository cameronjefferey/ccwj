

  create or replace view `ccwj-dbt`.`analytics`.`historical_equity_held`
  OPTIONS()
  as with ordering as (

select 
    symbol,
    row_number() over (partition by symbol,action,option_security_type order by date) as stock_action_order,
    date,
    lead(date) over (partition by symbol, option_security_type order by date)-1 as next_equity_date,
    action,
    option_security_type,
    case when action = 'Sell' then quantity*-1 else quantity end as quantity,
    amount,
    sum(case when action = 'Sell' then quantity*-1 else quantity end) over (partition by symbol,option_security_type order by date) as stock_action_quantity,
    sum(amount) over (partition by symbol,option_security_type order by date) as stock_action_amount,
from `ccwj-dbt`.`analytics`.`history`
where symbol = 'CFLT'
    and action in ('Buy','Sell')
order by date
)
, stock_activity as (
select 
    symbol,
    stock_action_order,
    date,
    case 
        when next_equity_date is null and action = 'Buy' then current_date()
        else next_equity_date
    end as next_equity_date,
    action,
    option_security_type,
    stock_action_quantity,
    stock_action_amount
from ordering 
)
, final as (
select 
    calendar_symbol_dates.day,
    stock_activity.symbol,
    stock_activity.stock_action_quantity,
    stock_activity.stock_action_amount,
    cflt_prices.close,
    cflt_prices.close*stock_activity.stock_action_quantity as equity_value,
from `ccwj-dbt`.`analytics`.`calendar_symbol_dates`
    left join stock_activity
        on stock_activity.date <= calendar_symbol_dates.day 
            and stock_activity.next_equity_date >= calendar_symbol_dates.day 
            and stock_activity.symbol = calendar_symbol_dates.symbol
    left join `ccwj-dbt`.`analytics`.`cflt_prices`
        on lower(cflt_prices.symbol) = lower(calendar_symbol_dates.symbol)
            and date(cflt_prices.date) = date(calendar_symbol_dates.day )
where 1=1
    and calendar_symbol_dates.symbol = 'CFLT'
    and calendar_symbol_dates.day < current_date 
order by calendar_symbol_dates.day desc 
)
select 
    *,
    round(stock_action_amount + equity_value,2) as total_gain_or_loss 
from final
where close is not null;

