

  create or replace view `ccwj-dbt`.`analytics`.`current_holdings_strategy`
  OPTIONS()
  as with current_holdings as (
select 
   distinct symbol
from `ccwj-dbt`.`analytics`.`current`
where symbol not in ('Cash','Account')
)
, holdings_strategy as (
select 
   current_holdings.symbol,
   coalesce(current_option_holdings.option_quantity,0) as option_quantity,
   coalesce(current_equity_holdings.equity_quantity,0) as equity_quantity,
   case 
      when current_option_holdings.option_quantity*-100 = current_equity_holdings.equity_quantity then 'Covered Call'
      when current_option_holdings.option_quantity is null and current_equity_holdings.equity_quantity is not null then 'Buy and Hold'
      else 'Other'
   end as strategy 
from current_holdings 
   left join `ccwj-dbt`.`analytics`.`current_equity_holdings` 
      on current_holdings.symbol = current_equity_holdings.symbol
   left join `ccwj-dbt`.`analytics`.`current_option_holdings` 
      on current_holdings.symbol = current_option_holdings.symbol

)

select *
from holdings_strategy;

