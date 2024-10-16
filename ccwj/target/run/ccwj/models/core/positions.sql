

  create or replace view `ccwj-dbt`.`analytics`.`positions`
  OPTIONS()
  as with active_positions as (
select 
    symbol,
    description,
    quantity,
    day_change_dollar,
    cost_bases,
    
from `ccwj-dbt`.`analytics`.`current`

)

, history as (
select 
    history.*
from `ccwj-dbt`.`analytics`.`history`
    left join `ccwj-dbt`.`analytics`.`current` as c 
        on c.trade_symbol = history.trade_symbol
where c.trade_symbol is null 
)
select 
*
from active_positions;

