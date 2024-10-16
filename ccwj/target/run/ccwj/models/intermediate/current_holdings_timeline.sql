

  create or replace view `ccwj-dbt`.`analytics`.`current_holdings_timeline`
  OPTIONS()
  as with current_holdings as (
select 
   distinct symbol
from `ccwj-dbt`.`analytics`.`current`
)
, holdings_strategy as (
select 
   current_holdings.symbol,
   min(case when history.security_type = 'Equity' then date end) as first_stock_purchase_date,
   min(case when history.security_type = 'Option' then date end) as first_option_purchase_date,
   max(case when history.security_type = 'Equity' then date end) as last_stock_purchase_date,
   max(case when history.security_type = 'Option' then date end) as last_option_purchase_date,
from current_holdings 
   join `ccwj-dbt`.`analytics`.`history` 
      on current_holdings.symbol = history.symbol
group by 1
)

, final as (
select 
   holdings_strategy.*,
   least(first_stock_purchase_date,first_option_purchase_date) as first_holdings_purchase_date
from holdings_strategy
)
select *
from final;

