

  create or replace view `ccwj-dbt`.`analytics`.`current_position_information`
  OPTIONS()
  as select 
   symbol,
   trade_symbol,
   option_security_type,
   quantity,
   price,
   market_value,
   cost_basis,
   gain_or_loss_dollar,
   security_type,
   margin_requirement 
from `ccwj-dbt`.`analytics`.`current`;

