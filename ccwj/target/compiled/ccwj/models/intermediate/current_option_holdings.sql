select 
   symbol,
   trade_symbol,
   cast(quantity as int) as option_quantity,
   price as option_price,
   market_value as option_market_value,
   cost_basis as option_cost_basis,
   gain_or_loss_dollar as option_gain_or_loss_dollar,
   security_type,
   margin_requirement as option_margin_requirement,
from `ccwj-dbt`.`analytics`.`current`
where security_type = 'Option'