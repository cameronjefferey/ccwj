select 
   symbol,
   trade_symbol,
   cast(quantity as int) as equity_quantity,
   price as equity_price,
   market_value as equity_market_value,
   cost_basis as equity_cost_basis,
   gain_or_loss_dollar as equity_gain_or_loss_dollar,
   security_type,
   margin_requirement as equity_margin_requirement,
from `ccwj-dbt`.`analytics`.`current`
where security_type = 'Equity'