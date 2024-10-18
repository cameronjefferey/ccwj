select 
   symbol,
   trade_symbol,
   option_security_type,
   quantity,
   price,
   market_value,
   cost_basis,
   gain_or_loss_dollar,
   security_type,
   margin_requirement,
   
from {{ ref('current')}}