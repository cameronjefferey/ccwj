with current_holdings as (
select 
   distinct symbol
from `ccwj-dbt`.`analytics`.`current`
)
, holdings_earnings_historical as (
select 
   current_holdings.symbol,
   history.security_type,
   sum(amount) as gain_or_loss,
from current_holdings 
   join `ccwj-dbt`.`analytics`.`history` 
      on current_holdings.symbol = history.symbol
group by 1,2
)
, holdings_earnings_current_value as (
select 
   current_holdings.symbol,
   c.security_type,
   sum(market_value) as current_market_value
from current_holdings
   join `ccwj-dbt`.`analytics`.`current` as c using (symbol)
group by 1,2
)
, final as (
select 
   symbol,
   security_type,
   gain_or_loss,
from holdings_earnings_historical 
UNION ALL 
select 
   symbol,
   security_type,
   current_market_value,
from holdings_earnings_current_value
)
select 
   symbol,
   security_type,
   sum(gain_or_loss) as current_position_overall_gain_or_loss 
from final 
group by 1,2