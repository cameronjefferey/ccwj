with combined as (
select 
   symbol,
   security_type,
   sum(market_value) as amount 
from {{ ref('current')}}
group by 1,2
UNION ALL 
select 
   symbol,
   security_type,
   sum(amount) as amount 
from {{ ref('history')}}
group by 1,2
)


select 
   symbol,
   sum(case when security_type = 'Equity' then amount end) as gain_or_loss_stock,
   sum(case when security_type = 'Option' then amount end) as gain_or_loss_option,
   sum(case when security_type in ('Option','Equity') then amount end) as gain_or_loss_stock_and_option,
from combined 
group by 1