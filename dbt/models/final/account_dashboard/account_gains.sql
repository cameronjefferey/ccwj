select 
    account,
    sum(if(transaction_type = 'current',gain_or_loss_dollar,0)) as unrealized_gain_or_loss,
    sum(if(transaction_type = 'historical',gain_or_loss_dollar,0)) as realized_gain_or_loss
from {{ ref('accounts') }}
group by 1