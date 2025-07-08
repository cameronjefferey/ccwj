select 
    symbol,
    sum(amount) as total_gain_or_loss,
from {{ ref('history_and_current_combined') }}

where history_and_current_combined.action in ('buy','buy to open','sell to open')
group by 1 