select 
    distinct account,
    symbol,
    position_open_date 
from {{ ref('current_positions')}}