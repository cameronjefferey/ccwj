select * 
from {{ ref('calendar_dates_and_positions')}}
where account = 'Sara Investment'
and symbol is not null 