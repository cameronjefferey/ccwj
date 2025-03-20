with all_symbols as (
select distinct symbol
from {{ ref('current')}}
)
, final as (
select 
    cast(calendar_dates.day as date) as day,
    all_symbols.symbol
from {{ ref('calendar_dates')}}
    cross join all_symbols 
) 
select * 
from final 
where EXTRACT(DAYOFWEEK from day) in (2,3,4,5,6)
