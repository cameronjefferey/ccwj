with all_symbols as (
select distinct symbol
from {{ ref('current')}}
)

select 
    calendar_dates.day,
    all_symbols.symbol
from {{ ref('calendar_dates')}}
    cross join all_symbols 
