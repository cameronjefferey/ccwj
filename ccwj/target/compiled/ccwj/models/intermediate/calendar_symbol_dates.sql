with all_symbols as (
select distinct symbol
from `ccwj-dbt`.`analytics`.`current`
)

select 
    calendar_dates.day,
    all_symbols.symbol
from `ccwj-dbt`.`analytics`.`calendar_dates`
    cross join all_symbols