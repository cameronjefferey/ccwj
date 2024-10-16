

  create or replace view `ccwj-dbt`.`analytics`.`daily_totals`
  OPTIONS()
  as with current_value as (
select 
   distinct symbol,
   calendar_dates.day as date,
   security_type,
   sum(market_value) as amount 
from `ccwj-dbt`.`analytics`.`calendar_dates`
    cross join `ccwj-dbt`.`analytics`.`current`
group by 1,2,3
)
, historical_value as (
select 
   symbol,
   date,
   security_type,
   sum(amount) as amount 
from `ccwj-dbt`.`analytics`.`history`
    join `ccwj-dbt`.`analytics`.`cflt_prices` using (date,symbol)
group by 1,2,3
)
, all_symbols as (
select distinct symbol 
from `ccwj-dbt`.`analytics`.`current`
UNION DISTINCT 
select distinct symbol 
from `ccwj-dbt`.`analytics`.`history`
)
, daily_dates as (
select 
    distinct calendar_dates.day,
    all_symbols.symbol
from `ccwj-dbt`.`analytics`.`calendar_dates`
    cross join all_symbols
where day <= current_date()
)
, daily_historical_gain_or_loss as (
select 
    daily_dates.day,
    daily_dates.symbol,
    security_type,
    round(sum(amount),2) as gain_or_loss,
from daily_dates 
    join historical_value 
        on historical_value.symbol = daily_dates.symbol
            and historical_value.date <= daily_dates.day 
group by 1,2,3
)
, final as (
select 
    daily_historical_gain_or_loss.day,
    daily_historical_gain_or_loss.symbol,
    daily_historical_gain_or_loss.security_type,
    sum(daily_historical_gain_or_loss.gain_or_loss) + sum(current_value.amount) as total_gain_or_loss
from daily_historical_gain_or_loss
    left join current_value 
        on current_value.symbol = daily_historical_gain_or_loss.symbol
            and current_value.security_type = daily_historical_gain_or_loss.security_type
            and date(current_value.date) = date(daily_historical_gain_or_loss.day)
group by 1,2,3
)
select * 
from final 
order by 1 desc;

