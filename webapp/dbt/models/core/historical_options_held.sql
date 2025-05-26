with option_history as (
select 
    date,
    coalesce(lead(date) over (partition by symbol order by date asc)-1,current_date()) as next_option_date,
    action,
    symbol,
    security_type,
    trade_type,
    case 
        when action in ('Assigned','Expired','Buy to Close','Sell to Close') then quantity*-1 
        else quantity
    end as quantity,
    amount,
    row_number() over (partition by symbol,trade_type order by date asc) as order_number,
    sum(case 
        when action in ('Assigned','Expired','Buy to Close','Sell to Close') then quantity*-1 
        else quantity
    end) over (partition by symbol,trade_type order by date asc) as cumulative_sum,
from {{ ref('history')}}
where 1=1
    and symbol = 'CFLT'
    and action in ('Sell to Open','Sell to Close','Buy to Open','Buy to Close','Expired','Assigned')
order by date desc 
)
, daily_totals as (
select 
    date,
    symbol,
    sum(quantity) as total,
    sum(amount) as amount,
from option_history 
group by 1,2
)
, cumulative_daily_totals as (
select 
    date,
    symbol,
    coalesce(lead(date) over (partition by symbol order by date)-1,current_date()) as next_option_date,
    total,
    sum(total) over (partition by symbol order by date asc) as cum_total,
    sum(amount) over (partition by symbol order by date asc) as cum_amount,
from daily_totals

)
, daily_equity_held as (
select 
    symbol,
    row_number() over (partition by symbol,action,option_security_type order by date) as stock_action_order,
    date,
    coalesce(lead(date) over (partition by symbol, option_security_type order by date)-1,current_date) as next_equity_date,
    action,
    option_security_type,
    case when action = 'Sell' then quantity*-1 else quantity end as quantity,
    amount,
    sum(case when action = 'Sell' then quantity*-1 else quantity end) over (partition by symbol,option_security_type order by date) as stock_action_quantity,
    sum(amount) over (partition by symbol,option_security_type order by date) as stock_action_amount,
from {{ ref('history')}}
where action in ('Buy','Sell')
order by date 
)
, final as (
select 
    calendar_symbol_dates.day,
    calendar_symbol_dates.symbol,
    cumulative_daily_totals.cum_total as options_quantity,
    daily_equity_held.stock_action_quantity as equity_quantity,
    daily_equity_held.amount as equity_amount,
    daily_equity_held.stock_action_amount as equity_amount_2,
from {{ ref('calendar_symbol_dates')}}
    left join cumulative_daily_totals 
        on cumulative_daily_totals.symbol = calendar_symbol_dates.symbol
            and date(cumulative_daily_totals.date) <= date(calendar_symbol_dates.day)
            and date(cumulative_daily_totals.next_option_date) >= date(calendar_symbol_dates.day)
    left join daily_equity_held 
        on daily_equity_held.symbol = calendar_symbol_dates.symbol 
            and date(daily_equity_held.date) <= date(calendar_symbol_dates.day)
            and date(daily_equity_held.next_equity_date) >= date(calendar_symbol_dates.day)
where 1=1
    and calendar_symbol_dates.symbol = 'CFLT'
    and calendar_symbol_dates.day <= current_date()
order by calendar_symbol_dates.day desc 
)
select *
from final  
order by day desc 
limit 100