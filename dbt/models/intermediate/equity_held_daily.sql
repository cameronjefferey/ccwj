with all_equity_held as (
    select 
        distinct account,
        symbol,
        transaction_date,
        action,
        case 
            when lower(action) = 'sell' then -quantity 
            when lower(action) = 'buy' then quantity
            else 0 end as quantity,
        cost_per_share
    from {{ ref('history')}}
    where action in ('buy', 'sell')
)
select 
    distinct calendar_dates_and_positions.day,
    calendar_dates_and_positions.account,
    calendar_dates_and_positions.symbol,
    all_equity_held.quantity,
    sum(quantity) over (partition by calendar_dates_and_positions.symbol order by calendar_dates_and_positions.day) as cumulative_equity_quantity,
from {{ ref('calendar_dates_and_positions')}}
    left join all_equity_held on all_equity_held.transaction_date = calendar_dates_and_positions.day
        and all_equity_held.symbol = calendar_dates_and_positions.symbol
        and all_equity_held.account = calendar_dates_and_positions.account
order by day desc