with equity_open_date as (
select 
    account,
    symbol,
    trade_symbol,
    quantity,
    min(amount) as position_open_amount,
    max(next_position_amount) as position_close_amount,
    min(transaction_date) as position_open_date,
    max(next_position_transaction_date) as position_close_date
from {{ ref('history_and_current_combined') }}
where is_current_position_establishement_1_0 = 1
    and security_type = 'Equity'
group by 1, 2, 3, 4
)
, option_calls_sold as (
select 
    account,
    symbol,
    trade_symbol,
    quantity,
    min(amount) as call_open_amount,
    max(next_position_amount) as call_close_amount,
{# TODO: The +1 is to account for rolling orders. Might need to rethink
the best way to do this. Right now it would be "first full 
day of trading" would be the open date which feels sus #}
    min(transaction_date+1) as position_open_date,
    max(next_position_transaction_date) as position_close_date
from {{ ref('history_and_current_combined') }}
where security_type = 'Option'
    and action in ('sell to open')
    and option_security_type = 'C'
group by 1, 2, 3, 4
)
, final as (
select 
{# TODO: Need to remove distinct and think about how to handle
when positions close and open on the same day #}
    calendar_dates_and_positions.day,
    calendar_dates_and_positions.symbol,
    calendar_dates_and_positions.account,
    option_calls_sold.trade_symbol,
    --Equity
    {# equity_open_date.position_open_amount as stock_open_amount,
    equity_open_date.position_close_amount as stock_close_amount,
    current_position_stock_price.close_price, #}
    equity_open_date.quantity as stock_quantity,
    equity_open_date.position_open_amount + (equity_open_date.quantity * current_position_stock_price.close_price) as equity_gain_or_loss,
    --Calls Sold
    option_calls_sold.quantity as option_calls_sold_quantity,
    {# option_calls_sold.call_open_amount as option_calls_sold_open_amount,
    option_calls_sold.call_close_amount as option_calls_sold_close_amount, #}
    case 
        when option_calls_sold.position_close_date = calendar_dates_and_positions.day 
            then option_calls_sold.call_open_amount + option_calls_sold.call_close_amount
        else null
    end as option_calls_sold_gain_or_loss,
    sum(case 
        when option_calls_sold.position_close_date = calendar_dates_and_positions.day 
            then option_calls_sold.call_open_amount + option_calls_sold.call_close_amount
        else null
    end) over ( partition by calendar_dates_and_positions.account,calendar_dates_and_positions.symbol order by calendar_dates_and_positions.day) as running_options_gain_or_loss,
from {{ ref('calendar_dates_and_positions')}}
    left join equity_open_date
        on calendar_dates_and_positions.day >= equity_open_date.position_open_date
        and calendar_dates_and_positions.day <= equity_open_date.position_close_date
        and calendar_dates_and_positions.symbol = equity_open_date.symbol
        and calendar_dates_and_positions.account = equity_open_date.account
    left join {{ ref('current_position_stock_price') }}
        on date(calendar_dates_and_positions.day) = date(current_position_stock_price.date) 
            and calendar_dates_and_positions.symbol = current_position_stock_price.symbol
    left join option_calls_sold
        on calendar_dates_and_positions.day >= option_calls_sold.position_open_date
        and calendar_dates_and_positions.day <= option_calls_sold.position_close_date
        and calendar_dates_and_positions.symbol = option_calls_sold.symbol
        and calendar_dates_and_positions.account = option_calls_sold.account
where equity_open_date.account = 'Cameron 401k'
)
select * from final