with equity_open_date as (
select 
    account,
    symbol,
    trade_symbol,
    min(transaction_date) as position_open_date,
    max(next_position_transaction_date) as position_close_date
from {{ ref('history_and_current_combined') }}
where (is_current_position_establishement_1_0 = 1
        or is_buying_more_shares_in_current_position_1_0 = 1)
    and position_establishment_order = 1
    and security_type = 'Equity'
group by 1, 2, 3
)
, equity_purchases as (
select 
    account,
    symbol,
    trade_symbol,
    transaction_date,
    quantity,
    amount,
    coalesce(lead(transaction_date) over (partition by account,symbol,trade_symbol order by transaction_date),current_date()) as next_position_transaction_date,
    sum(quantity) over (partition by account,symbol,trade_symbol order by transaction_date) as cumulative_quantity,
    sum(amount) over (partition by account,symbol,trade_symbol order by transaction_date) as cumulative_amount,
from {{ ref('history_and_current_combined')}}
where (is_current_position_establishement_1_0 = 1
        or is_buying_more_shares_in_current_position_1_0 = 1)
    and position_establishment_order = 1
    and security_type = 'Equity'
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
the best way to do this. Right now the "first full 
day of trading" would be the open date which feels sus #}
    min(transaction_date+1) as position_open_date,
    max(next_position_transaction_date) as position_close_date
from {{ ref('history_and_current_combined') }}
where security_type = 'Option'
    and action in ('sell to open')
    and option_security_type = 'C'
group by 1, 2, 3, 4
)
, dividends_paid as (
select 
    account,
    symbol,
    trade_symbol,
    transaction_date,
    amount 
from {{ ref('history_and_current_combined') }}
where security_type = 'Dividend'
)
, final as (
select 
    calendar_dates_and_positions.day,
    calendar_dates_and_positions.symbol,
    calendar_dates_and_positions.account,
    option_calls_sold.trade_symbol,
    --Equity
    equity_purchases.cumulative_quantity as stock_quantity,
    current_position_stock_price.close_price,
    equity_purchases.cumulative_amount,
    equity_purchases.cumulative_amount + (equity_purchases.cumulative_quantity * current_position_stock_price.close_price) as equity_gain_or_loss,
    --Calls Sold
    option_calls_sold.quantity as option_calls_sold_quantity,
    case 
        when option_calls_sold.position_close_date = calendar_dates_and_positions.day 
            then option_calls_sold.call_open_amount + option_calls_sold.call_close_amount
        when option_calls_sold.position_close_date = current_date() 
            then option_calls_sold.call_open_amount + current_position_stock_price.close_price
        else null
    end as option_calls_sold_gain_or_loss,
    sum(case 
        when option_calls_sold.position_close_date = calendar_dates_and_positions.day 
            then option_calls_sold.call_open_amount + option_calls_sold.call_close_amount
        when option_calls_sold.position_close_date = current_date() 
            then option_calls_sold.call_open_amount + current_position_stock_price.close_price
        else null
    end) over ( partition by calendar_dates_and_positions.account,calendar_dates_and_positions.symbol order by calendar_dates_and_positions.day) as running_options_gain_or_loss,

    --Dividends
    sum(dividends_paid.amount) over (partition by calendar_dates_and_positions.account,calendar_dates_and_positions.symbol order by calendar_dates_and_positions.day) as dividends_paid,  
from {{ ref('calendar_dates_and_positions')}}
    left join equity_purchases 
        on calendar_dates_and_positions.day >= equity_purchases.transaction_date 
            and calendar_dates_and_positions.day <= equity_purchases.next_position_transaction_date
            and calendar_dates_and_positions.symbol = equity_purchases.symbol
            and calendar_dates_and_positions.account = equity_purchases.account
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
        and equity_open_date.position_open_date <= option_calls_sold.position_open_date
    left join dividends_paid 
        on calendar_dates_and_positions.day = dividends_paid.transaction_date
        and calendar_dates_and_positions.symbol = dividends_paid.symbol
        and calendar_dates_and_positions.account = dividends_paid.account
)
select * 
from final 
where 1=1


    and stock_quantity > 0