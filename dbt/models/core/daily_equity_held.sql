with equity_open_date as (
select 
    account,
    symbol,
    session_id,
    max(quantity) as equity_quantity,
    sum(case when action in ('buy') then amount end) as equity_open_amount,
    sum(amount) as equity_gain_or_loss,
    min(transaction_date) as open_equity_date,
    max(transaction_date) as close_equity_date,
from {{ ref('position_equity_sessions') }}
group by 1,2,3
)
, option_calls_sold as (
select 
    account,
    symbol,
    trade_symbol,
    sum(case when action in {{ buy_actions() }} then quantity end) as quantity,
    sum(case when action in {{ buy_actions() }} then amount end) as call_open_amount,
    sum(case when action in {{ sell_actions() }} then amount end) as call_close_amount,
{# TODO: The +1 is to account for rolling orders. Might need to rethink
the best way to do this. Right now the "first full 
day of trading" would be the open date which feels sus #}
    min(case when action in {{ buy_actions() }} then transaction_date+1 end) as position_open_date,
    max(case when action in {{ sell_actions() }} then transaction_date end) as position_close_date
from {{ ref('history_and_current_combined') }}
where security_type in ('Call Option','Put Option')
    and option_security_type in ('C','P')
group by 1, 2, 3
)
, option_calls_sold_position_summary as (
select 
    account,
    symbol,
    position_close_date,
    case when position_open_date = position_close_date then position_open_date-1 else position_open_date end as position_open_date,
    sum(call_open_amount + call_close_amount) as total_gain_or_loss,
from option_calls_sold

group by 1, 2, 3,4
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
, daily_equity_session_totals as (
select 
    calendar_dates_and_positions.day,
    calendar_dates_and_positions.symbol,
    calendar_dates_and_positions.account,
    
    --Equity
    equity_open_date.session_id,
    equity_open_date.equity_quantity as stock_quantity,
    current_position_stock_price.close_price,
    equity_open_date.equity_open_amount,
    equity_open_amount + (equity_open_date.equity_quantity * current_position_stock_price.close_price) as equity_gain_or_loss,
    case when equity_open_date.close_equity_date = calendar_dates_and_positions.day then equity_gain_or_loss end as equity_session_gain_or_loss,
    
from {{ ref('calendar_dates_and_positions')}}
    left join equity_open_date
        on calendar_dates_and_positions.day >= equity_open_date.open_equity_date
        and calendar_dates_and_positions.day <= equity_open_date.close_equity_date
        and calendar_dates_and_positions.symbol = equity_open_date.symbol
        and calendar_dates_and_positions.account = equity_open_date.account
    left join {{ ref('current_position_stock_price') }}
        on date(calendar_dates_and_positions.day) = date(current_position_stock_price.date) 
            and calendar_dates_and_positions.symbol = current_position_stock_price.symbol
            --This isn't necessary if we update the python script. Stock price is not account specific
            and calendar_dates_and_positions.account = current_position_stock_price.account
)
, final as (
select 
    daily_equity_session_totals.day,
    daily_equity_session_totals.symbol,
    daily_equity_session_totals.account,
    daily_equity_session_totals.session_id,

    --Equity
    daily_equity_session_totals.stock_quantity,
    daily_equity_session_totals.close_price,
    daily_equity_session_totals.equity_open_amount,
    daily_equity_session_totals.equity_gain_or_loss,
    daily_equity_session_totals.equity_session_gain_or_loss,
    coalesce(daily_equity_session_totals.equity_gain_or_loss,0) + coalesce(sum(daily_equity_session_totals.equity_session_gain_or_loss) over ( 
        partition by daily_equity_session_totals.account, daily_equity_session_totals.symbol 
        order by daily_equity_session_totals.day
    ),0) - coalesce(daily_equity_session_totals.equity_session_gain_or_loss,0) as running_equity_gain_or_loss,

    --Options
    option_calls_sold_position_summary.total_gain_or_loss as option_calls_sold_gain_or_loss,
    sum(option_calls_sold_position_summary.total_gain_or_loss) over 
        (partition by daily_equity_session_totals.account, daily_equity_session_totals.symbol 
        order by daily_equity_session_totals.day) as running_options_calls_sold_gain_or_loss,

    --Dividends 
    dividends_paid.amount as dividends_paid_gain_or_loss,
    sum(dividends_paid.amount) over 
        (partition by daily_equity_session_totals.account, daily_equity_session_totals.symbol 
        order by daily_equity_session_totals.day) as running_dividends_paid_gain_or_loss
from daily_equity_session_totals
    left join option_calls_sold_position_summary
        on daily_equity_session_totals.day = option_calls_sold_position_summary.position_close_date
            and daily_equity_session_totals.symbol = option_calls_sold_position_summary.symbol
            and daily_equity_session_totals.account = option_calls_sold_position_summary.account
    left join dividends_paid 
        on daily_equity_session_totals.day = dividends_paid.transaction_date
            and daily_equity_session_totals.symbol = dividends_paid.symbol
            and daily_equity_session_totals.account = dividends_paid.account
)
select * 
from final 
where 1=1
    {# and account = 'Cameron 401k' #}
    {# and symbol = 'JEPI' #}
    {# and account = 'Sara Investment'
    and symbol = 'CFLT' #}
    {# and trade_symbol = 'CFLT 02/21/2025 33.00 C' #}
    and stock_quantity > 0
    and close_price is not null
    {# and running_equity_gain_or_loss is null  #}
    {# order by day desc #}


    {# --Calls Sold
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
    end) over ( partition by calendar_dates_and_positions.account,calendar_dates_and_positions.symbol order by calendar_dates_and_positions.day) as running_options_gain_or_loss, #}

    {# --Dividends
    sum(dividends_paid.amount) over (partition by calendar_dates_and_positions.account,calendar_dates_and_positions.symbol order by calendar_dates_and_positions.day) as dividends_paid,   #}
    
    {# left join option_calls_sold
        on calendar_dates_and_positions.day >= option_calls_sold.position_open_date
        and calendar_dates_and_positions.day <= option_calls_sold.position_close_date
        and calendar_dates_and_positions.symbol = option_calls_sold.symbol
        and calendar_dates_and_positions.account = option_calls_sold.account
        and equity_open_date.open_equity_date <= option_calls_sold.position_open_date
    left join dividends_paid 
        on calendar_dates_and_positions.day = dividends_paid.transaction_date
        and calendar_dates_and_positions.symbol = dividends_paid.symbol
        and calendar_dates_and_positions.account = dividends_paid.account #}