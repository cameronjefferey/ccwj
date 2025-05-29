with position_open_dates as (
    select 
        account,
        trade_symbol,
        min(transaction_date) as position_open_date
    from {{ ref('history') }}
    group by 1, 2
)
select 
    c.account,
    c.symbol,
    c.trade_symbol,
    position_open_dates.position_open_date
from {{ ref('current')}} as c
    left join position_open_dates using (account, trade_symbol)
where symbol not in ('Cash', 'Account')