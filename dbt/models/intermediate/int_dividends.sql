/*
    Dividend income aggregated by account and symbol.
*/

select
    account,
    underlying_symbol as symbol,
    sum(amount)       as total_dividend_income,
    count(*)          as dividend_count,
    min(trade_date)   as first_dividend_date,
    max(trade_date)   as last_dividend_date
from {{ ref('stg_history') }}
where action = 'dividend'
group by 1, 2
