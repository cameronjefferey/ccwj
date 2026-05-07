/*
    Lifetime dividend income aggregated by (account, user_id, symbol).

    See `int_dividend_events` for source detail (CSV-reported events plus
    yfinance-synthesized ex-div × holdings events).
*/

select
    account,
    user_id,
    symbol,
    round(sum(amount), 2) as total_dividend_income,
    count(*)              as dividend_count,
    min(trade_date)       as first_dividend_date,
    max(trade_date)       as last_dividend_date
from {{ ref('int_dividend_events') }}
group by 1, 2, 3
