/*
    Lifetime dividend income aggregated by (account, user_id, symbol).

    See `int_dividend_events` for source detail (CSV-reported events plus
    yfinance-synthesized ex-div × holdings events).
*/

select
    account,
    user_id,
    -- Stage 2 broker_account_id passthrough — upstream int_dividend_events
    -- already joins to dim_broker_accounts; any_value is safe because the
    -- mapping is functional on (account, user_id).
    any_value(broker_account_id) as broker_account_id,
    symbol,
    round(sum(amount), 2) as total_dividend_income,
    count(*)              as dividend_count,
    min(trade_date)       as first_dividend_date,
    max(trade_date)       as last_dividend_date
from {{ ref('int_dividend_events') }}
group by 1, 2, 4
