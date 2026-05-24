/*
    Lifetime dividend income aggregated by (account, user_id, symbol).

    See `int_dividend_events` for source detail (CSV-reported events plus
    yfinance-synthesized ex-div × holdings events).
*/

select
    account,
    user_id,
    -- v2 tenant_id passthrough — upstream int_dividend_events already
    -- joins to dim_broker_tenants; any_value is safe because the
    -- mapping is functional on (account, user_id). See
    -- docs/V2_TENANT_KEY_DESIGN.md.
    any_value(tenant_id) as tenant_id,
    symbol,
    round(sum(amount), 2) as total_dividend_income,
    count(*)              as dividend_count,
    min(trade_date)       as first_dividend_date,
    max(trade_date)       as last_dividend_date
from {{ ref('int_dividend_events') }}
group by 1, 2, 4
