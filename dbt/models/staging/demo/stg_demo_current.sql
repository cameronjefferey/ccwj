{{
    config(
        materialized='view'
    )
}}

/*
    Public demo current positions — a MIRROR of a real tenant, relabeled.
    See stg_demo_history.sql for the full rationale (why a mirror rather
    than shared tenancy, and why this reads the per-broker adapter rather
    than the raw source).

    Column order matches broker_current_rows() / stg_current's
    `current_as_strings` CTE exactly (user_id, tenant_id, then the common
    string columns) so the base model can UNION ALL without reordering.
*/

select
    cast(null as string)                    as user_id,
    'demo:demo-account'                     as tenant_id,
    'Demo Account'                          as Account,
    Symbol,
    Description,
    Quantity,
    Price,
    price_change_dollar,
    price_change_percent,
    market_value,
    day_change_dollar,
    day_change_percent,
    cost_bases,
    gain_or_loss_dollat,
    gain_or_loss_percent,
    rating,
    divident_reinvestment,
    is_capital_gain,
    percent_of_account,
    expiration_date,
    cost_per_share,
    last_earnings_date,
    dividend_yield,
    last_dividend,
    ex_dividend_date,
    pe_ratio,
    annual_week_low,
    annual_week_high,
    volume,
    intrinsic_value,
    in_the_money,
    security_type,
    margin_requirement
from {{ ref('stg_broker_alpaca_current') }}
where tenant_id = '{{ var("demo_source_tenant_id", "") }}'
  and '{{ var("demo_source_tenant_id", "") }}' != ''
