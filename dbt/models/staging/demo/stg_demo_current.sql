{{
    config(
        materialized='view'
    )
}}

/*
    Public demo current positions.

    TEMPORARY: while var('demo_temp_seed') is true this is
    dbt/seeds/demo_temp_current.csv, stamped demo:demo-account.
    Removal steps: dbt/seeds/DEMO_TEMP_SEED.md.

    The else branch is the bot mirror. See stg_demo_history.sql for why
    that mirror reads the per-broker adapter rather than the raw source.

    Column order matches broker_current_rows() / stg_current's
    `current_as_strings` CTE exactly (user_id, tenant_id, then the common
    string columns) so the base model can UNION ALL without reordering.
*/

{% if var('demo_temp_seed', false) %}

select
    cast(null as string)                    as user_id,
    'demo:demo-account'                     as tenant_id,
    'Demo Account'                          as Account,
    cast(Symbol as string)                  as Symbol,
    cast(Description as string)             as Description,
    cast(Quantity as string)                as Quantity,
    cast(Price as string)                   as Price,
    cast(price_change_dollar as string)     as price_change_dollar,
    cast(price_change_percent as string)    as price_change_percent,
    cast(market_value as string)            as market_value,
    cast(day_change_dollar as string)       as day_change_dollar,
    cast(day_change_percent as string)      as day_change_percent,
    cast(cost_bases as string)              as cost_bases,
    cast(gain_or_loss_dollat as string)     as gain_or_loss_dollat,
    cast(gain_or_loss_percent as string)    as gain_or_loss_percent,
    cast(rating as string)                  as rating,
    cast(divident_reinvestment as string)   as divident_reinvestment,
    cast(is_capital_gain as string)         as is_capital_gain,
    cast(percent_of_account as string)      as percent_of_account,
    cast(expiration_date as string)         as expiration_date,
    cast(cost_per_share as string)          as cost_per_share,
    cast(last_earnings_date as string)      as last_earnings_date,
    cast(dividend_yield as string)          as dividend_yield,
    cast(last_dividend as string)           as last_dividend,
    cast(ex_dividend_date as string)        as ex_dividend_date,
    cast(pe_ratio as string)                as pe_ratio,
    cast(annual_week_low as string)         as annual_week_low,
    cast(annual_week_high as string)        as annual_week_high,
    cast(volume as string)                  as volume,
    cast(intrinsic_value as string)         as intrinsic_value,
    cast(in_the_money as string)            as in_the_money,
    cast(security_type as string)           as security_type,
    cast(margin_requirement as string)      as margin_requirement
from {{ ref('demo_temp_current') }}

{% else %}

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

{% endif %}
