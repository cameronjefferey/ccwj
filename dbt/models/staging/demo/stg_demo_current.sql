{{
    config(
        materialized='view'
    )
}}

/*
    Public demo current positions.

    Always the bot mirror. While demo_temp_seed_on() is true, seed
    positions in dbt/seeds/demo_temp_current.csv are added. The same
    symbol is one row: quantity, market value, and cost are summed, so
    a shared ticker does not drop the mirror lot at stg_current's
    (tenant, symbol) dedup and does not double-count it.

    Flag off = the mirror select only. See dbt/seeds/DEMO_TEMP_SEED.md.

    Column order matches broker_current_rows() / stg_current's
    `current_as_strings` CTE exactly.
*/

with mirror as (
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
)

{% if demo_temp_seed_on() %}

, stacked as (
    select 'm' as src, m.* from mirror m
    union all
    select
        's'                                         as src,
        cast(null as string)                        as user_id,
        'demo:demo-account'                         as tenant_id,
        'Demo Account'                              as Account,
        cast(Symbol as string)                      as Symbol,
        cast(Description as string)                 as Description,
        cast(Quantity as string)                    as Quantity,
        cast(Price as string)                       as Price,
        cast(price_change_dollar as string)         as price_change_dollar,
        cast(price_change_percent as string)        as price_change_percent,
        cast(market_value as string)                as market_value,
        cast(day_change_dollar as string)           as day_change_dollar,
        cast(day_change_percent as string)          as day_change_percent,
        cast(cost_bases as string)                  as cost_bases,
        cast(gain_or_loss_dollat as string)         as gain_or_loss_dollat,
        cast(gain_or_loss_percent as string)        as gain_or_loss_percent,
        cast(rating as string)                      as rating,
        cast(divident_reinvestment as string)       as divident_reinvestment,
        cast(is_capital_gain as string)             as is_capital_gain,
        cast(percent_of_account as string)          as percent_of_account,
        cast(expiration_date as string)             as expiration_date,
        cast(cost_per_share as string)              as cost_per_share,
        cast(last_earnings_date as string)          as last_earnings_date,
        cast(dividend_yield as string)              as dividend_yield,
        cast(last_dividend as string)               as last_dividend,
        cast(ex_dividend_date as string)            as ex_dividend_date,
        cast(pe_ratio as string)                    as pe_ratio,
        cast(annual_week_low as string)             as annual_week_low,
        cast(annual_week_high as string)            as annual_week_high,
        cast(volume as string)                      as volume,
        cast(intrinsic_value as string)             as intrinsic_value,
        cast(in_the_money as string)                as in_the_money,
        cast(security_type as string)               as security_type,
        cast(margin_requirement as string)          as margin_requirement
    from {{ ref('demo_temp_current') }}
),

summed as (
    select
        upper(trim(Symbol)) as symbol_key,
        sum({{ parse_seed_number('Quantity') }}) as quantity,
        sum({{ parse_seed_number('market_value') }}) as market_value,
        sum({{ parse_seed_number('cost_bases') }}) as cost_bases,
        sum({{ parse_seed_number('gain_or_loss_dollat') }}) as gain_dollars,
        sum({{ parse_seed_number('day_change_dollar') }}) as day_change_dollar,
        coalesce(
            max(if(src = 'm', nullif(trim(Description), ''), null)),
            max(nullif(trim(Description), ''))
        ) as description,
        coalesce(
            max(if(src = 'm', nullif(trim(security_type), ''), null)),
            max(nullif(trim(security_type), ''))
        ) as security_type,
        coalesce(
            max(if(src = 'm', nullif(trim(in_the_money), ''), null)),
            max(nullif(trim(in_the_money), ''))
        ) as in_the_money,
        coalesce(
            max(if(src = 'm', nullif(trim(rating), ''), null)),
            max(nullif(trim(rating), ''))
        ) as rating,
        coalesce(
            max(if(src = 'm', nullif(trim(divident_reinvestment), ''), null)),
            max(nullif(trim(divident_reinvestment), ''))
        ) as divident_reinvestment,
        coalesce(
            max(if(src = 'm', nullif(trim(is_capital_gain), ''), null)),
            max(nullif(trim(is_capital_gain), ''))
        ) as is_capital_gain,
        coalesce(
            max(if(src = 'm', nullif(trim(expiration_date), ''), null)),
            max(nullif(trim(expiration_date), ''))
        ) as expiration_date,
        coalesce(
            max(if(src = 'm', nullif(trim(last_earnings_date), ''), null)),
            max(nullif(trim(last_earnings_date), ''))
        ) as last_earnings_date,
        coalesce(
            max(if(src = 'm', nullif(trim(ex_dividend_date), ''), null)),
            max(nullif(trim(ex_dividend_date), ''))
        ) as ex_dividend_date,
        coalesce(
            max(if(src = 'm', nullif(trim(price_change_percent), ''), null)),
            max(nullif(trim(price_change_percent), ''))
        ) as price_change_percent,
        coalesce(
            max(if(src = 'm', nullif(trim(day_change_percent), ''), null)),
            max(nullif(trim(day_change_percent), ''))
        ) as day_change_percent,
        coalesce(
            max(if(src = 'm', nullif(trim(percent_of_account), ''), null)),
            max(nullif(trim(percent_of_account), ''))
        ) as percent_of_account,
        coalesce(
            max(if(src = 'm', nullif(trim(dividend_yield), ''), null)),
            max(nullif(trim(dividend_yield), ''))
        ) as dividend_yield,
        coalesce(
            max(if(src = 'm', nullif(trim(last_dividend), ''), null)),
            max(nullif(trim(last_dividend), ''))
        ) as last_dividend,
        coalesce(
            max(if(src = 'm', nullif(trim(pe_ratio), ''), null)),
            max(nullif(trim(pe_ratio), ''))
        ) as pe_ratio,
        coalesce(
            max(if(src = 'm', nullif(trim(annual_week_low), ''), null)),
            max(nullif(trim(annual_week_low), ''))
        ) as annual_week_low,
        coalesce(
            max(if(src = 'm', nullif(trim(annual_week_high), ''), null)),
            max(nullif(trim(annual_week_high), ''))
        ) as annual_week_high,
        coalesce(
            max(if(src = 'm', nullif(trim(volume), ''), null)),
            max(nullif(trim(volume), ''))
        ) as volume,
        coalesce(
            max(if(src = 'm', nullif(trim(intrinsic_value), ''), null)),
            max(nullif(trim(intrinsic_value), ''))
        ) as intrinsic_value,
        coalesce(
            max(if(src = 'm', nullif(trim(margin_requirement), ''), null)),
            max(nullif(trim(margin_requirement), ''))
        ) as margin_requirement,
        coalesce(
            max(if(src = 'm', nullif(trim(price_change_dollar), ''), null)),
            max(nullif(trim(price_change_dollar), ''))
        ) as price_change_dollar_text
    from stacked
    group by 1
)

select
    cast(null as string) as user_id,
    'demo:demo-account' as tenant_id,
    'Demo Account' as Account,
    symbol_key as Symbol,
    description as Description,
    cast(quantity as string) as Quantity,
    cast(safe_divide(market_value, nullif(quantity, 0)) as string) as Price,
    price_change_dollar_text as price_change_dollar,
    price_change_percent,
    cast(market_value as string) as market_value,
    cast(day_change_dollar as string) as day_change_dollar,
    day_change_percent,
    cast(cost_bases as string) as cost_bases,
    cast(gain_dollars as string) as gain_or_loss_dollat,
    cast(safe_divide(100.0 * gain_dollars, nullif(abs(cost_bases), 0)) as string) as gain_or_loss_percent,
    rating,
    divident_reinvestment,
    is_capital_gain,
    percent_of_account,
    expiration_date,
    cast(safe_divide(cost_bases, nullif(quantity, 0)) as string) as cost_per_share,
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
from summed

{% else %}

select * from mirror

{% endif %}
