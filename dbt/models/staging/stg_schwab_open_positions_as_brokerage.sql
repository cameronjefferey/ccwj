{{
    config(
        materialized='view'
    )
}}

/*
  Map Schwab API-shaped open positions (schwab_open_positions seed) into the
  same column layout as current_positions.csv so downstream stg_current can
  union without touching manual export seeds.
*/

with src as (
    select * from {{ ref('schwab_open_positions') }}
    where trim(coalesce(cast(account as string), '')) != ''
),

mv_num as (
    select
        *,
        safe_cast(trim(replace(replace(replace(cast(market_value as string), '$', ''), ',', ''), ' ', '')) as float64) as mv_f,
        safe_cast(trim(replace(replace(replace(cast(cost_basis as string), '$', ''), ',', ''), ' ', '')) as float64) as cb_f
    from src
)

select
    trim(cast(account as string)) as Account,
    trim(cast(symbol as string)) as Symbol,
    trim(cast(description as string)) as Description,
    cast(safe_cast(quantity as float64) as string) as Quantity,
    cast(safe_cast(average_price as float64) as string) as Price,
    cast(null as string) as price_change_dollar,
    cast(null as string) as price_change_percent,
    cast(mv_f as string) as market_value,
    cast(null as string) as day_change_dollar,
    cast(null as string) as day_change_percent,
    cast(cb_f as string) as cost_bases,
    cast(safe_subtract(mv_f, cb_f) as string) as gain_or_loss_dollat,
    cast(
        case
            when cb_f is not null and cb_f != 0
                then round(safe_divide(safe_subtract(mv_f, cb_f), abs(cb_f)) * 100, 4)
        end as string
    ) as gain_or_loss_percent,
    cast(null as string) as rating,
    cast(null as string) as divident_reinvestment,
    cast(null as string) as is_capital_gain,
    cast(null as string) as percent_of_account,
    cast(null as string) as expiration_date,
    cast(null as string) as cost_per_share,
    cast(null as string) as last_earnings_date,
    cast(null as string) as dividend_yield,
    cast(null as string) as last_dividend,
    cast(null as string) as ex_dividend_date,
    cast(null as string) as pe_ratio,
    cast(null as string) as annual_week_low,
    cast(null as string) as annual_week_high,
    cast(null as string) as volume,
    cast(null as string) as intrinsic_value,
    cast(null as string) as in_the_money,
    case
        when upper(trim(coalesce(cast(asset_type as string), ''))) = 'EQUITY' then 'Equity'
        when upper(trim(coalesce(cast(asset_type as string), ''))) = 'OPTION' then 'Option'
        when upper(trim(coalesce(cast(asset_type as string), ''))) = 'COLLECTIVE_INVESTMENT' then 'ETFs & Closed End Funds'
        else trim(coalesce(cast(asset_type as string), ''))
    end as security_type,
    cast(null as string) as margin_requirement
from mv_num
