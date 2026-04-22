{{
    config(
        materialized='view'
    )
}}

/*
  Union export current_positions with Schwab-native rows. BigQuery infers
  heterogeneous types from CSV seeds (e.g. FLOAT64 market_value) while the
  Schwab branch emits STRINGs — normalize the left side to STRING so UNION ALL
  succeeds; stg_current already parses numerics from strings.
*/

select
    cast(Account as string) as Account,
    cast(Symbol as string) as Symbol,
    cast(Description as string) as Description,
    cast(Quantity as string) as Quantity,
    cast(Price as string) as Price,
    cast(price_change_dollar as string) as price_change_dollar,
    cast(price_change_percent as string) as price_change_percent,
    cast(market_value as string) as market_value,
    cast(day_change_dollar as string) as day_change_dollar,
    cast(day_change_percent as string) as day_change_percent,
    cast(cost_bases as string) as cost_bases,
    cast(gain_or_loss_dollat as string) as gain_or_loss_dollat,
    cast(gain_or_loss_percent as string) as gain_or_loss_percent,
    cast(rating as string) as rating,
    cast(divident_reinvestment as string) as divident_reinvestment,
    cast(is_capital_gain as string) as is_capital_gain,
    cast(percent_of_account as string) as percent_of_account,
    cast(expiration_date as string) as expiration_date,
    cast(cost_per_share as string) as cost_per_share,
    cast(last_earnings_date as string) as last_earnings_date,
    cast(dividend_yield as string) as dividend_yield,
    cast(last_dividend as string) as last_dividend,
    cast(ex_dividend_date as string) as ex_dividend_date,
    cast(pe_ratio as string) as pe_ratio,
    cast(annual_week_low as string) as annual_week_low,
    cast(annual_week_high as string) as annual_week_high,
    cast(volume as string) as volume,
    cast(intrinsic_value as string) as intrinsic_value,
    cast(in_the_money as string) as in_the_money,
    cast(security_type as string) as security_type,
    cast(margin_requirement as string) as margin_requirement
from {{ ref('current_positions') }}
union all
select * from {{ ref('stg_schwab_open_positions_as_brokerage') }}
