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
        safe_cast(trim(replace(replace(replace(cast(cost_basis as string), '$', ''), ',', ''), ' ', '')) as float64) as cb_f,
        abs(safe_cast(quantity as float64)) as qty_abs,
        safe_cast(average_price as float64) as ap_f
    from src
),

-- Schwab sometimes stores option cost as premium per share (×1) instead of position cost (×100 × contracts).
-- Align with app/schwab.py: prefer API when already ~avg×qty×100; if stored ~avg×|qty| only, scale to ×100.
corrected as (
    select
        *,
        ap_f * qty_abs * 100.0 as wcb_option_total,
        ap_f * qty_abs as wcb_mistake_shares
    from mv_num
),

with_cb as (
    select
        *,
        case
            when upper(trim(coalesce(cast(asset_type as string), ''))) = 'OPTION'
                 and ap_f is not null
                 and qty_abs is not null
            then
                case
                    when cb_f is not null
                         and wcb_option_total != 0
                         and abs(cb_f - wcb_option_total) / abs(wcb_option_total) <= 0.20
                    then cb_f
                    when cb_f is not null
                         and wcb_mistake_shares != 0
                         and abs(cb_f - wcb_mistake_shares) / abs(wcb_mistake_shares) <= 0.20
                    then wcb_option_total
                    else coalesce(cb_f, wcb_option_total)
                end
            else cb_f
        end as cb_f_final
    from corrected
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
    cast(cb_f_final as string) as cost_bases,
    cast(safe_subtract(mv_f, cb_f_final) as string) as gain_or_loss_dollat,
    cast(
        case
            when cb_f_final is not null and cb_f_final != 0
                then round(safe_divide(safe_subtract(mv_f, cb_f_final), abs(cb_f_final)) * 100, 4)
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
from with_cb
