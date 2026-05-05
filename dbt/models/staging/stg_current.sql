{{
    config(
        materialized='view'
    )
}}

-- Schwab sync and manual upload both merge into current_positions.csv, so
-- there's a single positions seed to read from. Normalize everything to
-- STRING so the demo union works regardless of BigQuery CSV type inference.
{%- set current_string_cols -%}
        cast(Account as string) as Account,
        cast(user_id as string) as user_id,
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
{%- endset %}

with current_as_strings as (
    select {{ current_string_cols }}
    from {{ ref('current_positions') }}
),

demo_as_strings as (
    select {{ current_string_cols }}
    from {{ ref('demo_current') }}
),

source as (
    select * from current_as_strings
    union all
    select * from demo_as_strings
),

-- Schwab API / some feeds use CBOE OSI (e.g. "RDDT  261218C00135000"); manual export uses
-- "TICK 12/18/2026 135.00 C". Parse both so Call/Put and snapshot unions work.
source_with_osi as (
    select
        s.*,
        trim(symbol) as sym_trim,
        upper(trim(symbol)) as sym_upper
    from source s
    where lower(trim(coalesce(security_type, ''))) not in ('cash and money market', '')
      and lower(trim(coalesce(symbol, ''))) not in ('account total', 'positions total')
),

-- BigQuery regexp_extract allows only one capturing group; parse OSI in SQL.
osi_parts as (
    select
        *,
        regexp_extract(sym_upper, r'(\d{6}[CP]\d{8})') as osi_full
    from source_with_osi
),

osi_split as (
    select
        *,
        substr(osi_full, 1, 6) as osi_ymd,
        substr(osi_full, 7, 1) as osi_cp,
        substr(osi_full, 8, 8) as osi_strike_raw
    from osi_parts
),

cleaned as (
    select
        trim(account) as account,

        -- Tenant key — see docs/USER_ID_TENANCY.md. Stage 0: nullable.
        safe_cast(nullif(trim(user_id), '') as int64) as user_id,

        -- Full trade symbol
        trim(symbol) as trade_symbol,

        -- Underlying ticker (first token; OSI and export both start with root)
        trim(split(sym_trim, ' ')[safe_offset(0)]) as underlying_symbol,

        coalesce(
            safe.parse_date('%m/%d/%Y', nullif(split(sym_trim, ' ')[safe_offset(1)], '')),
            case
                when osi_ymd is not null
                then date(
                    2000 + cast(substr(osi_ymd, 1, 2) as int64),
                    cast(substr(osi_ymd, 3, 2) as int64),
                    cast(substr(osi_ymd, 5, 2) as int64)
                )
            end
        ) as option_expiry,

        coalesce(
            safe_cast(split(sym_trim, ' ')[safe_offset(2)] as float64),
            safe_cast(safe_divide(safe_cast(osi_strike_raw as int64), 1000) as float64)
        ) as option_strike,

        coalesce(
            nullif(split(sym_trim, ' ')[safe_offset(3)], ''),
            osi_cp
        ) as option_type,

        case
            when coalesce(
                nullif(split(sym_trim, ' ')[safe_offset(3)], ''),
                osi_cp
            ) = 'C' then 'Call'
            when coalesce(
                nullif(split(sym_trim, ' ')[safe_offset(3)], ''),
                osi_cp
            ) = 'P' then 'Put'
            when lower(trim(coalesce(security_type, ''))) in ('equity', 'etfs & closed end funds') then 'Equity'
            else 'Other'
        end as instrument_type,

        trim(description) as description,
        safe_cast(quantity as float64) as quantity,
        safe_cast(price as float64) as current_price,
        -- Schwab CSV often has market_value/cost_bases as "$9,220.95"; strip $ and commas
        safe_cast(trim(replace(replace(replace(coalesce(cast(market_value as string), ''), '$', ''), ',', ''), ' ', '')) as float64) as market_value,
        safe_cast(trim(replace(replace(replace(coalesce(cast(cost_bases as string), ''), '$', ''), ',', ''), ' ', '')) as float64) as cost_basis,
        safe_cast(trim(replace(replace(replace(coalesce(cast(gain_or_loss_dollat as string), ''), '$', ''), ',', ''), ' ', '')) as float64) as unrealized_pnl,  -- typo in source
        safe_cast(trim(replace(replace(replace(coalesce(cast(gain_or_loss_percent as string), ''), '%', ''), ',', ''), ' ', '')) as float64) as unrealized_pnl_pct,
        trim(security_type) as security_type_raw,
        trim(in_the_money) as in_the_money,
        safe_cast(dividend_yield as float64) as dividend_yield,
        safe_cast(pe_ratio as float64) as pe_ratio,
        current_date() as snapshot_date

    from osi_split
)

select * from cleaned
