{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ ref('0417_history') }}
    union all
    select * from {{ ref('demo_history') }}
),

cleaned as (
    select
        trim(account) as account,

        -- Parse the effective date: use the "as of" date when present, otherwise the main date
        safe.parse_date(
            '%m/%d/%Y',
            regexp_extract(date, r'(\d{1,2}/\d{1,2}/\d{4})$')
        ) as trade_date,

        -- Keep raw action for debugging
        trim(action) as action_raw,

        -- Normalize action into a clean taxonomy
        case lower(trim(action))
            when 'buy'                  then 'equity_buy'
            when 'sell'                 then 'equity_sell'
            when 'sell short'           then 'equity_sell_short'
            when 'sell to open'         then 'option_sell_to_open'
            when 'buy to close'         then 'option_buy_to_close'
            when 'buy to open'          then 'option_buy_to_open'
            when 'sell to close'        then 'option_sell_to_close'
            when 'expired'              then 'option_expired'
            when 'assigned'             then 'option_assigned'
            when 'exchange or exercise' then 'option_exercised'
            when 'qualified dividend'   then 'dividend'
            when 'cash dividend'        then 'dividend'
            when 'special dividend'     then 'dividend'
            when 'special qual div'     then 'dividend'
            when 'pr yr cash div'       then 'dividend'
            when 'margin interest'      then 'margin_interest'
            when 'credit interest'      then 'credit_interest'
            when 'adr mgmt fee'        then 'adr_fee'
            else 'other'
        end as action,

        -- Full trade symbol (e.g. "CFLT 07/18/2025 26.00 C")
        trim(symbol) as trade_symbol,

        -- Underlying ticker (first token of the symbol)
        trim(split(trim(symbol), ' ')[safe_offset(0)]) as underlying_symbol,

        -- Option components (null for non-options)
        safe.parse_date('%m/%d/%Y', split(trim(symbol), ' ')[safe_offset(1)]) as option_expiry,
        safe_cast(split(trim(symbol), ' ')[safe_offset(2)] as float64) as option_strike,
        split(trim(symbol), ' ')[safe_offset(3)] as option_type,  -- 'C' or 'P'

        -- High-level instrument classification
        case
            when split(trim(symbol), ' ')[safe_offset(3)] = 'C' then 'Call'
            when split(trim(symbol), ' ')[safe_offset(3)] = 'P' then 'Put'
            when lower(trim(action)) in (
                'qualified dividend', 'cash dividend', 'special dividend',
                'special qual div', 'pr yr cash div'
            ) then 'Dividend'
            when lower(trim(action)) in (
                'margin interest', 'credit interest', 'adr mgmt fee'
            ) then 'Cash Event'
            else 'Equity'
        end as instrument_type,

        trim(description) as description,
        safe_cast(quantity as float64) as quantity,
        safe_cast(price as float64) as price,
        coalesce(safe_cast(fees_and_comm as float64), 0) as fees,
        coalesce(safe_cast(amount as float64), 0) as amount

    from source
    where trim(coalesce(action, '')) != ''
      and lower(trim(coalesce(action, ''))) != 'action'  -- filter leaked header row
)

select * from cleaned
