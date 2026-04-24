{{
    config(
        materialized='table'
    )
}}

-- Schwab sync and manual upload both merge into trade_history.csv, so
-- there's a single trade history seed to read from. Normalize demo seeds
-- to STRING to match (BigQuery CSV autodetect infers numerics).
with trade_history_as_strings as (
    select
        cast(Account as string) as Account,
        cast(Date as string) as Date,
        cast(Action as string) as Action,
        cast(Symbol as string) as Symbol,
        cast(Description as string) as Description,
        cast(Quantity as string) as Quantity,
        cast(Price as string) as Price,
        cast(fees_and_comm as string) as fees_and_comm,
        cast(Amount as string) as Amount
    from {{ ref('trade_history') }}
),

demo_as_strings as (
    select
        cast(Account as string) as Account,
        cast(Date as string) as Date,
        cast(Action as string) as Action,
        cast(Symbol as string) as Symbol,
        cast(Description as string) as Description,
        cast(Quantity as string) as Quantity,
        cast(Price as string) as Price,
        cast(fees_and_comm as string) as fees_and_comm,
        cast(Amount as string) as Amount
    from {{ ref('demo_history') }}
),

source as (
    select * from trade_history_as_strings
    union all
    select * from demo_as_strings
),

-- Same OSI handling as stg_current: Schwab API uses e.g. "RDDT  261218C00135000"
-- alongside manual export "TICK 12/18/2026 135.00 C".
source_parsed as (
    select
        s.*,
        trim(symbol) as sym_trim,
        upper(trim(symbol)) as sym_upper
    from source s
    where trim(coalesce(action, '')) != ''
      and lower(trim(coalesce(action, ''))) != 'action'  -- filter leaked header row
),

-- BigQuery regexp_extract allows only one capturing group; parse OSI in SQL.
osi_parts as (
    select
        *,
        regexp_extract(sym_upper, r'(\d{6}[CP]\d{8})') as osi_full
    from source_parsed
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

        -- Full trade symbol (export long form or Schwab OSI compact)
        trim(symbol) as trade_symbol,

        -- Underlying ticker (first token)
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
        ) as option_type,  -- 'C' or 'P'

        case
            when coalesce(
                nullif(split(sym_trim, ' ')[safe_offset(3)], ''),
                osi_cp
            ) = 'C' then 'Call'
            when coalesce(
                nullif(split(sym_trim, ' ')[safe_offset(3)], ''),
                osi_cp
            ) = 'P' then 'Put'
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

    from osi_split
)

select * from cleaned
