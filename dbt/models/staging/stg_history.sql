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

        -- Only accept the "export" 4th token if it's literally 'C' or 'P';
        -- otherwise the trade_symbol is the Schwab compact OSI form ("NET   240119C00080000")
        -- where split-by-space returns the OSI as the 4th token, which would
        -- corrupt option_type and cascade into instrument_type='Equity'.
        coalesce(
            case when nullif(split(sym_trim, ' ')[safe_offset(3)], '') in ('C', 'P')
                 then nullif(split(sym_trim, ' ')[safe_offset(3)], '')
            end,
            osi_cp
        ) as option_type,  -- 'C' or 'P'

        case
            when coalesce(
                case when nullif(split(sym_trim, ' ')[safe_offset(3)], '') in ('C', 'P')
                     then nullif(split(sym_trim, ' ')[safe_offset(3)], '')
                end,
                osi_cp
            ) = 'C' then 'Call'
            when coalesce(
                case when nullif(split(sym_trim, ' ')[safe_offset(3)], '') in ('C', 'P')
                     then nullif(split(sym_trim, ' ')[safe_offset(3)], '')
                end,
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
        coalesce(safe_cast(amount as float64), 0) as amount_raw

    from osi_split
),

-- Normalize amount sign by action.
--
-- Different upstream sources have shipped contradictory sign conventions for
-- the Amount column (older Schwab Connect: buys positive / STO negative; CSV
-- export and newer Schwab Connect: buys negative / STO positive). To guarantee
-- downstream models — int_equity_sessions, int_option_contracts, int_dividends —
-- always see a consistent "negative = cash out, positive = cash in" convention,
-- we re-sign every unambiguous action here using the absolute amount.
amount_signed as (
    select
        c.* except (amount_raw),
        case
            -- Cash out (negative)
            when c.action in (
                'equity_buy',
                'option_buy_to_open',
                'option_buy_to_close',
                'margin_interest',
                'adr_fee'
            ) then -abs(c.amount_raw)

            -- Cash in (positive)
            when c.action in (
                'equity_sell',
                'equity_sell_short',
                'option_sell_to_open',
                'option_sell_to_close',
                'dividend',
                'credit_interest'
            ) then abs(c.amount_raw)

            -- option_assigned / option_exercised / option_expired / 'other':
            -- preserve whatever the source reports (the broker's signed amount
            -- correctly captures the direction of the resulting equity flow).
            else c.amount_raw
        end as amount
    from cleaned c
)

select
    account, trade_date, action_raw, action, trade_symbol, underlying_symbol,
    option_expiry, option_strike, option_type, instrument_type, description,
    quantity, price, fees, amount
from amount_signed
-- Drop non-tradeable entries that Schwab Connect emits as fake "Buy" rows:
--   - CURRENCY_USD (cash settlement/transfer pseudo-trades)
--   - CUSIPs (e.g. "09247X101") — money-market funds and other non-ticker
--     securities that we don't price or chart, so they pollute positions
--     dashboards without adding signal.
where underlying_symbol != 'CURRENCY_USD'
  and not regexp_contains(underlying_symbol, r'^[A-Z0-9]{8}[0-9]$')
