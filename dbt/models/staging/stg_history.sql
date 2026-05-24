{{
    config(
        materialized='table'
    )
}}

-- v2 staging — see docs/V2_TENANT_KEY_DESIGN.md.
--
-- ``tenant_id`` is the v2 warehouse tenant key. Format:
--     ``"<broker_slug>:<broker_uuid>"``
-- e.g. ``"snaptrade:bed78305-a764-4c4d-b4c7-fe59e391f661"``.
-- Broker-stable (SnapTrade ships the UUID), never minted by us, never
-- transformed in transit. The structural property that retires the
-- orphan-tenancy / NULL-uid-backfill / canonical-owner-rewrite code
-- paths the v1 staging carried.
--
-- ``user_id`` is now INFORMATIONAL only — kept for admin / debug
-- surfaces. Tenant isolation is on ``tenant_id`` everywhere.
--
-- ``account`` is the broker-shipped display label (e.g. "Schwab ••••6342")
-- and stays as a column for templates that show account names. It is
-- NOT the join key.
--
-- The demo seed union is preserved so the demo user keeps working —
-- demo rows have tenant_id = NULL by convention and are filtered out
-- of every tenant-scoped read by ``_filter_df_by_tenant_ids``.
{% if execute %}
    {%- set _hist_cols = adapter.get_columns_in_relation(ref('trade_history')) | map(attribute='name') | list -%}
    {%- set _demo_cols = adapter.get_columns_in_relation(ref('demo_history')) | map(attribute='name') | list -%}
{% else %}
    {%- set _hist_cols = [] -%}
    {%- set _demo_cols = [] -%}
{% endif %}
{% set _hist_user_id_expr = "cast(user_id as string)" if 'user_id' in _hist_cols else "cast(null as string)" %}
{% set _demo_user_id_expr = "cast(user_id as string)" if 'user_id' in _demo_cols else "cast(null as string)" %}
{% set _hist_tenant_id_expr = "cast(tenant_id as string)" if 'tenant_id' in _hist_cols else "cast(null as string)" %}
{% set _demo_tenant_id_expr = "cast(tenant_id as string)" if 'tenant_id' in _demo_cols else "cast(null as string)" %}

with trade_history_as_strings as (
    select
        cast(Account as string) as Account,
        {{ _hist_user_id_expr }} as user_id,
        {{ _hist_tenant_id_expr }} as tenant_id,
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
        {{ _demo_user_id_expr }} as user_id,
        {{ _demo_tenant_id_expr }} as tenant_id,
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

source_parsed as (
    select
        s.*,
        trim(symbol) as sym_trim,
        upper(trim(symbol)) as sym_upper
    from source s
    where trim(coalesce(action, '')) != ''
      and lower(trim(coalesce(action, ''))) != 'action'
),

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

        -- user_id is informational under v2. Same FLOAT64-then-INT64
        -- coercion as v1 to handle pandas-emitted "9.0" decimal-string
        -- form from Postgres BIGINT exports.
        safe_cast(safe_cast(nullif(trim(user_id), '') as float64) as int64) as user_id,

        -- tenant_id is the v2 warehouse tenant key. Empty/NULL passes
        -- through; demo rows always have NULL here. Filters on
        -- ``tenant_id is not null`` are how tenant-scoped marts exclude
        -- demo data.
        nullif(trim(tenant_id), '') as tenant_id,

        safe.parse_date(
            '%m/%d/%Y',
            regexp_extract(date, r'(\d{1,2}/\d{1,2}/\d{4})$')
        ) as trade_date,

        trim(action) as action_raw,

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
            when 'adr mgmt fee'         then 'adr_fee'
            else 'other'
        end as action,

        trim(symbol) as trade_symbol,

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
            case when nullif(split(sym_trim, ' ')[safe_offset(3)], '') in ('C', 'P')
                 then nullif(split(sym_trim, ' ')[safe_offset(3)], '')
            end,
            osi_cp
        ) as option_type,

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

amount_signed as (
    select
        c.* except (amount_raw),
        case
            when c.action in (
                'equity_buy',
                'option_buy_to_open',
                'option_buy_to_close',
                'margin_interest',
                'adr_fee'
            ) then -abs(c.amount_raw)

            when c.action in (
                'equity_sell',
                'equity_sell_short',
                'option_sell_to_open',
                'option_sell_to_close',
                'dividend',
                'credit_interest'
            ) then abs(c.amount_raw)

            else c.amount_raw
        end as amount
    from cleaned c
)

select
    account, user_id, tenant_id,
    trade_date, action_raw, action, trade_symbol, underlying_symbol,
    option_expiry, option_strike, option_type, instrument_type, description,
    quantity, price, fees, amount
from amount_signed
where underlying_symbol != 'CURRENCY_USD'
  and not regexp_contains(underlying_symbol, r'^[A-Z0-9]{8}[0-9]$')
