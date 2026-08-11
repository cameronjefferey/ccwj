{{
    config(
        materialized='view'
    )
}}

-- v2 staging — see docs/V2_TENANT_KEY_DESIGN.md.
--
-- ``tenant_id`` is the v2 warehouse tenant key. Under v2 every row
-- is stamped at sync time with a broker-stable, never-recycled
-- tenant_id, so the v1 orphan-tenant / split-uid bug class is
-- structurally impossible. See docs/V2_TENANT_KEY_DESIGN.md.
-- Real-broker rows now arrive via the per-broker staging adapters
-- (dbt/models/staging/brokers/stg_broker_<slug>_current) rather than a
-- direct read of the current_positions seed. See stg_history.sql and
-- dbt/macros/broker_slug_from_account.sql for the rationale and the
-- add-a-brokerage procedure. The OSI parse / short-aware recompute /
-- dedup below are unchanged.
--
-- The demo branch is a relabeled MIRROR of a real tenant (the
-- EarningsFollower bot's Alpaca paper account), not fabricated seed data —
-- see dbt/models/staging/demo/stg_demo_current.sql.

with current_as_strings as (
    select * from {{ ref('stg_broker_schwab_current') }}
    union all
    select * from {{ ref('stg_broker_alpaca_current') }}
    union all
    select * from {{ ref('stg_broker_fidelity_current') }}
    union all
    select * from {{ ref('stg_broker_interactive_current') }}
    union all
    select * from {{ ref('stg_broker_other_current') }}
),

demo_as_strings as (
    select * from {{ ref('stg_demo_current') }}
),

source as (
    select * from current_as_strings
    union all
    select * from demo_as_strings
),

source_with_osi as (
    select
        s.*,
        trim(symbol) as sym_trim,
        upper(trim(symbol)) as sym_upper
    from source s
    where lower(trim(coalesce(security_type, ''))) not in ('cash and money market', '')
      and lower(trim(coalesce(symbol, ''))) not in ('account total', 'positions total')
),

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

cleaned_raw as (
    select
        trim(account) as account,

        safe_cast(safe_cast(nullif(trim(user_id), '') as float64) as int64) as user_id,

        nullif(trim(tenant_id), '') as tenant_id,

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

        -- Option Call/Put flag. The space-split token is ONLY honored when
        -- it is literally 'C'/'P' (the alternate human-readable format
        -- "AAPL 01/17/2026 150 C", where offset(3) = 'C'); otherwise the
        -- unambiguous OSI C/P char (osi_cp, from the `\d{6}[CP]\d{8}` slice)
        -- wins. Bug fixed 2026-07-08: for a 3-character root the OSI string
        -- is space-padded to 6 chars ("DAL   260717C00093000"), so
        -- split(' ')[3] lands on the OSI tail ("260717C00093000") instead
        -- of a C/P token; the old unguarded coalesce(token, osi_cp) let that
        -- tail shadow the correct osi_cp='C', typing every 3-char-root
        -- option as 'Other'. That dropped them from int_option_contracts'
        -- snapshot join (instrument_type in ('Call','Put')), so open-option
        -- P&L fell back to net premium instead of mark-to-market (DAL iron
        -- condor showed the $1,483 net credit as "unrealized" vs the true
        -- $73.34 MTM). This mirrors the guarded form already in stg_history.
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
            when lower(trim(coalesce(security_type, ''))) in (
                'equity', 'etfs & closed end funds', 'cryptocurrency'
            ) then 'Equity'
            else 'Other'
        end as instrument_type,

        trim(description) as description,
        safe_cast(quantity as float64) as quantity,
        safe_cast(price as float64) as current_price,
        safe_cast(trim(replace(replace(replace(coalesce(cast(market_value as string), ''), '$', ''), ',', ''), ' ', '')) as float64) as market_value,
        safe_cast(trim(replace(replace(replace(coalesce(cast(cost_bases as string), ''), '$', ''), ',', ''), ' ', '')) as float64) as cost_basis,
        safe_cast(trim(replace(replace(replace(coalesce(cast(gain_or_loss_dollat as string), ''), '$', ''), ',', ''), ' ', '')) as float64) as unrealized_pnl,
        safe_cast(trim(replace(replace(replace(coalesce(cast(gain_or_loss_percent as string), ''), '%', ''), ',', ''), ' ', '')) as float64) as unrealized_pnl_pct,
        trim(security_type) as security_type_raw,
        trim(in_the_money) as in_the_money,
        safe_cast(dividend_yield as float64) as dividend_yield,
        safe_cast(pe_ratio as float64) as pe_ratio,
        current_date() as snapshot_date

    from osi_split
),

-- Schwab's `gain_or_loss_dollat` for SHORT options (qty < 0) flips sign;
-- recompute correctly using sign-aware formula. See v1 stg_current
-- comment for the full incident write-up.
cleaned as (
    select
        * except (unrealized_pnl, unrealized_pnl_pct),
        case
            when instrument_type in ('Call', 'Put')
                 and quantity is not null
                 and quantity < 0
                 and market_value is not null
                 and cost_basis is not null
            then market_value + cost_basis
            else unrealized_pnl
        end as unrealized_pnl,
        case
            when instrument_type in ('Call', 'Put')
                 and quantity is not null
                 and quantity < 0
                 and market_value is not null
                 and cost_basis is not null
                 and cost_basis != 0
            then 100.0 * (market_value + cost_basis) / abs(cost_basis)
            else unrealized_pnl_pct
        end as unrealized_pnl_pct
    from cleaned_raw
),

-- Belt-and-suspenders dedup on (tenant_id, trade_symbol). Under v2
-- partition is on tenant_id (the structural tenant key) not on
-- (account, user_id) — the same physical broker account is one
-- tenant_id and dupes on it are real dupes.
deduped as (
    select *
    from cleaned
    qualify row_number() over (
        partition by coalesce(tenant_id, account), trade_symbol
        order by case when market_value is not null then 0 else 1 end,
                 case when cost_basis  is not null then 0 else 1 end,
                 market_value desc nulls last
    ) = 1
)

select * from deduped
