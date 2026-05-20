{{
    config(
        materialized='table'
    )
}}

-- Schwab sync and manual upload both merge into trade_history.csv, so
-- there's a single trade history seed to read from. Normalize demo seeds
-- to STRING to match (BigQuery CSV autodetect infers numerics).
--
-- ``user_id`` is the new tenant key (see ``docs/USER_ID_TENANCY.md``).
-- Detected via ``adapter.get_columns_in_relation`` so this model keeps
-- building during the deploy gap when the BQ seed table hasn't been
-- rewritten with the new schema yet (e.g. dbt-bigquery's seed loader
-- silently dropping the all-empty user_id column on first deploy).
{% if execute %}
    {%- set _hist_cols = adapter.get_columns_in_relation(ref('trade_history')) | map(attribute='name') | list -%}
    {%- set _demo_cols = adapter.get_columns_in_relation(ref('demo_history')) | map(attribute='name') | list -%}
{% else %}
    {%- set _hist_cols = [] -%}
    {%- set _demo_cols = [] -%}
{% endif %}
{% set _hist_user_id_expr = "cast(user_id as string)" if 'user_id' in _hist_cols else "cast(null as string)" %}
{% set _demo_user_id_expr = "cast(user_id as string)" if 'user_id' in _demo_cols else "cast(null as string)" %}
{# Stage 0 broker_account_id passthrough — see docs/BROKER_ACCOUNT_ID_MIGRATION.md.
   Detected via adapter.get_columns_in_relation so this model keeps building
   during the deploy gap when the BQ seed table hasn't been rewritten with
   the new schema yet. #}
{% set _hist_brk_id_expr = "cast(broker_account_id as string)" if 'broker_account_id' in _hist_cols else "cast(null as string)" %}
{% set _demo_brk_id_expr = "cast(broker_account_id as string)" if 'broker_account_id' in _demo_cols else "cast(null as string)" %}

with trade_history_as_strings as (
    select
        cast(Account as string) as Account,
        {{ _hist_user_id_expr }} as user_id,
        {{ _hist_brk_id_expr }} as broker_account_id,
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
        {{ _demo_brk_id_expr }} as broker_account_id,
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

        -- Tenant key — see docs/USER_ID_TENANCY.md. Stage 0 keeps user_id
        -- nullable so legacy rows (empty string in the seed CSV) survive
        -- the load. safe_cast → NULL on empty / non-numeric values.
        --
        -- Cast through FLOAT64 first because the seed CSV stores user_id
        -- as the pandas-emitted "9.0" / "2.0" decimal-string form (Postgres
        -- bigint values get serialized that way by the Schwab sync). BQ's
        -- safe_cast(STRING -> INT64) refuses any decimal point even when
        -- the fractional part is zero, so a direct cast silently returned
        -- NULL for every Schwab-synced row and broke `user_id`-based
        -- tenancy across the whole warehouse. Going via FLOAT64 -> INT64
        -- accepts "9.0" and still safe-fails on truly bogus strings.
        safe_cast(safe_cast(nullif(trim(user_id), '') as float64) as int64) as user_id,

        -- New stable tenant key — see docs/BROKER_ACCOUNT_ID_MIGRATION.md.
        -- Same FLOAT64-then-INT64 cast as user_id (seed CSV serializes
        -- Postgres BIGINT identically). Nullable through Stages 0-3;
        -- Stage 4 tightens to NOT NULL across the warehouse.
        safe_cast(safe_cast(nullif(trim(broker_account_id), '') as float64) as int64) as broker_account_id,

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
),

-- Orphan-tenant backfill: TWO failure modes, both seen in production.
--
-- (A) NULL → populated.  Broker sync runs BEFORE a user links the
--     account in the Postgres app DB, the resulting CSV rows land with
--     ``user_id = NULL``. After the user later links it, new sync rows
--     arrive tagged with the real user_id. The broker account label
--     (e.g. ``Schwab ••••0044``) is the SAME on both batches because
--     the broker doesn't change masks across syncs.
--
-- (B) Stale-uid → canonical-uid.  Trade-history rows are stamped with
--     a user_id that no longer exists in any current-positions or
--     account-balances surface (i.e. the broker no longer sees this
--     account under that user). Happens when:
--       - A user record gets renumbered or merged (e.g. test data
--         imported under uid=2 by a one-off script, while the
--         actually-logged-in user is uid=9 and owns the snapshot).
--       - A user re-links the same Schwab account under a NEW uid
--         after deleting the old account row in the app DB.
--     Detection: if `stg_current` ∪ `stg_account_balances` shows the
--     account under exactly ONE user_id, that uid is canonical and any
--     other uid in trade history is stale and gets re-stamped.
--
-- Without these backfills, downstream models that partition by
-- ``(account, user_id)`` (positions_summary, int_strategy_classification,
-- mart_daily_pnl, int_dividend_events, int_equity_sessions, …) treat
-- mismatched-uid rows as TWO DIFFERENT positions: buys sit in one
-- bucket while the current snapshot sits in another. The user's
-- Position Detail page reads $0 realized P&L on a fully-closed
-- position with thousands of dollars of actual proceeds, and the
-- dividend stream attaches to the wrong tenant entirely.
--
-- Real examples:
--   May 2026 (A): JEPI on Schwab ••••0044 showed $0 realized +
--   $0 dividends despite buy + 2 sells totaling ~$4,300 of realized
--   P&L. Recon banner caught a $2,560 chart vs $0 mart gap.
--
--   May 2026 (B): IYW on Emmory Investment showed two "Leg 1" pills,
--   a phantom "Dividend Closed -$1,957" strategy row, and a chart
--   terminal of -$1,957 vs hero $396.67 (the trade history rows were
--   stamped uid=2 and the current snapshot was stamped uid=9; the
--   account is the same broker account in both cases).
--
-- (B) Canonical owner per account. As of May 2026 this lives in its
-- own staging model: ``stg_canonical_account_owner``. The previous
-- in-line CTE pulled from stg_current ∪ stg_account_balances with a
-- ``count(distinct user_id) = 1`` guard, which silently failed to fire
-- when the BROKER surfaces were themselves dual-stamped (real example:
-- Cameron Investment / PLTR — same account stamped under uid=9 and
-- uid=13 across history AND current AND balances; every page surface
-- doubled). The new model picks the uid with the most recent trade
-- activity per account, which uniquely identifies the live tenant even
-- when both uids look "populated."
--
-- Read order (precedence, lowest to highest):
--   3) trade-history-only inferred uid (NULL → populated fallback)
--   2) raw a.user_id (the row's stamp)
--   1) canonical_user_id from stg_canonical_account_owner (wins ALWAYS
--      when set — the broker is canonical for "who owns this RIGHT NOW")
--
-- After backfill, rows that were stamped under a stale uid have been
-- rewritten to the canonical uid. They are now exact duplicates of
-- rows that were already canonical-stamped, so a final dedupe step
-- collapses them. Rows that ONLY exist under a stale uid (rare —
-- happens when historical sync stamped a uid that later got
-- de-linked) survive after the rewrite under the canonical uid.

-- (A) Trade-history-only fallback when no current/balance snapshot
-- exists for the account (fully-closed accounts, pre-history holdings).
account_owner as (
    select
        account,
        any_value(user_id) as inferred_user_id
    from amount_signed
    where user_id is not null
    group by 1
    having count(distinct user_id) = 1
),

backfilled as (
    select
        a.account,
        coalesce(co.canonical_user_id, a.user_id, ao.inferred_user_id) as user_id,
        a.broker_account_id,
        a.trade_date, a.action_raw, a.action,
        a.trade_symbol, a.underlying_symbol,
        a.option_expiry, a.option_strike, a.option_type,
        a.instrument_type, a.description,
        a.quantity, a.price, a.fees, a.amount
    from amount_signed a
    left join account_owner ao using (account)
    left join {{ ref('stg_canonical_account_owner') }} co using (account)
),

-- Dedupe rewritten fills.
--
-- When the canonical-uid rewrite collapses two stale stamps onto the
-- same canonical uid, the resulting rows are EXACT duplicates by
-- natural composite key (account, user_id, trade_date, trade_symbol,
-- action, quantity, amount, fees) — the seed-merge had ingested the
-- same broker fill under each historical uid. Without this dedupe
-- the page would still double (it'd just be double-stamped under the
-- canonical uid instead of split across uids). Tie-break is arbitrary
-- since the rows are byte-identical post-rewrite.
deduped as (
    select * from backfilled
    qualify row_number() over (
        -- BigQuery rejects FLOAT64 in PARTITION BY of window functions
        -- (NaN equality semantics). Stringify quantity/amount/fees so the
        -- byte-identical rewritten rows partition together. Exact-zero
        -- vs NULL deduplicates fine under this cast — both serialize
        -- deterministically.
        partition by
            account, user_id, trade_date, trade_symbol, action,
            cast(quantity as string),
            cast(amount   as string),
            cast(fees     as string)
        order by description nulls last
    ) = 1
)

-- NOTE on dividend reinvestment (DRIP) classification.
--
-- Detection lives DOWNSTREAM of stg_history in
-- ``int_drip_fills`` (intermediate/), not here. Reason: detection
-- requires joining to ``stg_daily_prices`` (yfinance ex-div calendar
-- — a fractional buy is a DRIP only when its trade_date is the
-- broker's payable date for a recent ex-div on the same symbol).
--
-- Putting that join in ``stg_history`` would move stg_history into
-- ``stg_daily_prices+`` and the CI workflow's two-pass build
-- (``dbt build --exclude "stg_daily_prices+"`` then
-- ``dbt build --select "stg_daily_prices+"``) would skip stg_history
-- (and effectively the whole warehouse) in Pass 1.
--
-- Consumers that need the DRIP flag join to ``int_drip_fills`` on
-- ``(account, user_id, trade_date, underlying_symbol)``.

select
    account, user_id, broker_account_id,
    trade_date, action_raw, action, trade_symbol, underlying_symbol,
    option_expiry, option_strike, option_type, instrument_type, description,
    quantity, price, fees, amount
from deduped
-- Drop non-tradeable entries that Schwab Connect emits as fake "Buy" rows:
--   - CURRENCY_USD (cash settlement/transfer pseudo-trades)
--   - CUSIPs (e.g. "09247X101") — money-market funds and other non-ticker
--     securities that we don't price or chart, so they pollute positions
--     dashboards without adding signal.
where underlying_symbol != 'CURRENCY_USD'
  and not regexp_contains(underlying_symbol, r'^[A-Z0-9]{8}[0-9]$')
