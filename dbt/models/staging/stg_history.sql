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
-- The demo union is preserved so the demo user keeps working — demo rows
-- carry tenant_id = 'demo:demo-account' matching the demo user's
-- broker_tenants row created by ``ensure_demo_user``, so the public demo
-- renders through the exact same tenant scoping as a real user. Since
-- Aug 2026 the demo is no longer fabricated seed CSVs but a relabeled
-- MIRROR of a real tenant (the EarningsFollower bot's Alpaca paper
-- account) — see dbt/models/staging/demo/stg_demo_history.sql.
--
-- Real-broker rows now arrive via the per-broker staging adapters
-- (dbt/models/staging/brokers/stg_broker_<slug>_history) rather than a
-- direct read of the trade_history seed. Each broker has its own model so
-- broker-specific quirks stay isolated and independently testable; the
-- ``_other_`` catch-all carries any not-yet-modeled broker so no row is
-- dropped. See dbt/macros/broker_slug_from_account.sql for how to add a
-- brokerage. The heavy OSI/option/dividend parse below is unchanged.

with trade_history_as_strings as (
    select * from {{ ref('stg_broker_schwab_history') }}
    union all
    select * from {{ ref('stg_broker_alpaca_history') }}
    union all
    select * from {{ ref('stg_broker_fidelity_history') }}
    union all
    select * from {{ ref('stg_broker_interactive_history') }}
    union all
    select * from {{ ref('stg_broker_other_history') }}
),

demo_as_strings as (
    select * from {{ ref('stg_demo_history') }}
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

        -- See ``parse_seed_date`` — MDY (4- and 2-digit year) + ISO,
        -- matching ``app.upload._canonicalize_date_mdy``. Run 33142404800
        -- still had 40 NULL-date CHECK 1 groups: the manual Schwab CSV
        -- tenant writes ``1/20/23`` (two-digit year).
        {{ parse_seed_date('date') }} as trade_date,

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
            -- External cash movements (the trader adding/removing their own
            -- money). NOT a trade — folded into a single ``cash_transfer``
            -- action so the /wealth + /accounts "exclude deposits &
            -- withdrawals" toggle can net them out. Sign is preserved from
            -- the seed (deposit +, withdrawal −) via the ``else`` branch in
            -- amount_signed below. Inert in every P&L / session / dividend
            -- model — they all filter to Equity/Call/Put/dividend.
            when 'deposit'              then 'cash_transfer'
            when 'withdrawal'           then 'cash_transfer'
            when 'cash transfer'        then 'cash_transfer'
            -- Schwab web-export labels (CSV upload). SnapTrade writes
            -- Deposit/Withdrawal; the CSV uses these instead. Cash-only
            -- Journal (no ticker, non-zero amount) is mapped just below.
            when 'funds received'       then 'cash_transfer'
            when 'moneylink transfer'   then 'cash_transfer'
            -- Schwab CSV ``Journal`` is cash when there is no ticker and a
            -- non-zero Amount (Emmory 2025-01-06 $500 FRM …852). Share
            -- journals carry a symbol/qty and stay ``other``.
            when 'journal' then
                case
                    when nullif(trim(symbol), '') is null
                     and abs(coalesce({{ parse_seed_number('amount') }}, 0)) > 0.005
                     and abs(coalesce({{ parse_seed_number('quantity') }}, 0)) < 1e-9
                    then 'cash_transfer'
                    else 'other'
                end
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
                'margin interest', 'credit interest', 'adr mgmt fee',
                'deposit', 'withdrawal', 'cash transfer',
                'funds received', 'moneylink transfer'
            ) then 'Cash Event'
            when lower(trim(action)) = 'journal'
             and nullif(trim(symbol), '') is null
            then 'Cash Event'
            else 'Equity'
        end as instrument_type,

        trim(description) as description,
        {{ parse_seed_number('quantity') }} as quantity,
        {{ parse_seed_number('price') }} as price,
        coalesce({{ parse_seed_number('fees_and_comm') }}, 0) as fees,
        coalesce({{ parse_seed_number('amount') }}, 0) as amount_raw

    from osi_split
),

amount_signed as (
    select
        c.* except (amount_raw),
        -- 1 when this row's Amount was still the gross premium and we
        -- netted ``fees`` here. The broker statement is already net, so
        -- it stays 0. After the expiry-date cap below, an order row and
        -- its later activity can share a fill key; prefer the statement.
        (
            c.action in (
                'option_buy_to_open', 'option_buy_to_close',
                'option_sell_to_open', 'option_sell_to_close'
            )
            and abs(coalesce(c.fees, 0)) > 0.005
            and c.quantity is not null
            and c.price is not null
            and abs(
                   abs(c.amount_raw) - abs(c.quantity) * c.price * 100
                ) <= 0.05
        ) as fee_adjusted,
        case
            -- Option order rows store gross premium (qty × price × 100)
            -- and the commission in ``fees``. The broker statement Amount
            -- is already net (Sep SPXW: 5 × 2.87 × 100 − 6.11 = 1,428.89).
            -- Subtract the fee only when the raw amount still matches the
            -- gross, so a net activity amount is not charged twice.
            when c.action in (
                'option_buy_to_open', 'option_buy_to_close',
                'option_sell_to_open', 'option_sell_to_close'
            )
             and abs(coalesce(c.fees, 0)) > 0.005
             and c.quantity is not null
             and c.price is not null
             and abs(
                    abs(c.amount_raw) - abs(c.quantity) * c.price * 100
                 ) <= 0.05
            then case
                when c.action in ('option_sell_to_open', 'option_sell_to_close')
                    then abs(c.quantity) * c.price * 100 - abs(c.fees)
                else -(abs(c.quantity) * c.price * 100 + abs(c.fees))
            end

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
),

-- Pair tickers (BTC-USD, BTCUSD) collapse onto the bare crypto symbol
-- the snapshot already uses, so fills join the holding. See
-- macros/crypto_pair_base.sql. Options keep their OSI trade_symbol.
crypto_norm as (
    select
        a.* except (trade_symbol, underlying_symbol),
        case
            when a.instrument_type in ('Call', 'Put') then a.trade_symbol
            else coalesce(ct.symbol, a.trade_symbol)
        end as trade_symbol,
        coalesce(cu.symbol, a.underlying_symbol) as underlying_symbol
    from amount_signed a
    left join {{ ref('stg_crypto_symbols') }} cu
        on cu.symbol = {{ crypto_pair_base_expr('a.underlying_symbol') }}
       and cu.symbol != ''
    left join {{ ref('stg_crypto_symbols') }} ct
        on a.instrument_type not in ('Call', 'Put')
       and ct.symbol = {{ crypto_pair_base_expr('a.trade_symbol') }}
       and ct.symbol != ''
),

-- A cash-settlement fill is often dated the broker's posting day
-- (SPXW Oct 1 expiry arrived as Oct 2). "as of MM/DD/YY(YY)" is the
-- trade date. Otherwise a close after the contract's own expiry is
-- the posting date — cap it at expiry. An earlier close keeps its
-- trade date. Opens are untouched.
-- ``_cross_source_fill_date`` in app/upload.py and
-- scripts/repair_history_fill_dedup.py must use this same cap. The
-- seed keeps the posting date; only the key and this column move.
-- Run 37090194526: BE 260306C00165000 landed twice (−42.92 / −42.12)
-- once both dates became the 2026-03-06 expiry.
dated as (
    select
        * except (trade_date),
        case
            when action in (
                'option_buy_to_close', 'option_sell_to_close',
                'option_expired', 'option_assigned', 'option_exercised'
            )
            then least(
                coalesce(as_of_date, trade_date),
                coalesce(option_expiry, as_of_date, trade_date)
            )
            else trade_date
        end as trade_date
    from (
        select
            c.*,
            safe.parse_date(
                '%m/%d/%Y',
                case
                    when regexp_extract(
                        c.description, r'(?i)\bas of\s+\d{1,2}/\d{1,2}/(\d{4})\b'
                    ) is not null
                    then concat(
                        lpad(regexp_extract(
                            c.description, r'(?i)\bas of\s+(\d{1,2})/\d{1,2}/\d{4}\b'
                        ), 2, '0'),
                        '/',
                        lpad(regexp_extract(
                            c.description, r'(?i)\bas of\s+\d{1,2}/(\d{1,2})/\d{4}\b'
                        ), 2, '0'),
                        '/',
                        regexp_extract(
                            c.description, r'(?i)\bas of\s+\d{1,2}/\d{1,2}/(\d{4})\b'
                        )
                    )
                    when regexp_extract(
                        c.description, r'(?i)\bas of\s+\d{1,2}/\d{1,2}/(\d{2})\b'
                    ) is not null
                    then concat(
                        lpad(regexp_extract(
                            c.description, r'(?i)\bas of\s+(\d{1,2})/\d{1,2}/\d{2}\b'
                        ), 2, '0'),
                        '/',
                        lpad(regexp_extract(
                            c.description, r'(?i)\bas of\s+\d{1,2}/(\d{1,2})/\d{2}\b'
                        ), 2, '0'),
                        '/',
                        case
                            when cast(regexp_extract(
                                c.description,
                                r'(?i)\bas of\s+\d{1,2}/\d{1,2}/(\d{2})\b'
                            ) as int64) < 80
                            then concat('20', regexp_extract(
                                c.description,
                                r'(?i)\bas of\s+\d{1,2}/\d{1,2}/(\d{2})\b'
                            ))
                            else concat('19', regexp_extract(
                                c.description,
                                r'(?i)\bas of\s+\d{1,2}/\d{1,2}/(\d{2})\b'
                            ))
                        end
                    )
                end
            ) as as_of_date
        from crypto_norm c
    )
),

-- Capping a posting date at expiry (or reading "as of") can land an
-- order fill and its later activity on the same check-2 grain:
-- (tenant, trade_date, action, trade_symbol, quantity, price@4dp).
-- Run 37090194526: BE 03/06/26 165C buy-to-close, 10 @ 0.042, amounts
-- -42.92 and -42.12. The raw dates still differ, so upload dedup keeps
-- both (rewriting the seed date is what inserted the twin). Collapse
-- here, after the date rewrite. Blank-price rows stay put — distinct
-- expiries share an empty Symbol and must not fuse.
fill_ranked as (
    select
        d.*,
        row_number() over (
            -- BigQuery rejects FLOAT64 in PARTITION BY (run 37091190523).
            -- NUMERIC matches the test grain: exact quantity, price at 4dp.
            partition by
                d.tenant_id,
                d.trade_date,
                d.action,
                d.trade_symbol,
                cast(round(d.quantity, 6) as numeric),
                cast(round(d.price, 4) as numeric)
            order by
                if(d.fee_adjusted, 1, 0) asc,
                length(coalesce(d.description, '')) desc,
                cast(round(abs(d.amount), 2) as numeric) asc,
                cast(round(d.amount, 2) as numeric) asc
        ) as _fill_rank
    from dated d
    where d.trade_symbol is not null
      and d.price is not null
),

history_rows as (
    select * except (_fill_rank)
    from fill_ranked
    where _fill_rank = 1

    union all

    select *
    from dated
    where trade_symbol is null
       or price is null
)

select
    account, user_id, tenant_id,
    trade_date, action_raw, action, trade_symbol, underlying_symbol,
    option_expiry, option_strike, option_type, instrument_type, description,
    quantity, price, fees, amount
from history_rows
-- CURRENCY_USD / CUSIP-shaped tickers are FX conversion noise, not trades.
-- Deposits and withdrawals ship with a NULL Symbol. ``NULL !=
-- 'CURRENCY_USD'`` is UNKNOWN in SQL, so the old predicate silently
-- dropped every cash_transfer (warehouse-wide 0 rows while the raw seed
-- held 15 IBKR Withdrawals) and made the exclude-transfers toggle a
-- no-op. Keep cash_transfer regardless of ticker; keep the FX/CUSIP
-- drop only for other rows.
where action = 'cash_transfer'
    or (
        underlying_symbol is not null
        and underlying_symbol != 'CURRENCY_USD'
        and not regexp_contains(underlying_symbol, r'^[A-Z0-9]{8}[0-9]$')
    )
