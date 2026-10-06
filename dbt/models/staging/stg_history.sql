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
-- renders through the exact same tenant scoping as a real user.
-- stg_demo_history is always that bot mirror. While demo_temp_seed_on()
-- the temporary book in dbt/seeds/demo_temp_history.csv is unioned on
-- top (see dbt/seeds/DEMO_TEMP_SEED.md). Flag off is the mirror only.
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

-- An estimated order fee copied onto a cash settlement (description
-- still contains "est. fee") nets 10 × 16.45 × 100 = 16,450 into
-- 16,462.22. The next rebuild puts the statement cash back and zeros
-- the fee. Same-day 0DTE estimates (expiry = trade date, no "as of")
-- stay. A later statement row has no mark, so it still wins the
-- fill rank below.
fee_guard as (
    select
        c.*,
        (
            regexp_contains(lower(coalesce(c.description, '')), r'est\. fee')
            and (
                c.action in (
                    'option_expired', 'option_assigned', 'option_exercised'
                )
                or regexp_contains(
                    lower(coalesce(c.description, '')),
                    r'\bas of\b|cash settlement'
                )
                or (
                    c.option_expiry is not null
                    and c.trade_date is not null
                    and c.option_expiry < c.trade_date
                )
            )
        ) as drop_estimated_fee
    from cleaned c
),

amount_signed as (
    select
        g.* except (amount_raw, fees, drop_estimated_fee),
        case
            when g.drop_estimated_fee then 0
            else g.fees
        end as fees,
        -- 1 when this row's Amount was still the gross premium and we
        -- netted ``fees`` here. The broker statement is already net, so
        -- it stays 0. After the expiry-date cap below, an order row and
        -- its later activity can share a fill key; prefer the statement.
        -- A dropped settlement estimate is 0 so the statement cash wins.
        case
            when g.drop_estimated_fee then false
            else (
            g.action in (
                'option_buy_to_open', 'option_buy_to_close',
                'option_sell_to_open', 'option_sell_to_close'
            )
            and abs(coalesce(g.fees, 0)) > 0.005
            and g.quantity is not null
            and g.price is not null
            and abs(
                   abs(g.amount_raw) - abs(g.quantity) * g.price * 100
                ) <= 0.05
            )
        end as fee_adjusted,
        case
            -- Copied estimate on a settlement: 16,450 became 16,462.22.
            -- Put qty × price × 100 back. A zero-amount expiry stays 0.
            when g.drop_estimated_fee
             and abs(coalesce(g.amount_raw, 0)) <= 0.005
            then g.amount_raw
            when g.drop_estimated_fee
             and g.quantity is not null
             and g.price is not null
             and abs(g.price) > 0
            then case
                when g.amount_raw < 0
                    then -(abs(g.quantity) * abs(g.price) * 100)
                else abs(g.quantity) * abs(g.price) * 100
            end

            -- Option order rows store gross premium (qty × price × 100)
            -- and the commission in ``fees``. The broker statement Amount
            -- is already net (Sep SPXW: 5 × 2.87 × 100 − 6.11 = 1,428.89).
            -- Subtract the fee only when the raw amount still matches the
            -- gross, so a net activity amount is not charged twice.
            when g.action in (
                'option_buy_to_open', 'option_buy_to_close',
                'option_sell_to_open', 'option_sell_to_close'
            )
             and abs(coalesce(g.fees, 0)) > 0.005
             and g.quantity is not null
             and g.price is not null
             and abs(
                    abs(g.amount_raw) - abs(g.quantity) * g.price * 100
                 ) <= 0.05
            then case
                when g.action in ('option_sell_to_open', 'option_sell_to_close')
                    then abs(g.quantity) * g.price * 100 - abs(g.fees)
                else -(abs(g.quantity) * g.price * 100 + abs(g.fees))
            end

            when g.action in (
                'equity_buy',
                'option_buy_to_open',
                'option_buy_to_close',
                'margin_interest',
                'adr_fee'
            ) then -abs(g.amount_raw)

            when g.action in (
                'equity_sell',
                'equity_sell_short',
                'option_sell_to_open',
                'option_sell_to_close',
                'dividend',
                'credit_interest'
            ) then abs(g.amount_raw)

            else g.amount_raw
        end as amount
    from fee_guard g
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

-- An estimated-fee order fill and the later statement for the same
-- close do not share price or date. ASTS 58C: the order is 8 × $0.45,
-- −$366.33, description contains "est. fee"; the statement is the
-- same buy-to-close of 8, a different price, often the next day.
-- Quantity, action, and tenant already match — those are not why the
-- pair survives. fill_ranked keys price@4dp, the raw symbol text, and
-- the exact trade_date, so both rows stay and the close quantity
-- doubles (8 opened, 16 closed, about −$529.93 instead of −$163.60).
--
-- Identity (contract, action, quantity, dates within one day) is not
-- enough. Close 8, reopen, close 8 the next day at a different premium
-- shares it, and dropping the earlier order deletes a real trade.
-- Also require either:
--   * the statement amount, or its price × qty × 100, is within fee
--     room (greatest($2, $1.50 × contracts)) of the estimate gross, or
--   * the other row is a statement and this row is the only order fill
--     (no later estimated-fee order at a different premium).
-- The statement amount wins. A lone estimate stays. A fill a week
-- later is a different trade. ``_drop_estimated_fee_shadows`` in
-- app/upload.py is the same rule for the next sync.
--
-- Joins, not EXISTS. Run 37223667300 failed stg_history with
-- "Correlated subqueries that reference other tables are not supported
-- unless they can be de-correlated": the real-fill EXISTS contained a
-- NOT EXISTS against ``dated`` again. BigQuery will not decorrelate
-- that nesting. A self-join plus a precomputed later-order set is the
-- same predicate.

-- An estimated-fee order dated after this one, same contract / action /
-- quantity, whose premium is outside fee room of this order's gross.
-- Same premium on a later day is a repost, not a second close.
later_separate_orders as (
    select distinct
        d.tenant_id,
        d.trade_date,
        d.action,
        d.underlying_symbol,
        d.option_expiry,
        d.option_strike,
        d.option_type,
        d.quantity,
        d.price
    from dated d
    inner join dated later
        on later.tenant_id = d.tenant_id
       and later.underlying_symbol = d.underlying_symbol
       and later.option_expiry = d.option_expiry
       and later.option_strike is not null
       and abs(later.option_strike - d.option_strike) < 0.001
       and later.option_type = d.option_type
       and later.action = d.action
       and later.quantity is not null
       and abs(abs(later.quantity) - abs(d.quantity)) < 0.0001
       and later.trade_date is not null
       and later.trade_date > d.trade_date
       and later.price is not null
       and regexp_contains(lower(coalesce(later.description, '')), r'est\. fee')
       and abs(
            abs(later.price) * abs(later.quantity) * 100
            - abs(d.price) * abs(d.quantity) * 100
          ) > greatest(2.0, 1.5 * abs(d.quantity))
    where regexp_contains(lower(coalesce(d.description, '')), r'est\. fee')
      and d.tenant_id is not null
      and d.underlying_symbol is not null
      and d.option_expiry is not null
      and d.option_strike is not null
      and d.option_type is not null
      and d.quantity is not null
      and d.trade_date is not null
      and d.price is not null
),

-- Estimates that have a statement fill within one day, and either the
-- dollars match or there is no later separate order.
est_fee_matched as (
    select distinct
        d.tenant_id,
        d.trade_date,
        d.action,
        d.underlying_symbol,
        d.option_expiry,
        d.option_strike,
        d.option_type,
        d.quantity,
        d.price,
        d.description
    from dated d
    inner join dated r
        on r.tenant_id = d.tenant_id
       and not regexp_contains(lower(coalesce(r.description, '')), r'est\. fee')
       and r.underlying_symbol = d.underlying_symbol
       and r.option_expiry = d.option_expiry
       and r.option_strike is not null
       and abs(r.option_strike - d.option_strike) < 0.001
       and r.option_type = d.option_type
       and r.action = d.action
       and r.quantity is not null
       and abs(abs(r.quantity) - abs(d.quantity)) < 0.0001
       and r.trade_date is not null
       and abs(date_diff(r.trade_date, d.trade_date, day)) <= 1
    left join later_separate_orders ls
        on ls.tenant_id = d.tenant_id
       and ls.trade_date is not distinct from d.trade_date
       and ls.action is not distinct from d.action
       and ls.underlying_symbol = d.underlying_symbol
       and ls.option_expiry is not distinct from d.option_expiry
       and ls.option_strike is not distinct from d.option_strike
       and ls.option_type is not distinct from d.option_type
       and ls.quantity is not distinct from d.quantity
       and ls.price is not distinct from d.price
    where regexp_contains(lower(coalesce(d.description, '')), r'est\. fee')
      and d.tenant_id is not null
      and d.underlying_symbol is not null
      and d.option_expiry is not null
      and d.option_strike is not null
      and d.option_type is not null
      and d.quantity is not null
      and d.trade_date is not null
      and (
          (
              d.price is not null
              and abs(d.price) > 0
              and (
                  abs(
                      abs(coalesce(r.amount, 0))
                      - abs(d.price) * abs(d.quantity) * 100
                  ) <= greatest(2.0, 1.5 * abs(d.quantity))
                  or (
                      r.price is not null
                      and abs(
                          abs(r.price) * abs(r.quantity) * 100
                          - abs(d.price) * abs(d.quantity) * 100
                      ) <= greatest(2.0, 1.5 * abs(d.quantity))
                  )
              )
          )
          or ls.tenant_id is null
      )
),

est_fee_shadow as (
    select
        d.*,
        m.tenant_id is not null as drop_est_fee_shadow
    from dated d
    left join est_fee_matched m
        on m.tenant_id = d.tenant_id
       and m.trade_date is not distinct from d.trade_date
       and m.action is not distinct from d.action
       and m.underlying_symbol is not distinct from d.underlying_symbol
       and m.option_expiry is not distinct from d.option_expiry
       and m.option_strike is not distinct from d.option_strike
       and m.option_type is not distinct from d.option_type
       and m.quantity is not distinct from d.quantity
       and m.price is not distinct from d.price
       and m.description is not distinct from d.description
),

-- Capping a posting date at expiry (or reading "as of") can land an
-- order fill and its later activity on the same check-2 grain:
-- (tenant, trade_date, action, trade_symbol, quantity, price@4dp).
-- Run 37090194526: BE 03/06/26 165C buy-to-close, 10 @ 0.042, amounts
-- -42.92 and -42.12. The raw dates still differ, so upload dedup keeps
-- both (rewriting the seed date is what inserted the twin). Collapse
-- here, after the date rewrite. Blank-price rows stay put — distinct
-- expiries share an empty Symbol and must not fuse. Estimated-fee
-- shadows (different price or a one-day posting lag) are already
-- flagged and left out of this rank.
fill_ranked as (
    select
        d.* except (drop_est_fee_shadow),
        row_number() over (
            -- BigQuery rejects FLOAT64 in a window PARTITION BY
            -- (run 37091190523: "Partitioning by expressions of type
            -- FLOAT64 is not allowed"). Cast the check-2 grain.
            partition by
                d.tenant_id,
                d.trade_date,
                d.action,
                d.trade_symbol,
                cast(d.quantity as string),
                cast(round(d.price, 4) as string)
            order by
                d.fee_adjusted asc,
                length(coalesce(d.description, '')) desc,
                abs(d.amount) asc,
                d.amount asc
        ) as _fill_rank
    from est_fee_shadow d
    where d.trade_symbol is not null
      and d.price is not null
      and not d.drop_est_fee_shadow
),

history_rows as (
    select * except (_fill_rank)
    from fill_ranked
    where _fill_rank = 1

    union all

    select * except (drop_est_fee_shadow)
    from est_fee_shadow
    where (trade_symbol is null or price is null)
      and not drop_est_fee_shadow
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
