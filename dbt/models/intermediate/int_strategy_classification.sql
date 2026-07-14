/*
    Strategy classification.

    Produces one row per classified "trade group" — either an equity session
    or an option contract — tagged with a strategy label:

      - Covered Call      (sold call while holding equity)
      - Cash-Secured Put  (sold put without equity)
      - Wheel             (put assigned → equity acquired, possibly with CCs)
      - Call Spread        (bought + sold call, same expiry, different strikes)
      - Put Spread         (bought + sold put,  same expiry, different strikes)
      - Iron Condor        (a call spread AND a put spread on the same
                            underlying + expiry, legged in together)
      - Long Call          (bought call, standalone)
      - Long Put           (bought put,  standalone, no equity)
      - Protective Put     (bought put while holding equity)
      - Naked Call         (sold call without equity)
      - Poor Man Covered Call (sold call covered by long call on same underlying, e.g. diagonal)
      - Buy and Hold       (equity only, no associated options; a nominal
                            <= 1-share "price-tracker" lot held alongside
                            options is instead FOLDED into that underlying's
                            dominant option strategy — see equity_classified)
      - Crypto             (crypto holding — can't have options, so it's structurally
                            a buy-and-hold, but we surface it as its own bucket so the
                            mirror reflects asset-class choice rather than fusing BTC
                            with the trader's VOO / JEPI buckets)
*/

with equity_sessions as (
    select * from {{ ref('int_equity_sessions') }}
),

option_contracts as (
    select * from {{ ref('int_option_contracts') }}
),

-- Crypto whitelist (see stg_crypto_symbols header comment). Used to
-- route equity-session rows for BTC / ETH / USDC / etc. into the
-- 'Crypto' strategy below. Crypto can't have options so the
-- covered-call / wheel / spread branches never need to consider it.
crypto_symbols as (
    select symbol from {{ ref('stg_crypto_symbols') }}
),

-- Broker-reported instrument class per CURRENT holding. This is the ONLY
-- reliable way to disambiguate ticker collisions: ``SEI`` is both the Sei
-- token AND Solaris Energy Infrastructure (NYSE equity). The whitelist
-- above matches on ticker alone and can't tell them apart; the broker
-- can. ``stg_current`` folds ``security_type='Cryptocurrency'`` into
-- ``instrument_type='Equity'`` but preserves the raw value in
-- ``security_type_raw``. We collapse it to two flags per (tenant, account,
-- user, symbol): broker_says_crypto and broker_says_equity. A whitelist
-- match is OVERRIDDEN to non-crypto when the broker explicitly reports the
-- holding as a conventional equity/ETF (the SEI-on-Schwab case). Closed
-- positions aren't in stg_current, so they fall back to the whitelist.
broker_security_signal as (
    select
        tenant_id,
        account,
        user_id,
        upper(trim(underlying_symbol)) as symbol,
        max(case when lower(coalesce(security_type_raw, '')) = 'cryptocurrency'
                 then 1 else 0 end) as broker_says_crypto,
        max(case when lower(coalesce(security_type_raw, '')) in ('equity', 'etfs & closed end funds')
                 then 1 else 0 end) as broker_says_equity
    from {{ ref('stg_current') }}
    where instrument_type = 'Equity'
    group by 1, 2, 3, 4
),

---------------------------------------------------------------------
-- 1. For each equity session, count associated option activity
---------------------------------------------------------------------
equity_options_summary as (
    select
        e.tenant_id,
        e.account,
        e.user_id,
        e.symbol,
        e.session_id,
        count(distinct case
            when oc.direction = 'Sold' and oc.option_type = 'C'
                 and oc.open_date >= e.open_date
                 and oc.open_date <= case when e.status = 'Open' then current_date() else e.last_trade_date end
            then oc.trade_symbol
        end) as num_sold_calls,
        count(distinct case
            when oc.direction = 'Bought' and oc.option_type = 'P'
                 and oc.open_date >= e.open_date
                 and oc.open_date <= case when e.status = 'Open' then current_date() else e.last_trade_date end
            then oc.trade_symbol
        end) as num_protective_puts,
        -- ANY option contract on this underlying overlapping the session
        -- (regardless of type/direction). Powers the "price-tracker" fold:
        -- a nominal equity lot held alongside options is part of the option
        -- play, not a standalone Buy and Hold. See equity_classified.
        count(distinct case
            when oc.open_date >= e.open_date
                 and oc.open_date <= case when e.status = 'Open' then current_date() else e.last_trade_date end
            then oc.trade_symbol
        end) as num_option_contracts
    from equity_sessions e
    left join option_contracts oc
        on e.account = oc.account
        and (e.user_id is not distinct from oc.user_id)
        and (e.tenant_id is not distinct from oc.tenant_id)
        and e.symbol = oc.underlying_symbol
    group by 1, 2, 3, 4, 5
),

---------------------------------------------------------------------
-- 2. Detect put assignments that led to equity sessions (→ Wheel)
---------------------------------------------------------------------
put_assignments as (
    select
        tenant_id,
        account,
        user_id,
        underlying_symbol,
        trade_symbol,
        close_date as assignment_date
    from option_contracts
    where close_type = 'Assigned'
      and option_type = 'P'
),

equity_from_assignment as (
    select distinct
        e.tenant_id,
        e.account,
        e.user_id,
        e.symbol,
        e.session_id
    from equity_sessions e
    join put_assignments pa
        on e.account = pa.account
        and (e.user_id is not distinct from pa.user_id)
        and (e.tenant_id is not distinct from pa.tenant_id)
        and e.symbol = pa.underlying_symbol
        and abs(date_diff(e.open_date, pa.assignment_date, day)) <= 5
),

---------------------------------------------------------------------
-- 3. Detect spread pairs (bought + sold, same underlying / expiry / type)
---------------------------------------------------------------------
spread_legs as (
    -- All trade_symbols that are part of a spread.
    -- Self-join keyed on (account, user_id) so two users with the same
    -- account label and similar option positions don't get classified
    -- as spreading against each other.
    select distinct a.tenant_id, a.account, a.user_id, a.trade_symbol
    from option_contracts a
    join option_contracts b
        on a.account           = b.account
        and (a.user_id is not distinct from b.user_id)
        and (a.tenant_id is not distinct from b.tenant_id)
        and a.underlying_symbol = b.underlying_symbol
        and a.option_expiry     = b.option_expiry
        and a.option_type       = b.option_type
        and a.option_strike    != b.option_strike
        and a.direction        != b.direction
        and abs(date_diff(a.open_date, b.open_date, day)) <= 7

    union distinct

    select distinct b.tenant_id, b.account, b.user_id, b.trade_symbol
    from option_contracts a
    join option_contracts b
        on a.account           = b.account
        and (a.user_id is not distinct from b.user_id)
        and (a.tenant_id is not distinct from b.tenant_id)
        and a.underlying_symbol = b.underlying_symbol
        and a.option_expiry     = b.option_expiry
        and a.option_type       = b.option_type
        and a.option_strike    != b.option_strike
        and a.direction        != b.direction
        and abs(date_diff(a.open_date, b.open_date, day)) <= 7
),

---------------------------------------------------------------------
-- 3a. Iron Condor: a call spread AND a put spread on the SAME underlying
--    and SAME expiry, legged in together (net-credit defined-risk range
--    trade — short call above + long call further above, short put below
--    + long put further below). We detect it structurally rather than by
--    strike geometry: within one (tenant, account, user, underlying,
--    expiry) there are >= 2 call legs that are spread members AND >= 2
--    put legs that are spread members. The open-date span guard (<= 7d)
--    keeps a call spread and a put spread opened months apart on the same
--    LEAP expiry from being fused into a "condor" they were never traded
--    as. A pure call spread OR pure put spread (only one side present)
--    stays 'Call Spread' / 'Put Spread' — this branch only fires when
--    BOTH sides exist, and it takes precedence over the generic spread
--    label below so the four legs read as one strategy.
--    Note: an iron butterfly (short call & short put at the same strike)
--    also satisfies this and will read as 'Iron Condor' — acceptable; we
--    do not distinguish the wingspan today.
iron_condor_groups as (
    select
        oc.tenant_id,
        oc.account,
        oc.user_id,
        oc.underlying_symbol,
        oc.option_expiry
    from option_contracts oc
    join spread_legs sl
        on oc.account = sl.account
        and (oc.user_id is not distinct from sl.user_id)
        and (oc.tenant_id is not distinct from sl.tenant_id)
        and oc.trade_symbol = sl.trade_symbol
    group by 1, 2, 3, 4, 5
    having count(distinct case when oc.option_type = 'C' then oc.trade_symbol end) >= 2
       and count(distinct case when oc.option_type = 'P' then oc.trade_symbol end) >= 2
       and date_diff(max(oc.open_date), min(oc.open_date), day) <= 7
),

iron_condor_legs as (
    select distinct
        oc.tenant_id,
        oc.account,
        oc.user_id,
        oc.trade_symbol
    from option_contracts oc
    -- Must itself be a spread leg (skips any stray naked leg on the same
    -- underlying/expiry that isn't part of a vertical).
    join spread_legs sl
        on oc.account = sl.account
        and (oc.user_id is not distinct from sl.user_id)
        and (oc.tenant_id is not distinct from sl.tenant_id)
        and oc.trade_symbol = sl.trade_symbol
    join iron_condor_groups g
        on oc.account = g.account
        and (oc.user_id is not distinct from g.user_id)
        and (oc.tenant_id is not distinct from g.tenant_id)
        and oc.underlying_symbol = g.underlying_symbol
        and oc.option_expiry     = g.option_expiry
),

---------------------------------------------------------------------
-- 3b. Poor Man Covered Call: short legs of matched pairs from int_pmcc_pairs.
--    PMCC = long call (expiry >= 180d, deep ITM proxy), short call (expiry <= 60d),
--    short strike > long strike, short qty <= long qty, long open when short written.
---------------------------------------------------------------------
pmcc_short_calls as (
    select distinct
        tenant_id,
        account,
        user_id,
        short_trade_symbol as trade_symbol
    from {{ ref('int_pmcc_pairs') }}
),

---------------------------------------------------------------------
-- 4. Classify option contracts
---------------------------------------------------------------------
options_classified as (
    select
        oc.tenant_id,
        oc.account,
        oc.user_id,
        oc.underlying_symbol                 as symbol,
        oc.trade_symbol,
        'option_contract'                    as trade_group_type,
        oc.option_type,
        oc.option_strike,
        oc.option_expiry,
        oc.direction,
        oc.status,
        oc.open_date,
        oc.close_date,
        oc.days_in_trade,
        oc.net_cash_flow,
        oc.total_pnl,
        -- Realized vs unrealized for options:
        --   Closed contracts: all P&L is realized
        --   Open contracts:   total_pnl is mark-to-market unrealized
        case when oc.status = 'Closed' then oc.total_pnl else 0 end as realized_pnl,
        case when oc.status = 'Open'   then oc.total_pnl else 0 end as unrealized_pnl,
        oc.num_trades,
        oc.close_type,
        oc.premium_received,
        oc.premium_paid,

        -- Strategy
        case
            -- Iron Condor: this leg is part of a call spread + put spread
            -- on the same underlying/expiry legged in together. Checked
            -- BEFORE the generic spread branch so all four legs collapse
            -- to one strategy label instead of splitting Call/Put Spread.
            when ic.trade_symbol is not null then 'Iron Condor'

            -- Spread (has a matching opposite-direction leg)
            when sl.trade_symbol is not null then
                case when oc.option_type = 'C' then 'Call Spread' else 'Put Spread' end

            -- Sold call with underlying equity (>= 100 shares) → Covered Call
            when oc.direction = 'Sold' and oc.option_type = 'C' and e.session_id is not null
                 and e.max_quantity_held >= 100
                then 'Covered Call'

            -- Sold call covered by long call (same underlying) → Poor Man Covered Call
            when oc.direction = 'Sold' and oc.option_type = 'C' and pmcc.trade_symbol is not null
                then 'Poor Man Covered Call'

            -- Sold call without equity or long cover → Naked Call
            when oc.direction = 'Sold' and oc.option_type = 'C'
                then 'Naked Call'

            -- Sold put → Cash-Secured Put
            when oc.direction = 'Sold' and oc.option_type = 'P'
                then 'Cash-Secured Put'

            -- Bought call → Long Call
            when oc.direction = 'Bought' and oc.option_type = 'C'
                then 'Long Call'

            -- Bought put with equity (>= 100 shares) → Protective Put
            when oc.direction = 'Bought' and oc.option_type = 'P' and e.session_id is not null
                 and e.max_quantity_held >= 100
                then 'Protective Put'

            -- Bought put standalone → Long Put
            when oc.direction = 'Bought' and oc.option_type = 'P'
                then 'Long Put'

            else 'Other Option'
        end as strategy,

        case when oc.total_pnl > 0 then true else false end as is_winner

    from option_contracts oc
    -- Check for spread membership
    left join spread_legs sl
        on oc.account = sl.account
        and (oc.user_id is not distinct from sl.user_id)
        and (oc.tenant_id is not distinct from sl.tenant_id)
        and oc.trade_symbol = sl.trade_symbol
    -- Check for iron-condor membership (call spread + put spread together)
    left join iron_condor_legs ic
        on oc.account = ic.account
        and (oc.user_id is not distinct from ic.user_id)
        and (oc.tenant_id is not distinct from ic.tenant_id)
        and oc.trade_symbol = ic.trade_symbol
    -- Check for PMCC (short call covered by long call on same underlying)
    left join pmcc_short_calls pmcc
        on oc.account = pmcc.account
        and (oc.user_id is not distinct from pmcc.user_id)
        and (oc.tenant_id is not distinct from pmcc.tenant_id)
        and oc.trade_symbol = pmcc.trade_symbol
    -- Check for overlapping equity session (Covered Call / Protective Put detection).
    --
    -- Defensive dedup: this join used to match raw `equity_sessions` directly,
    -- and a duplicated `int_equity_sessions` row (from a poisoned source — see
    -- 2026-05-11 entry in ~/.cursor/skills/broker-sync-safety/SKILL.md) would
    -- fan a single option contract into multiple classification rows. The same
    -- option fill then showed up as both "Naked Call" and "Covered Call" under
    -- the same trade_symbol on the position page, depending on which join
    -- branch fired. Wrapping the join in a `qualify row_number() ... = 1`
    -- subquery picks exactly one session per (account, user_id, trade_symbol):
    -- prefer Open over Closed (Open is the "live coverage" reading), then the
    -- session with the most shares held (covered-call eligibility threshold),
    -- then the one with the latest open_date (most recently established).
    -- Net: even if upstream `int_equity_sessions` ever holds duplicates, this
    -- mart's classification row count stays exactly one per option contract.
    left join (
        select
            oc2.tenant_id,
            oc2.account,
            oc2.user_id,
            oc2.trade_symbol,
            e.session_id,
            e.status,
            e.max_quantity_held
        from option_contracts oc2
        join equity_sessions e
            on oc2.account = e.account
            and (oc2.user_id is not distinct from e.user_id)
            and (oc2.tenant_id is not distinct from e.tenant_id)
            and oc2.underlying_symbol = e.symbol
            and oc2.open_date >= e.open_date
            and oc2.open_date <= case
                when e.status = 'Open' then current_date()
                else e.last_trade_date
            end
        qualify row_number() over (
            partition by oc2.tenant_id, oc2.account, oc2.user_id, oc2.trade_symbol
            order by case when e.status = 'Open' then 0 else 1 end,
                     e.max_quantity_held desc nulls last,
                     e.open_date desc,
                     e.session_id desc
        ) = 1
    ) e
        on oc.account = e.account
        and (oc.user_id is not distinct from e.user_id)
        and (oc.tenant_id is not distinct from e.tenant_id)
        and oc.trade_symbol = e.trade_symbol
),

-- Dominant option strategy per (tenant, account, user, underlying). Used to
-- FOLD a nominal "price-tracker" equity lot into the option play it belongs
-- to (see equity_classified). Picks the option strategy with the largest
-- absolute P&L on that underlying, tie-broken by most-recent open.
dominant_option_strategy as (
    select tenant_id, account, user_id, symbol, strategy as dominant_strategy
    from (
        select
            tenant_id, account, user_id, symbol, strategy,
            row_number() over (
                partition by tenant_id, account, user_id, symbol
                order by abs(coalesce(total_pnl, 0)) desc, open_date desc
            ) as rn
        from options_classified
    )
    where rn = 1
),

---------------------------------------------------------------------
-- 5. Classify equity sessions
---------------------------------------------------------------------
-- Realized P&L by session, summed from int_closed_equity_legs.
-- Captures the realized portion of an Open session that has had interim sells
-- (e.g. JEPI: bought 2000 shares, sold 1000, holding 1000 → realized $2,681,
-- unrealized = total_pnl − realized).
session_realized as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        session_id,
        sum(realized_pnl) as realized_pnl
    from {{ ref('int_closed_equity_legs') }}
    group by 1, 2, 3, 4, 5
),

equity_classified as (
    select
        e.tenant_id,
        e.account,
        e.user_id,
        e.symbol,
        concat(e.symbol, '_session_', cast(e.session_id as string)) as trade_symbol,
        'equity_session'                       as trade_group_type,
        cast(null as string)                   as option_type,
        cast(null as float64)                  as option_strike,
        cast(null as date)                     as option_expiry,
        cast(null as string)                   as direction,
        e.status,
        e.open_date,
        e.last_trade_date                      as close_date,
        e.days_held                            as days_in_trade,
        e.net_cash_flow,
        e.total_pnl,
        -- Realized vs unrealized for equity sessions:
        --   Closed session: every share has been sold → all P&L is realized
        --   Open session:   realized = sum of int_closed_equity_legs for any
        --                   interim sells; unrealized = total_pnl − realized
        case
            when e.status = 'Closed' then e.total_pnl
            else coalesce(sr.realized_pnl, 0)
        end as realized_pnl,
        case
            when e.status = 'Closed' then 0
            else e.total_pnl - coalesce(sr.realized_pnl, 0)
        end as unrealized_pnl,
        e.num_trades,
        cast(null as string)                   as close_type,
        cast(0 as float64)                     as premium_received,
        cast(0 as float64)                     as premium_paid,

        case
            -- Crypto wins first: BTC / ETH / USDC etc. land here from a
            -- broker (Coinbase via SnapTrade today) where options aren't
            -- a thing. We surface them as their own bucket so dashboards
            -- don't conflate the trader's BTC sit-and-hold with their
            -- VOO sit-and-hold — different asset class, different mental
            -- model, different tax treatment.
            --
            -- Broker-corroborated (2026-07-14): a whitelist match alone is
            -- NOT enough because crypto tickers collide with equities
            -- (SEI = Sei token vs Solaris Energy). We label Crypto when the
            -- broker itself reports the holding as crypto, OR when the
            -- ticker is whitelisted AND the broker does NOT report it as a
            -- conventional equity (covers closed crypto with no current
            -- snapshot). An equity the broker calls a stock/ETF is never
            -- Crypto even if its ticker collides. See broker_security_signal.
            when coalesce(bss.broker_says_crypto, 0) = 1
                 or (cs.symbol is not null and coalesce(bss.broker_says_equity, 0) = 0)
                then 'Crypto'
            when efa.session_id is not null and eos.num_sold_calls > 0
                then 'Wheel'
            when efa.session_id is not null
                then 'Wheel'
            when eos.num_sold_calls > 0 and e.max_quantity_held >= 100
                then 'Covered Call'

            -- Price-tracker fold (2026-07-14): a nominal equity lot (<= 1
            -- share) held alongside options on the same underlying is a
            -- "so I can watch the ticker" position, NOT a standalone Buy
            -- and Hold. Surfacing it as its own strategy row made a pure
            -- long-call position read as "mixed" and cluttered the Strategy
            -- Breakdown. Fold it into the dominant option strategy on that
            -- underlying so the symbol reads as the option play; the tiny
            -- equity P&L still shows in Breakdown-by-Type's Equity row. The
            -- <= 1 share threshold is deliberately tight to avoid
            -- reclassifying a genuine small holding, and the "has options"
            -- guard keeps standalone tiny lots as Buy and Hold.
            when coalesce(e.max_quantity_held, 0) <= 1
                 and coalesce(eos.num_option_contracts, 0) > 0
                then coalesce(dos.dominant_strategy, 'Long Call')

            else 'Buy and Hold'
        end as strategy,

        case when e.total_pnl > 0 then true else false end as is_winner

    from equity_sessions e
    left join equity_options_summary eos
        on e.account = eos.account
        and (e.user_id is not distinct from eos.user_id)
        and (e.tenant_id is not distinct from eos.tenant_id)
        and e.symbol = eos.symbol
        and e.session_id = eos.session_id
    left join equity_from_assignment efa
        on e.account = efa.account
        and (e.user_id is not distinct from efa.user_id)
        and (e.tenant_id is not distinct from efa.tenant_id)
        and e.symbol = efa.symbol
        and e.session_id = efa.session_id
    left join session_realized sr
        on e.account = sr.account
        and (e.user_id is not distinct from sr.user_id)
        and (e.tenant_id is not distinct from sr.tenant_id)
        and e.symbol = sr.symbol
        and e.session_id = sr.session_id
    left join crypto_symbols cs
        on upper(trim(e.symbol)) = cs.symbol
    left join broker_security_signal bss
        on e.account = bss.account
        and (e.user_id is not distinct from bss.user_id)
        and (e.tenant_id is not distinct from bss.tenant_id)
        and upper(trim(e.symbol)) = bss.symbol
    left join dominant_option_strategy dos
        on e.account = dos.account
        and (e.user_id is not distinct from dos.user_id)
        and (e.tenant_id is not distinct from dos.tenant_id)
        and e.symbol = dos.symbol
),

---------------------------------------------------------------------
-- 6. Union all classified trade groups
---------------------------------------------------------------------
classified as (
    select * from options_classified
    union all
    select * from equity_classified
)

---------------------------------------------------------------------
-- 7. v2 tenant_id is carried natively from staging through both the
-- equity-session and option-contract grains (each classified CTE
-- selects tenant_id as its first column), so no dim_broker_tenants
-- join is needed. The prior left join on (account_name, user_id)
-- fanned out when one (account_name, user_id) mapped to multiple
-- tenant_ids (e.g. several Schwab accounts sharing a display label).
---------------------------------------------------------------------
select * from classified
