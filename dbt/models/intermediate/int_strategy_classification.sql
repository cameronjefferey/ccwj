/*
    Strategy classification.

    Produces one row per classified "trade group" — either an equity session
    or an option contract — tagged with a strategy label:

      - Covered Call      (sold call FULLY covered by shares held when the
                           call was written — contracts × 100 vs shares
                           as-of the write date, with a 3-day buy-write
                           lookahead; see coverage_at_write)
      - Partially Covered Call (>= 100 shares at write but fewer than
                           contracts × 100 — some contracts are naked)
      - Cash-Secured Put  (sold put without equity)
      - Wheel             (put assigned → equity acquired, possibly with CCs)
      - Call Spread        (bought + sold call, same expiry, different strikes,
                            legged in within 7 days OR alive simultaneously)
      - Put Spread         (bought + sold put,  same treatment)
      - Iron Condor        (a call spread AND a put spread on the same
                            underlying + expiry, legged in together)
      - Diagonal Call Spread (sold call covered by a live longer-dated long
                            call that doesn't meet the PMCC windows)
      - Diagonal Put Spread  (sold put covered by a live longer-dated long put)
      - Straddle / Strangle  (call + put, same underlying/expiry/direction,
                            opened within 3 days; same strike = Straddle)
      - Long Call          (bought call, standalone)
      - Long Put           (bought put,  standalone, no equity)
      - Protective Put     (bought put while holding >= 100 shares at write)
      - Naked Call         (sold call without any coverage at write)
      - Poor Man Covered Call (sold call covered by long call matching the
                            int_pmcc_pairs windows — long >= 180d, short <= 60d)
      - Buy and Hold       (equity only, no associated options; a nominal
                            < 25-share "price-tracker" lot held alongside
                            options is instead FOLDED into that underlying's
                            dominant option strategy — see equity_classified)
      - Crypto             (crypto holding — can't have options, so it's structurally
                            a buy-and-hold, but we surface it as its own bucket so the
                            mirror reflects asset-class choice rather than fusing BTC
                            with the trader's VOO / JEPI buckets)

    Aug 2026 classification audit: coverage is judged AS OF THE WRITE DATE
    from the split-adjusted running share count (int_equity_fills, which
    includes synthesized opening balances for pre-window holdings), not from
    the session's lifetime max_quantity_held — a trader who once held 500
    shares but had sold them before writing a call was previously labeled
    Covered when the call was in fact naked.
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
        end) as num_option_contracts,
        -- Days of the session during which at least one sold call was live
        -- (summed per contract, so overlapping calls can exceed the session
        -- length — fine for a ratio threshold). Powers the >= 30% coverage
        -- requirement in equity_classified: a 2-year holding that carried a
        -- covered call for three weeks is a Buy and Hold whose equity P&L
        -- must not be attributed to 'Covered Call' (Aug 2026 audit F5:
        -- -$77K of equity P&L was mis-bucketed this way).
        sum(case
            when oc.direction = 'Sold' and oc.option_type = 'C'
                 and oc.open_date <= case when e.status = 'Open' then current_date() else e.last_trade_date end
                 and coalesce(oc.close_date, current_date()) >= e.open_date
            then date_diff(
                     least(coalesce(oc.close_date, current_date()),
                           case when e.status = 'Open' then current_date() else e.last_trade_date end),
                     greatest(oc.open_date, e.open_date),
                     day) + 1
            else 0
        end) as sold_call_covered_days
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
    --
    -- Pairing rule (widened Aug 2026 audit F6): legs pair when opened
    -- within 7 days of each other (the original rule — catches quick
    -- legging even if the first leg closed before the second opened) OR
    -- when their lifetimes OVERLAP (both alive at the same moment — a
    -- vertical is a vertical no matter how far apart the legs were
    -- legged in, as long as they actually coexisted). Pre-fix, opposite
    -- legs opened 8-45 days apart read as independent Naked Call +
    -- Long Call.
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
        and (
            abs(date_diff(a.open_date, b.open_date, day)) <= 7
            or (a.open_date <= coalesce(b.close_date, current_date())
                and b.open_date <= coalesce(a.close_date, current_date()))
        )

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
        and (
            abs(date_diff(a.open_date, b.open_date, day)) <= 7
            or (a.open_date <= coalesce(b.close_date, current_date())
                and b.open_date <= coalesce(a.close_date, current_date()))
        )
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
-- 3c. Coverage AS OF WRITE TIME (Aug 2026 audit F2).
--
--     For each option contract: how many shares did this account hold on
--     the day the contract was opened? Computed from int_equity_fills
--     (split-adjusted, today's share-units, INCLUDING synthesized opening
--     balances for pre-window holdings). A 3-day lookahead covers the
--     buy-write pattern (call sold first, shares land 1-3 days later —
--     4 real cases at audit time); greatest() means a sale within the
--     lookahead window can never REDUCE the at-write count.
--
--     required_shares converts contracts × 100 into today's share-units
--     via the split factor at the open date (an option's deliverable is
--     100 shares in THAT DAY's units), so the comparison is unit-safe
--     across splits. Snapshot-only contracts report 0 opened contracts —
--     floor at 1 so they still demand at least one contract of coverage.
---------------------------------------------------------------------
coverage_at_write as (
    select
        oc.tenant_id,
        oc.account,
        oc.user_id,
        oc.trade_symbol,
        greatest(
            sum(case when f.trade_date <= oc.open_date
                     then f.signed_quantity else 0 end),
            sum(case when f.trade_date <= date_add(oc.open_date, interval 3 day)
                     then f.signed_quantity else 0 end)
        ) as coverage_qty,
        any_value(100.0 * coalesce(sf.cumulative_split_factor, 1.0))
            as shares_per_contract,
        any_value(
            greatest(
                coalesce(oc.contracts_sold_to_open, 0),
                coalesce(oc.contracts_bought_to_open, 0),
                1.0
            ) * 100.0 * coalesce(sf.cumulative_split_factor, 1.0)
        ) as required_shares
    from option_contracts oc
    join {{ ref('int_equity_fills') }} f
        on  f.account = oc.account
        and (f.user_id is not distinct from oc.user_id)
        and (f.tenant_id is not distinct from oc.tenant_id)
        and f.symbol = oc.underlying_symbol
    left join {{ ref('int_split_factors') }} sf
        on  sf.symbol     = oc.underlying_symbol
        and sf.trade_date = oc.open_date
    group by 1, 2, 3, 4
),

---------------------------------------------------------------------
-- 3d. Diagonal spreads (Aug 2026 audit F4): a sold option covered by a
--     LIVE longer-dated long option of the same type on the same
--     underlying. The long must be open when the short is written and
--     expire after it. This is the PMCC structure without PMCC's strict
--     windows (long >= 180d, short <= 60d) — pre-fix these shorts fell
--     through to 'Naked Call' even though a long covered them.
---------------------------------------------------------------------
diagonal_cover as (
    select distinct
        s.tenant_id,
        s.account,
        s.user_id,
        s.trade_symbol
    from option_contracts s
    join option_contracts l
        on  s.account = l.account
        and (s.user_id is not distinct from l.user_id)
        and (s.tenant_id is not distinct from l.tenant_id)
        and s.underlying_symbol = l.underlying_symbol
        and s.option_type       = l.option_type
        and s.direction = 'Sold'
        and l.direction = 'Bought'
        and l.open_date <= s.open_date
        and coalesce(l.close_date, current_date()) >= s.open_date
        and l.option_expiry > s.option_expiry
),

---------------------------------------------------------------------
-- 3e. Straddles / strangles (Aug 2026 audit F6): a call and a put on the
--     same underlying + expiry, SAME direction, opened within 3 days.
--     Same strike = Straddle, different strikes = Strangle. When one leg
--     pairs with several candidates, prefer the Straddle reading.
--     Equity coverage takes precedence in the CASE below: a sold call
--     that is covered stays 'Covered Call' (covered-strangle structures
--     read as Covered Call + Cash-Secured Put, matching how income
--     traders think about them).
---------------------------------------------------------------------
straddle_legs as (
    select tenant_id, account, user_id, trade_symbol, pair_label
    from (
        select
            a.tenant_id,
            a.account,
            a.user_id,
            a.trade_symbol,
            case when a.option_strike = b.option_strike
                 then 'Straddle' else 'Strangle' end as pair_label,
            row_number() over (
                partition by a.tenant_id, a.account, a.user_id, a.trade_symbol
                order by case when a.option_strike = b.option_strike then 0 else 1 end
            ) as rn
        from option_contracts a
        join option_contracts b
            on  a.account = b.account
            and (a.user_id is not distinct from b.user_id)
            and (a.tenant_id is not distinct from b.tenant_id)
            and a.underlying_symbol = b.underlying_symbol
            and a.option_expiry     = b.option_expiry
            and a.direction         = b.direction
            and a.option_type      != b.option_type
            and abs(date_diff(a.open_date, b.open_date, day)) <= 3
    )
    where rn = 1
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
        -- Realized vs unrealized for options — driven by the contract's own
        -- realized_pnl so a PARTIAL close splits correctly (closed portion
        -- realized, open remainder unrealized). For a fully-closed contract
        -- realized_pnl == total_pnl (unrealized 0); for a fully-open one
        -- realized_pnl == 0 (all mark-to-market unrealized).
        coalesce(oc.realized_pnl, 0)                          as realized_pnl,
        oc.total_pnl - coalesce(oc.realized_pnl, 0)           as unrealized_pnl,
        oc.num_trades,
        oc.close_type,
        oc.premium_received,
        oc.premium_paid,

        -- Strategy. Coverage branches use coverage_at_write (shares held
        -- as of the write date, quantity-aware) — NOT the session-lifetime
        -- max_quantity_held, which labeled calls Covered when the shares
        -- had already been sold (Aug 2026 audit F2).
        case
            -- Iron Condor: this leg is part of a call spread + put spread
            -- on the same underlying/expiry legged in together. Checked
            -- BEFORE the generic spread branch so all four legs collapse
            -- to one strategy label instead of splitting Call/Put Spread.
            when ic.trade_symbol is not null then 'Iron Condor'

            -- Spread (has a matching opposite-direction leg)
            when sl.trade_symbol is not null then
                case when oc.option_type = 'C' then 'Call Spread' else 'Put Spread' end

            -- Sold call fully covered at write (contracts × 100 vs shares
            -- held on the write date, 3-day buy-write lookahead)
            when oc.direction = 'Sold' and oc.option_type = 'C'
                 and coalesce(cov.coverage_qty, 0) + 1e-6
                     >= coalesce(cov.required_shares, 100)
                then 'Covered Call'

            -- At least one contract's worth of shares, but not all
            -- contracts covered → some are naked. Surfaced as its own
            -- label so the trader can see the mixed exposure.
            when oc.direction = 'Sold' and oc.option_type = 'C'
                 and coalesce(cov.coverage_qty, 0) + 1e-6
                     >= coalesce(cov.shares_per_contract, 100)
                then 'Partially Covered Call'

            -- Sold call covered by long call matching PMCC windows
            when oc.direction = 'Sold' and oc.option_type = 'C' and pmcc.trade_symbol is not null
                then 'Poor Man Covered Call'

            -- Sold option covered by a live longer-dated long of the same
            -- type (diagonal / calendar structure outside PMCC windows)
            when oc.direction = 'Sold' and dg.trade_symbol is not null
                then case when oc.option_type = 'C'
                          then 'Diagonal Call Spread'
                          else 'Diagonal Put Spread' end

            -- Call + put pair, same expiry + direction, legged in within
            -- 3 days, with no equity coverage claiming the leg above.
            when stl.trade_symbol is not null
                 and (oc.direction = 'Bought'
                      or coalesce(cov.coverage_qty, 0)
                         < coalesce(cov.shares_per_contract, 100))
                then stl.pair_label

            -- Sold call without any coverage at write → Naked Call
            when oc.direction = 'Sold' and oc.option_type = 'C'
                then 'Naked Call'

            -- Sold put → Cash-Secured Put
            when oc.direction = 'Sold' and oc.option_type = 'P'
                then 'Cash-Secured Put'

            -- Bought call → Long Call
            when oc.direction = 'Bought' and oc.option_type = 'C'
                then 'Long Call'

            -- Bought put while holding >= 100 shares at write → Protective Put
            when oc.direction = 'Bought' and oc.option_type = 'P'
                 and coalesce(cov.coverage_qty, 0) + 1e-6
                     >= coalesce(cov.shares_per_contract, 100)
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
    -- Shares held as of the write date (Covered / Partially Covered /
    -- Protective Put detection). One row per contract by construction
    -- (GROUP BY trade_symbol in coverage_at_write), so — unlike the old
    -- session-overlap join this replaced — it cannot fan a contract into
    -- multiple classification rows even if int_equity_sessions ever holds
    -- duplicates (the 2026-05-11 poisoned-source incident).
    left join coverage_at_write cov
        on oc.account = cov.account
        and (oc.user_id is not distinct from cov.user_id)
        and (oc.tenant_id is not distinct from cov.tenant_id)
        and oc.trade_symbol = cov.trade_symbol
    -- Live longer-dated long cover of the same type (diagonal)
    left join diagonal_cover dg
        on oc.account = dg.account
        and (oc.user_id is not distinct from dg.user_id)
        and (oc.tenant_id is not distinct from dg.tenant_id)
        and oc.trade_symbol = dg.trade_symbol
    -- Straddle / strangle pair membership
    left join straddle_legs stl
        on oc.account = stl.account
        and (oc.user_id is not distinct from stl.user_id)
        and (oc.tenant_id is not distinct from stl.tenant_id)
        and oc.trade_symbol = stl.trade_symbol
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
        -- Closed-session total/realized come from int_closed_equity_legs
        -- (sells + residual writeoffs that model actually emits).
        -- e.total_pnl uses a different transfer/writeoff ladder that
        -- treats leftover cost on a superseded session as realized
        -- while the legs suppress that writeoff when the symbol is
        -- still held on the account (a later snapshot / opening-balance
        -- session owns those shares). CHECK 2 failed on DXCM / PL /
        -- ARCC / NVO / PEAK for 12 days (run 33301622998) because the
        -- positions list summed classification.realized (= e.total_pnl)
        -- while Position Detail summed the legs. Align on the legs
        -- grain — that's what Breakdown by Type renders.
        -- Open sessions keep e.total_pnl (mark-to-market) and take
        -- realized from the same legs sum (interim sells).
        case
            when e.status = 'Closed' then coalesce(sr.realized_pnl, 0)
            else e.total_pnl
        end as total_pnl,
        coalesce(sr.realized_pnl, 0) as realized_pnl,
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
            --
            -- Closed-collision guard (2026-08-04): the whitelist-only fallback
            -- also fires for a whitelisted-ticker equity ONCE IT CLOSES — a
            -- closed session has no stg_current row, so broker_says_equity
            -- flips to 0 and a genuine equity (Solaris Energy) gets mislabeled
            -- Crypto. But crypto has NO listed options, so a lot with any
            -- overlapping option contract on the same (tenant, account) is
            -- definitionally the equity, not the token. Yield the whitelist
            -- fallback to the option-overlap signal (num_option_contracts > 0)
            -- so the price-tracker fold below can claim it. Broker-EXPLICIT
            -- crypto (broker_says_crypto = 1) stays authoritative regardless.
            -- Real case: SEI, user 9, 1 tracking share + long call, closed
            -- 2026-07-30 → read as 'Crypto' instead of folding into Long Call
            -- (regression test tracker_lot_folds_into_option_strategy).
            when coalesce(bss.broker_says_crypto, 0) = 1
                 or (cs.symbol is not null
                     and coalesce(bss.broker_says_equity, 0) = 0
                     and coalesce(eos.num_option_contracts, 0) = 0)
                then 'Crypto'
            when efa.session_id is not null and eos.num_sold_calls > 0
                then 'Wheel'
            when efa.session_id is not null
                then 'Wheel'

            -- Covered Call requires SUBSTANTIAL coverage, not incidental:
            -- sold calls must have been live for >= 30% of the session's
            -- days (Aug 2026 audit F5). A multi-year holding that carried
            -- a call for a few weeks is a Buy and Hold — attributing its
            -- entire equity P&L to 'Covered Call' mis-bucketed -$77K at
            -- audit time. The call contracts themselves keep their own
            -- (option-side) labels regardless of this branch.
            when eos.num_sold_calls > 0 and e.max_quantity_held >= 100
                 and safe_divide(
                         eos.sold_call_covered_days,
                         date_diff(
                             case when e.status = 'Open'
                                  then current_date()
                                  else e.last_trade_date end,
                             e.open_date, day) + 1
                     ) >= 0.30
                then 'Covered Call'

            -- Price-tracker fold (2026-07-14; threshold widened Aug 2026
            -- audit F3): a nominal equity lot held alongside options on
            -- the same underlying is a "so I can watch the ticker"
            -- position, NOT a standalone Buy and Hold. Surfacing it as its
            -- own strategy row made a pure long-call position read as
            -- "mixed" and cluttered the Strategy Breakdown. Fold it into
            -- the dominant option strategy on that underlying; the tiny
            -- equity P&L still shows in Breakdown-by-Type's Equity row.
            -- Threshold is < 25 shares (was <= 1): real tracker lots run
            -- 1-20 shares (audit found 2.7 / 8 / 9.6 / 11 / 20-share
            -- trackers labeled Buy and Hold), while anything >= 25 shares
            -- is a real capital commitment that deserves its own row. A
            -- < 25-share lot can never cover a call (needs 100), so this
            -- cannot swallow a genuine covered-call session.
            when coalesce(e.max_quantity_held, 0) < 25
                 and coalesce(eos.num_option_contracts, 0) > 0
                then coalesce(dos.dominant_strategy, 'Long Call')

            else 'Buy and Hold'
        end as strategy,

        case
            when e.status = 'Closed' then coalesce(sr.realized_pnl, 0) > 0
            else e.total_pnl > 0
        end as is_winner

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
