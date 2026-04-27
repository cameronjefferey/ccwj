/*
    Strategy classification.

    Produces one row per classified "trade group" — either an equity session
    or an option contract — tagged with a strategy label:

      - Covered Call      (sold call while holding equity)
      - Cash-Secured Put  (sold put without equity)
      - Wheel             (put assigned → equity acquired, possibly with CCs)
      - Call Spread        (bought + sold call, same expiry, different strikes)
      - Put Spread         (bought + sold put,  same expiry, different strikes)
      - Long Call          (bought call, standalone)
      - Long Put           (bought put,  standalone, no equity)
      - Protective Put     (bought put while holding equity)
      - Naked Call         (sold call without equity)
      - Poor Man Covered Call (sold call covered by long call on same underlying, e.g. diagonal)
      - Buy and Hold       (equity only, no associated options)
*/

with equity_sessions as (
    select * from {{ ref('int_equity_sessions') }}
),

option_contracts as (
    select * from {{ ref('int_option_contracts') }}
),

---------------------------------------------------------------------
-- 1. For each equity session, count associated option activity
---------------------------------------------------------------------
equity_options_summary as (
    select
        e.account,
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
        end) as num_protective_puts
    from equity_sessions e
    left join option_contracts oc
        on e.account = oc.account
        and e.symbol = oc.underlying_symbol
    group by 1, 2, 3
),

---------------------------------------------------------------------
-- 2. Detect put assignments that led to equity sessions (→ Wheel)
---------------------------------------------------------------------
put_assignments as (
    select
        account,
        underlying_symbol,
        trade_symbol,
        close_date as assignment_date
    from option_contracts
    where close_type = 'Assigned'
      and option_type = 'P'
),

equity_from_assignment as (
    select distinct
        e.account,
        e.symbol,
        e.session_id
    from equity_sessions e
    join put_assignments pa
        on e.account = pa.account
        and e.symbol = pa.underlying_symbol
        and abs(date_diff(e.open_date, pa.assignment_date, day)) <= 5
),

---------------------------------------------------------------------
-- 3. Detect spread pairs (bought + sold, same underlying / expiry / type)
---------------------------------------------------------------------
spread_legs as (
    -- All trade_symbols that are part of a spread
    select distinct a.account, a.trade_symbol
    from option_contracts a
    join option_contracts b
        on a.account           = b.account
        and a.underlying_symbol = b.underlying_symbol
        and a.option_expiry     = b.option_expiry
        and a.option_type       = b.option_type
        and a.option_strike    != b.option_strike
        and a.direction        != b.direction
        and abs(date_diff(a.open_date, b.open_date, day)) <= 7

    union distinct

    select distinct b.account, b.trade_symbol
    from option_contracts a
    join option_contracts b
        on a.account           = b.account
        and a.underlying_symbol = b.underlying_symbol
        and a.option_expiry     = b.option_expiry
        and a.option_type       = b.option_type
        and a.option_strike    != b.option_strike
        and a.direction        != b.direction
        and abs(date_diff(a.open_date, b.open_date, day)) <= 7
),

---------------------------------------------------------------------
-- 3b. Poor Man Covered Call: short legs of matched pairs from int_pmcc_pairs.
--    PMCC = long call (expiry >= 180d, deep ITM proxy), short call (expiry <= 60d),
--    short strike > long strike, short qty <= long qty, long open when short written.
---------------------------------------------------------------------
pmcc_short_calls as (
    select distinct
        account,
        short_trade_symbol as trade_symbol
    from {{ ref('int_pmcc_pairs') }}
),

---------------------------------------------------------------------
-- 4. Classify option contracts
---------------------------------------------------------------------
options_classified as (
    select
        oc.account,
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
        and oc.trade_symbol = sl.trade_symbol
    -- Check for PMCC (short call covered by long call on same underlying)
    left join pmcc_short_calls pmcc
        on oc.account = pmcc.account
        and oc.trade_symbol = pmcc.trade_symbol
    -- Check for overlapping equity session (Covered Call / Protective Put detection)
    left join equity_sessions e
        on oc.account = e.account
        and oc.underlying_symbol = e.symbol
        and oc.open_date >= e.open_date
        and oc.open_date <= case
            when e.status = 'Open' then current_date()
            else e.last_trade_date
        end
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
        account,
        symbol,
        session_id,
        sum(realized_pnl) as realized_pnl
    from {{ ref('int_closed_equity_legs') }}
    group by 1, 2, 3
),

equity_classified as (
    select
        e.account,
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
            when efa.session_id is not null and eos.num_sold_calls > 0
                then 'Wheel'
            when efa.session_id is not null
                then 'Wheel'
            when eos.num_sold_calls > 0 and e.max_quantity_held >= 100
                then 'Covered Call'
            else 'Buy and Hold'
        end as strategy,

        case when e.total_pnl > 0 then true else false end as is_winner

    from equity_sessions e
    left join equity_options_summary eos
        on e.account = eos.account
        and e.symbol = eos.symbol
        and e.session_id = eos.session_id
    left join equity_from_assignment efa
        on e.account = efa.account
        and e.symbol = efa.symbol
        and e.session_id = efa.session_id
    left join session_realized sr
        on e.account = sr.account
        and e.symbol = sr.symbol
        and e.session_id = sr.session_id
)

---------------------------------------------------------------------
-- 6. Union all classified trade groups
---------------------------------------------------------------------
select * from options_classified
union all
select * from equity_classified
