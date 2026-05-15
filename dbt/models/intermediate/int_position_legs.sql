/*
    Position legs — merged-interval definition.

    Mental model: a "leg" is one continuous chapter of trading activity for
    a (user_id, account, symbol). "Continuous" = at least one position
    (equity OR open option contract) was alive every day, with no gap.

    Algorithm:
      1. Cast every equity session and every option contract as an
         interval [open_date, close_date_or_today].
      2. Sort by open_date and walk: a new leg starts whenever the next
         interval's open_date is strictly later than the running maximum
         close_date seen so far. Otherwise it merges into the current leg.
      3. Aggregate per merged leg.

    Why this matters for the page: under the previous "anchor each leg
    to an equity session, attach options by date overlap" rule, a single
    long-dated LEAP whose lifetime spanned an equity-session boundary
    AND a separate orphan options cluster afterwards produced TWO Open
    leg pills for what a trader thinks of as one ongoing chapter.
    PLTR / Cameron Investment was the visible case: equity session
    Jun-2025→Apr-2026 + Long Call still alive + post-equity short call
    used to render as Leg 2 (Open) and Leg 3 (Open). Under merged
    intervals it's one Open leg spanning Jun 2025 → today.

    Status:
      - Open  iff the leg's max close_date == current_date(), which
        only happens when an interval inside the leg is itself open.
      - Closed otherwise.

    Invariant (enforced by dbt test): at most one Open leg per
    (user_id, account, symbol). The merge algorithm guarantees this by
    construction — every still-open interval extends to today, and any
    other interval that touches today gets merged in.

    leg_id: sequential 1..N per (user_id, account, symbol) ordered by
    open_date. Bookmarked URLs from the previous mart shape (where
    orphan-options legs used negative ids) won't necessarily resolve
    to the same activity but ?leg=<n> still works.

    Tenancy: every CTE keys on (user_id, account, symbol). Cross-CTE
    joins use IS NOT DISTINCT FROM on user_id so legacy NULL rows
    still match each other but never leak across populated tenants.
*/

with equity_intervals as (
    select
        account,
        user_id,
        symbol,
        open_date,
        case when status = 'Open' then current_date() else last_trade_date end as close_date,
        cast('equity' as string) as source,
        case when status = 'Open' then true else false end as is_open,
        coalesce(total_pnl, 0)         as equity_pnl,
        cast(0 as float64)             as option_total_pnl,
        cast(0 as float64)             as option_unrealized_pnl,
        cast(0 as int64)               as option_count,
        cast(0 as int64)               as open_option_count,
        coalesce(max_quantity_held, 0) as max_quantity_held,
        coalesce(num_trades, 0)        as num_trades
    from {{ ref('int_equity_sessions') }}
),

option_intervals as (
    select
        account,
        user_id,
        underlying_symbol as symbol,
        open_date,
        case
            when status = 'Open' then current_date()
            else coalesce(close_date, open_date)
        end as close_date,
        cast('option' as string) as source,
        case when status = 'Open' then true else false end as is_open,
        cast(0 as float64)                              as equity_pnl,
        case when status = 'Closed' then coalesce(total_pnl, 0) else 0 end
                                                        as option_total_pnl,
        case when status = 'Open'   then coalesce(current_unrealized_pnl, 0) else 0 end
                                                        as option_unrealized_pnl,
        cast(1 as int64)                                as option_count,
        case when status = 'Open' then 1 else 0 end     as open_option_count,
        cast(0 as float64)                              as max_quantity_held,
        cast(1 as int64)                                as num_trades
    from {{ ref('int_option_contracts') }}
),

all_intervals_raw as (
    select * from equity_intervals
    union all
    select * from option_intervals
),

-- Defensive dedup: a duplicated upstream row (poisoned int_equity_sessions
-- after a stg_history dupe, or two rows in int_option_contracts for the
-- same OSI under the same tenant) used to fan into two intervals here, and
-- the chronological walk below would treat them as a real chapter split
-- because two intervals starting on the same day with the same close_date
-- still consume two `row_number()` slots. Visually that produced phantom
-- "Leg 1 / Leg 1 / Leg 2" pills under the merged-interval mart. Dropping
-- exact (account, user_id, symbol, source, open_date, close_date) duplicates
-- here keeps the leg sequence stable under input duplication. The plan-of-
-- record fix is upstream (the `_canonicalize_seed_cell` dedup in the seed
-- merge + the dbt singular test on stg_history), but this guard means a
-- future regression in either upstream model can't split a leg by accident.
-- See ~/.cursor/skills/broker-sync-safety/SKILL.md (2026-05-11).
all_intervals as (
    select
        account,
        user_id,
        symbol,
        open_date,
        close_date,
        source,
        is_open,
        equity_pnl,
        option_total_pnl,
        option_unrealized_pnl,
        option_count,
        open_option_count,
        max_quantity_held,
        num_trades
    from all_intervals_raw
    qualify row_number() over (
        partition by account, user_id, symbol, source, open_date, close_date
        order by is_open desc,                  -- prefer the still-open copy
                 option_unrealized_pnl desc,    -- then the richer P&L copy
                 num_trades desc
    ) = 1
),

-- Walk intervals chronologically. The "running max close" is the latest
-- close_date seen STRICTLY BEFORE the current row (rows between
-- unbounded preceding and 1 preceding); any interval that opens on or
-- before that date is part of the same continuous chapter, anything
-- opening later kicks off a fresh leg.
ordered as (
    select
        *,
        row_number() over (
            partition by user_id, account, symbol
            order by open_date, close_date, source
        ) as rn
    from all_intervals
),

with_running_max as (
    select
        *,
        max(close_date) over (
            partition by user_id, account, symbol
            order by open_date, close_date, source
            rows between unbounded preceding and 1 preceding
        ) as prev_max_close
    from ordered
),

with_break_flag as (
    select
        *,
        case
            when prev_max_close is null then 1
            when open_date > prev_max_close then 1
            else 0
        end as is_new_leg
    from with_running_max
),

-- Cumulative count of breaks = leg sequence number per partition.
-- Stable across rebuilds because the underlying sort is deterministic.
with_leg_seq as (
    select
        *,
        sum(is_new_leg) over (
            partition by user_id, account, symbol
            order by open_date, close_date, source
            rows between unbounded preceding and current row
        ) as leg_seq
    from with_break_flag
),

aggregated as (
    select
        account,
        user_id,
        symbol,
        leg_seq                                   as leg_id,
        min(open_date)                            as open_date,
        max(close_date)                           as last_activity_date,
        sum(equity_pnl)                           as equity_pnl,
        sum(option_total_pnl)                     as closed_options_pnl,
        sum(option_unrealized_pnl)                as open_options_pnl,
        sum(option_count)                         as options_count,
        sum(open_option_count)                    as open_options_count,
        max(max_quantity_held)                    as max_quantity_held,
        sum(num_trades)                           as num_trades,
        sum(case when source = 'equity' then 1 else 0 end) as equity_session_count,
        max(case when is_open then 1 else 0 end)  as has_open_interval
    from with_leg_seq
    group by 1, 2, 3, 4
),

final as (
    select
        account,
        user_id,
        symbol,
        leg_id,
        case
            when equity_session_count = 0 then cast('options_only'   as string)
            when options_count        = 0 then cast('equity_only'    as string)
            else                              cast('mixed'           as string)
        end as leg_type,
        case when has_open_interval = 1 then 'Open' else 'Closed' end as status,
        open_date,
        last_activity_date,
        equity_pnl,
        closed_options_pnl,
        open_options_pnl,
        equity_pnl + closed_options_pnl + open_options_pnl as combined_pnl,
        options_count,
        open_options_count,
        max_quantity_held,
        num_trades,
        equity_session_count = 0 as options_only,
        row_number() over (
            partition by user_id, account, symbol
            order by open_date, leg_id
        ) as display_leg_num,
        date_diff(last_activity_date, open_date, day) as days_held
    from aggregated
)

select * from final
