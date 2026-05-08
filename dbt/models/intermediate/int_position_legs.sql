/*
    Position legs — canonical leg view for the /position/<symbol> page.

    A "leg" is one chronological chapter of trading activity for a
    (user_id, account, symbol). Two kinds:

      1. equity_session — every equity ownership cycle from
         int_equity_sessions (positive `leg_id` = `session_id`).
         Any open or closed option contract whose open_date falls
         inside the session's active window is *attributed* to the
         session and rolls into its P&L.

      2. options_only   — option contracts whose open_date falls
         outside any equity session. Grouped into one leg per
         "gap" (before / between / after equity sessions).
         Negative `leg_id` (-1, -2, …) so they never collide with
         equity session_ids.

    Status is the canonical Open / Closed flag for the LEG:
      - Open if the underlying equity session is Open, OR
      - Open if any option contract assigned to the leg is Open.
    Otherwise Closed.

    Replaces ~200 lines of stateful Python in app/routes.py
    (the orphan-grouping / leg-pill construction). Keeps the
    `session_id` ⇄ `leg_id` contract so existing ?leg=<n> URLs
    keep working.

    Tenancy: every CTE keys on (user_id, account, symbol).
    All cross-CTE joins use IS NOT DISTINCT FROM on user_id so
    legacy NULL rows still match each other but never leak to
    a different non-NULL user.
*/

with equity_sessions as (
    select
        account,
        user_id,
        symbol,
        session_id,
        open_date,
        last_trade_date,
        status,
        total_pnl as equity_pnl,
        max_quantity_held,
        num_trades as equity_num_trades,
        case
            when status = 'Open' then current_date()
            else last_trade_date
        end as effective_end_date
    from {{ ref('int_equity_sessions') }}
),

option_contracts as (
    select
        account,
        user_id,
        underlying_symbol as symbol,
        trade_symbol,
        open_date,
        close_date,
        status,
        total_pnl,
        current_unrealized_pnl
    from {{ ref('int_option_contracts') }}
),

-- Attach each option to an equity session whose active window contains
-- the option's open_date. Equity sessions for the same
-- (user_id, account, symbol) are non-overlapping by construction
-- (one ownership cycle at a time), so this is at most one match.
options_with_session as (
    select
        o.account,
        o.user_id,
        o.symbol,
        o.trade_symbol,
        o.open_date,
        o.close_date,
        o.status,
        o.total_pnl,
        o.current_unrealized_pnl,
        e.session_id as assigned_session_id
    from option_contracts o
    left join equity_sessions e
        on  e.account = o.account
        and e.user_id is not distinct from o.user_id
        and e.symbol = o.symbol
        and o.open_date >= e.open_date
        and o.open_date <= e.effective_end_date
),

-- For options that didn't land in any equity session, compute a stable
-- gap_id = how many equity sessions opened on or before the option's
-- open_date. Two orphan options get the same gap_id iff they fall in
-- the same chronological gap, so a simple group-by-gap_id collapses
-- the cluster — no need for the order-walk dedup the Python did.
orphan_options as (
    select
        o.account,
        o.user_id,
        o.symbol,
        o.trade_symbol,
        o.open_date,
        o.close_date,
        o.status,
        o.total_pnl,
        o.current_unrealized_pnl,
        (
            select count(*)
            from equity_sessions e
            where e.account = o.account
              and e.user_id is not distinct from o.user_id
              and e.symbol = o.symbol
              and e.open_date <= o.open_date
        ) as gap_id
    from options_with_session o
    where o.assigned_session_id is null
),

-- Aggregate options that are attributed to an equity session.
session_option_rollup as (
    select
        account,
        user_id,
        symbol,
        assigned_session_id as session_id,
        sum(case when status = 'Closed' then total_pnl else 0 end)            as closed_options_pnl,
        sum(case when status = 'Open'   then current_unrealized_pnl else 0 end) as open_options_pnl,
        count(*) as options_count,
        sum(case when status = 'Open'   then 1 else 0 end) as open_options_count,
        max(case when status = 'Open'   then current_date() else close_date end) as last_option_activity_date
    from options_with_session
    where assigned_session_id is not null
    group by 1, 2, 3, 4
),

-- Aggregate options grouped per orphan gap.
orphan_rollup as (
    select
        account,
        user_id,
        symbol,
        gap_id,
        min(open_date) as open_date,
        -- "last activity": today if any option is still open, else the
        -- latest close_date in the cluster. Falls back to max(open_date)
        -- if close_date is somehow NULL on every contract.
        coalesce(
            max(case when status = 'Open' then current_date() else close_date end),
            max(open_date)
        ) as last_activity_date,
        sum(case when status = 'Closed' then total_pnl else 0 end)            as closed_options_pnl,
        sum(case when status = 'Open'   then current_unrealized_pnl else 0 end) as open_options_pnl,
        count(*) as options_count,
        sum(case when status = 'Open'   then 1 else 0 end) as open_options_count
    from orphan_options
    group by 1, 2, 3, 4
),

-- Equity-backed legs (the primary kind). Status flips to Open if any
-- attributed option is still live, even when the underlying equity
-- session itself has closed — that's exactly the case where the page
-- used to mislabel a leg "Closed" while a covered call was still open.
equity_session_legs as (
    select
        e.account,
        e.user_id,
        e.symbol,
        e.session_id                    as leg_id,
        cast('equity_session' as string) as leg_type,
        e.open_date,
        case
            when e.status = 'Open' or coalesce(so.open_options_count, 0) > 0
                then current_date()
            else greatest(
                e.last_trade_date,
                coalesce(so.last_option_activity_date, e.last_trade_date)
            )
        end as last_activity_date,
        case
            when e.status = 'Open' or coalesce(so.open_options_count, 0) > 0
                then 'Open'
            else 'Closed'
        end as status,
        coalesce(e.equity_pnl, 0)                  as equity_pnl,
        coalesce(so.closed_options_pnl, 0)         as closed_options_pnl,
        coalesce(so.open_options_pnl, 0)           as open_options_pnl,
        coalesce(so.options_count, 0)              as options_count,
        coalesce(so.open_options_count, 0)         as open_options_count,
        coalesce(e.max_quantity_held, 0)           as max_quantity_held,
        coalesce(e.equity_num_trades, 0)           as num_trades,
        false                                      as options_only
    from equity_sessions e
    left join session_option_rollup so
        on  so.session_id = e.session_id
        and so.account = e.account
        and so.user_id is not distinct from e.user_id
        and so.symbol = e.symbol
),

-- Option-only legs. leg_id is negative (-1, -2, …) so it never collides
-- with positive equity session_ids — preserves the historic URL
-- ?leg=-1 contract from the old Python implementation.
options_only_legs as (
    select
        account,
        user_id,
        symbol,
        cast(-(gap_id + 1) as int64)        as leg_id,
        cast('options_only' as string)      as leg_type,
        open_date,
        last_activity_date,
        case when open_options_count > 0 then 'Open' else 'Closed' end as status,
        cast(0.0 as float64)                as equity_pnl,
        closed_options_pnl,
        open_options_pnl,
        options_count,
        open_options_count,
        cast(0 as float64)                  as max_quantity_held,
        options_count                       as num_trades,
        true                                as options_only
    from orphan_rollup
),

all_legs as (
    select * from equity_session_legs
    union all
    select * from options_only_legs
),

final as (
    select
        account,
        user_id,
        symbol,
        leg_id,
        leg_type,
        status,
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
        options_only,
        row_number() over (
            partition by user_id, account, symbol
            order by open_date, leg_id
        ) as display_leg_num,
        date_diff(last_activity_date, open_date, day) as days_held
    from all_legs
)

select * from final
