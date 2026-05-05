{{ config(materialized='table') }}

/*
    Weekly P&L streaks — one row per (account, week_start).

    Uses a gaps-and-islands technique to detect consecutive winning or
    losing weeks. Exposes streak_length and streak_type for the weekly
    review's pattern detection.
*/

with weeks as (
    select
        account,
        user_id,
        week_start,
        total_pnl,
        case when total_pnl >= 0 then 'winning' else 'losing' end as week_result
    from {{ ref('mart_weekly_summary') }}
    where trades_closed > 0
),

-- Assign a group ID that changes whenever the streak type flips. Window
-- partitioned by (account, user_id) so two users with the same account
-- label can never share a streak count.
islands as (
    select
        *,
        row_number() over (partition by account, user_id order by week_start)
        - row_number() over (partition by account, user_id, week_result order by week_start)
        as island_id
    from weeks
),

streaks as (
    select
        account,
        user_id,
        week_start,
        total_pnl,
        week_result as streak_type,
        row_number() over (
            partition by account, user_id, week_result, island_id
            order by week_start
        ) as streak_length
    from islands
)

select
    account,
    user_id,
    week_start,
    total_pnl      as week_pnl,
    streak_type,
    cast(streak_length as int64) as streak_length
from streaks
order by account, user_id, week_start
