{{ config(materialized='table') }}

/*
    Weekly behavior-enriched summary — one row per (account, week_start).

    Built on top of mart_weekly_summary to add a simple, deterministic
    baseline for "what is normal" over the recent past.

    For each (account, week_start) we expose:
      - trades_closed, total_pnl, num_winners, num_losers
      - win_rate_week           (num_winners / (winners + losers))
      - avg_trades_closed_8w    (avg trades_closed over the prior 8 weeks, per account)
      - avg_total_pnl_8w        (avg total_pnl over the prior 8 weeks, per account)
      - avg_win_rate_8w         (avg win_rate_week over the prior 8 weeks, per account)
      - baseline_weeks_8w       (how many prior weeks went into the baseline)

    Weekly Review uses this to show:
      "How this week compares to your normal" — anchored in trade-level
    data, not psychology.
*/

with base as (
    select
        account,
        user_id,
        week_start,
        trades_closed,
        total_pnl,
        num_winners,
        num_losers,
        safe_divide(num_winners, nullif(num_winners + num_losers, 0)) as win_rate_week
    from {{ ref('mart_weekly_summary') }}
),

with_baseline as (
    select
        account,
        user_id,
        week_start,
        trades_closed,
        total_pnl,
        num_winners,
        num_losers,
        win_rate_week,

        -- Rolling baseline over the previous 8 weeks (per tenant),
        -- excluding the current week. Window keyed by (account, user_id)
        -- so two users with the same account label never share a baseline.
        avg(trades_closed) over (
            partition by account, user_id
            order by week_start
            rows between 8 preceding and 1 preceding
        ) as avg_trades_closed_8w,

        avg(total_pnl) over (
            partition by account, user_id
            order by week_start
            rows between 8 preceding and 1 preceding
        ) as avg_total_pnl_8w,

        avg(win_rate_week) over (
            partition by account, user_id
            order by week_start
            rows between 8 preceding and 1 preceding
        ) as avg_win_rate_8w,

        count(trades_closed) over (
            partition by account, user_id
            order by week_start
            rows between 8 preceding and 1 preceding
        ) as baseline_weeks_8w
    from base
)

select
    account,
    user_id,
    week_start,
    trades_closed,
    total_pnl,
    num_winners,
    num_losers,
    win_rate_week,
    avg_trades_closed_8w,
    avg_total_pnl_8w,
    avg_win_rate_8w,
    baseline_weeks_8w
from with_baseline
order by account, user_id, week_start

