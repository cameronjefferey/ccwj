/*
    Weekly trading summary — one row per (account, iso_week_start).
    Pre-aggregates closed trades, best/worst trade details, strategy stats,
    trade-open counts, and weekly dividend cash flows for the Weekly
    Review and temporal check-ins.
    ISO weeks run Monday→Sunday.

    Dividends-as-first-class:
      - total_pnl         = trade-only P&L from int_strategy_classification
                            (preserved as the trade-consistency signal — used
                            by mart_weekly_account_change as pnl_closed_trades
                            in the equity-decomposition waterfall, and by
                            mart_weekly_behavior_enriched).
      - dividends_amount  = cash dividends received in the week.
      - total_return      = total_pnl + dividends_amount, the headline
                            "what did this week make me" number for the
                            Weekly Review hero / sentence templates.
*/

with closed_trades as (
    select
        *,
        date_trunc(close_date, isoweek) as week_start
    from {{ ref('int_strategy_classification') }}
    where status = 'Closed'
      and close_date is not null
),

opened_trades as (
    select
        account,
        user_id,
        date_trunc(open_date, isoweek) as week_start,
        count(*)                        as trades_opened
    from {{ ref('int_strategy_classification') }}
    where open_date is not null
    group by 1, 2, 3
),

weekly_agg as (
    select
        account,
        user_id,
        week_start,
        count(*)                                          as trades_closed,
        sum(total_pnl)                                    as total_pnl,
        countif(is_winner)                                as num_winners,
        countif(not is_winner)                            as num_losers,
        sum(premium_received)                             as premium_received,
        sum(abs(premium_paid))                            as premium_paid,
        sum(num_trades)                                   as num_individual_trades
    from closed_trades
    group by 1, 2, 3
),

-- Best/worst windows partitioned per-tenant so two users with the same
-- account label never share a "best of week" ranking.
ranked_best as (
    select *, row_number() over (
        partition by account, user_id, week_start order by total_pnl desc
    ) as rn
    from closed_trades
),

ranked_worst as (
    select *, row_number() over (
        partition by account, user_id, week_start order by total_pnl asc
    ) as rn
    from closed_trades
),

strategy_stats as (
    select
        account,
        user_id,
        week_start,
        strategy,
        count(*)                                          as strat_trades,
        countif(is_winner)                                as strat_winners,
        safe_divide(countif(is_winner), count(*))         as strat_win_rate,
        sum(total_pnl)                                    as strat_pnl
    from closed_trades
    group by 1, 2, 3, 4
    having count(*) >= 2
),

ranked_strategy as (
    select *, row_number() over (
        partition by account, user_id, week_start order by strat_win_rate desc, strat_pnl desc
    ) as rn
    from strategy_stats
),

-- Weekly dividend cash flows (sum of stg_history dividend events per ISO week).
-- Aggregated here rather than in the consumer so every reader of
-- mart_weekly_summary gets the dividend stream in one place.
weekly_dividends as (
    select
        account,
        user_id,
        date_trunc(date, isoweek) as week_start,
        sum(dividends_amount)     as dividends_amount
    from {{ ref('mart_daily_pnl') }}
    group by 1, 2, 3
),

all_weeks as (
    select distinct account, user_id, week_start from weekly_agg
    union distinct
    select distinct account, user_id, week_start from opened_trades
    union distinct
    select distinct account, user_id, week_start from weekly_dividends
)

select
    aw.account,
    aw.user_id,
    aw.week_start,
    date_add(aw.week_start, interval 6 day) as week_end,

    coalesce(wa.trades_closed, 0)           as trades_closed,
    coalesce(wa.total_pnl, 0)              as total_pnl,
    coalesce(wd.dividends_amount, 0)       as dividends_amount,
    coalesce(wa.total_pnl, 0)
        + coalesce(wd.dividends_amount, 0) as total_return,
    coalesce(wa.num_winners, 0)            as num_winners,
    coalesce(wa.num_losers, 0)             as num_losers,
    coalesce(wa.premium_received, 0)       as premium_received,
    coalesce(wa.premium_paid, 0)           as premium_paid,
    coalesce(wa.num_individual_trades, 0)  as num_individual_trades,
    coalesce(ot.trades_opened, 0)          as trades_opened,

    -- best trade details
    rb.symbol         as best_symbol,
    rb.strategy       as best_strategy,
    rb.trade_symbol   as best_trade_symbol,
    rb.total_pnl      as best_pnl,
    rb.close_date     as best_close_date,

    -- worst trade details
    rw.symbol         as worst_symbol,
    rw.strategy       as worst_strategy,
    rw.trade_symbol   as worst_trade_symbol,
    rw.total_pnl      as worst_pnl,
    rw.close_date     as worst_close_date,

    -- top strategy
    rs.strategy       as top_strategy,
    rs.strat_win_rate as top_strategy_win_rate,
    rs.strat_trades   as top_strategy_trades,
    rs.strat_pnl      as top_strategy_pnl

from all_weeks aw
left join weekly_agg wa
    on aw.account = wa.account
    and (aw.user_id is not distinct from wa.user_id)
    and aw.week_start = wa.week_start
left join weekly_dividends wd
    on aw.account = wd.account
    and (aw.user_id is not distinct from wd.user_id)
    and aw.week_start = wd.week_start
left join opened_trades ot
    on aw.account = ot.account
    and (aw.user_id is not distinct from ot.user_id)
    and aw.week_start = ot.week_start
left join ranked_best rb
    on aw.account = rb.account
    and (aw.user_id is not distinct from rb.user_id)
    and aw.week_start = rb.week_start and rb.rn = 1
left join ranked_worst rw
    on aw.account = rw.account
    and (aw.user_id is not distinct from rw.user_id)
    and aw.week_start = rw.week_start and rw.rn = 1
left join ranked_strategy rs
    on aw.account = rs.account
    and (aw.user_id is not distinct from rs.user_id)
    and aw.week_start = rs.week_start and rs.rn = 1

order by aw.account, aw.user_id, aw.week_start
