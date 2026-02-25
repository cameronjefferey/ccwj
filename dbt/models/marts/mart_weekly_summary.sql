/*
    Weekly trading summary — one row per (account, iso_week_start).
    Pre-aggregates closed trades, best/worst trade details, strategy stats,
    and trade-open counts for the Weekly Review and temporal check-ins.
    ISO weeks run Monday→Sunday.
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
        date_trunc(open_date, isoweek) as week_start,
        count(*)                        as trades_opened
    from {{ ref('int_strategy_classification') }}
    where open_date is not null
    group by 1, 2
),

weekly_agg as (
    select
        account,
        week_start,
        count(*)                                          as trades_closed,
        sum(total_pnl)                                    as total_pnl,
        countif(is_winner)                                as num_winners,
        countif(not is_winner)                            as num_losers,
        sum(premium_received)                             as premium_received,
        sum(abs(premium_paid))                            as premium_paid,
        sum(num_trades)                                   as num_individual_trades
    from closed_trades
    group by 1, 2
),

ranked_best as (
    select *, row_number() over (
        partition by account, week_start order by total_pnl desc
    ) as rn
    from closed_trades
),

ranked_worst as (
    select *, row_number() over (
        partition by account, week_start order by total_pnl asc
    ) as rn
    from closed_trades
),

strategy_stats as (
    select
        account,
        week_start,
        strategy,
        count(*)                                          as strat_trades,
        countif(is_winner)                                as strat_winners,
        safe_divide(countif(is_winner), count(*))         as strat_win_rate,
        sum(total_pnl)                                    as strat_pnl
    from closed_trades
    group by 1, 2, 3
    having count(*) >= 2
),

ranked_strategy as (
    select *, row_number() over (
        partition by account, week_start order by strat_win_rate desc, strat_pnl desc
    ) as rn
    from strategy_stats
),

all_weeks as (
    select distinct account, week_start from weekly_agg
    union distinct
    select distinct account, week_start from opened_trades
)

select
    aw.account,
    aw.week_start,
    date_add(aw.week_start, interval 6 day) as week_end,

    coalesce(wa.trades_closed, 0)           as trades_closed,
    coalesce(wa.total_pnl, 0)              as total_pnl,
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
    on aw.account = wa.account and aw.week_start = wa.week_start
left join opened_trades ot
    on aw.account = ot.account and aw.week_start = ot.week_start
left join ranked_best rb
    on aw.account = rb.account and aw.week_start = rb.week_start and rb.rn = 1
left join ranked_worst rw
    on aw.account = rw.account and aw.week_start = rw.week_start and rw.rn = 1
left join ranked_strategy rs
    on aw.account = rs.account and aw.week_start = rs.week_start and rs.rn = 1

order by aw.account, aw.week_start
