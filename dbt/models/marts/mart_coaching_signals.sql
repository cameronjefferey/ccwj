{{ config(materialized='table') }}

/*
    Aggregated coaching signals per (account, strategy).

    Pre-computes the behavioral metrics that the AI coach narrates:
      - Exit timing: avg giveback, days held past peak, optimal exit rate
      - Roll behavior: count, timing, success rate
      - DTE performance: best/worst buckets

    Only includes contracts where data_reliable = true (>=40% snapshot
    density and >=3 snapshots). This prevents sparse-data contracts from
    distorting the aggregates.
*/

with exit_stats as (
    select
        account,
        user_id,
        strategy,
        count(*)                                                as total_closed,
        countif(data_reliable)                                  as reliable_contracts,
        round(avg(case when data_reliable then giveback_pct end), 1)
                                                                as avg_giveback_pct,
        round(avg(case when data_reliable then pnl_given_back end), 2)
                                                                as avg_pnl_given_back,
        round(avg(case when data_reliable then days_held_past_peak end), 1)
                                                                as avg_days_held_past_peak,
        countif(optimal_exit and data_reliable)                 as optimal_exits,
        round(safe_divide(
            countif(optimal_exit and data_reliable),
            nullif(countif(data_reliable), 0)
        ) * 100, 1)                                             as optimal_exit_rate,
        round(avg(case
            when data_reliable and direction = 'Sold'
                 and pct_of_premium_captured is not null
            then pct_of_premium_captured end), 1)
                                                                as avg_pct_premium_captured,
        round(avg(case when data_reliable then actual_pnl end), 2)
                                                                as avg_actual_pnl,
        round(sum(case when data_reliable then pnl_given_back else 0 end), 2)
                                                                as total_pnl_given_back,
        -- Data coverage: what fraction of closed contracts have reliable data
        round(safe_divide(
            countif(data_reliable),
            nullif(count(*), 0)
        ) * 100, 0)                                             as pct_contracts_reliable
    from {{ ref('int_option_exit_analysis') }}
    group by 1, 2, 3
),

roll_stats as (
    select
        account,
        user_id,
        count(*)                                    as num_rolls,
        round(avg(dte_at_roll), 1)                  as avg_dte_at_roll,
        countif(new_contract_outcome = 'Winner')    as roll_winners,
        countif(new_contract_outcome = 'Loser')     as roll_losers,
        round(safe_divide(
            countif(new_contract_outcome = 'Winner'),
            nullif(countif(new_contract_outcome in ('Winner', 'Loser')), 0)
        ) * 100, 1)                                 as roll_success_rate,
        round(avg(net_roll_credit), 2)              as avg_roll_credit,

        countif(dte_at_roll >= 7)                   as rolls_early,
        countif(dte_at_roll < 7)                    as rolls_late,
        round(safe_divide(
            countif(dte_at_roll >= 7 and new_contract_outcome = 'Winner'),
            nullif(countif(dte_at_roll >= 7 and new_contract_outcome in ('Winner', 'Loser')), 0)
        ) * 100, 1)                                 as early_roll_success_rate,
        round(safe_divide(
            countif(dte_at_roll < 7 and new_contract_outcome = 'Winner'),
            nullif(countif(dte_at_roll < 7 and new_contract_outcome in ('Winner', 'Loser')), 0)
        ) * 100, 1)                                 as late_roll_success_rate
    from {{ ref('int_option_rolls') }}
    group by 1, 2
),

dte_performance as (
    select
        account,
        user_id,
        strategy,
        dte_bucket,
        sum(num_trades) as bucket_trades,
        round(safe_divide(
            sum(case when outcome = 'Winner' then num_trades else 0 end),
            nullif(sum(num_trades), 0)
        ) * 100, 1) as bucket_win_rate,
        sum(total_pnl) as bucket_pnl
    from {{ ref('mart_option_trades_by_kind') }}
    group by 1, 2, 3, 4
    having sum(num_trades) >= 3
),

best_dte as (
    select * from (
        select *,
            row_number() over (
                partition by account, user_id, strategy
                order by bucket_win_rate desc, bucket_trades desc
            ) as rn
        from dte_performance
    ) where rn = 1
),

worst_dte as (
    select * from (
        select *,
            row_number() over (
                partition by account, user_id, strategy
                order by bucket_win_rate asc, bucket_trades desc
            ) as rn
        from dte_performance
    ) where rn = 1
),

base as (
    select distinct account, user_id, strategy
    from {{ ref('int_option_exit_analysis') }}
)

select
    b.account,
    b.user_id,
    b.strategy,

    coalesce(e.total_closed, 0)                 as total_closed,
    coalesce(e.reliable_contracts, 0)           as reliable_contracts,
    coalesce(e.pct_contracts_reliable, 0)       as pct_contracts_reliable,
    e.avg_giveback_pct,
    e.avg_pnl_given_back,
    e.avg_days_held_past_peak,
    e.optimal_exit_rate,
    e.avg_pct_premium_captured,
    e.avg_actual_pnl,
    coalesce(e.total_pnl_given_back, 0)         as total_pnl_given_back,

    coalesce(r.num_rolls, 0)                    as num_rolls,
    r.avg_dte_at_roll,
    r.roll_success_rate,
    r.avg_roll_credit,
    coalesce(r.rolls_early, 0)                  as rolls_early,
    coalesce(r.rolls_late, 0)                   as rolls_late,
    r.early_roll_success_rate,
    r.late_roll_success_rate,

    bd.dte_bucket                               as best_dte_bucket,
    bd.bucket_win_rate                          as best_dte_win_rate,
    bd.bucket_trades                            as best_dte_trades,

    wd.dte_bucket                               as worst_dte_bucket,
    wd.bucket_win_rate                          as worst_dte_win_rate,
    wd.bucket_trades                            as worst_dte_trades

from base b
left join exit_stats e
    on b.account = e.account
   and (b.user_id is not distinct from e.user_id)
   and b.strategy = e.strategy
left join roll_stats r
    on b.account = r.account
   and (b.user_id is not distinct from r.user_id)
left join best_dte bd
    on b.account = bd.account
   and (b.user_id is not distinct from bd.user_id)
   and b.strategy = bd.strategy
left join worst_dte wd
    on b.account = wd.account
   and (b.user_id is not distinct from wd.user_id)
   and b.strategy = wd.strategy
