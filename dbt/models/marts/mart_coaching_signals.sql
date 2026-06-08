{{ config(materialized='table') }}

/*
    Aggregated coaching signals per (account, strategy).

    Pre-computes the behavioral metrics that the AI coach narrates:
      - Exit timing: avg giveback, days held past peak, optimal exit rate
      - Roll timing: count/avg DTE, loss + ITM context (stock vs strike at close),
        replacement-contract win rate (not holistic "roll quality")
      - DTE performance: best/worst buckets

    Base grain: distinct (account, user_id, strategy) from a union of
    int_option_exit_analysis, roll legs tagged with strategy, and
    mart_option_trades_by_kind — so strategies with only DTE aggregates or
    only tagged rolls still appear; exit_stats / roll_stats / DTE joins
    remain left joins and may be sparse per row.

    Only exit_stats rows use data_reliable = true (>=40% snapshot density
    and >=3 snapshots); that filter is intentionally not applied to DTE buckets
    (see comments on those CTEs).
*/

with exit_stats as (
    /*
        Exit timing aggregates: only "reliable" closed contracts (dense daily
        snapshots) so giveback / days-past-peak are not driven by sparse MTM.
        This is a data-quality gate, not a statistical minimum like the
        having sum(num_trades) >= 3 used in dte_performance for bucket stability.
    */
    select
        account,
        user_id,
        tenant_id,
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
    group by 1, 2, 3, 4
),

/*
    Roll stats at (account, user_id, strategy): each roll is attributed to the
    strategy of the closed (old) leg via int_strategy_classification.
    Rolls whose old contract does not match an option_contract row in
    classification are excluded from strat-level roll metrics (no account-level
    duplicate broadcast to every strategy).
*/
roll_stats as (
    select
        r.account,
        r.user_id,
        r.tenant_id,
        sc.strategy,
        count(*)                                    as num_rolls,
        round(avg(r.dte_at_roll), 1)                as avg_dte_at_roll,
        countif(r.new_contract_outcome = 'Winner')  as roll_winners,
        countif(r.new_contract_outcome = 'Loser')   as roll_losers,
        round(safe_divide(
            countif(r.new_contract_outcome = 'Winner'),
            nullif(countif(r.new_contract_outcome in ('Winner', 'Loser')), 0)
        ) * 100, 1)                                 as replacement_win_rate,
        round(avg(r.net_roll_credit), 2)             as avg_roll_credit,

        countif(r.dte_at_roll >= 7)                 as rolls_early,
        countif(r.dte_at_roll < 7)                  as rolls_late,
        round(safe_divide(
            countif(r.dte_at_roll >= 7 and r.new_contract_outcome = 'Winner'),
            nullif(countif(r.dte_at_roll >= 7 and r.new_contract_outcome in ('Winner', 'Loser')), 0)
        ) * 100, 1)                                 as early_replacement_win_rate,
        round(safe_divide(
            countif(r.dte_at_roll < 7 and r.new_contract_outcome = 'Winner'),
            nullif(countif(r.dte_at_roll < 7 and r.new_contract_outcome in ('Winner', 'Loser')), 0)
        ) * 100, 1)                                 as late_replacement_win_rate,

        countif(coalesce(r.old_pnl, 0) < 0)         as rolls_after_losing_leg,
        round(safe_divide(
            countif(coalesce(r.old_pnl, 0) < 0),
            count(*)
        ) * 100, 1)                                 as pct_rolls_after_losing_leg,

        countif(r.dte_at_roll <= 1)                 as rolls_at_0_or_1_dte,
        round(safe_divide(
            countif(r.dte_at_roll <= 1),
            count(*)
        ) * 100, 1)                                 as pct_rolls_at_0_or_1_dte,

        countif(r.sold_short_itm_at_roll is true)     as rolls_sold_short_itm_count,
        countif(r.sold_short_itm_at_roll is not null) as rolls_with_spot_for_itm,
        round(safe_divide(
            countif(r.sold_short_itm_at_roll is true),
            nullif(countif(r.sold_short_itm_at_roll is not null), 0)
        ) * 100, 1)                                 as pct_rolls_sold_short_itm_when_known

    from {{ ref('int_option_rolls') }} r
    inner join {{ ref('int_strategy_classification') }} sc
        on r.account = sc.account
        and (r.user_id is not distinct from sc.user_id)
        and (r.tenant_id is not distinct from sc.tenant_id)
        and r.old_trade_symbol = sc.trade_symbol
        and sc.trade_group_type = 'option_contract'
    group by 1, 2, 3, 4
),

dte_performance as (
    /*
        DTE bucket win rates: trade-kind mart does not carry snapshot
        reliability; we instead require at least 3 trades per bucket so a
        bucket win rate is not built from one or two noisy outcomes.
    */
    select
        account,
        user_id,
        tenant_id,
        strategy,
        dte_bucket,
        sum(num_trades) as bucket_trades,
        round(safe_divide(
            sum(case when outcome = 'Winner' then num_trades else 0 end),
            nullif(sum(num_trades), 0)
        ) * 100, 1) as bucket_win_rate,
        sum(total_pnl) as bucket_pnl
    from {{ ref('mart_option_trades_by_kind') }}
    group by 1, 2, 3, 4, 5
    having sum(num_trades) >= 3
),

best_dte as (
    select * from (
        select *,
            -- After bucket_win_rate, prefer buckets with more trades so ties
            -- break toward statistically meaningful buckets (not a 1-trade fluke).
            row_number() over (
                partition by tenant_id, account, user_id, strategy
                order by bucket_win_rate desc, bucket_trades desc
            ) as rn
        from dte_performance
    ) where rn = 1
),

worst_dte as (
    select * from (
        select *,
            -- Lowest bucket_win_rate first; if two buckets tie on rate, prefer
            -- the one with more trades so the worst bucket is not a 1-trade fluke.
            row_number() over (
                partition by tenant_id, account, user_id, strategy
                order by bucket_win_rate asc, bucket_trades desc
            ) as rn
        from dte_performance
    ) where rn = 1
),

base as (
    select distinct account, user_id, tenant_id, strategy
    from (
        select account, user_id, tenant_id, strategy
        from {{ ref('int_option_exit_analysis') }}
        union distinct
        select
            r.account,
            r.user_id,
            r.tenant_id,
            sc.strategy
        from {{ ref('int_option_rolls') }} r
        inner join {{ ref('int_strategy_classification') }} sc
            on r.account = sc.account
            and (r.user_id is not distinct from sc.user_id)
            and (r.tenant_id is not distinct from sc.tenant_id)
            and r.old_trade_symbol = sc.trade_symbol
            and sc.trade_group_type = 'option_contract'
        union distinct
        select account, user_id, tenant_id, strategy
        from {{ ref('mart_option_trades_by_kind') }}
    )
)

select
    b.account,
    b.user_id,
    -- v2 tenant_id carried natively (part of the grain).
    b.tenant_id,
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
    r.replacement_win_rate,
    r.avg_roll_credit,
    coalesce(r.rolls_early, 0)                  as rolls_early,
    coalesce(r.rolls_late, 0)                   as rolls_late,
    r.early_replacement_win_rate,
    r.late_replacement_win_rate,

    coalesce(r.rolls_after_losing_leg, 0)           as rolls_after_losing_leg,
    r.pct_rolls_after_losing_leg,
    coalesce(r.rolls_at_0_or_1_dte, 0)              as rolls_at_0_or_1_dte,
    r.pct_rolls_at_0_or_1_dte,
    coalesce(r.rolls_sold_short_itm_count, 0)       as rolls_sold_short_itm_count,
    coalesce(r.rolls_with_spot_for_itm, 0)           as rolls_with_spot_for_itm,
    r.pct_rolls_sold_short_itm_when_known,

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
   and (b.tenant_id is not distinct from e.tenant_id)
   and b.strategy = e.strategy
left join roll_stats r
    on b.account = r.account
   and (b.user_id is not distinct from r.user_id)
   and (b.tenant_id is not distinct from r.tenant_id)
   and b.strategy = r.strategy
left join best_dte bd
    on b.account = bd.account
   and (b.user_id is not distinct from bd.user_id)
   and (b.tenant_id is not distinct from bd.tenant_id)
   and b.strategy = bd.strategy
left join worst_dte wd
    on b.account = wd.account
   and (b.user_id is not distinct from wd.user_id)
   and (b.tenant_id is not distinct from wd.tenant_id)
   and b.strategy = wd.strategy
