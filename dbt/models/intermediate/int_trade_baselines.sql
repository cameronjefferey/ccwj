{{ config(materialized='table') }}

/*
    Rolling per-account baselines evaluated at the moment each trade opened.

    All features reflect only PRIOR closed trades (close_date < this trade's
    open_date) so they represent the context the account had at open, not
    a peek at the future.

    Tenancy grain: `account`.  Every window is partitioned by account.

    Outputs (per (account, trade_symbol)):
      - acct_avg_notional_30d / acct_avg_notional_90d
      - size_vs_30d_baseline  / size_vs_90d_baseline
      - consecutive_losses_before (over all prior closed trades for account)
      - days_since_last_loss
      - strategy_win_rate_180d (null when < 10 prior same-strategy trades)
*/

with base as (
    select
        account,
        trade_symbol,
        strategy,
        open_date,
        close_date,
        notional_proxy,
        realized_pnl,
        is_winner
    from {{ ref('int_trade_features') }}
),

------------------------------------------------------------------
-- Build a per-account chronological stream of CLOSED trades, used
-- to compute "loss streak ending at this close" via gaps-and-islands.
-- stream_id is a stable ordering key within an account.
------------------------------------------------------------------
closed_stream as (
    select
        account,
        trade_symbol,
        close_date,
        is_winner,
        row_number() over (
            partition by account
            order by close_date, trade_symbol
        ) as stream_id
    from base
),

-- Gaps-and-islands: each time we see a winner, bump a group id.
-- Consecutive losses within the same group form a streak.
with_group as (
    select
        account,
        trade_symbol,
        close_date,
        is_winner,
        stream_id,
        sum(case when is_winner then 1 else 0 end) over (
            partition by account
            order by stream_id
            rows between unbounded preceding and current row
        ) as win_group_id
    from closed_stream
),

-- Loss streak as of each closed trade: count of losses within the current
-- open "since last winner" group, 0 if this trade was a winner.
loss_streak_at_close as (
    select
        account,
        trade_symbol,
        close_date,
        is_winner,
        case
            when is_winner then 0
            else sum(case when is_winner then 0 else 1 end) over (
                partition by account, win_group_id
                order by stream_id
                rows between unbounded preceding and current row
            )
        end as loss_streak_at_close,
        stream_id
    from with_group
),

------------------------------------------------------------------
-- For each opening trade, find the most recent prior close (by account).
-- The "consecutive_losses_before" is the loss_streak_at_close of that
-- prior closed trade (or 0 if there is none, or that prior was a winner).
------------------------------------------------------------------
trade_with_prior_close as (
    select
        b.account,
        b.trade_symbol,
        b.strategy,
        b.open_date,
        b.close_date,
        b.notional_proxy,
        b.realized_pnl,
        b.is_winner,

        (
            select ls.loss_streak_at_close
            from loss_streak_at_close ls
            where ls.account = b.account
              and ls.close_date < b.open_date
            order by ls.close_date desc, ls.stream_id desc
            limit 1
        ) as consecutive_losses_before,

        (
            select date_diff(b.open_date, max(ls.close_date), day)
            from loss_streak_at_close ls
            where ls.account = b.account
              and ls.close_date < b.open_date
              and ls.is_winner = false
        ) as days_since_last_loss
    from base b
),

------------------------------------------------------------------
-- Rolling 30d / 90d notional means + strategy 180d win rate
-- built via self-join on prior closed trades.
------------------------------------------------------------------
prior_notional as (
    select
        b.account,
        b.trade_symbol,
        b.open_date,
        avg(case
                when h.close_date >= date_sub(b.open_date, interval 30 day)
                then h.notional_proxy
            end) as acct_avg_notional_30d,
        avg(case
                when h.close_date >= date_sub(b.open_date, interval 90 day)
                then h.notional_proxy
            end) as acct_avg_notional_90d
    from base b
    left join base h
        on h.account = b.account
        and h.close_date < b.open_date
        and h.close_date >= date_sub(b.open_date, interval 90 day)
    group by 1, 2, 3
),

prior_strategy_wr as (
    select
        b.account,
        b.trade_symbol,
        if(count(h.trade_symbol) >= 10,
           safe_divide(countif(h.is_winner), count(h.trade_symbol)),
           null) as strategy_win_rate_180d,
        count(h.trade_symbol) as strategy_prior_trades_180d
    from base b
    left join base h
        on h.account = b.account
        and h.strategy = b.strategy
        and h.close_date < b.open_date
        and h.close_date >= date_sub(b.open_date, interval 180 day)
    group by 1, 2
)

select
    t.account,
    t.trade_symbol,
    t.strategy,
    t.open_date,
    t.close_date,
    t.notional_proxy,
    t.realized_pnl,
    t.is_winner,

    pn.acct_avg_notional_30d,
    pn.acct_avg_notional_90d,

    case
        when pn.acct_avg_notional_30d is not null and pn.acct_avg_notional_30d > 0
        then t.notional_proxy / pn.acct_avg_notional_30d
        else null
    end as size_vs_30d_baseline,

    case
        when pn.acct_avg_notional_90d is not null and pn.acct_avg_notional_90d > 0
        then t.notional_proxy / pn.acct_avg_notional_90d
        else null
    end as size_vs_90d_baseline,

    coalesce(t.consecutive_losses_before, 0)          as consecutive_losses_before,
    t.days_since_last_loss,
    psw.strategy_win_rate_180d,
    coalesce(psw.strategy_prior_trades_180d, 0)       as strategy_prior_trades_180d

from trade_with_prior_close t
left join prior_notional pn
    on t.account = pn.account
    and t.trade_symbol = pn.trade_symbol
left join prior_strategy_wr psw
    on t.account = psw.account
    and t.trade_symbol = psw.trade_symbol
