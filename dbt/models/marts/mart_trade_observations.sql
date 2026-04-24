{{ config(materialized='table') }}

/*
    mart_trade_observations — pure-SQL behavioral observations.

    One row per closed trade (account, trade_symbol), enriched with:
      - all features from int_trade_features
      - all baselines from int_trade_baselines
      - a neutral, evidence-only `observation_text`

    Philosophy (see AGENTS.md):
      - Present evidence, do not accuse.
      - No severity labels ('HIGH' / 'MEDIUM').  No psychological labeling.
      - observation_text is null unless evidence is strong AND baseline is
        well-supported (>= 10 prior strategy trades).

    The BQML layer downstream uses the same grain and features but
    contributes only an `anomaly_score` used for ordering.  The text
    itself is deterministic and traceable back to trade-level data.
*/

with joined as (
    select
        f.account,
        f.trade_symbol,
        f.underlying_symbol,
        f.strategy,
        f.trade_group_type,
        f.option_structure,
        f.direction_signed,

        f.open_date,
        f.close_date,
        f.holding_period_days,
        f.dte_at_open,
        f.dte_bucket,

        f.num_contracts,
        f.notional_proxy,
        f.realized_pnl,
        f.is_winner,
        f.status,
        f.day_of_week,
        f.hour_of_day,

        b.acct_avg_notional_30d,
        b.acct_avg_notional_90d,
        b.size_vs_30d_baseline,
        b.size_vs_90d_baseline,
        b.consecutive_losses_before,
        b.days_since_last_loss,
        b.strategy_win_rate_180d,
        b.strategy_prior_trades_180d
    from {{ ref('int_trade_features') }} f
    left join {{ ref('int_trade_baselines') }} b
        on f.account = b.account
        and f.trade_symbol = b.trade_symbol
),

-- Build neutral observation text.  Only emit a non-null text when the
-- evidence is strong and the strategy has enough history to be meaningful.
with_text as (
    select
        j.*,

        -- Deterministic observation: size noticeably above 30d baseline AND
        -- historical strategy win rate in this account is below 50%, with
        -- at least 10 prior same-strategy closed trades supporting the rate.
        case
            when j.size_vs_30d_baseline is not null
                 and j.size_vs_30d_baseline >= 1.5
                 and j.strategy_win_rate_180d is not null
                 and j.strategy_win_rate_180d < 0.50
                 and coalesce(j.strategy_prior_trades_180d, 0) >= 10
            then concat(
                'Opened a ', j.strategy,
                ' trade at ', cast(round(j.size_vs_30d_baseline, 1) as string),
                'x your 30-day average position size. ',
                'Trailing-180d win rate in ', j.strategy, ' for this account: ',
                cast(round(j.strategy_win_rate_180d * 100, 0) as string), '% over ',
                cast(j.strategy_prior_trades_180d as string), ' closed trades.'
            )

            -- A milder neutral observation: size >= 2x 30d baseline, regardless of WR,
            -- when there is at least some notional history.
            when j.size_vs_30d_baseline is not null
                 and j.size_vs_30d_baseline >= 2.0
                 and j.acct_avg_notional_30d is not null
            then concat(
                'Opened a ', j.strategy,
                ' trade at ', cast(round(j.size_vs_30d_baseline, 1) as string),
                'x your 30-day average position size.'
            )

            else null
        end as observation_text
    from joined j
)

select * from with_text
