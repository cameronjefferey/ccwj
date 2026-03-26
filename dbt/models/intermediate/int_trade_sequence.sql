{{
    config(
        materialized='view'
    )
}}
/*
    Ordered trade sequence with previous-trade context for pattern detection.

    One row per closed trade group (from int_strategy_classification),
    ordered by close_date per account. Adds:
      - trade_sequence_num: position in the account's trade history
      - prev_trade_outcome: Winner/Loser of the prior closed trade
      - is_post_loss: whether the immediately prior trade was a loser
      - outcome: Winner/Loser for this trade

    Enables: "What's your win rate on trades opened right after a loss?"
*/

with closed as (
    select
        account,
        symbol,
        trade_symbol,
        strategy,
        trade_group_type,
        open_date,
        close_date,
        total_pnl,
        is_winner,
        days_in_trade,
        case when is_winner then 'Winner' else 'Loser' end as outcome
    from {{ ref('int_strategy_classification') }}
    where status = 'Closed'
      and close_date is not null
),

sequenced as (
    select
        *,
        row_number() over (
            partition by account order by close_date, trade_symbol
        ) as trade_sequence_num,

        lag(case when is_winner then 'Winner' else 'Loser' end) over (
            partition by account order by close_date, trade_symbol
        ) as prev_trade_outcome,

        lag(total_pnl) over (
            partition by account order by close_date, trade_symbol
        ) as prev_trade_pnl
    from closed
)

select
    account,
    symbol,
    trade_symbol,
    strategy,
    trade_group_type,
    open_date,
    close_date,
    total_pnl,
    is_winner,
    outcome,
    days_in_trade,
    trade_sequence_num,
    prev_trade_outcome,
    prev_trade_pnl,
    coalesce(prev_trade_outcome = 'Loser', false) as is_post_loss
from sequenced
