{{
    config(
        materialized='view'
    )
}}
/*
    Roll detection: identifies when a trader closed one option and opened
    another on the same underlying + option type + direction within 1
    business day, with a different expiry.

    Tightened from the naive 3-day window:
      - Same direction (both Sold or both Bought) — you don't "roll" a
        sold call into a bought call
      - 0-1 day gap (same day or next day) — real rolls happen immediately
      - Different expiry — rolling to the same expiry isn't a roll, it's
        a separate trade

    Excludes contracts that expired or were assigned (those aren't
    intentional rolls).
*/

with closers as (
    select
        account,
        trade_symbol          as old_trade_symbol,
        underlying_symbol,
        option_type,
        direction             as old_direction,
        option_expiry         as old_expiry,
        option_strike         as old_strike,
        open_date             as old_open_date,
        close_date            as old_close_date,
        total_pnl             as old_pnl,
        premium_received      as old_premium_received,
        days_in_trade         as old_days_in_trade,
        close_type            as old_close_type,
        date_diff(option_expiry, close_date, day) as dte_at_close
    from {{ ref('int_option_contracts') }}
    where status = 'Closed'
      and close_type = 'Closed'
      and close_date is not null
),

openers as (
    select
        oc.account,
        oc.trade_symbol       as new_trade_symbol,
        oc.underlying_symbol,
        oc.option_type,
        oc.direction          as new_direction,
        oc.option_expiry      as new_expiry,
        oc.option_strike      as new_strike,
        oc.open_date          as new_open_date,
        oc.status             as new_status,
        oc.total_pnl          as new_pnl,
        oc.premium_received   as new_premium_received,
        coalesce(tk.outcome, case when oc.total_pnl > 0 then 'Winner' else 'Loser' end) as new_outcome
    from {{ ref('int_option_contracts') }} oc
    left join {{ ref('int_option_trade_kinds') }} tk
        on oc.account = tk.account
        and oc.trade_symbol = tk.trade_symbol
),

candidates as (
    select
        c.*,
        o.new_trade_symbol,
        o.new_direction,
        o.new_expiry,
        o.new_strike,
        o.new_open_date,
        o.new_status,
        o.new_pnl,
        o.new_premium_received,
        o.new_outcome,
        date_diff(o.new_open_date, c.old_close_date, day) as days_between,
        row_number() over (
            partition by c.account, c.old_trade_symbol
            order by abs(date_diff(o.new_open_date, c.old_close_date, day)),
                     o.new_open_date
        ) as match_rank
    from closers c
    join openers o
        on c.account = o.account
        and c.underlying_symbol = o.underlying_symbol
        and c.option_type = o.option_type
        -- Same direction: both sold or both bought
        and c.old_direction = o.new_direction
        -- Different contract
        and o.new_trade_symbol != c.old_trade_symbol
        -- Different expiry (rolling out)
        and o.new_expiry != c.old_expiry
        -- Tight window: same day or next business day
        and date_diff(o.new_open_date, c.old_close_date, day) between 0 and 1
)

select
    account,
    underlying_symbol,
    option_type,

    old_trade_symbol,
    old_direction,
    old_expiry,
    old_strike,
    old_open_date,
    old_close_date,
    old_pnl,
    old_premium_received,
    old_days_in_trade,
    dte_at_close as dte_at_roll,

    new_trade_symbol,
    new_direction,
    new_expiry,
    new_strike,
    new_open_date,
    new_status  as new_contract_status,
    new_pnl     as new_contract_pnl,
    new_outcome as new_contract_outcome,

    days_between,
    round(new_strike - old_strike, 2) as strike_change,
    round(
        coalesce(new_premium_received, 0)
        + coalesce(old_pnl, 0)
        - coalesce(old_premium_received, 0),
        2
    ) as net_roll_credit

from candidates
where match_rank = 1
