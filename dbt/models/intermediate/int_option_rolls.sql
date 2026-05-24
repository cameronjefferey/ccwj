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

    Underlying close on close_date joins stg_daily_prices so aggregates can
    separate “defensive” timing (near expiry / ITM short premium vs stock)
    from win-rate-on-next-contract framing.
*/

with closers as (
    select
        account,
        user_id,
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
        oc.user_id,
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
        and (oc.user_id is not distinct from tk.user_id)
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
            partition by c.account, c.user_id, c.old_trade_symbol
            order by abs(date_diff(o.new_open_date, c.old_close_date, day)),
                     o.new_open_date
        ) as match_rank
    from closers c
    join openers o
        on c.account = o.account
        and (c.user_id is not distinct from o.user_id)
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
    c.account,
    c.user_id,
    -- v2 tenant_id passthrough (see docs/V2_TENANT_KEY_DESIGN.md).
    dba.tenant_id,
    c.underlying_symbol,
    c.option_type,

    c.old_trade_symbol,
    c.old_direction,
    c.old_expiry,
    c.old_strike,
    c.old_open_date,
    c.old_close_date,
    c.old_pnl,
    c.old_premium_received,
    c.old_days_in_trade,
    c.dte_at_close as dte_at_roll,

    c.new_trade_symbol,
    c.new_direction,
    c.new_expiry,
    c.new_strike,
    c.new_open_date,
    c.new_status  as new_contract_status,
    c.new_pnl     as new_contract_pnl,
    c.new_outcome as new_contract_outcome,

    c.days_between,
    round(c.new_strike - c.old_strike, 2) as strike_change,
    round(
        coalesce(c.new_premium_received, 0)
        + coalesce(c.old_pnl, 0)
        - coalesce(c.old_premium_received, 0),
        2
    ) as net_roll_credit,

    dp.close_price as underlying_close_on_roll_date,
    (coalesce(c.old_pnl, 0) < 0) as closed_leg_was_loss,
    case
        when c.old_direction = 'Sold' and c.option_type = 'Call' and dp.close_price is not null
            then dp.close_price >= c.old_strike
        when c.old_direction = 'Sold' and c.option_type = 'Put' and dp.close_price is not null
            then dp.close_price <= c.old_strike
        else null
    end as sold_short_itm_at_roll

from candidates c
left join {{ ref('stg_daily_prices') }} dp
    on c.account = dp.account
    and (c.user_id is not distinct from dp.user_id)
    and c.underlying_symbol = dp.symbol
    and c.old_close_date = dp.date
left join {{ ref('dim_broker_tenants') }} dba
    on c.account = dba.account_name
    and (c.user_id is not distinct from dba.user_id)
where c.match_rank = 1
