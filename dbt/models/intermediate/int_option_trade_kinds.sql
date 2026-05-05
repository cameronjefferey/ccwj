{{
    config(
        materialized='view'
    )
}}
/*
    Option contracts enriched with trade "kind" attributes for grouping:

    - DTE at open (days to expiration when opened) and DTE bucket (0-7, 8-30, 31-60, 61-90, 91+)
    - Moneyness at open (ITM / ATM / OTM / Unknown) using underlying close on open_date when available
    - Strategy (from int_strategy_classification)
    - Outcome (Winner / Loser from total_pnl)
*/
with option_contracts as (
    select
        account,
        user_id,
        trade_symbol,
        underlying_symbol,
        option_expiry,
        option_strike,
        option_type,
        direction,
        status,
        open_date,
        close_date,
        total_pnl,
        net_cash_flow,
        premium_received,
        premium_paid,
        num_trades,
        days_in_trade,
        close_type
    from {{ ref('int_option_contracts') }}
),

strat as (
    select
        account,
        user_id,
        trade_symbol,
        strategy
    from {{ ref('int_strategy_classification') }}
    where trade_group_type = 'option_contract'
),

-- Underlying price on open_date for moneyness. stg_daily_prices is market
-- data (no user_id) — the join key is (account, symbol, date), not
-- per-tenant. Same close_price regardless of which user owned the symbol.
prices as (
    select account, symbol, date, close_price
    from {{ ref('stg_daily_prices') }}
),

enriched as (
    select
        oc.account,
        oc.user_id,
        oc.trade_symbol,
        oc.underlying_symbol,
        oc.option_expiry,
        oc.option_strike,
        oc.option_type,
        oc.direction,
        oc.status,
        oc.open_date,
        oc.close_date,
        oc.total_pnl,
        oc.net_cash_flow,
        oc.premium_received,
        oc.premium_paid,
        oc.num_trades,
        oc.days_in_trade,
        oc.close_type,
        coalesce(s.strategy, 'Other Option') as strategy,

        -- DTE at open and bucket
        date_diff(oc.option_expiry, oc.open_date, day) as dte_at_open,
        case
            when date_diff(oc.option_expiry, oc.open_date, day) <= 7   then '0-7 DTE'
            when date_diff(oc.option_expiry, oc.open_date, day) <= 30  then '8-30 DTE'
            when date_diff(oc.option_expiry, oc.open_date, day) <= 60  then '31-60 DTE'
            when date_diff(oc.option_expiry, oc.open_date, day) <= 90  then '61-90 DTE'
            else '91+ DTE'
        end as dte_bucket,

        -- Underlying price on open (nearest date on or before open_date if exact match missing)
        p.close_price as underlying_price_at_open,

        -- Outcome
        case when oc.total_pnl > 0 then 'Winner' else 'Loser' end as outcome

    from option_contracts oc
    left join strat s
        on oc.account = s.account
        and (oc.user_id is not distinct from s.user_id)
        and oc.trade_symbol = s.trade_symbol
    left join prices p
        on oc.account = p.account
        and oc.underlying_symbol = p.symbol
        and p.date = oc.open_date
)

select
    *,
    -- Moneyness at open: Call ITM when strike < underlying, OTM when strike > underlying; Put opposite.
    case
        when underlying_price_at_open is null then 'Unknown'
        when option_type = 'C' then
            case
                when option_strike < underlying_price_at_open * 0.98 then 'ITM'
                when option_strike > underlying_price_at_open * 1.02 then 'OTM'
                else 'ATM'
            end
        when option_type = 'P' then
            case
                when option_strike > underlying_price_at_open * 1.02 then 'ITM'
                when option_strike < underlying_price_at_open * 0.98 then 'OTM'
                else 'ATM'
            end
        else 'Unknown'
    end as moneyness_at_open,

    -- $ distance from strike to underlying price at open (positive = OTM for calls, ITM for puts)
    case
        when underlying_price_at_open is not null
        then round(option_strike - underlying_price_at_open, 2)
        else null
    end as strike_distance,

    -- P&L as % of cost basis (premium paid for bought, cost to close for sold)
    case
        when direction = 'Bought' and abs(premium_paid) > 0
        then round(total_pnl / abs(premium_paid) * 100, 1)
        when direction = 'Sold' and abs(premium_received) > 0
        then round(total_pnl / abs(premium_received) * 100, 1)
        else null
    end as pnl_pct

from enriched
