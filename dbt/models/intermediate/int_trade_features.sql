{{ config(materialized='table') }}

/*
    Trade-grain feature table for behavioral anomaly modeling.

    One row per closed option contract and per closed equity session,
    keyed on (account, trade_symbol).  Unions int_option_contracts +
    int_equity_sessions and joins the deterministic strategy label
    from int_strategy_classification plus DTE from int_option_trade_kinds.

    Grain: one closed trade.
    Tenancy: every row has `account` — multi-tenant isolation must use it.

    Known gaps (documented, intentional):
      - No `delta_at_open` — no greeks data in the warehouse.
      - No intraday `hour_of_day` — stg_history.trade_date is DATE only.
      - `notional_proxy` is an approximation:
          options:  (contracts_opened) * option_strike * 100
          equity:   abs(net_cash_flow)  -- rough proxy for shares-at-cost
*/

with strat as (
    select
        account,
        user_id,
        trade_symbol,
        strategy
    from {{ ref('int_strategy_classification') }}
),

option_kinds as (
    select
        account,
        user_id,
        trade_symbol,
        dte_at_open,
        dte_bucket
    from {{ ref('int_option_trade_kinds') }}
),

options as (
    select
        oc.account,
        oc.user_id,
        oc.trade_symbol,
        oc.underlying_symbol,
        s.strategy,

        -- 'option' surface
        cast('option' as string) as trade_group_type,
        oc.option_type                                            as option_structure,
        case
            when oc.direction = 'Sold'   then 'short_premium'
            when oc.direction = 'Bought' then 'long_premium'
            else 'unknown'
        end                                                       as direction_signed,

        oc.open_date,
        oc.close_date,
        oc.days_in_trade                                          as holding_period_days,
        ok.dte_at_open,
        ok.dte_bucket,

        -- number of contracts actually opened on this contract
        coalesce(oc.contracts_sold_to_open, 0)
          + coalesce(oc.contracts_bought_to_open, 0)              as num_contracts,

        -- notional_proxy: contracts * strike * 100 (approx; no delta available)
        case
            when oc.option_strike is not null
            then (coalesce(oc.contracts_sold_to_open, 0)
                  + coalesce(oc.contracts_bought_to_open, 0))
                 * oc.option_strike * 100.0
            else null
        end                                                       as notional_proxy,

        oc.total_pnl                                              as realized_pnl,
        case when oc.total_pnl > 0 then true else false end       as is_winner,
        oc.status

    from {{ ref('int_option_contracts') }} oc
    left join strat s
        on oc.account = s.account
        and (oc.user_id is not distinct from s.user_id)
        and oc.trade_symbol = s.trade_symbol
    left join option_kinds ok
        on oc.account = ok.account
        and (oc.user_id is not distinct from ok.user_id)
        and oc.trade_symbol = ok.trade_symbol
    where oc.status = 'Closed'
      and oc.close_date is not null
),

equity as (
    select
        es.account,
        es.user_id,
        concat(es.symbol, '_session_', cast(es.session_id as string)) as trade_symbol,
        es.symbol                                                 as underlying_symbol,
        coalesce(s.strategy, 'Buy and Hold')                      as strategy,

        cast('equity' as string)                                  as trade_group_type,
        cast(null as string)                                      as option_structure,
        cast('equity' as string)                                  as direction_signed,

        es.open_date,
        es.last_trade_date                                        as close_date,
        es.days_held                                              as holding_period_days,
        cast(null as int64)                                       as dte_at_open,
        cast(null as string)                                      as dte_bucket,

        -- use quantity of shares as "num_contracts" analog (nullable)
        es.max_quantity_held                                      as num_contracts,

        -- equity notional proxy: abs(net_cash_flow) = cost basis spent on buys-net-sells
        abs(coalesce(es.net_cash_flow, 0))                        as notional_proxy,

        es.total_pnl                                              as realized_pnl,
        case when es.total_pnl > 0 then true else false end       as is_winner,
        es.status

    from {{ ref('int_equity_sessions') }} es
    left join strat s
        on es.account = s.account
        and (es.user_id is not distinct from s.user_id)
        and concat(es.symbol, '_session_', cast(es.session_id as string)) = s.trade_symbol
    where es.status = 'Closed'
      and es.last_trade_date is not null
),

unioned as (
    select * from options
    union all
    select * from equity
)

select
    account,
    user_id,
    trade_symbol,
    underlying_symbol,
    coalesce(strategy, 'Other')                       as strategy,
    trade_group_type,
    option_structure,
    direction_signed,

    open_date,
    close_date,
    holding_period_days,
    dte_at_open,
    dte_bucket,

    num_contracts,
    notional_proxy,
    realized_pnl,
    is_winner,
    status,

    -- Derived time features; hour_of_day is not available (DATE only) — TODO if timestamps appear.
    extract(dayofweek from open_date)                 as day_of_week,
    cast(null as int64)                               as hour_of_day

from unioned
-- Exclude trades with no meaningful size signal (guards BQML feature scaling)
where notional_proxy is not null
  and notional_proxy > 0
