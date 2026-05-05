/*
    Daily trading activity metrics — one row per (account, trade_date).
    Pre-aggregates trade-level data so Mirror Score computation in Flask
    reads ~30 rows (one per day) instead of thousands of individual trades.
*/

with trades as (
    select
        account,
        user_id,
        trade_date,
        underlying_symbol as symbol,
        coalesce(abs(cast(amount as float64)), 0) as position_size
    from {{ ref('stg_history') }}
    where trade_date is not null
      and instrument_type not in ('Dividend', 'Cash Event')
),

daily as (
    select
        account,
        user_id,
        trade_date,
        count(*)                    as num_trades,
        sum(position_size)          as total_volume,
        avg(position_size)          as avg_position_size,
        stddev(position_size)       as position_size_std,
        max(position_size)          as max_position_size,
        count(distinct symbol)      as unique_symbols
    from trades
    group by 1, 2, 3
),

symbol_volumes as (
    select account, user_id, trade_date, symbol, sum(position_size) as vol
    from trades
    group by 1, 2, 3, 4
),

concentration as (
    select
        account,
        user_id,
        trade_date,
        safe_divide(max(vol), sum(vol)) as top_symbol_concentration
    from symbol_volumes
    group by 1, 2, 3
),

strategies_on_date as (
    select distinct t.account, t.user_id, t.trade_date, s.strategy
    from trades t
    inner join {{ ref('int_strategy_classification') }} s
        on  t.account    = s.account
        and (t.user_id is not distinct from s.user_id)
        and t.symbol     = s.symbol
        and t.trade_date >= s.open_date
        and t.trade_date <= coalesce(s.close_date, current_date())
),

strategy_agg as (
    select
        account,
        user_id,
        trade_date,
        string_agg(distinct strategy, ',' order by strategy) as strategies_used,
        count(distinct strategy) as unique_strategies
    from strategies_on_date
    group by 1, 2, 3
),

holding_times as (
    select
        t.account,
        t.user_id,
        t.trade_date,
        avg(s.days_in_trade) as avg_days_in_trade
    from trades t
    inner join {{ ref('int_strategy_classification') }} s
        on  t.account    = s.account
        and (t.user_id is not distinct from s.user_id)
        and t.symbol     = s.symbol
        and t.trade_date >= s.open_date
        and t.trade_date <= coalesce(s.close_date, current_date())
    where s.days_in_trade > 0
    group by 1, 2, 3
)

select
    d.account,
    d.user_id,
    d.trade_date,
    d.num_trades,
    d.total_volume,
    d.avg_position_size,
    d.position_size_std,
    d.max_position_size,
    d.unique_symbols,
    c.top_symbol_concentration,
    sa.strategies_used,
    coalesce(sa.unique_strategies, 0) as unique_strategies,
    ht.avg_days_in_trade
from daily d
left join concentration c
    on d.account = c.account
    and (d.user_id is not distinct from c.user_id)
    and d.trade_date = c.trade_date
left join strategy_agg sa
    on d.account = sa.account
    and (d.user_id is not distinct from sa.user_id)
    and d.trade_date = sa.trade_date
left join holding_times ht
    on d.account = ht.account
    and (d.user_id is not distinct from ht.user_id)
    and d.trade_date = ht.trade_date
order by d.account, d.user_id, d.trade_date
