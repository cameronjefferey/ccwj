{{ config(
    materialized='table',
    enabled=var('demo_temp_seed', false)
) }}

/*
    TEMPORARY demo account curve. On only while var('demo_temp_seed')
    is true. Delete this file when the trading bot should drive the
    demo again — steps in dbt/seeds/DEMO_TEMP_SEED.md.

    One row per day for demo:demo-account:

      account_value = cash from the temp-seed fills
                    + shares × public close (average cost if no close yet)
                    + option mark

    Option mark starts at the opening cash offset (short = minus the
    credit, long = plus the debit) and fades to 0 by the close date, so
    premium shows up over the life of the contract instead of all on
    the fill. An open contract fades toward today's snapshot mark.

    Closes are public yfinance prints. stg_daily_prices stores them once
    per (account, user_id, symbol, date); max(close) per (symbol, date)
    is the same print. This model never emits another tenant's positions.
*/

with fills as (
    select
        trade_date,
        action,
        trade_symbol,
        underlying_symbol,
        instrument_type,
        option_expiry,
        quantity,
        amount
    from {{ ref('stg_history') }}
    where tenant_id = 'demo:demo-account'
      and trade_date is not null
),

bounds as (
    select min(trade_date) as start_date, current_date() as end_date
    from fills
),

spine as (
    select as_of
    from bounds,
    unnest(generate_date_array(start_date, end_date)) as as_of
),

cash_events as (
    select trade_date as as_of, sum(amount) as cash_delta
    from fills
    group by 1
),

cash_by_day as (
    select
        s.as_of,
        sum(coalesce(e.cash_delta, 0)) over (order by s.as_of) as cash_value
    from spine s
    left join cash_events e using (as_of)
),

share_events as (
    select
        trade_date as as_of,
        underlying_symbol as symbol,
        sum(case
            when action = 'equity_buy' then quantity
            when action in ('equity_sell', 'equity_sell_short') then -quantity
            else 0
        end) as qty_delta
    from fills
    where action in ('equity_buy', 'equity_sell', 'equity_sell_short')
    group by 1, 2
),

share_grid as (
    select sym.symbol, s.as_of
    from (select distinct symbol from share_events) sym
    cross join spine s
),

share_qty as (
    select
        g.symbol,
        g.as_of,
        sum(coalesce(e.qty_delta, 0)) over (
            partition by g.symbol order by g.as_of
        ) as qty
    from share_grid g
    left join share_events e
        on e.symbol = g.symbol and e.as_of = g.as_of
),

closes as (
    select symbol, date as as_of, max(close_price) as close_price
    from {{ ref('stg_daily_prices') }}
    where symbol in (select symbol from share_events)
    group by 1, 2
),

avg_cost as (
    select
        underlying_symbol as symbol,
        safe_divide(
            sum(case when action = 'equity_buy' then -amount else 0 end),
            nullif(sum(case when action = 'equity_buy' then quantity else 0 end), 0)
        ) as avg_cost
    from fills
    where action = 'equity_buy'
    group by 1
),

share_marked as (
    select
        q.symbol,
        q.as_of,
        q.qty,
        coalesce(
            last_value(c.close_price ignore nulls) over (
                partition by q.symbol order by q.as_of
                rows between unbounded preceding and current row
            ),
            a.avg_cost
        ) as mark
    from share_qty q
    left join closes c
        on c.symbol = q.symbol and c.as_of = q.as_of
    left join avg_cost a
        on a.symbol = q.symbol
),

equity_mv as (
    select as_of, sum(qty * mark) as equity_mv
    from share_marked
    where qty > 0 and mark is not null
    group by 1
),

opt as (
    select
        trade_symbol,
        min(if(action in ('option_sell_to_open', 'option_buy_to_open'), trade_date, null)) as open_date,
        coalesce(
            min(if(
                action in (
                    'option_buy_to_close', 'option_sell_to_close',
                    'option_expired', 'option_assigned', 'option_exercised'
                ),
                trade_date,
                null
            )),
            if(max(option_expiry) < current_date(), max(option_expiry), null)
        ) as close_date,
        sum(if(action = 'option_sell_to_open', amount, 0)) as short_credit,
        sum(if(action = 'option_buy_to_open', amount, 0)) as long_debit,
        sum(if(action = 'option_sell_to_open', quantity, 0)) as sold_qty
    from fills
    where instrument_type in ('Call', 'Put')
      and trade_symbol is not null
    group by 1
),

opt_open as (
    select
        o.trade_symbol,
        o.open_date,
        o.close_date,
        case
            when o.sold_qty > 0 then -o.short_credit
            else -o.long_debit
        end as open_mark,
        sn.market_value as snap_mv
    from opt o
    left join {{ ref('stg_current') }} sn
        on sn.tenant_id = 'demo:demo-account'
       and sn.trade_symbol = o.trade_symbol
       and sn.instrument_type in ('Call', 'Put')
    where o.open_date is not null
),

opt_days as (
    select
        o.trade_symbol,
        s.as_of,
        o.open_date,
        o.close_date,
        o.open_mark,
        o.snap_mv
    from opt_open o
    join spine s
        on s.as_of >= o.open_date
),

option_mv as (
    select
        as_of,
        sum(
            case
                when close_date is not null and as_of >= close_date then 0
                when close_date is not null and close_date <= open_date then 0
                when close_date is not null then
                    open_mark * (
                        1 - safe_divide(
                            date_diff(as_of, open_date, day),
                            greatest(date_diff(close_date, open_date, day), 1)
                        )
                    )
                else
                    open_mark + (
                        coalesce(snap_mv, 0) - open_mark
                    ) * safe_divide(
                        date_diff(as_of, open_date, day),
                        greatest(date_diff(current_date(), open_date, day), 1)
                    )
            end
        ) as option_mv
    from opt_days
    group by 1
)

select
    s.as_of,
    c.cash_value,
    c.cash_value
        + coalesce(e.equity_mv, 0)
        + coalesce(o.option_mv, 0) as account_value
from spine s
join cash_by_day c using (as_of)
left join equity_mv e using (as_of)
left join option_mv o using (as_of)
