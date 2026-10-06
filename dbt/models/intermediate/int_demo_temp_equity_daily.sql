{{ config(
    materialized='table',
    enabled=demo_temp_seed_on()
) }}

/*
    TEMPORARY seed contribution to the demo account curve. Built only
    while demo_temp_seed_on() is true. It is added onto the bot mirror
    in mart_account_equity_daily. Flag off: this model is disabled, so
    the mart adds zero seed dollars. Steps: dbt/seeds/DEMO_TEMP_SEED.md.

    Reads the seed CSVs directly. Do not read stg_history for
    demo:demo-account — that frame is the mirror plus the seed, and
    using it here would add the mirror's fills a second time.

    One row per day:

      account_value = cash from the seed fills (including the deposit)
                    + seed shares × public close
                    + seed option mark

    Option mark starts at the opening cash offset (short = minus the
    credit, long = plus the debit) and fades to 0 by the close date, so
    premium shows up over the life of the contract instead of all on
    the fill. An open contract fades toward the seed snapshot mark.

    Closes are public yfinance prints. stg_daily_prices stores them once
    per (account, user_id, symbol, date); max(close) per (symbol, date)
    is the same print. This model never emits another tenant's positions.
*/

with raw_fills as (
    select
        {{ parse_seed_date('Date') }} as trade_date,
        case lower(trim(cast(Action as string)))
            when 'buy'                  then 'equity_buy'
            when 'sell'                 then 'equity_sell'
            when 'sell short'           then 'equity_sell_short'
            when 'sell to open'         then 'option_sell_to_open'
            when 'buy to close'         then 'option_buy_to_close'
            when 'buy to open'          then 'option_buy_to_open'
            when 'sell to close'        then 'option_sell_to_close'
            when 'expired'              then 'option_expired'
            when 'assigned'             then 'option_assigned'
            when 'exchange or exercise' then 'option_exercised'
            when 'deposit'              then 'cash_transfer'
            else 'other'
        end as action,
        nullif(trim(cast(Symbol as string)), '') as trade_symbol,
        nullif(trim(split(trim(cast(Symbol as string)), ' ')[safe_offset(0)]), '') as underlying_symbol,
        safe.parse_date(
            '%m/%d/%Y',
            nullif(split(trim(cast(Symbol as string)), ' ')[safe_offset(1)], '')
        ) as option_expiry,
        case
            when nullif(split(trim(cast(Symbol as string)), ' ')[safe_offset(3)], '') = 'C'
                then 'Call'
            when nullif(split(trim(cast(Symbol as string)), ' ')[safe_offset(3)], '') = 'P'
                then 'Put'
            when lower(trim(cast(Action as string))) = 'deposit'
                then 'Cash Event'
            else 'Equity'
        end as instrument_type,
        {{ parse_seed_number('Quantity') }} as quantity,
        {{ parse_seed_number('Price') }} as price,
        coalesce({{ parse_seed_number('fees_and_comm') }}, 0) as fees,
        coalesce({{ parse_seed_number('Amount') }}, 0) as amount_raw
    from {{ ref('demo_temp_history') }}
),

fills as (
    select
        trade_date,
        action,
        trade_symbol,
        underlying_symbol,
        instrument_type,
        option_expiry,
        quantity,
        case
            when action in (
                'option_buy_to_open', 'option_buy_to_close',
                'option_sell_to_open', 'option_sell_to_close'
            )
             and abs(fees) > 0.005
             and quantity is not null
             and price is not null
             and abs(abs(amount_raw) - abs(quantity) * price * 100) <= 0.05
            then case
                when action in ('option_sell_to_open', 'option_sell_to_close')
                    then abs(quantity) * price * 100 - abs(fees)
                else -(abs(quantity) * price * 100 + abs(fees))
            end
            when action in ('equity_buy', 'option_buy_to_open', 'option_buy_to_close')
                then -abs(amount_raw)
            when action in (
                'equity_sell', 'equity_sell_short',
                'option_sell_to_open', 'option_sell_to_close'
            )
                then abs(amount_raw)
            else amount_raw
        end as amount
    from raw_fills
    where trade_date is not null
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
    left join (
        select
            trim(cast(Symbol as string)) as trade_symbol,
            {{ parse_seed_number('market_value') }} as market_value
        from {{ ref('demo_temp_current') }}
    ) sn
        on sn.trade_symbol = o.trade_symbol
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
