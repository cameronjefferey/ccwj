{{
    config(
        materialized='view'
    )
}}
/*
    Poor Man's Covered Call (PMCC) matched pairs.

    A PMCC consists of:
    - Long call: expiration >= 180 days from open, deep ITM (we use strike < short strike as proxy; no delta in data).
    - Short call: same underlying, expiration <= 60 days from short open.
    - Short strike > long strike.
    - Short quantity <= long quantity (contracts_sold_to_open <= contracts_bought_to_open).
    - Long was still open when short was written (status = 'Open' or long.close_date >= short.open_date).

    Returns: underlying, long/short details, net debit, time overlap.
*/
with option_contracts as (
    select * from {{ ref('int_option_contracts') }}
),

calls as (
    select
        account,
        trade_symbol,
        underlying_symbol,
        direction,
        option_expiry,
        option_strike,
        open_date,
        close_date,
        status,
        contracts_sold_to_open,
        contracts_bought_to_open,
        premium_received,
        premium_paid,
        net_cash_flow
    from option_contracts
    where option_type = 'C'
),

long_calls as (
    select *
    from calls
    where direction = 'Bought'
      and date_diff(option_expiry, open_date, day) >= 180
),

short_calls as (
    select *
    from calls
    where direction = 'Sold'
      and date_diff(option_expiry, open_date, day) <= 60
),

paired as (
    select
        short.account,
        short.underlying_symbol,
        short.trade_symbol   as short_trade_symbol,
        long.trade_symbol    as long_trade_symbol,
        -- Long details
        long.option_expiry   as long_expiry,
        long.option_strike   as long_strike,
        long.open_date       as long_open_date,
        long.close_date      as long_close_date,
        long.status          as long_status,
        long.contracts_bought_to_open as long_quantity,
        long.premium_paid    as long_premium_paid,
        long.premium_received as long_premium_received,
        long.net_cash_flow   as long_net_cash_flow,
        -- Short details
        short.option_expiry  as short_expiry,
        short.option_strike  as short_strike,
        short.open_date      as short_open_date,
        short.close_date     as short_close_date,
        short.status         as short_status,
        short.contracts_sold_to_open as short_quantity,
        short.premium_received as short_premium_received,
        short.premium_paid   as short_premium_paid,
        short.net_cash_flow  as short_net_cash_flow,
        -- Long was open when short was written
        (long.status = 'Open' or long.close_date >= short.open_date) as long_open_when_short_written
    from short_calls short
    inner join long_calls long
        on short.account = long.account
        and short.underlying_symbol = long.underlying_symbol
        and short.trade_symbol != long.trade_symbol
        and short.option_strike > long.option_strike
        and short.option_expiry < long.option_expiry
        and short.contracts_sold_to_open <= long.contracts_bought_to_open
        and (long.status = 'Open' or long.close_date >= short.open_date)
        and long.open_date <= short.open_date
)

select
    account,
    underlying_symbol,
    long_trade_symbol,
    short_trade_symbol,
    -- Long call details
    long_expiry,
    long_strike,
    long_open_date,
    long_close_date,
    long_status,
    long_quantity,
    long_premium_paid,
    long_premium_received,
    long_net_cash_flow,
    -- Short call details
    short_expiry,
    short_strike,
    short_open_date,
    short_close_date,
    short_status,
    short_quantity,
    short_premium_received,
    short_premium_paid,
    short_net_cash_flow,
    -- Net debit (positive = paid to open the spread)
    (long_premium_paid - coalesce(long_premium_received, 0))
    - (short_premium_received - coalesce(short_premium_paid, 0)) as net_debit,
    -- Time overlap
    greatest(long_open_date, short_open_date) as overlap_start,
    least(
        coalesce(long_close_date, current_date()),
        coalesce(short_close_date, current_date())
    ) as overlap_end,
    date_diff(
        least(
            coalesce(long_close_date, current_date()),
            coalesce(short_close_date, current_date())
        ),
        greatest(long_open_date, short_open_date),
        day
    ) + 1 as overlap_days
from paired
