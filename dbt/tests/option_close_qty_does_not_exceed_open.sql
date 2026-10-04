/*
    Closing quantity on one option contract must not exceed the quantity
    opened, per tenant.

    An estimated-fee buy-to-close that escaped dedup (ASTS 58C: 8
    opened, 8 real buy-to-close + 8 estimated-fee buy-to-close) reads
    as 16 closed. The shadow drop in stg_history removes that extra
    close. This test is the backstop: any contract whose closes still
    outrun its opens fails the next warehouse build. The failing rows
    (tenant + contract) are the list of remaining over-closes.

    Positions that start mid-window have no opening fill in the seed
    (opened = 0). Those are excluded so a long-held name does not
    false-positive. A contract that was fully closed and also has an
    expiry row of the same size fails here — that is a real over-close.
*/

with legs as (
    select
        tenant_id,
        underlying_symbol,
        option_expiry,
        option_strike,
        option_type,
        sum(case
            when action in ('option_sell_to_open', 'option_buy_to_open')
                then abs(quantity)
            else 0
        end) as opened,
        sum(case
            when action in (
                'option_buy_to_close', 'option_sell_to_close',
                'option_expired', 'option_assigned', 'option_exercised'
            )
                then abs(quantity)
            else 0
        end) as closed
    from {{ ref('stg_history') }}
    where tenant_id is not null
      and instrument_type in ('Call', 'Put')
      and option_expiry is not null
      and option_strike is not null
      and option_type is not null
    group by
        tenant_id,
        underlying_symbol,
        option_expiry,
        option_strike,
        option_type
)

select
    tenant_id,
    underlying_symbol,
    option_expiry,
    option_strike,
    option_type,
    opened,
    closed
from legs
where opened > 0.01
  and closed > opened + 0.01
