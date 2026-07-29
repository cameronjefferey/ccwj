/*
    Alpaca option activities omit open/close metadata and use descriptions
    such as "BUY FILL at ...". When recent_orders has aged out, both sides of
    a completed round trip arrive as "to Open". A contract whose buy and sell
    quantities exactly offset, has no explicit terminal event, and is absent
    from the current snapshot must close on its final fill date — not remain
    Open until expiry.
*/

with activity_contracts as (
    select
        tenant_id,
        account,
        user_id,
        trade_symbol,
        max(trade_date) as expected_close_date,
        sum(amount) as expected_total_pnl
    from {{ ref('stg_history') }}
    where {{ broker_slug_from_account('account') }} = 'alpaca'
      and instrument_type in ('Call', 'Put')
    group by 1, 2, 3, 4
    having countif(
        regexp_contains(
            coalesce(description, ''),
            r'(?i) (PARTIAL_)?FILL at '
        )
    ) > 0
       and countif(action in (
            'option_buy_to_close', 'option_sell_to_close',
            'option_expired', 'option_assigned', 'option_exercised'
       )) = 0
       and sum(case
            when action in ('option_buy_to_open', 'option_buy_to_close')
            then quantity else 0
       end) > 0
       and sum(case
            when action in ('option_sell_to_open', 'option_sell_to_close')
            then quantity else 0
       end) > 0
       and abs(
            sum(case
                when action in ('option_buy_to_open', 'option_buy_to_close')
                then quantity else 0
            end)
            - sum(case
                when action in ('option_sell_to_open', 'option_sell_to_close')
                then quantity else 0
            end)
       ) < 1e-9
),

expected_closed as (
    select a.*
    from activity_contracts a
    where not exists (
        select 1
        from {{ ref('stg_current') }} c
        where c.instrument_type in ('Call', 'Put')
          and (c.tenant_id is not distinct from a.tenant_id)
          and c.account = a.account
          and (c.user_id is not distinct from a.user_id)
          and c.trade_symbol = a.trade_symbol
    )
)

select
    e.tenant_id,
    e.account,
    e.user_id,
    e.trade_symbol,
    e.expected_close_date,
    o.close_date as actual_close_date,
    e.expected_total_pnl,
    o.total_pnl as actual_total_pnl,
    o.status,
    o.close_type
from expected_closed e
left join {{ ref('int_option_contracts') }} o
    on (o.tenant_id is not distinct from e.tenant_id)
    and o.account = e.account
    and (o.user_id is not distinct from e.user_id)
    and o.trade_symbol = e.trade_symbol
where o.trade_symbol is null
   or o.status != 'Closed'
   or o.close_type != 'Closed'
   or o.close_date != e.expected_close_date
   or abs(o.total_pnl - e.expected_total_pnl) > 0.01
