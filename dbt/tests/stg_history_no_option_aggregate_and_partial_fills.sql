/*
    No Alpaca option trade group may retain both an orders-feed aggregate row
    and its activities-feed execution rows.

    recent_orders reports one aggregate quantity while activities reports the
    same order as N fills (for example, 11 contracts versus 7 + 4). Matching
    on exact quantity therefore leaves both copies in stg_history and doubles
    contract counts and premiums. The Alpaca adapter makes the aggregate
    authoritative for (tenant_id, date, contract, buy/sell side).
*/

with flagged as (
    select
        tenant_id,
        trade_date,
        trade_symbol,
        case
            when action in ('option_buy_to_open', 'option_buy_to_close') then 'buy'
            when action in ('option_sell_to_open', 'option_sell_to_close') then 'sell'
        end as trade_side,
        regexp_contains(description, r'(?i) (PARTIAL_)?FILL at ') as is_partial_fill
    from {{ ref('stg_history') }}
    where tenant_id is not null
      and instrument_type in ('Call', 'Put')
      and action in (
          'option_buy_to_open', 'option_buy_to_close',
          'option_sell_to_open', 'option_sell_to_close'
      )
)

select
    tenant_id,
    trade_date,
    trade_symbol,
    trade_side,
    countif(is_partial_fill) as n_partial_fill,
    countif(not is_partial_fill) as n_aggregate
from flagged
group by tenant_id, trade_date, trade_symbol, trade_side
having countif(is_partial_fill) > 0
   and countif(not is_partial_fill) > 0
