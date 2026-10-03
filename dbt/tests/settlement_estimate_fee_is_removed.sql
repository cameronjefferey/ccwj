-- Estimated order fees must not survive on a settlement, expiry, or
-- assignment. The Oct 1 SPXW 7650/7655C cash settlement was charged
-- $12.22 on each leg (spread −$3,598.88 instead of −$3,574.44).
-- stg_history zeros that fee and restores qty × price × 100 on the
-- next rebuild. A same-day 0DTE estimate (no "as of", expiry = trade
-- date) is not in this set.

select
    tenant_id,
    trade_date,
    action,
    trade_symbol,
    fees,
    amount,
    description
from {{ ref('stg_history') }}
where regexp_contains(lower(coalesce(description, '')), r'est\. fee')
  and (
        action in ('option_expired', 'option_assigned', 'option_exercised')
        or regexp_contains(lower(coalesce(description, '')), r'\bas of\b|cash settlement')
        or (
            option_expiry is not null
            and trade_date is not null
            and option_expiry < trade_date
        )
  )
  and (
        abs(coalesce(fees, 0)) > 0.005
        or (
            quantity is not null
            and price is not null
            and abs(price) > 0
            and abs(coalesce(amount, 0)) > 0.005
            and abs(abs(amount) - abs(quantity) * abs(price) * 100) > 0.05
        )
  )
