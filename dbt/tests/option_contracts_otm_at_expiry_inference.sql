{#
    OTM-at-expiry auto-close — same-day inference invariant.

    On the option's expiry day itself, the existing calendar-truth rule
    in int_option_contracts (``option_expiry < current_date()``) is still
    false (it requires the next calendar day). Without an extra rule the
    contract stays Open until the next dbt build runs after midnight UTC,
    or the Monday broker sync ships an explicit ``option_expired`` event.

    The OTM-at-expiry inference closes that gap: when stg_daily_prices
    has booked the underlying's close on expiry day AND the close is
    strictly OTM relative to the strike, the contract must be marked
    Closed in the SAME build that picked up today's price.

    This test fails for any contract where:
      - option_expiry = current_date()
      - stg_daily_prices has a row for (underlying_symbol, option_expiry)
        in the same tenant
      - the close price is strictly OTM (call below strike OR put above
        strike)
      - status is still 'Open'

    Pre-fix: a Friday-expiry short call whose underlying closes below
    the strike read as Open all weekend, with the broker snapshot's
    stale cost-to-close fed into the live override — chart and Hero KPI
    both wrong by ~ the contract's market_value until Monday's sync.
#}

with contracts as (
    select * from {{ ref('int_option_contracts') }}
),

prices as (
    select * from {{ ref('stg_daily_prices') }}
)

select
    c.account,
    c.user_id,
    c.trade_symbol,
    c.underlying_symbol,
    c.option_expiry,
    c.option_strike,
    c.option_type,
    c.status,
    p.close_price as underlying_close_on_expiry
from contracts c
join prices p
    on c.account            = p.account
    and (c.user_id is not distinct from p.user_id)
    and c.underlying_symbol = p.symbol
    and c.option_expiry     = p.date
where c.option_expiry = current_date()
  and c.option_strike is not null
  and p.close_price is not null
  and c.status = 'Open'
  and (
      (c.option_type = 'C' and p.close_price < c.option_strike)
      or (c.option_type = 'P' and p.close_price > c.option_strike)
  )
