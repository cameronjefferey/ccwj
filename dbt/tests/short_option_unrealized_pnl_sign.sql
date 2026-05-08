-- Singular test: every SHORT option (qty < 0) in stg_current must have
-- ``unrealized_pnl = market_value + cost_basis`` (the trader's true cash
-- P&L: premium received plus the cost-to-close liability), NOT Schwab's
-- ``gain_or_loss_dollat`` value (which is ``market_value - cost_basis``
-- and inverts the sign for shorts).
--
-- See ``stg_current.sql`` (the ``cleaned`` CTE) for the long-form
-- writeup of the bug. If this test fails, someone reverted that
-- override or added a new code path that bypasses it.
select
    account,
    user_id,
    trade_symbol,
    quantity,
    market_value,
    cost_basis,
    unrealized_pnl,
    market_value + cost_basis as expected_unrealized_pnl,
    unrealized_pnl - (market_value + cost_basis) as drift
from {{ ref('stg_current') }}
where instrument_type in ('Call', 'Put')
  and quantity is not null
  and quantity < 0
  and market_value is not null
  and cost_basis is not null
  -- 1-cent tolerance for floating-point round-trips
  and abs(unrealized_pnl - (market_value + cost_basis)) > 0.01
