-- Singular test: every Equity row in `int_enriched_current` must satisfy
-- ``abs(quantity * current_price - market_value) <= $0.01``.
--
-- This invariant makes "Strategy Breakdown total" (which uses market_value)
-- and any per-share-times-quantity displays (Today Strip, Open Positions
-- card, position cards) reconcile. It would have caught the
-- "averagePrice -> Price" Schwab Connect bug from May 2026 the day it
-- shipped: that bug made every synced equity row land with
-- ``current_price == cost_per_share``, which is correct for new positions
-- (mv == cb at open) but drifts arbitrarily as the position appreciates,
-- silently hiding unrealized P&L on every UI surface that derived position
-- value as ``qty * current_price``.
--
-- Scope: equity only — option contracts carry a contract multiplier
-- (typically 100x) that makes the equality false by design.
--
-- Tolerance: $0.01 absorbs sequential rounding (Schwab quotes to 0.0001;
-- yfinance to 0.01; we round to 4dp at write time). Anything wider is a
-- structural bug.
select
    account,
    user_id,
    trade_symbol,
    quantity,
    current_price,
    market_value,
    quantity * current_price as derived_market_value,
    abs(quantity * current_price - market_value) as drift
from {{ ref('int_enriched_current') }}
where instrument_type = 'Equity'
  and quantity is not null
  and quantity != 0
  and current_price is not null
  and current_price > 0
  and market_value is not null
  and market_value > 0
  and abs(quantity * current_price - market_value) > 0.01
