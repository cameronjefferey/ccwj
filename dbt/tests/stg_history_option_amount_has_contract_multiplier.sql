-- Options are quoted per-share but a contract controls 100 shares, so an
-- option trade's cash ``amount`` must be the GROSS premium
-- (quantity * price * 100), never the per-share value.
--
-- Alpaca's SnapTrade ``activities`` feed ships option amounts WITHOUT the 100x
-- multiplier (2 contracts @ $2.59 → $5.18 instead of $518). Left uncorrected,
-- closed-option realized P&L collapsed toward $0 and the account cash
-- reconciliation was off by ~$14.7k (2026-07-16). stg_broker_alpaca_history
-- recomputes these fills as quantity*price*100; this test guards that any
-- future broker/feed shipping per-share option amounts is caught instead of
-- silently understating option P&L 100x.
--
-- Flags option open/close rows whose |amount| is far below the expected gross
-- premium (< 10x per-share ≈ 1/10th of the 100x gross). Expiries (amount 0)
-- and rows without a usable price are excluded.

select
    tenant_id,
    trade_symbol,
    trade_date,
    action,
    quantity,
    price,
    amount,
    round(quantity * price * 100, 2) as expected_gross
from {{ ref('stg_history') }}
where tenant_id is not null
  and instrument_type in ('Call', 'Put')
  and action in (
      'option_buy_to_open', 'option_sell_to_open',
      'option_buy_to_close', 'option_sell_to_close'
  )
  and quantity > 0
  and price > 0
  and abs(amount) < quantity * price * 10
