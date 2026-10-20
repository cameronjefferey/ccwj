-- Per-broker account-balances adapter: Empower (via SnapTrade).
-- Dual-source, same as the other brokers: account_balances seed
-- (src_priority 1) plus legacy current_positions cash / account-total
-- rows (src_priority 2). stg_account_balances dedups on
-- (coalesce(tenant_id, account), row_type), keeping priority 1.
--
-- The account total is the SnapTrade reported total when the sync
-- wrote one. This model does not derive a second total from holdings
-- and does not invent cash.
--
-- Explicit projection:
--   account, user_id, tenant_id, row_type, market_value, cost_basis,
--   unrealized_pnl, unrealized_pnl_pct, percent_of_account, src_priority

with empower_balances as (
    {{ broker_balances_rows('empower') }}
)

select
    account,
    user_id,
    tenant_id,
    row_type,
    market_value,
    cost_basis,
    unrealized_pnl,
    unrealized_pnl_pct,
    percent_of_account,
    src_priority
from empower_balances
