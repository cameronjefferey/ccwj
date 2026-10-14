-- Per-broker account-balances adapter: Vanguard (via SnapTrade).
-- Dual-source, same as the other brokers: account_balances seed
-- (src_priority 1) plus legacy current_positions cash / account-total
-- rows (src_priority 2). stg_account_balances dedups on
-- (coalesce(tenant_id, account), row_type), keeping priority 1.
--
-- Explicit projection:
--   account, user_id, tenant_id, row_type, market_value, cost_basis,
--   unrealized_pnl, unrealized_pnl_pct, percent_of_account, src_priority

with vanguard_balances as (
    {{ broker_balances_rows('vanguard') }}
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
from vanguard_balances
