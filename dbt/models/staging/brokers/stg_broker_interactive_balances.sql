-- Per-broker account-balances adapter: Interactive Brokers / IBKR (via SnapTrade).
-- Slug is 'interactive' (lowercased first token of "Interactive Brokers …").
-- Dual-source (account_balances seed + legacy current_positions cash/total
-- rows). Thin passthrough today; add IBKR-specific balance quirks HERE.
-- Unioned into stg_account_balances.
{{ broker_balances_rows('interactive') }}
