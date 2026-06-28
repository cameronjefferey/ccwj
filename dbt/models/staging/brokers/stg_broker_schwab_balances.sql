-- Per-broker account-balances adapter: Schwab (via SnapTrade).
-- Dual-source (account_balances seed + legacy current_positions cash/total
-- rows). Thin passthrough today; add Schwab-specific balance quirks HERE.
-- Unioned into stg_account_balances.
{{ broker_balances_rows('schwab') }}
