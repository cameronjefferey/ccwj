-- Per-broker account-balances adapter: Fidelity (via SnapTrade).
-- Dual-source (account_balances seed + legacy current_positions cash/total
-- rows). Thin passthrough today; add Fidelity-specific balance quirks HERE.
-- Unioned into stg_account_balances.
{{ broker_balances_rows('fidelity') }}
