-- Per-broker current-positions adapter: Interactive Brokers / IBKR (via SnapTrade).
-- Slug is 'interactive' (lowercased first token of "Interactive Brokers …").
-- Thin passthrough today; add IBKR-specific snapshot quirks HERE.
-- Unioned into stg_current.
{{ broker_current_rows('interactive') }}
