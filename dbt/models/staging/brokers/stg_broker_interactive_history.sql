-- Per-broker history adapter: Interactive Brokers / IBKR (via SnapTrade).
-- Slug is 'interactive' because the account label is "Interactive Brokers …"
-- and the broker slug is its lowercased first token. Thin passthrough today;
-- add IBKR-specific history quirks HERE. Unioned into stg_history.
{{ broker_history_rows('interactive') }}
