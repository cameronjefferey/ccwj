-- Per-broker history adapter: Alpaca (via SnapTrade).
-- Thin passthrough today; add Alpaca-specific history quirks HERE so they
-- stay isolated and independently testable. Unioned into stg_history.
{{ broker_history_rows('alpaca') }}
