-- Per-broker history adapter: Schwab (via SnapTrade).
-- Thin passthrough today; add Schwab-specific history quirks HERE so they
-- stay isolated and independently testable. Unioned into stg_history.
{{ broker_history_rows('schwab') }}
