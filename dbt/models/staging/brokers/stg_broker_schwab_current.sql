-- Per-broker current-positions adapter: Schwab (via SnapTrade).
-- Thin passthrough today; add Schwab-specific snapshot quirks HERE.
-- Unioned into stg_current.
{{ broker_current_rows('schwab') }}
