-- Per-broker current-positions adapter: Alpaca (via SnapTrade).
-- Thin passthrough today; add Alpaca-specific snapshot quirks HERE.
-- Unioned into stg_current.
{{ broker_current_rows('alpaca') }}
