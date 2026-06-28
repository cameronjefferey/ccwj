-- Per-broker current-positions adapter: Fidelity (via SnapTrade).
-- Thin passthrough today; add Fidelity-specific snapshot quirks HERE.
-- Unioned into stg_current.
{{ broker_current_rows('fidelity') }}
