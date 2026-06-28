-- Per-broker history adapter: Fidelity (via SnapTrade).
-- Thin passthrough today; add Fidelity-specific history quirks HERE so they
-- stay isolated and independently testable. Unioned into stg_history.
{{ broker_history_rows('fidelity') }}
