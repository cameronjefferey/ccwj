-- Per-broker current-positions catch-all: any real broker without a
-- dedicated model (slug NOT IN known_brokers()). Guarantees no snapshot
-- row is dropped before its broker gets its own model.
{{ broker_current_rows(none, is_catch_all=true) }}
