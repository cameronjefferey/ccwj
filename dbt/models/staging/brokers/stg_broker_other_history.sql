-- Per-broker history catch-all: any real broker without a dedicated model
-- (slug NOT IN known_brokers()). Guarantees no history row is dropped
-- before its broker gets its own stg_broker_<slug>_history model.
{{ broker_history_rows(none, is_catch_all=true) }}
