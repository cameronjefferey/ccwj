-- Per-broker account-balances catch-all: any real broker without a
-- dedicated model (slug NOT IN known_brokers()). Guarantees no balance
-- row is dropped before its broker gets its own model.
{{ broker_balances_rows(none, is_catch_all=true) }}
