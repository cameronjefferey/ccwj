-- Per-broker history adapter: Vanguard (via SnapTrade).
--
-- Account labels whose first token is "Vanguard" ("Vanguard Account",
-- "Vanguard ••••1234") land here and nowhere else. The catch-all
-- (stg_broker_other_history) excludes known_brokers(), so these rows
-- cannot also appear under Schwab, Fidelity, Alpaca, or IBKR.
-- tenant_id stays snaptrade:<uuid> — the broker slug is display-only
-- and is not a tenancy boundary.
--
-- Column mapping is the canonical trade_history seed, one column per
-- seed field. Vanguard payload quirks (string tickers, dividend
-- reinvestment that also buys shares, cash-equivalent sweeps, a null
-- NAV) are normalized in app/snaptrade_normalize.py before the seed
-- write, so this model does not re-interpret broker JSON.
--
-- Explicit projection (same names and order as broker_history_rows):
--   Account, user_id, tenant_id, Date, Action, Symbol, Description,
--   Quantity, Price, fees_and_comm, Amount, _row_seq

with vanguard_history as (
    {{ broker_history_rows('vanguard') }}
)

select
    Account,
    user_id,
    tenant_id,
    Date,
    Action,
    Symbol,
    Description,
    Quantity,
    Price,
    fees_and_comm,
    Amount,
    _row_seq
from vanguard_history
