-- Per-broker history adapter: Fidelity (via SnapTrade).
--
-- Account labels whose first token is "Fidelity" ("Fidelity Account",
-- "Fidelity ••••5678", "Fidelity Investments ••••5678",
-- "Fidelity NetBenefits ••••5678") land here and nowhere else. The
-- catch-all (stg_broker_other_history) excludes known_brokers(), so
-- these rows cannot also appear under Schwab, Vanguard, Alpaca, or IBKR.
-- tenant_id stays snaptrade:<uuid> — the broker slug is display-only
-- and is not a tenancy boundary.
--
-- Column mapping is the canonical trade_history seed, one column per
-- seed field. Fidelity payload quirks (string or object tickers, mutual
-- funds, SPAXX/FDRXX sweeps when cash_equivalent is true, fractional
-- shares, dividend reinvestment) are normalized in
-- app/snaptrade_normalize.py before the seed write, so this model does
-- not re-interpret broker JSON.
--
-- Explicit projection (same names and order as broker_history_rows):
--   Account, user_id, tenant_id, Date, Action, Symbol, Description,
--   Quantity, Price, fees_and_comm, Amount, _row_seq

with fidelity_history as (
    {{ broker_history_rows('fidelity') }}
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
from fidelity_history
