-- Per-broker history adapter: Empower (via SnapTrade).
--
-- Account labels whose first token is "Empower" ("Empower Account",
-- "Empower Retirement ••••1234") land here and nowhere else. The
-- catch-all (stg_broker_other_history) excludes known_brokers(), so
-- these rows cannot also appear under Schwab, Vanguard, Fidelity,
-- Alpaca, or IBKR. A hyphenated first token ("Empower-Retirement")
-- is a different slug and stays in the catch-all until a live label
-- shows that alias is required. tenant_id stays snaptrade:<uuid> —
-- the broker slug is display-only and is not a tenancy boundary.
--
-- Column mapping is the canonical trade_history seed, one column per
-- seed field. Activity types SnapTrade does not fold into the shared
-- map (a fund exchange, a share transfer) are dropped before the seed
-- write. This model does not turn those gaps into a Buy, and it does
-- not invent an opening fill or a cost basis. Option Buy / Sell on an
-- OSI contract is relabeled later in stg_history by the same
-- first-verb rule as Vanguard, and only when the row is already on
-- the tape.
--
-- Explicit projection (same names and order as broker_history_rows):
--   Account, user_id, tenant_id, Date, Action, Symbol, Description,
--   Quantity, Price, fees_and_comm, Amount, _row_seq

with empower_history as (
    {{ broker_history_rows('empower') }}
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
from empower_history
