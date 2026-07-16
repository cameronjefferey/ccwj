-- Per-broker history adapter: Alpaca (via SnapTrade).
-- Add Alpaca-specific history quirks HERE so they stay isolated and
-- independently testable. Unioned into stg_history.
--
-- ── Alpaca duplicate-fill quirk (2026-07-16) ─────────────────────────────
-- The SAME order arrives from TWO SnapTrade feeds at DIFFERENT granularity,
-- so the shared cross-source dedup in app/upload._dedup_history_rows (which
-- keys on Date/Action/Symbol/Quantity/Price) can't collapse them:
--
--   * recent_orders  → ONE aggregate row at the order's filled_quantity,
--     Description = company name ("Kaiser Aluminum Corporation"). This is
--     authoritative (SnapTrade's own filled_quantity for the order).
--   * activities     → N per-execution partial fills, Description like
--     "KALU BUY FILL at 162.12" / "... PARTIAL_FILL at 162.14". Alpaca paper's
--     activities feed chronically LAGS / under-reports partials (e.g. reports
--     20 of a 21-share order), so summing the partials is short of the truth.
--
-- Keeping BOTH doubled every day-traded equity's cost basis (KALU: 52 shares /
-- $8,430 instead of 26 / $4,215), producing a ~-$67k phantom UNREALIZED loss
-- for the account vs the broker snapshot's ~-$1.2k — the position/accounts
-- pages showed total return -$77k when the real figure was ~-$15k on a $100k
-- paper account. See broker-sync-safety "Bugs we've shipped" 2026-07-16.
--
-- Fix: for each EQUITY trade group (tenant_id, Date, Symbol, Action — the
-- equity lane is Action 'Buy'/'Sell'; option actions carry an open/close
-- suffix and are a different lane), if an orders-aggregate row exists, keep
-- ONLY the aggregate row(s) and drop the activities partial-fill rows. When no
-- aggregate exists (the order aged out of recent_orders), keep the activities
-- fills. Verified 2026-07-16 to reconcile net shares to the broker snapshot for
-- ALL 54 of the account's equity symbols. Options pass through untouched (they
-- have a separate amount-multiplier quirk, not a duplication).

with alpaca_rows as (
    {{ broker_history_rows('alpaca') }}
),

flagged as (
    select
        *,
        -- Alpaca activities partial-fill signature (broker-original wording).
        -- orders_to_history_df writes the plain company name, never this.
        regexp_contains(Description, r'(?i) (PARTIAL_)?FILL at ') as _is_partial_fill,
        -- Equity trade lane. Option actions are 'Buy to Open' / 'Sell to Close'
        -- / … and expiries are 'option_expired'-style — none equal 'Buy'/'Sell'.
        (Action in ('Buy', 'Sell')) as _is_equity_trade
    from alpaca_rows
),

grouped as (
    select
        *,
        countif(_is_equity_trade and not _is_partial_fill)
            over (partition by tenant_id, Date, Symbol, Action) as _n_aggregate
    from flagged
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
    Amount
from grouped
where
    -- Non-equity rows (options, expiries, dividends, fees): untouched.
    not _is_equity_trade
    -- Equity group WITH an orders-aggregate: keep aggregate(s), drop the
    -- lagging activities partial fills.
    or (_is_equity_trade and _n_aggregate >= 1 and not _is_partial_fill)
    -- Equity group with NO aggregate (order aged out of recent_orders):
    -- keep the activities partial fills — they're the only record we have.
    or (_is_equity_trade and _n_aggregate = 0)
