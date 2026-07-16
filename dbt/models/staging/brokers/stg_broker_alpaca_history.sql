-- Per-broker history adapter: Alpaca (via SnapTrade).
-- Add Alpaca-specific history quirks HERE so they stay isolated and
-- independently testable. Unioned into stg_history.
--
-- ── Alpaca EQUITY duplicate-fill quirk (2026-07-16) ──────────────────────
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
-- for the account vs the broker snapshot's ~-$1.2k. See broker-sync-safety
-- "Bugs we've shipped" 2026-07-16.
--
-- Fix: for each EQUITY trade group (tenant_id, Date, Symbol, Action — the
-- equity lane is Action 'Buy'/'Sell'), if an orders-aggregate row exists, keep
-- ONLY the aggregate row(s) and drop the activities partial-fill rows. When no
-- aggregate exists (the order aged out of recent_orders), keep the activities
-- fills.
--
-- ── Alpaca OPTION quirk (2026-07-16, follow-on) ──────────────────────────
-- The activities feed is ALSO broken for options, in TWO ways at once:
--   1. NO 100x contract multiplier — activities ``amount`` is the per-SHARE
--      gross (2 contracts @ $2.59 → $5.18, not $518). recent_orders applies
--      the 100x correctly (orders_to_history_df: ``contract_mult = 100``).
--   2. NO open/close signal — Alpaca activities carry bare "… BUY FILL at …"
--      so _resolve_option_action can't tell open from close and labels
--      EVERYTHING "to Open". recent_orders ships the real Buy/Sell to
--      Open/Close.
-- Net effect on a closed spread: a correct orders close row (e.g. Buy to Close
-- 2 @ 2.59 = -$518) plus a mislabeled, 100x-too-small activities dup (Buy to
-- Open 2 @ 2.59 = -$5.18). Option realized P&L collapsed toward $0 (account's
-- closed option P&L read +$1,019 vs the ~-$14k the broker actually lost); the
-- account cash reconciliation was off by +$14.7k. Verified 2026-07-16: with
-- the rule below, EVERY fully-closed spread reproduces the external bot's
-- realized P&L to the dollar (RKLB -1188, AAPL -705, BAC -925, AMD -1150,
-- NFLX -290, MSFT +151, MRVL +55, …) and account cash reconciles to within
-- <1% (float/fee noise) of broker truth.
--
-- Fix (option lane): match activities fills to orders rows on
-- (tenant_id, Date, Symbol, Quantity, Price, buy/sell-side) — side is safe
-- because the mislabel only flips open/close, never the Buy/Sell verb.
--   * orders / Expired rows (plain description): authoritative, kept as-is.
--   * activities fill WITH a matching orders row: a mislabeled 100x-too-small
--     dup of that order → DROP.
--   * activities fill with NO matching orders row: a genuine open whose order
--     aged out of recent_orders. KEEP it, but RECOMPUTE Amount as
--     Quantity*Price*100 (immune to the raw per-share scale) and trust its
--     "to Open" label (the only fills that survive are genuine opens; every
--     mislabeled close has a recent orders row and is dropped above).
--
-- The correction lives HERE (not in activities_to_history_df) on purpose:
-- Alpaca activities are authoritatively per-share, so scaling in staging is
-- deterministic and idempotent. Fixing it forward in normalize would leave
-- historical per-share seed rows behind and create a MIXED-scale seed the
-- staging layer could no longer disambiguate.

with alpaca_rows as (
    {{ broker_history_rows('alpaca') }}
),

flagged as (
    select
        *,
        -- Alpaca activities partial-fill signature (broker-original wording).
        -- orders_to_history_df writes the plain company name / OSI, never this.
        -- coalesce → false so a NULL Description never makes the flag NULL
        -- (a NULL flag would make the WHERE OR-chain NULL and silently drop
        -- the row).
        coalesce(regexp_contains(Description, r'(?i) (PARTIAL_)?FILL at '), false)
            as _is_partial_fill,
        -- Equity trade lane. Option actions are 'Buy to Open' / 'Sell to Close'
        -- / … and expiries are 'Expired' — none equal 'Buy'/'Sell'.
        (Action in ('Buy', 'Sell')) as _is_equity_trade,
        -- Option lane: Symbol carries an OSI contract (YYMMDD[C|P]strike8).
        -- coalesce → false so cash/dividend/interest rows (NULL Symbol) don't
        -- get a NULL flag that would NULL the WHERE OR-chain and drop them.
        coalesce(regexp_contains(upper(Symbol), r'\d{6}[CP]\d{8}'), false)
            as _is_option,
        -- Buy vs Sell verb (preserved even when open/close is mislabeled).
        if(starts_with(Action, 'Buy'), 'buy', 'sell') as _opt_side
    from alpaca_rows
),

grouped as (
    select
        *,
        -- Equity: does an orders-aggregate exist for this trade group?
        countif(_is_equity_trade and not _is_partial_fill)
            over (partition by tenant_id, Date, Symbol, Action) as _n_aggregate,
        -- Option: does an authoritative orders row exist for this fill's
        -- (contract, date, qty, price, side)? Normalize qty/price so
        -- "16.30" and "16.3" collapse.
        countif(_is_option and not _is_partial_fill) over (
            partition by
                tenant_id, Date, Symbol, _opt_side,
                cast(round(safe_cast(Quantity as float64), 6) as string),
                cast(round(safe_cast(Price as float64), 6) as string)
        ) as _opt_n_ord
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
    -- Recompute the gross premium for kept option activities fills; every
    -- other row passes its Amount through untouched. (stg_history's
    -- amount_signed CTE re-signs by action, so magnitude is what matters.)
    case
        when _is_option and _is_partial_fill
            then cast(
                safe_cast(Quantity as float64) * safe_cast(Price as float64) * 100
                as string)
        else Amount
    end as Amount
from grouped
where
    -- Equity group WITH an orders-aggregate: keep aggregate(s), drop the
    -- lagging activities partial fills.
    (_is_equity_trade and _n_aggregate >= 1 and not _is_partial_fill)
    -- Equity group with NO aggregate (order aged out of recent_orders):
    -- keep the activities partial fills — they're the only record we have.
    or (_is_equity_trade and _n_aggregate = 0)
    -- Option orders / Expired rows: authoritative, kept as-is.
    or (_is_option and not _is_partial_fill)
    -- Option activities fill with NO matching orders row: genuine aged-out
    -- open, kept (Amount recomputed above). Fills WITH a matching orders row
    -- are mislabeled 100x-too-small dups and fall through (dropped).
    or (_is_option and _is_partial_fill and _opt_n_ord = 0)
    -- Everything else (dividends, cash events, non-OSI misc): untouched.
    or (not _is_equity_trade and not _is_option)
