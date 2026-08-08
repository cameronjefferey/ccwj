-- int_option_trade_kinds must stay 1:1 with int_option_contracts.
--
-- The model enriches each contract with strategy + underlying price at
-- open. Both joins must be non-fanning: the price join in particular used
-- to hit stg_daily_prices' physical (account, user_id, symbol, date) grain
-- through the colliding ACCOUNT LABEL and fanned each contract N× (one
-- copy per tenant sharing "Schwab Account" that held the underlying).
-- That tripled rows flowed into int_option_exit_analysis and
-- mart_coaching_signals, so the AI coach narrated ~2-3× inflated
-- closed-contract counts (caught by scripts/audit/reconcile.py CHECK 12,
-- fixed 2026-08-08 by deduping prices to (symbol, date)).
--
-- Fails if any (tenant, trade_symbol) appears more times in trade_kinds
-- than in the contract base.

with kinds as (
    select
        coalesce(tenant_id, account) as tkey,
        trade_symbol,
        count(*) as n
    from {{ ref('int_option_trade_kinds') }}
    group by 1, 2
),

contracts as (
    select
        coalesce(tenant_id, account) as tkey,
        trade_symbol,
        count(*) as n
    from {{ ref('int_option_contracts') }}
    group by 1, 2
)

select
    k.tkey,
    k.trade_symbol,
    k.n as kinds_rows,
    c.n as contract_rows
from kinds k
join contracts c using (tkey, trade_symbol)
where k.n != c.n
