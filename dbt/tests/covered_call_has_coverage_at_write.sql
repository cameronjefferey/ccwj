{#
    Covered Call honesty invariant (Aug 2026 classification audit F2).

    Every option contract labeled 'Covered Call' or 'Partially Covered
    Call' must have been backed by at least ~100 shares (one contract's
    deliverable, in the write date's share-units) held by the SAME
    (tenant, account, user) as of the contract's open date + the 3-day
    buy-write lookahead — including synthesized opening-balance fills,
    which count at write even when dated after the write (they are
    "day before first fill" placeholders for pre-window shares).

    Current holdings do not count. Pre-fix, coverage was judged off the
    session's lifetime max_quantity_held — a call written AFTER the
    shares were sold still read as Covered.

    Re-derives coverage independently from int_equity_fills so a
    refactor of coverage_at_write that silently breaks the join
    surfaces here.

    Fails (returns rows) for any (Partially) Covered Call whose
    recomputed at-write share count is under 95% of one contract's
    deliverable (5% tolerance for float noise on fractional-share
    accounts).
#}

with covered_calls as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        trade_symbol,
        strategy,
        open_date
    from {{ ref('int_strategy_classification') }}
    where trade_group_type = 'option_contract'
      and strategy in ('Covered Call', 'Partially Covered Call')
),

shares_at_write as (
    select
        cc.tenant_id,
        cc.account,
        cc.user_id,
        cc.trade_symbol,
        -- Mirrors the model: synthetic openings always count; real fills
        -- must land on/before open or open+3d.
        greatest(
            sum(case
                    when coalesce(f.is_synthetic_opening, false)
                      or f.trade_date <= cc.open_date
                    then f.signed_quantity else 0 end),
            sum(case
                    when coalesce(f.is_synthetic_opening, false)
                      or f.trade_date <= date_add(cc.open_date, interval 3 day)
                    then f.signed_quantity else 0 end)
        ) as shares_held
    from covered_calls cc
    left join {{ ref('int_equity_fills') }} f
        on  f.account = cc.account
        and (f.user_id is not distinct from cc.user_id)
        and (f.tenant_id is not distinct from cc.tenant_id)
        and f.symbol = cc.symbol
    group by 1, 2, 3, 4
),

-- One contract's deliverable in today's share-units (the fills above are
-- today-unit adjusted): 100 × forward split factor at the open date.
deliverable as (
    select
        cc.tenant_id,
        cc.account,
        cc.user_id,
        cc.trade_symbol,
        100.0 * coalesce(sf.cumulative_split_factor, 1.0) as shares_per_contract
    from covered_calls cc
    left join {{ ref('int_split_factors') }} sf
        on  sf.symbol     = cc.symbol
        and sf.trade_date = cc.open_date
)

select
    cc.tenant_id,
    cc.account,
    cc.user_id,
    cc.symbol,
    cc.trade_symbol,
    cc.strategy,
    saw.shares_held,
    d.shares_per_contract
from covered_calls cc
join shares_at_write saw
    on  cc.account = saw.account
    and (cc.user_id is not distinct from saw.user_id)
    and (cc.tenant_id is not distinct from saw.tenant_id)
    and cc.trade_symbol = saw.trade_symbol
join deliverable d
    on  cc.account = d.account
    and (cc.user_id is not distinct from d.user_id)
    and (cc.tenant_id is not distinct from d.tenant_id)
    and cc.trade_symbol = d.trade_symbol
where coalesce(saw.shares_held, 0) < 0.95 * d.shares_per_contract
