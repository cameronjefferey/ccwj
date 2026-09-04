{#
    Covered Call honesty invariant (Aug 2026 classification audit F2;
    OPEN-contract current-holdings path Sep 2026 / CCJ).

    Every option contract labeled 'Covered Call' or 'Partially Covered
    Call' must be backed by at least ~100 shares (one contract's
    deliverable) held by the SAME (tenant, account, user):

      - CLOSED contracts: as of the open date + the 3-day buy-write
        lookahead (write-date coverage). Pre-F2, coverage was judged
        off the session's lifetime max_quantity_held — a call written
        AFTER the shares were sold still read as Covered.
      - OPEN contracts: write-date coverage OR shares held now (fill
        ledger running qty or broker snapshot). A live 100-share +
        1 short call is a covered call even when the stock arrived
        via transfer / snapshot-only lot / a buy more than 3 days
        after the write.

    Re-derives coverage independently so a refactor of option_coverage
    that silently breaks the join surfaces here.

    Fails (returns rows) for any (Partially) Covered Call whose
    recomputed backing is under 95% of one contract's deliverable
    (5% tolerance for float noise on fractional-share accounts).
#}

with covered_calls as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        trade_symbol,
        strategy,
        status,
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
        -- Mirrors the model's greatest(at-open, at-open + 3d lookahead):
        -- a buy-write's shares may land after the call, and a sale within
        -- the lookahead must not retroactively strip the at-open coverage.
        greatest(
            sum(case when f.trade_date <= cc.open_date
                     then f.signed_quantity else 0 end),
            sum(case when f.trade_date <= date_add(cc.open_date, interval 3 day)
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
),

ledger_now as (
    select
        cc.tenant_id,
        cc.account,
        cc.user_id,
        cc.trade_symbol,
        sum(f.signed_quantity) as shares_held
    from covered_calls cc
    left join {{ ref('int_equity_fills') }} f
        on  f.account = cc.account
        and (f.user_id is not distinct from cc.user_id)
        and (f.tenant_id is not distinct from cc.tenant_id)
        and f.symbol = cc.symbol
    group by 1, 2, 3, 4
),

snapshot_now as (
    select
        cc.tenant_id,
        cc.account,
        cc.user_id,
        cc.trade_symbol,
        sum(c.quantity) as shares_held
    from covered_calls cc
    left join {{ ref('stg_current') }} c
        on  c.account = cc.account
        and (c.user_id is not distinct from cc.user_id)
        and (c.tenant_id is not distinct from cc.tenant_id)
        and c.underlying_symbol = cc.symbol
        and c.instrument_type = 'Equity'
    group by 1, 2, 3, 4
)

select
    cc.tenant_id,
    cc.account,
    cc.user_id,
    cc.symbol,
    cc.trade_symbol,
    cc.strategy,
    cc.status,
    saw.shares_held as shares_at_write,
    greatest(coalesce(ln.shares_held, 0), coalesce(sn.shares_held, 0))
        as shares_now,
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
left join ledger_now ln
    on  cc.account = ln.account
    and (cc.user_id is not distinct from ln.user_id)
    and (cc.tenant_id is not distinct from ln.tenant_id)
    and cc.trade_symbol = ln.trade_symbol
left join snapshot_now sn
    on  cc.account = sn.account
    and (cc.user_id is not distinct from sn.user_id)
    and (cc.tenant_id is not distinct from sn.tenant_id)
    and cc.trade_symbol = sn.trade_symbol
where
    coalesce(saw.shares_held, 0) < 0.95 * d.shares_per_contract
    and not (
        cc.status = 'Open'
        and greatest(coalesce(ln.shares_held, 0), coalesce(sn.shares_held, 0))
            >= 0.95 * d.shares_per_contract
    )
