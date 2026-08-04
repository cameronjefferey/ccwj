{# 
    Per-contract invariants for int_option_contract_daily_pnl.

    Three properties must hold for the chart to be correct:

    (1) NO DOUBLE-EMISSION: for any (contract, date) there must be
        at most one MTM row AND at most one realized row.
    (2) NO MTM-ON-CLOSE-DAY: a closed contract must NOT have an
        MTM row on its own close_date. The realized branch owns
        close_date; if an MTM row also exists, the chart will
        double-count net_cash_flow + MTM on close_date.
    (3) REALIZED MATCHES REALIZED_PNL: the realized credit emitted on
        the realization date equals int_option_contracts.realized_pnl.
        For a fully-closed contract realized_pnl == net_cash_flow ==
        total_pnl (unchanged guarantee: "the chart on close_date credits
        exactly what int_option_contracts says"). For a PARTIAL close it
        equals the realized wedge of the sold portion (net_cash_flow +
        market_value − unrealized_pnl), attributed on the closing-fill
        date while the open remainder marks to market separately.

    Returns offending rows (test fails if non-zero).
#}

with daily as (
    select * from {{ ref('int_option_contract_daily_pnl') }}
),
contracts as (
    select * from {{ ref('int_option_contracts') }}
),

-- (1) No duplicate rows for any (tenant, contract, date, is_realized_close).
-- tenant_id is in the grain so two physical accounts sharing a display
-- label that both hold the SAME option contract (same OSI) aren't fused
-- into a false double-emission.
double_emission as (
    select
        tenant_id,
        account,
        user_id,
        trade_symbol,
        date,
        is_realized_close,
        count(*) as n,
        'double_emission' as failure_kind
    from daily
    group by 1, 2, 3, 4, 5, 6
    having count(*) > 1
),

-- (2) Closed contract must not have an MTM row on close_date.
mtm_on_close_day as (
    select
        d.tenant_id,
        d.account,
        d.user_id,
        d.trade_symbol,
        d.date,
        cast(false as bool) as is_realized_close,
        cast(null as int64) as n,
        'mtm_on_close_day' as failure_kind
    from daily d
    join contracts c
        on d.account = c.account
        and (d.user_id is not distinct from c.user_id)
        and (d.tenant_id is not distinct from c.tenant_id)
        and d.trade_symbol = c.trade_symbol
    where c.close_date is not null
      and d.date = c.close_date
      and d.is_realized_close = false
),

-- (3) Realized credit on the realization date == realized_pnl.
realized_mismatch as (
    select
        d.tenant_id,
        d.account,
        d.user_id,
        d.trade_symbol,
        d.date,
        cast(true as bool) as is_realized_close,
        cast(null as int64) as n,
        'realized_neq_realized_pnl' as failure_kind
    from daily d
    join contracts c
        on d.account = c.account
        and (d.user_id is not distinct from c.user_id)
        and (d.tenant_id is not distinct from c.tenant_id)
        and d.trade_symbol = c.trade_symbol
    where d.is_realized_close = true
      and abs(d.pnl_today - c.realized_pnl) > 0.01
)

select * from double_emission
union all
select * from mtm_on_close_day
union all
select * from realized_mismatch
