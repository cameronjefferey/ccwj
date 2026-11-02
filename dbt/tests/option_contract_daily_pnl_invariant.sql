{# 
    Per-contract invariants for int_option_contract_daily_pnl.

    Three properties must hold for the chart to be correct:

    (1) NO DOUBLE-EMISSION: for any (contract, date) there must be
        at most one MTM row AND at most one realized row.
    (2) NO MTM-ON-CLOSE-DAY: a closed contract must NOT have an
        MTM row on its own close_date. The realized branch owns
        close_date; if an MTM row also exists, the chart will
        double-count net_cash_flow + MTM on close_date.
    (3) REALIZED MATCHES REALIZED_PNL: realized credits summed across
        every close date equal int_option_contracts.realized_pnl.
        For a fully-closed contract realized_pnl == net_cash_flow ==
        total_pnl (unchanged guarantee: "the chart on close_date credits
        exactly what int_option_contracts says"). For a PARTIAL close it
        equals the realized wedge of every sold portion (net_cash_flow +
        market_value − unrealized_pnl), split across its closing-fill
        dates while the open remainder marks to market separately.
    (4) MULTI-DATE CLOSES STAY MULTI-DATE: every distinct close date in
        stg_history has a realized row. Otherwise a later reduction can
        silently move all prior realized P&L onto the latest session.

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

-- (3) Realized credits across all close dates == realized_pnl.
realized_mismatch as (
    select
        c.tenant_id,
        c.account,
        c.user_id,
        c.trade_symbol,
        c.realized_close_date as date,
        cast(true as bool) as is_realized_close,
        cast(null as int64) as n,
        'realized_neq_realized_pnl' as failure_kind
    from contracts c
    left join daily d
        on d.account = c.account
        and (d.user_id is not distinct from c.user_id)
        and (d.tenant_id is not distinct from c.tenant_id)
        and d.trade_symbol = c.trade_symbol
        and d.is_realized_close = true
    where c.realized_close_date is not null
    group by 1, 2, 3, 4, 5, 6, 7, 8, c.realized_pnl
    having abs(coalesce(sum(d.pnl_today), 0) - c.realized_pnl) > 0.01
),

history_close_dates as (
    select
        h.tenant_id,
        h.account,
        h.user_id,
        h.trade_symbol,
        least(h.trade_date, coalesce(h.option_expiry, h.trade_date)) as date
    from {{ ref('stg_history') }} h
    where h.instrument_type in ('Call', 'Put')
      and h.action in (
          'option_buy_to_close', 'option_sell_to_close',
          'option_expired', 'option_assigned', 'option_exercised'
      )
      and exists (
          select 1
          from {{ ref('stg_history') }} o
          where (o.tenant_id is not distinct from h.tenant_id)
            and o.account = h.account
            and (o.user_id is not distinct from h.user_id)
            and o.trade_symbol = h.trade_symbol
            and o.instrument_type in ('Call', 'Put')
            and o.action in ('option_sell_to_open', 'option_buy_to_open')
            and o.trade_date <= least(
                h.trade_date, coalesce(h.option_expiry, h.trade_date)
            )
      )
    group by 1, 2, 3, 4, 5
),

expected_close_dates as (
    select *
    from (
        select * from history_close_dates
        union distinct
        select
            c.tenant_id,
            c.account,
            c.user_id,
            c.trade_symbol,
            c.realized_close_date as date
        from contracts c
        where c.realized_close_date is not null
          and exists (
              select 1
              from history_close_dates h
              where (h.tenant_id is not distinct from c.tenant_id)
                and h.account = c.account
                and (h.user_id is not distinct from c.user_id)
                and h.trade_symbol = c.trade_symbol
          )
    )
    qualify count(*) over (
        partition by tenant_id, account, user_id, trade_symbol
    ) > 1
),

-- (4) Every close date on a multi-date contract has a realized row.
missing_realized_date as (
    select
        e.tenant_id,
        e.account,
        e.user_id,
        e.trade_symbol,
        e.date,
        cast(true as bool) as is_realized_close,
        cast(null as int64) as n,
        'missing_realized_close_date' as failure_kind
    from expected_close_dates e
    join contracts c
        on e.account = c.account
        and (e.user_id is not distinct from c.user_id)
        and (e.tenant_id is not distinct from c.tenant_id)
        and e.trade_symbol = c.trade_symbol
        and c.realized_close_date is not null
    left join daily d
        on e.account = d.account
        and (e.user_id is not distinct from d.user_id)
        and (e.tenant_id is not distinct from d.tenant_id)
        and e.trade_symbol = d.trade_symbol
        and e.date = d.date
        and d.is_realized_close = true
    where d.trade_symbol is null
)

select * from double_emission
union all
select * from mtm_on_close_day
union all
select * from realized_mismatch
union all
select * from missing_realized_date
