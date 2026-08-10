{{
    config(
        materialized='view'
    )
}}

/*
    Canonical equity fill stream — real fills from stg_history UNION the
    synthetic opening-balance buys from int_opening_balances, with split
    adjustment applied CENTRALLY.

    Every model that walks a running equity share count reads THIS view
    (int_equity_sessions, int_closed_equity_legs, int_dividend_events,
    mart_daily_pnl's equity branches, mart_benchmark's buy cash, and the
    coverage-at-write CTEs in int_strategy_classification). Before Aug 2026
    each of them joined int_split_factors and cased on action separately;
    centralizing means the opening-balance synthesis and the split
    adjustment can never drift apart between consumers.

    Column contract:
      quantity_raw    — broker-reported quantity in FILL-DATE share units
                        (synthetic rows: today-unit qty, raw not meaningful)
      quantity        — TODAY's share-units (split-adjusted); use for any
                        running count or share comparison
      signed_quantity — quantity signed by side (buy +, sell/short -)
      amount          — cash flow, split-invariant, synthetic rows carry
                        the estimated opening cost (negative = cash out)
      is_synthetic_opening — true for inferred opening-balance rows; the
                        UI discloses these (Position Detail banner), and
                        int_drip_fills deliberately does NOT read this view
                        so synthetic fractional openings can't be
                        misdetected as dividend reinvestments.

    Synthetic rows are dated the day BEFORE the symbol's first real fill so
    they sort first in every (trade_date, action)-ordered window, and their
    quantity is already in today's units — do NOT re-apply split factors.
*/

with real_fills as (
    select
        h.tenant_id,
        h.account,
        h.user_id,
        h.underlying_symbol as symbol,
        h.trade_symbol,
        h.trade_date,
        h.action,
        h.quantity as quantity_raw,
        h.quantity * coalesce(sf.cumulative_split_factor, 1.0) as quantity,
        case
            when h.action = 'equity_buy'
                then  h.quantity * coalesce(sf.cumulative_split_factor, 1.0)
            when h.action in ('equity_sell', 'equity_sell_short')
                then -h.quantity * coalesce(sf.cumulative_split_factor, 1.0)
            else 0
        end as signed_quantity,
        h.amount,
        false as is_synthetic_opening
    from {{ ref('stg_history') }} h
    left join {{ ref('int_split_factors') }} sf
        on  sf.symbol     = h.underlying_symbol
        and sf.trade_date = h.trade_date
    where h.instrument_type = 'Equity'
),

synthetic_openings as (
    select
        ob.tenant_id,
        ob.account,
        ob.user_id,
        ob.symbol,
        ob.symbol as trade_symbol,
        ob.opening_date as trade_date,
        'equity_buy' as action,
        ob.opening_qty as quantity_raw,
        ob.opening_qty as quantity,          -- already today's units
        ob.opening_qty as signed_quantity,
        ob.est_amount as amount,
        true as is_synthetic_opening
    from {{ ref('int_opening_balances') }} ob
    where ob.opening_qty > 0.01
      and ob.price_source != 'unpriced'
)

select * from real_fills
union all
select * from synthetic_openings
