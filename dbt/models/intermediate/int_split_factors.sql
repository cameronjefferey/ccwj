{{
    config(
        materialized='view'
    )
}}

/*
    Per (symbol, trade_date) cumulative forward split factor.

    Definition: the multiplier that converts a quantity expressed in
    pre-split shares (as recorded by the broker on the trade date) into
    the equivalent quantity in TODAY's share-units.

      cumulative_split_factor(trade_date)
        = PRODUCT(split_ratio for every split where split_date > trade_date)

    Examples:
      - XLU 2:1 split on 2025-12-05.
        - Trade on 2025-10-29: factor = 2.0 (one future split, ratio 2).
        - Trade on 2026-04-27: factor = 1.0 (no future splits).
      - Hypothetical RVSN 1:30 reverse on 2025-03-01 then 1:30 reverse
        on 2026-01-15.
        - Trade on 2024-12-30: factor = 0.0333 × 0.0333 ≈ 0.00111.
        - Trade on 2025-06-01: factor = 0.0333 (only second future split).
        - Trade on 2026-02-01: factor = 1.0.

    Consumers apply:
      - adjusted_quantity   = raw_quantity × factor
      - adjusted_price      = raw_price    / factor
      - adjusted_strike     = raw_strike   / factor   (option contracts)
      - amount              = UNCHANGED    (cash flow is split-invariant)

    Independent of stg_daily_prices: only joins stg_history (for the
    distinct trade_date set) and stg_split_events. Stays in CI pass 1.

    Why per-(symbol, trade_date) and not per-row: the join in downstream
    models is `(symbol, trade_date)` which is the natural grain for
    every fill on the same day getting the same factor. Materializing
    once and joining is cheaper than recomputing the cumulative
    PRODUCT inside every consumer.
*/

with splits as (
    select symbol, split_date, split_ratio
    from {{ ref('stg_split_events') }}
),

distinct_trade_dates as (
    select distinct
        underlying_symbol as symbol,
        trade_date
    from {{ ref('stg_history') }}
    where underlying_symbol is not null
      and trade_date is not null
),

-- For each (symbol, trade_date), find every split for that symbol whose
-- date is STRICTLY AFTER the trade. Compute cumulative product via
-- exp(sum(ln(...))). LN safety: stg_split_events filters to
-- split_ratio > 0 already.
factors as (
    select
        d.symbol,
        d.trade_date,
        coalesce(
            exp(sum(
                case
                    when s.split_date is not null
                     and s.split_date > d.trade_date
                    then ln(s.split_ratio)
                    else 0
                end
            )),
            1.0
        ) as cumulative_split_factor
    from distinct_trade_dates d
    left join splits s
        on s.symbol = d.symbol
    group by 1, 2
)

select
    symbol,
    trade_date,
    cumulative_split_factor
from factors
