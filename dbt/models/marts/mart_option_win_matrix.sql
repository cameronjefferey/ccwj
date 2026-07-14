{{ config(materialized='table') }}

/*
    Option Win/Loss matrix cells — pre-bucketed DTE x Strike-Distance grid.

    Powers the Position Detail "Win/Loss Matrix" heatmap. This used to be
    built row-by-row in Flask (`_build_option_matrices`: nested loops over
    DTE/strike buckets per strategy on every page load). dbt owns the
    bucketing now; Flask just reshapes the pre-aggregated cells into the
    nested template dict.

    Grain: one row per
      (tenant_id, account, user_id, underlying_symbol, strategy,
       trade_symbol, dte_label, strike_col). trade_symbol is one option
       contract, so the DTE/strike bucket labels are deterministic per row;
       it is retained so the Position Detail leg filter can scope the matrix
       to the contracts in the selected leg (Flask re-aggregates the cells).

    The bucket boundaries and label strings MUST stay byte-identical to the
    Python that renders them (`_build_option_matrices` in app/routes.py):
      - DTE bins on dte_at_open: 0-7 / 8-14 / 15-30 / 31-60 / 61+
      - Strike distance as % of underlying at open, signed, half-open [lo, hi):
        <-10% / -10 to -5% / -5 to -2% / ATM +-2% / +2 to +5% / +5 to +10% / >+10%
    (labels use en-dash / em-dash exactly as the UI expects).

    Only CLOSED contracts with a known strike distance are bucketed, matching
    the old POSITION_MATRIX_QUERY (`status = 'Closed' AND strike_distance IS
    NOT NULL`). Aggregation emits RAW count / wins / sum_pnl so Flask can
    combine cells across tenants/accounts and round exactly once (avg and
    win-rate rounding happen after the union, as before).

    Tenancy: carries tenant_id natively from int_option_trade_kinds; the app
    scopes reads by tenant_id in SQL and re-filters the DataFrame.
*/

with kinds as (
    select
        tenant_id,
        account,
        user_id,
        underlying_symbol,
        trade_symbol,
        strategy,
        dte_at_open,
        strike_distance,
        underlying_price_at_open,
        total_pnl
    from {{ ref('int_option_trade_kinds') }}
    where status = 'Closed'
      and strike_distance is not null
),

bucketed as (
    select
        tenant_id,
        account,
        user_id,
        underlying_symbol,
        trade_symbol,
        strategy,
        total_pnl,

        -- DTE bin on dte_at_open (matches Python DTE_BINS exactly).
        case
            when dte_at_open between 0 and 7   then '0\u20137'
            when dte_at_open between 8 and 14  then '8\u201314'
            when dte_at_open between 15 and 30 then '15\u201330'
            when dte_at_open between 31 and 60 then '31\u201360'
            else '61+'
        end as dte_label,

        case
            when dte_at_open between 0 and 7   then 0
            when dte_at_open between 8 and 14  then 1
            when dte_at_open between 15 and 30 then 2
            when dte_at_open between 31 and 60 then 3
            else 4
        end as dte_order,

        -- Strike distance as signed % of underlying at open. Half-open bins
        -- [lo, hi) implemented via ordered `<` thresholds (matches Python).
        case
            when underlying_price_at_open is null or underlying_price_at_open <= 0 then '\u2014'
            when (strike_distance / underlying_price_at_open) * 100 < -10 then '<-10%'
            when (strike_distance / underlying_price_at_open) * 100 < -5  then '-10 to -5%'
            when (strike_distance / underlying_price_at_open) * 100 < -2  then '-5 to -2%'
            when (strike_distance / underlying_price_at_open) * 100 < 2   then 'ATM \u00b12%'
            when (strike_distance / underlying_price_at_open) * 100 < 5   then '+2 to +5%'
            when (strike_distance / underlying_price_at_open) * 100 < 10  then '+5 to +10%'
            else '>+10%'
        end as strike_col,

        case
            when underlying_price_at_open is null or underlying_price_at_open <= 0 then 7
            when (strike_distance / underlying_price_at_open) * 100 < -10 then 0
            when (strike_distance / underlying_price_at_open) * 100 < -5  then 1
            when (strike_distance / underlying_price_at_open) * 100 < -2  then 2
            when (strike_distance / underlying_price_at_open) * 100 < 2   then 3
            when (strike_distance / underlying_price_at_open) * 100 < 5   then 4
            when (strike_distance / underlying_price_at_open) * 100 < 10  then 5
            else 6
        end as strike_order
    from kinds
)

select
    tenant_id,
    account,
    user_id,
    underlying_symbol,
    trade_symbol,
    strategy,
    dte_label,
    dte_order,
    strike_col,
    strike_order,
    count(*)                                        as trade_count,
    sum(case when total_pnl > 0 then 1 else 0 end)  as wins,
    sum(total_pnl)                                  as sum_pnl
from bucketed
group by
    tenant_id, account, user_id, underlying_symbol, trade_symbol, strategy,
    dte_label, dte_order, strike_col, strike_order
