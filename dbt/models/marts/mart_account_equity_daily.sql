{{ config(materialized='table') }}

/*
    Daily account equity breakdown.

    One row per (account, date) with:
      - equity_value   (MTM equity P&L: realized + unrealized)
      - option_value   (options P&L to date, treated as value change)
      - cash_value     (approx. starting from 0 + all trade cash flows)
      - account_value  = equity_value + option_value + cash_value

    This mirrors the account-level curve used in _build_account_chart_from_daily_pnl,
    but persisted in BigQuery so we can compute weekly account-change waterfalls.
*/

with daily as (
    select
        account,
        symbol,
        date,
        options_amount,
        dividends_amount,
        equity_buy_cost,
        equity_buy_qty,
        equity_sell_proceeds,
        equity_sell_qty,
        other_amount,
        close_price
    from {{ ref('mart_daily_pnl') }}
),

-- Reconstruct per-symbol equity state with window functions:
-- running shares, running cost, realized equity P&L to date.
equity_state as (
    select
        account,
        symbol,
        date,
        close_price,
        equity_buy_qty,
        equity_buy_cost,
        equity_sell_qty,
        equity_sell_proceeds,

        sum(equity_buy_qty) over w_sh as shares_before,
        sum(equity_buy_cost) over w_sh as cost_before
    from daily
    window w_sh as (
        partition by account, symbol
        order by date
        rows between unbounded preceding and current row
    )
),

equity_pnl_cumulative as (
    select
        account,
        symbol,
        date,
        close_price,
        shares_before,
        cost_before,
        sum(
            case
                when equity_sell_qty > 0 and shares_before > 0 then
                    equity_sell_proceeds
                    - (cost_before / nullif(shares_before, 0))
                      * least(equity_sell_qty, shares_before)
                when equity_sell_qty > 0 then equity_sell_proceeds
                else 0
            end
        ) over (
            partition by account, symbol
            order by date
            rows between unbounded preceding and current row
        ) as realized_equity_pnl_to_date
    from equity_state
),

equity_value_by_symbol as (
    select
        account,
        symbol,
        date,
        realized_equity_pnl_to_date,
        case
            when close_price > 0 and shares_before > 0 then
                shares_before * close_price - cost_before
            else 0
        end as unrealized_equity_pnl
    from equity_pnl_cumulative
),

equity_value_by_account as (
    select
        account,
        date,
        sum(realized_equity_pnl_to_date + unrealized_equity_pnl) as equity_value
    from equity_value_by_symbol
    group by 1, 2
),

cash_and_options as (
    select
        account,
        date,
        -- Cumulative options P&L acts like option "value"
        sum(options_amount) over w_acc as option_value,
        -- Approximate cash balance: cumulative cash flows from trades
        sum(dividends_amount + other_amount
            + equity_sell_proceeds
            - equity_buy_cost) over w_acc as cash_value
    from daily
    window w_acc as (
        partition by account
        order by date
        rows between unbounded preceding and current row
    )
)

select
    c.account,
    c.date,
    coalesce(e.equity_value, 0)       as equity_value,
    coalesce(c.option_value, 0)       as option_value,
    coalesce(c.cash_value, 0)         as cash_value,
    coalesce(e.equity_value, 0)
      + coalesce(c.option_value, 0)
      + coalesce(c.cash_value, 0)     as account_value
from cash_and_options c
left join equity_value_by_account e
  on c.account = e.account
 and c.date    = e.date
order by c.account, c.date

