/*
    Positions summary — the mart that powers the Positions Dashboard.

    One row per (account, symbol, strategy) with:
      - P&L (total, realized, unrealized)
      - Win/loss stats
      - Duration
      - Premium collected / paid
      - Dividend income
*/

with classified as (
    select * from {{ ref('int_strategy_classification') }}
),

dividends as (
    select * from {{ ref('int_dividends') }}
),

---------------------------------------------------------------------
-- Aggregate by account × symbol × strategy
---------------------------------------------------------------------
strategy_summary as (
    select
        account,
        symbol,
        strategy,

        -- Status
        case
            when countif(status = 'Open') > 0 and countif(status = 'Closed') > 0 then 'Mixed'
            when countif(status = 'Open') > 0 then 'Open'
            else 'Closed'
        end as status,

        -- P&L
        sum(total_pnl) as total_pnl,
        sum(case when status = 'Closed' then total_pnl else 0 end) as realized_pnl,
        sum(case when status = 'Open'   then total_pnl else 0 end) as unrealized_pnl,

        -- Premium flows (option strategies)
        sum(premium_received) as total_premium_received,
        sum(abs(premium_paid)) as total_premium_paid,

        -- Trade counts
        count(*) as num_trade_groups,
        sum(num_trades) as num_individual_trades,
        countif(is_winner and status = 'Closed') as num_winners,
        countif(not is_winner and status = 'Closed') as num_losers,

        -- Win rate (closed trade groups only)
        safe_divide(
            countif(is_winner and status = 'Closed'),
            nullif(countif(status = 'Closed'), 0)
        ) as win_rate,

        -- Average P&L per closed trade group
        safe_divide(
            sum(case when status = 'Closed' then total_pnl else 0 end),
            nullif(countif(status = 'Closed'), 0)
        ) as avg_pnl_per_trade,

        -- Duration
        round(avg(days_in_trade), 1) as avg_days_in_trade,

        -- Date span
        min(open_date) as first_trade_date,
        max(coalesce(close_date, current_date())) as last_trade_date

    from classified
    group by 1, 2, 3
),

---------------------------------------------------------------------
-- Attach dividend income (once per account × symbol, to the primary equity strategy)
---------------------------------------------------------------------
with_dividend_rank as (
    select
        ss.*,
        row_number() over (
            partition by ss.account, ss.symbol
            order by
                case ss.strategy
                    when 'Wheel'        then 1
                    when 'Covered Call'  then 2
                    when 'Buy and Hold'  then 3
                    else 99
                end
        ) as dividend_rank
    from strategy_summary ss
),

final as (
    select
        wdr.account,
        wdr.symbol,
        wdr.strategy,
        wdr.status,

        -- P&L
        round(wdr.total_pnl, 2)       as total_pnl,
        round(wdr.realized_pnl, 2)    as realized_pnl,
        round(wdr.unrealized_pnl, 2)  as unrealized_pnl,

        -- Premiums
        round(wdr.total_premium_received, 2) as total_premium_received,
        round(wdr.total_premium_paid, 2)     as total_premium_paid,

        -- Trade counts
        wdr.num_trade_groups,
        wdr.num_individual_trades,
        wdr.num_winners,
        wdr.num_losers,
        round(wdr.win_rate, 4)          as win_rate,
        round(wdr.avg_pnl_per_trade, 2) as avg_pnl_per_trade,
        wdr.avg_days_in_trade,

        -- Dates
        wdr.first_trade_date,
        wdr.last_trade_date,

        -- Dividends (attributed to one strategy per symbol to avoid double-counting)
        case when wdr.dividend_rank = 1
            then round(coalesce(d.total_dividend_income, 0), 2)
            else 0
        end as total_dividend_income,
        case when wdr.dividend_rank = 1
            then coalesce(d.dividend_count, 0)
            else 0
        end as dividend_count,

        -- All-in return = strategy P&L + dividends
        round(
            wdr.total_pnl
            + case when wdr.dividend_rank = 1 then coalesce(d.total_dividend_income, 0) else 0 end
        , 2) as total_return

    from with_dividend_rank wdr
    left join dividends d
        on wdr.account = d.account
        and wdr.symbol = d.symbol
)

select * from final
order by account, symbol, strategy
