/*
    Positions summary — the mart that powers the Positions Dashboard.

    One row per (account, symbol, strategy) with:
      - P&L (total, realized, unrealized) — total_pnl INCLUDES attributed dividends
      - Win/loss stats
      - Duration
      - Premium collected / paid
      - Dividend income (also surfaced separately for breakdown UX)

    Dividends-as-first-class:
      Dividends are a peer P&L stream alongside equity and options. The headline
      total_pnl number folds in the dividend income that's been attributed to
      this strategy via the dividend_rank ordering. A Buy-and-Hold position
      whose dividend income exceeds its price appreciation is reclassified
      as the "Dividend" strategy — capturing the trader who buys for yield.
      total_return is preserved as an alias of total_pnl for back-compat.
*/

with classified as (
    select * from {{ ref('int_strategy_classification') }}
),

dividends as (
    select * from {{ ref('int_dividends') }}
),

symbol_meta as (
    select * from {{ ref('stg_symbol_metadata') }}
),

---------------------------------------------------------------------
-- Aggregate by account × symbol × strategy
---------------------------------------------------------------------
strategy_summary as (
    select
        account,
        user_id,
        symbol,
        strategy,

        -- Status: treat any symbol/strategy with at least one open trade group as Open.
        -- Mixed (both open and closed) is folded into Open to keep the UX simple.
        case
            when countif(status = 'Open') > 0 then 'Open'
            else 'Closed'
        end as status,

        -- P&L (realized/unrealized are pre-split inside int_strategy_classification
        -- so an Open equity session with interim sells correctly attributes the
        -- already-realized portion to realized_pnl rather than unrealized_pnl).
        sum(total_pnl) as total_pnl,
        sum(realized_pnl) as realized_pnl,
        sum(unrealized_pnl) as unrealized_pnl,

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
    group by 1, 2, 3, 4
),

---------------------------------------------------------------------
-- Attach dividend income (once per account × symbol, to the primary equity strategy).
-- Window partitioned by (account, user_id, symbol) so the dividend
-- ranking can't bleed across tenants who happen to share a label.
---------------------------------------------------------------------
with_dividend_rank as (
    select
        ss.*,
        row_number() over (
            partition by ss.account, ss.user_id, ss.symbol
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

-- Pre-compute the attributed dividend amount per (strategy row) so the
-- final SELECT can read a single value in multiple places without
-- repeating the rank/coalesce expression.
with_attributed_dividends as (
    select
        wdr.*,
        case when wdr.dividend_rank = 1 then coalesce(d.total_dividend_income, 0) else 0 end
            as attributed_dividend_income,
        case when wdr.dividend_rank = 1 then coalesce(d.dividend_count, 0) else 0 end
            as attributed_dividend_count
    from with_dividend_rank wdr
    left join dividends d
        on wdr.account = d.account
        and (wdr.user_id is not distinct from d.user_id)
        and wdr.symbol = d.symbol
),

final as (
    select
        wad.account,
        wad.user_id,
        wad.symbol,

        -- Strategy reclassification: a "Buy and Hold" position whose dividend
        -- income exceeds its price-appreciation P&L (the trade-only total) is
        -- bucketed as "Dividend" — recognising the buy-for-yield trader.
        -- We only reclassify when this strategy is also the dividend-rank
        -- holder so we never invent a Dividend bucket on a row that isn't
        -- actually carrying dividend income (e.g. a Buy and Hold row that
        -- sits behind a Wheel on the same symbol).
        case
            when wad.dividend_rank = 1
                 and wad.strategy = 'Buy and Hold'
                 and wad.attributed_dividend_income > greatest(wad.total_pnl, 0)
                then 'Dividend'
            else wad.strategy
        end as strategy,

        wad.status,

        -- P&L — total_pnl includes attributed dividends so it is the headline
        -- "what did this position make me" number. realized_pnl and
        -- unrealized_pnl remain trade-only so the breakdown still maps to
        -- equity/option mark-to-market mechanics.
        round(wad.total_pnl + wad.attributed_dividend_income, 2) as total_pnl,
        round(wad.realized_pnl, 2)                               as realized_pnl,
        round(wad.unrealized_pnl, 2)                             as unrealized_pnl,
        round(wad.total_pnl, 2)                                  as trade_only_pnl,

        -- Premiums
        round(wad.total_premium_received, 2) as total_premium_received,
        round(wad.total_premium_paid, 2)     as total_premium_paid,

        -- Trade counts
        wad.num_trade_groups,
        wad.num_individual_trades,
        wad.num_winners,
        wad.num_losers,
        round(wad.win_rate, 4)          as win_rate,
        round(wad.avg_pnl_per_trade, 2) as avg_pnl_per_trade,
        wad.avg_days_in_trade,

        -- Dates
        wad.first_trade_date,
        wad.last_trade_date,

        -- Dividends (attributed to one strategy per symbol to avoid
        -- double-counting). Surfaced separately so the UI can still show
        -- "Dividends" as a peer to Equity/Options in the breakdown.
        round(wad.attributed_dividend_income, 2) as total_dividend_income,
        wad.attributed_dividend_count            as dividend_count,

        -- total_return is now an alias of total_pnl. Kept for back-compat
        -- with templates and downstream marts. New code should prefer
        -- total_pnl.
        round(wad.total_pnl + wad.attributed_dividend_income, 2) as total_return,

        -- Sector / subsector context (yfinance, refreshed daily). Coalesce so
        -- a missing-from-yfinance ticker still has 'Unknown' instead of NULL,
        -- which lets the app filter/group without special-casing nulls.
        coalesce(sm.sector, 'Unknown')      as sector,
        coalesce(sm.subsector, 'Unknown')   as subsector,
        sm.long_name                         as company_name,
        sm.market_cap                        as market_cap

    from with_attributed_dividends wad
    left join symbol_meta sm
        on upper(trim(wad.symbol)) = sm.symbol
)

select * from final
order by account, user_id, symbol, strategy
