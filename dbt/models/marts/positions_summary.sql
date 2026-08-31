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
      this strategy via the dividend_rank ordering. A Buy-and-Hold is
      reclassified as "Dividend" only when the cash is a real yield (see
      the CASE in `final`) — not merely because the stock happened to pay
      a coupon while the price did the work (or the work went the other way).
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
        -- v2 tenant_id is part of the grain (int_strategy_classification
        -- carries it natively from staging) so two physical accounts that
        -- share a display label don't fuse their per-symbol/strategy rows.
        tenant_id,
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

        -- Trade counts (DRIP reinvestments excluded upstream in
        -- int_equity_sessions.num_trades — they are lots, not placed trades)
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
    group by tenant_id, account, user_id, symbol, strategy
),

---------------------------------------------------------------------
-- Attach dividend income (once per account × symbol, to the primary equity strategy).
-- Logic lives in the attribute_dividends_to_strategy macro so the
-- mart and the runtime DATE_FILTERED_QUERY in app/positions_page.py can
-- never silently drift. ATTRIBUTION_INVARIANT: keep these two paths in
-- sync — see the macro docstring.
---------------------------------------------------------------------
{{ attribute_dividends_to_strategy('strategy_summary', 'dividends') }},

-- Invested capital for the yield test. int_equity_fills is the canonical
-- buy stream (real fills UNION synthesized opening balances), so a
-- mid-window start still has a denominator. GREATEST with the live
-- snapshot cost basis covers transfer-in lots that never produced a fill.
-- ATTRIBUTION_INVARIANT: the DATE_FILTERED_QUERY mirror in
-- app/positions_page.py must join the same two sources (lifetime, not
-- windowed — otherwise a full-history date filter would disagree with
-- this mart).
invested as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        sum(abs(amount)) as fill_capital
    from {{ ref('int_equity_fills') }}
    where action = 'equity_buy'
    group by 1, 2, 3, 4
),

current_basis as (
    select
        tenant_id,
        account,
        user_id,
        underlying_symbol as symbol,
        sum(abs(coalesce(cost_basis, 0))) as cost_basis
    from {{ ref('stg_current') }}
    where instrument_type = 'Equity'
    group by 1, 2, 3, 4
),

final as (
    select
        wad.account,
        wad.user_id,
        wad.tenant_id,  -- v2 passthrough; see strategy_summary CTE
        wad.symbol,

        -- Buy and Hold → Dividend only when the cash is a real yield, not
        -- an incidental coupon. The old rule
        --   divs > greatest(price_pnl, 0)
        -- labeled every underwater stock that paid anything (UFO −$6,571
        -- with a $17 dividend → "Dividend"). Two paths, both required to
        -- keep JEPI / SCHD / a utility held for income while dropping
        -- MSFT / QTUM / a crashed thematic ETF:
        --   A. Yield: ≥ 2.5% of invested AND ≥ 15% of the P&L story, with
        --      a $200 capital floor (same floor as Daily Review).
        --   B. Story: dividends are ≥ 40% of (|price P&L| + dividends) —
        --      SCHD-like cases just under the yield cutoff, and rows
        --      with no reliable capital (opening-balance-only history).
        -- Rank-1 only so we never invent a Dividend bucket on a Buy and
        -- Hold sitting behind a Wheel on the same symbol.
        case
            when wad.dividend_rank = 1
                 and wad.strategy = 'Buy and Hold'
                 and wad.attributed_dividend_income > 0
                 and (
                     (
                         greatest(coalesce(inv.fill_capital, 0), coalesce(cb.cost_basis, 0)) >= 200
                         and safe_divide(
                                 wad.attributed_dividend_income,
                                 greatest(coalesce(inv.fill_capital, 0), coalesce(cb.cost_basis, 0))
                             ) >= 0.025
                         and safe_divide(
                                 wad.attributed_dividend_income,
                                 abs(wad.total_pnl) + wad.attributed_dividend_income
                             ) >= 0.15
                     )
                     or safe_divide(
                            wad.attributed_dividend_income,
                            abs(wad.total_pnl) + wad.attributed_dividend_income
                        ) >= 0.40
                 )
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
    left join invested inv
        on (wad.tenant_id is not distinct from inv.tenant_id)
        and wad.account = inv.account
        and (wad.user_id is not distinct from inv.user_id)
        and wad.symbol = inv.symbol
    left join current_basis cb
        on (wad.tenant_id is not distinct from cb.tenant_id)
        and wad.account = cb.account
        and (wad.user_id is not distinct from cb.user_id)
        and wad.symbol = cb.symbol
    left join symbol_meta sm
        on upper(trim(wad.symbol)) = sm.symbol
)

select * from final
order by account, user_id, symbol, strategy
