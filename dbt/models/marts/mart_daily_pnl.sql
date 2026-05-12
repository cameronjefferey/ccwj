/*
    Daily P&L building blocks — pre-aggregated for chart rendering.

    One row per (account, symbol, date).  Covers every date that has either
    a trade, an option snapshot, a dividend, or a daily close price from
    the yfinance pipeline.

    OPTION P&L ATTRIBUTION (see AGENTS.md "Option P&L Attribution" for
    the full rule and int_option_contract_daily_pnl for the per-contract
    grain):

      The chart should show realize-on-close + MTM-while-open, NOT
      cash-flow-on-fill-date. STO premium does not become "yours" until
      the position closes.

      Two columns expose this:
        cumulative_options_pnl       =  cumulative SUM of realized
                                        contributions (each closed
                                        contract credited on its
                                        close_date)
        open_options_unrealized_pnl  =  point-in-time MTM at date d of
                                        all currently-open contracts
                                        with snapshot data

      Total options P&L at d = the two above, ADDED.

      ``options_amount`` is preserved (legacy diagnostic — sum of raw
      ``stg_history.amount`` for option fills on this date) but is NOT
      what the chart should sum. Reconcile audits and downstream
      cross-checks read it but the presentation layer must not.

      ``option_market_value`` and ``option_cost_basis`` (from
      int_daily_option_value) are still exposed for backwards-compat
      diagnostics. Do not use them for new chart math —
      ``open_options_unrealized_pnl`` already wraps them with the right
      sign convention.

    Equity columns provide the daily buy/sell events so the presentation
    layer can compute running average-cost P&L.

    PRICE PRECEDENCE (see AGENTS.md "Pricing Precedence" for the full rule):

      Historical days (date < today): yfinance daily close is the only
      source — broker doesn't provide historical OHLC, and the chart
      mark-to-market math depends on having a value every day.

      Today's row (date = current_date()): prefer the broker snapshot's
      implied price (market_value / quantity from stg_current) when the
      snapshot is FRESH (snapshot_date = today). yfinance is the fallback.

      Why: the position page renders three "current value" totals
      (Strategy Breakdown, Breakdown by Type, Chart Terminal) all of which
      should agree. Strategy Breakdown / Breakdown by Type read broker
      directly via int_enriched_current. The chart used to read yfinance
      close for today's row, which created a structural disagreement
      hidden only by `_align_position_pnl_chart_with_kpi` rescaling the
      whole chart series (a silent distortion, see app/routes.py).
      Sourcing today's close from broker when fresh makes all three
      surfaces reconcile by construction.
*/

with trade_daily as (
    select
        account,
        user_id,
        underlying_symbol as symbol,
        trade_date as date,

        sum(case when instrument_type in ('Call', 'Put')
            then amount else 0 end)                                     as options_amount,

        sum(case when instrument_type = 'Equity' and action = 'equity_buy'
            then abs(amount) else 0 end)                                as equity_buy_cost,

        sum(case when instrument_type = 'Equity' and action = 'equity_buy'
            then abs(coalesce(quantity, 0)) else 0 end)                 as equity_buy_qty,

        sum(case when instrument_type = 'Equity'
                      and action in ('equity_sell', 'equity_sell_short')
            then amount else 0 end)                                     as equity_sell_proceeds,

        sum(case when instrument_type = 'Equity'
                      and action in ('equity_sell', 'equity_sell_short')
            then abs(coalesce(quantity, 0)) else 0 end)                 as equity_sell_qty,

        sum(case when instrument_type not in ('Call', 'Put', 'Equity', 'Dividend')
            then amount else 0 end)                                     as other_amount

    from {{ ref('stg_history') }}
    where trade_date is not null
      and underlying_symbol is not null
    group by 1, 2, 3, 4
),

-- Dividends source: int_dividend_events. UNIONs CSV-reported dividends with
-- yfinance-synthesized ex-div × holdings events. Reading stg_history's
-- action='dividend' rows directly here was broken for ~99% of users —
-- Schwab Connect drops DIVIDEND_OR_INTEREST and most users never run a
-- manual CSV upload, so the chart's cumulative_dividends_pnl line on JEPI /
-- JEPQ / SCHD positions stayed flat at $0 even when the user clearly owned
-- thousands of shares for years.
dividend_daily as (
    select
        account,
        user_id,
        symbol,
        trade_date as date,
        sum(amount) as dividends_amount
    from {{ ref('int_dividend_events') }}
    group by 1, 2, 3, 4
),

-- Daily close prices per (account, symbol, date). stg_daily_prices carries
-- a per-tenant user_id from the price loader, so when two users legitimately
-- share an account label AND a symbol (e.g. user 2 and user 9 both holding
-- BE under "Sara Investment") there are TWO rows per (account, symbol, date)
-- with the SAME close_price but different user_ids. Dedup on (account, symbol,
-- date) here so the downstream join doesn't fan every row of `joined` into
-- two — that doubling propagated all the way into `cumulative_options_pnl`
-- and showed up as a chart whose options line was 2× the real per-tenant
-- value (May 2026 BE/Sara screenshot, see SKILL.md 2026-05-11). Price is
-- a public datum keyed on (symbol, date); user_id is not part of its identity.
prices as (
    select
        account,
        symbol,
        date,
        any_value(close_price) as close_price
    from {{ ref('stg_daily_prices') }}
    group by 1, 2, 3
),

-- Broker-implied current price for today's row only. See header comment for
-- the full price-precedence rationale. We pull from stg_current (raw broker
-- snapshot) rather than int_enriched_current to avoid a circular reference —
-- int_enriched_current already reads stg_daily_prices for its yfinance
-- fallback. snapshot_date = current_date() gates this to fresh snapshots
-- only; stale brokers leave the row null and yfinance carries.
broker_today_prices as (
    select
        account,
        user_id,
        underlying_symbol as symbol,
        snapshot_date as date,
        market_value / quantity as close_price
    from {{ ref('stg_current') }}
    where instrument_type = 'Equity'
      and quantity is not null
      and quantity != 0
      and market_value is not null
      and market_value != 0
      and snapshot_date = current_date()
),

-- Per (account, user_id, symbol, date) options P&L decomposition.
-- See int_option_contract_daily_pnl for the per-contract grain and
-- attribution rule. Two streams to aggregate:
--   * realized_today    = sum of contracts realizing on this date
--                         (chart accumulates this as a running total)
--   * open_unrealized_today = sum of MTM for currently-open contracts
--                             with snapshot data on this date
--                             (point-in-time, NOT accumulated)
options_pnl_per_day as (
    select
        account,
        user_id,
        symbol,
        date,
        sum(case when is_realized_close then pnl_today else 0 end)
            as realized_today,
        sum(case when not is_realized_close then pnl_today else 0 end)
            as open_unrealized_today
    from {{ ref('int_option_contract_daily_pnl') }}
    group by 1, 2, 3, 4
),

-- Build the per-tenant date spine from rows that have user_id
-- (trade_daily, options_pnl_per_day, daily_option). prices have no
-- user_id so we expand them per-tenant via a join to known
-- (account, user_id) pairs; without that the price-only rows would
-- produce NULL user_id rows that the app filter would drop.
known_tenants as (
    select distinct account, user_id, symbol from trade_daily
    union distinct
    select distinct account, user_id, symbol from {{ ref('int_daily_option_value') }}
    union distinct
    select distinct account, user_id, symbol from options_pnl_per_day
),

all_dates as (
    select distinct account, user_id, symbol, date from (
        select account, user_id, symbol, date from trade_daily
        union distinct
        select account, user_id, symbol, date from dividend_daily
        union distinct
        select account, user_id, symbol, date from {{ ref('int_daily_option_value') }}
        union distinct
        select account, user_id, symbol, date from options_pnl_per_day
        union distinct
        select kt.account, kt.user_id, kt.symbol, p.date
        from known_tenants kt
        join prices p
            on kt.account = p.account
            and kt.symbol = p.symbol
    )
),

daily_option as (
    select account, user_id, symbol, date, option_market_value, option_cost_basis
    from {{ ref('int_daily_option_value') }}
),

joined as (
    select
        ad.account,
        ad.user_id,
        ad.symbol,
        ad.date,
        coalesce(td.options_amount, 0)        as options_amount,
        coalesce(dd.dividends_amount, 0)      as dividends_amount,
        coalesce(td.equity_buy_cost, 0)       as equity_buy_cost,
        coalesce(td.equity_buy_qty, 0)        as equity_buy_qty,
        coalesce(td.equity_sell_proceeds, 0)  as equity_sell_proceeds,
        coalesce(td.equity_sell_qty, 0)       as equity_sell_qty,
        coalesce(td.other_amount, 0)          as other_amount,

        -- Realize-on-close + MTM-while-open option contributions.
        -- See header docstring for the attribution rule and
        -- int_option_contract_daily_pnl for the per-contract grain.
        --
        -- ``options_realized_today``: cash sum of contracts realizing
        -- TODAY (each closed contract appears ONCE on its close_date
        -- with full net_cash_flow). Cumulated downstream.
        --
        -- ``open_unrealized_today``: SUM of MTM across every open
        -- contract on this date. The per-contract spine is dense
        -- across the open lifetime, so on dates with no open contracts
        -- there are no rows in opd → NULL via LEFT JOIN, which we
        -- coalesce to 0. Point-in-time, NOT cumulated.
        coalesce(opd.realized_today, 0)        as options_realized_today,
        coalesce(opd.open_unrealized_today, 0) as open_unrealized_today,

        -- Price source: fresh broker snapshot today (when present) trumps
        -- yfinance close. yfinance handles every other day. See header
        -- comment for why this asymmetry matters for chart reconciliation.
        case
            when ad.date = current_date()
                 and bt.close_price is not null
                 and bt.close_price > 0
            then bt.close_price
            else p.close_price
        end as close_price,

        -- Legacy diagnostic columns: raw snapshot mark and basis.
        -- Charts must NOT add these to options P&L — they're already
        -- folded into ``open_unrealized_today`` above with the right
        -- sign convention. Kept exposed so the reconcile audit script
        -- and existing dashboards keep functioning.
        o.option_market_value,
        o.option_cost_basis,

        -- "has_trade" = at least one real trade or dividend on this date
        -- (vs price-only rows). Including dividends here is intentional:
        -- a div-only day is meaningful activity for the chart legend.
        case
            when td.date is not null then true
            when dd.date is not null then true
            else false
        end as has_trade

    from all_dates ad
    left join trade_daily td
        on ad.account = td.account
        and (ad.user_id is not distinct from td.user_id)
        and ad.symbol = td.symbol
        and ad.date = td.date
    left join dividend_daily dd
        on ad.account = dd.account
        and (ad.user_id is not distinct from dd.user_id)
        and ad.symbol = dd.symbol
        and ad.date = dd.date
    left join prices p
        on ad.account = p.account
        and ad.symbol = p.symbol
        and ad.date = p.date
    -- Broker today price: keyed by (account, user_id, symbol, date).
    -- Only contributes when ad.date = current_date() AND broker snapshot
    -- is fresh (= today). For all other dates this join produces null and
    -- the case expression above falls through to yfinance.
    left join broker_today_prices bt
        on ad.account = bt.account
        and (ad.user_id is not distinct from bt.user_id)
        and ad.symbol = bt.symbol
        and ad.date = bt.date
    left join daily_option o
        on ad.account = o.account
        and (ad.user_id is not distinct from o.user_id)
        and ad.symbol = o.symbol
        and ad.date = o.date
    left join options_pnl_per_day opd
        on ad.account = opd.account
        and (ad.user_id is not distinct from opd.user_id)
        and ad.symbol = opd.symbol
        and ad.date = opd.date
),

-- Carry forward latest snapshot option values so every date (on or
-- after first snapshot) has option P&L from snapshots. Window keyed
-- by (account, user_id, symbol) so two tenants can't share a fill.
filled as (
    select
        account,
        user_id,
        symbol,
        date,
        options_amount,
        dividends_amount,
        equity_buy_cost,
        equity_buy_qty,
        equity_sell_proceeds,
        equity_sell_qty,
        other_amount,
        last_value(close_price ignore nulls) over (
            partition by account, user_id, symbol order by date
            rows between unbounded preceding and current row
        ) as close_price,
        has_trade,
        last_value(option_market_value ignore nulls) over (
            partition by account, user_id, symbol order by date
            rows between unbounded preceding and current row
        ) as option_market_value,
        last_value(option_cost_basis ignore nulls) over (
            partition by account, user_id, symbol order by date
            rows between unbounded preceding and current row
        ) as option_cost_basis,
        -- ``cumulative_options_pnl`` is now realize-on-close cumulative.
        -- Each closed contract's realized P&L lands ONCE on close_date
        -- and persists. Pre-fix this was sum(stg_history.amount over
        -- date) which credited STO premium on STO date — see
        -- int_option_contract_daily_pnl docstring for why that was
        -- wrong.
        sum(options_realized_today) over w as cumulative_options_pnl,
        -- ``open_options_unrealized_pnl`` is point-in-time MTM of all
        -- currently-open contracts at this date. The per-contract
        -- spine is dense (one row per day per open contract via
        -- generate_date_array), so mart-level just passes through
        -- whatever options_pnl_per_day produced. NO carry-forward at
        -- this layer — that would mistakenly persist MTM after the
        -- last open contract closed (the per-contract spine
        -- terminates at close_date).
        open_unrealized_today as open_options_unrealized_pnl,
        sum(dividends_amount) over w  as cumulative_dividends_pnl,
        sum(other_amount) over w      as cumulative_other_pnl
    from joined
    window w as (partition by account, user_id, symbol order by date)
)

select
    account,
    user_id,
    symbol,
    date,
    options_amount,
    dividends_amount,
    equity_buy_cost,
    equity_buy_qty,
    equity_sell_proceeds,
    equity_sell_qty,
    other_amount,
    close_price,
    has_trade,
    option_market_value,
    option_cost_basis,
    cumulative_options_pnl,
    open_options_unrealized_pnl,
    cumulative_dividends_pnl,
    cumulative_other_pnl
from filled
order by account, user_id, symbol, date
