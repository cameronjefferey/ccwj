{{
    config(
        materialized='table'
    )
}}

/*
    Inferred opening equity balances — one row per (tenant, account, user,
    symbol) whose trade history starts MID-POSITION.

    Why this exists (Aug 2026 classification audit): SnapTrade/Schwab only
    backfill a limited transaction window, and share TRANSFER rows are
    deliberately dropped (ambiguous). A trader with 5+ years of history —
    the NORMAL first-interaction case for this product — shows up with
    positions whose buys predate the window. Without an opening balance:

      - running share counts go NEGATIVE (UBER/jefflsmith: history nets to
        -800 shares) and the sells are silently dropped from sessions;
      - sells of pre-window shares land in a tiny in-window session
        (IYW/jefflsmith: a "2.7-share" session carrying $215K of phantom
        P&L — proceeds with no matching cost);
      - covered/naked call judgments are made against a ledger that says
        the trader holds nothing;
      - dividends on pre-window holdings are never synthesized.

    92 positions across 4 users were affected at audit time.

    THE QUANTITY IS PROVABLE; ONLY THE COST IS ESTIMATED. The deficit is
    exact arithmetic (broker snapshot + history are both known). The
    opening COST uses a confidence ladder, best first:

      1. broker_cost_basis — position still held: per-share basis from
         stg_current (cost_basis / quantity). Exact in broker terms, and
         keeps int_equity_sessions' open-session realized formula
         (total_buy_cost - cost_basis_remaining) consistent by
         construction.
      2. market_close — position fully closed pre-window (no broker basis
         exists): last stg_daily_prices close on/before the opening date,
         else the earliest close after it. An estimate, disclosed in the
         UI (Position Detail "history starts here" banner).
      3. first_fill — no price data at all: per-share price of the first
         in-window fill. Weakest; still disclosed.

    Deficit definition (all in TODAY's share-units, split-adjusted):

      opening_qty = greatest(
          current_shares - net_history_shares,   -- snapshot reconciliation
          -min_running_qty,                      -- floor: running never < 0
          0
      )

    The two signals cover complementary cases: a still-held pre-window
    position needs the first term (IYW); a pre-window position that was
    fully closed in-window needs the second (UBER, current = 0).

    Guards:
      - Symbols with ANY equity_sell_short fill are skipped — a genuinely
        short position legitimately runs negative and must not be
        "corrected" into a synthetic long.
      - Symbols with NO history rows at all are skipped — the existing
        snapshot_equity_sessions path in int_equity_sessions already
        handles those with the broker's real cost basis, which beats any
        estimate we could synthesize.
      - Deficits under 0.01 shares are float noise, not positions.

    Units: opening quantity is emitted in TODAY's share-units (it is
    derived from today-unit inputs). int_equity_fills passes it through
    WITHOUT re-applying split factors. For market_close pricing the close
    price is as-reported (fill-date units — the price loader deliberately
    un-adjusts yfinance's back-adjustment), so cash = (qty_today /
    factor_at_price_date) * close.

    Consumed by int_equity_fills (which every running-quantity model reads)
    and by the Position Detail banner query in app/position_detail.py.
*/

with equity_fills as (
    select
        h.tenant_id,
        h.account,
        h.user_id,
        h.underlying_symbol as symbol,
        h.trade_date,
        h.action,
        h.quantity,
        h.amount,
        h.quantity * coalesce(sf.cumulative_split_factor, 1.0) as qty_today,
        case
            when h.action = 'equity_buy'
                then  h.quantity * coalesce(sf.cumulative_split_factor, 1.0)
            when h.action in ('equity_sell', 'equity_sell_short')
                then -h.quantity * coalesce(sf.cumulative_split_factor, 1.0)
            else 0
        end as signed_qty_today
    from {{ ref('stg_history') }} h
    left join {{ ref('int_split_factors') }} sf
        on  sf.symbol     = h.underlying_symbol
        and sf.trade_date = h.trade_date
    where h.instrument_type = 'Equity'
      and h.action in ('equity_buy', 'equity_sell', 'equity_sell_short')
      and h.trade_date is not null
      and trim(coalesce(h.underlying_symbol, '')) != ''
),

running as (
    select
        *,
        sum(signed_qty_today) over (
            partition by tenant_id, account, user_id, symbol
            order by trade_date, action
            rows between unbounded preceding and current row
        ) as running_qty
    from equity_fills
),

per_symbol as (
    select
        tenant_id,
        account,
        user_id,
        symbol,
        min(trade_date)                                            as first_trade_date,
        sum(signed_qty_today)                                      as net_history_qty,
        min(running_qty)                                           as min_running_qty,
        countif(action = 'equity_sell_short')                      as short_fills,
        -- first-fill per-share price (that-date units) — weakest cost fallback
        array_agg(
            safe_divide(abs(amount), nullif(quantity, 0))
            order by trade_date, action
            limit 1
        )[safe_offset(0)]                                          as first_fill_price
    from running
    group by 1, 2, 3, 4
),

current_holdings as (
    select
        tenant_id,
        account,
        user_id,
        underlying_symbol as symbol,
        sum(coalesce(quantity, 0))   as current_qty,
        sum(coalesce(cost_basis, 0)) as current_cost_basis
    from {{ ref('stg_current') }}
    where instrument_type = 'Equity'
    group by 1, 2, 3, 4
),

deficits as (
    select
        p.tenant_id,
        p.account,
        p.user_id,
        p.symbol,
        p.first_trade_date,
        date_sub(p.first_trade_date, interval 1 day) as opening_date,
        p.net_history_qty,
        p.min_running_qty,
        p.first_fill_price,
        coalesce(c.current_qty, 0)        as current_qty,
        coalesce(c.current_cost_basis, 0) as current_cost_basis,
        greatest(
            coalesce(c.current_qty, 0) - p.net_history_qty,
            -p.min_running_qty,
            0
        ) as opening_qty
    from per_symbol p
    left join current_holdings c
        on  p.account = c.account
        and (p.user_id is not distinct from c.user_id)
        and (p.tenant_id is not distinct from c.tenant_id)
        and p.symbol = c.symbol
    where p.short_fills = 0
),

needs_opening as (
    select * from deficits where opening_qty > 0.01
),

-- Symbol-level daily closes (as-reported, fill-date units). stg_daily_prices
-- is (account, user_id, symbol, date)-grained; close_price is identical
-- across accounts for a (symbol, date), so max() is a safe collapse.
symbol_prices as (
    select symbol, date, max(close_price) as close_price
    from {{ ref('stg_daily_prices') }}
    where coalesce(close_price, 0) > 0
    group by 1, 2
),

-- For positions NOT currently held: pick the close nearest the opening
-- date — prefer the latest close ON OR BEFORE it, else the earliest after.
market_price_pick as (
    select
        n.tenant_id,
        n.account,
        n.user_id,
        n.symbol,
        p.date        as price_date,
        p.close_price as close_price
    from needs_opening n
    join symbol_prices p
        on p.symbol = n.symbol
    qualify row_number() over (
        partition by n.tenant_id, n.account, n.user_id, n.symbol
        order by
            case when p.date <= n.opening_date then 0 else 1 end,
            case when p.date <= n.opening_date then p.date end desc,
            p.date asc
    ) = 1
),

-- Forward split factor at the chosen price date (splits strictly after it),
-- computed inline from stg_split_events: int_split_factors only has rows
-- for dates that appear in stg_history, and the opening/price dates
-- generally don't.
market_price_factor as (
    select
        mp.tenant_id,
        mp.account,
        mp.user_id,
        mp.symbol,
        mp.price_date,
        mp.close_price,
        coalesce(
            exp(sum(
                case
                    when s.split_date is not null and s.split_date > mp.price_date
                    then ln(s.split_ratio)
                    else 0
                end
            )),
            1.0
        ) as price_date_split_factor
    from market_price_pick mp
    left join {{ ref('stg_split_events') }} s
        on s.symbol = mp.symbol
    group by 1, 2, 3, 4, 5, 6
),

-- Same inline factor for the first-fill fallback (first_trade_date units).
first_fill_factor as (
    select
        n.tenant_id,
        n.account,
        n.user_id,
        n.symbol,
        coalesce(
            exp(sum(
                case
                    when s.split_date is not null and s.split_date > n.first_trade_date
                    then ln(s.split_ratio)
                    else 0
                end
            )),
            1.0
        ) as first_fill_split_factor
    from needs_opening n
    left join {{ ref('stg_split_events') }} s
        on s.symbol = n.symbol
    group by 1, 2, 3, 4
)

select
    n.tenant_id,
    n.account,
    n.user_id,
    n.symbol,
    n.opening_date,
    n.first_trade_date,
    n.opening_qty,                 -- TODAY's share-units
    n.net_history_qty,
    n.min_running_qty,
    n.current_qty,

    -- Cost ladder (see header). est_amount is the signed cash flow of the
    -- synthetic buy (negative = cash out), split-invariant by construction.
    case
        when n.current_qty > 0.01 and n.current_cost_basis > 0
            then 'broker_cost_basis'
        when mpf.close_price is not null
            then 'market_close'
        when n.first_fill_price is not null
            then 'first_fill'
        else 'unpriced'
    end as price_source,

    case
        -- Broker basis: per-share basis across currently-held shares
        -- (today's units — current_qty is a today-unit count).
        when n.current_qty > 0.01 and n.current_cost_basis > 0
            then -n.opening_qty * safe_divide(n.current_cost_basis, n.current_qty)
        -- Market close: close is in price-date units; convert today-unit
        -- qty back to price-date units before multiplying.
        when mpf.close_price is not null
            then -safe_divide(n.opening_qty, mpf.price_date_split_factor) * mpf.close_price
        -- First fill: per-share price in first_trade_date units.
        when n.first_fill_price is not null
            then -safe_divide(n.opening_qty, fff.first_fill_split_factor) * n.first_fill_price
        else 0
    end as est_amount,

    mpf.price_date as market_price_date

from needs_opening n
left join market_price_factor mpf
    on  n.account = mpf.account
    and (n.user_id is not distinct from mpf.user_id)
    and (n.tenant_id is not distinct from mpf.tenant_id)
    and n.symbol = mpf.symbol
left join first_fill_factor fff
    on  n.account = fff.account
    and (n.user_id is not distinct from fff.user_id)
    and (n.tenant_id is not distinct from fff.tenant_id)
    and n.symbol = fff.symbol
