{{
    config(
        materialized='table'
    )
}}

/*
    Sector movers — recent price move per symbol, with sector context.

    One row per SYMBOL (NOT per tenant). This is pure market data: close
    prices from stg_daily_prices (yfinance) joined to sector/subsector from
    stg_symbol_metadata. The close price for a symbol on a given day is the
    same regardless of which tenant happened to hold it, so we collapse the
    per-(account, user_id, symbol, day) price rows down to one price per
    (symbol, day) before computing the move.

    Powers the "Movers in your sectors" section of the /earnings (Earnings
    Watch) page. The Flask layer takes the CURRENT USER's held sectors
    (tenant-scoped) and filters this symbol-grain table to those sectors —
    so the *selection* is user-scoped even though this table carries no
    tenant columns and is safe to read unscoped (no per-tenant rows exist
    here to leak).

    UNIVERSE LIMIT: stg_daily_prices / stg_symbol_metadata only cover symbols
    SOMEONE on the platform has traded (plus SPY/QQQ benchmarks). This is not
    a whole-market scan. Broader, market-wide discovery is exactly what the
    EarningsFollower deep-link is for; this mart just flags peers we already
    have prices for so the user sees same-sector context without leaving.

    "Recent move" = pct change of the latest close vs the close LOOKBACK_DAYS
    trading-rows earlier (per symbol). We rank by trading rows rather than
    calendar days so weekends/holidays don't null out the comparison.
*/

{% set lookback_rows = 6 %}  -- ~1 trading week back

with prices as (
    -- Collapse to one price per (symbol, day). max() is an arbitrary pick;
    -- all rows for a (symbol, day) carry the same market close.
    select
        upper(trim(symbol)) as symbol,
        date,
        max(close_price)    as close_price
    from {{ ref('stg_daily_prices') }}
    where close_price is not null
      and close_price > 0
    group by 1, 2
),

ranked as (
    select
        symbol,
        date,
        close_price,
        row_number() over (
            partition by symbol order by date desc
        ) as rn_desc
    from prices
),

-- Latest close and the close ~1 trading week earlier, per symbol.
latest as (
    select symbol, date as as_of_date, close_price as latest_close
    from ranked
    where rn_desc = 1
),

prior as (
    select symbol, close_price as prior_close
    from ranked
    where rn_desc = {{ lookback_rows + 1 }}
),

meta as (
    select symbol, sector, subsector, long_name, market_cap
    from {{ ref('stg_symbol_metadata') }}
)

select
    l.symbol,
    l.as_of_date,
    l.latest_close,
    p.prior_close,
    safe_divide(l.latest_close - p.prior_close, p.prior_close) as pct_change,
    abs(safe_divide(l.latest_close - p.prior_close, p.prior_close)) as abs_pct_change,
    coalesce(m.sector, 'Unknown')    as sector,
    coalesce(m.subsector, 'Unknown') as subsector,
    m.long_name,
    m.market_cap
from latest l
join prior p using (symbol)
left join meta m using (symbol)
where p.prior_close is not null
  and p.prior_close > 0
  -- Freshness guard: only symbols whose LATEST close is genuinely recent.
  -- Without this, a symbol that stopped pricing weeks ago compares its stale
  -- "latest" to a row 6 trading-days before THAT, surfacing a misleading
  -- "~1 week move" for a stale name.
  and l.as_of_date >= date_sub(current_date(), interval 10 day)
