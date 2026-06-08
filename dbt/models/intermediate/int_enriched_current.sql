/*
    Current positions enriched with the right "current price".

    Price precedence (from most-trusted to least):

      1) Broker-derived current price = market_value / quantity, when the
         broker snapshot is FRESH (snapshot_date >= current_date - 7).
         This is the broker's own implied current per-share price at the
         time of last sync — by definition consistent with market_value /
         cost_basis / unrealized_pnl that the rest of the model carries
         through unchanged.

      2) yfinance daily close (`stg_daily_prices.close_price`), when
         broker is stale or quantity == 0. yfinance is updated end-of-day,
         which is more current than a 2-week-old broker snapshot but less
         current than today's broker tick.

      3) Broker `current_price` column itself, as a final fallback (covers
         odd cases where market_value is missing but Price isn't).

    Why deriving rather than trusting `current_price` directly:

      Schwab Connect was historically writing the `averagePrice` API field
      (per-share COST basis) into the seed's `Price` column. dbt then maps
      `Price -> current_price`. For ~every Schwab-synced equity row this
      meant `current_price == cost_per_share` and `qty * current_price ==
      cost_basis`, completely hiding unrealized P&L on every UI surface
      that multiplied per-share-price by quantity. The schwab.py side was
      patched alongside this model (see `_schwab_position_cost_basis` /
      "averagePrice mislabeled as Price" entry in
      ~/.cursor/skills/broker-sync-safety/SKILL.md), but we ALSO
      defensively derive `mv / qty` here so the model is correct even if
      a future seed-write regression ever puts the wrong number back into
      `Price`.

    Non-equity rows (options, etc.) pass through unchanged. Options carry
    a contract multiplier (typically 100) that makes `mv / qty`
    semantically different from "per share of underlying" — option
    pricing flows through `int_option_pnl_series` and
    `snapshot_options_market_values_daily` instead.

    INVARIANT (enforced by tests/dbt/int_enriched_current_equity_price_consistent.sql):
      For Equity rows with quantity != 0 and market_value > 0,
      abs(quantity * current_price - market_value) <= $0.01.

    OPTION ROWS — auto-close filter:

      Schwab's snapshot lags actual expiry processing by 1-2 trading
      days, and on expiry day itself the snapshot still carries the
      contract with a stale cost-to-close. ``int_option_contracts``
      resolves both ambiguities upstream — its calendar-truth rule
      realizes past-expiry contracts and the OTM-at-expiry inference
      realizes Friday's worthless options before the Monday sync. We
      mirror that decision here by dropping any option row that
      ``int_option_contracts`` has already marked Closed.

      Without this filter the page double-counts: the chart's live-
      today override reads ``current_df.unrealized_pnl`` and adds the
      stale -$183 mark-to-close on top of the realized credit the
      mart already booked from the auto-close, and the position legs
      table shows the same contract under both "Open" (current_df)
      and "Closed" (int_strategy_classification).
*/

with current_positions as (
    select * from {{ ref('stg_current') }}
),

-- Mirror int_option_contracts' calendar-truth + OTM-at-expiry close
-- decision so option rows already auto-closed by the contracts model
-- don't leak back into "currently held" UI surfaces. The status
-- column is the single source of truth for "is this contract still
-- live"; see int_option_contracts header.
option_contract_status as (
    select
        tenant_id,
        account,
        user_id,
        trade_symbol,
        status
    from {{ ref('int_option_contracts') }}
),

latest_prices as (
    select account, symbol, close_price, date as price_date
    from (
        select
            account, symbol, close_price, date,
            row_number() over (partition by account, symbol order by date desc) as rn
        from {{ ref('stg_daily_prices') }}
    )
    where rn = 1
),

symbol_meta as (
    select * from {{ ref('stg_symbol_metadata') }}
),

priced as (
    select
        cp.*,
        lp.close_price as yf_close,
        lp.price_date as yf_price_date,

        -- Broker freshness gate: last sync within 7d.
        case
            when cp.snapshot_date is not null
                 and cp.snapshot_date >= date_sub(current_date(), interval 7 day)
            then true
            else false
        end as broker_is_fresh,

        -- Broker-implied current per-share price, derived from the same
        -- market_value figure we use everywhere downstream so the math
        -- reconciles. NULL when we can't compute it (qty=0, mv missing, or
        -- mv/qty would round to 0 — which would itself be a stale-snapshot
        -- footgun).
        case
            when cp.instrument_type = 'Equity'
                 and cp.quantity is not null
                 and cp.quantity != 0
                 and cp.market_value is not null
                 and cp.market_value != 0
            then cp.market_value / cp.quantity
        end as broker_implied_price
    from current_positions cp
    left join latest_prices lp
        on cp.account = lp.account
        and cp.underlying_symbol = lp.symbol
)

select
    p.account,
    p.user_id,
    -- v2 tenant_id carried natively from stg_current.
    p.tenant_id,
    p.trade_symbol,
    p.underlying_symbol,
    p.option_expiry,
    p.option_strike,
    p.option_type,
    p.instrument_type,
    p.description,
    p.quantity,

    -- Equity current_price: fresh broker -> yfinance fallback -> raw broker
    case
        when p.instrument_type = 'Equity'
             and p.broker_is_fresh
             and p.broker_implied_price is not null
             and p.broker_implied_price > 0
        then p.broker_implied_price
        when p.instrument_type = 'Equity'
             and p.yf_close is not null
             and p.yf_close > 0
        then p.yf_close
        when p.instrument_type = 'Equity'
             and p.broker_implied_price is not null
             and p.broker_implied_price > 0
        then p.broker_implied_price
        else p.current_price
    end as current_price,

    -- Market value: with the price coming from broker-implied mv/qty, the
    -- multiply-back is identical to cp.market_value (an explicit invariant
    -- we test). When yfinance is the price source (broker stale), we
    -- recompute mv from yfinance because the snapshot mv is also stale by
    -- the same definition.
    case
        when p.instrument_type = 'Equity'
             and p.broker_is_fresh
             and p.broker_implied_price is not null
             and p.broker_implied_price > 0
             and p.quantity is not null
             and p.quantity != 0
        then p.market_value
        when p.instrument_type = 'Equity'
             and p.yf_close is not null
             and p.yf_close > 0
             and p.quantity is not null
             and p.quantity != 0
        then p.quantity * p.yf_close
        else p.market_value
    end as market_value,

    p.cost_basis,

    -- Unrealized P&L: same precedence ladder as price/mv.
    case
        when p.instrument_type = 'Equity'
             and p.broker_is_fresh
             and p.broker_implied_price is not null
             and p.broker_implied_price > 0
             and p.quantity is not null
             and p.quantity != 0
             and p.cost_basis is not null
             and p.cost_basis != 0
        then p.market_value - p.cost_basis
        when p.instrument_type = 'Equity'
             and p.yf_close is not null
             and p.yf_close > 0
             and p.quantity is not null
             and p.quantity != 0
             and p.cost_basis is not null
             and p.cost_basis != 0
        then p.quantity * p.yf_close - p.cost_basis
        else p.unrealized_pnl
    end as unrealized_pnl,

    -- Unrealized P&L % — mirrors unrealized_pnl path, divided by |cost_basis|.
    case
        when p.instrument_type = 'Equity'
             and p.broker_is_fresh
             and p.broker_implied_price is not null
             and p.broker_implied_price > 0
             and p.quantity is not null
             and p.quantity != 0
             and p.cost_basis is not null
             and p.cost_basis != 0
        then 100.0 * (p.market_value - p.cost_basis) / abs(p.cost_basis)
        when p.instrument_type = 'Equity'
             and p.yf_close is not null
             and p.yf_close > 0
             and p.quantity is not null
             and p.quantity != 0
             and p.cost_basis is not null
             and p.cost_basis != 0
        then 100.0 * (p.quantity * p.yf_close - p.cost_basis) / abs(p.cost_basis)
        else p.unrealized_pnl_pct
    end as unrealized_pnl_pct,

    p.security_type_raw,
    p.in_the_money,
    p.dividend_yield,
    p.pe_ratio,
    p.snapshot_date,
    p.yf_price_date as price_date,

    -- Sector / subsector from yfinance (Unknown when missing)
    coalesce(sm.sector, 'Unknown')      as sector,
    coalesce(sm.subsector, 'Unknown')   as subsector,
    sm.long_name                         as company_name

from priced p
left join symbol_meta sm
    on upper(trim(p.underlying_symbol)) = sm.symbol
left join option_contract_status oc
    on p.account = oc.account
    and (p.user_id is not distinct from oc.user_id)
    and (p.tenant_id is not distinct from oc.tenant_id)
    and p.trade_symbol = oc.trade_symbol
where not (
    -- Drop options the contracts model has already realized.
    -- Equity rows (and option rows with no matching contract) pass
    -- through unchanged because the first conjunct is false.
    p.instrument_type in ('Call', 'Put')
    and oc.status = 'Closed'
)
