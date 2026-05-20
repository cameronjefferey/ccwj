{{ config(materialized='view') }}

/*
    Dividend Reinvestment (DRIP) detection.

    Schwab Connect emits DRIP fills as plain `Buy` rows in trade
    history, with the SAME description as a real buy ("iShares US
    Technology ETF"). The user's actual intent ("I bought 10 shares
    of IYW with my own cash") is materially different from "Schwab
    took the dividend they just paid me and bought back fractional
    shares". Without separation the position page reads as if the
    user made many tiny chaotic buys when in fact they made one or
    two real buys plus quarterly DRIP.

    Detection signal:
      1) action = 'equity_buy' AND instrument_type = 'Equity'
      2) quantity in (0, 1) — fractional, but > 0
      3) trade_date is the broker's payable date for a recent ex-div
         on the same symbol. We look back up to 30 calendar days
         from the buy: ex-div date → record date → payable date is
         typically 3-21 calendar days for ETFs and most equities.
         Quarterly payers are 3 months apart so a 30-day window
         can't ever match the wrong ex-div.

    (3) is the disambiguator that separates DRIPs from legitimate
    fractional buys. People can buy fractional shares directly from
    Schwab ("buy $50 of AAPL"); those don't land in the
    days-after-ex-div payable window for the same ticker.

    --------------------------------------------------------------
    Why this lives in `intermediate/` and not `staging/stg_history`:

    Detection joins to ``stg_daily_prices`` (yfinance ex-div
    calendar). Putting that join in stg_history would move stg_history
    into ``stg_daily_prices+`` and the CI workflow's two-pass build
    (``dbt build --exclude "stg_daily_prices+"`` then
    ``dbt build --select "stg_daily_prices+"``) would skip stg_history
    (and effectively the whole warehouse) in Pass 1. Keeping
    detection downstream preserves the two-pass invariant: stg_history
    builds in Pass 1 with all its non-price-dependent dependents;
    DRIP detection waits for Pass 2 (when prices have been refreshed
    by current_position_stock_price.py).

    Consumers:
      - `int_dividend_events` joins to use broker-actual amounts in
        place of yfinance synthetic estimates
      - `app/routes.py POSITION_TRADES_QUERY` joins to surface DRIP
        rows as their own action type in the Raw Transaction Log
*/

with ex_div_dates as (
    select distinct
        symbol,
        date as ex_div_date
    from {{ ref('stg_daily_prices') }}
    where coalesce(dividend, 0) > 0
),

candidate_buys as (
    select
        account,
        user_id,
        trade_date,
        underlying_symbol,
        quantity,
        amount
    from {{ ref('stg_history') }}
    where action = 'equity_buy'
      and instrument_type = 'Equity'
      and quantity > 0
      and quantity < 1
)

select
    cb.account,
    cb.user_id,
    -- Stage 2 broker_account_id passthrough.
    any_value(dba.broker_account_id) as broker_account_id,
    cb.trade_date,
    cb.underlying_symbol,
    cb.quantity,
    cb.amount,
    max(edd.ex_div_date) as matched_ex_div_date,
    abs(cb.amount) as drip_amount
from candidate_buys cb
join ex_div_dates edd
    on  edd.symbol      = cb.underlying_symbol
    and edd.ex_div_date <= cb.trade_date
    and edd.ex_div_date >= date_sub(cb.trade_date, interval 30 day)
left join {{ ref('dim_broker_accounts') }} dba
    on cb.account = dba.account_name
    and (cb.user_id is not distinct from dba.user_id)
group by 1, 2, 4, 5, 6, 7
