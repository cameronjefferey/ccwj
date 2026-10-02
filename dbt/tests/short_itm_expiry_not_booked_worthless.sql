/*
    A short (or long) option that is past expiry, with no closing fill,
    must not be booked as if it expired worthless when the expiry close
    is missing or not strictly out of the money.

    Cash-settled index options (SPXW and the rest) settle the next
    morning. The as-of Buy to Close / Sell to Close is the real close.
    Until that row is in history, realizing net_cash_flow reports the
    opening credit as a win (SPXW 7650/7655, Oct 2026: the page showed
    +$2,325.56 while the statement was down $821.08).

    Strictly OTM contracts are excluded: those still realize on the
    expiry close. A priced equity that is ITM or ATM, and an unpriced
    index root, must be status 'Settlement pending' with no close date
    and no booked P&L.
*/

with prices as (
    select
        symbol,
        date,
        any_value(close_price) as close_price
    from {{ ref('stg_daily_prices') }}
    where close_price is not null
    group by 1, 2
)

select
    c.tenant_id,
    c.account,
    c.trade_symbol,
    c.underlying_symbol,
    c.option_expiry,
    c.option_strike,
    c.option_type,
    c.direction,
    c.status,
    c.close_type,
    c.close_date,
    c.realized_pnl,
    c.total_pnl,
    c.premium_received,
    p.close_price as expiry_close
from {{ ref('int_option_contracts') }} c
left join prices p
    on upper(trim(c.underlying_symbol)) = upper(trim(p.symbol))
    and c.option_expiry = p.date
where c.option_expiry < current_date()
  and coalesce(c.contracts_closed, 0) < 1e-6
  and not coalesce(c.opened_before_history, false)
  and (
      (
          p.close_price is not null
          and c.option_strike is not null
          and not (
              (c.option_type = 'C' and p.close_price < c.option_strike)
              or (c.option_type = 'P' and p.close_price > c.option_strike)
          )
      )
      or (
          p.close_price is null
          and upper(trim(coalesce(c.underlying_symbol, ''))) in (
              'SPX', 'SPXW', 'XSP', 'NDX', 'NDXP', 'RUT', 'RUTW',
              'VIX', 'DJX', 'OEX', 'XEO', 'RVX'
          )
      )
  )
  and (
      c.status != 'Settlement pending'
      or c.close_date is not null
      or abs(coalesce(c.realized_pnl, 0)) > 0.01
      or abs(coalesce(c.total_pnl, 0)) > 0.01
  )
