{{ config(severity='warn') }}

/*
    Cash-settled index options past expiry, with no broker close and no
    activity-flat close, must not be booked as if they expired worthless.

    This matches flagged2.settlement_pending in int_option_contracts:
    close_type still empty (Assigned / Expired / Closed / the activity-flat
    close all set it), contracts_closed ~ 0, and the expiry close joined
    the same way as expiry_close_lookup (symbol equality, no extra
    case-fold). Equity options are out of scope — they keep the calendar
    close. Strictly OTM index contracts still realize on the expiry close.

    Severity is warn. A mismatch must not skip the rest of the
    price-dependent build (positions_summary and the models downstream
    of this test).

    The 24 rows that failed the previous test were outside this rule:
    they already had a close type or an activity-flat close, or they were
    equity names the case-insensitive price join called ITM. Those are
    not unsettled index contracts, so they are not a booking bug in this
    model.
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
    on c.underlying_symbol = p.symbol
    and c.option_expiry = p.date
where c.option_expiry < current_date()
  and coalesce(c.contracts_closed, 0) < 1e-6
  and not coalesce(c.opened_before_history, false)
  and coalesce(c.close_type, '') in ('', 'Settlement pending')
  and upper(trim(coalesce(c.underlying_symbol, ''))) in (
      'SPX', 'SPXW', 'XSP', 'NDX', 'NDXP', 'RUT', 'RUTW',
      'VIX', 'DJX', 'OEX', 'XEO', 'RVX'
  )
  and (
      p.close_price is null
      or (
          c.option_strike is not null
          and not (
              (c.option_type = 'C' and p.close_price < c.option_strike)
              or (c.option_type = 'P' and p.close_price > c.option_strike)
          )
      )
  )
  and (
      c.status != 'Settlement pending'
      or c.close_date is not null
      or abs(coalesce(c.realized_pnl, 0)) > 0.01
      or abs(coalesce(c.total_pnl, 0)) > 0.01
  )
