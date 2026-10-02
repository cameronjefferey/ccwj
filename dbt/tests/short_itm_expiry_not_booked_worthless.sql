{{ config(severity='warn') }}

/*
    An option with no closing activity settles from the official close
    once the expiry session is over. It must not book the opening credit
    as a worthless win, and it must not wait in 'Settlement pending'.

    With a close:
      status Closed, close_type 'Settled at expiry (est.)', close_date
      is the expiry, realized/total = net_cash_flow + signed intrinsic
      for an ITM cash-settled index (else + $0). SPXW uses the SPX close
      when SPXW itself has no price row.

    Without a close: stay Open, no close date, no booked P&L.

    A broker close (contracts_closed > 0) must not keep the estimate
    label, and its realized P&L must stay net_cash_flow so the intrinsic
    is not added a second time.
*/

with prices as (
    select
        symbol,
        date,
        any_value(close_price) as close_price
    from {{ ref('stg_daily_prices') }}
    where close_price is not null
    group by 1, 2
),

resolved as (
    select
        c.*,
        coalesce(exact.close_price, parent.close_price) as expiry_close
    from {{ ref('int_option_contracts') }} c
    left join prices exact
        on upper(trim(c.underlying_symbol)) = upper(trim(exact.symbol))
        and c.option_expiry = exact.date
    left join prices parent
        on exact.close_price is null
        and c.option_expiry = parent.date
        and upper(trim(parent.symbol)) = case upper(trim(c.underlying_symbol))
            when 'SPXW' then 'SPX'
            when 'NDXP' then 'NDX'
            when 'RUTW' then 'RUT'
        end
),

gated as (
    select
        *,
        (
            option_expiry < current_date('America/New_York')
            or (
                option_expiry = current_date('America/New_York')
                and time(current_datetime('America/New_York')) >= (
                    case
                        when upper(trim(coalesce(underlying_symbol, ''))) in (
                            'SPX', 'SPXW', 'XSP', 'NDX', 'NDXP', 'RUT', 'RUTW',
                            'VIX', 'DJX', 'OEX', 'XEO', 'RVX'
                        )
                        then time '16:15:00'
                        else time '16:00:00'
                    end
                )
            )
        ) as session_over,
        case
            when upper(trim(coalesce(underlying_symbol, ''))) not in (
                'SPX', 'SPXW', 'XSP', 'NDX', 'NDXP', 'RUT', 'RUTW',
                'VIX', 'DJX', 'OEX', 'XEO', 'RVX'
            ) then 0.0
            else round(
                (case when direction = 'Sold' then -1.0 else 1.0 end)
                * greatest(
                    case
                        when option_type = 'C' then expiry_close - option_strike
                        when option_type = 'P' then option_strike - expiry_close
                        else 0.0
                    end,
                    0.0
                )
                * 100.0
                * coalesce(
                    case
                        when direction = 'Sold' then contracts_sold_to_open
                        else contracts_bought_to_open
                    end,
                    0.0
                ),
                2
            )
        end as est_cash
    from resolved
    where not coalesce(opened_before_history, false)
      and option_expiry is not null
      and coalesce(contracts_closed, 0) < 1e-6
)

select
    tenant_id,
    account,
    trade_symbol,
    underlying_symbol,
    option_expiry,
    status,
    close_type,
    close_date,
    realized_pnl,
    total_pnl,
    net_cash_flow,
    expiry_close,
    'estimate' as violation
from gated
where session_over
  and expiry_close is not null
  and option_strike is not null
  and coalesce(close_type, '') not in (
      'Expired', 'ExpiredOTM', 'Assigned', 'Exercised', 'Closed'
  )
  and (
      status != 'Closed'
      or close_type != 'Settled at expiry (est.)'
      or close_date != option_expiry
      or abs(coalesce(realized_pnl, 0) - (coalesce(net_cash_flow, 0) + est_cash)) > 0.05
      or abs(coalesce(total_pnl, 0) - (coalesce(net_cash_flow, 0) + est_cash)) > 0.05
  )

union all

select
    tenant_id,
    account,
    trade_symbol,
    underlying_symbol,
    option_expiry,
    status,
    close_type,
    close_date,
    realized_pnl,
    total_pnl,
    net_cash_flow,
    expiry_close,
    'unpriced' as violation
from gated
where session_over
  and expiry_close is null
  and coalesce(close_type, '') not in (
      'Expired', 'ExpiredOTM', 'Assigned', 'Exercised', 'Closed'
  )
  and (
      status = 'Closed'
      or close_type = 'Settled at expiry (est.)'
      or close_date is not null
      or abs(coalesce(realized_pnl, 0)) > 0.01
  )

union all

select
    c.tenant_id,
    c.account,
    c.trade_symbol,
    c.underlying_symbol,
    c.option_expiry,
    c.status,
    c.close_type,
    c.close_date,
    c.realized_pnl,
    c.total_pnl,
    c.net_cash_flow,
    cast(null as float64) as expiry_close,
    'broker_dedupe' as violation
from {{ ref('int_option_contracts') }} c
where not coalesce(c.opened_before_history, false)
  and coalesce(c.contracts_closed, 0) >= 1e-6
  and c.status = 'Closed'
  and (
      c.close_type = 'Settled at expiry (est.)'
      or abs(coalesce(c.realized_pnl, 0) - coalesce(c.net_cash_flow, 0)) > 0.05
  )
