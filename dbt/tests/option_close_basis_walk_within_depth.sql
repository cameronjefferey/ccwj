-- depends_on: {{ ref('int_option_contract_daily_pnl') }}

{#
  The option basis walk is unrolled to var('option_close_basis_max_steps')
  close dates per OCC contract. A longer chain would drop the tail and
  mis-date realized P&L. Fail the build instead. One slot is reserved
  for a calendar settlement that has no broker close row on that date.
#}

with history_closes as (
    select
        tenant_id,
        account,
        user_id,
        trade_symbol,
        count(distinct least(trade_date, coalesce(option_expiry, trade_date))) as close_dates
    from {{ ref('stg_history') }}
    where instrument_type in ('Call', 'Put')
      and action in (
          'option_buy_to_close',
          'option_sell_to_close',
          'option_expired',
          'option_assigned',
          'option_exercised'
      )
    group by 1, 2, 3, 4
)

select
    tenant_id,
    account,
    user_id,
    trade_symbol,
    close_dates
from history_closes
where close_dates > {{ var('option_close_basis_max_steps') - 1 }}
