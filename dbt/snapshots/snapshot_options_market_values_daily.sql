{#
  Daily snapshot of option positions' market values.
  Source: stg_current, filtered to options only (Call/Put).
  Run after each upload to preserve history; full-refresh does not wipe this table.
#}
{% snapshot snapshot_options_market_values_daily %}
{{
    config(
        target_schema='analytics',
        target_database=target.database,
        unique_key=['account', 'trade_symbol'],
        strategy='check',
        check_cols=['market_value', 'quantity', 'cost_basis', 'current_price'],
        invalidate_hard_deletes=True,
    )
}}

select
    account,
    trade_symbol,
    underlying_symbol,
    option_expiry,
    option_strike,
    option_type,
    instrument_type,
    description,
    quantity,
    current_price,
    market_value,
    cost_basis,
    unrealized_pnl,
    unrealized_pnl_pct,
    current_date() as snapshot_date
from {{ ref('stg_current') }}
where instrument_type in ('Call', 'Put')

{% endsnapshot %}
