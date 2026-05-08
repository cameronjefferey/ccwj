{#
  Daily snapshot of option positions' market values.
  Source: stg_current, filtered to options only (Call/Put).
  Run after each upload to preserve history; full-refresh does not wipe this table.

  ``unique_key`` includes ``user_id`` because the cross-tenant guard
  has been removed: two users may legitimately register the same
  ``account_name`` (parent monitoring child's "Schwab Account", a test
  user re-using a real label, etc.) and the snapshot grain must keep
  their option positions apart. Same Stage 4 promotion as
  ``snapshot_account_balances_daily`` — see that snapshot's docstring
  for the long-form rationale.

  ``coalesce(user_id, -1)`` keeps legacy rows that pre-date the
  ``user_id`` column from breaking the MERGE (NULL = NULL is false in a
  MERGE predicate). The sentinel only appears in BQ, never in app
  reads — Flask filters by the real ``users.id``.
#}
{% snapshot snapshot_options_market_values_daily %}
{{
    config(
        target_schema='analytics',
        target_database=target.database,
        unique_key=['account', 'user_id', 'trade_symbol'],
        strategy='check',
        check_cols=['market_value', 'quantity', 'cost_basis', 'current_price'],
        invalidate_hard_deletes=True,
    )
}}

select
    account,
    coalesce(user_id, -1) as user_id,
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
