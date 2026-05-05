{#
  Daily snapshot of option positions' market values.
  Source: stg_current, filtered to options only (Call/Put).
  Run after each upload to preserve history; full-refresh does not wipe this table.

  user_id is carried as a payload column (not part of unique_key) so we
  preserve the existing snapshot table schema during the user_id-tenancy
  migration — adding user_id to unique_key would invalidate the existing
  snapshot's MERGE predicate and lose every row of history.

  Tenant scoping for snapshot reads is enforced in the app via
  ``_user_scoped_filter`` / ``_filter_df_by_user`` (see
  ``docs/USER_ID_TENANCY.md``). The cross-tenant guard prevents two
  users from simultaneously claiming the same ``account_name`` so the
  ``(account, trade_symbol)`` unique_key remains a valid grain in
  practice. A Stage 4 follow-up can promote ``user_id`` into the
  unique_key once the cross-tenant guard is removed.
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
    user_id,
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
