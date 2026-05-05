{#
  Daily snapshot of account-level balances (cash + account total).
  Source: stg_account_balances (export seeds + schwab_account_balances seed).
  Run after each upload so we never lose history; full-refresh does not wipe this table.

  user_id is carried as a payload column (not part of unique_key) so we
  preserve the existing snapshot table schema during the user_id-tenancy
  migration — adding user_id to unique_key would invalidate the existing
  snapshot's MERGE predicate and lose every row of history.

  Tenant scoping for snapshot reads is enforced in the app via
  ``_user_scoped_filter`` / ``_filter_df_by_user`` (see
  ``docs/USER_ID_TENANCY.md``). The cross-tenant guard prevents two
  users from simultaneously claiming the same ``account_name`` so the
  ``(account, row_type)`` unique_key remains a valid grain in
  practice. A Stage 4 follow-up can promote ``user_id`` into the
  unique_key once the cross-tenant guard is removed.
#}
{% snapshot snapshot_account_balances_daily %}
{{
    config(
        target_schema='analytics',
        target_database=target.database,
        unique_key=['account', 'row_type'],
        strategy='check',
        check_cols=['market_value', 'cost_basis', 'unrealized_pnl', 'percent_of_account'],
        invalidate_hard_deletes=True,
    )
}}

select
    account,
    user_id,
    row_type,
    market_value,
    cost_basis,
    unrealized_pnl,
    unrealized_pnl_pct,
    percent_of_account,
    current_date() as snapshot_date
from {{ ref('stg_account_balances') }}

{% endsnapshot %}
