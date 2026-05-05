{#
  Daily snapshot of account-level balances (cash + account total).
  Source: stg_account_balances (export seeds + schwab_account_balances seed).
  Run after each upload so we never lose history; full-refresh does not wipe this table.
#}
{% snapshot snapshot_account_balances_daily %}
{#
  user_id is part of the unique_key so two users with the same account
  label keep separate balance histories — see docs/USER_ID_TENANCY.md.
  coalesce-to-0 keeps Stage 0 legacy rows (user_id NULL pre-backfill)
  from clobbering each other on the snapshot's grain check.
#}
{{
    config(
        target_schema='analytics',
        target_database=target.database,
        unique_key=['account', 'user_id_key', 'row_type'],
        strategy='check',
        check_cols=['market_value', 'cost_basis', 'unrealized_pnl', 'percent_of_account'],
        invalidate_hard_deletes=True,
    )
}}

select
    account,
    user_id,
    coalesce(user_id, -1) as user_id_key,
    row_type,
    market_value,
    cost_basis,
    unrealized_pnl,
    unrealized_pnl_pct,
    percent_of_account,
    current_date() as snapshot_date
from {{ ref('stg_account_balances') }}

{% endsnapshot %}
