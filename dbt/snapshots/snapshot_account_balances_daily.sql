{#
  Daily snapshot of account-level balances (cash + account total).
  Source: stg_account_balances (export seeds + schwab_account_balances seed).
  Run after each upload so we never lose history; full-refresh does not wipe this table.
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
    row_type,
    market_value,
    cost_basis,
    unrealized_pnl,
    unrealized_pnl_pct,
    percent_of_account,
    current_date() as snapshot_date
from {{ ref('stg_account_balances') }}

{% endsnapshot %}
