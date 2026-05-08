{#
  Daily snapshot of account-level balances (cash + account total).
  Source: stg_account_balances (export seeds + schwab_account_balances seed).
  Run after each upload so we never lose history; full-refresh does not wipe this table.

  ``unique_key`` includes ``user_id`` because the cross-tenant guard
  has been removed: two users may legitimately register the same
  ``account_name`` (parent monitoring child's "Schwab Account", a test
  user re-using a real label, etc.) and the snapshot grain must keep
  their balances apart. With the old ``(account, row_type)`` key, a
  fresh sync that produced two rows for the same account label under
  different ``user_id``s blew up the MERGE with
  ``UPDATE/MERGE must match at most one source row for each target row``.

  ``coalesce(user_id, -1)`` keeps legacy rows that pre-date the
  ``user_id`` column from breaking the MERGE (NULL is never equal to
  NULL in a MERGE predicate). The sentinel only appears in BQ, never
  in app reads — Flask filters by the real ``users.id``.

  Tenant scoping for snapshot reads is still enforced in the app via
  ``_user_scoped_filter`` / ``_filter_df_by_user`` (see
  ``docs/USER_ID_TENANCY.md``). This change just makes the warehouse
  side honest about the grain.
#}
{% snapshot snapshot_account_balances_daily %}
{{
    config(
        target_schema='analytics',
        target_database=target.database,
        unique_key=['account', 'user_id', 'row_type'],
        strategy='check',
        check_cols=['market_value', 'cost_basis', 'unrealized_pnl', 'percent_of_account'],
        invalidate_hard_deletes=True,
    )
}}

-- ``user_id`` is part of the unique_key now, but dbt's MERGE predicate
-- treats NULL = NULL as false, so legacy rows with no ``user_id`` would
-- be re-inserted on every run and produce duplicates. Backfill NULLs to
-- ``-1`` here so the MERGE is well-defined; ``-1`` never matches a real
-- ``users.id`` (PG sequences start at 1) so app-side tenant filters
-- treat it as "unowned legacy" the same way they always have.
select
    account,
    coalesce(user_id, -1) as user_id,
    row_type,
    market_value,
    cost_basis,
    unrealized_pnl,
    unrealized_pnl_pct,
    percent_of_account,
    current_date() as snapshot_date
from {{ ref('stg_account_balances') }}

{% endsnapshot %}
