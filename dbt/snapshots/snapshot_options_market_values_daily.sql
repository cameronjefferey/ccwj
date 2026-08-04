{#
  Daily snapshot of option positions' market values.
  Source: stg_current, filtered to options only (Call/Put).
  Run after each upload to preserve history; full-refresh does not wipe this table.

  ``unique_key`` leads with ``tenant_id`` (the broker-stable v2 grain)
  because the ``account`` display label is NOT unique: a SINGLE user can
  link several physical accounts that all surface as "Schwab Account"
  (SnapTrade returned no distinct masked number, so every one collapses
  to the same label + ``user_id``). With the old
  ``(account, user_id, trade_symbol)`` key, two tenants of the same user
  holding the same contract collide on one target row and the MERGE blows
  up with ``UPDATE/MERGE must match at most one source row for each target
  row`` — which silently ABORTS the whole non-price dbt build and freezes
  ``positions_summary`` + the weekly/benchmark/strategy marts (real case
  2026-07-30..08-04: user 18 held ``JPM 260807C00355000`` in two "Schwab
  Account" tenants). Same fix + rationale as
  ``snapshot_account_balances_daily`` — see that snapshot's docstring.
  ``account``/``user_id`` stay in the key as a fallback for legacy/demo
  rows that pre-date ``tenant_id`` (NULL tenant_id collapses on the label).

  ``coalesce(user_id, -1)`` keeps legacy rows that pre-date the
  ``user_id`` column from breaking the MERGE (NULL = NULL is false in a
  MERGE predicate). The sentinel only appears in BQ, never in app
  reads — Flask filters by the real ``users.id``.
#}
{#
  target_schema follows the build target (CI/prod -> analytics, local
  dev -> analytics_dev) so dev builds never MERGE into the prod
  snapshot table. See snapshot_account_balances_daily.sql.
#}
{% snapshot snapshot_options_market_values_daily %}
{{
    config(
        target_schema=target.schema,
        target_database=target.database,
        unique_key=['tenant_grain', 'user_id', 'trade_symbol'],
        strategy='check',
        check_cols=['market_value', 'quantity', 'cost_basis', 'current_price'],
        invalidate_hard_deletes=True,
    )
}}

select
    account,
    tenant_id,
    -- Broker-stable grain for the unique_key. ``tenant_id`` is the v2
    -- isolation key; fall back to ``account`` for legacy/demo rows that
    -- pre-date it so the MERGE predicate never sees a NULL key column
    -- (NULL = NULL is false in a MERGE and would re-insert every run).
    coalesce(nullif(trim(tenant_id), ''), account) as tenant_grain,
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
