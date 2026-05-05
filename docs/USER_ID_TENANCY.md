# User-ID Tenancy — Architecture, Migration, and Rollback

## Why this exists

Until this change, the BigQuery dataset's only tenant key was a free-form string
column — `account` (a.k.a. `account_name` in Postgres `user_accounts`). The
trader sets it during CSV upload or it comes from Schwab's API name on connect.
That means two users picking the same label (e.g. `investment1`) collide in
BigQuery: any query filtered as `WHERE account = 'investment1'` returns rows
from **both** users.

This actually happened in production: see commit history around `Tenant
isolation: enforce one account label = one user` and the Render cron logs that
exposed `investment1` claimed by more than one `user_id` row in
`user_accounts`.

The Postgres unique index `uniq_user_accounts_global_account_name`
(installed by `_migrate_account_name_unique_index` in `app/models.py`)
prevents the **claim** of a duplicate label, and the request-time
`find_cross_tenant_account_conflicts` check (in `_user_account_list`) hides
the data path when a duplicate sneaks through. Both are belt-and-suspenders
hacks around the underlying flaw: `account_name` is not a tenant key.

## Goal

Make `(user_id, account_name)` the BigQuery tenant key. After this change:

- Every user-tied row in BQ carries a `user_id` (Postgres SERIAL from `users.id`).
- Every BQ query Flask issues filters `WHERE user_id = <current_user.id>` — and
  *also* by the user's chosen `account_name` filter when the dropdown is set,
  but the user-id filter is the security boundary.
- Two users with the same `account_name` is a **non-event** — they have
  different `user_id`s, the rows never join.
- `account_name` becomes a per-user display label, no longer a tenant key.
  The cross-tenant guard and the global unique index become belt-and-suspenders
  rather than load-bearing.

## Sequencing (this is the entire reason this doc exists)

You CANNOT flip the filter on day one. The order matters:

### Stage 0 — Plumbing (this branch's first commits)
- Add `user_id` (nullable `INT64`) column to every user-tied seed CSV
  (`trade_history`, `current_positions`, `schwab_account_balances`,
  `demo_history`, `demo_current`).
- Update `dbt/seeds/schema.yml` to declare the column.
- Update Schwab sync (`app/schwab.py::_run_sync`) and CSV upload
  (`app/upload.py::merge_and_push_seeds`) to write `user_id` on every new row
  going forward.
- Surface `user_id` in `stg_*` models (cast to `INT64`, `NULL` when seed cell
  is empty so legacy rows don't crash the load).

**Behavior unchanged.** App still filters on `account_name` only. dbt still
builds. Existing rows have `user_id IS NULL` — they're tolerated, not yet
filtered on.

### Stage 1 — Backfill
- Run `scripts/backfill_seed_user_ids.py` against prod's `user_accounts` table:
  for each row in each seed CSV, look up `account_name → user_id` and write
  it back to GitHub.
- Pre-condition: cross-tenant duplicates resolved (see
  `tests/test_data_isolation.py::TestCrossTenantAccountConflictGuard` and
  the operator SQL in
  `app/models.py::_migrate_account_name_unique_index`'s logged hint).
  Backfill skips ambiguous rows (label owned by >1 user) and prints the count
  so the operator can resolve before re-running.
- After this, every row has a `user_id`. Cron's incremental sync continues
  to write fresh rows with `user_id`.

### Stage 2 — Propagate through dbt
- Pass `user_id` through every intermediate (`int_*`) and mart (`mart_*` /
  `positions_summary` / `snapshot_options_market_values_daily`).
- Audit every `GROUP BY` that includes `account` to also include `user_id`.
  Otherwise an aggregation across the same-name accounts of two users would
  collapse them into one row again — a different shape of leak.
- Audit every `JOIN ... ON account = ...` to also match on `user_id`.
  Otherwise int_dividends + positions_summary could cross-join across users.

### Stage 3 — Switch the filter
- New helper `_user_scoped_filter(user_id, accounts, col="account",
  user_col="user_id")` produces
  `WHERE (user_id = X OR user_id IS NULL) AND TRIM(CAST(account AS STRING)) IN (...)`.
  The `OR user_id IS NULL` leg is the Stage 0/1 leniency for legacy
  rows that haven't been backfilled yet — it drops in Stage 4.
- New helper `_filter_df_by_user(df, user_id, accounts)` filters DataFrames
  by both columns, dropping rows whose `user_id` is a different populated
  id and keeping NULL-`user_id` rows only when the row's `account` is
  in the user's allowed list.
- The legacy `_account_sql_filter`, `_account_sql_and`, and
  `_filter_df_by_accounts` helpers are kept and now **internally**
  resolve the current user via Flask-Login and call the new
  user-scoped helpers. This means every existing call site picks up
  the security upgrade without an audit. Admin and unauthenticated
  paths get `user_id=None` and behave as before.
- When `col` is qualified (e.g. `sc.account`), the helper auto-prefixes
  `user_id` with the same alias so JOINs aren't ambiguous.
- The cross-tenant guard becomes informational (logs but doesn't
  strip) — at the data layer the user-id predicate makes
  `account_name` collisions a non-event.

### Stage 4 — Clean up
- Drop the cross-tenant request-time guard (it's belt-on-belt now).
- Optionally drop `uniq_user_accounts_global_account_name` (still useful
  for UX so two users don't pick the same display label, but no longer
  load-bearing for security).

## Why nullable user_id (during Stage 0/1)

dbt seed loads are strict — if `schema.yml` declares a column the CSV
doesn't have, the load fails. Going `NOT NULL` on day one means the next
deploy would break `dbt build` until every CSV cell is backfilled in the
same commit. Nullable lets us:

- Land the writer change without breaking CI.
- Backfill at the operator's pace.
- Keep query semantics unchanged until Stage 3 explicitly flips them.

`NOT NULL` (and the corresponding `WHERE user_id IS NOT NULL` defenses)
land in Stage 4 once we've verified zero NULLs across all seeds.

## Demo data

The demo user is shared (everyone logs in as `demo`). Demo seeds carry the
demo user's `user_id`. The "All Accounts" view for the demo user filters
exactly to that `user_id` — same model as a real user, just with one
`user_id` instead of one-per-real-trader. Demo `account_name` values
(`Demo Account` etc.) stay as-is.

## Admin path

`is_admin(current_user.username)` returns `None` from `_user_account_list`
(meaning "no filter"). Under user-id tenancy, admin queries pass
`user_id=None` to `_user_scoped_filter`, which emits **no** user-id
predicate. Behavior unchanged: admins see everything.

## Rollback

Each stage's commit is independently revertable:

- Stage 0 revert → seeds drop the `user_id` column → dbt re-loads cleanly
  with the old shape. App is already not filtering on it, so no behavior
  difference.
- Stage 1 revert → just stop running the backfill. user_id values stay
  populated where written; new rows from cron continue to populate them.
  No revert needed at the data layer.
- Stage 2 revert → revert the dbt model commits. Marts go back to the old
  shape. App must also revert to Stage 1 helpers.
- Stage 3 revert → flip helpers back to `account_name`-only filtering. The
  cross-tenant guard is still in place from prior commits, so we're back
  to the previous safety stance.

The point of staging is that we're never one bad commit away from a
broken `dbt build` or a leaking page.

## Tests

`tests/test_data_isolation.py::TestUserIdTenancyHelpers` covers Stage 3:

- `_user_scoped_filter` emits a `user_id = X OR user_id IS NULL`
  predicate alongside the account filter, qualifies the alias when
  `col` is qualified, and fails closed on empty account lists.
- `_filter_df_by_user` drops rows whose `user_id` is a different
  populated id, keeps NULL-`user_id` legacy rows when their `account`
  is in the allowed list, drops NULL-`user_id` rows whose account is
  *not* in the list, and bypasses the user check for admin.
- A direct regression for the original `investment1` incident: a
  DataFrame containing both users' rows is reduced to each user's
  own rows when filtered with their respective `user_id`, with
  no cross-tenant bleed.

## Daily price loader

`current_position_stock_price.py` writes
`ccwj-dbt.analytics.daily_position_performance` (the source for
`stg_daily_prices`) with a `user_id` column. `stg_daily_prices.sql`
guards the column with `adapter.get_columns_in_relation` so the model
keeps building during the cron-rebuild gap on the very first deploy
(when the source table hasn't been rewritten with the new schema yet).

## What this doesn't fix

- **Two users with the same Schwab `account_number`** (the cron's
  `User 5/6/7 (52293852)` log line). User-id tenancy makes this a
  non-issue for *data isolation* — each user's BQ rows have their own
  `user_id` so they don't see each other's. But it doesn't tell you whether
  three users on the same brokerage account is intentional or test cruft.
  That's a separate cleanup conversation.

- **Display nicknames colliding across users** (cosmetic, not a security
  issue). Handled by `app/__init__.py::_account_label_filter` and
  unchanged here.
