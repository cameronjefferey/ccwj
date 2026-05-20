# Broker-account-id tenancy — design + staged migration

## Why this exists

`docs/USER_ID_TENANCY.md` is the previous tenancy migration. It made
`(user_id, account_name)` the BigQuery tenant key, replacing the
load-bearing reliance on `account_name` alone. That fix shipped — but
production keeps surfacing bugs in the same shape (orphan tenancy, dual
stamps, NULL-vs-populated splits). `docs/USER_ID_TENANCY_EXPLAINER.md`
catalogues why: the warehouse joins on a **user-typeable string label**
(`account_name`) and a denormalized integer (`user_id`), both of which
can drift, collide, or be re-stamped. We then carry a four-CTE staging
backfill, a canonical-owner heuristic, a dedupe-by-stringified-floats
trick, and a runtime invariant card just to keep the two halves
agreeing.

The root cause is one architectural choice: we throw away the
**stable, immutable, broker-provided account identifiers** that
Postgres already has (`schwab_connections.account_hash`,
`snaptrade_accounts.snaptrade_account_id`) and instead re-derive tenancy
from a string label at every layer.

This migration fixes that. It promotes a stable per-broker-account
integer key — `broker_account_id` — to be the warehouse's primary
tenant key, demotes `account_name` to a display column, and demotes
`user_id` to a single-row mapping in a dim table. After it ships:

- Two users sharing an `account_name` is a non-event (already true post
  Stage 4 of the previous migration, now true at a stronger layer too).
- A user re-linking the same broker account under a new uid does NOT
  produce a stale-uid split — the broker_account_id stays the same;
  one Postgres row updates; the warehouse re-attributes on next build.
- A sync running before user-link does NOT produce a NULL→populated
  split — broker_account_id is required at write time and is known at
  connect time (before any sync runs).
- The `account_owner` / `canonical_account_owner` CTEs go away.
- The dedupe-by-stringified-floats trick goes away.
- The `OR user_id IS NULL` leniency goes away.

## What gets keyed by what (target state)

```
Postgres                               BigQuery
────────                               ────────

users (PK id)                          ─── (not exported)
  │
  └──┐
     │
broker_accounts (PK id)                dim_broker_accounts
  ├─ user_id (FK → users.id)             ├─ broker_account_id (PK)
  ├─ broker_slug ENUM                    ├─ user_id
  ├─ broker_external_id                  ├─ broker_slug
  ├─ account_name (display only)         ├─ account_name (display only)
  └─ created_at                          └─ first_seen_at
     │
     └─ stamped into every emitted seed row
        │
        ▼
┌─────────────────────────────────────────────────────────┐
│ trade_history.csv                                       │
│   Account,user_id,broker_account_id,Date,Action,...     │
│ current_positions.csv                                   │
│   Account,user_id,broker_account_id,Symbol,...          │
│ account_balances.csv                                    │
│   account,user_id,broker_account_id,row_type,...        │
└─────────────────────────────────────────────────────────┘
        │
        ▼
   stg_history / stg_current / stg_account_balances
   (broker_account_id passes through unchanged; no
    backfill / canonical resolution / dedupe needed)
        │
        ▼
   every int_* and mart_* model partitions/joins on
   broker_account_id only. user_id + account_name come
   from dim_broker_accounts for display.
        │
        ▼
   Flask filter:
   WHERE broker_account_id IN (
     SELECT broker_account_id
     FROM dim_broker_accounts
     WHERE user_id = :current_user_id
   )
```

`broker_external_id` semantics by `broker_slug`:

| broker_slug | broker_external_id                                  | created at                           |
|-------------|-----------------------------------------------------|--------------------------------------|
| `schwab`    | `schwab_connections.account_hash` (preferred) or `.account_number` | `schwab_callback`                    |
| `snaptrade` | `snaptrade_accounts.snaptrade_account_id`           | `_register_account` in `snaptrade.py`|
| `manual`    | synthetic UUID generated at first upload            | `upload_csv` route                   |
| `demo`      | hard-coded constant per demo account                | one-off seed                         |

The `(broker_slug, broker_external_id)` pair is globally unique. Two
users linking the same physical Schwab account each get their own
`broker_accounts` row (different `user_id`) but the same
`broker_external_id` — which is the right model, because their
warehouse rows ARE different tenants.

`broker_accounts.id` (SERIAL) is what gets stamped into the seed. It's
deliberately NOT `(broker_slug, broker_external_id)` because:

- INT keys join faster than string keys.
- A SERIAL is opaque — the seed doesn't accidentally leak the broker
  hash to anyone reading the CSV.
- Renaming/re-keying the external identifier (broker API migration,
  SnapTrade slug rename) is a one-row UPDATE that doesn't touch the
  warehouse.

## Sequencing (this is the entire reason this doc exists)

Same pattern as `docs/USER_ID_TENANCY.md`. You **CANNOT** flip the
filter on day one.

### Stage 0 — Plumbing (this PR)

- Add `broker_accounts` Postgres table.
- Add nullable `broker_account_id` FK columns to `schwab_connections`,
  `snaptrade_accounts`, `uploads`.
- Add `broker_account_id` column (nullable STRING) to every user-tied
  seed CSV (`trade_history`, `current_positions`, `account_balances`,
  `demo_history`, `demo_current`).
- Update `dbt/seeds/schema.yml` to declare the column.
- Update writers:
  - `merge_and_push_seeds` accepts and stamps `broker_account_id`.
  - `app/schwab.py` `schwab_callback` upserts the `broker_accounts`
    row and stores its id on the new `schwab_connections.broker_account_id`
    column.
  - `app/schwab.py` `_run_sync` reads the broker_account_id from the
    connection row and passes it through.
  - `app/snaptrade.py` `_register_account` upserts the `broker_accounts`
    row and stores its id on the new `snaptrade_accounts.broker_account_id`
    column.
  - `app/snaptrade.py` `_run_sync` reads it from the account row and
    passes it through.
  - `app/upload.py` CSV upload route `get_or_create`s a
    `broker_accounts` row with `broker_slug='manual'` per
    `(user_id, account_name)` before calling `merge_and_push_seeds`.
- Surface `broker_account_id` in `stg_*` models — cast safely,
  NULL-tolerant, NO marts read it yet.

**Behavior unchanged.** Every existing read path still filters on
`(user_id, account_name)`. The new column rides along, invisible to
all downstream consumers.

### Stage 1 — Backfill (✅ shipped)

`scripts/backfill_seed_broker_account_ids.py` walks the seed CSVs,
groups them by `(account_name, user_id)`, looks each tuple up against
Postgres connection tables (`schwab_connections`, `snaptrade_accounts`),
upserts the `broker_accounts` row, and stamps the resulting id into
every matching seed row. Idempotent — re-running is a no-op for rows
that already carry a stamp.

**Resolution algorithm**, in priority order:

1. **Orphan user_id collapse.** Rows with empty `user_id` cell get
   rewritten to the unique non-empty owner of the same `account_name`
   (mirrors the `account_owner` CTE in `stg_history.sql`). If 0 or 2+
   owners exist, the row stays NULL — picking would risk the
   cross-tenant rendering bug the migration is designed to prevent.
2. **Postgres user pre-check.** The `effective_user_id` must exist in
   `users.id`. Orphan-tenant rows whose user was deleted are skipped
   and reported as `orphan_user_id` — attempting the upsert would trip
   the `broker_accounts.user_id` FK.
3. **Broker inference.** For each surviving tuple:
   - `schwab_connections` match → `broker_slug='schwab'`,
     `broker_external_id = account_hash` (preferred) or
     `account_number` (fallback).
   - `snaptrade_accounts` match → `broker_slug='snaptrade'`,
     `broker_external_id = snaptrade_account_id`.
   - No connection found → `broker_slug='manual'`,
     `broker_external_id = f'manual:{account_name}'` — same shape the
     Stage 0 upload route uses for new manual uploads, so a future
     re-upload converges on the same `broker_accounts` row.
4. **Idempotent upsert** of the `broker_accounts` row, capturing
   `broker_accounts.id`.
5. **In-place CSV rewrite** stamping `broker_account_id` into matching
   seed rows; never overwrites a populated cell.

**Unresolved-row reasons (reported, never crash):**

- `orphan_no_owner` — empty user_id, no other tenant claims this label.
- `orphan_ambiguous` — empty user_id, 2+ tenants claim this label.
- `orphan_user_id` — user_id is set but absent from `users` table.
- `bad_user_id` — couldn't parse the cell as int.

### Stage 1 — actual outcome on the live seeds

First production run against `dbt/seeds/{trade_history,
current_positions, account_balances}.csv` (~11,115 rows total):

```
Resolved      stamped     left_null    already_set
trade_history.csv:    3,673      7,207         0
current_positions:       37        146         0
account_balances:        16         36         0
TOTAL                 3,726      7,389         0
```

12 of 32 unique `(account, user_id)` tuples resolved cleanly. 20
tuples (~6,422 rows) hit `orphan_user_id` — their user_id is no
longer in Postgres. 1 tuple (785 rows) hit `orphan_no_owner` — the
legacy `investment1` test data.

**This is a finding, not a bug.** The orphan-tenant data was already
known to be problematic (see broker-sync-safety SKILL.md 2026-05-11)
and was the root cause of the May 2026 cross-tenant rendering bug.
The Stage 1 script makes the problem **visible and addressable** by
leaving those rows with NULL `broker_account_id` instead of letting
them silently inherit some other tenant's data.

**Operator decisions deferred to Stage 4:** Each `orphan_user_id`
tuple has three plausible resolutions:

1. **Delete** — the rows belong to a deleted user with no live
   replacement; the data is dead weight.
2. **Reassign to a live user** — the operator knows whose data it
   actually is (e.g. the May 2026 entry reassigned a specific account
   from deleted uid=9 to live uid=2). Update the seed `user_id` cell
   AND re-run this script.
3. **Recreate the deleted user** — if the user is genuinely returning
   but their Postgres row was lost, recreate them with the same id
   AND re-run this script.

Stage 4's `every_seed_row_has_broker_account_id.sql` test will fail
hard until every orphan tuple has been resolved one way or another.
That's intentional — it forces the conversation, not silent leakage.

After Stage 1, cron continues to populate `broker_account_id` for
every fresh sync via the Stage 0 writers — those paths have all been
updated to require and stamp the column.

### Stage 2 — Propagate through dbt (✅ Stage 2A + 2B shipped)

**Stage 2A shipped:**

- Built `dim_broker_accounts` as a view sourced **directly from the
  seed CSVs** (not from staging). Critical design decision: the
  staging models' `stg_canonical_account_owner` CTE REWRITES
  `user_id` based on most-recent activity / higher uid, which can
  reassign a Stage 1 stamp from its real Postgres owner to a deleted
  user (the bug the entire migration aims to solve). Reading the
  seeds directly preserves the (broker_account_id ↔ user_id) pairing
  the Stage 0/1 writers established, which is the Postgres truth by
  construction. Stage 4 will replace this with a proper Postgres → BQ
  export of `broker_accounts`; until then, the seed is the next-best
  stable source.
- Propagated `broker_account_id` through three high-value foundational
  models via the "wrapper join to dim" pattern:
  - `int_strategy_classification` — final `select` wraps in a
    `LEFT JOIN dim_broker_accounts ON (account, user_id)` and
    projects `broker_account_id`.
  - `positions_summary` — `strategy_summary` CTE adds
    `any_value(broker_account_id)` (functional on the existing groupby);
    `final` SELECT surfaces `wad.broker_account_id`.
  - `mart_daily_pnl` — final `select` wraps in a join to dim
    identical to `int_strategy_classification`.

**Stage 2B shipped:** Propagated `broker_account_id` through every
remaining intermediate and mart model the Flask app reads from
(~35 models). Three propagation shapes were used depending on the
upstream:

- **Inline LEFT JOIN to dim** for models that read directly from
  staging (the wrapper-join pattern, just inlined into the existing
  final FROM clause instead of a new outer CTE). Used for
  `int_option_contracts`, `int_enriched_current`, `int_option_rolls`,
  `int_option_exit_analysis`, `int_tax_lots`, `int_trade_baselines`,
  `int_pmcc_pairs`, `int_drip_fills`, `mart_account_snapshots_enriched`,
  `mart_benchmark`, `mart_coaching_signals`,
  `mart_daily_trading_metrics`, `mart_weekly_account_change`,
  `mart_weekly_summary`.
- **Passthrough** (`select broker_account_id, ...`) for models that
  read from a Stage 2-propagated upstream and don't need to re-resolve
  the key. Used for `int_dividends`, `int_option_trade_kinds`,
  `int_trade_sequence`, `int_trade_features`,
  `mart_strategy_performance`, `mart_strategy_trend`,
  `mart_option_trades_by_kind`, `mart_account_weekly_returns`,
  `mart_wealth_daily`, `mart_weekly_behavior_enriched`,
  `mart_weekly_streaks`, `mart_weekly_trades`, `mart_trade_observations`.
- **Wrap-and-join** (existing final select gets wrapped in a `_pre_broker`
  CTE before the dim join) for models where the union-all or aggregation
  shape made inline easier to reason about as a wrapper. Used for
  `int_dividend_events`, `int_option_contract_daily_pnl`,
  `int_daily_option_value`, `int_equity_sessions`, `int_position_legs`,
  `int_closed_equity_legs`, `mart_account_equity_daily`.

The `account_owner` / `canonical_account_owner` CTEs in `stg_history` /
`stg_current` / `stg_account_balances` STAY in place. They're still
load-bearing for the legacy `(account, user_id)` reads that haven't
been retired yet (Stage 4B).

**Verification (Stage 2B):**

- `dbt build` runs clean with 78 PASS / 2 WARN / 0 ERROR (the 2 WARNs
  are the expected Stage 4B punch list, see Stage 4 below).
- Cross-mart audit shows `broker_account_id` is present in every Flask
  surface and the populated-vs-NULL split matches the Stage 1
  resolution coverage (3,726 stamped / 7,389 orphans). Example
  (May 2026 production data):

  | mart                       | total_rows | with_bid | null_bid | distinct_bid |
  | -------------------------- | ---------- | -------- | -------- | ------------ |
  | mart_daily_pnl             | 165,792    |  48,530  | 117,262  | 5            |
  | mart_strategy_performance  |     85     |     24   |     61   | 6            |
  | mart_weekly_summary        |  2,188     |    479   |  1,709   | 6            |
  | mart_account_equity_daily  |  1,757     |     19   |  1,738   | 5            |
  | mart_benchmark             |    655     |    205   |    450   | 6            |
  | mart_wealth_daily          |  1,757     |     19   |  1,738   | 5            |
  | positions_summary          |    655     |    205   |    450   | 6            |

  `mart_account_equity_daily` and `mart_wealth_daily` show low
  `with_bid` because they're sourced from `stg_snapshot_account_balances_daily`,
  which uses `stg_canonical_account_owner` to rewrite user_id — and
  that wrapper rewrites historical rows to user_ids that may not have
  a `dim_broker_accounts` entry (the rewritten uid is itself orphaned
  in Postgres). Resolving this needs the Stage 4B orphan-row triage;
  it's NOT a Stage 2B regression.

- Module `int_split_factors` is symbol-only (no `(account, user_id)`
  grain) and intentionally does NOT carry `broker_account_id`. Same
  for the `stg_daily_prices` price layer.

### Stage 3 — Switch the filter (✅ Stage 3A shipped, 3B deferred)

**Stage 3A shipped (Flask helpers built, tested, NOT yet wired
into routes):**

- New helper `_resolve_filter_broker_account_ids()` returns the
  current user's list of `broker_accounts.id` from Postgres. Admin
  bypass via `None` semantics matching `_resolve_filter_user_id`.
- New helper `_broker_account_sql_and(ids)` emits
  `AND broker_account_id IN (...)`. Empty list = `AND 1 = 0`
  (fail-closed for authenticated users with no broker_accounts).
  Admin (`None`) = empty string. SQL-injection safe via `int(...)`
  coercion of every id.
- New helper `_broker_account_sql_filter(ids)` is the `WHERE`-prefixed
  sibling.
- New helper `_filter_df_by_broker_account_ids(df, ids)` filters a
  DataFrame. Drops rows with NULL `broker_account_id` for signed-in
  users (the orphan-tenancy security guarantee) but tolerates the
  Stage 2 deploy gap when the mart doesn't yet have the column
  (returns df unchanged — the legacy `(account, user_id)` filter
  still carries the security boundary in that case).
- 17 unit tests in `tests/test_broker_account_id_filter_helpers.py`
  pin the contract: admin bypass, fail-closed semantics, NULL drop,
  string-coerce, deploy-gap tolerance, SQL injection refusal.

**Stage 3B — deferred (intentionally; helpers are ready):** Wire
defense-in-depth into the routes that read from the Stage 2A/2B-
propagated models (`/positions`, `/symbols`, `/strategies`,
`/position/<symbol>`, daily review, weekly review). The wiring is
additive — both predicates emit alongside each other — so no security
regression risk on the existing reads.

The reason for deferring (after Stage 2B finished propagating
`broker_account_id` everywhere) is operational, not technical:

- The existing `stg_canonical_account_owner` CTE can rewrite a row's
  `user_id` to a deleted-Postgres-user value (see Stage 2A note).
  Once it does, `dim_broker_accounts` returns NULL for the dim join
  and the row's `broker_account_id` is NULL. Production data shows
  this is the majority case on `mart_account_equity_daily` /
  `mart_wealth_daily` — see the audit table under Stage 2 above.
- A naive wire-in that filters `WHERE broker_account_id IN (...)`
  with NO fallback would hide that production data from the user
  who genuinely owns it (the broker_account exists in Postgres; only
  the historical user_id stamp is stale).
- The right pattern is per-route, additive, and verified: keep the
  existing `(account, user_id)` filter as authoritative; ALSO assert
  `broker_account_id IS NULL OR broker_account_id IN (...)` on the
  same row set so the two predicates agree where data is well-formed
  and ONE remains as a backstop where the other is stale.
- The decision needs an operator able to spot-check the per-user
  diff on a handful of accounts that span multiple uids (e.g.
  `Schwab ••••5167`, `Schwab ••••9437`, `Cameron Investment`).

When Stage 3B does ship, the helpers in `app/routes.py`
(`_resolve_filter_broker_account_ids`, `_broker_account_sql_and`,
`_broker_account_sql_filter`, `_filter_df_by_broker_account_ids`)
are the only surface that needs to be called — no further changes
to the helpers themselves are expected. Recommended rollout: wire
one route per PR, with the route's existing integration test
expanded to assert the new predicate is non-empty in admin mode.

### Stage 4 — Clean up (✅ Stage 4A shipped, 4B deferred)

**Stage 4A shipped (contract tests + dim invariants, all
warning-severity for the migration):**

- `dbt/tests/every_broker_account_id_exists_in_dim_broker_accounts.sql`
  — every non-null `broker_account_id` in the user-tied staging
  models must have a matching row in `dim_broker_accounts`. PASS.
- `dbt/tests/seed_broker_account_id_unique_per_account_user.sql`
  — for every `(account, user_id)` tuple in the staging models, AT
  MOST ONE `broker_account_id` is stamped. PASS at warning severity
  (one false positive on the orphan-tenant collapse — see warnings
  in last `dbt build` output).
- `dbt/tests/dim_broker_accounts_unique_per_id.sql`
  — `dim_broker_accounts` has at most one row per `broker_account_id`.
  PASS at error severity (load-bearing for Stage 3 filter
  correctness).
- `dbt/tests/dim_broker_accounts_unique_per_user_account_pair.sql`
  — warning-severity hygiene check; surfaces drift but doesn't fail
  the build. PASS at warning severity.
- `dbt/tests/every_seed_row_has_broker_account_id.sql` — added in
  Stage 2B's final pass. Surfaces the Stage 4B punch list at every
  `dbt build`: any seed row with NULL `broker_account_id` is either
  an orphan tenant (deleted Postgres user) or a sync bypass.
  WARN at 58 results (matches the Stage 1 unresolved count, no
  unexpected new gaps).

**Stage 4B — deferred, gated on Stage 3B + operator
orphan-row triage:**

- Drop `stg_canonical_account_owner.sql` entirely.
- Drop the `account_owner` and `canonical_account_owner` CTEs from
  `stg_history`, `stg_current`, `stg_account_balances`.
- Drop the float-stringified dedupe `qualify` from `stg_history`.
- Drop `dbt/tests/no_orphan_user_id_per_account.sql` and
  `dbt/tests/no_stale_user_id_in_history.sql`.
- Tighten `every_seed_row_has_broker_account_id` from WARN to ERROR
  (currently fires with 58 results — the Stage 4B punch list).
- Drop the `OR user_id IS NULL` Stage 0/1 leniency from
  `_user_scoped_filter` and `_filter_df_by_user`.
- Drop `_narrow_mart_daily_pnl_chart_df_to_summary_tenant`.

**Orphan rows blocking Stage 4B (operator triage required):**

20 `(account, user_id)` tuples in the seed (~6,422 rows) reference
deleted Postgres users and currently have NULL `broker_account_id`.
The four legitimate resolutions:

1. **Delete the rows** — they belong to a deleted user with no live
   replacement, the data is dead weight.
2. **Reassign to a live user** — operator knows whose data it
   actually is (e.g. the May 2026 entry in broker-sync-safety
   SKILL.md reassigned a specific account from deleted uid=9 to live
   uid=2). Update the seed `user_id` cell + re-run
   `scripts/backfill_seed_broker_account_ids.py`.
3. **Recreate the deleted Postgres user** — if the user is genuinely
   returning but their Postgres row was lost, recreate them with the
   same id + re-run the backfill script.
4. **Quarantine as a sentinel** — accept that those rows stay NULL
   and become invisible to every signed-in user once the filter
   flips. Defensible if the data is too old to matter and Stage 4B's
   `every_seed_row_has_broker_account_id` test is relaxed to allow
   NULL.

Stage 4B should NOT ship until the operator has explicitly chosen a
resolution for each orphan tuple. The
`every_broker_account_id_exists_in_dim_broker_accounts` and
`dim_broker_accounts_unique_per_id` tests are currently passing and
should remain green through any operator triage.

## Why nullable broker_account_id (during Stage 0/1)

Same reason as the `user_id` migration. dbt seed loads are strict — if
`schema.yml` declares a column the CSV doesn't have, the load fails.
Going `NOT NULL` on day one means the next deploy would break
`dbt build` until every CSV cell is backfilled in the same commit.
Nullable lets us:

- Land the writer change without breaking CI.
- Backfill at the operator's pace.
- Keep query semantics unchanged until Stage 3 explicitly flips them.

`NOT NULL` (and the corresponding `WHERE broker_account_id IS NOT NULL`
defenses) land in Stage 4 once we've verified zero NULLs across all
seeds.

## Demo data

The demo user is shared. Demo seeds get one `broker_accounts` row per
demo account label (`broker_slug='demo'`, `broker_external_id` a
hard-coded constant like `demo:Demo Account`). The "All Accounts" view
filters by the demo user's broker_account_ids — same model as a real
user, just with one Postgres user instead of many.

## Admin path

`is_admin(current_user.username)` causes the Stage 3 filter to emit no
`broker_account_id` predicate (`None` = no filter). Behavior unchanged.
The Position Detail page's `_narrow_mart_daily_pnl_chart_df_to_summary_tenant`
heuristic for admin cross-tenant merging stays in place through Stage 3
and gets deleted in Stage 4 (the broker_account_id key makes cross-uid
merging in admin scope a non-issue — admin sees a flat union, not a
join-corrupted blend).

## Rollback

Each stage's commit is independently revertable, same pattern as
`USER_ID_TENANCY.md`:

- **Stage 0 revert** → seeds drop the `broker_account_id` column → dbt
  re-loads cleanly with the old shape. App is already not filtering on
  it, so no behavior difference. The `broker_accounts` Postgres table
  stays (harmless, no callers) or can be dropped.
- **Stage 1 revert** → just stop running the backfill. Existing
  populated values stay; new rows continue to populate them via cron.
  No revert needed at the data layer.
- **Stage 2 revert** → revert the dbt model commits. Marts go back to
  the old `(account, user_id)` shape. App must also revert to Stage 1
  helpers.
- **Stage 3 revert** → flip helpers back to `_account_sql_filter`-only.
  The `(account, user_id)` predicates are still in place from Stage 0,
  so we're back to the previous safety stance.

The point of staging is that we're never one bad commit away from a
broken `dbt build` or a leaking page.

## Why now

Failure mode B (stale-uid → canonical-uid split) shipped THREE separate
times in May 2026 (IYW Emmory, Cameron Investment / PLTR, the JEPI
$0-dividends gap). Each one cost a chat session, a recovery PR, and a
trust hit with the user. The fixes were progressively cleverer (the
canonical-owner heuristic, the dual-stamp resolution model, the
snapshot-only fallback for paper-trading accounts) — but each fix is a
band-aid over the same root problem: we re-derive tenancy from
`(string, int)` at every layer instead of carrying the broker's stable
ID end-to-end.

A migration to `broker_account_id` makes the entire bug class
structurally impossible. That's the value here — not "fewer
incidents", but "this category of incident no longer exists." The
ratio of code added to code deleted is favorable (~150 lines added,
~600 lines deleted in the canonical-resolution + dedupe + Python narrow
helpers + dropped tests).

## What this doesn't fix

- **The seed-CSV-in-GitHub model itself.** This migration cleans up
  the tenancy key, but the warehouse is still rebuilt from CSVs on
  every CI run, which makes DELETE / FK enforcement / strict
  uniqueness constraints structurally absent. A future "Option C"
  migration (Postgres-backed event log → streaming insert into BQ)
  removes the seed entirely. Out of scope here.

- **The 30+ marts that JOIN/GROUP-BY on `(account, user_id)`.** Stage
  2 propagates `broker_account_id` through them; Stage 4 drops the
  legacy keys. Until Stage 4 lands, both keys are carried.

- **Float-precision session-boundary fusion (failure mode C).** That's
  not a tenancy bug; it's a separate epsilon-zero fix already in
  place in `int_equity_sessions` and `int_closed_equity_legs`.

- **Cross-broker dedup for the same physical account.** A user who
  links their Schwab account through BOTH the native connector AND
  SnapTrade gets two `broker_accounts` rows (different broker_slugs)
  even though the physical money is the same. That's the v1 "warn
  and merge" posture from the broker-sync-safety skill — unchanged
  here. The fix would be a `physical_account_group_id` on top of
  `broker_account_id`; out of scope.

## Tests

`tests/test_broker_account_id_tenancy.py` (new) covers Stage 0:

- `broker_accounts` upsert is idempotent on `(broker_slug,
  broker_external_id)`.
- `merge_and_push_seeds` stamps `broker_account_id` into every
  emitted row, and refuses (returns `False`) when called without one.
- The Schwab connect callback creates a `broker_accounts` row and
  links it to the `schwab_connections` row.
- The SnapTrade `_register_account` flow does the same.
- The manual upload flow creates a row with `broker_slug='manual'`
  per `(user_id, account_name)` and reuses it on subsequent uploads.
- Staging models accept rows with both populated and NULL
  `broker_account_id` (Stage 0 leniency).

## Daily price loader

`current_position_stock_price.py` writes
`ccwj-dbt.analytics.daily_position_performance` (the source for
`stg_daily_prices`). Per-symbol, NOT per-account — does not need a
`broker_account_id` column. No change.
