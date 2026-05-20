# How `user_id` / account matching works in HappyTrader

A copy-pasteable, verbose explainer of the tenancy plumbing — what it is,
where it lives, why it keeps breaking, and the known failure modes.
Intended as context to share with another AI / reviewer who does not
have the codebase open.

---

## 0. The original problem (why this exists at all)

This app is multi-tenant. A single shared BigQuery dataset
(`ccwj-dbt.analytics`) holds rows for **every** user. The only "account"
identifier that comes off the broker is a free-form **string label** —
e.g. `"Schwab ••••0044"`, `"Brokerage"`, `"investment1"`,
`"Cameron Investment"`. The user types it (manual CSV upload) or it's
derived from the Schwab / SnapTrade API.

Account labels are **not unique across users**. Two different traders
can both call their account `"Brokerage"` — one of the original
incidents that motivated this whole layer was a production bug where
two users had the label `investment1` and `WHERE account = 'investment1'`
returned **both** users' rows to whoever queried it.

So the design rule is:

> **`account_name` is NOT a tenant key. `(user_id, account_name)` is the
> tenant key. Every BigQuery query MUST filter by `user_id`.**

This is documented in `docs/USER_ID_TENANCY.md` and enforced by an
always-on Cursor rule (`.cursor/rules/bigquery-tenant-isolation.mdc`).

---

## 1. The two halves of the data model

There are **two storage systems** that need to agree on who owns what.

### A. Postgres (application DB — Flask-side, source of truth for "who is logged in")

- `users(id SERIAL PRIMARY KEY, username, ...)` — Postgres SERIAL is the
  canonical user identity.
- `user_accounts(user_id INT REFERENCES users(id), account_name TEXT,
  PRIMARY KEY (user_id, account_name))` — many-to-many of "this user is
  allowed to see rows tagged with this label." Sharing a label across
  users is allowed; only `(user_id, account_name)` is unique.
- `schwab_connections(user_id, account_hash, account_number,
  account_name, token_json, ...)` — one row per linked native-Schwab
  account, OAuth tokens live here.
- `snaptrade_connections(user_id PRIMARY KEY, snaptrade_user_id,
  snaptrade_secret, ...)` — one row per SnapTrade user.
- `snaptrade_accounts(user_id, snaptrade_account_id, broker_slug,
  account_number_masked, account_name, ...)` — one row per linked
  SnapTrade account (Fidelity / Vanguard / Robinhood / IBKR).

Postgres is the **single source of truth** for "which `user_id` does the
current request belong to" — Flask-Login owns `current_user.id`.

### B. BigQuery seeds + warehouse (analytical store)

The warehouse is rebuilt from **CSV seeds in `dbt/seeds/`** on every CI
run:

- `trade_history.csv` — every fill (cols: `Account, user_id, Date,
  Action, Symbol, Description, Quantity, Price, fees_and_comm, Amount`)
- `current_positions.csv` — open positions snapshot (cols:
  `Account, user_id, Symbol, ...`)
- `account_balances.csv` — cash + account-total rows (cols:
  `account, user_id, row_type, market_value, ...`)
- `demo_history.csv`, `demo_current.csv` — same shape, demo user only.

Both CSV columns matter — `Account` is the label, `user_id` is the
Postgres SERIAL.

dbt then layers `stg_*` → `int_*` → `mart_*` models on top, and **every
downstream mart partitions by `(account, user_id)`** — `positions_summary`,
`int_strategy_classification`, `mart_daily_pnl`, `int_dividend_events`,
`int_equity_sessions`, `int_closed_equity_legs`, `int_option_contracts`,
`int_position_legs`, etc.

---

## 2. The write path — who stamps `user_id` when?

There are exactly three writers that produce seed rows:

1. **Manual CSV upload (`app/upload.py::merge_and_push_seeds`)** — user
   is logged in, so we know `current_user.id`. Function signature
   **requires** `user_id`:

   ```python
   def merge_and_push_seeds(
       account_name, history_df, current_df,
       *, commit_message, user_id, skip_history=False, balances_df=None,
   ):
       ...
       if user_id is None:
           # Stage 0+ never wants an unowned write — the cross-tenant guard
           # only works if every row carries the right user_id from day one.
           return False, "user_id is required.", 0, 0, None

       user_id_int = int(user_id)
       ...
       for df in [history_df, current_df]:
           ...
           df.insert(0, "Account", account_name)
           df.insert(1, "user_id", user_id_int)
   ```

2. **Native Schwab sync (`app/schwab.py::_run_sync`)** — runs from the
   web UI (logged-in user) OR from a CLI/cron (`app/schwab_sync_cli.py`)
   that iterates `schwab_connections` rows. In both cases the `user_id`
   comes from the `schwab_connections.user_id` column for that
   connection, and is passed into
   `merge_and_push_seeds(..., user_id=user_id)`.

3. **SnapTrade sync (`app/snaptrade.py`, `app/snaptrade_sync_cli.py`)** —
   same shape: iterates `snaptrade_accounts` and writes seed rows with
   `user_id=row.user_id`.

So **every new row goes into the warehouse already tagged with the right
`user_id`**. This is the "Stage 0/1" world from
`docs/USER_ID_TENANCY.md` — at this point everything works in theory.

In practice, three failure modes break it. See section 5.

---

## 3. The read path — staging-layer backfill + query-time filter

Two layers of defense sit between the seed and the rendered page.

### 3a. Staging-layer backfill in dbt

`stg_history.sql`, `stg_current.sql`, and `stg_account_balances.sql`
each:

1. **Cast `user_id` from the seed string.** Important quirk: the seed
   stores `user_id` as the pandas-emitted decimal string form (`"9.0"`,
   `"13.0"`) because Postgres BIGINT serializes that way via pandas.
   BigQuery's `SAFE_CAST(STRING -> INT64)` **rejects any decimal point**
   even when `.0`. Earlier code did a direct cast and silently NULL'd
   `user_id` for every Schwab-synced row. The fix is to cast through
   FLOAT64 first:

   ```sql
   safe_cast(safe_cast(nullif(trim(user_id), '') as float64) as int64) as user_id
   ```

   This appears identically in `stg_history.sql`, `stg_current.sql`,
   `stg_account_balances.sql`, and `stg_canonical_account_owner.sql`.

2. **Compute an `account_owner` CTE** that handles the
   "NULL → populated" case (failure mode A below): for each account
   where every non-NULL row agrees on one uid
   (`count(distinct user_id) = 1`), capture that as `inferred_user_id`.

3. **Read from a separate staging model — `stg_canonical_account_owner.sql`** —
   which handles the "stale-uid → canonical-uid" case (failure mode B):
   per `account`, pick the `user_id` with the most recent
   `max(trade_date)` from `stg_history`. Tie-break by higher `user_id`
   (newer Postgres SERIAL). Fall back to snapshot-only uids for
   paper-trading / freshly linked / never-traded accounts.

4. **Backfill with precedence:**

   ```sql
   coalesce(
       co.canonical_user_id,   -- 1) canonical (wins even over populated stamps)
       a.user_id,              -- 2) row's own stamp (Stage 0/1 happy path)
       ao.inferred_user_id     -- 3) account_owner CTE (NULL → populated fallback)
   ) as user_id
   ```

5. **Dedupe** by the natural composite key — when the canonical rewrite
   folds a stale-uid row onto a canonical-uid row that already exists,
   the two rows are byte-identical post-rewrite and one is dropped via:

   ```sql
   qualify row_number() over (
       partition by
           account, user_id, trade_date, trade_symbol, action,
           cast(quantity as string),
           cast(amount   as string),
           cast(fees     as string)
       order by description nulls last
   ) = 1
   ```

The key subtlety in `stg_canonical_account_owner.sql`: it reads directly
from the **`trade_history` seed** (not from `stg_history`) so it can
compute canonical owner BEFORE `stg_history`'s own backfill runs.
Otherwise you have a cycle.

### 3b. Query-time / DataFrame filter in Flask

Every BigQuery read goes through one of these helpers (defined in
`app/routes.py` ~lines 130–686, also re-imported from
`app/wealth.py`, `app/weekly_review.py`, `app/strategies.py`):

- `_user_account_list()` — returns the list of allowed account labels
  for `current_user.id` (admin gets `None`).
- `_account_sql_filter(accounts)` / `_account_sql_and(accounts)` —
  produce a `WHERE` (or `AND`) clause:

  ```python
  if user_id is not None:
      parts.append(f"({user_col} = {int(user_id)} OR {user_col} IS NULL)")
  if accounts is not None:
      parts.append(f"TRIM(CAST({col} AS STRING)) IN (...quoted list...)")
  ```

  The `OR user_id IS NULL` leg is Stage 0/1 leniency for legacy rows
  that pre-date the user_id column (drops in Stage 4 when all legacy
  seed rows are backfilled). For non-admin users the `user_id = X`
  predicate is the actual security boundary.

- `_filter_df_by_accounts(df, accounts)` — DataFrame analogue. Drops
  rows whose `user_id` is a *different populated* id; keeps NULL-`user_id`
  rows only when their `account` is in the user's allowed list; admin
  bypasses the user check.

Both layers (SQL + DataFrame) run on **every** user-facing query. The
cursor rule `bigquery-tenant-isolation.mdc` makes "did you call
`_filter_df_by_accounts` on every DataFrame in `_bq_parallel`" a
non-negotiable checklist item before any PR ships.

---

## 4. The downstream marts: what they partition by

This is where the whole thing rises or falls. **Every** consumer mart
includes `user_id` in:

- `JOIN ... USING (account, user_id)` (or equivalent ON clause).
- `GROUP BY account, user_id, ...`
- `PARTITION BY account, user_id, ...` (window functions, primary keys
  for incremental snapshots).

Models that do this (non-exhaustive): `int_equity_sessions`,
`int_closed_equity_legs`, `int_position_legs`, `int_option_contracts`,
`int_option_contract_daily_pnl`, `int_option_rolls`,
`int_strategy_classification`, `int_trade_sequence`,
`int_trade_baselines`, `int_daily_option_value`,
`int_option_pnl_series`, `mart_daily_pnl`, `mart_strategy_trend`,
`mart_account_equity_daily`, `mart_account_weekly_returns`,
`mart_account_snapshots_enriched`, `mart_weekly_*`, `mart_benchmark`,
`mart_coaching_signals`, `mart_wealth_daily`,
`mart_option_trades_by_kind`, the `attribute_dividends_to_strategy`
macro, and the snapshot tables `stg_snapshot_account_balances_daily` /
`stg_snapshot_options_market_values_daily`.

**This is where the fragility lives.** If even one model forgets to add
`user_id` to its grain, the same physical position gets split across
two partitions and downstream consumers either double-count, lose
realized P&L, or both.

---

## 5. The failure modes (all three are real, all three shipped to production)

The May 2026 rule file
`.cursor/rules/position-detail-orphan-tenancy-reconciliation.mdc`
enumerates them.

### Failure A — "NULL → populated split"

**Sequence:** Schwab Connect cron ran for a user **before** the Postgres
app linked `user_id` to the broker account mask (e.g. user signed up,
started a sync, but hadn't completed the "link this account label to my
user" step). The first batch of rows landed with `user_id = NULL`.
After linking, subsequent syncs wrote the **same masked `account`** with
`user_id` populated.

**Symptom:** A position whose buys live under
`(account="Schwab ••••0044", user_id=NULL)` and whose sells live under
`(account="Schwab ••••0044", user_id=9)` looks like two separate
positions to every downstream mart. `int_strategy_classification`
reports `$0` realized for the closed leg (because the buys are missing
from its partition), the chart shows the real number (`mart_daily_pnl`
rolls correctly when only the visible side is summed), and the
reconciliation banner fires.

**Real example (May 2026):** JEPI on `Schwab ••••0044` showed
`$0 realized + $0 dividends` despite buy + 2 sells totaling ~$4,300
of realized P&L. Recon banner caught a $2,560 chart-vs-mart gap.

**Fix:** the `account_owner` CTE in each of `stg_history`,
`stg_current`, `stg_account_balances`. If every non-NULL row for an
account agrees on one uid, that uid is used to rewrite the NULL rows.

### Failure B — "stale-uid → canonical-uid split"

**Sequence:** Same account stamped under **multiple non-NULL uids** in
trade history (or in current/balances). Happens when:

- A user record gets renumbered / merged / re-imported under a different
  stamp (e.g. test data imported under uid=2 by a one-off script while
  the actually-logged-in user is uid=9 today).
- A user re-links the same Schwab/SnapTrade account under a NEW uid
  after the old app-side `users` row was deleted.
- (Worst case observed) the broker surfaces themselves are dual-stamped —
  same broker account has rows in `current_positions` AND
  `account_balances` under BOTH uids.

**Symptom:** Every downstream consumer doubles. Position Detail shows
two "Leg 1" pills, the strategy breakdown shows the position twice,
Hero P&L is roughly 2× the real total. The
`count(distinct user_id) = 1` guard in the `account_owner` CTE refuses
to fire (correctly — it can't tell which uid is right), leaving both
rows in place.

**Real examples (May 2026):**

- IYW on Emmory Investment: trade history stamped `user_id=2`, current
  snapshot stamped `user_id=9`. Page rendered two phantom "Leg 1" pills,
  a "Dividend Closed -$1,957" strategy row, hero $396.67 vs chart
  terminal -$1,957.
- Cameron Investment / PLTR: same account stamped under uid=9 and
  uid=13 across **history AND current AND balances**. Every leg, every
  closed trade, every breakdown total rendered **2×**. Hero -$9,878.18
  on a position whose actual total is roughly half of that.

**Fix:** the **`stg_canonical_account_owner.sql`** model (added May
2026). Per `account`, the canonical uid is the one with `max(trade_date)`
in trade history; ties broken by higher uid (newest SERIAL). When trade
history has no rows for the account (paper-trading / fresh link), it
falls through to snapshot-only and tie-breaks by higher uid. The
canonical uid wins **even over a populated `user_id` stamp** on the row
itself, because we trust freshest activity more than a stale historical
stamp. Then a dedupe step collapses the byte-identical duplicates.

The `account_owner` CTE with `count(distinct user_id) = 1` is kept as a
separate fallback for the "trade-history-only, no canonical model
match" path.

### Failure C — "float-precision session boundary fusion"

Not a `user_id` failure per se, but it interacts with B and is in the
same rule file. `int_equity_sessions` detects new sessions via
`prev_running_qty = 0 AND running_qty > 0`. With FLOAT64 share counts a
perfectly-closed round-trip can leave `running_qty` at `-1e-17` rather
than exactly `0`. The strict equality fails, a closed round-trip and a
fresh new lot get **fused into one session_id**, and the resulting
"leg" reads as a single Closed chapter with closed-loss math even
though half of it is genuinely Open.

**Fix:** epsilon zero check `abs(prev_running_qty) < 1e-9 AND
running_qty > 1e-9` in `int_equity_sessions.sql` and
`int_closed_equity_legs.sql`. 1e-9 is 5 orders of magnitude tighter
than any real fractional fill (broker min ≈ 0.0001).

---

## 6. Reconciliation invariant — the runtime detector

`app/routes.py::position_detail` computes a runtime invariant card
(admin-only) that compares three numbers for the same `(account, symbol)`
scope:

1. **Hero total return** — sum of `positions_summary.total_pnl`.
2. **Breakdown by Type total** — sum of `closed_equity_df` +
   `closed_legs_df` + `current_df` + `int_dividend_events`.
3. **Chart terminal** — `cumulative_options_pnl[today] +
   open_options_unrealized_pnl[today] + running_equity_pnl[today] +
   cumulative_dividends[today]` from `_build_chart_from_daily_pnl`.

If any pair disagrees by more than $1, the invariant card fires. This is
the "smoke alarm" — any `(account, user_id)` split anywhere in the
upstream marts trips it because the three numbers come from different
aggregation paths and disagree by exactly the duplicate/missing
partition.

CI also runs `scripts/audit/reconcile.py` and two singular dbt tests:

- `dbt/tests/no_orphan_user_id_per_account.sql` — for each of
  `stg_history`, `stg_current`, `stg_account_balances`, fails if any
  `account` has BOTH NULL and populated `user_id` rows OR has more than
  one distinct populated `user_id`. This is the "did the backfill
  actually fire" test.
- `dbt/tests/no_stale_user_id_in_history.sql` — for each `account` where
  `stg_current ∪ stg_account_balances` agree on exactly one canonical
  uid, fails if `stg_history` has a row under a different uid for the
  same account. This is the "did the canonical rewrite actually fire on
  history" test.

---

## 7. Why this keeps causing issues (the honest part)

The architecture is sound but fragile because:

1. **The seed CSVs are a write-once-per-sync flat-file store with no
   schema enforcement** — there's no DB-level constraint that says
   "this row must have a non-NULL `user_id`". Every new sync code path
   (Schwab native, SnapTrade-per-broker, manual upload, future broker N)
   is a fresh chance to forget to stamp the column. The only guard is
   `merge_and_push_seeds` requiring `user_id`, but bypassing it
   (writing a CSV directly, a one-off script, a backfill tool) leaves
   NULLs that the staging-layer backfill has to clean up later.

2. **Account labels are user-chosen and broker-dependent** — they can
   be renamed by the user in `user_accounts`, re-derived by Schwab on
   connect (`Schwab ••••0044` → `Brokerage`), or SnapTrade-formatted
   (`_stable_account_name(broker_slug, masked)`). A rename produces
   orphan rows under the old label. The current rule is "never rename
   after first sync" but it's a convention, not enforced.

3. **The canonical-owner heuristic is `max(trade_date)`-based** — works
   for the common case where the freshest activity is the right tenant,
   but isn't strictly correct. If user X traded the account last week
   and the account got re-linked to user Y who hasn't traded yet, X
   wins on `max(trade_date)` even though Y is the current owner. The
   snapshot-only fallback (highest uid) tries to catch this case but is
   a heuristic too.

4. **The Postgres → BigQuery linkage is by string label, not by ID** —
   Postgres knows `user_id=9 owns account_name='Schwab ••••0044'`,
   BigQuery knows `account='Schwab ••••0044' has rows stamped user_id=9` —
   they're held in sync by the writers, but no foreign key, no
   referential check, no automated reconciliation between the two stores.

5. **The Stage 0/1 leniency leg `OR user_id IS NULL` is still there** —
   it means a malformed sync that writes NULL `user_id` will *still* be
   visible to the user via the account-label filter. It's safe (the
   user owns the label) but it hides bugs. Stage 4 (drop the leniency)
   hasn't shipped because nobody's verified the seeds have zero NULLs.

6. **The dedupe step relies on byte-identical rows post-rewrite** —
   quantity/amount/fees are stringified into the partition key to
   dodge BigQuery's "FLOAT64 in PARTITION BY rejected" rule. If two
   stale-uid rows differ by a single fee cent (rounding drift between
   sync runs), the dedupe misses and you double-count.

7. **The Position Detail page itself does additional Python-side
   `_filter_df_by_accounts` calls and a
   `_narrow_mart_daily_pnl_chart_df_to_summary_tenant` heuristic** —
   when admin scope merges two Postgres tenants under one `account`
   label, the chart helper picks the modal `user_id` from the summary
   frame to narrow the chart frame. This is a band-aid for cases where
   the staging backfill hasn't fully resolved.

8. **There are 30+ dbt models that JOIN/GROUP-BY on `(account, user_id)`** —
   adding a new model is one forgotten `user_id` away from
   re-introducing the same bug class.

---

## 8. Key files for an outside reviewer

- **Docs:** `docs/USER_ID_TENANCY.md`,
  `.cursor/rules/bigquery-tenant-isolation.mdc`,
  `.cursor/rules/position-detail-orphan-tenancy-reconciliation.mdc`.
- **Writers:** `app/upload.py::merge_and_push_seeds` (~line 846),
  `app/schwab.py::_run_sync` (~line 1285),
  `app/snaptrade.py` + `app/snaptrade_sync_cli.py`.
- **Postgres schema:** `app/models.py` lines 70 (`user_accounts`),
  115 (`schwab_connections`), 266 (`snaptrade_connections`),
  284 (`snaptrade_accounts`).
- **Staging backfill:** `dbt/models/staging/stg_history.sql`,
  `stg_current.sql`, `stg_account_balances.sql`,
  `stg_canonical_account_owner.sql`.
- **Query-time filter:** `app/routes.py` lines ~73 (`_user_account_list`),
  130–166 (`_account_sql_*` + `_filter_df_by_accounts`),
  546–686 (`_user_scoped_filter` + `_filter_df_by_user`).
- **Tests:** `dbt/tests/no_orphan_user_id_per_account.sql`,
  `dbt/tests/no_stale_user_id_in_history.sql`,
  `dbt/tests/stg_history_no_duplicate_fills_per_tenant.sql`,
  `tests/test_data_isolation.py`, `tests/test_orphan_tenant_iyw.py`.

---

## 9. The 90-second mental model

> Postgres knows who logs in (`users.id` = `user_id`). Each user has
> zero or more linked broker accounts (`user_accounts`,
> `schwab_connections`, `snaptrade_accounts`), each carrying a
> free-form `account_name` label. Sync code writes flat-file CSV seeds
> (`trade_history`, `current_positions`, `account_balances`) that each
> carry **both** `Account` and `user_id`. dbt loads those seeds, and at
> the staging layer rewrites `user_id` to a canonical value per
> `account` (most-recently-active uid wins; NULLs get backfilled), then
> dedupes. Every downstream mart partitions by `(account, user_id)`. At
> read time Flask filters every query by
> `WHERE user_id = current_user.id AND account IN (user's labels)` *and*
> re-filters the resulting DataFrame. The whole system has three known
> failure modes (NULL split, stale-uid split, float-precision session
> fusion), each with a regression test and a runtime invariant card on
> Position Detail. The fragility is that 30+ marts all need to remember
> `(account, user_id)` and any new writer is one forgotten `user_id`
> stamp from re-introducing the bug.

---

If you want the next AI to dig deeper, point them at
`stg_canonical_account_owner.sql` and
`.cursor/rules/position-detail-orphan-tenancy-reconciliation.mdc` first —
those two files contain the densest summary of the design and the
known failure modes.
