## Agents

This document describes how AI agents are used in this repository and how to work with them effectively.

- **Purpose**: Capture conventions, expectations, and tips for using Cursor (and other AI agents) on this project.
- **Audience**: Anyone editing code here, including future you.

---

## Project Context

- **Repo**: HappyTrader / `ccwj`
- **Primary stack**:
  - Python (Flask app) — routes, templates, auth
  - dbt for analytics / transformations (BigQuery)
  - GitHub Actions for scheduled builds (`bigquery_update.yml`)
  - `refresh.sh` for local dev builds (targeted `--select` vs full CI build)
  - `current_position_stock_price.py` fetches daily prices (including SPY/QQQ benchmarks)

**BigQuery is multi-tenant in practice:** a shared dataset can contain many `account` labels. Unscoped symbol-only (or unfiltered) queries have leaked other users’ rows to a signed-in user before. **Every BQ read for user-facing pages must be scoped in SQL and/or with `_filter_df_by_accounts` on every DataFrame before merge or render.** See `.cursor/rules/bigquery-tenant-isolation.mdc` (always on for agents) — follow it for every change under `app/` that touches queries.

**Tenant key is v2 `tenant_id` (`snaptrade:<broker_uuid>`)** — see `docs/V2_TENANT_KEY_DESIGN.md`. All broker connectivity is **SnapTrade-only** (including Schwab); direct native OAuth was removed in Phase 7. Policy: `.cursor/rules/snaptrade-only-broker-integrations.mdc`. Postgres holds `broker_tenants` (one row per physical account); warehouse seeds and marts join on `tenant_id` only. `account` and `user_id` columns in seeds are display/metadata — never the isolation boundary. Legacy docs `docs/archive/BROKER_ACCOUNT_ID_MIGRATION.md` and `docs/archive/USER_ID_TENANCY_EXPLAINER.md` are superseded.

**`tenant_id` is also the analytics GRAIN, not just the security scope (June 2026 re-grain).** The `account` display label is NOT unique — SnapTrade routinely returns a generic `"{Broker} Account"` for multiple physical accounts (one user had 5 Schwab accounts all labeled "Schwab Account", `user_id 9`). Every per-position / per-account `GROUP BY`, `PARTITION BY`, pandas dedup key, AND the `snapshot_account_balances_daily` snapshot `unique_key` keys on `coalesce(tenant_id, account)` so distinct accounts don't fuse (which silently corrupts cost basis / sessions / realized P&L with no failing test unless a uniqueness invariant trips). The old `left join dim_broker_tenants on (account_name, user_id)` passthroughs were dropped (they fan out N× under colliding labels) — `tenant_id` is carried natively from staging. The Flask UI disambiguates colliding labels via `_disambiguated_tenant_labels` (`Schwab Account (••<uuid tail>)`) and is tenant-addressable via `?tenant=<tenant_id>`. Regression: `dbt/tests/position_legs_tenant_split.sql`. When adding any new per-account mart/CTE or changing a snapshot `unique_key`, key on `tenant_id`; genuinely user-spanning CTEs (`shares_held_anywhere`/`shares_held_elsewhere`) stay user-grained on purpose.

**The SAME brokerage account legitimately belongs to MULTIPLE users — this is an intended product feature, NOT a duplication/orphan bug. Never "dedupe", "merge", or "reconcile" an account across different `user_id`s, and never purge one user's tenant because another user holds "the same" account.** The product is built for shared visibility: e.g. a parent links their own accounts **and** their daughter's and sees them all in one view, while the daughter is a separate user who links (or is granted) only her own account and sees just that. Two users, one underlying brokerage account — by design. Mechanically each user connects through their **own** SnapTrade registration, so the same physical account surfaces under a **distinct `tenant_id` per user** (different SnapTrade UUIDs), synced independently. The two copies' balances can legitimately **drift** (different sync timestamps / one connection healthy, the other disabled) — that is expected, not corruption. Isolation and view-scoping are always `user → the set of tenant_ids that user owns` (Postgres `broker_tenants`); the warehouse may hold many users' tenants for the same broker account. **Contrast with the orphan/stale-uid tenancy BUG** (`.cursor/rules/position-detail-orphan-tenancy-reconciliation.mdc`): that is the SAME user's single position split across `user_id = NULL` / stale-uid / canonical-uid partitions and must be backfilled. Cross-**USER** sharing of an account is intentional; same-**USER** split across uid partitions is the bug. The two are not the same thing — do not "fix" the former.

**Seed data lives in BigQuery, not git (Aug 2026 seed-store migration).** Tenant trade data (trade history / current positions / account balances) is stored in the raw dataset **`ccwj-dbt.analytics_raw`** (dbt source `raw_broker`), written directly by the app via **`app/seed_store.py`** — atomic per-table `WRITE_TRUNCATE` loads under the existing cluster-wide seed advisory lock, all-STRING columns plus a hidden `_row_seq` INT64 that preserves write order. The old flow (seed CSVs committed to GitHub, push-triggered CI rebuild) is retired; git history of `dbt/seeds/*.csv` is the archive. The battle-tested pandas merge/dedup layer in `app/upload.py` (`_merge_seed_with_existing`, `_dedup_history_rows`) is UNCHANGED — only the storage I/O under it moved (the read/write seam is still `_get_file_content` / `_commit_git_paths`, names kept for the 50+ merge tests). Reads FAIL CLOSED (`SeedStoreError` → abort sync; only a genuinely missing table reads as empty), and the byte-exact no-op skip still prevents pointless rebuilds. After a CHANGED write the app POSTs a `workflow_dispatch` for `bigquery_update.yml` (`_dispatch_warehouse_rebuild`; log-don't-crash — a scheduled nightly run backstops missed dispatches); the post-sync processing UI polls a `dispatch:<unix_ts>` build marker instead of a commit SHA. Recovery: BigQuery time travel (`FOR SYSTEM_TIME AS OF`) gives 7-day point-in-time restore on every raw table.

**Never set a table expiration in the `analytics` dataset (Aug 2026 incident).** The dataset carried a 60-day default table expiration from its creation. Rebuilt-every-run tables never notice (CREATE OR REPLACE resets the clock), but the SCD2 **snapshot tables are the only long-lived tables** — `snapshot_account_balances_daily` hit day 60 on 2026-08-07 and BigQuery silently deleted it (`InternalTableExpired` in the audit log; NOT a dbt or app bug). The next `dbt build` found no table and recreated it with only that day's rows, so every "vs yesterday / 1w / 1m" delta and the Daily Account Δ calendar rendered "—" while dbt/pytest stayed green. History was restored from the dev mirror (`analytics_dev`), which happened to hold a copy taken 15 minutes before expiry — **time travel could NOT recover it because a recreated table with the same name ends undelete for the prior generation.** Defenses now in place: the dataset default expiration is removed and every table's `expires` cleared; `scripts/snapshot_guard.py` copies both snapshot tables to `analytics_backups` (14-day self-expiring) before every warehouse build and **fails the build** if pre-today snapshot history ever shrinks. If that guard trips: restore from the newest `analytics_backups.snapshot_*_<yyyymmdd>` copy BEFORE the next build merges bad state forward. Snapshots are accumulated observations — they cannot be rebuilt from source.

**Local dev and production are environment-separated (June 2026; re-keyed Aug 2026).** Local dev reads/writes its own warehouse and raw dataset so dev-environment writes never mix with production (numeric user ids collide across the two Postgres databases; an admin purge by `user_id=10` once deleted the other environment's rows). The knobs: `.env` sets `BQ_DATASET=analytics_dev` (every app query's hardcoded `ccwj-dbt.analytics.` ref is rewritten at the `get_bigquery_client()` chokepoint — `_apply_dataset_override` in `app/bigquery_client.py`) and `BQ_RAW_DATASET=analytics_raw_dev` (the app's seed writers target the dev raw dataset; non-prod raw datasets never dispatch a CI rebuild). Production leaves both env vars unset. Operator loop: `docs/LOCAL_DEV.md` / `./scripts/dev.sh`.

The dev dataset is a **full mirror for testing**, kept fresh two ways. (1) **App/UI work — clone:** `scripts/dev_clone_prod.py` (`./scripts/dev.sh --sync`) COPYs prod `analytics` → `analytics_dev` (read-only on prod; refuses to write `analytics` / `analytics_raw` / `analytics_backups`). CI runs this after every `bigquery_update.yml` / `prices_refresh.yml` job (`.github/workflows/dev_mirror.yml`), so local Flask sees the same marts customers see without a laptop `dbt build`. (2) **dbt model work — rebuild:** `scripts/dev-refresh.sh` rebuilds `analytics_raw_dev` from prod `analytics_raw` MERGED with the local environment's own syncs (merge logic in `scripts/dev_refresh_raw.py`, local tenants from the local `broker_tenants` win), then dbt-builds `analytics_dev` from it — using the **working tree's dbt code** with `DBT_RAW_DATASET=analytics_raw_dev` pointing the `raw_broker` source at the dev copy and `dbt/profiles.dev.yml` (temp `--profiles-dir`; dataset `analytics_dev`) so a missing `~/.dbt` cannot silently hit prod. `refresh.sh` / plain `dbt build` from `dbt/` still target prod `analytics` (the repo's `dbt/profiles.yml` takes precedence). dbt snapshots use `target_schema=target.schema` (never hardcode `'analytics'`, or dev builds MERGE into the prod snapshot tables). Raw market-data sources (`sources.yml` `external`, prices/splits/earnings) intentionally stay shared on `analytics` — they're not tenant data and the loaders only run once. Never purge shared-warehouse rows by numeric `user_id` — only by `tenant_id`. Tenant linking without a prod DB password: `scripts/dev-link-prod-tenants.py --from-warehouse --prod-user-id <n>` (or `./scripts/dev.sh --link` with `DEV_PROD_USER_ID`).

**Per-broker staging layer (June 2026).** The three base staging models (`stg_history`, `stg_current`, `stg_account_balances`) no longer read the seeds directly — each is now a UNION of thin per-broker adapter models in `dbt/models/staging/brokers/` (`stg_broker_{schwab,alpaca,fidelity,interactive}_*`, plus an `stg_broker_other_*` catch-all) followed by the unchanged heavy parse. (`interactive` = IBKR; slug is the lowercased first token of the account label "Interactive Brokers …".) Each broker has its own model so broker-specific quirks (date formats, sign conventions, duplicate-fill patterns) stay isolated and independently queryable/testable instead of being special-cased in the shared parse. Broker identity is DISPLAY-derived (not a tenancy boundary): `tenant_id` is the literal `snaptrade:<uuid>` for EVERY broker, so the only broker signal in the warehouse is the account-label prefix — `dbt/macros/broker_slug_from_account.sql` maps it to a slug (`"Schwab Account"`→`schwab`, `"Alpaca Paper Account"`→`alpaca`). Tenant isolation stays on `tenant_id`; never scope a user-facing read by `broker_slug`. The catch-all is mutually-exclusive+exhaustive with the named brokers so no row is ever dropped; the `dbt/tests/broker_split_preserves_all_rows.sql` test enforces union-count parity. **To add a brokerage:** (1) add its slug to `known_brokers()`, (2) add `stg_broker_<slug>_{history,current,balances}` (one-line `broker_*_rows('<slug>')` calls), (3) add those three models to the UNION in the matching base staging model, (4) add them to the per-surface unions in `dbt/tests/broker_split_preserves_all_rows.sql`. `dim_broker_tenants.broker_slug` now shows the real brokerage (was the always-`snaptrade` aggregator slug, kept as `aggregator_slug`).

**The public demo is a MIRROR of a real tenant, not fabricated data (Aug 2026).** The demo user (`demo`/`demo123`, tenant `demo:demo-account`, label `Demo Account`) used to be fed by hand-written CSV seeds plus a synthetic account-value curve (`int_demo_equity_daily`). Both are **deleted**. The demo is now a relabeled copy of the EarningsFollower trading bot's Alpaca paper account, set by `var('demo_source_tenant_id')` in `dbt/dbt_project.yml`, built by `dbt/models/staging/demo/stg_demo_{history,current,balances}` and unioned into the three base staging models exactly where the seeds used to be. **Two invariants to preserve:** (1) the mirror reads the **per-broker adapters** (`stg_broker_alpaca_*`), NEVER `source('raw_broker', …)` — the adapter drops Alpaca's duplicate partial fills and repairs the missing 100x option multiplier, so mirroring raw would reproduce a phantom ~-$67k unrealized loss and ~+$14.7k cash break in the demo; (2) the mirror stamps `tenant_id='demo:demo-account'` itself, keeping the demo a genuinely separate tenant that renders through the **same tenant scoping as any real user** — do NOT "simplify" this by pointing the demo user at the bot's tenant, which is impossible anyway (`broker_tenants.tenant_id` is a PRIMARY KEY, one tenant = one user). History/marks are mirrored at three layers: staging (above), the balance snapshot (`bal_versions` in `mart_account_equity_daily`), and option marks (`versions` in `int_option_marks_daily`). The `where account != 'Demo Account'` filter in `mart_account_equity_daily.bal_versions` is **load-bearing** — the SCD2 snapshot still holds legacy fabricated demo versions that would otherwise collide with the mirror at the same `tenant_grain`. EarningsFollower deep-links back in at `/earningsfollower/<symbol>`.

**Reverse trial is the billing model (Aug 2026).** Every new
signup is `users.plan='trial'`: full product, no card. The 30-day clock
starts at FIRST DATA (`trial_started_at`, stamped once at the first
successful sync in `_sync_one_connection` / first CSV upload), not at
signup. Day 30 the mirror FREEZES — every page stays readable, but syncs
and uploads stop; day 60 the daily `happytrader-plan-lifecycle` cron
(`app/plan_lifecycle_cli.py`, also sends the day-23/30/53 lifecycle
emails, dedupe per trial episode via `email_sends`) removes the SnapTrade
authorizations and deletes the SnapTrade user so aggregator per-account
billing stops. `broker_tenants` rows are marked `disconnected` but KEPT —
**warehouse data is never purged**; a returning subscriber reconnects and
history is intact. Everything except `plan` + `trial_started_at` is
DERIVED (`app/plan.py`: `derive_plan_state`, day 30/60 boundaries) — no
cron flips states, and derivation FAILS OPEN to exempt on DB errors.
Gating: the MANDATORY chokepoint is `user_sync_allowed` at the top of
`_sync_one_connection` (returns `error='plan_frozen'` WITHOUT recording a
sync attempt — a frozen skip must never look like a broken connection);
webhook + nightly CLI have efficiency skips; manual routes
(connect/sync/refresh/upload) use `plan_block_writes` (the
`demo_block_writes` twin, redirects to /pricing). Existing users were
grandfathered to `plan='beta'` (never freezes) by the one-shot backfill in
`_migrate_users_plan_columns`; admins + demo are always exempt. Banners
live in base.html via `plan_status` (context processor in
`app/__init__.py`). Pinned by `tests/test_plan_lifecycle.py`.

**Stripe is the payment layer; `plan='active'` is the whole seam
(Aug 2026).** Pro is **$19.99/mo or $199.99/yr** via Stripe-hosted Checkout
+ Billing Portal — no card data touches this app. Everything lives in
**`app/billing.py`** (`/billing/checkout`, `/billing/success`,
`/billing/portal`, `/webhooks/stripe`); setup runbook in
`docs/STRIPE_SETUP.md`. `users.plan` stays the ONLY column the product
reads — the new `users.stripe_*` / `subscription_*` columns
(`_migrate_users_stripe_columns`) are a MIRROR of Stripe for support and UI,
never a second source of gating truth. Status mapping: `active`/`trialing`
→ `plan='active'`; **`past_due`/`incomplete` also stay active** (Stripe is
still retrying — yanking the mirror mid-dunning then restoring it is worse
than a few days of risk); terminal statuses revert to
`plan_before_subscription` or `trial`. Four rules that are load-bearing:
(1) **`app.db.execute` coerces params to a tuple, so every plan write uses
POSITIONAL `%s`** — a dict of named params raises "N placeholders but M
parameters" and a paying customer silently never activates (pinned by
`test_plan_writes_use_positional_params`); (2) **cancellation backdates
`trial_started_at` to the freeze boundary** so churn lands in `frozen`
(readable, broker connected through the 30-day grace, instant win-back), not
in a fresh trial and not straight to disconnected — a grandfathered beta
user returns to `beta` via `plan_before_subscription`; (3) **webhook
signature verification is mandatory and deliveries are idempotent** via the
`stripe_events` table, a handler failure returns 500 WITHOUT recording so
Stripe retries, and an event whose customer can't be matched to a user is
acked but logged at ERROR (an unlinked subscription needs a human); (4) **the
live Stripe account is SHARED with sibling products (EarningsFollower, Job
Glow) and Stripe gives every endpoint the ACCOUNT's whole event stream**, so
every plan write is gated on `subscription_is_ours(sub)` — a price-id match
against `STRIPE_PRICE_MONTHLY` / `STRIPE_PRICE_ANNUAL`. All three apps stamp
`client_reference_id` / `metadata.user_id` with THEIR OWN numeric user ids, so
an ungated event resolves to the same-numbered HappyTrader user: a stranger's
purchase grants free Pro and a stranger's cancellation freezes a paying
customer. Price is the only discriminator that can't collide and that works
retroactively on subscriptions predating the check (a metadata marker does
not). There is NO per-product webhook filter to configure in Stripe — this
MUST stay in code, on the webhook AND on `/billing/success` (whose session id
arrives in a URL). Pinned by the sibling-product tests in
`tests/test_billing.py`.
Subscribing also queues an immediate catch-up sync through the webhook's
existing debounce queue (`queue_resume_sync` → `_queue_snaptrade_sync`) so
"Resume my mirror" means now — called only AFTER the plan flips, since
`user_sync_allowed` would refuse otherwise. `stripe_enabled()` is
all-or-nothing (secret key + BOTH price IDs) so an unconfigured deploy
refuses checkout instead of charging; the Pro card still shows signup.
Admin `/admin/users` keeps the
manual set-plan / +30d levers for comps. Pinned by `tests/test_billing.py`.

**HappyTrader AI is a second Stripe subscription** (`STRIPE_PRICE_AI_MONTHLY`,
suggested $9.99/mo) that unlocks paid-tier models on `/insights` (Gemini Pro,
Sonnet, Opus — add more by appending a `tier="paid"` row to `MODEL_CATALOG`
in `app/llm.py`). It writes only `users.ai_*` columns — never `users.plan`.
A Pro cancellation must not clear the add-on; an AI cancellation must not
freeze the mirror. Same shared-account rule: `ai_subscription_is_ours`
matches the AI price id. Checkout is `/billing/checkout-ai`; Ask AI persists
turns in `insight_messages` and sends the last 12 to the model. Haiku/Flash
stay available without the add-on. **Admin and grandfathered `beta` are
not exceptions** — `user_can_use_paid_llm` reads only
`users.ai_subscription_status`. Pinned by the AI-addon tests in
`tests/test_billing.py` and catalog tests in `tests/test_llm.py`.

**Brokerage sync is the most failure-prone surface in the product** — SnapTrade aggregator (Schwab, Fidelity, Vanguard, Robinhood, IBKR, etc.). Before editing `app/snaptrade.py`, `app/snaptrade_normalize.py`, `app/upload.py` (especially `merge_and_push_seeds` / `_merge_seed_with_existing`), `app/seed_store.py`, `app/snaptrade_sync_cli.py`, the raw seed table shape (`analytics_raw`, source `raw_broker`), `.github/workflows/bigquery_update.yml`, the multi-account Sync flows on `/profile?tab=account` / `/snaptrade/accounts`, or any column on `broker_tenants` / `snaptrade_users` (`connection_broken_at`, `first_sync_completed`), **load the `broker-sync-safety` agent skill** (`~/.cursor/skills/broker-sync-safety/SKILL.md`) and walk its pre-flight checklist. The skill is an append-only register of bugs already shipped, the invariants that must hold, and the recovery runbook. **When you ship a sync fix, append a new "Bugs we've shipped" entry to that skill before closing the PR** — the structured format (symptom / root cause + file:line / fix commit / regression test / lesson) is documented at the bottom of SKILL.md.

---

## Product Identity

This product is not a trading dashboard. It is a **Trading Mirror**.

Its purpose is to help active options traders:
- Understand how they trade
- Identify recurring loss patterns
- Improve execution consistency
- Separate market conditions from personal behavior

It is **process-first, not P/L-first**.

The product does not try to:
- Predict markets
- Optimize trades
- Provide trade ideas
- Compete with broker dashboards

It reflects behavior back to the trader.

---

## Core Philosophy

- Outcome is context. Process is the signal.
- The market is the weather, not the judge.
- We compare traders to themselves, not to others.
- We surface patterns, not opinions.
- We avoid psychological labeling.
- We do not accuse. We present evidence.

---

## Page-by-Page Status

### Overview (`/overview` — endpoint still named `weekly_review` for url_for() compat) — PRIMARY EXPERIENCE
**Status: Close-based recap. Nav dropdown with Today. Never uses the word "today".**

Canonical URL is `/overview` (`/daily-review` and `/weekly-review` are aliases).
This is the page a paying customer opens for the **last completed session**. It should answer:
> "What happened at the last close (including what I traded that session), what should I watch, and how is every position / strategy / sector doing in total?"

Live / in-session last-trade numbers live on **Today** (`/today`, endpoint `today_view`).
Overview movers, trades, and snapshots all cap at `_snapshot_as_of_date` (during Friday's
open that is Thursday). After the bell — and all weekend — that date is Friday.
Copy always names the session date. A stale warehouse close must not rewind
Overview back to Thursday once Saturday has started.

The endpoint name is still `weekly_review` so the 30+ `url_for('weekly_review', ...)`
callers don't break.

What's working:
- Session hero: last-close account delta + market context + that session's fill count
- Session trades: every fill dated the **review session** from `stg_history`
 (`DAY_TRADES_QUERY`, shared with the time-machine day page). The dollar
 column is **realized G/L** (equity: `int_closed_equity_legs.realized_pnl`
 = sale vs average cost; option: `int_option_contracts.realized_pnl` on
 `realized_close_date`), not fill cash/proceeds. Opens and closes that
 have not yet matched a warehouse realized row render as an em dash.
 The session is always the last completed ET weekday (`_snapshot_as_of_date`)
 — never calendar-today while the regular session is still open. Adds and
 trims on a long-held position show here even though **Trades this week**
 only lists groups that opened or closed this ISO week. Same-day close +
 open of the same option type (same symbol + tenant, different strike/expiry)
 is grouped as one **Rolled** row (`_group_day_rolls`). Pairing keys on
 `tenant_id`. Lives in `build_daily_review_batch` as `today_trades` (same
 `trades_as_of` as `moves_as_of`) so the cache warmer replays it.
- Since you last looked: stock moves / newly ITM / newly near expiry / opens & closes
- Account snapshot row: close / vs prior session / vs 1w / vs 1m (per-account and total)
- Session movers: $ price-impact on currently-held shares for that close
  (`TODAY_MOVES_QUERY` / options / dividends capped at `@as_of` = snapshot cutoff)
- Watch list: upcoming earnings (≤14d), expiring options (≤14d, **not already expired**), ex-divs (≤30d). Overview drops past-expiry option rows (and mart-Closed contracts still lingering in the broker snapshot) before the positions strip / watch list aggregate — Schwab's snapshot lags expiry 1-2 days and a missing `trade_symbol` join used to keep those contracts on the page. Ex-div dates prefer `stg_ex_div_calendar` (yfinance `Ticker.calendar`, persisted by `scripts/refresh_earnings_calendar.py`); the last+median cadence heuristic is the fallback and is labeled "projected" in UI. Option expiry comparisons use the New York market date, not the viewer's profile date, so users east of the U.S. do not lose Friday contracts while Friday's session is still open.
- Daily account Δ heatmap (rolling 12 weeks, 4 visible by default)
- Current positions strip (open-position cards with live prices)
- Position / strategy / sector / subsector scorecards (performance by account)

### Today (`/today`, endpoint `today_view`) — LIVE SESSION
**Status: In-session last-trade / last-sync page with an always-on delay disclaimer.**

This is the only surface allowed to say "today". Banner: numbers can lag the
broker; they are not the official close (that's Overview). Nav sits in the
same Overview dropdown. Weekend and pre-market are **not** a live session:
do not replay Friday's close (or 24/7 crypto bars) as "today" — Overview
already has the last completed session. `/today` then shows an empty
"no live session" state.

- Calendar-today fills (`DAY_TRADES_QUERY` with `@day` = user today). Same-day
  trades often land after the next sync (activities are T+1).
- Movers (only while `_session_is_live`: open or after-hours) use the two
  newest `stg_daily_prices` rows with `date <=` calendar today — includes
  in-session last-trade bars. Header is holdings price impact, not full
  account value.
- After-hours movers: broker mark vs official close (moved here from Overview).
  Only rendered once `_us_market_session()` is `after_hours` AND the broker mark
  is post-close (`post_close_broker_tenant_ids` in `app/snaptrade.py`). Gate is
  STRICT. Query anchors close on `CURRENT_DATE('America/New_York')`. Dev mirror
  has NULL sync timestamps so the section stays hidden locally — expected.
- Open-contract live marks (`OPEN_OPTION_RECORD_QUERY`).

Batch: `build_today_batch`. Cache warmer replays it with calendar today.

What was removed in the rebuild:
- Friday / Monday / Mid-week mode pill toggle
- Mon-Fri "diary" timeline (kept as a helper for tests; not rendered)
- Behavioral baseline cards (volume / win-rate / pnl vs 8-week average)
- "Numbers" disclosure with weekly best/worst trades
- "What we noticed" / Patterns / Coach's Take cards
- Watch Next Week (Friday-only) section

Implementation notes:
- The new POSITION_ATTRIBUTION_QUERY joins `int_strategy_classification` (for equity vs
  option P&L split) + `int_dividends` (for div income) + `stg_history` (for buy-cash
  capital) + `int_enriched_current` (for current-leg snapshot). Tenancy-scoped at the
  SQL level via {account_filter} on every CTE, and the resulting DataFrame is filtered
  via `_filter_df_by_accounts` defensively (both layers, per the bigquery-tenant-isolation
  rule).
- Annualized return uses (net / capital) × (365 / max(days_held, 30)) with a $200
  capital floor so dust-lot dividends don't extrapolate to four-digit %.
- Strategy / sector / subsector breakdowns are pure pandas groupby on the per-symbol
  rows — totals reconcile by construction.
- Ex-dividend dates: canonical source is `stg_ex_div_calendar` (yfinance
  `Ticker.calendar` Ex-Dividend Date, written by
  `scripts/refresh_earnings_calendar.py` into `earnings_calendar` on the
  warehouse rebuild — ETFs like JEPI often have an ex-div and no
  earnings, so the loader persists a row even when Earnings Date is
  missing). Daily Review (`EX_DIV_CALENDAR_QUERY` +
  `_build_upcoming_dividends`) and the weekly preview email
  (`_EX_DIVS_SQL`) prefer a calendar date that is still on/after today.
  Fallback is the last+median cadence heuristic on
  `stg_daily_prices.dividend` (median spacing of last 6 events),
  rolled forward with `CEIL(days_since / spacing)` when last + one
  spacing is already in the past. Heuristic rows are labeled
  "projected" in UI / `~` in the email; calendar rows are not. The
  calendar query is a separate batch key so a missing view cannot
  blank the heuristic. Symbol-grain public market data — do NOT run
  the calendar frame through `_filter_df_by_tenant_ids`.

What could be better:
- ~~"Today's $ impact" only covers equity price-moves~~ — closed Aug 2026: the movers
 card now folds in per-symbol option P&L day-moves (`TODAY_OPTIONS_MOVES_QUERY`, the
 day delta of `cumulative_options_pnl + open_options_unrealized_pnl` from
 `mart_daily_pnl` — captures MTM drift AND same-day realizations) and dividends paid
 today (`TODAY_DIVIDENDS_QUERY` on `int_dividend_events`, anchored to the equity
 movers' as-of date). Header shows combined "Today's $ impact" with a
 stocks/options/dividends split. Both queries live inside `build_daily_review_batch`
 so the cache warmer replays them automatically.
- Position attribution capital is a proxy (sum of buy-cash). Doesn't account for cash
  released by closures or for variance margin on shorts.
- No "what I expected vs what happened" framing — could use a 1-line summary that
  surfaces above the snapshot table.

### Position Detail (`/position/<symbol>`) — DEEP DIVE PAGE
**Status: Functional with recent fixes. Complex page with the most logic.**

What's working:
- Position Legs with sequential numbering (Leg 1, Leg 2, etc.) — **canonical
  definition lives in `int_position_legs` mart**, not in Python anymore.
  Legs include open option contracts so the pill status agrees with the
  banner (was a long-standing bug — the legs section used to read only
  CLOSED contracts and showed "all closed" pills next to an Open banner).
  ```dbt/models/intermediate/int_position_legs.sql```
- Leg filtering — click a leg pill to scope the entire page (URL ?leg=<n>)
- **Breakdown by Type** card (Equity / Options / Dividends rows) sits
  above Strategy Breakdown. Source: `closed_equity_df` + `closed_legs_df`
  + `current_df` + `int_dividend_events`, all leg-aware. Equity row
  collapses multiple closure events for one session into "1 session" so
  partially-sold positions don't read as multiple chapters.
- Strategy Breakdown re-aggregates per leg under a leg filter. The leg
  path rebuilds rows from `int_strategy_classification` filtered by
  `open_date in_leg_range` instead of using `positions_summary` (which
  is full-symbol and was making the table look frozen on filter).
- `closed_equity_df` leg-filter uses `open_date` overlap, NOT
  `int_equity_sessions.session_id` — under the merged-interval mart the
  pill `leg_id` is sequential per merged chapter and may not equal the
  equity session_id. Old session_id-based filter spilled equity into
  the wrong leg's tables.
- Cumulative P&L chart with equity, options, dividends, total lines + stock price overlay
- Win/Loss Matrix (DTE vs Strike Distance) per strategy
- Expandable raw trades per leg (click arrow to see underlying transactions)
- KPIs recalculated per-leg when filtered
- Strategy breakdown table filtered by leg
- Orphan options grouped into non-overlapping "options only" legs (mart owns
  the gap-id assignment; old Python had ordering edge cases that produced
  duplicate negative session_ids in rare cases)
- Short position handling in equity P&L (call assignments selling more shares than held)
- Snapshot market value nulled out when leg-filtering (prevents cross-leg inflation)
- Cumulative columns re-zeroed per-leg so chart starts at 0
- Covered Call classification requires >= 100 shares (`int_strategy_classification`)
- `_date_to_leg` prioritizes equity sessions over orphan sessions

**Orphan tenancy + reconciliation (critical):** If Schwab synced **before** the user linked `user_id`, history can sit under **`user_id = NULL`** and later fills under **the same masked `account` + real `user_id`**. Marts partition `(account, user_id)` → buys and sells **split**, producing **\$0 dividends / \$0 KPIs while the chart is non‑zero** and tripping the **admin reconciliation invariant** (Strategy breakdown vs breakdown-by-type vs chart terminal). Fix is staging backfill in `stg_history` / `stg_current` / `stg_account_balances`; regression test **`dbt/tests/no_orphan_user_id_per_account.sql`**. Details: `.cursor/rules/position-detail-orphan-tenancy-reconciliation.mdc`.

**Opening balances + classification accuracy (Aug 2026 audit):** SnapTrade/Schwab
only backfill a limited transaction window, so a long-tenured trader's positions
routinely start MID-POSITION (buys predate the window) — 92 positions across 4
users at audit time. **`int_opening_balances`** infers the missing opening
share count per (tenant, account, user, symbol) with provable arithmetic
(`greatest(current − net_history, −min_running, 0)`, all today-unit
split-adjusted; symbols with `equity_sell_short` fills or no history rows are
skipped) and prices it on a confidence ladder: broker cost basis when the
position is still held (exact, keeps the open-session realized formula
consistent) → market close at the window start → first-fill price.
**`int_opening_cash`** is the account-grain sibling: day-1
`account_value` is the missing deposit (wealth exclude-transfers), not a
synthetic `stg_history` fill. **`int_equity_fills`**
is the canonical equity fill stream — real fills UNION synthetic openings with
split adjustment applied CENTRALLY (today's share-units) — and every
running-quantity consumer reads it: `int_equity_sessions`,
`int_closed_equity_legs`, `int_dividend_events`, `mart_daily_pnl` (via its
`opening_daily` UNION), `mart_benchmark`, and the coverage CTEs in
`int_strategy_classification`. The quantity is provable; only the COST is an
estimate — Position Detail renders a "history starts mid-position" disclosure
banner (`opening_balances` context, `POSITION_OPENING_BALANCES_QUERY`) naming
the pricing method and linking the CSV-upload path to replace estimates with
real fills. Strategy classification judges coverage **as of the write date**
(`coverage_at_write`: split-aware contracts × 100 vs shares held at open + 3-day
buy-write lookahead), labels partial coverage (`Partially Covered Call`),
detects diagonals (`Diagonal Call/Put Spread` — live longer-dated long cover
outside PMCC windows), straddles/strangles, pairs verticals by lifetime overlap
(not just ±7d legging), requires ≥30% covered-days for an equity session to be
labeled `Covered Call`, and folds <25-share tracker lots (was ≤1) into the
dominant option strategy. Regression tests:
`dbt/tests/no_unexplained_negative_running_equity_qty.sql`,
`dbt/tests/covered_call_has_coverage_at_write.sql`,
`dbt/tests/tracker_lot_folds_into_option_strategy.sql`. When adding a new
strategy label: update the FIVE template color maps (`position_detail.html`,
`positions.html` ×2, `accounts.html` ×2) and check the dividend-rank CASEs
(macro + `DATE_FILTERED_QUERY`) if the label should ever receive dividends.

**Stock splits (critical):** Schwab ships `stg_history.quantity` in the **share-units that existed at the fill time** — pre-split for old buys, post-split for new sells. The broker snapshot (`stg_current`) is always in **today's** share-units. Without explicit split-adjustment, FIFO cost basis on a buy → split → sell mismatches units and produces **massive phantom realized losses** (XLU May 2026: $-65,925 phantom on a position whose real realized was +$1,822.50). Splits land in `daily_split_events` (loader: `current_position_stock_price.py`) → `stg_split_events` → `int_split_factors`, then JOINed and applied to quantity in `int_equity_sessions`, `int_closed_equity_legs`, `int_dividend_events`, and `mart_daily_pnl`. Cash flow is split-invariant. Regression: `dbt/tests/equity_running_qty_matches_snapshot_after_splits.sql` + `tests/test_stock_splits.py`. Details: `.cursor/rules/stock-splits-share-unit.mdc`.

**Verification:** Never ship Position Detail / `mart_daily_pnl` / `_build_chart_from_daily_pnl` changes validated on **one symbol only**. Always check at least **one dividend ETF** (JEPI‑class), a **mixed equity+option** position, **multiple tenants/accounts**, and — if the change touches running share counts — at least **one symbol with a known split during the user's window** (XLU is the canonical regression case).

Known issues:
- Heavy Python computation: `_build_chart_from_daily_pnl` iterates every row to compute
  running average-cost equity P&L. This is stateful and hard to move to dbt, but is a
  performance concern for positions with years of daily data.
- `_build_option_matrices` uses nested loops over DTE/strike buckets in Flask.
- Pre-snapshot option P&L shows only cash flows (no mark-to-market). This means a dip
  when a LEAP is purchased that recovers once snapshots begin. Acceptable tradeoff.
- Position detail now lives in its own module `app/position_detail.py`
  (~2,600 lines incl. the tag routes) with the chart machinery in
  `app/pnl_charts.py` — see "Code Organization" below for the Aug 2026
  routes.py split.

### Home (`/`, `/index`)
**Status: Working. Public landing page; logged-in users redirect to Overview.**

There is no separate dashboard page — Overview is the authenticated home.

### Trader Profile (`/story`, endpoint `trader_story`)
**Status: Working. The mirror across every symbol.**

Runs the position-review engine over the user's whole history and folds the
per-symbol fingerprints into one profile. The page opens with a recurring
**This week / Last week** loop (`app/story_loop.py`) so the profile is
worth opening again: this week lists open options inside 14d (spreads
grouped) with the live mark (`+$450 · 3d`). Each watch runs the same
insight picker (`_collect_watch_insights` / `_pick_watch_insight`):
leftover-vs-expiry at this structure × this DTE, leftover on this
symbol, hold-later / close-earlier vs other shorts, roll/expire rate
at this horizon, rare-to-hold-this-far, live mark vs leftover, credit
size, then bookkeeping. Highest score wins; hold-later + leftover
compose when both independently clear. Numbers are never invented;
"naked call" only when `positions_summary` has an open Naked Call and
no covered-call label. Last week
reports fills/rolls/premium vs the median completed week and whether that
looked like them. Lifetime Profile Summary, Execution Review, notable
positions, style scoreboard, and year-by-year rows follow. Details in
"App-shell UX layer" under Code Organization.

### Positions List (`/positions`)
**Status: Working with recent filter-discipline pass.** Entry point to position detail.

Lists all positions with strategy tags, P&L, status. Links to position detail.
Pagination in Python (`per_page = 25`).

What's working:
- Hero "X open / Y closed" chips **and** the "Across N accounts" line honor
  every active filter (account,
  strategy, symbol, status, subsector, sector, date range). Pre-fix the
  chips read off the unfiltered df and lied about the body.
- Pagination + symbol-cell links preserve all 7 filter dimensions.
- "No accounts linked yet" copy fires only when the user genuinely has no
  linked accounts. Connected-but-empty users get a "data is pending"
  message instead.
- Quick Stats Winners shows raw `num_winners`, not the buggy
  `total_trades * win_rate` derivation that over-reported by 2-3x.
- P&amp;L by Strategy is a stacked realized vs unrealized bar (dividends
  count as realized so the stack still equals headline total P&amp;L).
- Date-filtered view (DATE_FILTERED_QUERY) uses the same realized /
  unrealized split and same status logic as the positions_summary mart;
  pre-fix the date view emitted a 3rd "Mixed" status the all-time view
  never showed, and derived realized_pnl from total_pnl by status which
  collapsed open-equity-with-interim-sells P&L into unrealized.

Architecture:
- Dividend attribution rules live in
  `dbt/macros/attribute_dividends_to_strategy.sql` (single source of
  truth). `dbt/models/marts/positions_summary.sql` calls the macro.
  The runtime DATE_FILTERED_QUERY in `app/positions_page.py` mirrors the macro
  output in inlined SQL (it has to — start/end dates come from the URL
  at request time, after dbt has finished building). `ATTRIBUTION_INVARIANT`
  comments in both files cross-reference; integration test
  `test_date_filtered_at_full_window_matches_mart` (set `RUN_BQ_TESTS=1`)
  pins them together.
- Buy and Hold → Dividend reclassification is a yield test, not "any
  coupon." A position is labeled Dividend only when dividends are
  ≥ 2.5% of invested capital AND ≥ 15% of the P&L story ($200 capital
  floor), or when dividends are ≥ 40% of (|price P&L| + dividends).
  The old `divs > greatest(price_pnl, 0)` rule labeled every underwater
  stock that paid anything (UFO −$6,571 / $17 dividend). Invested
  capital is `int_equity_fills` buy cash GREATEST the live snapshot
  cost basis. Pinned by `dbt/tests/dividend_strategy_is_real_yield.sql`.

Known issues:
- DATE_FILTERED_QUERY is still ~150 lines of inlined SQL in app/positions_page.py.
  Can't be a pure dbt mart because of the runtime parameterization, but
  the dividend-attribution complexity now lives in dbt.

### Symbols / Daily P&L (`/symbols`) — RETIRED (Aug 2026 surface audit)
**Status: URL 301s to `/positions`.** Position Detail answers everything it
did, one symbol at a time, with the tab strip for flipping between symbols.
`app/symbols_page.py` survives only as the home of the shared
`TRADES_QUERY` / `CURRENT_POSITIONS_QUERY` (consumed by `/accounts`) and
the `/api/nav/symbols` Cmd+K endpoint.

### Strategies (`/strategies`) — two views
**Status: Improved — drill-down now includes Breakdown by Type + tenant hardening.**

One "Strategies" surface with a view switch (Aug 2026 surface audit):
**Performance** (default, `app/strategies.py`) and **Fit matrix**
(`?view=fit`, the former `/strategy-fit` page — win-rate/expectancy by
strategy × sector/DTE/moneyness, `app/strategy_fit.py` +
`render_strategy_fit_view`). `/strategy-fit` 301s to `/strategies?view=fit`.

Cards still roll up lifetime performance from `mart_strategy_performance`; monthly context comes from `mart_strategy_trend`. When you click a strategy, you now get a **Breakdown by Type** table (equity sessions vs option contracts vs attributed dividends): equity and options are summed from `int_strategy_classification`; dividends roll up from attributed `total_dividend_income` on `positions_summary`. That mirrors the Position Detail mental model for a single strategy label.

Tenant isolation: row-level query results go through `_filter_df_by_accounts(...)` before any pandas work, same as `/positions`. Pure `SUM(...) ...` aggregates without an account column rely on SQL `_account_sql_and` only. Failed `mart_strategy_trend` reads are logged instead of silently swallowed.

Symbol links in the drill-down table preserve the selected account filter (`?account=`).

**Still could be stronger:** richer narrative on the cards, less request-time SQL (pre-aggregate symbol tables in dbt), DTE breakdown moved fully into the warehouse.
### Accounts (`/accounts`) — two views
**Status: Working. One surface for per-account performance AND value/composition.**

**Performance** (default, `app/accounts_page.py`): per-account KPI cards,
P&L-earned charts, windowed KPI cards, Net deposits KPI.
Breakdown tables list positions active in the selected range with
**lifetime** Stock/Option/Dividend/capital (do not feed Daily Review's
``week_start`` into attribution — that mixed full-to-date open P&L with
lifetime dividends under a 1M/3M label).
The **P&L by Strategy Over Time** chart is the same daily mark-to-market
walk as Cumulative P&L (`_build_account_chart_from_daily_pnl`), bucketed
by primary strategy per `(tenant_id, symbol)`. Do not attribute open
groups to today — that dumped lifetime unrealized onto the last date and
made a buy-and-hold account look like a cliff.
**Value & composition** (`?view=value`, `app/wealth.py` +
`render_wealth_view` — the former `/wealth` page, merged Aug 2026 surface
audit; `/wealth` 301s): reads `mart_wealth_daily` (account_value / cash /
equity / options per day, plus cumulative dividends / interest / fees).
Stacked-area chart of composition with a total line; hero shows allocation
+ change-over-time.

**History gap is disclosed, not hidden (Aug 2026).** Broker sync is not a
lifetime or 5-year archive: we *request* up to 1825 days; SnapTrade clamps
to whatever the broker still has on file (often ~1–2 years of trades).
Daily values start even later — the first SCD2 snapshot after connect.
Accounts Performance (All range) and Value (All range) share a quiet,
dismissible note with Positions (All time), Position Detail, Strategies,
Strategy fit, Sectors, Trader Profile, and Insights
(``_history_window_note.html``, localStorage key `ht-history-note-dismissed`)
that the record is complete from account creation (the date the account
was connected), with a CSV link for earlier years. Sync copy on
profile / get-started / snaptrade_accounts must not say "~5 years". CSV
fills in *trades*; it cannot recreate daily balances before snapshots started.

**Deposits & withdrawals toggle (Aug 2026).** Deposits/withdrawals move
account value without being trading gains, which distorts "how am I doing"
on balance-based surfaces. External cash movements are now CAPTURED (they
were previously dropped): SnapTrade `DEPOSIT` / `WITHDRAWAL` / `CONTRIBUTION`
/ `INTERNAL_CASH_TRANSFER_IN` / `INTERNAL_CASH_TRANSFER_OUT` activities map
to `action = 'cash_transfer'` / `instrument_type = 'Cash Event'` in
`stg_history` (deposit +, withdrawal −; `TRANSFER` /
`DISTRIBUTION` stay dropped — they can be SHARE transfers or ambiguous
income). Cash-only **`JOURNAL`** (no ticker, non-zero amount) is a
deposit — Schwab CSV `Journal` and SnapTrade `JOURNAL` both map that
way (Emmory `$500` `JOURNAL FRM …852`). Share journals keep a symbol/qty
and stay dropped. Schwab CSV uploads also use **`Funds Received`** and **`MoneyLink
Transfer`** for external cash — those map to `cash_transfer` in
`stg_history` (and the upload merge key) so a CSV backfill actually
itemizes deposits. Cash events often have a NULL ticker; `stg_history` must keep
those rows (do not filter with `underlying_symbol != 'CURRENCY_USD'` —
NULL comparisons drop them and the toggle becomes a warehouse-wide no-op). `mart_wealth_daily` exposes `net_deposit_today` +
`cumulative_net_deposits`. The value view's **"Exclude deposits &
withdrawals"** toggle (`?exclude_transfers=1`) subtracts lifetime
``cumulative_net_deposits`` from the account-value line (as-if-you-hadn't-
moved-money) and strips in-window cash flow from the change-over-time
numbers. Two modes, never stacked on one tenant: **itemized** (at least
one `cash_transfer` on or before the first snapshot — e.g. Emmory CSV)
uses Σ those rows, so the exclude line on day 1 is account value minus
real deposits (pre-snapshot trading P&L stays); **fallback** (no such
rows) treats day-1 account value as inferred opening cash
(``int_opening_cash``) so the exclude line starts at $0, then stacks
explicit transfers after that date. `/accounts` adds a **Net deposits** KPI card (its P&L chart was already
deposit-free by construction) that re-windows client-side like Realized.

**Opening cash + later transfers.** Broker activity feeds are a short T+1
window and cash movements were dropped before capture shipped, so most
accounts start mid-life. The first snapshot's `account_value` *is* the
missing deposit **unless** a CSV (or a long broker window) already
itemized the cash-ins. Both mart columns are 0 only for a $0 first snapshot
with no cash_transfer rows.

**Dedup caveat (accepted).** Transfer rows go through the same
`_dedup_history_rows` contract as every other history row: two
value-identical rows (same Date / Action / Description / Amount) collapse
to one. Schwab genuinely re-ships duplicates, so this is deliberate — but
it means two REAL same-day deposits of the same amount with identical
broker descriptions would fuse. Same trade-off trades already accept; do
NOT "fix" it by embedding SnapTrade activity ids in Description (an id
that varies between reads would accumulate duplicates on every T+1
re-read, which is worse).

**Containment.** `cash_transfer` is INERT in every trade/session/option/
dividend model (all filter to Equity/Call/Put/dividend). The one catch-all,
`mart_daily_pnl.other_amount` (feeds the P&L charts), has an explicit
`action <> 'cash_transfer'` guard. Regression:
`dbt/tests/cash_transfer_is_inert_in_trading_pnl.sql` (a cash_transfer row
must be a `Cash Event`); unit coverage in `tests/test_snaptrade_normalize.py`
and `tests/test_wealth_chart.py`.

### Sectors (`/sectors`)
**Status: Working. Sector / industry rollups (`app/sectors_page.py`).**

### Earnings Watch (`/earnings`)
**Status: Working. Upcoming earnings on held symbols (`app/earnings_page.py`);**
gated on `EARNINGS_FOLLOWER_ENABLED`, cross-links to the EarningsFollower
tandem product.

### AI Insights (`/insights`)
**Status: Working. Multi-model narrative (Gemini + Claude).**

Reads `positions_summary` mart plus coaching signals, builds a deterministic
brief, then a model narrates it. Follows ARCHITECTURE.md: AI interprets,
doesn't compute.

Users pick a model from the dropdown: **Included** (Flash, Haiku) vs
**HappyTrader AI** (Gemini Pro, Sonnet, Opus — `tier="paid"` in
`MODEL_CATALOG`). Paid rows appear when the vendor key is set; the add-on
is the spend gate. Add a model by appending a catalog row.

### Admin overview (`/admin`)
**Status: Working.** Operator pulse for “how is the site doing”: plan mix,
broken connections, open feedback, 7-day page use (`usage_events` — logged
from authenticated HTML navigations only, no query strings), broker mix,
weekly signups, newest people. Drill-downs stay at `/admin/users`,
`/admin/audit`, `/admin/feedback`. Postgres-only so it still loads when
the warehouse is unhappy. Page views (`usage_events`) rank what people
open in the last 7 days (ranked unique people then views; bars are views;
demo excluded;
logged-out Home/Pricing/FAQ count). Non-admins get 404.

### Get Started (`/get-started`) — one onboarding surface
**Status: Working.** Checklist while the user is connecting/waiting for
data; once warehouse rows exist it flips to the former `/first-look`
"here's what we found" trading profile (`render_first_look_view` in
`app/first_look.py`; `/first-look` 301s here). The post-upload and
post-sync processing pages land here on first data.

After SnapTrade connect, the first 10 users of a brokerage we have not
modeled yet (anything outside schwab / alpaca / fidelity / interactive —
the `stg_broker_other_*` catch-all) see a note: if they choose to
subscribe, six months of Pro is included free, plus a thank-you for
patience while we calibrate. Applied
at checkout (`EARLY_BROKER_TRIAL_DAYS`, default 180). Optional `EARLY_BROKER_PROMO_CODE`
if a matching Stripe promotion code exists. The trial applies only to the
user's first Pro subscription and is Pro-checkout-only, so it cannot be
regranted after cancellation or apply to sibling products on the shared
Stripe account.
The cohort is stamped on `snaptrade_accounts.early_broker_cohort` at
first insert (`app/early_broker.py`) so the note still shows after the
11th user of that broker arrives. Surfaces: `/snaptrade/accounts`,
`/sync/processing`, `/get-started` / first look. Create the matching
Stripe promotion code before setting the env var (Checkout already
allows promo codes).

### Upload (`/upload`)
**Status: Working. CSV upload + SnapTrade sync entry points.**
CSV upload parses Schwab's web export. History and current-positions
files are both optional (upload either or both). The account picker is
**tenant-addressed** (same nicknames as Positions, `option value` is
`tenant_id`) so picking an existing account merges into that tenant —
it must not mint a new `manual:manual:<label>` from a nickname. “Create
new account” is the only path that creates an owner-scoped manual tenant
(`manual:manual:<user_id>:<label>`), and even then a typed name that
uniquely matches an owned nickname/account_name attaches instead of
duplicating. An ambiguous shared broker label
(several SnapTrade tenants all named `Schwab Account`) is rejected —
it must not mint `manual:manual:Schwab Account` beside them. Schwab CSV
equity Price is cents (`$58.97`) while SnapTrade keeps the fill Price
(`58.965`); `_dedup_history_rows` collapses those on Amount@2dp so the
same buy is not counted twice. The page lists a collapsed section
per brokerage; Schwab's section (open by default) holds export steps plus
the history / positions file drop, account picker, and submit — copy that
branch when another parser ships. Every other broker is a request
that posts to `/feedback` (prepopulated topic, optional sample-CSV offer,
free-text notes).

### Removed pages (do not resurrect without a product decision)
- **Journal** (2025) — conflicted with "works fully without user input."
  All code and models are gone.
- **Mirror Score / Benchmark / Trade Kinds / Taxes** (2026) — cut in the
  product focus passes; their marts (`mart_benchmark` etc.) still build and
  feed other surfaces.
- **Community** (Aug 2026 surface audit) — feed/follows/public profiles
  shipped flag-off, saw no usage, and argued with the "compare traders to
  themselves" identity. Routes, templates, nav, flag, and model helpers are
  deleted; the Postgres tables (`user_follows`, `community_posts`,
  `community_published_trades`) remain until a deliberate drop migration.
- **Daily P&L `/symbols`, standalone `/wealth`, `/strategy-fit`,
  `/first-look`** (Aug 2026 surface audit) — merged/retired as above; all
  four legacy URLs 301 to their new homes.

---

## Architectural Principles

### 1. Trade-Level Canonical Grain

The canonical grain of the system is **one closed trade**.

All aggregation rolls up from trade-level features.
Not from position-day. Not from strategy-day. Not from account-day.

### 2. dbt Owns Computation

Heavy logic belongs in dbt.

**dbt should compute:**
- Trade-level derived features
- Weekly aggregates
- Pattern detection inputs
- Mirror Score components
- Benchmark-relative calculations
- Strategy classification
- Equity session detection
- Option contract lifecycle

**Flask should:**
- Authenticate users
- Select account scope
- Query precomputed tables
- Render views
- Never perform heavy aggregation

**Current violations (known debt):**
- `_build_chart_from_daily_pnl` in `app/pnl_charts.py`: stateful equity P&L simulation via
  row iteration. Hard to move to dbt because of running average-cost logic, but heavy.
- `_build_option_matrices` (also `app/pnl_charts.py`): nested groupby + loops in Flask.
- `DATE_FILTERED_QUERY` in `app/positions_page.py`: runtime-parameterized analytical SQL (not a
  static mart) — documented rationale exists but still violates the principle.

If logic is found in Flask that belongs in dbt: flag it, move it, document it.

**Before pushing dbt changes** (avoids learning errors only in prod): with `~/.dbt/profiles.yml` and network, run
`cd dbt && ../.venv/bin/dbt parse && ../.venv/bin/dbt build`, or `scripts/dbt-validate.sh` (same). `parse` is fast and offline; `build` must succeed against BigQuery. If a snapshot fails on the first `build` but succeeds on `dbt snapshot --select <name>`, re-run the full `build` once (rare BQ/dag race).

### 3. Multi-Account Is Required

Users trade multiple accounts. All logic must:
- Scope by `tenant_id` (the isolation key; display `account` labels collide)
- Support "All Accounts" view
- Avoid assuming single-account structure

Users can **group** accounts (kids / sara / 401ks) on Settings → Accounts & data.
Membership is many-to-many on `tenant_id`; `?groups=` is the union of selected
groups' members, then intersected with `?account=` / `?tenant=` / `?tenants=`.
The Groups and Account controls are multi-select dropdowns to the left of
other filters, each with Apply and Reset. Account values are `tenant_id`
(`?tenants=`). Groups always lists every group (picking an account must not
hide the others). Selecting groups limits the account list to members.
With two or more accounts and no groups yet, the Groups slot is a quiet
"Group accounts" link to Settings → Accounts & data (`#account-groups`) —
not an empty dropdown. Never key groups on the SnapTrade `"Schwab Account"`
label.

### 4. Performance Rules

Page speed matters.
- No heavy queries in request handlers
- No per-request aggregations over raw trade tables
- Always read from precomputed marts
- Optimize for weekly read performance
- Market data comes from `stg_daily_prices` in BigQuery (not live yfinance calls)

**Query-cache lifecycle (Aug 2026).** Every user-facing BQ read goes through
`cached_query_df` (`app/query_cache.py`): per-worker L1 TTLCache (10 min) +
shared Redis L2 (`ccwj-query-cache` on Render, `QUERY_CACHE_REDIS_URL`, TTL
24h). The long L2 TTL is safe ONLY because the cache is explicitly flushed
when the data actually changes: `bigquery_update.yml` and
`prices_refresh.yml` end with a `curl POST /internal/cache/flush`
(`X-Cache-Flush-Token` = `CACHE_FLUSH_TOKEN` secret, set both as a GitHub
secret and a Render env var). The endpoint (`app/cache_ops.py`) clears the
cache and warms the hottest per-user query sets in a background thread —
the Overview core batch (`build_daily_review_batch` in
`app/weekly_review.py`, shared with the view so warmed keys are EXACTLY the
keys a request looks up) plus the positions-list default query, per user
with linked tenants plus one unscoped (admin) pass. If you change any of
those queries' SQL construction, keep the view and the warmer reading the
same builder or warming silently stops matching. New page queries should
use `cached_query_df` (or `_bq_parallel`, which wraps it) — a direct
`client.query().to_dataframe()` in a request handler bypasses the whole
cache and re-pays 1-5s per load. The BigQuery client itself is memoized
process-wide (`get_bigquery_client`) — do not construct per-request clients.

### 5. Pricing Precedence (CLOSE-BASED for equities; broker for cash/options/intraday)

The product reads "what is this symbol worth right now" from two
fundamentally different sources, and they have different freshness and
precision profiles. Mixing them silently is the most expensive bug class
in the repo (May 2026: a single position page showed $7,465 / $7,463.61 /
$11,709 across three "current value" totals — three different sources,
three different prices, all rendered to the user as if they agreed).

**CLOSE-BASED REPORTING (June 2026 amendment).** The rule used to be
"broker snapshot wins when fresh, even for today." That captured the
broker's transient AFTER-HOURS mark whenever a sync landed after the 4pm
ET bell (real case June 2026: a 1:49pm PT manual sync pulled 4:49pm ET
extended-hours marks, so every "current value" disagreed with the close
the trader actually traded against). We flipped it for **equities/ETFs**:
reporting now anchors on the **official daily close**, and the broker mark
is used only as the intraday "right now" price before the close publishes.
The after-hours drift is surfaced separately (Today → After-hours
movers), never in the core numbers.

**The rule, anywhere a UI surface displays "current value":**

1. **Equities snap to the official close once published.** For an
   equity/ETF, today's price is `stg_daily_prices.close_price` where
   `date = current_date()` whenever that row exists (yfinance only
   publishes today's close AFTER the regular session ends, so its
   presence means "the bell rang, snap to it"). **Before** the close
   publishes (intraday), fall back to the broker live mark
   `market_value / quantity` from a FRESH `stg_current`
   (`snapshot_date >= current_date - 7`) — derive `mv / quantity`, not
   `current_price` directly (Schwab once shipped `Price` = per-share cost
   basis; see `~/.cursor/skills/broker-sync-safety/SKILL.md` 2026-05-11).
   Then latest prior close, then raw broker `current_price`. Cash and
   OPTIONS stay broker-based (no per-contract close exists; the broker
   mark is the only intraday option price). This ladder lives at the
   chokepoints: `int_enriched_current`, `mart_daily_pnl` (`broker_today_prices`
   + today CASE), `int_equity_sessions`, `mart_account_equity_daily`
   (equity-sleeve repricing — cash/margin/options untouched), and the
   Flask LIVE TODAY OVERRIDEs in `_build_chart_from_daily_pnl` /
   `_build_account_chart_from_daily_pnl` (which read close-priced
   `int_enriched_current`).

2. **yfinance fills the gap when broker is stale or absent.**
   `stg_daily_prices.close_price` (yfinance daily close) is also the
   fallback for stale snapshots, cold-start users, or positions where the
   broker never reported a snapshot. yfinance is the only legitimate
   source for HISTORICAL prices (broker doesn't ship per-day OHLC) and for
   contextual data (SPY/QQQ benchmarks, sector metadata, ex-dividend
   amounts).

3. **Today's equity row prefers the close; historical days are
   always yfinance.** For `mart_daily_pnl`'s *today* row, the official
   close wins over the broker mark once published, else the broker live
   mark carries intraday — see the "PRICE PRECEDENCE" header comment in
   `dbt/models/marts/mart_daily_pnl.sql`. For every historical day
   yfinance is the only source. For "snap to close" to show the settled
   close the SAME evening, today's close must be in `stg_daily_prices` at
   build time — the evening price-only refresh
   (`.github/workflows/prices_refresh.yml`) exists for exactly this.

4. **Use full-precision broker fields, not derived ones.** Schwab's
   stg_history fill `price` rounds to 2 decimals; stg_current's
   `cost_basis` and `market_value` carry full broker precision. For OPEN
   options, derive total P&L from snapshot's `unrealized_pnl` directly,
   not from `net_cash_flow + market_value` (mixing rounded fills with
   precise marks accumulates ~$1-2 of drift per contract — caused the
   May 2026 invariant card $1.39 disagreement).

**Surfaces that legitimately stay yfinance-only** (do not "fix" these):

- `mart_benchmark` (entry/exit hold P&L; needs historical close)
- `int_option_trade_kinds` (moneyness on open_date; historical)
- `int_option_rolls` (underlying close on roll date; historical)
- `int_dividend_events` (per-share div × holdings; broker doesn't ship
  per-share div amounts cleanly)
- `weekly_review.py` SPY/QQQ market context queries (no broker source
  for benchmarks)
- `weekly_review.py` WEEKLY_STOCK_MOVEMENT / TRADING_DAYS (date range
  + market calendar)

**Enforcement.** `dbt/tests/int_enriched_current_equity_price_consistent.sql`
is the structural invariant — for every Equity row in `int_enriched_current`,
`abs(qty * current_price - market_value) <= $0.01`. The Position Detail page
also computes a runtime invariant (`invariant_warning` in `app/position_detail.py`)
that compares **Hero total return**, **Breakdown by Type total**, and **chart
terminal** (`> $1` gap → admin-only card). Σ labeled strategy rows are not
included; attribution partitions equity across strategies and may diverge from
ledger rollups while the three checks above still agree.

**Anti-pattern to avoid.** `_align_position_pnl_chart_with_kpi` in
`app/pnl_charts.py` used to silently rescale the chart series when the
chart's terminal disagreed with the KPI. That hid a structural bug for
months. The function is now restricted to sub-$1 rounding noise; larger
gaps log loudly and trip the invariant card. Do not weaken this guard.

### 6. Option P&L Attribution (realize-on-close + MTM-while-open)

Daily option marks are this product's unique value proposition. We sync
broker snapshots so the chart can show a real options leg moving every
day — not just two cash steps on STO and BTC dates. Every chart that
plots options P&L over time MUST follow this rule:

**For each option contract, the chart shows:**

1. **$0 contribution before `open_date`** — the position didn't exist.
2. **Daily mark-to-market while open** — at each date `d`, contribute
   `cost_basis + market_value` from the snapshot (sign-correct for
   shorts and longs both; matches `short_aware_unrealized_pnl` in
   `app/upload.py`). Carry forward the last-known snapshot value
   across snapshot gaps (weekends, sync skips) up to `close_date`.
3. **$0 contribution while open if the contract has NEVER been
   snapshotted** — defer the credit to `close_date` rather than
   crediting STO premium on STO date. This applies to contracts opened
   before snapshot infrastructure existed for that user.
4. **Full realized P&L on `close_date`** — when the contract closes
   (BTC, STC, expiry, assignment, exercise) credit the full
   `net_cash_flow` (sum of all explicit fills) on the close date and
   keep that value forever.

**The mart shape:**

`mart_daily_pnl` exposes two columns per (account, user_id, symbol, date):

- `cumulative_options_pnl` — running sum of realized contributions
  across every contract that closed on or before this date.
  Monotonically accumulates.
- `open_options_unrealized_pnl` — point-in-time MTM at `d` of all
  currently-open contracts. NOT cumulative; on dates with no open
  contracts the value is 0.

The chart formula at any date is **`cumulative_options_pnl +
open_options_unrealized_pnl`**. Nothing else. There is NO `options_amount`
running-sum branch and no separate `option_market_value` add-on — those
exist as legacy diagnostics only and using them double-counts.

**`int_option_contract_daily_pnl`** is the per-contract per-date grain
that powers the mart. Adding a new option-aware UI surface? Read from
that model directly rather than hand-rolling another aggregation.

**Why this matters (the bug we're avoiding).** Pre-fix the chart
summed `stg_history.amount` for option fills on their fill date. A
short call sold for $3,000 in premium and held to OTM expiry showed a
$3,000 SPIKE on STO date and stayed flat through expiry — claiming the
P&L was earned on day 1 when in reality it was at risk for 7 days and
crystallized on day 7. For BTC closes the chart drew a $3,000 spike up
on STO date and an offsetting $3,800 spike down on BTC date — same net,
totally wrong shape. Realize-on-close fixes this by attributing the
single net realized P&L to the actual realization moment.

**Schwab's snapshot lags actual expiry by 1-2 trading days.** The
`status` and `close_date` columns in `int_option_contracts` use
calendar truth (`option_expiry < current_date()` overrides
"snapshot-implies-open"), and the today-row patch in chart helpers
filters live `current_df` rows by `option_expiry >= today` to avoid
double-counting an expired contract that the broker hasn't dropped yet.
Both layers must keep this invariant.

**OTM-at-expiry inference (same-day auto-close).** The calendar-truth
rule above only fires the DAY AFTER expiry — on expiry day itself
(`option_expiry = current_date()`) the contract stays Open until BQ's
`current_date()` advances. That gap matters when a Friday-expiry short
call closes OTM at 4:00 PM ET: the trader checking the page Friday
evening or over the weekend would otherwise see the broker snapshot's
stale cost-to-close baked into the live override, even though the
bell already settled the contract at $0. The `otm_at_expiry` CTE in
`int_option_contracts` joins `stg_daily_prices` on the underlying's
expiry-day close and marks the contract Closed (with
`close_type='ExpiredOTM'`) when the close is STRICTLY OTM relative to
the strike (call: `close < strike`; put: `close > strike`). ITM/ATM
expiries are left as Open because the broker still has discretion
(auto-exercise threshold) and the realized number differs by
assignment vs. exercise — wait for the broker action. The Monday sync
ships explicit `option_expired` and the existing `close_type` branch
takes over with the same `net_cash_flow`. `int_enriched_current`
mirrors the decision by filtering out option rows whose
`int_option_contracts.status='Closed'`, so the chart's live override
and `_compute_breakdown_by_type` don't double-count the broker's
stale mark on top of the mart's already-realized credit.

**Reconciliation invariant.** `cumulative_options_pnl(today) +
open_options_unrealized_pnl(today)`, summed across all (account,
user_id, symbol) rows for a position, MUST equal
`Σ int_option_contracts.total_pnl` for the same scope. The position
page renders an admin-only invariant card that surfaces any
disagreement; `scripts/audit/reconcile.py` CHECK 9 enforces this in CI.

**Where this rule lives:**
- Per-contract grain: `dbt/models/intermediate/int_option_contract_daily_pnl.sql`
- Mart: `dbt/models/marts/mart_daily_pnl.sql`
- Position chart: `_build_chart_from_daily_pnl` in `app/pnl_charts.py`
- Account chart: `_build_account_chart_from_daily_pnl` in `app/pnl_charts.py`
- Tests: `tests/test_chart_options_pnl.py`

**Historical marks come from `int_option_marks_daily` (Aug 2026 rewire).**
`stg_current.snapshot_date` is always `current_date()` — the live snapshot
carries TODAY's mark only. Historical per-day marks live in the SCD2
snapshot `snapshot_options_market_values_daily` (accumulating since
2026-08-04; the prior generation died in the dataset-expiration incident),
which `int_option_marks_daily` unfolds into one end-of-day mark per
(tenant, contract, day) — latest `dbt_valid_from` wins on multi-sync days,
the SCD2 `user_id=-1` MERGE sentinel maps back to NULL. Both
`int_option_contract_daily_pnl` and `int_option_pnl_series` union it
(history, `date < current_date()`) with live `stg_current` (today) —
disjoint by construction. Before this rewire the accumulated history was
consumed by NOTHING: every historical day contributed $0 MTM and the
"daily option values" differentiator existed only for today's row. Days
before first capture still fall through to $0 + realize-on-close.

**Execution review (`int_option_exit_quality` + `app/execution_quality.py`,
Aug 2026).** Every resolved contract is graded against the record of what
happened after the decision — this is the "grade the trader on ALL the
data we know" differentiator surface. Two evidence layers: (1) the EXPIRY
COUNTERFACTUAL, computable for the entire trade record from
`stg_daily_prices` (needs no snapshot history): for early BTC/STC closes,
`early_close_vs_expiry_delta` = closing cash − hypothetical expiry
settlement (negative = paid to close a contract that went on to expire
worthless; positive = dodged an ITM finish). A rolled-away contract's own
intrinsic at its ORIGINAL expiry answers roll necessity ("never tested"
vs "sidestepped $X"). Assignments/exercises/expiries are deliberately
ungraded (NULL delta). (2) the MARKS RECORD (peak capture / giveback via
`int_option_exit_analysis`, now a TABLE — it's read at request time by
insights + weekly_review), gated per-contract on `data_reliable` and
strengthening automatically as `int_option_marks_daily` coverage accrues.
Surfaces: Trader Profile "Execution review" card (gated ≥5 graded
contracts — the "after X days of data" promise), Position review mirror
sentences (≥2 graded), and day-row verdicts ("After the fact: …")
appended to the completing close's headline via the `exit_notes`
param of `build_position_story`. Copy register: neutral evidence, counts
and dollars, never advice — every early close also removed risk, and the
sentences must not pretend otherwise. READABILITY REGISTER (Aug 2026
pass, user feedback "a lot of words, I don't know the takeaway"): these
surfaces are TAKEAWAY-FIRST, not prose-first. The profile card is one
headline number + scannable findings rows ({label, value, tone, detail} —
one bold number per row, never a number repeated in both a sentence and
a chip); Daily Review verdict rows show the signed delta in its own
column with a SHORT action line (`action` field), while the full
`sentence` form is reserved for the weekly EMAIL (no layout to lean on).
New execution copy must follow this shape — no paragraph blocks on pages. All three models keep one row per
contract (`dbt/tests/option_exit_quality_one_row_per_contract.sql`);
queries are tenant-scoped + project tenant_id (pinned in
`tests/test_tenant_filtered_queries_carry_tenant_id.py`); aggregation +
phrasing pinned by `tests/test_execution_quality.py`.

**Execution review recurring surfaces (Aug 2026 follow-up).** The
lifetime card alone is read-once; three surfaces make the grading
RECURRING: (1) **Verdict maturation** — a verdict "lands" on the closed
contract's `option_expiry` (the day the counterfactual becomes knowable).
Overview's "Execution Review" section (`weekly_review.html`) shows
verdicts landed in the trailing 7 days (`verdicts_landed`) plus the
pending open loop ("N verdicts pending — next lands Fri …",
`verdicts_pending`); the weekly summary EMAIL carries the same landed
list (`_VERDICTS_SQL` in `app/email_digests_cli.py` reuses
`verdicts_landed` for phrasing so email and page can never drift).
Complementary legs of one structure (put/call spread, iron condor,
straddle/strangle) on the same `tenant_id` + expiry collapse to one
row whose dollar is the **net** vs holding every leg — a VICR $210
put and $190 put are one Put Spread, not two offsetting ±$25k lines.
Standalone options stay their own verdict. Pending counts use the
same grouping.
(2) **Rolling self-comparison** — `execution_trend` (90-day window,
≥3 recent AND ≥3 baseline exits) adds the "number that moves" sentence +
chip to the Trader Profile card: recent avg early-close delta per
contract vs the lifetime baseline before the window. (3) **Live
open-contract record** — `OPEN_OPTION_RECORD_QUERY` (int_option_contracts
Open rows) + `open_option_record`: shorts show % of premium captured so
far, longs show mark vs paid, both with days-left; strictly
observational, never advice. Verdicts live in `build_daily_review_batch`
(Overview); the open-contract record lives in `build_today_batch` (Today).
Windowing is client-side (no dates in SQL) so cached frames stay valid across
days.

---

## Mirror Score Rules

The Mirror Score:
- Reflects process, not profitability
- Is composed of 4 equally weighted components
- Must function without journaling
- Must be explainable in plain language
- Must not depend on external benchmarking

It is not a leaderboard score, performance score, or risk-adjusted return metric.
It is a behavioral consistency signal.

Definitions are expected to evolve. Design for flexibility.

---

## Market Comparison Rules

Market comparison is contextual only. It should:
- Normalize emotional reactions
- Provide environmental context (SPY/QQQ week and YTD returns)
- Show "outperforming/underperforming X indexes" as framing

It should not:
- Affect Mirror Score (unless explicitly decided later)
- Introduce gamification or create win/loss badges

The market is framing, not scoring.

---

## Pattern Detection Rules

When identifying loss patterns:
- Only surface patterns supported by data
- No speculative language
- No psychological labeling (e.g., "revenge trading")
- Use neutral phrasing

Good: "Losses clustered after prior losses."
Bad: "You revenge traded."

Patterns must be deterministic, traceable to trade-level data, and link to supporting trades.

---

## Email digests read the warehouse directly — keep SQL + templates in sync

The lifecycle/product email crons (`app/email_digests_cli.py`, run on Render —
weekly summary / weekly preview / re-engagement / connection reminder) run
**outside dbt** and query marts + intermediate models with **inlined SQL**
(`mart_weekly_summary`, `int_enriched_current`, `stg_earnings_calendar`,
`stg_daily_prices`, …). They are a **hidden downstream consumer of the schema**:
renaming/removing a column, changing a mart's grain, or altering a data
definition silently breaks a digest. The sub-query raises a runtime
`400 Unrecognized name: <col>`, the CLI **catches it and the whole section just
vanishes from the email** — no failing dbt test, no failing pytest, nothing in
the UI. The only symptom is a user saying "my email is missing X."

**Therefore: any change to a dbt model column, mart shape, or user-facing data
definition MUST also update, in the same change:**
1. the inlined SQL in `app/email_digests_cli.py` (the `_*_SQL` constants for
   weekly_summary / weekly_preview), AND
2. the matching render/template code in `app/email.py` (the `send_*_email`
   helpers + `_wrap_html`) if the fields those templates read changed.

This is the same "surface-change consistency" discipline the Flask routes get —
the email CLI is just an easily-forgotten surface because it's a cron, not a
page. Grep `email_digests_cli.py` for the model/column you're touching before
calling a schema change done.

**Canonical example (Jul 2026):** `int_enriched_current` exposes the option
strike as `option_strike`, not `strike`. The weekly_preview expirations query
selected `strike`, 400'd every run, and the entire "Options expiring" section
disappeared from every preview email with no error surfaced to the user or any
test. Fix: `option_strike AS strike` in `_EXPIRATIONS_SQL`.

To preview a template fully populated without sending, monkeypatch
`app.email.send_email` to capture `html_body`, call the `send_*_email` helper
with sample data, and render the HTML (e.g. headless Chrome `--screenshot`).

---

## Build Pipeline

### Local Development (`refresh.sh`)
```
git pull → dbt build (targeted: stg_history+ stg_current+ stg_account_balances)
→ python current_position_stock_price.py
→ dbt build (targeted: stg_daily_prices+)
```
Use `--prices` flag to skip git pull and first dbt pass (prices only).

### CI/CD
- **Pytest** (`.github/workflows/ci.yml`): runs on every PR and on pushes that touch `app/` / `tests/` / `dbt/` / `scripts/`. Uses a Postgres service so DB-backed tests run; BigQuery integration tests stay opt-in via `RUN_BQ_TESTS=1`.
- **Warehouse rebuild** (`.github/workflows/bigquery_update.yml`): `workflow_dispatch` fired by the app after every CHANGED seed write (`_dispatch_warehouse_rebuild` in `app/upload.py`), push to master/main (dbt model/loader paths), nightly scheduled backstop, manual dispatch.
  ```
  checkout → dbt build (full) → python current_position_stock_price.py → dbt build (full)
  ```
- **Reconcile audit** (`.github/workflows/reconcile.yml`): nightly + manual; runs `scripts/audit/reconcile.py` and fails the job on any FAIL check.
- **Evening prices** (`.github/workflows/prices_refresh.yml`): snaps equities to the official close after the bell.
- **Ops alert** (`.github/workflows/ops_alert.yml`): Telegram ping on a real **failure** of the warehouse rebuild, evening prices, or reconcile audit, then a Cursor cloud agent (`scripts/ops_cursor_hotfix.py` → `POST /v1/agents`) to hotfix, merge, and re-run the job. Follow-up Telegrams when the agent launches and when a `cursor/*` PR merges — each is a short sentence (`warehouse rebuild failed` / `a Cursor agent is working on it` / `the hotfix is live`), not the raw workflow name. Auto-agents are capped at **two** `[cursor-hotfix]` commits in 48 hours (not consecutive-from-tip — a human commit between auto-fixes used to reset the streak). Skip if a Hotfix agent for that workflow is already ACTIVE. After the cap Telegram still pages the failure but no new agent is launched. `ops_alert` uses a per-workflow concurrency group so parallel failures cannot spawn two agents at once. Cancelled overlapping rebuilds do not fire. Needs `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` (Telegram) and `CURSOR_API_KEY` (agent launch, no-ops if unset). Lives in GHA rather than a Cursor Automation because Automations' workflow-run trigger only fires for *push*-started runs — production warehouse rebuilds are `workflow_dispatch` / `schedule`. The key owner's Cursor account must have GitHub connected to this repo. Pinned by `tests/test_ops_cursor_hotfix.py` and `tests/test_ops_telegram.py`.
- **Product Telegram pings** (`app/ops_notify.py`): signup, Pro / AI subscribe, cancel, first data (trial clock), feedback, broken broker connection. Same bot token/chat id on the **Render web service** (GitHub secrets only cover CI). Early-stage default is all of those; quiet later with `OPS_NOTIFY_EVENTS=none` or a subset (`subscribe,cancel`). Never blocks a user request.

Note: the warehouse workflow runs two full `dbt build`s vs local targeted selects. These could be aligned.

**Deploy/CI churn guards (July 2026; simplified Aug 2026).** Since the seed-store
migration, syncs write BigQuery directly — there are no data commits to `master`
at all, so a data change can never redeploy the Flask app. (The **`ccwj` Render
web service Build Filter** — Ignored Paths: `dbt/**`, `docs/**`, `tests/**`,
`scripts/**`, `.github/**`, `.cursor/**`, `**/*.md` — remains as a guard for
non-app CODE commits; **it's a dashboard toggle, not in `render.yaml`** — see
`docs/SNAPTRADE_SETUP.md`.) Rebuild churn is bounded by: (1) the byte-exact
no-op skip in `_commit_git_paths` (no change → no write → no dispatch); (2)
**weekend webhook auto-syncs are history-only** (`history_only` in
`_run_sync`/`_sync_one_connection`, gated by `_market_closed_all_day`): they
still read activities to catch Friday's T+1 fills but never rewrite the
drifting positions/balances snapshots, so a quiet weekend produces no write →
no build (the intraday poll `--intraday` is history-only for the same reason
every run); (3) the workflow's single concurrency group with
cancel-in-progress collapsing rapid dispatches into one build.

---

## Error Handling (Known Debt)

Several `except: pass` blocks in the page modules silently swallow errors:
- Dashboard: portfolio chart, mirror score history, trader profile
- Position detail (`app/position_detail.py`): entire chart/query block
- Get-started (`app/marketing.py`): has-data check

These make debugging difficult. Errors should at minimum be logged.

---

## Code Organization

**routes.py was split into per-page modules (Aug 2026 refactor).** The old
~8,900-line monolith is now:

- `app/routes.py` (~1,000 lines) — SHARED page plumbing only: tenant scoping
  (`_tenants_for_scope`, `_user_account_list`, `_user_tenant_list`), account
  label mapping/disambiguation, `_bq_parallel`, leg/session/tag helpers —
  plus a re-export facade so `from app.routes import X` keeps working for
  older callers and tests. The facade imports at the BOTTOM of routes.py are
  load-bearing (page modules import helpers from app.routes at import time);
  don't move them up.
- `app/pnl_charts.py` — all cumulative-P&L chart machinery shared by
  Position Detail / Symbols / Accounts (`_build_chart_from_daily_pnl`,
  `_build_account_chart_from_daily_pnl`, partition hygiene, KPI alignment,
  `_build_option_matrices`, CHART_DATA[_ALL]_QUERY).
- Page modules, one per surface, endpoint names unchanged:
  `app/marketing.py` (landing/pricing/FAQ/health/onboarding),
  `app/positions_page.py` (/positions), `app/position_detail.py`
  (/position/<symbol> + tag routes), `app/sectors_page.py` (/sectors),
  `app/accounts_page.py` (/accounts, incl. the ?view=value wealth view
  rendered by `app/wealth.py`), `app/strategies.py` (/strategies, incl.
  the ?view=fit matrix rendered by `app/strategy_fit.py`),
  `app/earnings_page.py` (/earnings), `app/profile_page.py` (/profile).
  `app/symbols_page.py` is queries + the Cmd+K API + a 301 only.
- Routes register via `@app.route` on import (same pattern as admin.py /
  snaptrade.py); `app/__init__.py` imports every page module. No blueprints —
  endpoint names and every `url_for()` caller are unchanged.
- When a test monkeypatches a helper a page uses (e.g. `get_bigquery_client`,
  `_tenants_for_scope`), it must patch the PAGE module's namespace (where the
  name is resolved at call time), not `app.routes`.

**Global nav UX (Aug 2026).** `app/static/js/nav.js` (loaded from base.html for
authenticated users only) owns two cross-page behaviors: (1) the **Cmd+K / "/"
quick-switcher** — palette listing the user's symbols (open positions first;
`/api/nav/symbols` in `app/symbols_page.py`, tenant-scoped in SQL, response
cached server-side via `cached_query_df` and client-side in per-user
sessionStorage for 10 min) plus static page destinations; also opened by the navbar "Jump to…"
button; and (2) the **navigation progress bar** (`#ht-progress`) that animates
on internal link clicks / form submits so BigQuery-backed page loads don't feel
frozen. Shared CSS (palette, progress bar, and the `.ht-sticky` sticky-`thead`
wrapper class used on the longest tables — positions list, Daily Review
position breakdown + trades-this-week) lives in base.html's style block.
`scripts/dev_render_pages.py` renders any page as a logged-in dev user to
/tmp/ht_pages for headless-Chrome screenshots (mobile QA). CAVEAT: new
headless Chrome enforces a ~500px minimum viewport, so `--window-size=390`
screenshots are actually 500px layouts cropped to 390 — to QA true phone
width, wrap the rendered HTML in a 390px iframe and screenshot that.

**App-shell UX layer (Aug 2026).** Five surfaces added in one pass:
- **Skeleton-first render** (`app/skeleton.py` + `templates/_skeleton.html`):
  the four BigQuery-heavy pages (`/daily-review`, `/positions`, `/accounts`,
  `/position/<symbol>`) serve an instant shimmer shell to genuine browser
  navigations (`Sec-Fetch-Mode: navigate`), which re-fetches the same URL
  with `X-HT-Full: 1` and document.write()s the real page. A fast full
  render (<1.5s, i.e. warm query cache) sets a 10-min per-endpoint cookie
  that skips the shell entirely. Test clients / curl / monitors send no
  Sec-Fetch-Mode header and always get the full page; `?_full=1` is the
  JS-error escape hatch. Pinned by `tests/test_skeleton_shell.py`.
- **PWA install** (`app/static/manifest.webmanifest`, `app/static/sw.js`
  served at `/sw.js` scope-root from `app/marketing.py`, icons generated by
  `scripts/generate_pwa_icons.py`): service worker caches STATIC assets +
  `/offline` fallback only — navigations and data are network-only, never
  cached (auth pages must not be servable stale). nav.js surfaces the
  browser install prompt as an "Install app" item in the Account menu.
- **Time-machine day review** (`/daily-review/day/<yyyy-mm-dd>`,
  `day_detail()` in `app/weekly_review.py`, `templates/day_detail.html`):
  every past Daily-Review heatmap cell links to a per-day drill-down
  (account swing vs prior day, fills, option P&L moves, dividends, SPY/QQQ
  context, prev/next weekday nav). All five queries are tenant-scoped and
  project `tenant_id`.
- **Position review** (`app/position_story.py`, called from
  `app/position_detail.py`; UI card titled "Position review", internal
  names keep the `story` vocabulary): plain-English day-by-day review of
  the position, not a transaction-log rehash. A per-account state machine
  detects and names maneuvers — rolls (same-day close+open, strike/expiry
  direction, net credit), wheels (CSP → assignment → covered calls →
  called away, with cumulative premium), covered vs naked calls, kept
  premium on OTM expiry, splits ("your 100 shares became 300", also
  required to keep running-share state honest across pre/post-split
  fill units), assignment/exercise voice (short vs long inferred from
  tracked state or the same-day mechanical share fill at the strike,
  which is swallowed rather than double-narrated). Between trade days
  it narrates INTERLUDES from the daily-mark chart series — "A quiet 13
  weeks: +$3,434 with no trades placed" — the data only HappyTrader has
  (per-day option marks), plus "no activity / fully out of the position"
  breaks for long gaps while closed. While narrating, the engine
  accumulates a BEHAVIORAL FINGERPRINT (`_new_stats()`: rolls,
  premium collected, covered calls, kept-at-expiry, wheels completed,
  contract W/L, quiet-stretch P&L, adds/trims…) recorded by the same
  branches that write the sentences, so the mirror can never disagree
  with the review rows. `compose_mirror()` turns that + the tab-strip
  book rank into a 2-4 sentence MIRROR SUMMARY ("RKLB: 22 trade days
  across 20 months… you traded RKLB primarily for income… ranks #4 of
  94 symbols") rendered always-visible above the day-by-day review,
  which is COLLAPSED by default behind "Show the day-by-day review".
  Chart↔review choreography is CLICK-driven (deliberate act, not hover
  strobe): clicking a chart dot opens the review and scrolls/flashes its
  day; clicking a review day pops the dot's tooltip on the chart (click
  again to put it away). Leg-filter aware. Pinned by
  `tests/test_position_story.py`. COPY REGISTER: all user-facing copy is
  deliberately professional/buttoned-up (Aug 2026 revision) — no
  book/story/chapter metaphors; "trade days", "positions", factual
  sentences. Keep new sentences in that register.
- **Trader profile** (`/story`, `app/trader_story.py`, endpoint
  `trader_story`, nav "Trader Profile"): runs the review engine across
  EVERY symbol the user traded (one `stg_history` scan +
  `int_dividend_events` + `positions_summary` rollup + public
  `stg_split_events`, all through `_bq_parallel`; trades/divs/summary
  tenant-scoped in SQL AND DataFrame-filtered, pinned in
  `tests/test_tenant_filtered_queries_carry_tenant_id.py`), then folds
  the per-symbol fingerprints into one profile. Recurring loop first
  (`app/story_loop.py`, `compose_story_loop`): THIS WEEK is open options
  expiring within 14d (same-tenant complementary legs grouped like
  Execution Review). Each watch shows live unrealized P&L from
  `OPEN_OPTION_RECORD_QUERY` plus the single strongest claim from the
  same insight picker (leftover at this structure × DTE, leftover on
  this symbol, hold-later / close-earlier, horizon roll/expire, rare
  hold, live-vs-leftover, credit size). A 10-day short call does not
  inherit a 2-DTE leftover. Open strategy labels
  (`STORY_OPEN_STRATEGIES_QUERY`) name naked/covered when unambiguous —
  never invent leftover % or say "naked" without the classification
  row. Evidence, not advice. LAST WEEK compares
  the prior ISO week to the median of prior weeks
  (≥4) and headlines like/unlike (quiet when they usually trade, a
  roll burst, first expiries). Questions, not advice. Then a PROFILE
  SUMMARY
  (takeaway-first, Aug 2026 readability pass: ONE identity headline —
  income vs directional vs stock — plus scannable {label, value, tone,
  detail} fact rows for the income/directional books, contract record,
  kept-at-expiry, dividends, busiest day; the old six-sentence prose
  block + stat-chip strip repeating the same numbers was removed, and
  the card never repeats the hero's symbols/trade-days/since counts),
  NOTABLE POSITIONS cards
  (Top performer / Largest loss / Most active / Longest held / Most
  re-entered — each linking to the position page whose history proves
  the claim), a per-style scoreboard (income/directional/stock ×
  positions, profitable count, P&L), and YEAR-BY-YEAR rows computed
  straight from fills. CONSISTENCY INVARIANT: yearly premium sums STO
  credits from fills, so the fingerprint's `premium_collected` must
  count a roll's open leg too (recorded in the roll branch; pinned by
  `test_roll_open_leg_counts_as_premium_collected`) — otherwise the
  summary and the yearly rows disagree on the same page. Interlude
  (quiet-stretch) stats stay zero here: they need the per-day chart
  series, too heavy to build 90× per load; that voice remains a
  Position Detail feature. No profile-specific mart — the page is
  composed in pandas from cached queries (~all-symbol history for one
  user is thousands of rows, not millions). Skeleton-wrapped. Pinned by
  `tests/test_trader_story.py`.
- **Dark mode**: navbar toggle → localStorage → inline head script sets
  `data-bs-theme` pre-paint. Ink/surface colors are `--ht-ink` /
  `--ht-label` / `--ht-muted` / `--ht-surface` tokens on `:root` and
  `[data-bs-theme="dark"]` in base.html — page style blocks must use
  those, not hardcoded `#0f172a` (light-only ink, vanishes on a dark
  card) or `#94a3b8` (dark-only muted, vanishes on white). Bootstrap 5.3
  handles its components; leftover hardcoded light surfaces are
  overridden in base.html's `[data-bs-theme="dark"]` block; charts
  recolor via Chart.js global defaults + theme-aware `borderColor`
  ternaries in page templates. New templates with hardcoded light
  styling need a token or a dark override.

**Design refresh layer (Aug 2026).** base.html carries a global "Design
refresh" style block that owns the app's look: slate page background
(#eef1f6) so white cards read as bordered surfaces, Inter font, denser
card padding (`body .card.p-4` beats the Bootstrap utility's !important
via specificity — the same body-prefix trick lets the layer out-rank
every page's own `<style>` block regardless of order), .95rem tables
with uppercase column headers, darker section labels, and the
`.ht-statbar` component (one bordered row of label-over-value stats —
used on /positions and /position/<symbol>; prefer it over rows of
single-number KPI cards). Two rules of the layer: (1) never set a
heading `color` globally — headings must inherit so dark heroes and
dark mode stay readable; (2) new-page KPI strips should reuse
`.ht-statbar`, not invent another card grid.

Remaining debt:
- Auth/account fetching is inconsistent: some modules use `app.auth`, others use `app.models`.
- BigQuery project/dataset (`ccwj-dbt.analytics`) is hardcoded in query strings across files.

---

## What This Product Is Not

Do not add:
- Real-time trading signals
- Trade recommendations
- Position management automation
- Social comparison features
- Gamification systems
- Achievement badges

Unless explicitly instructed.

---

## When in Doubt

Ask: "Does this reinforce the trading mirror concept?"
Ask: "Is this process-focused or outcome-focused?"

If unclear: stop. Ask the human. Do not assume.

---

## Development Behavior Rules

- Do not invent data models
- Do not fabricate columns
- Do not create placeholder metrics without marking them clearly
- Leave TODO comments when assumptions are required
- Prefer structural clarity over cleverness
- Simplicity over feature sprawl

---

## Success Criteria

The product succeeds if:
- A trader understands why a week went poorly
- A trader sees recurring behavioral patterns
- A trader feels grounded after a volatile week
- A trader adjusts behavior based on insight
- The app cannot be replaced by a simple P/L dashboard

---

## Internal Design Check

Before shipping a change, ask:
1. Does this make Overview stronger?
2. Does this move logic out of Flask and into dbt?
3. Does this increase clarity?
4. Does this reduce cognitive noise?
5. If the change touches `stg_history` / staging `user_id`, `mart_daily_pnl`, or `_build_chart_from_daily_pnl`: did you validate **multiple symbols** (including at least one dividend-heavy position like JEPI) and rule out **`user_id`-NULL splits** on the same `account` mask? See `.cursor/rules/position-detail-orphan-tenancy-reconciliation.mdc`.
6. If the change touches running share counts, FIFO cost basis, or anything that JOINs `stg_history.quantity` to `stg_current`: did you validate against at least one symbol with a known stock split during the user's trade window (XLU is the canonical anchor)? See `.cursor/rules/stock-splits-share-unit.mdc`.

If not, reconsider.
