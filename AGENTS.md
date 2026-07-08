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

**Local dev and production are environment-separated (June 2026).** Local dev reads/writes its own warehouse and seed branch so dev-environment writes never mix with production (numeric user ids collide across the two Postgres databases; an admin purge by `user_id=10` once deleted the other environment's rows). The knobs: `.env` sets `BQ_DATASET=analytics_dev` (every app query's hardcoded `ccwj-dbt.analytics.` ref is rewritten at the `get_bigquery_client()` chokepoint — `_apply_dataset_override` in `app/bigquery_client.py`) and `GITHUB_BRANCH=dev-seeds` (seed CSV reads/writes target the `dev-seeds` branch; CI builds prod from master/main only). Production leaves both env vars unset.

The dev dataset is a **full mirror for testing**: `scripts/dev-refresh.sh` builds `analytics_dev` from latest prod seeds (origin/master) MERGED with the local environment's own syncs (origin/dev-seeds; merge logic in `scripts/merge_dev_seeds.py`, local tenants from the local `broker_tenants` win) — using the **working tree's dbt code**, so model changes are testable against real data before they ship. `refresh.sh` / plain `dbt build` from `dbt/` still target prod `analytics` (the repo's `dbt/profiles.yml` takes precedence over `~/.dbt` — dev builds MUST pass `--profiles-dir ~/.dbt --target dev`). dbt snapshots use `target_schema=target.schema` (never hardcode `'analytics'`, or dev builds MERGE into the prod snapshot tables). Raw market-data sources (`sources.yml`, prices/splits/earnings) intentionally stay shared on `analytics` — they're not tenant data and the loaders only run once. Never purge shared-warehouse rows by numeric `user_id` — only by `tenant_id`.

**Per-broker staging layer (June 2026).** The three base staging models (`stg_history`, `stg_current`, `stg_account_balances`) no longer read the seeds directly — each is now a UNION of thin per-broker adapter models in `dbt/models/staging/brokers/` (`stg_broker_{schwab,alpaca,fidelity,interactive}_*`, plus an `stg_broker_other_*` catch-all) followed by the unchanged heavy parse. (`interactive` = IBKR; slug is the lowercased first token of the account label "Interactive Brokers …".) Each broker has its own model so broker-specific quirks (date formats, sign conventions, duplicate-fill patterns) stay isolated and independently queryable/testable instead of being special-cased in the shared parse. Broker identity is DISPLAY-derived (not a tenancy boundary): `tenant_id` is the literal `snaptrade:<uuid>` for EVERY broker, so the only broker signal in the warehouse is the account-label prefix — `dbt/macros/broker_slug_from_account.sql` maps it to a slug (`"Schwab Account"`→`schwab`, `"Alpaca Paper Account"`→`alpaca`). Tenant isolation stays on `tenant_id`; never scope a user-facing read by `broker_slug`. The catch-all is mutually-exclusive+exhaustive with the named brokers so no row is ever dropped; the `dbt/tests/broker_split_preserves_all_rows.sql` test enforces union-count parity. **To add a brokerage:** (1) add its slug to `known_brokers()`, (2) add `stg_broker_<slug>_{history,current,balances}` (one-line `broker_*_rows('<slug>')` calls), (3) add those three models to the UNION in the matching base staging model, (4) add them to the per-surface unions in `dbt/tests/broker_split_preserves_all_rows.sql`. `dim_broker_tenants.broker_slug` now shows the real brokerage (was the always-`snaptrade` aggregator slug, kept as `aggregator_slug`).

**Brokerage sync is the most failure-prone surface in the product** — SnapTrade aggregator (Schwab, Fidelity, Vanguard, Robinhood, IBKR, etc.). Before editing `app/snaptrade.py`, `app/snaptrade_normalize.py`, `app/upload.py` (especially `merge_and_push_seeds` / `_merge_seed_with_existing`), `app/snaptrade_sync_cli.py`, the `dbt/seeds/*.csv` shape, `.github/workflows/bigquery_update.yml`, the multi-account Sync flows on `/profile?tab=account` / `/snaptrade/accounts`, or any column on `broker_tenants` / `snaptrade_users` (`connection_broken_at`, `first_sync_completed`), **load the `broker-sync-safety` agent skill** (`~/.cursor/skills/broker-sync-safety/SKILL.md`) and walk its pre-flight checklist. The skill is an append-only register of bugs already shipped, the invariants that must hold, and the recovery runbook. **When you ship a sync fix, append a new "Bugs we've shipped" entry to that skill before closing the PR** — the structured format (symptom / root cause + file:line / fix commit / regression test / lesson) is documented at the bottom of SKILL.md.

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

### Daily Review (`/daily-review` — endpoint still named `weekly_review` for url_for() compat) — PRIMARY EXPERIENCE
**Status: Rebuilt May 2026. End-of-day pulse page; mode-switching removed.**

This is the page a paying customer opens at market close every day. It should answer:
> "What just happened, what should I watch, and how is every position / strategy / sector
> doing in total?"

The previous Friday / Monday / Mid-week mode toggle was deleted. Users want the same answer
every day — the modes were three pages glued together. The endpoint name is still
`weekly_review` (route URL is `/daily-review` with `/weekly-review` kept as a legacy alias for
bookmarks) so the 30+ `url_for('weekly_review', ...)` callers across templates, auth,
profile, upload, admin, etc. don't break.

What's working (May 2026 rebuild):
- Today hero: account total + day delta + market context line
- Since you last looked: stock moves / newly ITM / newly near expiry / opens & closes
- Account snapshot row: today / vs yesterday / vs 1w / vs 1m (per-account and total)
- Today's biggest movers: $ price-impact on currently-held shares, sorted up/down
- After-hours movers: broker mark (as of last sync) vs today's official close,
 per held equity — surfaces post-close drift without polluting the
 close-based core numbers (reads `stg_current` mark deliberately; the only
 user-facing surface that intentionally shows the broker after-hours mark).
 Only rendered once the U.S. regular session has closed (`_us_market_session()`
 state == `after_hours`) AND the broker mark itself is post-close. The
 warehouse has no per-row capture time (`stg_current.snapshot_date` is just
 `current_date()`), so `post_close_broker_tenant_ids` in `app/snaptrade.py`
 reads SnapTrade's authoritative per-account `holdings_last_successful_sync`
 (falling back to `last_sync_at` when a broker never reports the former) and
 returns the SET of tenant_ids that synced at/after today's 4pm ET close;
 the query is SCOPED to exactly those tenants (`tenant_id = snaptrade:<uuid>`).
 A mid-session/stale sync would otherwise compare a pre-close intraday mark to
 the official close and render the day's move BACKWARDS (real case 2026-07-07:
 BE synced ~$295 mid-session, closed $269.57 → a bogus +$25.88/sh "after-hours"
 gain). Per-tenant scoping (NOT an all-or-nothing weakest-link gate) so one
 stale/broken account is dropped from the aggregate instead of hiding the whole
 section from the healthy post-close accounts. During the open session/pre-market
 the query is skipped entirely. NOTE the query anchors the close on
 `CURRENT_DATE('America/New_York')`, not bare `CURRENT_DATE()` (UTC) — the
 latter rolls to "tomorrow" at 8pm ET and made the section silently empty every
 evening (the window it's most useful). The gate is STRICT and never softened
 to "just render something": showing a stale/intraday mark as after-hours drift
 erodes trust in the whole page, so if there is no genuine post-close sync the
 section stays hidden. DEV NOTE: local dev is built from committed seed CSVs
 (no live syncs) so sync timestamps are NULL and the section will not appear in
 dev — that is expected, not a bug; it renders in prod once a real post-close
 sync lands and today's close is published.
- Watch list: upcoming earnings (≤14d), expiring options (≤14d), projected ex-divs (≤30d)
- Daily account Δ heatmap (rolling 12 weeks, 4 visible by default)
- Current positions strip (open-position cards with live prices)
- Position breakdown table: per-symbol G/L Stock | G/L Option | Dividend | Net |
  Capital | Days | %Return | Annualized — same shape as the trader's external Excel
- Strategy breakdown: same shape rolled up by classified strategy
- Sector breakdown: same shape rolled up by yfinance sector
- Subsector breakdown: same shape by yfinance industry

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
- Projected ex-dividend dates come from a cadence heuristic on `stg_daily_prices.dividend`
  (median spacing of last 6 events). Labeled "projected" in UI; the long-term fix is a
  yfinance Calendar refresher script that ships real future ex-div dates.

What could be better:
- "Today's $ impact" right now only covers equity price-moves. Option MTM moves and
  dividends paid today aren't yet in the today-net number.
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

**Stock splits (critical):** Schwab ships `stg_history.quantity` in the **share-units that existed at the fill time** — pre-split for old buys, post-split for new sells. The broker snapshot (`stg_current`) is always in **today's** share-units. Without explicit split-adjustment, FIFO cost basis on a buy → split → sell mismatches units and produces **massive phantom realized losses** (XLU May 2026: $-65,925 phantom on a position whose real realized was +$1,822.50). Splits land in `daily_split_events` (loader: `current_position_stock_price.py`) → `stg_split_events` → `int_split_factors`, then JOINed and applied to quantity in `int_equity_sessions`, `int_closed_equity_legs`, `int_dividend_events`, and `mart_daily_pnl`. Cash flow is split-invariant. Regression: `dbt/tests/equity_running_qty_matches_snapshot_after_splits.sql` + `tests/test_stock_splits.py`. Details: `.cursor/rules/stock-splits-share-unit.mdc`.

**Verification:** Never ship Position Detail / `mart_daily_pnl` / `_build_chart_from_daily_pnl` changes validated on **one symbol only**. Always check at least **one dividend ETF** (JEPI‑class), a **mixed equity+option** position, **multiple tenants/accounts**, and — if the change touches running share counts — at least **one symbol with a known split during the user's window** (XLU is the canonical regression case).

Known issues:
- Heavy Python computation: `_build_chart_from_daily_pnl` iterates every row to compute
  running average-cost equity P&L. This is stateful and hard to move to dbt, but is a
  performance concern for positions with years of daily data.
- `_build_option_matrices` uses nested loops over DTE/strike buckets in Flask.
- Pre-snapshot option P&L shows only cash flows (no mark-to-market). This means a dip
  when a LEAP is purchased that recovers once snapshots begin. Acceptable tradeoff.
- `routes.py` is still long. Position detail used to be ~1,650 lines; the
  legs teardown removed ~150 of stateful Python.

### Dashboard / Home (`/`, `/index`, `/dashboard`)
**Status: Working. Summary landing page.**

Shows account overview, recent trades, portfolio chart, trader profile.
Silent error handling (`except: pass`) on chart build, mirror score history, and
trader profile — errors are invisible in production.

### Positions List (`/positions`)
**Status: Working with recent filter-discipline pass.** Entry point to position detail.

Lists all positions with strategy tags, P&L, status. Links to position detail.
Pagination in Python (`per_page = 25`).

What's working:
- Hero "X open / Y closed" chips honor every active filter (account,
  strategy, symbol, status, subsector, sector, date range). Pre-fix the
  chips read off the unfiltered df and lied about the body.
- Pagination + symbol-cell links preserve all 7 filter dimensions.
- "No accounts linked yet" copy fires only when the user genuinely has no
  linked accounts. Connected-but-empty users get a "data is pending"
  message instead.
- Quick Stats Winners shows raw `num_winners`, not the buggy
  `total_trades * win_rate` derivation that over-reported by 2-3x.
- Date-filtered view (DATE_FILTERED_QUERY) uses the same realized /
  unrealized split and same status logic as the positions_summary mart;
  pre-fix the date view emitted a 3rd "Mixed" status the all-time view
  never showed, and derived realized_pnl from total_pnl by status which
  collapsed open-equity-with-interim-sells P&L into unrealized.

Architecture:
- Dividend attribution rules live in
  `dbt/macros/attribute_dividends_to_strategy.sql` (single source of
  truth). `dbt/models/marts/positions_summary.sql` calls the macro.
  The runtime DATE_FILTERED_QUERY in `app/routes.py` mirrors the macro
  output in inlined SQL (it has to — start/end dates come from the URL
  at request time, after dbt has finished building). `ATTRIBUTION_INVARIANT`
  comments in both files cross-reference; integration test
  `test_date_filtered_at_full_window_matches_mart` (set `RUN_BQ_TESTS=1`)
  pins them together.

Known issues:
- DATE_FILTERED_QUERY is still ~150 lines of inlined SQL in routes.py.
  Can't be a pure dbt mart because of the runtime parameterization, but
  the dividend-attribution complexity now lives in dbt.

### Symbols (`/symbols`)
**Status: Functional but being superseded by Position Detail.**

Previously the main way to view per-symbol data. Matrix and detail logic has been
moved to Position Detail. This page is now mostly a navigation step.
Still has heavy pandas work (groupby, iterrows) that could be simplified.

### Strategies (`/strategies`)
**Status: Improved — drill-down now includes Breakdown by Type + tenant hardening.**

Cards still roll up lifetime performance from `mart_strategy_performance`; monthly context comes from `mart_strategy_trend`. When you click a strategy, you now get a **Breakdown by Type** table (equity sessions vs option contracts vs attributed dividends): equity and options are summed from `int_strategy_classification`; dividends roll up from attributed `total_dividend_income` on `positions_summary`. That mirrors the Position Detail mental model for a single strategy label.

Tenant isolation: row-level query results go through `_filter_df_by_accounts(...)` before any pandas work, same as `/positions`. Pure `SUM(...) ...` aggregates without an account column rely on SQL `_account_sql_and` only. Failed `mart_strategy_trend` reads are logged instead of silently swallowed.

Symbol links in the drill-down table preserve the selected account filter (`?account=`).

**Still could be stronger:** richer narrative on the cards, less request-time SQL (pre-aggregate symbol tables in dbt), DTE breakdown moved fully into the warehouse.
### Mirror Score (`/mirror-score`)
**Status: Functional but definition is evolving.**

Behavioral consistency signal. 4 equally weighted components. Reads from
`mart_daily_trading_metrics` but does rolling-window comparison in Flask.
This Flask-side computation could eventually move to a dbt mart.

### Benchmark (`/benchmark`)
**Status: Working. "If You Did Nothing" comparison.**

Reads from `mart_benchmark`. Light aggregation in Flask for strategy grouping.
Uses different auth import pattern (`app.auth.get_accounts_for_user`) vs other pages.

### AI Insights (`/insights`)
**Status: Working. Gemini-powered narrative.**

Reads `positions_summary` mart, builds prompt, sends to Gemini.
Follows ARCHITECTURE.md: AI interprets, doesn't compute.

### Trade Kinds (`/trade-kinds`)
**Status: Working. Option trade classification.**

DTE bucket, moneyness, outcome analysis. Reads from `mart_option_trades_by_kind`.
Optional Gemini summary.

### Taxes (`/taxes`)
**Status: Working. Tax lot reporting.**

Reads from `int_tax_lots` and `stg_history` (dividends). Clean.

### First Look (`/get-started`)
**Status: Working. Onboarding page.**

### Upload (`/upload`)
**Status: Working. CSV upload + Schwab sync.**

---

## Journal — REMOVED

All journal features have been removed from the product. The journal concept did not
align with the core principle that the system must work fully without user input.

What was removed:
- Journal pages and templates (deleted: `journal.html`, `journal_form.html`, `journal_import.html`)
- Journal prompts/CTAs from all pages (weekly review, position detail, insights, etc.)
- Journal import from `__init__.py` (routes no longer registered)
- Mood tracking and behavioral anomaly sections

Cleanup still needed:
- `app/journal.py` file still on disk (dead code, not imported)
- `app/models.py` still has `journal_entries` / `journal_tags` tables and CRUD helpers
- `tests/test_data_isolation.py` still references `/journal/` routes (will 404)

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
- `_build_chart_from_daily_pnl` in `routes.py`: stateful equity P&L simulation via
  row iteration. Hard to move to dbt because of running average-cost logic, but heavy.
- `_build_option_matrices`: nested groupby + loops in Flask.
- Mirror Score rolling-window comparison done in Flask instead of dbt.
- Symbols page has extensive pandas groupby/iterrows.
- `DATE_FILTERED_QUERY` in `routes.py`: runtime-parameterized analytical SQL (not a
  static mart) — documented rationale exists but still violates the principle.

If logic is found in Flask that belongs in dbt: flag it, move it, document it.

**Before pushing dbt changes** (avoids learning errors only in prod): with `~/.dbt/profiles.yml` and network, run
`cd dbt && ../.venv/bin/dbt parse && ../.venv/bin/dbt build`, or `scripts/dbt-validate.sh` (same). `parse` is fast and offline; `build` must succeed against BigQuery. If a snapshot fails on the first `build` but succeeds on `dbt snapshot --select <name>`, re-run the full `build` once (rare BQ/dag race).

### 3. Multi-Account Is Required

Users trade multiple accounts. All logic must:
- Scope by `account_id`
- Support "All Accounts" view
- Avoid assuming single-account structure

### 4. Performance Rules

Page speed matters.
- No heavy queries in request handlers
- No per-request aggregations over raw trade tables
- Always read from precomputed marts
- Optimize for weekly read performance
- Market data comes from `stg_daily_prices` in BigQuery (not live yfinance calls)

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
The after-hours drift is surfaced separately (Daily Review → After-hours
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
also computes a runtime invariant (`invariant_warning` in `app/routes.py`)
that compares **Hero total return**, **Breakdown by Type total**, and **chart
terminal** (`> $1` gap → admin-only card). Σ labeled strategy rows are not
included; attribution partitions equity across strategies and may diverge from
ledger rollups while the three checks above still agree.

**Anti-pattern to avoid.** `_align_position_pnl_chart_with_kpi` in
`app/routes.py` used to silently rescale the chart series when the
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
- Position chart: `_build_chart_from_daily_pnl` in `app/routes.py`
- Account chart: `_build_account_chart_from_daily_pnl` in `app/routes.py`
- Tests: `tests/test_chart_options_pnl.py`

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

## Build Pipeline

### Local Development (`refresh.sh`)
```
git pull → dbt build (targeted: stg_history+ stg_current+ stg_account_balances)
→ python current_position_stock_price.py
→ dbt build (targeted: stg_daily_prices+)
```
Use `--prices` flag to skip git pull and first dbt pass (prices only).

### CI/CD (`.github/workflows/bigquery_update.yml`)
Triggers: push to master, daily cron (9 PM UTC / ~1 PM PST), manual dispatch.
```
checkout → dbt build (full) → python current_position_stock_price.py → dbt build (full)
```
Note: CI runs two full `dbt build`s vs local targeted selects. These could be aligned.

---

## Error Handling (Known Debt)

Multiple `except: pass` blocks in `routes.py` silently swallow errors:
- Dashboard: portfolio chart, mirror score history, trader profile
- Position detail: entire chart/query block
- Get-started: has-data check

These make debugging difficult. Errors should at minimum be logged.

---

## Code Organization (Known Debt)

- `routes.py` is a ~2700-line monolith handling dashboard, positions list, position detail,
  symbols, accounts, and marketing pages. Position detail alone is ~500 lines.
- `mirror_score.py` and `benchmark.py` import helpers from `routes.py` — unusual coupling.
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
1. Does this make the Daily Review stronger?
2. Does this move logic out of Flask and into dbt?
3. Does this increase clarity?
4. Does this reduce cognitive noise?
5. If the change touches `stg_history` / staging `user_id`, `mart_daily_pnl`, or `_build_chart_from_daily_pnl`: did you validate **multiple symbols** (including at least one dividend-heavy position like JEPI) and rule out **`user_id`-NULL splits** on the same `account` mask? See `.cursor/rules/position-detail-orphan-tenancy-reconciliation.mdc`.
6. If the change touches running share counts, FIFO cost basis, or anything that JOINs `stg_history.quantity` to `stg_current`: did you validate against at least one symbol with a known stock split during the user's trade window (XLU is the canonical anchor)? See `.cursor/rules/stock-splits-share-unit.mdc`.

If not, reconsider.
