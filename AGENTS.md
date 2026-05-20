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

**Tenant key is migrating from `(account_name, user_id)` to a stable `broker_account_id`** — see `docs/BROKER_ACCOUNT_ID_MIGRATION.md`. **Stages 0, 1, 2A, 2B, 3A, 4A all live as of 2026-05-20.** Stage 0 (Postgres `broker_accounts` table + stamping at every writer + nullable column on every seed CSV) is in. Stage 1 (one-shot seed backfill via `scripts/backfill_seed_broker_account_ids.py`) has stamped 12 resolvable `(account, user_id)` tuples (3,726 rows); 20 tuples (~7,389 rows) referencing deleted Postgres users remain NULL pending operator triage. Stage 2A built `dim_broker_accounts` (sourced from seed CSVs directly, NOT staging — staging's canonical-owner CTE would assign deleted-user uids and break the mapping) and propagated `broker_account_id` through `int_strategy_classification`, `positions_summary`, and `mart_daily_pnl`. **Stage 2B finished propagation through every remaining intermediate and mart model the Flask app reads from** (~35 models, three patterns: inline LEFT JOIN to dim for staging readers, passthrough for downstream readers, wrap-and-join for union/aggregation shapes). `int_split_factors` is symbol-only and intentionally does not carry the column. Stage 3A added Flask helpers `_broker_account_sql_and`, `_broker_account_sql_filter`, `_filter_df_by_broker_account_ids`, `_resolve_filter_broker_account_ids` in `app/routes.py` (TESTED but NOT yet wired into routes — Stage 3B is operator-gated). Stage 4A added 5 warning-severity dbt singular tests (`every_broker_account_id_exists_in_dim_broker_accounts`, `seed_broker_account_id_unique_per_account_user`, `dim_broker_accounts_unique_per_id`, `dim_broker_accounts_unique_per_user_account_pair`, `every_seed_row_has_broker_account_id`). The remaining ~7,389 orphan rows currently have NULL `broker_account_id` and surface in `every_seed_row_has_broker_account_id` (58 distinct rows in the test output — the Stage 4B punch list) — pending operator triage (delete / reassign / recreate / quarantine). Stage 3B (wire defense-in-depth into routes) and Stage 4B (drop legacy CTEs + leniency) are deferred to follow-up operator passes. **DO NOT** flip the filter ahead of Stage 3B — production data shows `mart_account_equity_daily` / `mart_wealth_daily` rows can have NULL `broker_account_id` even for live users because `stg_canonical_account_owner` rewrites historical snapshot user_ids to stale values that don't appear in dim; the right Stage 3B pattern is additive (`broker_account_id IS NULL OR broker_account_id IN (...)` on top of the existing `(account, user_id)` filter), one route at a time. Any new sync code or writer must pass `broker_account_id` to `merge_and_push_seeds` (it's required); derive it via `get_or_create_broker_account(...)` from the connection / account row.

**Brokerage sync is the most failure-prone surface in the product** — both the native Schwab connector and the SnapTrade aggregator (Fidelity/Vanguard/Robinhood/IBKR). Three production regressions shipped in a single chat in May 2026 (banner persistence, bulk lookback, seed merge dedup+tenancy). Before editing `app/schwab.py`, `app/snaptrade.py`, `app/snaptrade_normalize.py`, `app/upload.py` (especially `merge_and_push_seeds` / `_merge_seed_with_existing`), `app/schwab_sync_cli.py`, `app/snaptrade_sync_cli.py`, the `dbt/seeds/*.csv` shape, `.github/workflows/bigquery_update.yml`, the multi-account Sync flows on `/profile?tab=account` / `/schwab/accounts` / `/snaptrade/accounts`, or any column on `schwab_connections` / `snaptrade_connections` / `snaptrade_accounts` (`refresh_token_invalid_at`, `connection_broken_at`, `schwab_first_sync_completed`, `first_sync_completed`, `token_json`), **load the `broker-sync-safety` agent skill** (`~/.cursor/skills/broker-sync-safety/SKILL.md`) and walk its pre-flight checklist. The skill is an append-only register of bugs already shipped (across BOTH connectors), the invariants that must hold, and the recovery runbook. **When you ship a sync fix, append a new "Bugs we've shipped" entry to that skill before closing the PR** — the structured format (symptom / root cause + file:line / fix commit / regression test / lesson) is documented at the bottom of SKILL.md.

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

### 5. Pricing Precedence (broker-first when fresh, yfinance fallback)

The product reads "what is this symbol worth right now" from two
fundamentally different sources, and they have different freshness and
precision profiles. Mixing them silently is the most expensive bug class
in the repo (May 2026: a single position page showed $7,465 / $7,463.61 /
$11,709 across three "current value" totals — three different sources,
three different prices, all rendered to the user as if they agreed).

**The rule, anywhere a UI surface displays "current value":**

1. **Broker snapshot wins when fresh.** `stg_current` (the Schwab /
   manual-CSV positions snapshot) is the source of truth for current
   per-share price, market value, cost basis, and unrealized P&L
   whenever `snapshot_date >= current_date - 7`. Derive per-share price
   as `market_value / quantity` (the broker's own implied current price)
   rather than trusting `current_price` directly — Schwab's API has
   shipped at least one bug where `Price` was actually the per-share cost
   basis (see `~/.cursor/skills/broker-sync-safety/SKILL.md` 2026-05-11).

2. **yfinance fills the gap when broker is stale or absent.**
   `stg_daily_prices.close_price` (yfinance daily close) is the fallback
   for stale snapshots, cold-start users, or positions where the broker
   never reported a snapshot. yfinance is also the only legitimate source
   for HISTORICAL prices (broker doesn't ship per-day OHLC) and for
   contextual data (SPY/QQQ benchmarks, sector metadata, ex-dividend
   amounts).

3. **Today is asymmetric.** For mart_daily_pnl's *today* row,
   broker-implied (mv/qty) wins over yfinance close when the snapshot is
   fresh — see the "PRICE PRECEDENCE" header comment in
   `dbt/models/marts/mart_daily_pnl.sql`. For every historical day
   yfinance is the only source.

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
