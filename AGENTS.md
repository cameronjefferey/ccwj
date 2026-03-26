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

### Weekly Review (`/weekly-review`) — PRIMARY EXPERIENCE
**Status: Working well. Core anchor of the product.**

This is the page a paying customer opens daily. It should answer:
> "How did I trade this week compared to my own historical behavior, given the market context?"

What's working:
- Comparison table (vs yesterday, last week, last month, start of week)
- Market comparison row (SPY / QQQ) with "outperforming X indexes" summary
- Today Strip (open positions with live prices)
- Expiring This Week section
- Daily P&L Calendar Heatmap
- Best / Worst trade of the week cards
- Trades table for the week
- Friday / Monday / mid-week mode switching
- Market performance from BigQuery (replaced yfinance for speed)
- Combined queries for weekly summary and open positions (performance win)

What was removed:
- All journal prompts, mood tracking, behavioral anomaly sections, and reflection CTAs
- Journal is fully removed from the product (see Journal section below)

What could be better:
- Friday mode could have a stronger "week in review" narrative
- Pattern detection ("losses clustered after prior losses") is not yet surfaced here
- No streak tracking (consecutive winning/losing weeks)

### Position Detail (`/position/<symbol>`) — DEEP DIVE PAGE
**Status: Functional with recent fixes. Complex page with the most logic.**

What's working:
- Position Legs with sequential numbering (Leg 1, Leg 2, etc.)
- Leg filtering — click a leg pill to scope the entire page
- Cumulative P&L chart with equity, options, dividends, total lines + stock price overlay
- Win/Loss Matrix (DTE vs Strike Distance) per strategy
- Expandable raw trades per leg (click arrow to see underlying transactions)
- KPIs recalculated per-leg when filtered
- Strategy breakdown table filtered by leg
- Orphan options grouped into non-overlapping "options only" legs
- Short position handling in equity P&L (call assignments selling more shares than held)
- Snapshot market value nulled out when leg-filtering (prevents cross-leg inflation)
- Cumulative columns re-zeroed per-leg so chart starts at 0
- Covered Call classification requires >= 100 shares (`int_strategy_classification`)
- `_date_to_leg` prioritizes equity sessions over orphan sessions

Known issues:
- Heavy Python computation: `_build_chart_from_daily_pnl` iterates every row to compute
  running average-cost equity P&L. This is stateful and hard to move to dbt, but is a
  performance concern for positions with years of daily data.
- `_build_option_matrices` uses nested loops over DTE/strike buckets in Flask.
- Pre-snapshot option P&L shows only cash flows (no mark-to-market). This means a dip
  when a LEAP is purchased that recovers once snapshots begin. Acceptable tradeoff.
- `routes.py` is a monolith (~2700+ lines). Position detail logic alone is ~500 lines.

### Dashboard / Home (`/`, `/index`, `/dashboard`)
**Status: Working. Summary landing page.**

Shows account overview, recent trades, portfolio chart, trader profile.
Silent error handling (`except: pass`) on chart build, mirror score history, and
trader profile — errors are invisible in production.

### Positions List (`/positions`)
**Status: Working. Entry point to position detail.**

Lists all positions with strategy tags, P&L, status. Links to position detail.
Pagination in Python (`per_page = 25`).

### Symbols (`/symbols`)
**Status: Functional but being superseded by Position Detail.**

Previously the main way to view per-symbol data. Matrix and detail logic has been
moved to Position Detail. This page is now mostly a navigation step.
Still has heavy pandas work (groupby, iterrows) that could be simplified.

### Strategies (`/strategies`)
**Status: Needs rework.**

The strategy cards at the top don't convey useful information when you click into a
strategy. The page reads from `mart_strategy_performance` (good) but the presentation
needs to tell a better story. Should answer: "For this strategy, am I getting better
or worse over time?"

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
1. Does this make the Weekly Review stronger?
2. Does this move logic out of Flask and into dbt?
3. Does this increase clarity?
4. Does this reduce cognitive noise?

If not, reconsider.
