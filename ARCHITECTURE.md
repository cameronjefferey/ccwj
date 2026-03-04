# ARCHITECTURE.md

# System Architecture Overview

This document defines the technical architecture for the Trade Analytics platform.

The guiding principle:

- dbt performs computation.
- The database stores truth.
- Flask orchestrates and renders.
- The frontend displays precomputed data.
- AI interprets metrics but never fabricates them.

This architecture prioritizes:
- Page speed
- Analytical clarity
- Maintainability
- Evolvability

---

# High-Level System Flow

User Upload → Flask → Raw Database Tables → dbt Transformations → Analytics Marts → Flask API/Views → UI → AI Insight Layer

There must be a strict separation between:

1. Raw ingestion
2. Transformation
3. Presentation
4. Interpretation

---

# Layer 1: Ingestion Layer (Flask + Database)

## Responsibilities

Flask handles:

- Authentication
- File upload (CSV only)
- Account association
- Validation (schema validation only)
- Writing raw rows to database
- Triggering dbt run

Flask does NOT:

- Compute metrics
- Aggregate performance
- Calculate equity curves
- Join benchmark data
- Derive behavioral statistics

If heavy logic appears in Flask, that is a design error.

---

## Raw Data Storage

Raw uploads should be stored in:

- raw_trades table
- raw_accounts table

These must reflect the CSV schema as closely as possible.

No transformation at this stage beyond:
- Type casting
- Column normalization
- Required-field validation

If CSV format changes:
- Fail clearly
- Do not guess mappings

---

# Layer 2: Transformation Layer (dbt)

This is the analytical engine of the system.

All performance logic lives here.

## dbt Model Structure

Use a layered approach:

### 1. Staging Layer (stg_*)
Purpose: Clean and standardize raw data.

Examples:
- stg_trades
- stg_accounts

Tasks:
- Normalize column names
- Cast data types
- Standardize timestamps
- Remove obvious duplicates
- Create canonical trade IDs

No heavy aggregation here.

---

### 2. Intermediate Layer (int_*)
Purpose: Derive reusable calculations.

Examples:
- int_trade_metrics
- int_daily_performance
- int_equity_curve
- int_benchmark_returns

Trade-level metrics:
- Net PnL
- Gross PnL
- Fees
- R multiple
- Hold duration
- Win/Loss flag

Daily-level metrics:
- Daily PnL
- Daily return %
- Rolling volatility
- Rolling drawdown

All derived once.

---

### 3. Mart Layer (mart_*)
Purpose: Dashboard-ready tables.

These tables should require zero additional transformation by Flask.

Examples:

- mart_weekly_dashboard
- mart_behavior_metrics
- mart_account_summary
- mart_benchmark_comparison

Each mart table must be:

- Account-aware
- Date-filterable
- Pre-aggregated
- Indexed

These tables power the dashboard directly.

---

# Benchmark Architecture

Default benchmark: SPY

Benchmark data should be:

- Stored in benchmark_prices table
- Transformed in int_benchmark_returns
- Joined in mart tables

Benchmark comparison should be:

- Weekly return
- Relative return (alpha)
- Cumulative comparison vs user equity

Flask must never fetch benchmark prices directly.

---

# Multi-Account Design

Every core table must include:

- user_id
- account_id

Aggregation must support:

- Single account view
- All accounts combined

Combined performance must be computed in dbt, not dynamically in Flask.

Never compute cross-account aggregates in templates.

---

# Layer 3: Application Layer (Flask)

Flask is an orchestration layer only.

## Responsibilities

- Routing
- Authentication
- User session management
- Fetching precomputed metrics
- Passing data to templates
- Triggering AI insights

## Structure

Use:

- Blueprints
- Service layer abstraction
- Repository pattern for data access

Example structure:

app/
  auth/
  dashboard/
  uploads/
  services/
  repositories/
  templates/

Routes must not contain raw SQL beyond simple selects from mart tables.

All heavy queries should be encapsulated in repository classes.

---

# Dashboard Data Contract

The `/dashboard` route should fetch:

From mart_weekly_dashboard:
- Weekly PnL
- Weekly return %
- Benchmark weekly return
- Relative performance

From mart_behavior_metrics:
- Win rate
- Expectancy
- Profit factor
- Avg winner
- Avg loser
- Drawdown metrics

From int_equity_curve (or mart_equity_curve):
- Time series for chart rendering

The frontend should not derive metrics.

---

# AI Insight Layer

AI is interpretive only.

Input to AI:
- Weekly metrics
- 4–8 week rolling stats
- Benchmark comparison
- Risk metrics

AI must:
- Use only provided data
- Never invent historical context
- Clearly state insufficient data when applicable

AI output should be:
- Short narrative summary
- Strength observation
- Risk observation
- Behavioral suggestion

AI does not compute.

---

# Caching Strategy

Optional but recommended:

- Cache benchmark data
- Cache dashboard mart queries per user
- Invalidate cache after new upload

Never cache raw uploads.

---

# Database Design Principles

- Use primary keys
- Index user_id
- Index account_id
- Index trade_date
- Index weekly grouping fields

Avoid:

- Repeated full-table scans
- Dynamic subqueries in Flask
- Complex joins at request time

All complex joins should be materialized in dbt marts.

---

# Performance Target

- Dashboard load < 1.5 seconds
- AI generation < 3 seconds
- dbt run time acceptable under async background job

Uploads may be async if necessary.

---

# Evolution & Extensibility

Future expansions must not require:

- Rewriting core mart logic
- Breaking account aggregation
- Recomputing historical tables manually

Design marts to allow:

- Strategy tagging
- Regime segmentation
- Risk scoring
- Behavioral classification
- Day-of-week analysis

Schema must remain flexible.

---

# Strict Separation Rules

If you see:

- Mathematical logic in Flask → move to dbt
- Data transformation in templates → refactor
- AI inventing numbers → block
- Combined-account aggregation in route handlers → move to dbt

This separation is mandatory.

---

# Failure Protocol

If any requirement is ambiguous:

1. Stop.
2. Ask for clarification.
3. Do not assume.
4. Do not fabricate schema or metrics.

---

# Final Principle

The database is the source of truth.
dbt is the brain.
Flask is the messenger.
AI is the interpreter.

Keep those roles clean.