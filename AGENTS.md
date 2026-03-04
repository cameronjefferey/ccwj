## Agents

This document describes how AI agents are used in this repository and how to work with them effectively.

- **Purpose**: Capture conventions, expectations, and tips for using Cursor (and other AI agents) on this project.
- **Audience**: Anyone editing code here, including future you.

---

## Project context

- **Repo**: HappyTrader / `ccwj`
- **Primary stack**:
  - Python (Flask app)
  - dbt for analytics / transformations
  - GitHub Actions for automation

You can expand this section with any high-level architecture notes that are helpful for an AI assistant to know.

---
Product Identity

This product is not a trading dashboard.

It is a Trading Mirror.

Its purpose is to help active options traders:

Understand how they trade

Identify recurring loss patterns

Improve execution consistency

Separate market conditions from personal behavior

It is process-first, not P/L-first.

The product does not try to:

Predict markets

Optimize trades

Provide trade ideas

Compete with broker dashboards

It reflects behavior back to the trader.

Core Philosophy

Outcome is context. Process is the signal.

The market is the weather, not the judge.

We compare traders to themselves, not to others.

We surface patterns, not opinions.

We avoid psychological labeling.

We do not accuse. We present evidence.

The Weekly Reflection Is the Anchor

The “Weekly Trading Reflection” page is the primary experience.

Everything else supports it.

That page must:

Tell a coherent story

Load fast

Be deterministic

Be explainable

Require zero journaling input to function

It should answer:

“How did I trade this week compared to my own historical behavior, given the market context?”

Architectural Principles
1. Trade-Level Canonical Grain

The canonical grain of the system is:

One closed trade.

All aggregation rolls up from trade-level features.

Not from position-day.
Not from strategy-day.
Not from account-day.

2. dbt Owns Computation

Heavy logic belongs in dbt.

dbt should compute:

Trade-level derived features

Weekly aggregates

Pattern detection inputs

Mirror Score components

Benchmark-relative calculations

Flask should:

Authenticate users

Select account scope

Query precomputed tables

Render views

Never perform heavy aggregation

If logic is found in Flask that belongs in dbt:

Flag it

Move it

Document it

3. Multi-Account Is Required

Users trade multiple accounts.

All logic must:

Scope by account_id

Support “All Accounts” view

Avoid assuming single-account structure

If an existing model assumes one account per user, it must be corrected.

Mirror Score Rules

The Mirror Score:

Reflects process, not profitability.

Is composed of 4 equally weighted components.

Must function without journaling.

Must be explainable in plain language.

Must not depend on external benchmarking.

It is not:

A leaderboard score

A performance score

A risk-adjusted return metric

It is a behavioral consistency signal.

Definitions are expected to evolve.
Design for flexibility.

Market Comparison Rules

Market comparison is contextual only.

It should:

Normalize emotional reactions

Provide environmental context

It should not:

Affect Mirror Score (unless explicitly decided later)

Introduce gamification

Create win/loss badges

The market is framing, not scoring.

Pattern Detection Rules

When identifying loss patterns:

Only surface patterns supported by data.

No speculative language.

No psychological labeling (e.g., “revenge trading”).

Use neutral phrasing.

Good:

“Losses clustered after prior losses.”

Bad:

“You revenge traded.”

Patterns must:

Be deterministic

Be traceable to trade-level data

Link to supporting trades

Reflection Prompts

Journaling is optional.

It should:

Enhance insight if used

Never block functionality

Never be required for scoring

If journaling data is missing:

The system must still work fully.

Performance Rules

Page speed matters.

No heavy queries in request handlers.

No per-request aggregations over raw trade tables.

Always read from precomputed marts.

Optimize for weekly read performance.

What This Product Is Not

Do not add:

Real-time trading signals

Trade recommendations

Position management automation

Social comparison features

Gamification systems

Achievement badges

Unless explicitly instructed.

When in Doubt

If unsure about a design or implementation choice:

Ask:

“Does this reinforce the trading mirror concept?”

Ask:

“Is this process-focused or outcome-focused?”

If unclear:

Stop.

Ask the human.

Do not assume.

Development Behavior Rules

Do not invent data models.

Do not fabricate columns.

Do not create placeholder metrics without marking them clearly.

Leave TODO comments when assumptions are required.

Prefer structural clarity over cleverness.

Simplicity over feature sprawl.

Success Criteria

The product succeeds if:

A trader understands why a week went poorly.

A trader sees recurring behavioral patterns.

A trader feels grounded after a volatile week.

A trader adjusts behavior based on insight.

The app cannot be replaced by a simple P/L dashboard.

Internal Design Check

Before shipping a change, ask:

Does this make the Weekly Reflection stronger?

Does this move logic out of Flask and into dbt?

Does this increase clarity?

Does this reduce cognitive noise?

If not, reconsider.