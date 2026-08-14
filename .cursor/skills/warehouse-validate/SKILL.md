---
name: warehouse-validate
description: >-
  Validate and build dbt changes for the ccwj BigQuery warehouse — prod vs
  dev targets, profiles gotchas, snapshot safety, and the checks required
  before pushing. Use when editing anything under dbt/ (models, macros,
  tests, snapshots), when a dev table is missing, or before pushing a dbt
  change.
---

# Warehouse Validate — dbt build discipline for ccwj

## Fast validation (always, before pushing any dbt change)

```bash
cd dbt && ../.venv/bin/dbt parse          # fast, offline; catches SQL/YAML/ref errors
```

(The project hook runs this automatically after each dbt file edit; a clean
parse there means this step is already green.)

## Building — know which warehouse you're pointing at

| Command | Builds into | Use for |
|---|---|---|
| `cd dbt && ../.venv/bin/dbt build` | **prod `analytics`** (repo `dbt/profiles.yml` wins) | Shipping a model so prod has the table before the app change deploys |
| `./scripts/dev.sh --sync` | COPY prod `analytics` → **`analytics_dev`** (no dbt) | UI work; local pages should match customers. CI also does this after every prod warehouse / prices build |
| `./scripts/dev.sh --refresh` / `scripts/dev-refresh.sh` | rebuilds `analytics_raw_dev` from prod raw + local syncs, then full dbt build into **`analytics_dev`** via `dbt/profiles.dev.yml` | Testing *your* model changes against real data first |

- Targeted builds: `--select int_option_marks_daily+` (model and downstream).
- Dev dbt builds MUST use `dbt/profiles.dev.yml` (handled by `dev-refresh.sh`) — plain `dbt build` from `dbt/` always hits prod. Do not rely on a hand-maintained `~/.dbt/profiles.yml`.
- A missing table in `analytics_dev` (e.g. `scripts/dev_render_pages.py`
  errors) means the model was built to prod only — `--sync` to clone prod's
  copy, or `--refresh` to rebuild from local dbt.

## Snapshot safety (non-negotiable)

- Snapshots are the ONLY long-lived tables; everything else rebuilds each run.
- `target_schema` must stay `target.schema` — hardcoding `'analytics'` makes
  dev builds MERGE into prod snapshots.
- NEVER set a table expiration in the `analytics` dataset (the Aug 2026
  incident silently deleted `snapshot_account_balances_daily` at day 60).
- `scripts/snapshot_guard.py` backs up snapshots to `analytics_backups`
  before every CI build and fails the build if pre-today history shrinks.
  If it trips: restore from the newest `analytics_backups.snapshot_*_<yyyymmdd>`
  BEFORE the next build merges bad state forward.
- If a snapshot fails on full `build` but passes `dbt snapshot --select <name>`,
  re-run the full build once (rare BQ/dag race).

## Before pushing a schema change (columns renamed/removed, grain changed)

- [ ] `dbt parse` + a real `dbt build` (prod) succeeded
- [ ] Deploy order: dbt ships before or with the app change that reads it
- [ ] Grep `app/email_digests_cli.py` for the model/column — the email crons
      inline SQL and fail SILENTLY (section just vanishes from the email)
- [ ] Any new per-account mart keys on `tenant_id` (never the `account` label)
- [ ] New tenant-facing app queries project `tenant_id` and are added to
      `tests/test_tenant_filtered_queries_carry_tenant_id.py`
- [ ] If touching share counts / FIFO / `stg_history.quantity` joins:
      validate a split symbol (XLU) — see `stock-splits-share-unit.mdc`

## Quick source checks (read-only)

Prefer the `bigquery` MCP server (`execute_sql`, SELECT-only) for row counts
and tenant lookups — e.g. before assuming a pipeline bug, count the source
rows (`data-pipeline-fixes.mdc` rule #1).
