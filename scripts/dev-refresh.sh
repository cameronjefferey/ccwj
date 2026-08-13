#!/usr/bin/env bash
# Dev-environment refresh — the local counterpart of refresh.sh.
#
# Local dev is environment-separated from production:
#   - raw seeds: `analytics_raw_dev` (the app writes there via BQ_RAW_DATASET)
#   - warehouse: `analytics_dev`     (Flask reads there via BQ_DATASET)
# Production stays on `analytics_raw` + `analytics`, built by GitHub
# Actions only (the app dispatches the rebuild after each changed write).
#
# analytics_dev is a FULL MIRROR for local testing: prod raw seed rows
# (from analytics_raw) PLUS the local environment's own syncs (from
# analytics_raw_dev; merge logic in scripts/dev_refresh_raw.py — local
# tenants registered in the local Postgres win). Everything is testable
# locally with real data; local writes still never touch prod.
#
# The dbt build runs from the WORKING TREE (models/snapshots/macros), so
# model changes are testable against real data before they ship.
#
# This script builds into analytics_dev only. It NEVER touches the prod
# dataset: refresh.sh / plain `dbt build` from dbt/ use the repo's
# dbt/profiles.yml (dataset: analytics) — that path is for intentional
# prod builds only.
#
# NOTE: dbt prefers a profiles.yml in the project directory over ~/.dbt,
# so a --profiles-dir that is NOT the repo's dbt/ is REQUIRED — without it
# the build targets prod `analytics` (this exact mistake wiped prod once
# during setup; don't remove the flag). We use the in-repo
# dbt/profiles.dev.yml (dataset: analytics_dev) copied to a temp dir, so
# a missing ~/.dbt/profiles.yml can never silently hit prod.
# DBT_RAW_DATASET points the raw_broker source at analytics_raw_dev.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -x "$SCRIPT_DIR/dbt/.venv/bin/dbt" ]]; then
  VENV_DBT="$SCRIPT_DIR/dbt/.venv/bin/dbt"
  VENV_PY="$SCRIPT_DIR/dbt/.venv/bin/python"
elif [[ -x "$SCRIPT_DIR/.venv/bin/dbt" ]]; then
  VENV_DBT="$SCRIPT_DIR/.venv/bin/dbt"
  VENV_PY="$SCRIPT_DIR/.venv/bin/python"
else
  echo "No dbt virtualenv found at dbt/.venv or .venv." >&2
  exit 1
fi

echo "==> Step 1: rebuild analytics_raw_dev = prod raw seeds + local syncs"
"$VENV_PY" "$SCRIPT_DIR/scripts/dev_refresh_raw.py"

echo "==> Step 2: backfill accumulating snapshot history from prod"
# The dbt snapshots (account balances + option MVs) accrue one row per day the
# build runs. Prod runs daily and has continuous history; analytics_dev would
# otherwise only have the days dev-refresh happened to run — making Daily
# Review deltas / the Δ heatmap look nothing like prod. Seed dev from prod's
# accumulated history; the dbt snapshot step below then MERGEs today on top.
BQ_DATASET=analytics_dev "$VENV_PY" "$SCRIPT_DIR/scripts/dev_backfill_snapshots.py"

echo "==> Step 3: dbt build into analytics_dev (working tree code, in-repo profiles.dev.yml)"
# The repo dbt/profiles.yml targets PROD analytics (CI / intentional prod
# builds). A hand-maintained ~/.dbt/profiles.yml is easy to skip or point
# at the wrong dataset. Copy the in-repo dev profile into a temp dir so
# `dbt build` cannot silently fall through to prod.
PROFILES_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ht-dbt-dev-XXXXXX")"
trap 'rm -rf "$PROFILES_DIR"' EXIT
cp "$SCRIPT_DIR/dbt/profiles.dev.yml" "$PROFILES_DIR/profiles.yml"
cd "$SCRIPT_DIR/dbt"
DBT_RAW_DATASET=analytics_raw_dev "$VENV_DBT" build --profiles-dir "$PROFILES_DIR" --target dev

echo "==> Done. analytics_dev = latest prod data + local dev syncs, built from local code."
