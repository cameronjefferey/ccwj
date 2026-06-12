#!/usr/bin/env bash
# Dev-environment refresh — the local counterpart of refresh.sh.
#
# Local dev is environment-separated from production:
#   - seeds:    `dev-seeds` branch   (web app syncs there via GITHUB_BRANCH)
#   - warehouse: `analytics_dev`     (Flask reads there via BQ_DATASET)
# Production stays on master + `analytics`, built by GitHub Actions only.
#
# analytics_dev is a FULL MIRROR for local testing: latest prod seeds
# from origin/master PLUS the local environment's own syncs from
# origin/dev-seeds (merged by scripts/merge_dev_seeds.py — dev-seeds rows
# win for tenants registered in the local Postgres). Everything is
# testable locally with real data; local writes still never touch prod.
#
# This script builds into analytics_dev only. It NEVER touches the prod
# dataset: refresh.sh / plain `dbt build` from dbt/ use the repo's
# dbt/profiles.yml (dataset: analytics) — that path is for intentional
# prod builds only.
#
# NOTE: dbt prefers a profiles.yml in the project directory over ~/.dbt,
# so the --profiles-dir flag below is REQUIRED — without it the build
# targets prod `analytics` (this exact mistake wiped prod once during
# setup; don't remove the flag).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DBT="$SCRIPT_DIR/dbt/.venv/bin/dbt"
VENV_PY="$SCRIPT_DIR/dbt/.venv/bin/python"
WORKTREE=$(mktemp -d /tmp/ccwj-dev-refresh.XXXXXX)

cleanup() {
  cd "$SCRIPT_DIR"
  git worktree remove --force "$WORKTREE" 2>/dev/null || rm -rf "$WORKTREE"
}
trap cleanup EXIT

echo "==> Step 1: fetch latest master (prod seeds) + dev-seeds (local syncs)"
cd "$SCRIPT_DIR"
git fetch origin master dev-seeds

echo "==> Step 2: materialize dev-seeds in a worktree"
git worktree add --detach "$WORKTREE" origin/dev-seeds

echo "==> Step 3: overlay LOCAL dbt code (test your working tree, not the branch)"
# The point of the dev environment is testing local changes against real
# data BEFORE they ship — so the build must use the working tree's models/
# snapshots/macros, not whatever code dev-seeds branched from. Only the
# tenant-keyed seed CSVs stay from dev-seeds (merged with prod next step).
rsync -a \
  --exclude 'seeds/trade_history.csv' \
  --exclude 'seeds/current_positions.csv' \
  --exclude 'seeds/account_balances.csv' \
  --exclude 'target' --exclude '.venv' --exclude 'logs' --exclude 'dbt_packages' \
  "$SCRIPT_DIR/dbt/" "$WORKTREE/dbt/"
# The repo-level profiles.yml would override ~/.dbt and silently target
# prod `analytics` — remove it from the build dir entirely.
rm -f "$WORKTREE/dbt/profiles.yml"

echo "==> Step 4: merge prod seeds into the worktree (full local mirror)"
"$VENV_PY" "$SCRIPT_DIR/scripts/merge_dev_seeds.py" "$SCRIPT_DIR" "$WORKTREE/dbt/seeds"

echo "==> Step 5: dbt build into analytics_dev (profiles from ~/.dbt)"
cd "$WORKTREE/dbt"
"$VENV_DBT" build --profiles-dir "$HOME/.dbt" --target dev

echo "==> Done. analytics_dev = latest prod data + local dev syncs, built from local code."
