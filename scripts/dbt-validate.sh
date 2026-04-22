#!/usr/bin/env bash
# Local validation before push: dbt parse (no BQ) + dbt build (seeds + models + snapshots).
# Requires: venv with dbt-bigquery, ~/.dbt/profiles.yml, GCP auth, network for build.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/dbt"
DBT="${ROOT}/.venv/bin/dbt"
if [[ ! -x "$DBT" ]]; then
  echo "Missing $DBT — create .venv and: pip install dbt-bigquery" >&2
  exit 1
fi
"$DBT" parse
"$DBT" build "$@"
