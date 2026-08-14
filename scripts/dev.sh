#!/usr/bin/env bash
# One command to develop locally against real prod data.
#
# The daily loop:
#
#   ./scripts/dev.sh                 # start the app (hot-reload) at :5000
#   ./scripts/dev.sh --sync          # clone latest prod marts into analytics_dev, then start
#   ./scripts/dev.sh --link          # mirror your prod tenants locally, then start
#   ./scripts/dev.sh --refresh       # rebuild analytics_dev from YOUR dbt models (slow)
#   ./scripts/dev.sh --sync --link --no-run   # set up, don't start the server
#
# Most edits (templates, routes, Python) hot-reload instantly under
# FLASK_DEBUG — just save and refresh the browser.
#
# Data freshness:
#   --sync     copies prod `analytics` → `analytics_dev` (BigQuery COPY).
#              Fast. This is what you want when the local UI looks empty or
#              stale. CI also runs this after every prod warehouse build, so
#              analytics_dev is usually already within one build of prod.
#   --refresh  merges prod raw + local syncs, then `dbt build` into
#              analytics_dev using this working tree. Use when you changed
#              dbt models and need to see YOUR SQL against real data.
#   --link     one-time step to "become" your prod self (see docs/LOCAL_DEV.md).
#
# Environment separation is enforced by .env (BQ_DATASET=analytics_dev,
# BQ_RAW_DATASET=analytics_raw_dev); this script refuses to start if .env is
# pointed at prod, so you can never accidentally run the local app against
# the prod warehouse or write local syncs into the prod raw seed tables.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

# Prefer dbt/.venv (historical), then the repo-root .venv. Either is a
# full app install; failing closed beats `python: command not found`.
if [[ -f "$SCRIPT_DIR/dbt/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/dbt/.venv/bin/activate"
elif [[ -f "$SCRIPT_DIR/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/.venv/bin/activate"
else
  echo "No virtualenv found at dbt/.venv or .venv. Run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

DO_SYNC=0
DO_REFRESH=0
DO_LINK=0
DO_LIST=0
DO_RUN=1
PORT="${PORT:-5000}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sync)    DO_SYNC=1 ;;
    --refresh) DO_REFRESH=1 ;;
    --link)    DO_LINK=1 ;;
    --list-tenants) DO_LIST=1 ;;
    --no-run)  DO_RUN=0 ;;
    --port)    shift; PORT="$1" ;;
    -h|--help)
      sed -n '2,28p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
  shift
done

# Pull only the vars this script reasons about out of .env (don't `source`
# the whole file — values like `EMAIL_FROM=HappyTrader <a@b>` aren't shell
# safe). flask run / the link script load the full .env themselves via
# python-dotenv, so the app process env is fully covered regardless.
env_val() {
  # last assignment wins; strip surrounding quotes if present
  local v
  v="$(grep -E "^$1=" "$SCRIPT_DIR/.env" 2>/dev/null | tail -1 | cut -d= -f2-)"
  v="${v%\"}"; v="${v#\"}"; v="${v%\'}"; v="${v#\'}"
  printf '%s' "$v"
}
if [[ -f "$SCRIPT_DIR/.env" ]]; then
  : "${BQ_DATASET:=$(env_val BQ_DATASET)}"
  : "${BQ_RAW_DATASET:=$(env_val BQ_RAW_DATASET)}"
  : "${PROD_DATABASE_URL:=$(env_val PROD_DATABASE_URL)}"
  : "${DEV_PROD_USERNAME:=$(env_val DEV_PROD_USERNAME)}"
  : "${DEV_PROD_USER_ID:=$(env_val DEV_PROD_USER_ID)}"
fi

# Refuse to run the LOCAL app against the PROD warehouse. analytics_dev is
# the dev mirror; an unset/`analytics` BQ_DATASET means prod (the June 2026
# cross-env incident). Bail loudly rather than silently reading prod.
if [[ "${BQ_DATASET:-analytics}" != "analytics_dev" ]]; then
  echo "REFUSING TO START: BQ_DATASET='${BQ_DATASET:-<unset>}' is not 'analytics_dev'." >&2
  echo "  Local dev must read the dev mirror. Set BQ_DATASET=analytics_dev in .env." >&2
  exit 1
fi

# Same discipline for the raw seed store: an unset BQ_RAW_DATASET means the
# app's sync/upload writers would WRITE_TRUNCATE the PROD raw seed tables.
if [[ "${BQ_RAW_DATASET:-analytics_raw}" != "analytics_raw_dev" ]]; then
  echo "REFUSING TO START: BQ_RAW_DATASET='${BQ_RAW_DATASET:-<unset>}' is not 'analytics_raw_dev'." >&2
  echo "  Local seed writes must target the dev raw dataset. Set BQ_RAW_DATASET=analytics_raw_dev in .env." >&2
  exit 1
fi

if [[ "$DO_LIST" == "1" ]]; then
  echo "==> Listing prod user_id → tenants from the warehouse (read-only)"
  python "$SCRIPT_DIR/scripts/dev-link-prod-tenants.py" --list-warehouse
  if [[ "$DO_RUN" == "0" && "$DO_SYNC" == "0" && "$DO_REFRESH" == "0" && "$DO_LINK" == "0" ]]; then
    exit 0
  fi
fi

if [[ "$DO_LINK" == "1" ]]; then
  echo "==> Linking your prod broker tenants into local Postgres"
  LINK_ARGS=(--create-local-user)
  if [[ -n "${PROD_DATABASE_URL:-}" ]]; then
    : "${DEV_PROD_USERNAME:?Set DEV_PROD_USERNAME in .env to your prod username (the account to mirror)}"
    python "$SCRIPT_DIR/scripts/dev-link-prod-tenants.py" \
      --prod-username "$DEV_PROD_USERNAME" "${LINK_ARGS[@]}"
  elif [[ -n "${DEV_PROD_USER_ID:-}" ]]; then
    : "${DEV_PROD_USERNAME:?Set DEV_PROD_USERNAME in .env to the local username to attach tenants to}"
    python "$SCRIPT_DIR/scripts/dev-link-prod-tenants.py" \
      --from-warehouse --prod-user-id "$DEV_PROD_USER_ID" \
      --local-username "$DEV_PROD_USERNAME" "${LINK_ARGS[@]}"
  else
    echo "  Need either:" >&2
    echo "    PROD_DATABASE_URL + DEV_PROD_USERNAME   (read-only prod Postgres)" >&2
    echo "    DEV_PROD_USER_ID + DEV_PROD_USERNAME    (no password; warehouse path)" >&2
    echo "  Find your user_id:  ./scripts/dev.sh --list-tenants --no-run" >&2
    echo "  See docs/LOCAL_DEV.md." >&2
    exit 1
  fi
fi

if [[ "$DO_SYNC" == "1" ]]; then
  echo "==> Cloning prod analytics → analytics_dev (fast; not a dbt rebuild)"
  python "$SCRIPT_DIR/scripts/dev_clone_prod.py"
fi

if [[ "$DO_REFRESH" == "1" ]]; then
  echo "==> Rebuilding analytics_dev from prod seeds + local syncs (your dbt code)"
  "$SCRIPT_DIR/scripts/dev-refresh.sh"
fi

if [[ "$DO_RUN" == "1" ]]; then
  echo "==> Starting HappyTrader at http://localhost:${PORT}  (Ctrl-C to stop)"
  echo "    Edits to templates/routes/Python hot-reload automatically."
  FLASK_APP=app:app FLASK_DEBUG=1 python -m flask run --port "$PORT"
else
  echo "==> Setup complete (--no-run). Start later with: ./scripts/dev.sh"
fi
