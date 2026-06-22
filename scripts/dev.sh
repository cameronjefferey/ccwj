#!/usr/bin/env bash
# One command to develop locally against real prod data.
#
# The daily loop:
#
#   ./scripts/dev.sh                 # start the app (hot-reload) at :5000
#   ./scripts/dev.sh --refresh       # rebuild analytics_dev from prod, then start
#   ./scripts/dev.sh --link          # mirror your prod tenants locally, then start
#   ./scripts/dev.sh --refresh --link --no-run   # set up, don't start the server
#
# Most edits (templates, routes, Python) hot-reload instantly under
# FLASK_DEBUG — just save and refresh the browser. You only need --refresh
# when you change dbt models or want fresher prod data in analytics_dev.
# --link is a one-time step to "become" your prod self (see docs/LOCAL_DEV.md).
#
# Environment separation is enforced by .env (BQ_DATASET=analytics_dev,
# GITHUB_BRANCH=dev-seeds); this script refuses to start if .env is pointed
# at prod, so you can never accidentally run the local app against analytics.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

# shellcheck disable=SC1091
source "$SCRIPT_DIR/dbt/.venv/bin/activate"

DO_REFRESH=0
DO_LINK=0
DO_RUN=1
PORT="${PORT:-5000}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --refresh) DO_REFRESH=1 ;;
    --link)    DO_LINK=1 ;;
    --no-run)  DO_RUN=0 ;;
    --port)    shift; PORT="$1" ;;
    -h|--help)
      sed -n '2,20p' "$0"; exit 0 ;;
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
  : "${PROD_DATABASE_URL:=$(env_val PROD_DATABASE_URL)}"
  : "${DEV_PROD_USERNAME:=$(env_val DEV_PROD_USERNAME)}"
fi

# Refuse to run the LOCAL app against the PROD warehouse. analytics_dev is
# the dev mirror; an unset/`analytics` BQ_DATASET means prod (the June 2026
# cross-env incident). Bail loudly rather than silently reading prod.
if [[ "${BQ_DATASET:-analytics}" != "analytics_dev" ]]; then
  echo "REFUSING TO START: BQ_DATASET='${BQ_DATASET:-<unset>}' is not 'analytics_dev'." >&2
  echo "  Local dev must read the dev mirror. Set BQ_DATASET=analytics_dev in .env." >&2
  exit 1
fi

if [[ "$DO_LINK" == "1" ]]; then
  echo "==> Linking your prod broker tenants into local Postgres"
  if [[ -z "${PROD_DATABASE_URL:-}" ]]; then
    echo "  PROD_DATABASE_URL is not set. Add it to .env (or export it), e.g." >&2
    echo "    PROD_DATABASE_URL=postgresql://USER:PASS@HOST:PORT/DB" >&2
    echo "  Then re-run with --link. See docs/LOCAL_DEV.md." >&2
    exit 1
  fi
  : "${DEV_PROD_USERNAME:?Set DEV_PROD_USERNAME in .env to your prod username (the account to mirror)}"
  python "$SCRIPT_DIR/scripts/dev-link-prod-tenants.py" \
    --prod-username "$DEV_PROD_USERNAME" --create-local-user
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
