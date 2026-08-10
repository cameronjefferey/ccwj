#!/bin/bash
# postToolUse hook: after any file edit under dbt/, run `dbt parse` (offline,
# ~2-5s with partial parsing) and feed a failure back to the agent as
# additional_context so broken SQL/YAML is caught at edit time instead of in
# the prod warehouse build. Silent on success and on non-dbt edits.
#
# Fails open by design: a hook bug must never block normal work.

set -u

input=$(cat)

# Only act on file-editing tools.
tool=$(echo "$input" | jq -r '.tool_name // .hook_event_name // empty' 2>/dev/null)
case "$tool" in
  *rite*|*dit*|*trReplace*|*earchReplace*|*otebook*) : ;;
  *) exit 0 ;;
esac

# Extract the edited path from whichever field this tool populates.
path=$(echo "$input" | jq -r '
  .tool_input.file_path // .tool_input.path // .tool_input.target_notebook
  // .file_path // empty' 2>/dev/null)
[ -z "$path" ] && exit 0

# Only dbt model/config files (models, macros, tests, snapshots, sources).
case "$path" in
  *"/dbt/"*.sql|*"/dbt/"*.yml|*"/dbt/"*.yaml|dbt/*.sql|dbt/*.yml|dbt/*.yaml) : ;;
  *) exit 0 ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dbt_bin="$repo_root/.venv/bin/dbt"
[ -x "$dbt_bin" ] || exit 0

out=$(cd "$repo_root/dbt" && "$dbt_bin" parse 2>&1)
status=$?

if [ $status -ne 0 ]; then
  # Last ~25 lines carry the dbt error; hand them to the agent.
  tail_out=$(echo "$out" | tail -25)
  jq -n --arg ctx "dbt parse FAILED after editing $path — fix before moving on:

$tail_out" '{additional_context: $ctx}'
fi

exit 0
