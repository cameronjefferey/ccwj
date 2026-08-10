#!/bin/bash
# preToolUse hook: require explicit user approval before the agent edits or
# deletes the repo's most dangerous files:
#
#   .env             — holds the dev/prod environment split (BQ_DATASET /
#                      BQ_RAW_DATASET). A wrong value here points local dev
#                      writes at the PRODUCTION warehouse (see the June 2026
#                      cross-environment purge incident in AGENTS.md).
#   scripts/admin/** — destructive one-shot operations (tenant purges,
#                      cutover resets). Should be edited deliberately,
#                      never as collateral in a broad refactor.
#
# Everything else is allowed through untouched. Fails open: a hook bug must
# never block normal work.

set -u

input=$(cat)

allow() { echo '{ "permission": "allow" }'; exit 0; }

tool=$(echo "$input" | jq -r '.tool_name // empty' 2>/dev/null)
case "$tool" in
  *rite*|*dit*|*trReplace*|*earchReplace*|*elete*|*otebook*) : ;;
  *) allow ;;
esac

path=$(echo "$input" | jq -r '
  .tool_input.file_path // .tool_input.path // .tool_input.target_notebook
  // .file_path // empty' 2>/dev/null)
[ -z "$path" ] && allow

case "$path" in
  *"/.env"|.env)
    jq -n '{
      permission: "ask",
      user_message: "The agent wants to modify .env — this file controls the dev/prod dataset split (BQ_DATASET / BQ_RAW_DATASET). Approve only if you expect an environment change.",
      agent_message: "A hook flagged this edit: .env controls the dev/prod environment separation (AGENTS.md). Confirm the change is intentional and does not point local dev at production datasets."
    }'
    exit 0
    ;;
  *"/scripts/admin/"*|scripts/admin/*)
    jq -n '{
      permission: "ask",
      user_message: "The agent wants to modify a script under scripts/admin/ (destructive one-shot operations). Review before approving.",
      agent_message: "A hook flagged this edit: scripts/admin/ holds destructive operations (tenant purges, cutover resets). Confirm the user asked for this change."
    }'
    exit 0
    ;;
esac

allow
