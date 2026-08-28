"""Launch a Cursor cloud agent to hotfix a failed ops workflow.

Used by ``.github/workflows/ops_alert.yml`` after a warehouse / prices /
reconcile job fails. POSTs to the Cloud Agents API so a cloud agent can
read the failed run, land a minimal fix, and re-run the job.

No-op (exit 0) when ``CURSOR_API_KEY`` is unset so a missing key cannot
fail the alert job. Setup is in the workflow file header.

Cursor Automations' "workflow run completed" trigger only fires for
*push*-started runs; production warehouse rebuilds are almost always
``workflow_dispatch`` (app seed write) or ``schedule``. This launcher is
the path that actually sees those failures.
"""
from __future__ import annotations

import base64
import json
import os
import sys
import uuid
import urllib.error
import urllib.request

CURSOR_AGENTS_API = "https://api.cursor.com/v1/agents"
CURSOR_AGENT_URL = "https://cursor.com/agents/{agent_id}"
DEFAULT_REPO_URL = "https://github.com/cameronjefferey/ccwj"
def agent_id_for_run(run_id: str) -> str:
    """Stable ``bc-<uuid>`` so a double-fire of the same run is a 409, not two agents."""
    raw = str(uuid.uuid5(uuid.NAMESPACE_URL, f"ccwj-ops-hotfix:{run_id}"))
    return f"bc-{raw}"


def compose_prompt(
    *,
    name: str,
    event: str,
    branch: str,
    url: str,
    sha: str,
    repo: str,
) -> str:
    run_url = (url or "").strip() or "(missing run URL)"
    return f"""You are the HappyTrader (ccwj) on-call fixer. A production GitHub Actions
job failed. Investigate, land a minimal hotfix, and get the job green again.

## Failed run
- Workflow: {name or "unknown"}
- Event: {event or "unknown"}
- Branch: {branch or "master"}
- SHA: {sha or "unknown"}
- Run URL: {run_url}
- Repo: {repo or "cameronjefferey/ccwj"}

## What to do
1. Fetch logs: `gh run view` on that URL (and `gh run view --log-failed`). Identify
   the failing job/step and the exact error. Do not guess from the workflow name.
2. Root-cause it: code/model regression vs config vs infra flake vs data invariant
   vs missing secret. Read the code the log points at.
3. If it is a **code/model bug you can prove**, make the smallest safe fix:
   - Open a PR from a `cursor/...` branch (do NOT push straight to master).
   - Commit message AND PR title include `[cursor-hotfix]` and the failed run id.
   - Merge the PR once you are confident (squash is fine). Never force-push.
   - Re-run the failed workflow on master:
     * Warehouse (`Update Daily Position Performance`): a push that touches
       `dbt/**`, `scripts/**`, `current_position_stock_price.py`, or the
       workflow file retriggers automatically. If the fix is only under
       `app/**` (paths-ignored), `gh workflow run "Update Daily Position
       Performance" --ref master` after merge.
     * Evening prices / reconcile: `gh workflow run` that workflow on master
       after merge (they are schedule/dispatch, not push).
   - Flask/Render: an `app/**` push to master deploys the web service. A
     dbt-only fix does NOT redeploy Render (build filter ignores `dbt/**`).
     Do not trigger a Render deploy for a warehouse-only fix.
4. If it is **infra, credentials, quota, yfinance, or a flake**, do not "fix"
   it with code. Comment on a PR or leave a short summary and stop.
5. If a previous `[cursor-hotfix]` for this SAME failure already merged and
   this is a repeat, STOP. Do not loop. Summarize why the first fix missed.

## Hard no's (HappyTrader invariants)
- Do NOT weaken, skip, or delete `scripts/snapshot_guard.py`, dbt uniqueness /
  isolation tests, or tenant-scoping. A red build is better than silent data loss.
- Duplicate-fill failures (`stg_history_no_duplicate_fills_per_tenant`) are a
  MERGE/DEDUP bug in `app/upload.py` (`_dedup_history_rows` /
  `_merge_seed_with_existing`) or a staging adapter, NOT a reason to drop the
  test. Fix the upstream grain. Never purge warehouse rows by numeric `user_id`.
- Do NOT touch SnapTrade refresh (`refresh_brokerage_authorization`), add a
  native broker OAuth path, or unscoped BigQuery reads.
- Do NOT invent data, fabricate columns, or "fix" by commenting out the
  failing step in the workflow.
- Cancelled overlapping warehouse rebuilds are expected — you should not be
  looking at a cancelled run.

## Done looks like
A merged PR (or a clear written reason you did not merge) plus the failed
workflow re-dispatched or naturally retriggered. Include the new run URL in
your final message.
"""


def compose_launch_body(
    *,
    name: str,
    event: str,
    branch: str,
    url: str,
    sha: str,
    repo: str,
    repo_url: str,
    run_id: str,
) -> dict:
    display = f"Hotfix: {name or 'ops job'}"
    if len(display) > 100:
        display = display[:97] + "..."
    starting_ref = (branch or "master").strip() or "master"
    return {
        "prompt": {
            "text": compose_prompt(
                name=name,
                event=event,
                branch=branch,
                url=url,
                sha=sha,
                repo=repo,
            )
        },
        "name": display,
        "agentId": agent_id_for_run(run_id),
        "repos": [
            {
                "url": repo_url,
                "startingRef": starting_ref,
            }
        ],
        "autoCreatePR": True,
        "skipReviewerRequest": True,
        "workOnCurrentBranch": False,
    }


def should_skip_loop(*, branch: str, sha: str, skip_sha: str) -> str | None:
    """Return a skip reason if this failure is already a cursor-hotfix loop."""
    b = (branch or "").strip()
    if b.startswith("cursor/"):
        return f"head branch {b} is already a cursor agent branch"
    if skip_sha and sha and skip_sha.strip() == sha.strip():
        return "this SHA was already auto-hotfixed (loop guard)"
    return None


def _basic_auth_header(api_key: str) -> str:
    token = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def launch_agent(body: dict, *, api_key: str, timeout: int = 30) -> dict:
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        CURSOR_AGENTS_API,
        data=payload,
        method="POST",
        headers={
            "Authorization": _basic_auth_header(api_key),
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw) if raw else {}
            return {"status": resp.status, "data": data}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        try:
            data = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            data = {"raw": raw[:500]}
        return {"status": exc.code, "data": data}


def agent_url_from_response(result: dict, fallback_id: str) -> str:
    data = result.get("data") or {}
    agent = data.get("agent") if isinstance(data, dict) else None
    if isinstance(agent, dict):
        if agent.get("url"):
            return str(agent["url"])
        if agent.get("id"):
            return CURSOR_AGENT_URL.format(agent_id=agent["id"])
    return CURSOR_AGENT_URL.format(agent_id=fallback_id)


def _write_github_output(key: str, value: str) -> None:
    path = (os.environ.get("GITHUB_OUTPUT") or "").strip()
    if not path:
        return
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(f"{key}={value}\n")


def repo_url_from_env() -> str:
    server = (os.environ.get("GITHUB_SERVER_URL") or "https://github.com").rstrip("/")
    repo = (os.environ.get("GITHUB_REPOSITORY") or "").strip()
    if repo:
        return f"{server}/{repo}"
    return DEFAULT_REPO_URL


def main(argv: list[str] | None = None) -> int:
    api_key = (os.environ.get("CURSOR_API_KEY") or "").strip()
    if not api_key:
        print("ops_cursor_hotfix: skipped (CURSOR_API_KEY unset)")
        return 0

    name = os.environ.get("ALERT_NAME") or "ops job"
    event = os.environ.get("ALERT_EVENT") or ""
    branch = os.environ.get("ALERT_BRANCH") or "master"
    url = os.environ.get("ALERT_URL") or ""
    sha = os.environ.get("ALERT_SHA") or ""
    run_id = os.environ.get("ALERT_RUN_ID") or url or "unknown"
    repo = os.environ.get("GITHUB_REPOSITORY") or "cameronjefferey/ccwj"

    skip = should_skip_loop(
        branch=branch,
        sha=sha,
        skip_sha=os.environ.get("HOTFIX_SKIP_SHA") or "",
    )
    if skip:
        print(f"ops_cursor_hotfix: skipped ({skip})")
        return 0

    body = compose_launch_body(
        name=name,
        event=event,
        branch=branch,
        url=url,
        sha=sha,
        repo=repo,
        repo_url=repo_url_from_env(),
        run_id=str(run_id),
    )
    fallback_id = body["agentId"]
    try:
        result = launch_agent(body, api_key=api_key)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"ops_cursor_hotfix: launch failed: {exc}", file=sys.stderr)
        return 1

    status = int(result.get("status") or 0)
    agent_url = agent_url_from_response(result, fallback_id)
    if status in (200, 201):
        print(f"ops_cursor_hotfix: launched {agent_url}")
        _write_github_output("agent_url", agent_url)
        return 0
    if status == 409:
        print(f"ops_cursor_hotfix: already launched {agent_url}")
        _write_github_output("agent_url", agent_url)
        return 0

    print(
        f"ops_cursor_hotfix: Cursor API {status}: {json.dumps(result.get('data'))[:400]}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
