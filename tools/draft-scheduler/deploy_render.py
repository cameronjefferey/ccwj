#!/usr/bin/env python3
"""Create the free Render web service for Draft Night.

Needs RENDER_API_KEY (Account Settings → API Keys).
Optional: RENDER_OWNER_ID if the account has more than one workspace.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

API = "https://api.render.com/v1"
REPO = os.environ.get("DRAFT_RENDER_REPO", "https://github.com/cameronjefferey/ccwj")
BRANCH = os.environ.get("DRAFT_RENDER_BRANCH", "cursor/draft-scheduler-c770")
NAME = os.environ.get("DRAFT_RENDER_NAME", "draft-night")


def _req(method: str, path: str, token: str, body: dict | None = None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        API + path,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()
        raise SystemExit(f"Render API {exc.code} {path}: {detail}") from exc


def main() -> int:
    token = os.environ.get("RENDER_API_KEY", "").strip()
    if not token:
        print("Set RENDER_API_KEY (Render Dashboard → Account Settings → API Keys).", file=sys.stderr)
        return 2

    owner_id = os.environ.get("RENDER_OWNER_ID", "").strip()
    if not owner_id:
        _, owners = _req("GET", "/owners", token)
        rows = owners if isinstance(owners, list) else []
        if not rows:
            print("No Render workspaces on this API key.", file=sys.stderr)
            return 1
        if len(rows) > 1:
            print("Multiple workspaces; set RENDER_OWNER_ID to one of:", file=sys.stderr)
            for row in rows:
                owner = row.get("owner") or row
                print(f"  {owner.get('id')}  {owner.get('name') or owner.get('email')}", file=sys.stderr)
            return 2
        first = rows[0].get("owner") or rows[0]
        owner_id = first["id"]

    payload = {
        "type": "web_service",
        "name": NAME,
        "ownerId": owner_id,
        "repo": REPO,
        "branch": BRANCH,
        "rootDir": "tools/draft-scheduler",
        "autoDeploy": "yes",
        "envVars": [
            {"key": "DRAFT_TITLE", "value": "Draft Night"},
            {"key": "DRAFT_TIME", "value": "4:00 PM Pacific"},
            {"key": "DRAFT_START", "value": "2026-08-15"},
            {"key": "DRAFT_END", "value": "2026-09-08"},
            {"key": "DRAFT_KICKOFF", "value": "2026-09-09"},
            {"key": "PYTHON_VERSION", "value": "3.12.8"},
        ],
        "serviceDetails": {
            "runtime": "python",
            "plan": "free",
            "region": "oregon",
            "healthCheckPath": "/",
            "envSpecificDetails": {
                "buildCommand": "pip install -r requirements.txt",
                "startCommand": "gunicorn -b 0.0.0.0:$PORT app:app",
            },
        },
    }
    status, created = _req("POST", "/services", token, payload)
    service = (created or {}).get("service") or created or {}
    details = service.get("serviceDetails") or {}
    url = details.get("url") or service.get("dashboardUrl")
    print(json.dumps({
        "http": status,
        "id": service.get("id"),
        "name": service.get("name"),
        "url": url,
        "dashboardUrl": service.get("dashboardUrl"),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
