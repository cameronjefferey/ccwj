"""Send a one-shot ops alert to Telegram.

Used by ``.github/workflows/ops_alert.yml`` when a warehouse / prices /
reconcile job fails. No-op (exit 0) when ``TELEGRAM_BOT_TOKEN`` or
``TELEGRAM_CHAT_ID`` is unset so a missing bot cannot fail CI.

Setup is in the workflow file header (BotFather → secrets → test dispatch).
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

# Internal Actions names → the sentence a human would say.
_JOB_ALIASES = {
    "Update Daily Position Performance": "Warehouse rebuild",
    "Evening price refresh (snap equities to official close)": "Evening price refresh",
    "Warehouse reconcile audit": "Warehouse audit",
    "Manual hotfix": "That job",
    "Ops alert": "Ops",
}


def friendly_job_name(name: str) -> str:
    n = " ".join((name or "").split())
    return _JOB_ALIASES.get(n, n or "A job")


def _the_job(name: str) -> str:
    job = friendly_job_name(name)
    if not job:
        return "the job"
    return job[0].lower() + job[1:]


def _clean_pr_title(title: str) -> str:
    t = " ".join((title or "").split())
    t = re.sub(r"^\[cursor-hotfix\]\s*", "", t, flags=re.I)
    return t


def _labeled(label: str, url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    return f"{label}\n{u}"


def compose_hotfix_started(
    *,
    name: str,
    agent_url: str,
    url: str = "",
) -> str:
    # Failed-run URL lives on the failure ping; this one is just the agent.
    _ = url
    lines = [
        f"HappyTrader — a Cursor agent is working on the {_the_job(name)}.",
        _labeled("Watch it:", agent_url),
    ]
    return "\n\n".join(line for line in lines if line)


def compose_hotfix_merged(
    *,
    name: str,
    pr_url: str,
    branch: str = "",
) -> str:
    # cursor/hotfix-… branch names are not worth reading.
    _ = branch
    lines = ["HappyTrader — the hotfix is live."]
    title = _clean_pr_title(name)
    if title:
        lines.append(title)
    link = _labeled("See the change:", pr_url)
    if link:
        lines.append(link)
    return "\n\n".join(lines)


def is_cursor_hotfix_branch(ref: str) -> bool:
    """Cursor cloud agents push to ``cursor/...`` branches."""
    b = (ref or "").strip()
    if b.startswith("refs/heads/"):
        b = b[len("refs/heads/") :]
    return b.startswith("cursor/")


def compose_failure(
    *,
    name: str,
    url: str,
    agent_url: str = "",
    hotfix_skip: str = "",
) -> str:
    job = friendly_job_name(name)
    lines = [f"HappyTrader — {_the_job(name)} failed."]
    skip = (hotfix_skip or "").strip()
    if skip == "retries_exhausted":
        lines.append("A Cursor agent already tried twice. This needs a person.")
    elif skip == "hotfix_in_flight":
        lines.append("A Cursor agent is already working on it.")
    elif (agent_url or "").strip():
        lines.append("A Cursor agent is looking at it.")
    link = _labeled("Failed run:", url)
    if link:
        lines.append(link)
    return "\n\n".join(lines)


def compose_message(
    *,
    name: str,
    conclusion: str,
    event: str,
    branch: str,
    url: str,
    agent_url: str = "",
    kind: str = "",
    hotfix_skip: str = "",
) -> str:
    # event/branch stay in the signature (workflow env) but are not
    # dumped into the ping — workflow_dispatch / master is noise.
    _ = event
    k = (kind or "").strip().lower()
    if k == "started":
        return compose_hotfix_started(name=name, agent_url=agent_url, url=url)
    if k == "merged":
        return compose_hotfix_merged(name=name, pr_url=url, branch=branch)
    if (conclusion or "").strip().lower() == "test":
        return "HappyTrader — Telegram is wired. This was a test ping."
    status = (conclusion or "failed").strip().lower()
    if status not in ("failure", "failed"):
        return (
            f"HappyTrader — {friendly_job_name(name)} ended ({status}).\n\n"
            + (_labeled("Run:", url) or "")
        ).rstrip()
    return compose_failure(
        name=name,
        url=url,
        agent_url=agent_url,
        hotfix_skip=hotfix_skip,
    )


def send_telegram(text: str, *, token: str, chat_id: str, timeout: int = 20) -> None:
    payload = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        TELEGRAM_API.format(token=token),
        data=payload,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    data = json.loads(body) if body else {}
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API did not ok: {body[:300]}")


def main(argv: list[str] | None = None) -> int:
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        print("ops_telegram: skipped (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID unset)")
        return 0
    text = compose_message(
        name=os.environ.get("ALERT_NAME") or "ops job",
        conclusion=os.environ.get("ALERT_CONCLUSION") or "failure",
        event=os.environ.get("ALERT_EVENT") or "",
        branch=os.environ.get("ALERT_BRANCH") or "",
        url=os.environ.get("ALERT_URL") or "",
        agent_url=os.environ.get("ALERT_AGENT_URL") or "",
        kind=os.environ.get("ALERT_KIND") or "",
        hotfix_skip=os.environ.get("ALERT_HOTFIX_SKIP") or "",
    )
    try:
        send_telegram(text, token=token, chat_id=chat_id)
    except (urllib.error.URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
        print(f"ops_telegram: send failed: {exc}", file=sys.stderr)
        return 1
    print("ops_telegram: sent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
