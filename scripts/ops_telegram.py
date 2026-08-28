"""Send a one-shot ops alert to Telegram.

Used by ``.github/workflows/ops_alert.yml`` when a warehouse / prices /
reconcile job fails. No-op (exit 0) when ``TELEGRAM_BOT_TOKEN`` or
``TELEGRAM_CHAT_ID`` is unset so a missing bot cannot fail CI.

Setup is in the workflow file header (BotFather → secrets → test dispatch).
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def compose_hotfix_started(
    *,
    name: str,
    agent_url: str,
    url: str = "",
) -> str:
    lines = [
        "HappyTrader: hotfix started",
        name or "ops job",
        (agent_url or "").strip(),
    ]
    run = (url or "").strip()
    if run:
        lines.append(run)
    return "\n".join(line for line in lines if line)


def compose_hotfix_merged(
    *,
    name: str,
    pr_url: str,
    branch: str = "",
) -> str:
    lines = [
        "HappyTrader: hotfix merged",
        name or "ops job",
    ]
    b = (branch or "").strip()
    if b:
        lines.append(f"branch: {b}")
    lines.append((pr_url or "").strip())
    return "\n".join(line for line in lines if line)


def is_cursor_hotfix_branch(ref: str) -> bool:
    """Cursor cloud agents push to ``cursor/...`` branches."""
    b = (ref or "").strip()
    if b.startswith("refs/heads/"):
        b = b[len("refs/heads/") :]
    return b.startswith("cursor/")


def compose_message(
    *,
    name: str,
    conclusion: str,
    event: str,
    branch: str,
    url: str,
    agent_url: str = "",
    kind: str = "",
) -> str:
    k = (kind or "").strip().lower()
    if k == "started":
        return compose_hotfix_started(name=name, agent_url=agent_url, url=url)
    if k == "merged":
        return compose_hotfix_merged(name=name, pr_url=url, branch=branch)
    if (conclusion or "").strip().lower() == "test":
        return "HappyTrader ops alert test ping. Telegram is wired."
    status = (conclusion or "failed").strip().lower()
    verb = "FAILED" if status in ("failure", "failed") else status.upper()
    lines = [
        f"HappyTrader: {name} {verb}",
        f"event: {event or '?'}",
        f"branch: {branch or '?'}",
        url or "",
    ]
    agent = (agent_url or "").strip()
    if agent:
        lines.append(f"Cursor agent: {agent}")
    return "\n".join(lines).rstrip()


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
