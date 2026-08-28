"""Operator Telegram pings for early product events.

Warehouse-job failures stay in ``scripts/ops_telegram.py`` (GitHub Actions).
This module is the Flask-app twin: signup, subscribe, cancel, first data,
feedback, broken broker connections.

Never raises. No-op when ``TELEGRAM_BOT_TOKEN`` / ``TELEGRAM_CHAT_ID`` are
unset. Sends on a daemon thread so a slow Telegram hop cannot stall signup
or a Stripe webhook.

Early-stage default is noisy (every event below). Quiet later with
``OPS_NOTIFY_EVENTS`` on the Render web service:

  unset        → all default events
  none / off   → silence product pings (CI failure alerts are separate)
  subscribe,cancel  → only those kinds
"""
from __future__ import annotations

import json
import logging
import os
import threading
import urllib.error
import urllib.parse
import urllib.request

from app.utils import DEMO_USERNAME

_log = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

DEFAULT_EVENTS = frozenset({
    "signup",
    "subscribe",
    "subscribe_ai",
    "canceling",
    "cancel",
    "cancel_ai",
    "feedback",
    "first_data",
    "broken",
})

_QUIET = frozenset({"none", "off", "0", "false"})


def enabled_events():
    """Which product events currently ping. Empty set = quiet."""
    raw = os.environ.get("OPS_NOTIFY_EVENTS")
    if raw is None:
        return DEFAULT_EVENTS
    parts = {p.strip().lower() for p in raw.split(",") if p.strip()}
    if not parts or parts <= _QUIET:
        return frozenset()
    return frozenset(parts)


def event_enabled(kind: str) -> bool:
    return (kind or "").strip().lower() in enabled_events()


def user_label(user_id, username=None) -> str:
    """How we name a person in a ping — ``@alice``, else ``user 9``.

    DB lookup runs only when ``username is None`` (caller omitted it).
    An explicit empty string means "no handle" and must not pick up a
    leftover ``users.id`` from another test (CI: ``test_data_isolation``
    creates ``share_legit_*`` as id=9, then
    ``test_user_label_uses_passed_username`` expected ``user 9``).
    """
    name = (username or "").strip()
    if username is None and not name and user_id is not None:
        try:
            from app.db import fetch_one
            row = fetch_one("SELECT username FROM users WHERE id = %s", (user_id,))
            name = ((row or {}).get("username") or "").strip()
        except Exception:
            name = ""
    if name:
        return f"@{name}"
    if user_id is not None:
        return f"user {user_id}"
    return "someone"


def compose(kind: str, *, who: str, **extra) -> str:
    """Same voice as the warehouse ops pings: one sentence, then a labeled detail."""
    kind = (kind or "").strip().lower()
    who = (who or "someone").strip() or "someone"
    if kind == "signup":
        return f"HappyTrader — {who} signed up."
    if kind == "first_data":
        return (
            f"HappyTrader — {who} connected their first account. "
            "The trial clock started."
        )
    if kind == "subscribe":
        if extra.get("resumed"):
            return (
                f"HappyTrader — {who} kept Pro. "
                "The pending cancel was lifted."
            )
        interval = (extra.get("interval") or "").strip()
        if interval and interval.lower() != "pro":
            line = f"HappyTrader — {who} subscribed to Pro ({interval})."
        else:
            line = f"HappyTrader — {who} subscribed to Pro."
        was = (extra.get("was_plan") or "").strip()
        if was and was not in ("active", "unknown"):
            line += f" They were on {was}."
        return line
    if kind == "canceling":
        return (
            f"HappyTrader — {who} canceled Pro. "
            "It stays on until the period ends."
        )
    if kind == "cancel":
        status = (extra.get("status") or "canceled").strip() or "canceled"
        return f"HappyTrader — {who}'s Pro ended ({status})."
    if kind == "subscribe_ai":
        return f"HappyTrader — {who} added HappyTrader AI."
    if kind == "cancel_ai":
        status = (extra.get("status") or "canceled").strip() or "canceled"
        return f"HappyTrader — {who}'s HappyTrader AI ended ({status})."
    if kind == "feedback":
        snippet = " ".join(str(extra.get("snippet") or "").split())
        lines = [f"HappyTrader — {who} sent feedback."]
        if snippet:
            lines.append(snippet)
        return "\n\n".join(lines)
    if kind == "broken":
        lines = [f"HappyTrader — {who}'s broker connection broke."]
        acct = (extra.get("account_id") or "").strip()
        if acct:
            lines.append(f"Account:\n{acct}")
        return "\n\n".join(lines)
    return f"HappyTrader — {who}: {kind}."


def notify_event(kind: str, *, user_id=None, username=None, **extra) -> bool:
    """Compose the shared-voice sentence and queue it."""
    who = extra.pop("who", None) or user_label(user_id, username=username)
    text = compose(kind, who=who, **extra)
    return notify(kind, text, username=username)


def _configured():
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    return token, chat_id


def _deliver(text: str, *, token: str, chat_id: str, timeout: int = 12) -> None:
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


def notify(kind: str, text: str, *, username=None) -> bool:
    """Queue a Telegram ping. Returns True when a send was attempted."""
    try:
        kind = (kind or "").strip().lower()
        if not event_enabled(kind):
            return False
        uname = (username or "").strip().lower()
        # Demo signups/syncs are noise; demo feedback is not — testers use that seat.
        if uname == DEMO_USERNAME and kind != "feedback":
            return False
        # Pytest must never fire a real Telegram ping just because .env has
        # the bot token. Tests that want a send set OPS_NOTIFY_SYNC=1 and
        # mock urllib.
        if os.environ.get("PYTEST_CURRENT_TEST") and not (
            os.environ.get("OPS_NOTIFY_SYNC") or ""
        ).strip():
            return False
        token, chat_id = _configured()
        if not token or not chat_id:
            return False
        message = (text or "").strip()
        if not message:
            return False
        sync = (os.environ.get("OPS_NOTIFY_SYNC") or "").strip() in ("1", "true", "yes")
        if sync:
            _deliver(message, token=token, chat_id=chat_id)
        else:
            threading.Thread(
                target=_deliver_safe,
                args=(message, token, chat_id),
                daemon=True,
                name="ops-notify",
            ).start()
        return True
    except Exception as exc:
        _log.debug("ops notify skipped: %s", exc)
        return False


def _deliver_safe(text: str, token: str, chat_id: str) -> None:
    try:
        _deliver(text, token=token, chat_id=chat_id)
    except (urllib.error.URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
        _log.warning("ops notify send failed: %s", exc)
    except Exception as exc:
        _log.warning("ops notify send failed: %s", exc)
