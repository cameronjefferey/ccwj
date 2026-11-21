"""Failure logs for the SnapTrade cron.

Render keeps the cron's stderr. These lines are the only record of a red
run, so they include the exception class, the category, and the traceback.
They must not include private identifiers, account numbers, tokens, user
secrets, balances, or position values.
"""
from __future__ import annotations

import re
import sys
import traceback

_SCRUBS = (
    (
        re.compile(
            r"(?i)\b(bearer|token|secret|password|api[_-]?key|"
            r"usersecret|consumer_key)\b\s*[:=]\s*\S+"
        ),
        r"\1=…",
    ),
    (re.compile(r"(?i)authorization:\s*\S+"), "authorization: …"),
    (re.compile(r"https?://\S+"), "[url]"),
    (re.compile(r"(?i)snaptrade:[A-Za-z0-9_-]+"), "snaptrade:…"),
    (
        re.compile(
            r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-"
            r"[0-9a-f]{12}\b",
            re.I,
        ),
        "…",
    ),
    (re.compile(r"[\w.+-]+@[\w.-]+\.\w+"), "…@…"),
    (re.compile(r"\$\s*[\d,]+(?:\.\d+)?"), "$…"),
    (
        re.compile(
            r"(?i)\b(user_id|account_id|snaptrade_account_id)\s*[=:]\s*\S+"
        ),
        r"\1=…",
    ),
    (re.compile(r"[•*]{2,}\d+"), "…"),
    # Account numbers and other long ids. Traceback line numbers are shorter.
    (re.compile(r"\b\d{6,}\b"), "…"),
)

_BROKER = re.compile(r"^[a-z0-9_-]{1,32}$")


def redact_sync_log(text) -> str:
    """Return ``text`` with secrets, account ids, and money removed."""
    out = "" if text is None else str(text)
    for pattern, repl in _SCRUBS:
        out = pattern.sub(repl, out)
    lines = []
    for line in out.splitlines():
        if line.count(",") >= 4:
            lines.append("[row omitted]")
        else:
            lines.append(line)
    return "\n".join(lines)


def sync_failure_category(error=None, exc=None) -> str:
    """A short reason bucket for one account or for the shared seed store.

    Categories: ``timeout``, ``rate_limit``, ``connection_broken``, ``auth``,
    ``seed_read``, ``seed_write``, ``seed_merge``, ``broker_read``,
    ``unexpected``.
    """
    blob = " ".join(
        part for part in (
            type(exc).__name__ if exc is not None else "",
            "" if error is None else str(error),
            "" if exc is None else str(exc),
        ) if part
    ).lower()
    if "seedfetch" in blob or "bigquery read" in blob:
        return "seed_read"
    if any(
        token in blob
        for token in (
            "seed_write", "seed write", "bigquery load", "arrow",
            "pyarrow", "failed to parse",
        )
    ):
        return "seed_write"
    if "timeout" in blob or "timed out" in blob:
        return "timeout"
    if "429" in blob or "rate limit" in blob or "too many requests" in blob:
        return "rate_limit"
    if "connection_broken" in blob or "disabled" in blob:
        return "connection_broken"
    if any(token in blob for token in ("session_expired", "unauthorized", "401", "403")):
        return "auth"
    if "seed_merge" in blob or "poison frame" in blob:
        return "seed_merge"
    if error and str(error).strip() not in ("", "unknown", "unexpected"):
        return "broker_read"
    return "unexpected"


def safe_broker_slug(value) -> str:
    """Return a brokerage slug that is safe to print, or ``""``."""
    slug = str(value or "").strip().lower()
    if _BROKER.fullmatch(slug):
        return slug
    return ""


def log_sync_failure(category, *, exc=None, detail=None, broker=None) -> None:
    """Print one diagnosable failure. The traceback is scrubbed first."""
    slug = safe_broker_slug(broker)
    broker_bit = f" broker={slug}" if slug else ""
    print(
        f"snaptrade sync failure category={category}{broker_bit}",
        file=sys.stderr,
    )
    if exc is not None:
        text = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
        print(redact_sync_log(text), file=sys.stderr)
    elif detail:
        print(redact_sync_log(detail)[:800], file=sys.stderr)
