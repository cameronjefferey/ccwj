"""
Outbound email abstraction.

Two backends, picked by the EMAIL_BACKEND env var:

- ``log`` (default) — write the rendered email to the app logger. This is
  what the closed-beta runs on: the operator reads the password-reset
  link out of Render logs and pastes it into Slack/SMS for the tester.
  No SMTP credentials needed; no risk of leaking email addresses to a
  third-party provider while the auth flow is still being shaken down.

- ``smtp`` — stdlib ``smtplib`` over TLS. Configured by the env vars
  documented in ``send_password_reset_email`` below. We deliberately
  avoid pulling in a vendor SDK (SES, Postmark, SendGrid) so the binary
  surface stays small; swapping to a vendor later is a single function.

Callers should use the high-level helpers (``send_password_reset_email``)
rather than touching the backends directly so we have one place to add
templating, retry, or per-user opt-out later.
"""
from __future__ import annotations

import logging
import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import Optional

_log = logging.getLogger(__name__)


def _selected_backend() -> str:
    return (os.environ.get("EMAIL_BACKEND", "log") or "log").strip().lower()


def _from_address() -> str:
    """The 'From:' header. Falls back to a placeholder so tests don't
    need to set anything."""
    return os.environ.get("EMAIL_FROM", "HappyTrader <noreply@happytrader.me>").strip()


def _send_via_log(*, to: str, subject: str, body: str) -> None:
    """Backend that just logs the email. Used in dev / closed beta.

    We pick a clearly-tagged log line so the operator can grep
    ``EMAIL_OUTBOX`` to find the full message (and the reset link inside)
    without having to scroll past unrelated logs.
    """
    _log.warning(
        "EMAIL_OUTBOX backend=log to=%s subject=%r\n%s",
        to,
        subject,
        body,
    )


def _send_via_smtp(*, to: str, subject: str, body: str) -> None:
    """Stdlib SMTP backend.

    Env:
      EMAIL_SMTP_HOST     — required (e.g. smtp.postmarkapp.com)
      EMAIL_SMTP_PORT     — default 587
      EMAIL_SMTP_USER     — auth username
      EMAIL_SMTP_PASSWORD — auth password / API token
      EMAIL_FROM          — From: header
      EMAIL_SMTP_USE_SSL  — '1' for direct TLS (port 465); default starttls.
    """
    host = os.environ.get("EMAIL_SMTP_HOST", "").strip()
    if not host:
        _log.error(
            "EMAIL_BACKEND=smtp but EMAIL_SMTP_HOST is empty; falling back to log."
        )
        _send_via_log(to=to, subject=subject, body=body)
        return

    port = int(os.environ.get("EMAIL_SMTP_PORT", "587"))
    user = os.environ.get("EMAIL_SMTP_USER", "")
    password = os.environ.get("EMAIL_SMTP_PASSWORD", "")
    use_ssl = (os.environ.get("EMAIL_SMTP_USE_SSL", "0") or "0") in ("1", "true", "yes")

    msg = EmailMessage()
    msg["From"] = _from_address()
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)

    ctx = ssl.create_default_context()
    if use_ssl:
        with smtplib.SMTP_SSL(host, port, context=ctx, timeout=15) as srv:
            if user:
                srv.login(user, password)
            srv.send_message(msg)
    else:
        with smtplib.SMTP(host, port, timeout=15) as srv:
            srv.ehlo()
            srv.starttls(context=ctx)
            srv.ehlo()
            if user:
                srv.login(user, password)
            srv.send_message(msg)


def send_email(*, to: str, subject: str, body: str) -> None:
    """Dispatch to the configured backend. Never raises on send failure
    (caller cannot recover); errors are logged so we keep the request
    flow predictable for the user."""
    backend = _selected_backend()
    try:
        if backend == "smtp":
            _send_via_smtp(to=to, subject=subject, body=body)
        else:
            _send_via_log(to=to, subject=subject, body=body)
    except Exception as exc:  # pragma: no cover (defensive)
        _log.exception("Email send failed (backend=%s): %s", backend, exc)


# ---------------------------------------------------------------------------
# High-level helpers
# ---------------------------------------------------------------------------


def send_password_reset_email(
    *,
    to: str,
    username: str,
    reset_url: str,
    ttl_minutes: int = 60,
) -> None:
    """Render and send the standard password-reset email.

    The body is plain text on purpose: closed-beta inboxes are noisy,
    we don't need to fight HTML rendering across providers, and the
    raw URL is grep-able if a tester pastes the email at us during
    a bug report.
    """
    subject = "Reset your HappyTrader password"
    body = (
        f"Hi {username},\n\n"
        "Someone (hopefully you) asked to reset your HappyTrader password.\n"
        "Open the link below to choose a new one — it expires in "
        f"{ttl_minutes} minutes and only works once:\n\n"
        f"{reset_url}\n\n"
        "If you didn't ask for this, you can ignore this email. Your "
        "current password will keep working.\n\n"
        "— HappyTrader\n"
    )
    send_email(to=to, subject=subject, body=body)
