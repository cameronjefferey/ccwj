"""
Outbound email abstraction.

Three backends, picked by the EMAIL_BACKEND env var:

- ``log`` (default) — write the rendered email to the app logger. This is
  what the closed-beta runs on: the operator reads the password-reset
  link out of Render logs and pastes it into Slack/SMS for the tester.
  No credentials needed; no risk of leaking email addresses to a
  third-party provider while the auth flow is still being shaken down.

- ``smtp`` — stdlib ``smtplib`` over TLS. Configured by the ``EMAIL_SMTP_*``
  env vars. Works with any provider that exposes SMTP (Resend, Postmark,
  SES, …) and is the zero-code way to ship transactional mail: point it at
  the vendor's SMTP host and password reset goes out for real.

- ``resend`` — Resend HTTP API (https://resend.com) via stdlib ``urllib``.
  No vendor SDK — we keep the binary surface small. Needed for the richer
  product-marketing sends (HTML, ``List-Unsubscribe`` headers); set
  ``RESEND_API_KEY`` and ``EMAIL_FROM`` (a verified domain).

Callers should use the high-level helpers (``send_password_reset_email``,
``send_connection_dropped_email``, ``send_weekly_summary_email``, …) rather
than touching the backends directly so we have one place to add templating,
retry, suppression, or per-user opt-out.

Transactional vs lifecycle:
  Transactional mail (password reset, connection dropped) always sends —
  there is no opt-out. Lifecycle / product-marketing mail (weekly summary,
  weekly preview, re-engagement) carries a ``List-Unsubscribe`` header and
  an in-body unsubscribe link, and the *caller* is responsible for checking
  the recipient's opt-in flag before calling these helpers.
"""
from __future__ import annotations

import json
import logging
import os
import smtplib
import ssl
import urllib.error
import urllib.request
from email.message import EmailMessage
from typing import Mapping, Optional

_log = logging.getLogger(__name__)


def _selected_backend() -> str:
    return (os.environ.get("EMAIL_BACKEND", "log") or "log").strip().lower()


def _from_address() -> str:
    """The 'From:' header. Falls back to a placeholder so tests don't
    need to set anything."""
    return os.environ.get("EMAIL_FROM", "HappyTrader <noreply@happytrader.me>").strip()


def app_base_url() -> str:
    """Absolute origin for building links in emails sent outside a request
    context (cron digests, the sync CLI). Inside a request the caller can
    pass an ``url_for(..., _external=True)`` link instead.

    Override with ``APP_BASE_URL`` (e.g. http://localhost:5000 in dev).
    """
    return (os.environ.get("APP_BASE_URL", "https://happytrader.me") or "").strip().rstrip("/")


def _send_via_log(
    *, to: str, subject: str, body: str, html_body: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
) -> None:
    """Backend that just logs the email. Used in dev / closed beta.

    We pick a clearly-tagged log line so the operator can grep
    ``EMAIL_OUTBOX`` to find the full message (and any link inside)
    without having to scroll past unrelated logs.
    """
    _log.warning(
        "EMAIL_OUTBOX backend=log to=%s subject=%r headers=%s\n%s",
        to,
        subject,
        dict(headers) if headers else {},
        body,
    )


def _send_via_smtp(
    *, to: str, subject: str, body: str, html_body: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
) -> None:
    """Stdlib SMTP backend.

    Env:
      EMAIL_SMTP_HOST     — required (e.g. smtp.resend.com)
      EMAIL_SMTP_PORT     — default 587
      EMAIL_SMTP_USER     — auth username (Resend: the literal 'resend')
      EMAIL_SMTP_PASSWORD — auth password / API token
      EMAIL_FROM          — From: header
      EMAIL_SMTP_USE_SSL  — '1' for direct TLS (port 465); default starttls.
    """
    host = os.environ.get("EMAIL_SMTP_HOST", "").strip()
    if not host:
        _log.error(
            "EMAIL_BACKEND=smtp but EMAIL_SMTP_HOST is empty; falling back to log."
        )
        _send_via_log(to=to, subject=subject, body=body, html_body=html_body, headers=headers)
        return

    port = int(os.environ.get("EMAIL_SMTP_PORT", "587"))
    user = os.environ.get("EMAIL_SMTP_USER", "")
    password = os.environ.get("EMAIL_SMTP_PASSWORD", "")
    use_ssl = (os.environ.get("EMAIL_SMTP_USE_SSL", "0") or "0") in ("1", "true", "yes")

    msg = EmailMessage()
    msg["From"] = _from_address()
    msg["To"] = to
    msg["Subject"] = subject
    for key, val in (headers or {}).items():
        msg[key] = val
    msg.set_content(body)
    if html_body:
        msg.add_alternative(html_body, subtype="html")

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


def _send_via_resend(
    *, to: str, subject: str, body: str, html_body: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
) -> None:
    """Resend HTTP API backend (stdlib urllib — no vendor SDK).

    Env:
      RESEND_API_KEY — required (re_...). Get one at https://resend.com.
      EMAIL_FROM     — must be on a domain verified in Resend.
    """
    api_key = os.environ.get("RESEND_API_KEY", "").strip()
    if not api_key:
        _log.error(
            "EMAIL_BACKEND=resend but RESEND_API_KEY is empty; falling back to log."
        )
        _send_via_log(to=to, subject=subject, body=body, html_body=html_body, headers=headers)
        return

    payload: dict = {
        "from": _from_address(),
        "to": [to],
        "subject": subject,
        "text": body,
    }
    if html_body:
        payload["html"] = html_body
    if headers:
        payload["headers"] = dict(headers)

    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            # api.resend.com sits behind Cloudflare, which 403s (error 1010)
            # the default "Python-urllib/x" agent as a bot. Send a real UA.
            "User-Agent": "HappyTrader/1.0 (+https://happytrader.me)",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            status = getattr(resp, "status", 200)
            if status >= 300:
                raise RuntimeError(f"Resend API returned HTTP {status}")
    except urllib.error.HTTPError as exc:
        # urlopen raises on 4xx/5xx; Resend puts the actionable reason
        # (e.g. "domain not verified", restricted/test key) in the JSON
        # body, which is lost unless we read it off the error object.
        try:
            detail = exc.read().decode("utf-8", "replace")
        except Exception:
            detail = ""
        raise RuntimeError(
            f"Resend API returned HTTP {exc.code}: {detail or exc.reason}"
        ) from exc


def _is_suppressed(to: str, category: str) -> bool:
    """Check the suppression list before sending. ``hard_bounce`` /
    ``invalid`` / ``manual`` block everything; ``complaint`` blocks only
    lifecycle mail (we may still send critical transactional). Fails open
    (sends) if the lookup errors, so a DB hiccup never silently drops a
    password reset."""
    try:
        from app.models import get_email_suppression

        reason = get_email_suppression(to)
        if not reason:
            return False
        if reason == "complaint" and category != "lifecycle":
            return False
        return True
    except Exception as exc:  # pragma: no cover (defensive)
        _log.warning("suppression check failed (sending anyway): %s", exc)
        return False


def send_email(
    *, to: str, subject: str, body: str, html_body: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
    category: str = "transactional",
) -> None:
    """Dispatch to the configured backend. Never raises on send failure
    (caller cannot recover); errors are logged so we keep the request
    flow predictable for the user.

    ``body`` is always the plain-text alternative. ``html_body`` is
    optional; when present, capable backends send a multipart message.
    ``headers`` lets callers add e.g. ``List-Unsubscribe``.
    ``category`` is 'transactional' (default) or 'lifecycle' — it controls
    how the suppression list is applied (complaints suppress lifecycle only).
    """
    if _is_suppressed(to, category):
        _log.info("Email suppressed (category=%s) to=%s subject=%r", category, to, subject)
        return

    backend = _selected_backend()
    try:
        if backend == "smtp":
            _send_via_smtp(to=to, subject=subject, body=body, html_body=html_body, headers=headers)
        elif backend == "resend":
            _send_via_resend(to=to, subject=subject, body=body, html_body=html_body, headers=headers)
        else:
            _send_via_log(to=to, subject=subject, body=body, html_body=html_body, headers=headers)
    except Exception as exc:  # pragma: no cover (defensive)
        _log.exception("Email send failed (backend=%s): %s", backend, exc)


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

_ACCENT = "#6d5dfc"


def _wrap_html(*, title: str, inner_html: str, unsubscribe_url: Optional[str] = None) -> str:
    """Minimal, inline-styled HTML shell. Email clients strip <style>
    blocks and external CSS, so everything is inlined. Kept deliberately
    plain — the goal is reliable rendering across Gmail / Outlook / Apple
    Mail, not a pixel-perfect newsletter."""
    foot = ""
    if unsubscribe_url:
        foot = (
            '<p style="margin:24px 0 0;font-size:12px;color:#9aa0a6;">'
            "You're receiving this because you opted into HappyTrader emails. "
            f'<a href="{unsubscribe_url}" style="color:#9aa0a6;">Unsubscribe</a>.'
            "</p>"
        )
    return (
        '<!DOCTYPE html><html><body style="margin:0;padding:0;'
        'background:#f4f5f7;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        'style="background:#f4f5f7;padding:24px 0;"><tr><td align="center">'
        '<table role="presentation" width="560" cellpadding="0" cellspacing="0" '
        'style="background:#ffffff;border-radius:12px;overflow:hidden;'
        'box-shadow:0 1px 3px rgba(0,0,0,0.08);">'
        f'<tr><td style="background:{_ACCENT};padding:18px 28px;">'
        '<span style="color:#fff;font-size:18px;font-weight:700;">HappyTrader</span></td></tr>'
        f'<tr><td style="padding:28px;">'
        f'<h1 style="margin:0 0 16px;font-size:20px;color:#1a1a2e;">{title}</h1>'
        f'{inner_html}{foot}'
        '</td></tr></table>'
        '<p style="margin:16px 0 0;font-size:11px;color:#b0b4ba;">'
        "HappyTrader — your trading mirror.</p>"
        '</td></tr></table></body></html>'
    )


def _unsubscribe_headers(unsubscribe_url: Optional[str]) -> Optional[dict]:
    if not unsubscribe_url:
        return None
    return {
        "List-Unsubscribe": f"<{unsubscribe_url}>",
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    }


def _money(val) -> str:
    try:
        n = float(val or 0)
    except (TypeError, ValueError):
        n = 0.0
    sign = "-" if n < 0 else ""
    return f"{sign}${abs(n):,.2f}"


# ---------------------------------------------------------------------------
# High-level helpers — transactional (always send, no opt-out)
# ---------------------------------------------------------------------------


def send_password_reset_email(
    *,
    to: str,
    username: str,
    reset_url: str,
    ttl_minutes: int = 60,
) -> None:
    """Render and send the standard password-reset email."""
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
    html_body = _wrap_html(
        title="Reset your password",
        inner_html=(
            f'<p style="color:#3c4043;font-size:15px;">Hi {username},</p>'
            '<p style="color:#3c4043;font-size:15px;">Someone (hopefully you) asked to '
            "reset your HappyTrader password. Use the button below to choose a new one "
            f"— it expires in {ttl_minutes} minutes and only works once.</p>"
            f'<p style="margin:24px 0;"><a href="{reset_url}" '
            f'style="background:{_ACCENT};color:#fff;text-decoration:none;padding:12px 22px;'
            'border-radius:8px;font-weight:600;display:inline-block;">Choose a new password</a></p>'
            '<p style="color:#9aa0a6;font-size:13px;">If the button doesn\'t work, paste this '
            f'link into your browser:<br><a href="{reset_url}" style="color:{_ACCENT};">{reset_url}</a></p>'
            '<p style="color:#9aa0a6;font-size:13px;">Didn\'t ask for this? Ignore this email '
            "— your current password keeps working.</p>"
        ),
    )
    send_email(to=to, subject=subject, body=body, html_body=html_body)


def send_connection_dropped_email(
    *,
    to: str,
    username: str,
    broker_label: str,
    account_label: str,
    reconnect_url: str,
) -> None:
    """Tell a user their brokerage connection dropped and needs reconnecting.

    This is the practical "your token expired, please renew" message:
    SnapTrade connections are detected as broken at sync time (the broker
    revoked the grant or the authorization expired), so we notify reactively
    rather than on a countdown. Transactional — always sent.
    """
    broker = broker_label or "your broker"
    acct = f" ({account_label})" if account_label else ""
    subject = f"Action needed: reconnect {broker} to keep your data fresh"
    body = (
        f"Hi {username},\n\n"
        f"Your {broker}{acct} connection dropped, so HappyTrader can no longer "
        "pull your latest trades and positions. This usually happens when the "
        "broker's authorization expires and needs to be renewed.\n\n"
        "Reconnect in a minute here:\n"
        f"{reconnect_url}\n\n"
        "Your historical data is safe — reconnecting just resumes the daily sync.\n\n"
        "— HappyTrader\n"
    )
    html_body = _wrap_html(
        title="Reconnect your brokerage",
        inner_html=(
            f'<p style="color:#3c4043;font-size:15px;">Hi {username},</p>'
            f'<p style="color:#3c4043;font-size:15px;">Your <strong>{broker}</strong>{acct} '
            "connection dropped, so we can no longer pull your latest trades and positions. "
            "This usually happens when the broker's authorization expires and needs renewing.</p>"
            f'<p style="margin:24px 0;"><a href="{reconnect_url}" '
            f'style="background:{_ACCENT};color:#fff;text-decoration:none;padding:12px 22px;'
            'border-radius:8px;font-weight:600;display:inline-block;">Reconnect now</a></p>'
            '<p style="color:#9aa0a6;font-size:13px;">Your historical data is safe — '
            "reconnecting just resumes the daily sync.</p>"
        ),
    )
    send_email(to=to, subject=subject, body=body, html_body=html_body)


def send_connection_reminder_email(
    *,
    to: str,
    username: str,
    broker_label: str,
    account_label: str,
    stale_days: int,
    reconnect_url: str,
) -> None:
    """Recurring "you're still disconnected" nudge with a day count.

    The one-time ``send_connection_dropped_email`` fires the moment a sync
    flips ``connection_broken_at`` (week 0). This is the WEEKLY follow-up for
    users who haven't reconnected yet — same reconnect ask, but it leads with
    how long the data has been frozen so the cost of inaction is concrete.
    Transactional (account-health) — always sent; no opt-out.
    """
    broker = broker_label or "your broker"
    acct = f" ({account_label})" if account_label else ""
    days = max(1, int(stale_days or 0))
    day_phrase = f"{days} day" + ("s" if days != 1 else "")
    subject = f"Still disconnected: reconnect {broker} ({day_phrase} of frozen data)"
    body = (
        f"Hi {username},\n\n"
        f"Your {broker}{acct} connection has been disconnected for {day_phrase}, "
        "so HappyTrader hasn't been able to pull new trades, positions, or "
        "balances that whole time — your dashboard is showing stale numbers.\n\n"
        "Reconnect in a minute here:\n"
        f"{reconnect_url}\n\n"
        "Your historical data is safe — reconnecting just resumes the daily sync.\n\n"
        "— HappyTrader\n"
    )
    html_body = _wrap_html(
        title="Still disconnected — reconnect to unfreeze your data",
        inner_html=(
            f'<p style="color:#3c4043;font-size:15px;">Hi {username},</p>'
            f'<p style="color:#3c4043;font-size:15px;">Your <strong>{broker}</strong>{acct} '
            f"connection has been disconnected for <strong>{day_phrase}</strong>, so we "
            "haven't pulled new trades, positions, or balances that whole time — your "
            "dashboard is showing stale numbers.</p>"
            f'<p style="margin:24px 0;"><a href="{reconnect_url}" '
            f'style="background:{_ACCENT};color:#fff;text-decoration:none;padding:12px 22px;'
            'border-radius:8px;font-weight:600;display:inline-block;">Reconnect now</a></p>'
            '<p style="color:#9aa0a6;font-size:13px;">Your historical data is safe — '
            "reconnecting just resumes the daily sync.</p>"
        ),
    )
    send_email(to=to, subject=subject, body=body, html_body=html_body)


def send_welcome_verify_email(
    *,
    to: str,
    username: str,
    verify_url: str,
) -> None:
    """Signup welcome that doubles as the email-verification ask. One email
    at signup (welcome + verify CTA) rather than two. Transactional."""
    subject = "Welcome to HappyTrader — confirm your email"
    body = (
        f"Hi {username},\n\n"
        "Welcome to HappyTrader — your trading mirror. Confirm your email so "
        "we can send you password resets and (if you opt in) your weekly recap:\n\n"
        f"{verify_url}\n\n"
        "Next, connect a brokerage so we can build your Daily Review.\n\n"
        "— HappyTrader\n"
    )
    html_body = _wrap_html(
        title="Welcome to HappyTrader",
        inner_html=(
            f'<p style="color:#3c4043;font-size:15px;">Hi {username}, welcome to '
            "HappyTrader — your trading mirror.</p>"
            '<p style="color:#3c4043;font-size:15px;">Confirm your email so we can send '
            "password resets and (if you opt in) your weekly recap.</p>"
            f'<p style="margin:24px 0;"><a href="{verify_url}" '
            f'style="background:{_ACCENT};color:#fff;text-decoration:none;padding:12px 22px;'
            'border-radius:8px;font-weight:600;display:inline-block;">Confirm my email</a></p>'
            '<p style="color:#9aa0a6;font-size:13px;">Then connect a brokerage and we\'ll '
            "build your Daily Review automatically.</p>"
            '<p style="color:#9aa0a6;font-size:13px;">If the button doesn\'t work, paste this '
            f'link into your browser:<br><a href="{verify_url}" style="color:{_ACCENT};">{verify_url}</a></p>'
        ),
    )
    send_email(to=to, subject=subject, body=body, html_body=html_body)


def send_data_ready_email(
    *,
    to: str,
    username: str,
    dashboard_url: str,
) -> None:
    """Fired once after a user's first successful broker sync — their data
    is now in the product. Transactional (one-time activation moment)."""
    subject = "Your HappyTrader data is ready"
    body = (
        f"Hi {username},\n\n"
        "Good news — we finished pulling your brokerage data. Your positions, "
        "trades, and P&L are now in your Daily Review.\n\n"
        f"Take a look: {dashboard_url}\n\n"
        "— HappyTrader\n"
    )
    html_body = _wrap_html(
        title="Your data is ready",
        inner_html=(
            f'<p style="color:#3c4043;font-size:15px;">Hi {username},</p>'
            '<p style="color:#3c4043;font-size:15px;">Good news — we finished pulling your '
            "brokerage data. Your positions, trades, and P&amp;L are now in your Daily Review.</p>"
            f'<p style="margin:24px 0;"><a href="{dashboard_url}" '
            f'style="background:{_ACCENT};color:#fff;text-decoration:none;padding:12px 22px;'
            'border-radius:8px;font-weight:600;display:inline-block;">Open your Daily Review</a></p>'
        ),
    )
    send_email(to=to, subject=subject, body=body, html_body=html_body)


# ---------------------------------------------------------------------------
# High-level helpers — lifecycle / product-marketing (opt-out)
# Callers MUST check the recipient's opt-in flag before calling these.
# ---------------------------------------------------------------------------


def send_weekly_summary_email(
    *,
    to: str,
    username: str,
    summary: Mapping,
    dashboard_url: str,
    unsubscribe_url: str,
) -> None:
    """Weekly recap of how the trader's week went.

    ``summary`` keys (all optional; missing → omitted from the email):
      week_label, total_return, total_pnl, dividends, trades_closed,
      num_winners, num_losers, best_symbol, best_pnl, worst_symbol, worst_pnl.
    """
    week = summary.get("week_label") or "this past week"
    subject = f"Your HappyTrader week: {week}"

    lines = [f"Hi {username},", "", f"Here's how your week went ({week}):", ""]
    lines.append(f"  Net return:   {_money(summary.get('total_return'))}")
    if summary.get("dividends"):
        lines.append(f"  Dividends:    {_money(summary.get('dividends'))}")
    if summary.get("trades_closed") is not None:
        wl = ""
        if summary.get("num_winners") is not None and summary.get("num_losers") is not None:
            wl = f" ({summary.get('num_winners')}W / {summary.get('num_losers')}L)"
        lines.append(f"  Trades closed: {summary.get('trades_closed')}{wl}")
    if summary.get("best_symbol"):
        lines.append(f"  Best trade:   {summary.get('best_symbol')} {_money(summary.get('best_pnl'))}")
    if summary.get("worst_symbol"):
        lines.append(f"  Worst trade:  {summary.get('worst_symbol')} {_money(summary.get('worst_pnl'))}")
    lines += ["", f"See the full breakdown: {dashboard_url}", "", "— HappyTrader", ""]
    body = "\n".join(lines)

    rows = [
        ("Net return", _money(summary.get("total_return"))),
    ]
    if summary.get("dividends"):
        rows.append(("Dividends", _money(summary.get("dividends"))))
    if summary.get("trades_closed") is not None:
        wl = ""
        if summary.get("num_winners") is not None and summary.get("num_losers") is not None:
            wl = f" ({summary.get('num_winners')}W / {summary.get('num_losers')}L)"
        rows.append(("Trades closed", f"{summary.get('trades_closed')}{wl}"))
    if summary.get("best_symbol"):
        rows.append(("Best trade", f"{summary.get('best_symbol')} {_money(summary.get('best_pnl'))}"))
    if summary.get("worst_symbol"):
        rows.append(("Worst trade", f"{summary.get('worst_symbol')} {_money(summary.get('worst_pnl'))}"))
    rows_html = "".join(
        f'<tr><td style="padding:6px 0;color:#9aa0a6;font-size:14px;">{label}</td>'
        f'<td style="padding:6px 0;color:#1a1a2e;font-size:14px;font-weight:600;text-align:right;">{val}</td></tr>'
        for label, val in rows
    )
    html_body = _wrap_html(
        title=f"Your week: {week}",
        inner_html=(
            f'<p style="color:#3c4043;font-size:15px;">Hi {username}, here\'s how your week went.</p>'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            f'style="margin:8px 0 20px;">{rows_html}</table>'
            f'<p style="margin:8px 0 0;"><a href="{dashboard_url}" '
            f'style="background:{_ACCENT};color:#fff;text-decoration:none;padding:12px 22px;'
            'border-radius:8px;font-weight:600;display:inline-block;">See the full breakdown</a></p>'
        ),
        unsubscribe_url=unsubscribe_url,
    )
    send_email(
        to=to, subject=subject, body=body, html_body=html_body,
        headers=_unsubscribe_headers(unsubscribe_url), category="lifecycle",
    )


def send_weekly_preview_email(
    *,
    to: str,
    username: str,
    preview: Mapping,
    dashboard_url: str,
    unsubscribe_url: str,
) -> None:
    """Look-ahead email: what's on the calendar for the coming weeks.

    ``preview`` keys (all optional, each a list of short strings):
      earnings, expirations, ex_dividends.
    """
    subject = "Your HappyTrader week ahead"
    earnings = list(preview.get("earnings") or [])
    expirations = list(preview.get("expirations") or [])
    ex_divs = list(preview.get("ex_dividends") or [])

    def _txt_block(title, items):
        if not items:
            return []
        return [f"{title}:"] + [f"  • {it}" for it in items] + [""]

    lines = [f"Hi {username},", "", "Here's what's on your radar:", ""]
    lines += _txt_block("Upcoming earnings (≤14d)", earnings)
    lines += _txt_block("Options expiring (≤14d)", expirations)
    lines += _txt_block("Projected ex-dividends (≤30d)", ex_divs)
    if not (earnings or expirations or ex_divs):
        lines += ["Nothing major on the calendar — a quiet stretch ahead.", ""]
    lines += [f"Open your Daily Review: {dashboard_url}", "", "— HappyTrader", ""]
    body = "\n".join(lines)

    def _html_block(title, items):
        if not items:
            return ""
        lis = "".join(f'<li style="margin:2px 0;color:#1a1a2e;font-size:14px;">{it}</li>' for it in items)
        return (
            f'<p style="margin:16px 0 4px;color:#9aa0a6;font-size:13px;font-weight:600;'
            f'text-transform:uppercase;letter-spacing:.04em;">{title}</p>'
            f'<ul style="margin:0;padding-left:18px;">{lis}</ul>'
        )

    blocks = (
        _html_block("Upcoming earnings (≤14d)", earnings)
        + _html_block("Options expiring (≤14d)", expirations)
        + _html_block("Projected ex-dividends (≤30d)", ex_divs)
    )
    if not blocks:
        blocks = '<p style="color:#3c4043;font-size:15px;">Nothing major on the calendar — a quiet stretch ahead.</p>'
    html_body = _wrap_html(
        title="Your week ahead",
        inner_html=(
            f'<p style="color:#3c4043;font-size:15px;">Hi {username}, here\'s what\'s on your radar.</p>'
            f"{blocks}"
            f'<p style="margin:20px 0 0;"><a href="{dashboard_url}" '
            f'style="background:{_ACCENT};color:#fff;text-decoration:none;padding:12px 22px;'
            'border-radius:8px;font-weight:600;display:inline-block;">Open your Daily Review</a></p>'
        ),
        unsubscribe_url=unsubscribe_url,
    )
    send_email(
        to=to, subject=subject, body=body, html_body=html_body,
        headers=_unsubscribe_headers(unsubscribe_url), category="lifecycle",
    )


def send_reengagement_email(
    *,
    to: str,
    username: str,
    days_away: int,
    dashboard_url: str,
    unsubscribe_url: str,
) -> None:
    """Gentle nudge for a user who hasn't logged in for a while."""
    subject = "Your trades are still moving — see what changed"
    body = (
        f"Hi {username},\n\n"
        f"It's been about {days_away} days since you last checked in. Your "
        "positions kept moving while you were away — opens, closes, dividends, "
        "and expirations are all waiting in your Daily Review.\n\n"
        f"Pick up where you left off: {dashboard_url}\n\n"
        "— HappyTrader\n"
    )
    html_body = _wrap_html(
        title="See what changed while you were away",
        inner_html=(
            f'<p style="color:#3c4043;font-size:15px;">Hi {username},</p>'
            f'<p style="color:#3c4043;font-size:15px;">It\'s been about <strong>{days_away} days</strong> '
            "since you last checked in. Your positions kept moving while you were away — opens, "
            "closes, dividends, and expirations are all waiting in your Daily Review.</p>"
            f'<p style="margin:24px 0;"><a href="{dashboard_url}" '
            f'style="background:{_ACCENT};color:#fff;text-decoration:none;padding:12px 22px;'
            'border-radius:8px;font-weight:600;display:inline-block;">Pick up where you left off</a></p>'
        ),
        unsubscribe_url=unsubscribe_url,
    )
    send_email(
        to=to, subject=subject, body=body, html_body=html_body,
        headers=_unsubscribe_headers(unsubscribe_url), category="lifecycle",
    )
