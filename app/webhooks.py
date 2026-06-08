"""
Inbound webhooks.

Currently: Resend delivery events. Resend signs webhooks with Svix headers
(``svix-id``, ``svix-timestamp``, ``svix-signature``). We verify the
signature with stdlib HMAC so we don't pull in the ``svix`` SDK, then act on
bounce / complaint events by adding the recipient to the email suppression
list (``app.models.add_email_suppression``).

Set ``RESEND_WEBHOOK_SECRET`` (the ``whsec_...`` value Resend shows when you
create the webhook). If it's unset we skip verification and log a warning —
fine for local dev, never for prod.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import time

from flask import request

from app import app
from app.extensions import csrf, limiter
from app.models import add_email_suppression

_log = logging.getLogger(__name__)

# Reject events whose timestamp is too far from now (replay protection).
_WEBHOOK_TOLERANCE_SECONDS = 5 * 60


def _verify_svix_signature(secret: str, headers, body: bytes) -> bool:
    """Verify a Svix-style signature (the scheme Resend uses).

    signed_content = f"{svix_id}.{svix_timestamp}.{raw_body}"
    expected       = base64( HMAC_SHA256(secret_bytes, signed_content) )
    The ``svix-signature`` header is a space-separated list of
    ``v1,<base64sig>`` entries; a match against any one passes.
    """
    svix_id = headers.get("svix-id") or headers.get("webhook-id")
    svix_ts = headers.get("svix-timestamp") or headers.get("webhook-timestamp")
    svix_sig = headers.get("svix-signature") or headers.get("webhook-signature")
    if not (svix_id and svix_ts and svix_sig):
        return False

    # Replay protection: timestamp must be recent.
    try:
        if abs(time.time() - int(svix_ts)) > _WEBHOOK_TOLERANCE_SECONDS:
            return False
    except (TypeError, ValueError):
        return False

    # Secret is "whsec_<base64>"; the signing key is the decoded base64 part.
    raw_secret = secret.split("_", 1)[1] if secret.startswith("whsec_") else secret
    try:
        key = base64.b64decode(raw_secret)
    except Exception:
        key = raw_secret.encode()

    signed_content = b"%s.%s.%s" % (svix_id.encode(), svix_ts.encode(), body)
    expected = base64.b64encode(hmac.new(key, signed_content, hashlib.sha256).digest()).decode()

    for part in svix_sig.split():
        # entries look like "v1,<sig>"; compare the signature portion only.
        sig = part.split(",", 1)[1] if "," in part else part
        if hmac.compare_digest(sig, expected):
            return True
    return False


def _recipients(data: dict):
    """Resend puts recipients in ``data.to`` (string or list)."""
    to = data.get("to")
    if isinstance(to, str):
        return [to]
    if isinstance(to, list):
        return [t for t in to if t]
    return []


@app.route("/webhooks/resend", methods=["POST"])
@csrf.exempt
@limiter.limit("240 per minute")
def resend_webhook():
    body = request.get_data() or b""
    secret = (os.environ.get("RESEND_WEBHOOK_SECRET") or "").strip()

    if secret:
        if not _verify_svix_signature(secret, request.headers, body):
            _log.warning("resend_webhook: signature verification failed")
            return ("invalid signature", 401)
    else:
        _log.warning(
            "resend_webhook: RESEND_WEBHOOK_SECRET unset — skipping signature "
            "verification (acceptable in dev only)."
        )

    try:
        event = json.loads(body.decode() or "{}")
    except Exception:
        return ("bad payload", 400)

    etype = (event.get("type") or "").strip().lower()
    data = event.get("data") or {}

    if etype == "email.bounced":
        # Resend bounce events are deliverability failures. Treat as a hard
        # block unless the payload clearly marks it transient/soft.
        bounce = data.get("bounce") or {}
        subtype = (bounce.get("type") or bounce.get("subType") or "").lower()
        reason = "hard_bounce" if "transient" not in subtype and "soft" not in subtype else None
        if reason:
            for addr in _recipients(data):
                add_email_suppression(addr, reason, detail=json.dumps(bounce)[:500])
            _log.info("resend_webhook: suppressed %d bounced recipient(s)", len(_recipients(data)))
    elif etype == "email.complained":
        for addr in _recipients(data):
            add_email_suppression(addr, "complaint", detail=data.get("email_id"))
        _log.info("resend_webhook: suppressed %d complaint(s)", len(_recipients(data)))
    # Other event types (delivered, opened, clicked, delivery_delayed) are
    # acknowledged but not acted on yet.

    return ("", 200)
