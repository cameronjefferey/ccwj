"""
Inbound webhooks.

1. Resend delivery events. Resend signs webhooks with Svix headers
   (``svix-id``, ``svix-timestamp``, ``svix-signature``). We verify the
   signature with stdlib HMAC so we don't pull in the ``svix`` SDK, then act on
   bounce / complaint events by adding the recipient to the email suppression
   list (``app.models.add_email_suppression``). Set ``RESEND_WEBHOOK_SECRET``.

2. SnapTrade account events. SnapTrade fires ``ACCOUNT_HOLDINGS_UPDATED`` once
   per account when ITS OWN daily sync (or a manual refresh) finishes pulling
   fresh holdings from the broker. That is the "SnapTrade is updated" signal —
   we react by running OUR sync for that account (read SnapTrade's now-fresh
   data → merge → push seeds). This is the event-driven "once X completes, kick
   off Y" flow; it needs NO paid force-refresh and NO polling cron. Set
   ``SNAPTRADE_WEBHOOK_SECRET`` to the value configured in the SnapTrade
   dashboard webhook (it arrives in the payload's ``webhookSecret`` field).

If a secret env var is unset we skip verification and log a warning — fine for
local dev, never for prod.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import threading
import time

from flask import request

from app import app
from app.extensions import csrf, limiter
from app.models import add_email_suppression

_log = logging.getLogger(__name__)

# One global advisory-lock key for ALL SnapTrade webhook-triggered syncs: every
# sync pushes the SAME seed CSVs (trade_history / current_positions /
# account_balances), and the GitHub ref update is not fast-forward-safe under
# concurrency, so a burst of per-account webhooks must push one-at-a-time.
_SNAPTRADE_SYNC_LOCK_KEY = 8274013

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


def _run_snaptrade_holdings_sync(user_id, snaptrade_account_id):
    """Background worker: SnapTrade just finished updating this account's
    holdings, so read its (now-fresh) data and push our seeds.

    Runs OFF the webhook request thread (we return 200 immediately) and under a
    cluster-wide advisory lock so a burst of per-account webhooks pushes
    seeds one-at-a-time. ``force_refresh=False`` — SnapTrade already pulled
    fresh data, so we must NOT pay to force another refresh. Never raises.
    """
    from app.db import advisory_lock
    from app.models import get_snaptrade_account
    from app.snaptrade import (
        _bulk_sync_lookback_days,
        _routine_lookback_days,
        _sync_one_connection,
        SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS,
    )

    with app.app_context():
        try:
            with advisory_lock(_SNAPTRADE_SYNC_LOCK_KEY):
                acc_row = get_snaptrade_account(user_id, snaptrade_account_id)
                if not acc_row:
                    _log.warning(
                        "snaptrade_webhook: no account row for user_id=%s account=%s",
                        user_id, snaptrade_account_id,
                    )
                    return
                lookback = _bulk_sync_lookback_days(
                    bool(acc_row.get("first_sync_completed")),
                    force_full_history=False,
                    routine_days=_routine_lookback_days(),
                    full_days=SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS,
                )
                res = _sync_one_connection(user_id, acc_row, lookback_days=lookback)
                _log.info(
                    "snaptrade_webhook sync user_id=%s account=%s: ok=%s rows=%s/%s pushed=%s",
                    user_id, snaptrade_account_id, res.get("ok"),
                    res.get("history_rows"), res.get("current_rows"),
                    res.get("github_pushed"),
                )
        except Exception as exc:  # pragma: no cover (defensive — never crash the thread)
            _log.exception(
                "snaptrade_webhook sync failed user_id=%s account=%s: %s",
                user_id, snaptrade_account_id, exc,
            )


@app.route("/webhooks/snaptrade", methods=["POST"])
@csrf.exempt
@limiter.limit("240 per minute")
def snaptrade_webhook():
    """Handle SnapTrade webhooks. The one we act on is
    ``ACCOUNT_HOLDINGS_UPDATED`` — fired when SnapTrade finishes syncing an
    account's holdings from the broker — which triggers our own sync for that
    account. Other event types are acknowledged (200) but not acted on yet.
    """
    body = request.get_data() or b""
    try:
        event = json.loads(body.decode() or "{}")
    except Exception:
        return ("bad payload", 400)
    if not isinstance(event, dict):
        return ("bad payload", 400)

    # SnapTrade authenticates by echoing the shared secret in the payload
    # (``webhookSecret``), not via an HMAC header. Constant-time compare.
    expected = (os.environ.get("SNAPTRADE_WEBHOOK_SECRET") or "").strip()
    if expected:
        provided = str(event.get("webhookSecret") or "")
        if not hmac.compare_digest(provided, expected):
            _log.warning("snaptrade_webhook: secret mismatch — rejecting")
            return ("invalid signature", 401)
    else:
        _log.warning(
            "snaptrade_webhook: SNAPTRADE_WEBHOOK_SECRET unset — skipping "
            "verification (acceptable in dev only)."
        )

    event_type = (event.get("eventType") or "").strip().upper()
    snap_user_id = event.get("userId")
    account_id = (event.get("accountId") or "").strip()

    if event_type == "ACCOUNT_HOLDINGS_UPDATED" and snap_user_id and account_id:
        from app.models import get_user_id_by_snaptrade_user_id
        user_id = get_user_id_by_snaptrade_user_id(str(snap_user_id))
        if user_id is not None:
            # Fire-and-forget: SnapTrade expects a prompt 200; the sync (broker
            # read + GitHub push) runs in a background thread, serialized by the
            # advisory lock so a burst of per-account events doesn't race.
            threading.Thread(
                target=_run_snaptrade_holdings_sync,
                args=(user_id, account_id),
                name=f"snaptrade-sync-{account_id[:8]}",
                daemon=True,
            ).start()
            _log.info(
                "snaptrade_webhook: ACCOUNT_HOLDINGS_UPDATED queued sync "
                "user_id=%s account=%s", user_id, account_id,
            )
        else:
            _log.warning(
                "snaptrade_webhook: no HappyTrader user for SnapTrade userId=%s",
                snap_user_id,
            )

    # Always 200 for authenticated, parseable events (even ones we don't act
    # on) so SnapTrade doesn't retry.
    return ("", 200)
