"""
Inbound webhooks.

1. Resend delivery events. Resend signs webhooks with Svix headers
   (``svix-id``, ``svix-timestamp``, ``svix-signature``). We verify the
   signature with stdlib HMAC so we don't pull in the ``svix`` SDK, then act on
   bounce / complaint events by adding the recipient to the email suppression
   list (``app.models.add_email_suppression``). Set ``RESEND_WEBHOOK_SECRET``.

2. SnapTrade account events. SnapTrade fires ``ACCOUNT_HOLDINGS_UPDATED`` per
   account when it detects a holdings change from the broker (on the REAL-TIME
   plan this is near-real-time and can fire many times a day; it was once-daily
   under the old Daily/cached plan). That is the "SnapTrade is updated" signal —
   we react by running OUR sync for that account (read SnapTrade's now-fresh
   data → merge → push seeds). This is the event-driven "once X completes, kick
   off Y" flow; it needs NO paid force-refresh and NO polling cron. Because the
   real-time plan fires so often and each changed sync triggers a dbt build, the
   handler DEBOUNCES per account (``_queue_snaptrade_sync``) so a burst collapses
   into a single sync — reporting is close-based, so intraday mark churn is not
   worth a build-per-event.

   AUTH: SnapTrade **deprecated webhook secrets**. Authenticity is now proven by
   the ``Signature`` header: base64( HMAC-SHA256( canonical-json-body, key =
   your **consumer key** ) ), where the canonical body is
   ``json.dumps(payload, separators=(",", ":"), sort_keys=True)``. We verify
   against ``SNAPTRADE_CONSUMER_KEY`` (already set for the API). There is no
   separate webhook secret to configure in the dashboard.

If the verifying key is unset we skip verification and log a warning — fine for
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

# Debounce/coalesce window for webhook-triggered syncs. Under SnapTrade's
# REAL-TIME plan (upgraded from Daily/cached), ``ACCOUNT_HOLDINGS_UPDATED``
# fires many times a day — every holdings/mark change, not just once after the
# nightly broker refresh. Each *changed* sync pushes seeds → triggers a dbt
# build. Because reporting is CLOSE-BASED (intraday broker marks are NOT our
# core numbers — see the pricing-precedence rule in AGENTS.md), running one
# full sync+build per intraday mark wiggle is wasted CI. So we COALESCE a burst
# of events for the same account into a single sync: the first event spawns a
# worker that waits for the account to go quiet for this window, absorbing any
# events that land meanwhile. Set ``SNAPTRADE_WEBHOOK_DEBOUNCE_SECONDS=0`` to
# disable (sync immediately, one per event — the pre-real-time behavior).
_WEBHOOK_DEBOUNCE_SECONDS = int(
    os.environ.get("SNAPTRADE_WEBHOOK_DEBOUNCE_SECONDS", "60") or "60"
)

# Per-account coalescing state (per process). Combined with the cluster-wide
# advisory lock in ``_run_snaptrade_holdings_sync`` this both collapses bursts
# WITHIN a worker (debounce) and serializes pushes ACROSS workers (lock).
_pending_lock = threading.Lock()
_pending_sync_at: dict = {}   # (user_id, account_id) -> latest event monotonic ts
_scheduled_keys: set = set()  # keys that currently have a live debounce worker


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


def _verify_snaptrade_signature(payload: dict, signature: str, consumer_key: str) -> bool:
    """Verify SnapTrade's ``Signature`` header (secrets are deprecated).

    expected = base64( HMAC_SHA256( consumer_key, canonical_body ) )
    canonical_body = json.dumps(payload, separators=(",", ":"), sort_keys=True)

    SnapTrade computes the HMAC over the RE-SERIALIZED canonical JSON (sorted
    keys, compact separators), not the raw bytes — so we recompute from the
    parsed dict, exactly per their docs example.
    """
    if not signature or not consumer_key:
        return False
    sig_content = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    digest = hmac.new(consumer_key.encode(), sig_content.encode(), hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode()
    return hmac.compare_digest(signature, expected)


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


def _run_debounced_snaptrade_sync(user_id, snaptrade_account_id):
    """Debounce worker: wait until this account has been *quiet* for the full
    ``_WEBHOOK_DEBOUNCE_SECONDS`` window (absorbing any events that arrive in
    the meantime), then run exactly one real sync. Spawned by
    ``_queue_snaptrade_sync``; one per account at a time.
    """
    key = (user_id, snaptrade_account_id)
    debounce = _WEBHOOK_DEBOUNCE_SECONDS
    if debounce > 0:
        # Sleep until the window elapses with no newer event. Each incoming
        # event bumps _pending_sync_at[key], extending the wait (coalesce).
        while True:
            with _pending_lock:
                last = _pending_sync_at.get(key, 0.0)
            remaining = debounce - (time.monotonic() - last)
            if remaining <= 0:
                break
            time.sleep(remaining)
    with _pending_lock:
        _scheduled_keys.discard(key)
        _pending_sync_at.pop(key, None)
    _run_snaptrade_holdings_sync(user_id, snaptrade_account_id)


def _queue_snaptrade_sync(user_id, snaptrade_account_id):
    """Coalesce a burst of ``ACCOUNT_HOLDINGS_UPDATED`` events for one account
    into a single sync. Marks the account dirty; spawns a debounce worker only
    if one isn't already running for it. Returns True when a NEW worker was
    spawned, False when an existing worker will absorb this event.
    """
    key = (user_id, snaptrade_account_id)
    with _pending_lock:
        _pending_sync_at[key] = time.monotonic()
        if key in _scheduled_keys:
            return False
        _scheduled_keys.add(key)
    threading.Thread(
        target=_run_debounced_snaptrade_sync,
        args=(user_id, snaptrade_account_id),
        name=f"snaptrade-sync-{str(snaptrade_account_id)[:8]}",
        daemon=True,
    ).start()
    return True


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

    # SnapTrade signs with the `Signature` header = base64(HMAC-SHA256(body,
    # key=consumer_key)). Webhook secrets are deprecated, so verify against
    # SNAPTRADE_CONSUMER_KEY (the same key used for API calls).
    consumer_key = (os.environ.get("SNAPTRADE_CONSUMER_KEY") or "").strip()
    if consumer_key:
        signature = request.headers.get("Signature") or ""
        if not _verify_snaptrade_signature(event, signature, consumer_key):
            _log.warning("snaptrade_webhook: signature verification failed — rejecting")
            return ("invalid signature", 401)
        # NOTE: no eventTimestamp/replay rejection. The HMAC signature already
        # authenticates the sender, and the action (re-read SnapTrade + push
        # seeds) is idempotent. SnapTrade RETRIES undelivered webhooks with a
        # 30-min backoff, carrying the ORIGINAL (now-old) eventTimestamp — a
        # freshness window would reject every retry, which previously dropped
        # legitimate events. Accept and process regardless of age.
    else:
        _log.warning(
            "snaptrade_webhook: SNAPTRADE_CONSUMER_KEY unset — skipping "
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
            # read + GitHub push) runs off-thread, DEBOUNCED per account so a
            # real-time burst collapses into one sync, and serialized by the
            # advisory lock so pushes across workers don't race.
            spawned = _queue_snaptrade_sync(user_id, account_id)
            _log.info(
                "snaptrade_webhook: ACCOUNT_HOLDINGS_UPDATED %s "
                "user_id=%s account=%s",
                "queued sync" if spawned else "coalesced into pending sync",
                user_id, account_id,
            )
        else:
            _log.warning(
                "snaptrade_webhook: no HappyTrader user for SnapTrade userId=%s",
                snap_user_id,
            )

    # Always 200 for authenticated, parseable events (even ones we don't act
    # on) so SnapTrade doesn't retry.
    return ("", 200)
