"""Stripe subscriptions — Checkout, Billing Portal, and the webhook.

THE SEAM: ``users.plan`` is the only thing the rest of the app reads
(``app/plan.py`` derives every gate from it). This module's entire job is to
keep that one column honest with Stripe:

    paying (active / trialing / past_due)  ->  plan = 'active'
    gone   (canceled / unpaid / expired)   ->  plan = plan_before_subscription
                                                      or 'trial'

Reverting to ``trial`` leaves ``trial_started_at`` intact, so a lapsed
subscriber lands straight back in the frozen-but-readable state the
reverse trial already defines — no separate "churned" state to maintain.
A grandfathered beta user who subscribes and later cancels returns to
'beta' via ``plan_before_subscription``.

Two prices, both live in Stripe (IDs come from env, never hardcoded):
$19.99/month and $199.99/year. Amounts are read back FROM Stripe for
display where possible so the page can never disagree with what the card
is charged.

Design notes worth keeping:

* **Checkout, not raw card fields.** Stripe-hosted Checkout means no card
  data touches this app and SCA/3DS, tax, and promo codes are Stripe's
  problem.
* **The success redirect reconciles immediately.** Waiting for the webhook
  would leave a paying user staring at a trial banner for a few seconds.
  The webhook is still the source of truth for everything after checkout
  (renewals, dunning, cancellations) — the success path is just an
  optimistic fast path over the same activation function.
* **Webhook is idempotent** via ``stripe_events``: Stripe retries, and
  every handler writes plan state.
* **`stripe_enabled()` gates the whole surface.** With no keys configured
  (tests, local dev, the pre-launch site) the routes 404-equivalent out and
  the pricing page falls back to the waitlist form, so nothing half-works.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from flask import flash, jsonify, redirect, request, url_for
from flask_login import current_user, login_required

from app import app
from app.db import execute, fetch_one
from app.extensions import csrf, limiter
from app.plan import PLAN_ACTIVE, PLAN_TRIAL, TRIAL_DAYS

_log = logging.getLogger(__name__)

# Display copy. The authoritative amounts live in the Stripe Prices these IDs
# point at; these strings exist so marketing pages render without an API call.
PRICE_MONTHLY_DISPLAY = "19.99"
PRICE_ANNUAL_DISPLAY = "199.99"
PRICE_ANNUAL_MONTHLY_EQUIV = "16.67"

# Stripe subscription statuses that mean "this person is paying us".
# past_due / incomplete are IN here deliberately: Stripe is still retrying the
# card, and yanking a trader's mirror mid-dunning (then restoring it hours
# later) is worse than carrying a few days of risk. Stripe emits
# customer.subscription.deleted when it finally gives up.
PAYING_STATUSES = frozenset({"active", "trialing", "past_due", "incomplete"})


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _env(name: str) -> str:
    return (os.environ.get(name, "") or "").strip()


def price_id(period: str) -> str:
    return _env("STRIPE_PRICE_ANNUAL" if period == "annual" else "STRIPE_PRICE_MONTHLY")


def stripe_enabled() -> bool:
    """True only when a secret key AND both price IDs are configured. Anything
    less is a misconfiguration, and a half-configured checkout that 500s is
    worse than a pricing page that still shows the waitlist."""
    return bool(_env("STRIPE_SECRET_KEY") and price_id("monthly") and price_id("annual"))


def _stripe():
    """Configured Stripe client, or None when billing is off / the SDK is
    missing. Imported lazily so the app boots without the package."""
    if not stripe_enabled():
        return None
    try:
        import stripe as stripe_sdk
    except ImportError:  # pragma: no cover - package is in requirements.txt
        _log.error("Stripe is configured but the stripe package is not installed")
        return None
    stripe_sdk.api_key = _env("STRIPE_SECRET_KEY")
    return stripe_sdk


def _base_url() -> str:
    from app.email import app_base_url

    return app_base_url()


# ---------------------------------------------------------------------------
# Postgres state (see _migrate_users_stripe_columns in app/models.py)
# ---------------------------------------------------------------------------

_BILLING_COLS = (
    "plan, plan_before_subscription, stripe_customer_id, stripe_subscription_id, "
    "subscription_status, subscription_price_id, subscription_current_period_end, "
    "subscription_cancel_at_period_end"
)


def billing_row(user_id):
    """Every Stripe column for a user, or None. Never raises."""
    try:
        return fetch_one(
            f"SELECT {_BILLING_COLS} FROM users WHERE id = %s", (user_id,)
        )
    except Exception as exc:
        _log.warning("billing_row(%s) failed: %s", user_id, exc)
        return None


def user_id_for_customer(customer_id):
    """Reverse lookup for webhook events, which carry a Stripe customer id
    rather than our user id."""
    try:
        row = fetch_one(
            "SELECT id FROM users WHERE stripe_customer_id = %s", (str(customer_id),)
        )
        return row["id"] if row else None
    except Exception as exc:
        _log.warning("user_id_for_customer(%s) failed: %s", customer_id, exc)
        return None


def save_customer_id(user_id, customer_id):
    try:
        execute(
            "UPDATE users SET stripe_customer_id = %s WHERE id = %s",
            (str(customer_id), user_id),
        )
        return True
    except Exception as exc:
        _log.warning("save_customer_id(%s) failed: %s", user_id, exc)
        return False


def event_already_handled(event_id) -> bool:
    """Claim a Stripe event id. True when we've already processed it (Stripe
    retries deliveries, and every handler writes plan state)."""
    try:
        row = fetch_one(
            "SELECT 1 AS seen FROM stripe_events WHERE event_id = %s", (str(event_id),)
        )
        return bool(row)
    except Exception as exc:
        # Fail OPEN: better to risk re-applying an idempotent plan write than
        # to drop a cancellation because the dedupe table was unreachable.
        _log.warning("event_already_handled(%s) failed: %s", event_id, exc)
        return False


def record_event(event_id, event_type):
    try:
        execute(
            "INSERT INTO stripe_events (event_id, event_type) VALUES (%s, %s) "
            "ON CONFLICT (event_id) DO NOTHING",
            (str(event_id), str(event_type or "")),
        )
    except Exception as exc:
        _log.warning("record_event(%s) failed: %s", event_id, exc)


def _ts(epoch):
    if not epoch:
        return None
    try:
        return datetime.fromtimestamp(int(epoch), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


# ---------------------------------------------------------------------------
# Plan transitions
# ---------------------------------------------------------------------------


def activate_subscription(
    user_id,
    *,
    subscription_id=None,
    customer_id=None,
    status="active",
    stripe_price_id=None,
    current_period_end=None,
    cancel_at_period_end=False,
):
    """Mark a user as a paying subscriber. Idempotent — Checkout's success
    redirect and the webhook both call this, often within the same second.

    ``plan_before_subscription`` is captured only on the transition INTO
    'active' (and only for a non-trial plan) so a grandfathered beta user
    can be returned to 'beta' if they later cancel. Re-running on an
    already-active user must not overwrite it with 'active'.
    """
    # NOTE: app.db.execute coerces params to a tuple, so these must be
    # POSITIONAL placeholders — named (%(name)s) params silently fail.
    try:
        execute(
            """
            UPDATE users
               SET plan_before_subscription = CASE
                       WHEN plan <> %s AND plan <> %s THEN plan
                       ELSE plan_before_subscription
                   END,
                   plan = %s,
                   plan_updated_at = NOW(),
                   stripe_customer_id = COALESCE(%s, stripe_customer_id),
                   stripe_subscription_id = COALESCE(%s, stripe_subscription_id),
                   subscription_status = %s,
                   subscription_price_id = COALESCE(%s, subscription_price_id),
                   subscription_current_period_end = %s,
                   subscription_cancel_at_period_end = %s
             WHERE id = %s
               AND (
                   %s IS NULL
                   OR stripe_subscription_id IS NULL
                   OR stripe_subscription_id = %s
                   OR COALESCE(subscription_status, '') NOT IN
                      ('active', 'trialing', 'past_due', 'incomplete')
               )
            """,
            (
                PLAN_ACTIVE,
                PLAN_TRIAL,
                PLAN_ACTIVE,
                str(customer_id) if customer_id else None,
                str(subscription_id) if subscription_id else None,
                str(status or "active"),
                str(stripe_price_id) if stripe_price_id else None,
                current_period_end,
                bool(cancel_at_period_end),
                user_id,
                str(subscription_id) if subscription_id else None,
                str(subscription_id) if subscription_id else None,
            ),
        )
        _log.info("Stripe: user_id=%s activated (status=%s)", user_id, status)
        return True
    except Exception as exc:
        _log.exception("activate_subscription(%s) failed: %s", user_id, exc)
        return False


def deactivate_subscription(user_id, *, status="canceled", subscription_id=None):
    """Subscription is gone for good. Fall back to the plan they held before
    subscribing (beta stays beta) or to 'trial'.

    Churn lands in exactly the same place trial expiry does — FROZEN, not
    disconnected. The trial clock is backdated to the freeze boundary
    (``NOW() - TRIAL_DAYS``) so the derived state is 'frozen' on day one of
    churn: every page stays readable, syncs stop, and the broker connection
    survives the usual 30-day grace so a win-back resumes instantly instead
    of requiring a reconnect. Backdating (rather than resetting to NOW) is
    what keeps this from being a free extra month of full access on every
    cancel. Beta users keep their clock untouched — they never freeze.

    Warehouse data is never touched, and the Stripe customer id is kept so a
    returning subscriber reuses their saved card and invoice history.
    """
    # Stripe does not guarantee webhook ordering. The subscription-id predicate
    # makes a delayed cancellation for sub_A a no-op after sub_B has already
    # activated; without it, that stale event freezes a currently-paying user.
    # Positional placeholders only — see the note in activate_subscription.
    try:
        execute(
            """
            UPDATE users
               SET plan = COALESCE(NULLIF(plan_before_subscription, ''), %s),
                   trial_started_at = CASE
                       WHEN COALESCE(NULLIF(plan_before_subscription, ''), %s) = %s
                       THEN NOW() - make_interval(days => %s)
                       ELSE trial_started_at
                   END,
                   plan_updated_at = NOW(),
                   subscription_status = %s,
                   subscription_cancel_at_period_end = FALSE,
                   stripe_subscription_id = NULL
             WHERE id = %s
               AND (
                   %s IS NULL
                   OR stripe_subscription_id IS NULL
                   OR stripe_subscription_id = %s
               )
            """,
            (
                PLAN_TRIAL,
                PLAN_TRIAL,
                PLAN_TRIAL,
                int(TRIAL_DAYS),
                str(status or "canceled"),
                user_id,
                str(subscription_id) if subscription_id else None,
                str(subscription_id) if subscription_id else None,
            ),
        )
        _log.info("Stripe: user_id=%s subscription ended (status=%s)", user_id, status)
        return True
    except Exception as exc:
        _log.exception("deactivate_subscription(%s) failed: %s", user_id, exc)
        return False


def apply_subscription(user_id, sub) -> bool:
    """Route one Stripe Subscription object to the right plan transition.
    ``sub`` is a Stripe object or plain dict (webhook payload)."""
    status = str(_get(sub, "status") or "")
    if status in PAYING_STATUSES:
        return activate_subscription(
            user_id,
            subscription_id=_get(sub, "id"),
            customer_id=_get(sub, "customer"),
            status=status,
            stripe_price_id=_subscription_price_id(sub),
            current_period_end=_ts(_subscription_period_end(sub)),
            cancel_at_period_end=bool(_get(sub, "cancel_at_period_end")),
        )
    return deactivate_subscription(
        user_id,
        status=status or "canceled",
        subscription_id=_get(sub, "id"),
    )


def queue_resume_sync(user_id):
    """Kick off a catch-up sync right after someone subscribes.

    A frozen mirror is stale by definition, and "Resume my mirror" has to mean
    NOW — waiting for the next webhook or the 23:00 UTC backstop would leave a
    paying user looking at the same stale numbers that made them subscribe.

    Reuses the webhook's per-account debounce queue, so this is the same
    battle-tested path (advisory lock, one sync per account, all the
    merge_and_push_seeds invariants) rather than a second way to sync. Call it
    only AFTER the plan is set to 'active' — ``_sync_one_connection`` still has
    the mandatory ``user_sync_allowed`` gate and would refuse otherwise. Only
    fires on the subscribe transition, never on monthly renewals.
    """
    try:
        from app.models import get_snaptrade_accounts
        from app.webhooks import _queue_snaptrade_sync

        accounts = get_snaptrade_accounts(user_id) or []
        for acc in accounts:
            account_id = acc.get("snaptrade_account_id")
            if account_id:
                _queue_snaptrade_sync(user_id, account_id)
        if accounts:
            _log.info(
                "Stripe: queued resume sync for user_id=%s (%d account(s))",
                user_id, len(accounts),
            )
    except Exception as exc:
        # Never let a sync hiccup affect the checkout result — the nightly
        # backstop will catch up regardless.
        _log.warning("queue_resume_sync(%s) failed: %s", user_id, exc)


def _get(obj, key, default=None):
    """Stripe objects support both attribute and mapping access; webhook
    payloads parsed as dicts support only the latter."""
    if obj is None:
        return default
    try:
        return obj[key]
    except (KeyError, TypeError):
        return getattr(obj, key, default)


def _subscription_price_id(sub):
    items = _get(sub, "items") or {}
    data = _get(items, "data") or []
    if not data:
        return None
    return _get(_get(data[0], "price") or {}, "id")


def our_price_ids() -> frozenset:
    """The HappyTrader Pro price ids, read from the same env vars checkout uses."""
    return frozenset(p for p in (price_id("monthly"), price_id("annual")) if p)


def _subscription_price_ids(sub) -> set:
    items = _get(sub, "items") or {}
    return {
        pid
        for pid in (
            _get(_get(item, "price") or {}, "id")
            for item in (_get(items, "data") or [])
        )
        if pid
    }


def subscription_is_ours(sub) -> bool:
    """Whether this Stripe Subscription is for HappyTrader Pro.

    MANDATORY on every webhook branch that writes plan state. The Stripe
    account is SHARED with sibling products (EarningsFollower, Job Glow), and
    Stripe delivers each endpoint the ACCOUNT's entire event stream — there is
    no per-product webhook filter to configure. Those apps stamp the same
    ``client_reference_id`` / ``metadata.user_id`` convention with THEIR OWN
    numeric user ids, so an unfiltered event resolves to the same-numbered
    HappyTrader user: a stranger's purchase would grant that user free Pro,
    and a stranger's cancellation would freeze a paying customer.

    Price is the only trustworthy discriminator here — it can't collide across
    products, and it works retroactively on subscriptions created before this
    check existed (unlike a metadata marker).
    """
    ours = our_price_ids()
    if not ours:
        return False  # Fail closed: unconfigured billing owns no subscription.
    return bool(_subscription_price_ids(sub) & ours)


def cancel_subscription_for_account_deletion(user_id):
    """Cancel any live HappyTrader subscription before deleting its user.

    The Postgres row is the only durable mapping from a HappyTrader user to
    Stripe's customer/subscription ids. Deleting it first leaves a renewing
    subscription that webhooks cannot re-link and the former user cannot
    manage through the portal. Account deletion therefore fails closed until
    Stripe confirms the subscription is canceled (or already absent/terminal).

    Returns ``(ok, error_message)`` and never raises.
    """
    row = billing_row(user_id)
    if row is None:
        # ``billing_row`` deliberately hides read errors from ordinary UI
        # callers, but deletion must fail closed. Treating an unreadable row
        # as "no subscription" would delete the only durable Stripe mapping
        # while a live subscription could continue renewing.
        _log.error(
            "Stripe: refusing account deletion for user_id=%s because "
            "billing state could not be verified",
            user_id,
        )
        return False, "Could not verify the Stripe subscription."
    subscription_id = (
        str(row.get("stripe_subscription_id") or "").strip()
    )
    if not subscription_id:
        return True, None

    stripe_sdk = _stripe()
    if stripe_sdk is None:
        return False, "Stripe billing is not configured."

    try:
        sub = stripe_sdk.Subscription.retrieve(subscription_id)
    except Exception as exc:
        # A prior cancellation may have removed the object before our local
        # mirror cleared. Missing means there is nothing left that can renew.
        if (
            getattr(exc, "code", None) == "resource_missing"
            or "no such subscription" in str(exc).lower()
        ):
            return True, None
        _log.exception(
            "Stripe: could not retrieve subscription %s before deleting "
            "user_id=%s: %s",
            subscription_id, user_id, exc,
        )
        return False, "Could not verify the Stripe subscription."

    if not subscription_is_ours(sub):
        _log.error(
            "Stripe: refusing account deletion for user_id=%s because stored "
            "subscription %s is not a HappyTrader price",
            user_id, subscription_id,
        )
        return False, "Stored subscription does not belong to HappyTrader."

    status = str(_get(sub, "status") or "").strip().lower()
    if status in {"canceled", "unpaid", "incomplete_expired"}:
        return True, None

    try:
        stripe_sdk.Subscription.cancel(subscription_id)
        _log.info(
            "Stripe: canceled subscription %s before deleting user_id=%s",
            subscription_id, user_id,
        )
        return True, None
    except Exception as exc:
        if (
            getattr(exc, "code", None) == "resource_missing"
            or "no such subscription" in str(exc).lower()
        ):
            return True, None
        _log.exception(
            "Stripe: cancellation failed for subscription %s before deleting "
            "user_id=%s: %s",
            subscription_id, user_id, exc,
        )
        return False, "Could not cancel the Stripe subscription."


def _invoice_is_ours(inv) -> bool:
    """Same shared-account guard for invoice events, whose line items carry the
    price at one of two paths depending on API version."""
    ours = our_price_ids()
    if not ours:
        return False
    lines = _get(inv, "lines") or {}
    for line in (_get(lines, "data") or []):
        if _get(_get(line, "price") or {}, "id") in ours:
            return True
        # 2025-03+ API versions moved it under pricing.price_details.
        pricing = _get(line, "pricing") or {}
        if _get(_get(pricing, "price_details") or {}, "price") in ours:
            return True
    return False


def _subscription_period_end(sub):
    """Stripe moved ``current_period_end`` onto subscription ITEMS in the
    2025-03 API version; older payloads still carry it at the top level."""
    top = _get(sub, "current_period_end")
    if top:
        return top
    items = _get(sub, "items") or {}
    data = _get(items, "data") or []
    if data:
        return _get(data[0], "current_period_end")
    return None


# ---------------------------------------------------------------------------
# Read model for templates
# ---------------------------------------------------------------------------


def subscription_summary(user_id):
    """What the profile page and banners need. None when billing is off or
    the user has never subscribed."""
    row = billing_row(user_id)
    if not row:
        return None
    if not row.get("stripe_customer_id") and not row.get("subscription_status"):
        return None
    price = row.get("subscription_price_id") or ""
    period = "annual" if price and price == price_id("annual") else (
        "monthly" if price and price == price_id("monthly") else None
    )
    return {
        "status": row.get("subscription_status"),
        "is_paying": (row.get("subscription_status") or "") in PAYING_STATUSES,
        "period": period,
        "amount": (
            PRICE_ANNUAL_DISPLAY if period == "annual"
            else PRICE_MONTHLY_DISPLAY if period == "monthly"
            else None
        ),
        "renews_on": row.get("subscription_current_period_end"),
        "cancel_at_period_end": bool(row.get("subscription_cancel_at_period_end")),
        "has_customer": bool(row.get("stripe_customer_id")),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


def _billing_off_response():
    msg = "Subscriptions aren't switched on yet — nothing was charged."
    if request.path.startswith("/api/") or request.accept_mimetypes.best == "application/json":
        return jsonify({"error": "billing_disabled", "message": msg}), 503
    flash(msg, "warning")
    return redirect(url_for("pricing"))


@app.route("/billing/checkout", methods=["POST"])
@login_required
@limiter.limit("20 per hour")
def billing_checkout():
    """Start Stripe Checkout for the monthly or annual price."""
    from app.utils import demo_block_writes

    blocked = demo_block_writes("subscribing")
    if blocked:
        return blocked

    stripe_sdk = _stripe()
    if stripe_sdk is None:
        return _billing_off_response()

    period = "annual" if (request.form.get("period") or "").strip() == "annual" else "monthly"
    row = billing_row(current_user.id) or {}

    # Already paying: send them to the portal to switch plans instead of
    # stacking a second subscription on the same customer.
    if (row.get("subscription_status") or "") in PAYING_STATUSES and row.get(
        "stripe_subscription_id"
    ):
        flash("You're already subscribed — manage or switch your plan here.", "info")
        return redirect(url_for("profile", tab="billing"))

    customer_id = row.get("stripe_customer_id")
    base = _base_url()
    try:
        kwargs = {
            "mode": "subscription",
            "line_items": [{"price": price_id(period), "quantity": 1}],
            "success_url": f"{base}/billing/success?session_id={{CHECKOUT_SESSION_ID}}",
            "cancel_url": f"{base}/pricing",
            "client_reference_id": str(current_user.id),
            "allow_promotion_codes": True,
            "billing_address_collection": "auto",
            # Carried onto the Subscription so webhook events can find the
            # user even if the customer-id write lost a race.
            "subscription_data": {"metadata": {"user_id": str(current_user.id)}},
            "metadata": {"user_id": str(current_user.id)},
        }
        if customer_id:
            kwargs["customer"] = customer_id
            kwargs["customer_update"] = {"address": "auto", "name": "auto"}
        else:
            email = (getattr(current_user, "email", "") or "").strip()
            if email:
                kwargs["customer_email"] = email
        session_obj = stripe_sdk.checkout.Session.create(**kwargs)
    except Exception as exc:
        app.logger.exception("Stripe checkout create failed: %s", exc)
        flash(
            "Couldn't open the checkout page just now. Nothing was charged — "
            "try again in a moment.",
            "danger",
        )
        return redirect(url_for("pricing"))

    return redirect(session_obj.url, code=303)


@app.route("/billing/success")
@login_required
def billing_success():
    """Post-checkout landing. Reconciles the subscription right away so the
    user never sees a stale trial banner while waiting on the webhook."""
    stripe_sdk = _stripe()
    session_id = (request.args.get("session_id") or "").strip()
    activated = False

    if stripe_sdk is not None and session_id:
        try:
            sess = stripe_sdk.checkout.Session.retrieve(
                session_id, expand=["subscription"]
            )
            # Only trust a session that belongs to THIS user — the id travels
            # in a URL and must not be replayable against another account.
            ref = str(_get(sess, "client_reference_id") or "")
            if ref and ref != str(current_user.id):
                app.logger.warning(
                    "Stripe: checkout session %s does not belong to user_id=%s",
                    session_id, current_user.id,
                )
            else:
                sub = _get(sess, "subscription")
                if isinstance(sub, str):
                    sub = stripe_sdk.Subscription.retrieve(sub)
                # Same shared-account guard as the webhook: this session id
                # comes from a URL, and a sibling product's session on the same
                # Stripe account can carry a client_reference_id that matches
                # this user's id. Price is what proves the purchase was ours.
                if sub is None or not subscription_is_ours(sub):
                    app.logger.warning(
                        "Stripe: checkout session %s is not a HappyTrader Pro "
                        "purchase — ignoring for user_id=%s",
                        session_id, current_user.id,
                    )
                else:
                    customer_id = _get(sess, "customer")
                    if customer_id:
                        save_customer_id(current_user.id, customer_id)
                    activated = apply_subscription(current_user.id, sub)
                    if activated:
                        queue_resume_sync(current_user.id)
        except Exception as exc:
            # The webhook is the source of truth — a failure here only costs
            # the user a few seconds of stale banner, so never show an error.
            app.logger.exception("Stripe success reconcile failed: %s", exc)

    if activated:
        flash(
            "You're subscribed — your mirror keeps updating every close. "
            "Thank you for supporting HappyTrader.",
            "success",
        )
    else:
        flash(
            "Payment received. Your subscription is being confirmed and will "
            "be active in a moment.",
            "success",
        )
    return redirect(url_for("weekly_review"))


@app.route("/billing/portal", methods=["POST"])
@login_required
def billing_portal():
    """Hand off to Stripe's Billing Portal for card updates, invoices,
    plan switches, and cancellation. Stripe owns that whole UI."""
    from app.utils import demo_block_writes

    blocked = demo_block_writes("managing a subscription")
    if blocked:
        return blocked

    stripe_sdk = _stripe()
    if stripe_sdk is None:
        return _billing_off_response()

    row = billing_row(current_user.id) or {}
    customer_id = row.get("stripe_customer_id")
    if not customer_id:
        flash("You don't have a subscription to manage yet.", "info")
        return redirect(url_for("pricing"))

    try:
        portal = stripe_sdk.billing_portal.Session.create(
            customer=customer_id,
            return_url=f"{_base_url()}/profile?tab=billing",
        )
    except Exception as exc:
        app.logger.exception("Stripe billing portal create failed: %s", exc)
        flash("Couldn't open the billing portal just now. Try again shortly.", "danger")
        return redirect(url_for("profile", tab="billing"))

    return redirect(portal.url, code=303)


@app.route("/webhooks/stripe", methods=["POST"])
@csrf.exempt
@limiter.limit("240 per minute")
def stripe_webhook():
    """Stripe's source of truth for everything after checkout: renewals,
    failed payments, plan switches, cancellations.

    Signature verification is MANDATORY — this endpoint grants paid access,
    so an unsigned request must never reach a plan write.
    """
    secret = _env("STRIPE_WEBHOOK_SECRET")
    stripe_sdk = _stripe()
    if stripe_sdk is None or not secret:
        app.logger.warning("Stripe webhook hit while billing is not configured")
        return ("", 503)

    payload = request.get_data()
    signature = request.headers.get("Stripe-Signature", "")
    try:
        event = stripe_sdk.Webhook.construct_event(payload, signature, secret)
    except Exception as exc:
        app.logger.warning("Stripe webhook signature rejected: %s", exc)
        return ("", 400)

    event_id = _get(event, "id")
    event_type = str(_get(event, "type") or "")
    if event_already_handled(event_id):
        return ("", 200)

    obj = _get(_get(event, "data") or {}, "object") or {}

    try:
        if event_type == "checkout.session.completed":
            # Resolve the subscription and confirm it's OURS before any write:
            # a sibling product's session carries its own user id in the very
            # fields we match on (see subscription_is_ours). A session with no
            # subscription is never ours — checkout is always mode=subscription.
            sub_id = _get(obj, "subscription")
            sub = stripe_sdk.Subscription.retrieve(sub_id) if sub_id else None
            if sub is None or not subscription_is_ours(sub):
                _log_foreign_event(event_type, obj)
            else:
                user_id = _user_id_from_session(obj)
                customer_id = _get(obj, "customer")
                if user_id and customer_id:
                    save_customer_id(user_id, customer_id)
                if user_id:
                    if not apply_subscription(user_id, sub):
                        # Plan writers deliberately return False on DB errors.
                        # Treat that as a failed delivery so Stripe retries;
                        # acknowledging it would leave a paying customer frozen.
                        raise RuntimeError(
                            f"plan activation failed for user_id={user_id}"
                        )
                    # Subscribe transition only — renewals arrive as
                    # invoice/subscription.updated events, not checkout.
                    queue_resume_sync(user_id)

        elif event_type in (
            "customer.subscription.created",
            "customer.subscription.updated",
            "customer.subscription.deleted",
        ):
            if not subscription_is_ours(obj):
                _log_foreign_event(event_type, obj)
            elif (user_id := _user_id_from_subscription(obj)):
                if event_type == "customer.subscription.deleted":
                    changed = deactivate_subscription(
                        user_id,
                        status=str(_get(obj, "status") or "canceled"),
                        subscription_id=_get(obj, "id"),
                    )
                else:
                    changed = apply_subscription(user_id, obj)
                if not changed:
                    # Returning 200 records the event and prevents Stripe from
                    # retrying. A failed plan write must remain unrecorded.
                    raise RuntimeError(
                        f"plan update failed for user_id={user_id}"
                    )
            else:
                # Retrying won't help (the mapping is missing, not flaky), so
                # this is acknowledged — but it means a real subscription is
                # unlinked from any account, which needs a human. Most likely
                # cause: a subscription created directly in the Stripe
                # dashboard without the user_id metadata.
                app.logger.error(
                    "Stripe: %s for customer=%s could not be matched to a user "
                    "— subscription %s is unlinked",
                    event_type, _get(obj, "customer"), _get(obj, "id"),
                )

        elif event_type == "invoice.payment_failed":
            # No plan change — Stripe is still retrying, and it emits
            # customer.subscription.deleted when dunning finally gives up.
            if _invoice_is_ours(obj):
                app.logger.warning(
                    "Stripe: invoice payment failed for customer=%s",
                    _get(obj, "customer"),
                )
    except Exception as exc:
        # Return 500 so Stripe retries; do NOT record the event.
        app.logger.exception("Stripe webhook %s failed: %s", event_type, exc)
        return ("", 500)

    record_event(event_id, event_type)
    return ("", 200)


def _log_foreign_event(event_type, obj):
    """A sibling product's event arriving on the shared Stripe account. This is
    expected traffic, not an error — logged at INFO so triage can see it was
    deliberately ignored rather than lost."""
    app.logger.info(
        "Stripe: ignoring %s — not a HappyTrader Pro subscription (customer=%s)",
        event_type,
        _get(obj, "customer"),
    )


def _user_id_from_session(sess):
    ref = _get(sess, "client_reference_id")
    if ref:
        try:
            return int(ref)
        except (TypeError, ValueError):
            pass
    meta = _get(sess, "metadata") or {}
    uid = _get(meta, "user_id")
    if uid:
        try:
            return int(uid)
        except (TypeError, ValueError):
            pass
    customer_id = _get(sess, "customer")
    return user_id_for_customer(customer_id) if customer_id else None


def _user_id_from_subscription(sub):
    meta = _get(sub, "metadata") or {}
    uid = _get(meta, "user_id")
    if uid:
        try:
            return int(uid)
        except (TypeError, ValueError):
            pass
    customer_id = _get(sub, "customer")
    return user_id_for_customer(customer_id) if customer_id else None


