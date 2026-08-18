"""Stripe subscriptions (app/billing.py).

The whole billing surface exists to keep ONE column honest — ``users.plan`` —
because every gate in the product derives from it (app/plan.py). These tests
pin the parts where getting it wrong either charges someone for nothing or
gives away the product:

- paying vs gone status mapping (past_due keeps access; canceled does not)
- the activation SQL captures ``plan_before_subscription`` only on the way IN,
  and only for a non-trial plan, so a grandfathered beta user who cancels
  returns to 'beta' instead of a lapsed trial
- webhook signature verification is mandatory, and replays are idempotent
- a checkout session id from a URL can't be replayed against another account
- ``stripe_enabled()`` is all-or-nothing, so a half-configured deploy shows
  the waitlist instead of a broken checkout
- the resume sync fires only AFTER the plan flips to active (the
  ``user_sync_allowed`` chokepoint would refuse otherwise)
"""
import types
from datetime import datetime, timedelta, timezone

import pytest

import app.billing as billing
from app.plan import PLAN_TRIAL, STATE_ACTIVE, STATE_ACTIVE_CANCELING
import app.plan as plan_mod

NOW = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def stripe_config(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_x")
    monkeypatch.setenv("STRIPE_PRICE_MONTHLY", "price_monthly")
    monkeypatch.setenv("STRIPE_PRICE_ANNUAL", "price_annual")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_x")


@pytest.fixture
def captured_sql(monkeypatch):
    calls = []
    monkeypatch.setattr(billing, "execute", lambda sql, params=None: calls.append((sql, params)))
    return calls


# ---------------------------------------------------------------------------
# Configuration gate
# ---------------------------------------------------------------------------


def test_stripe_enabled_is_all_or_nothing(monkeypatch):
    for name in ("STRIPE_SECRET_KEY", "STRIPE_PRICE_MONTHLY", "STRIPE_PRICE_ANNUAL"):
        monkeypatch.delenv(name, raising=False)
    assert billing.stripe_enabled() is False

    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_x")
    assert billing.stripe_enabled() is False, "keys without prices is a misconfiguration"

    monkeypatch.setenv("STRIPE_PRICE_MONTHLY", "price_monthly")
    assert billing.stripe_enabled() is False, "monthly alone is not enough"

    monkeypatch.setenv("STRIPE_PRICE_ANNUAL", "price_annual")
    assert billing.stripe_enabled() is True


def test_prices_are_the_advertised_amounts():
    assert billing.PRICE_MONTHLY_DISPLAY == "19.99"
    assert billing.PRICE_ANNUAL_DISPLAY == "199.99"


# ---------------------------------------------------------------------------
# Account deletion — Stripe must stop before its user mapping disappears
# ---------------------------------------------------------------------------


def test_account_deletion_cancels_live_subscription(monkeypatch, stripe_config):
    calls = []

    class _Subscriptions:
        @staticmethod
        def retrieve(subscription_id):
            calls.append(("retrieve", subscription_id))
            return _sub("active", id=subscription_id)

        @staticmethod
        def cancel(subscription_id):
            calls.append(("cancel", subscription_id))

    monkeypatch.setattr(billing, "billing_row", lambda _uid: {
        "stripe_subscription_id": "sub_live",
    })
    monkeypatch.setattr(
        billing, "_stripe",
        lambda: types.SimpleNamespace(Subscription=_Subscriptions),
    )

    assert billing.cancel_subscription_for_account_deletion(9) == (True, None)
    assert calls == [("retrieve", "sub_live"), ("cancel", "sub_live")]


def test_account_deletion_fails_closed_on_stripe_error(
    monkeypatch, stripe_config
):
    class _Subscriptions:
        @staticmethod
        def retrieve(_subscription_id):
            return _sub("active")

        @staticmethod
        def cancel(_subscription_id):
            raise RuntimeError("stripe unavailable")

    monkeypatch.setattr(billing, "billing_row", lambda _uid: {
        "stripe_subscription_id": "sub_live",
    })
    monkeypatch.setattr(
        billing, "_stripe",
        lambda: types.SimpleNamespace(Subscription=_Subscriptions),
    )

    ok, error = billing.cancel_subscription_for_account_deletion(9)
    assert ok is False
    assert "cancel" in error.lower()


def test_account_deletion_fails_closed_when_billing_state_is_unreadable(
    monkeypatch, stripe_config
):
    monkeypatch.setattr(billing, "billing_row", lambda _uid: None)
    monkeypatch.setattr(
        billing,
        "_stripe",
        lambda: pytest.fail("Stripe must not be consulted without billing state"),
    )

    ok, error = billing.cancel_subscription_for_account_deletion(9)

    assert ok is False
    assert "verify" in error.lower()


def test_account_deletion_refuses_foreign_subscription(
    monkeypatch, stripe_config
):
    canceled = []
    foreign = _sub("active")
    foreign["items"]["data"][0]["price"]["id"] = "price_sibling_product"

    class _Subscriptions:
        @staticmethod
        def retrieve(_subscription_id):
            return foreign

        @staticmethod
        def cancel(subscription_id):
            canceled.append(subscription_id)

    monkeypatch.setattr(billing, "billing_row", lambda _uid: {
        "stripe_subscription_id": "sub_foreign",
    })
    monkeypatch.setattr(
        billing, "_stripe",
        lambda: types.SimpleNamespace(Subscription=_Subscriptions),
    )

    ok, error = billing.cancel_subscription_for_account_deletion(9)
    assert ok is False
    assert "happytrader" in error.lower()
    assert canceled == []


# ---------------------------------------------------------------------------
# Status mapping — who keeps access
# ---------------------------------------------------------------------------


def _sub(status, **kw):
    base = {
        "id": "sub_1",
        "customer": "cus_1",
        "status": status,
        "cancel_at_period_end": False,
        "items": {"data": [{
            "price": {"id": "price_monthly"},
            "current_period_end": 1788000000,
        }]},
    }
    base.update(kw)
    return base


@pytest.mark.parametrize("status", ["active", "trialing", "past_due", "incomplete"])
def test_paying_statuses_activate(monkeypatch, status):
    seen = {}
    monkeypatch.setattr(
        billing, "activate_subscription",
        lambda uid, **kw: seen.update({"uid": uid, **kw}) or True,
    )
    monkeypatch.setattr(
        billing, "deactivate_subscription",
        lambda *a, **k: pytest.fail(f"{status} must keep access"),
    )
    assert billing.apply_subscription(9, _sub(status)) is True
    assert seen["uid"] == 9
    assert seen["status"] == status
    assert seen["stripe_price_id"] == "price_monthly"


@pytest.mark.parametrize("status", ["canceled", "unpaid", "incomplete_expired", "paused"])
def test_terminal_statuses_deactivate(monkeypatch, status):
    seen = {}
    monkeypatch.setattr(
        billing, "activate_subscription",
        lambda *a, **k: pytest.fail(f"{status} must not grant access"),
    )
    monkeypatch.setattr(
        billing, "deactivate_subscription",
        lambda uid, **kw: seen.update({"uid": uid, **kw}) or True,
    )
    assert billing.apply_subscription(9, _sub(status)) is True
    assert seen["status"] == status
    assert seen["subscription_id"] == "sub_1"


def test_period_end_read_from_items_when_absent_at_top_level():
    # Stripe moved current_period_end onto subscription items in the 2025-03
    # API version; a None here would blank the "renews on" date everywhere.
    assert billing._subscription_period_end(_sub("active")) == 1788000000
    assert billing._subscription_period_end(
        _sub("active", current_period_end=1700000000)
    ) == 1700000000


def test_ts_survives_garbage():
    assert billing._ts(None) is None
    assert billing._ts("not-a-number") is None
    assert billing._ts(1788000000).tzinfo is timezone.utc


# ---------------------------------------------------------------------------
# Plan transitions — the SQL contract
# ---------------------------------------------------------------------------


def test_plan_writes_use_positional_params(captured_sql):
    """app.db.execute coerces params to a tuple, so a dict of named params
    silently fails with 'N placeholders but M parameters'. That failure mode
    means a paying customer never gets activated, so pin the calling
    convention: positional placeholders, count matching the params."""
    billing.activate_subscription(5, subscription_id="sub_1", customer_id="cus_1")
    billing.deactivate_subscription(5)
    for sql, params in captured_sql:
        assert isinstance(params, tuple), "app.db.execute only supports positional params"
        assert "%(" not in sql, "named placeholders are silently broken by app.db.execute"
        assert sql.count("%s") == len(params)


def test_activation_only_captures_prior_plan_on_the_way_in(captured_sql):
    billing.activate_subscription(
        5, subscription_id="sub_1", customer_id="cus_1", status="active",
        stripe_price_id="price_annual",
    )
    sql, params = captured_sql[0]
    # A beta user's grandfathering must survive a subscribe -> cancel round
    # trip, and re-running activation must not overwrite it with 'active'.
    assert "plan_before_subscription = CASE" in sql
    assert "WHEN plan <> %s AND plan <> %s THEN plan" in sql
    assert params[:3] == ("active", PLAN_TRIAL, "active")


def test_deactivation_falls_back_to_prior_plan_then_trial(captured_sql):
    billing.deactivate_subscription(
        5, status="canceled", subscription_id="sub_current",
    )
    sql, params = captured_sql[0]
    assert "COALESCE(NULLIF(plan_before_subscription, ''), %s)" in sql
    assert params[0] == PLAN_TRIAL
    assert "stripe_customer_id = NULL" not in sql, "keep the customer for resubscribes"
    assert "stripe_subscription_id = %s" in sql
    assert params[-2:] == ("sub_current", "sub_current")


def test_activation_does_not_replace_another_paying_subscription(captured_sql):
    billing.activate_subscription(5, subscription_id="sub_new", status="active")
    sql, params = captured_sql[0]
    assert "stripe_subscription_id IS NULL" in sql
    assert "COALESCE(subscription_status, '') NOT IN" in sql
    assert params[-2:] == ("sub_new", "sub_new")


def test_churn_lands_in_frozen_grace_not_a_fresh_trial(captured_sql):
    """Cancelling must backdate the clock to the freeze boundary: frozen (so
    it isn't a free extra month) but still inside the 30-day connected grace
    (so a win-back resumes without reconnecting the broker)."""
    from app.plan import GRACE_DAYS, TRIAL_DAYS, derive_plan_state

    billing.deactivate_subscription(5)
    sql, params = captured_sql[0]
    assert "make_interval(days => %s)" in sql
    assert TRIAL_DAYS in params
    # Only when they fall back to trial — a beta user's clock stays untouched.
    assert "ELSE trial_started_at" in sql

    backdated = NOW - timedelta(days=TRIAL_DAYS)
    assert derive_plan_state("trial", backdated, now=NOW) == "frozen"
    # ...and the connection survives the full grace window from here.
    assert derive_plan_state(
        "trial", backdated, now=NOW + timedelta(days=GRACE_DAYS - 1),
    ) == "frozen"


def test_deactivation_never_touches_warehouse_or_tenants(captured_sql):
    billing.deactivate_subscription(5)
    sql, _ = captured_sql[0]
    for forbidden in ("broker_tenants", "DELETE", "snaptrade_accounts"):
        assert forbidden not in sql


# ---------------------------------------------------------------------------
# Resume sync — ordering matters
# ---------------------------------------------------------------------------


def test_resume_sync_queues_every_account(monkeypatch):
    import app.models as models_mod
    import app.webhooks as webhooks_mod

    queued = []
    monkeypatch.setattr(
        models_mod, "get_snaptrade_accounts",
        lambda uid: [{"snaptrade_account_id": "acc-1"}, {"snaptrade_account_id": "acc-2"}],
    )
    monkeypatch.setattr(
        webhooks_mod, "_queue_snaptrade_sync",
        lambda uid, acc: queued.append((uid, acc)) or True,
    )
    billing.queue_resume_sync(4)
    assert queued == [(4, "acc-1"), (4, "acc-2")]


def test_resume_sync_never_raises(monkeypatch):
    import app.models as models_mod

    monkeypatch.setattr(
        models_mod, "get_snaptrade_accounts",
        lambda uid: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    billing.queue_resume_sync(4)  # must not raise — checkout already succeeded


# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------


class _FakeStripe:
    """Minimal stand-in for the stripe module."""

    def __init__(self, event=None, subscription=None, verify=True):
        self._event = event
        self._subscription = subscription
        self._verify = verify
        self.retrieved = []
        outer = self

        class _Webhook:
            @staticmethod
            def construct_event(payload, signature, secret):
                if not outer._verify:
                    raise ValueError("bad signature")
                return outer._event

        class _Subscription:
            @staticmethod
            def retrieve(sub_id, **kw):
                outer.retrieved.append(sub_id)
                return outer._subscription

        self.Webhook = _Webhook
        self.Subscription = _Subscription


def _post_webhook(client):
    return client.post(
        "/webhooks/stripe",
        data=b"{}",
        headers={"Stripe-Signature": "t=1,v1=deadbeef"},
        content_type="application/json",
    )


def test_webhook_rejects_bad_signature(client, monkeypatch, stripe_config):
    monkeypatch.setattr(billing, "_stripe", lambda: _FakeStripe(verify=False))
    monkeypatch.setattr(
        billing, "apply_subscription",
        lambda *a, **k: pytest.fail("unsigned payload must never reach a plan write"),
    )
    assert _post_webhook(client).status_code == 400


def test_webhook_503s_when_billing_unconfigured(client, monkeypatch):
    monkeypatch.delenv("STRIPE_WEBHOOK_SECRET", raising=False)
    monkeypatch.setattr(billing, "_stripe", lambda: None)
    assert _post_webhook(client).status_code == 503


def test_webhook_activates_on_checkout_completed(client, monkeypatch, stripe_config):
    event = {
        "id": "evt_1",
        "type": "checkout.session.completed",
        "data": {"object": {
            "customer": "cus_1",
            "subscription": "sub_1",
            "client_reference_id": "42",
        }},
    }
    fake = _FakeStripe(event=event, subscription=_sub("active"))
    monkeypatch.setattr(billing, "_stripe", lambda: fake)
    monkeypatch.setattr(billing, "event_already_handled", lambda eid: False)

    recorded, customers, applied, resumed = [], [], [], []
    monkeypatch.setattr(billing, "record_event", lambda eid, t: recorded.append(eid))
    monkeypatch.setattr(
        billing, "save_customer_id", lambda uid, cid: customers.append((uid, cid)),
    )
    monkeypatch.setattr(
        billing, "apply_subscription",
        lambda uid, sub: applied.append((uid, sub["status"])) or True,
    )
    monkeypatch.setattr(billing, "queue_resume_sync", lambda uid: resumed.append(uid))

    assert _post_webhook(client).status_code == 200
    assert customers == [(42, "cus_1")]
    assert applied == [(42, "active")]
    assert resumed == [42], "a new subscriber's stale mirror should catch up now"
    assert recorded == ["evt_1"]


def test_webhook_replay_is_a_noop(client, monkeypatch, stripe_config):
    event = {"id": "evt_1", "type": "customer.subscription.deleted",
             "data": {"object": _sub("canceled")}}
    monkeypatch.setattr(billing, "_stripe", lambda: _FakeStripe(event=event))
    monkeypatch.setattr(billing, "event_already_handled", lambda eid: True)
    monkeypatch.setattr(
        billing, "deactivate_subscription",
        lambda *a, **k: pytest.fail("a replayed event must not re-apply"),
    )
    assert _post_webhook(client).status_code == 200


def test_webhook_returns_500_without_recording_so_stripe_retries(
    client, monkeypatch, stripe_config
):
    event = {"id": "evt_2", "type": "customer.subscription.updated",
             "data": {"object": _sub("active", metadata={"user_id": "42"})}}
    monkeypatch.setattr(billing, "_stripe", lambda: _FakeStripe(event=event))
    monkeypatch.setattr(billing, "event_already_handled", lambda eid: False)
    monkeypatch.setattr(
        billing, "record_event",
        lambda *a: pytest.fail("a failed event must stay un-recorded so Stripe retries"),
    )
    monkeypatch.setattr(
        billing, "apply_subscription",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("postgres down")),
    )
    assert _post_webhook(client).status_code == 500


def test_webhook_retries_when_plan_writer_returns_false(
    client, monkeypatch, stripe_config
):
    """Plan writers catch DB exceptions and return False. The webhook must turn
    that into a 500 instead of acknowledging and permanently losing the event."""
    event = {"id": "evt_plan_false", "type": "customer.subscription.updated",
             "data": {"object": _sub("active", metadata={"user_id": "42"})}}
    monkeypatch.setattr(billing, "_stripe", lambda: _FakeStripe(event=event))
    monkeypatch.setattr(billing, "event_already_handled", lambda eid: False)
    monkeypatch.setattr(
        billing, "record_event",
        lambda *a: pytest.fail("failed plan writes must remain retryable"),
    )
    monkeypatch.setattr(billing, "apply_subscription", lambda *a, **k: False)

    assert _post_webhook(client).status_code == 500


def test_checkout_webhook_retries_when_activation_returns_false(
    client, monkeypatch, stripe_config
):
    event = {
        "id": "evt_checkout_false",
        "type": "checkout.session.completed",
        "data": {"object": {
            "customer": "cus_1",
            "subscription": "sub_1",
            "client_reference_id": "42",
        }},
    }
    fake = _FakeStripe(event=event, subscription=_sub("active"))
    monkeypatch.setattr(billing, "_stripe", lambda: fake)
    monkeypatch.setattr(billing, "event_already_handled", lambda eid: False)
    monkeypatch.setattr(billing, "save_customer_id", lambda *a: True)
    monkeypatch.setattr(billing, "apply_subscription", lambda *a, **k: False)
    monkeypatch.setattr(
        billing, "record_event",
        lambda *a: pytest.fail("failed checkout activation must be retried"),
    )

    assert _post_webhook(client).status_code == 500


def test_deleted_event_passes_subscription_id_to_atomic_guard(
    client, monkeypatch, stripe_config
):
    event = {"id": "evt_stale_delete", "type": "customer.subscription.deleted",
             "data": {"object": _sub(
                 "canceled", id="sub_old", metadata={"user_id": "42"},
             )}}
    monkeypatch.setattr(billing, "_stripe", lambda: _FakeStripe(event=event))
    monkeypatch.setattr(billing, "event_already_handled", lambda eid: False)
    monkeypatch.setattr(billing, "record_event", lambda *a: None)
    seen = {}
    monkeypatch.setattr(
        billing, "deactivate_subscription",
        lambda uid, **kw: seen.update({"uid": uid, **kw}) or True,
    )

    assert _post_webhook(client).status_code == 200
    assert seen["uid"] == 42
    assert seen["subscription_id"] == "sub_old"


def test_webhook_acks_but_logs_an_unmatchable_customer(client, monkeypatch, stripe_config):
    """A subscription we can't map to a user is a support problem, not a retry
    problem — ack it, but it must be loud in the logs rather than swallowed."""
    event = {"id": "evt_3", "type": "customer.subscription.updated",
             "data": {"object": _sub("active")}}
    monkeypatch.setattr(billing, "_stripe", lambda: _FakeStripe(event=event))
    monkeypatch.setattr(billing, "event_already_handled", lambda eid: False)
    monkeypatch.setattr(billing, "record_event", lambda *a: None)
    monkeypatch.setattr(billing, "_user_id_from_subscription", lambda obj: None)

    errors = []
    monkeypatch.setattr(billing.app.logger, "error", lambda *a, **k: errors.append(a))
    assert _post_webhook(client).status_code == 200
    assert errors, "an unlinked subscription must be logged"


def test_webhook_resolves_user_from_metadata_then_customer(monkeypatch):
    monkeypatch.setattr(billing, "user_id_for_customer", lambda cid: 77)
    assert billing._user_id_from_subscription({"metadata": {"user_id": "5"}}) == 5
    assert billing._user_id_from_subscription({"customer": "cus_1"}) == 77
    assert billing._user_id_from_session({"client_reference_id": "8"}) == 8
    assert billing._user_id_from_session({"metadata": {"user_id": "9"}}) == 9
    # Garbage must not blow up or resolve to a random account.
    monkeypatch.setattr(billing, "user_id_for_customer", lambda cid: None)
    assert billing._user_id_from_session({"client_reference_id": "not-an-int"}) is None


# ---------------------------------------------------------------------------
# Shared Stripe account — a sibling product must never move OUR plans
#
# One live Stripe account serves HappyTrader Pro, EarningsFollower and Job
# Glow, and Stripe delivers every endpoint the ACCOUNT's full event stream.
# All three apps stamp client_reference_id / metadata.user_id with their own
# numeric user ids, so an unfiltered event resolves to the same-numbered
# HappyTrader user: a stranger's purchase would grant free Pro, and a
# stranger's cancellation would freeze a paying customer. Price is the gate.
# ---------------------------------------------------------------------------


def _foreign_sub(status="active", **kw):
    """A sibling product's subscription, carrying its own user id."""
    return _sub(
        status,
        items={"data": [{
            "price": {"id": "price_earningsfollower_monthly"},
            "current_period_end": 1788000000,
        }]},
        metadata={"user_id": "4"},
        **kw,
    )


def test_subscription_is_ours_matches_on_price(stripe_config):
    assert billing.subscription_is_ours(_sub("active")) is True
    assert billing.subscription_is_ours(_foreign_sub()) is False
    assert billing.subscription_is_ours({}) is False


def test_both_our_periods_are_recognised(stripe_config):
    assert billing.our_price_ids() == frozenset({"price_monthly", "price_annual"})
    annual = _sub("active", items={"data": [{"price": {"id": "price_annual"}}]})
    assert billing.subscription_is_ours(annual) is True


def test_subscription_is_ours_fails_closed_when_unconfigured(monkeypatch):
    for name in ("STRIPE_PRICE_MONTHLY", "STRIPE_PRICE_ANNUAL"):
        monkeypatch.delenv(name, raising=False)
    assert billing.our_price_ids() == frozenset()
    assert billing.subscription_is_ours(_sub("active")) is False, (
        "with no configured price we own no subscription — never guess"
    )


def test_sibling_checkout_does_not_grant_our_user_free_pro(
    client, monkeypatch, stripe_config
):
    """EarningsFollower user #4 subscribing must not activate HappyTrader #4."""
    event = {
        "id": "evt_foreign_1",
        "type": "checkout.session.completed",
        "data": {"object": {
            "customer": "cus_stranger",
            "subscription": "sub_stranger",
            "client_reference_id": "4",
            "metadata": {"user_id": "4"},
        }},
    }
    fake = _FakeStripe(event=event, subscription=_foreign_sub())
    monkeypatch.setattr(billing, "_stripe", lambda: fake)
    monkeypatch.setattr(billing, "event_already_handled", lambda eid: False)
    recorded = []
    monkeypatch.setattr(billing, "record_event", lambda eid, t: recorded.append(eid))
    monkeypatch.setattr(
        billing, "save_customer_id",
        lambda *a: pytest.fail("must not bind a stranger's customer to our user"),
    )
    monkeypatch.setattr(
        billing, "apply_subscription",
        lambda *a, **k: pytest.fail("must not grant Pro for another product's sale"),
    )
    monkeypatch.setattr(
        billing, "queue_resume_sync",
        lambda *a: pytest.fail("must not sync for a user who never paid"),
    )

    assert _post_webhook(client).status_code == 200
    assert recorded == ["evt_foreign_1"], "ack and dedupe, but no plan write"


@pytest.mark.parametrize("event_type", [
    "customer.subscription.created",
    "customer.subscription.updated",
    "customer.subscription.deleted",
])
def test_sibling_subscription_events_never_move_our_plan(
    client, monkeypatch, stripe_config, event_type
):
    """The expensive direction: a stranger's cancellation on the shared account
    must not freeze one of our paying customers."""
    event = {"id": f"evt_{event_type}", "type": event_type,
             "data": {"object": _foreign_sub("canceled")}}
    monkeypatch.setattr(billing, "_stripe", lambda: _FakeStripe(event=event))
    monkeypatch.setattr(billing, "event_already_handled", lambda eid: False)
    monkeypatch.setattr(billing, "record_event", lambda *a: None)
    monkeypatch.setattr(
        billing, "apply_subscription",
        lambda *a, **k: pytest.fail("another product's subscription is not ours"),
    )
    monkeypatch.setattr(
        billing, "deactivate_subscription",
        lambda *a, **k: pytest.fail("a stranger's cancel must not freeze our customer"),
    )
    errors = []
    monkeypatch.setattr(billing.app.logger, "error", lambda *a, **k: errors.append(a))

    assert _post_webhook(client).status_code == 200
    assert not errors, "expected sibling traffic is not an error condition"


def test_checkout_without_a_subscription_is_ignored(client, monkeypatch, stripe_config):
    """Sibling apps also sell one-time payments. Our checkout is always
    mode=subscription, so a session with no subscription is never ours."""
    event = {"id": "evt_one_time", "type": "checkout.session.completed",
             "data": {"object": {"customer": "cus_x", "client_reference_id": "7"}}}
    monkeypatch.setattr(billing, "_stripe", lambda: _FakeStripe(event=event))
    monkeypatch.setattr(billing, "event_already_handled", lambda eid: False)
    monkeypatch.setattr(billing, "record_event", lambda *a: None)
    monkeypatch.setattr(
        billing, "save_customer_id",
        lambda *a: pytest.fail("no subscription means no HappyTrader purchase"),
    )
    assert _post_webhook(client).status_code == 200


def test_invoice_ownership_covers_both_api_shapes(stripe_config):
    assert billing._invoice_is_ours(
        {"lines": {"data": [{"price": {"id": "price_annual"}}]}}
    ) is True
    assert billing._invoice_is_ours(
        {"lines": {"data": [{"price": {"id": "price_theirs"}}]}}
    ) is False
    # 2025-03+ API versions nest it under pricing.price_details.
    assert billing._invoice_is_ours(
        {"lines": {"data": [{"pricing": {"price_details": {"price": "price_monthly"}}}]}}
    ) is True


# ---------------------------------------------------------------------------
# Success redirect — session ids travel in URLs
# ---------------------------------------------------------------------------


def test_success_ignores_session_belonging_to_another_user(monkeypatch, stripe_config):
    from app import app as flask_app

    class _Sessions:
        @staticmethod
        def retrieve(sid, **kw):
            return {"client_reference_id": "999", "customer": "cus_x",
                    "subscription": _sub("active")}

    fake = types.SimpleNamespace(
        checkout=types.SimpleNamespace(Session=_Sessions),
        Subscription=types.SimpleNamespace(retrieve=lambda s, **k: _sub("active")),
    )
    monkeypatch.setattr(billing, "_stripe", lambda: fake)
    monkeypatch.setattr(
        billing, "apply_subscription",
        lambda *a, **k: pytest.fail("must not activate from someone else's session"),
    )
    monkeypatch.setattr(billing, "save_customer_id", lambda *a: None)
    monkeypatch.setattr(billing, "current_user", types.SimpleNamespace(
        is_authenticated=True, id=42, email="a@b.c",
    ))
    with flask_app.test_request_context("/billing/success?session_id=cs_1"):
        resp = billing.billing_success()
        assert resp.status_code == 302


def test_success_ignores_a_sibling_products_session(monkeypatch, stripe_config):
    """The id matches this user, but the purchase was for another product on the
    shared Stripe account — buying EarningsFollower must not unlock Pro."""
    from app import app as flask_app

    foreign = _foreign_sub()

    class _Sessions:
        @staticmethod
        def retrieve(sid, **kw):
            # client_reference_id matches the logged-in user: both apps number
            # their users independently and #4 exists in each.
            return {"client_reference_id": "4", "customer": "cus_stranger",
                    "subscription": foreign}

    fake = types.SimpleNamespace(
        checkout=types.SimpleNamespace(Session=_Sessions),
        Subscription=types.SimpleNamespace(retrieve=lambda s, **k: foreign),
    )
    monkeypatch.setattr(billing, "_stripe", lambda: fake)
    monkeypatch.setattr(
        billing, "apply_subscription",
        lambda *a, **k: pytest.fail("another product's purchase must not grant Pro"),
    )
    monkeypatch.setattr(
        billing, "save_customer_id",
        lambda *a: pytest.fail("must not bind a stranger's Stripe customer"),
    )
    monkeypatch.setattr(billing, "current_user", types.SimpleNamespace(
        is_authenticated=True, id=4, email="a@b.c",
    ))
    with flask_app.test_request_context("/billing/success?session_id=cs_1"):
        assert billing.billing_success().status_code == 302


# ---------------------------------------------------------------------------
# Banner: a pending cancellation is stated, not sprung
# ---------------------------------------------------------------------------


def test_active_subscriber_gets_no_banner(monkeypatch):
    monkeypatch.setattr(plan_mod, "_is_exempt_username", lambda u: False)
    monkeypatch.setattr(plan_mod, "get_user_plan_row", lambda uid: {
        "plan": "active", "trial_started_at": None, "username": "u",
        "subscription_cancel_at_period_end": False,
        "subscription_current_period_end": NOW,
    })
    assert plan_mod.plan_status_for_banner(1, now=NOW) is None


def test_pending_cancellation_banner_shows_the_end_date(monkeypatch):
    monkeypatch.setattr(plan_mod, "_is_exempt_username", lambda u: False)
    monkeypatch.setattr(plan_mod, "get_user_plan_row", lambda uid: {
        "plan": "active", "trial_started_at": None, "username": "u",
        "subscription_cancel_at_period_end": True,
        "subscription_current_period_end": NOW,
    })
    banner = plan_mod.plan_status_for_banner(1, now=NOW)
    assert banner["state"] == STATE_ACTIVE_CANCELING
    assert banner["frozen_on"] == NOW.date()


def test_cancelling_subscriber_still_syncs(monkeypatch):
    # They paid through the period end — access must not degrade early.
    monkeypatch.setattr(plan_mod, "plan_state", lambda uid: STATE_ACTIVE)
    assert plan_mod.user_sync_allowed(1) is True


def test_plan_row_falls_back_to_narrow_select(monkeypatch):
    """A database missing the Stripe columns must still resolve plan state —
    otherwise every user silently fails open to exempt."""
    calls = []

    def fake_fetch_one(sql, params=None):
        calls.append(sql)
        if "subscription_status" in sql:
            raise RuntimeError('column "subscription_status" does not exist')
        return {"plan": "trial", "trial_started_at": None, "username": "u"}

    monkeypatch.setattr(plan_mod, "fetch_one", fake_fetch_one)
    row = plan_mod.get_user_plan_row(1)
    assert row["plan"] == "trial"
    assert len(calls) == 2


# ---------------------------------------------------------------------------
# Read model
# ---------------------------------------------------------------------------


def test_subscription_summary_labels_the_period(monkeypatch, stripe_config):
    monkeypatch.setattr(billing, "billing_row", lambda uid: {
        "plan": "active", "stripe_customer_id": "cus_1",
        "stripe_subscription_id": "sub_1", "subscription_status": "active",
        "subscription_price_id": "price_annual",
        "subscription_current_period_end": NOW,
        "subscription_cancel_at_period_end": False,
        "plan_before_subscription": None,
    })
    s = billing.subscription_summary(1)
    assert s["period"] == "annual"
    assert s["amount"] == billing.PRICE_ANNUAL_DISPLAY
    assert s["is_paying"] is True


def test_subscription_summary_none_for_never_subscribed(monkeypatch):
    monkeypatch.setattr(billing, "billing_row", lambda uid: {
        "plan": "trial", "stripe_customer_id": None, "subscription_status": None,
    })
    assert billing.subscription_summary(1) is None
