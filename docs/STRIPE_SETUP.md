# Stripe Setup Guide

HappyTrader Pro is **$19.99/month** or **$199.99/year** (two months free),
billed through [Stripe](https://stripe.com) Checkout + Billing Portal.

Billing is layered on top of the reverse trial (`app/plan.py`, see AGENTS.md):
the trial gives 30 days of full access with no card, then the mirror freezes.
Subscribing sets `users.plan = 'active'`, which is the only thing the rest of
the product reads. Everything Stripe-related lives in **`app/billing.py`**.

## Prerequisites

- A Stripe account with a business profile completed (needed to leave test mode)
- The `stripe` Python package (pinned in `requirements.txt`)

## Step 1 — Create the two Prices

In the Stripe Dashboard → **Product catalog** → **Add product**:

1. Product name: `HappyTrader Pro` (this string shows on the checkout page and
   the invoice, so use the customer-facing name).
2. Add a **recurring** price: **$19.99 USD / month**. Copy its price ID
   (`price_...`).
3. On the same product, **Add another price**: **$199.99 USD / year**. Copy
   that price ID too.

Two prices on ONE product is what lets a subscriber switch between monthly and
annual in the Billing Portal without cancelling and resubscribing.

The dollar amounts in `app/billing.py` (`PRICE_MONTHLY_DISPLAY` /
`PRICE_ANNUAL_DISPLAY`) are **display copy only** — the amount actually
charged is whatever the Stripe Price says. If you change a price in Stripe,
change those constants in the same commit or the marketing page will lie.

## Step 2 — Configure the Billing Portal

Stripe Dashboard → **Settings** → **Billing** → **Customer portal**:

- Enable **Update payment method**, **Cancel subscription**, and
  **Invoice history**.
- For cancellation, choose **at end of billing period** (not immediately).
  The app is built around this: a pending cancellation shows an honest "your
  subscription ends <date>" banner and access continues until then.
- Under **Products**, add the HappyTrader Pro product with both prices so
  customers can switch monthly ↔ annual themselves.
- Leave **quantity adjustment OFF**. Pro is one seat per account and gating is a
  boolean (`plan='active'`), so a customer who raised the quantity would pay a
  multiple for nothing.

To verify this config from the API, `products` is an **expandable** field — it
reads as `null` unless you ask for it, which looks alarmingly like "no plans
attached":

```bash
curl -s -u "$STRIPE_SECRET_KEY:" -G \
  "https://api.stripe.com/v1/billing_portal/configurations/<bpc_id>" \
  --data-urlencode "expand[]=features.subscription_update.products"
```

## Step 3 — Create the webhook endpoint

Stripe Dashboard → **Developers** → **Webhooks** → **Add endpoint**:

- URL: `https://happytrader.me/webhooks/stripe`
- Events to send:
  - `checkout.session.completed`
  - `customer.subscription.created`
  - `customer.subscription.updated`
  - `customer.subscription.deleted`
  - `invoice.payment_failed`

> ### This Stripe account is shared with other products
>
> The live account also serves **EarningsFollower** and **Job Glow**, and Stripe
> has **no per-product webhook filter** — every endpoint receives the entire
> account's event stream. All three apps stamp `client_reference_id` and
> `metadata.user_id` with their own numeric user ids, so an EarningsFollower
> subscription from *their* user #4 arrives here looking exactly like a purchase
> by *our* user #4.
>
> `app/billing.py` therefore gates every plan write on
> `subscription_is_ours(sub)`, which matches the subscription's price id against
> `STRIPE_PRICE_MONTHLY` / `STRIPE_PRICE_ANNUAL`. **Do not remove this, and do
> not replace it with a metadata marker** — price ids can't collide across
> products and the check works on subscriptions created before it existed.
> Selecting fewer events in the dashboard does *not* substitute for it.
>
> Note the reverse direction is the sibling apps' problem to fix in their own
> code: HappyTrader's `checkout.session.completed` events are delivered to their
> endpoints too.

Copy the **signing secret** (`whsec_...`). Signature verification is mandatory
— the endpoint grants paid access, so it returns 400 on any unsigned or
mis-signed request and never reaches a plan write.

## Step 4 — Configure HappyTrader

The `ccwj` web service is **manually managed in the Render dashboard** (it is
deliberately not in `app/render.yaml`), so add these under
**Environment** there, and to `.env` for local dev:

```bash
# Secret key: sk_test_... in dev, sk_live_... in production
STRIPE_SECRET_KEY=sk_test_...

# The two Price IDs from Step 1
STRIPE_PRICE_MONTHLY=price_...
STRIPE_PRICE_ANNUAL=price_...

# Signing secret from Step 3
STRIPE_WEBHOOK_SECRET=whsec_...
```

**All four are required together.** `stripe_enabled()` is deliberately
all-or-nothing: with anything missing, the checkout routes refuse (503 /
redirect), the Pro card falls back to the waitlist form, and the profile
Billing tab says paid plans aren't switched on. A half-configured deploy shows
a coherent pre-launch page instead of a checkout that 500s.

No Stripe env vars are needed by any cron — the lifecycle cron only ever
looks at `plan = 'trial'` users, so subscribers are never disconnected.

## Step 5 — Test with test-mode cards

Use `sk_test_...` keys and test-mode Price IDs, then run the flows against a
local server with the Stripe CLI forwarding webhooks:

```bash
stripe login
stripe listen --forward-to localhost:5000/webhooks/stripe
# prints a whsec_... for STRIPE_WEBHOOK_SECRET in your local .env
```

Card `4242 4242 4242 4242` (any future expiry / any CVC) succeeds;
`4000 0000 0000 0341` succeeds then fails on renewal, which is the useful one
for exercising dunning.

Worth walking through at least once before going live:

| Flow | Expected result |
| --- | --- |
| Subscribe monthly from `/pricing` | `plan='active'`, banner gone, catch-up sync queued |
| Subscribe while frozen | Mirror resumes — a sync is queued immediately, not at the next nightly run |
| Cancel in the portal | Access continues; "subscription ends <date>" banner appears |
| Let the cancelled period lapse | `plan` reverts, state derives to `frozen`, broker stays connected for 30 days |
| Beta user subscribes then cancels | Returns to `beta`, NOT to a lapsed trial |
| Replay a webhook (`stripe events resend`) | Second delivery is a no-op (`stripe_events`) |

## How the plan mapping works

`app/billing.py` maps Stripe subscription status onto `users.plan`:

| Stripe status | `users.plan` | Why |
| --- | --- | --- |
| `active`, `trialing` | `active` | Paying |
| `past_due`, `incomplete` | `active` | Stripe is still retrying the card — yanking the mirror mid-dunning and restoring it hours later is worse than carrying a few days of risk |
| `canceled`, `unpaid`, `incomplete_expired` | `plan_before_subscription` or `trial` | Stripe gave up |

On cancellation the trial clock is backdated to the freeze boundary, so a
churned subscriber lands in **frozen** (readable, not syncing, broker still
connected for the 30-day grace) rather than straight to disconnected. The
Stripe customer id is kept forever so a returning subscriber reuses their
saved card and invoice history.

## Going live

1. Complete Stripe's business profile / bank details and activate the account.
2. Recreate the product + both prices in **live mode** (test-mode IDs do not
   work with live keys) and create a **live** webhook endpoint.
3. Swap all four env vars on Render to the live values, then redeploy.
4. Subscribe once with a real card, confirm the Billing tab and the invoice
   email, then cancel and confirm the pending-cancellation banner.

## Related

- `app/billing.py` — checkout, portal, success reconcile, webhook
- `app/plan.py` — reverse trial derivation; `plan='active'` is the seam
- `tests/test_billing.py` — status mapping, webhook signature/idempotency,
  churn-lands-in-grace, positional-param contract
- `docs/SNAPTRADE_SETUP.md` — the other per-user cost driver
