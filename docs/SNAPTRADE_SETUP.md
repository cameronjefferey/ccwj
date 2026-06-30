# SnapTrade (multi-broker) Setup Guide

Connect HappyTrader to ~20 brokerages (Schwab, Fidelity, Vanguard, Robinhood,
IBKR, Tradier, etc.) via the [SnapTrade](https://snaptrade.com) aggregator.
**v2 architecture:** all broker OAuth flows go through SnapTrade — there is no
native Schwab connector.

## Prerequisites

- A SnapTrade developer account ([sign up](https://dashboard.snaptrade.com/signup))
- Your `clientId` and `consumerKey` from the SnapTrade dashboard
- Python 3.10+ (for the `snaptrade-python-sdk` package, already pinned in `requirements.txt`)

## Step 1 — Create a SnapTrade developer account

1. Go to [dashboard.snaptrade.com/signup](https://dashboard.snaptrade.com/signup) and create an account.
2. Verify your email and complete the onboarding checklist.
3. Pricing: SnapTrade charges per **connected user**, not per request.
   The first sandbox is free; production accounts have a per-user fee.
   Confirm the current plan on [snaptrade.com/pricing](https://snaptrade.com/pricing)
   before opening this up to non-beta users.

## Step 2 — Get your credentials

In the SnapTrade dashboard:

- **Client ID** — public identifier for your app.
- **Consumer Key** — secret used to authenticate API calls.
- **Webhook Secret** (optional, Phase 3) — for event-driven syncs.

Both Client ID and Consumer Key are bearer credentials. Treat them
like database passwords; never commit them to git.

## Step 3 — Configure HappyTrader

Add to `.env` (local) or set as environment variables on Render:

```bash
# SnapTrade aggregator (covers Fidelity, Vanguard, Robinhood, IBKR,
# Tradier, etc.). Optional — feature is hidden when not configured.
SNAPTRADE_CLIENT_ID=your-client-id
SNAPTRADE_CONSUMER_KEY=your-consumer-key

# OPTIONAL — where SnapTrade returns the user after the Connection
# Portal flow. Unlike Schwab, SnapTrade does NOT require a pre-registered
# allow-list of redirect URIs; we pass this value per-session via the
# ``customRedirect`` parameter of ``login_snap_trade_user`` and SnapTrade
# honors whatever we send. If unset, the code falls back to Flask's
# ``url_for("snaptrade_callback", _external=True)`` which auto-builds
# the right URL from the request host (works locally and in prod).
# Set this only if you want to override the host (e.g. point to a
# different domain than the request hostname).
SNAPTRADE_REDIRECT_URI=https://your-domain.com/snaptrade/callback
```

Unlike Schwab, you do **not** need to register this URL anywhere in the
SnapTrade dashboard — there is no "allowed redirect URIs" list. SnapTrade
stores ONE default redirect URI per Client ID (visible via the Get Client
Info endpoint) which is used only as a fallback when `customRedirect`
isn't passed; our code always passes `customRedirect`, so the dashboard
default is effectively bypassed.

For local development, just leave `SNAPTRADE_REDIRECT_URI` unset and
visit the app over `http://127.0.0.1:5000`. SnapTrade's Connection
Portal accepts non-HTTPS localhost URIs for development.

## Step 4 — Verify the integration

1. Restart the app so it picks up the new env vars.
2. Sign in as any user (the demo user blocks SnapTrade writes).
3. Visit `/profile?tab=account` and look for the **More brokerages**
   card with a **Connect another broker** button.
4. Click through the SnapTrade Connection Portal in sandbox mode and
   pick the SnapTrade demo broker.
5. After the redirect, `/snaptrade/accounts` should list the demo
   account. Click **Sync now** — this asks SnapTrade to repoll the
   broker for fresh data first (a sync that only re-reads SnapTrade's
   cache is pointless), then commits the result to GitHub (just like a
   Schwab sync), which triggers the dbt rebuild and feeds Position
   Detail / Daily Review like any other tenant. If nothing changed
   since the last sync, the commit (and the dbt build) is skipped — the
   broker repoll is rate-limited to ~once per 10 min per brokerage so
   rapid clicks don't incur extra SnapTrade billing. The standalone
   "Refresh from broker" button was retired because Sync now now does
   it by default; the daily cron still reads SnapTrade's cache (it
   auto-refreshes nightly) and relies on the holdings-freshness
   backstop to flag stalled connections.

## Step 5 — Enable event-driven syncs (webhook, recommended)

SnapTrade fires an **`ACCOUNT_HOLDINGS_UPDATED`** webhook the moment its own
daily sync finishes pulling fresh holdings for an account from the broker. That
is the authoritative "SnapTrade is updated" signal — HappyTrader listens for it
and immediately runs our sync for that account (read SnapTrade's now-fresh data
→ merge → push seeds). This is the "once SnapTrade completes, kick off
HappyTrader" flow: it costs **zero** billed API calls (no forced refresh) and
keeps the "Broker data as of" strip honest without any polling.

To enable it: in the SnapTrade dashboard → **Webhooks**, set the listener URL to
`https://<your-domain>/webhooks/snaptrade`. That's it — there is **no secret to
configure**.

**Authentication.** SnapTrade **deprecated webhook secrets**. Every delivery now
carries a `Signature` header = `base64(HMAC-SHA256(canonical-json-body, key =
your consumer key))`, where the canonical body is
`json.dumps(payload, separators=(",", ":"), sort_keys=True)`. The handler
(`app/webhooks.py` → `snaptrade_webhook`) recomputes that HMAC with
`SNAPTRADE_CONSUMER_KEY` (already set for the API) and rejects mismatches with
`401`. No `SNAPTRADE_WEBHOOK_SECRET` is needed.

On a verified `ACCOUNT_HOLDINGS_UPDATED`, the handler maps the SnapTrade `userId`
back to a HappyTrader user and runs `_sync_one_connection(..., force_refresh=False)`
in a background thread serialized by a cluster-wide Postgres advisory lock (a
burst of per-account webhooks must push the shared seed CSVs one-at-a-time). If
`SNAPTRADE_CONSUMER_KEY` is unset the endpoint logs a warning and skips
verification — acceptable for local dev only.

## Architecture notes

- **No new seed CSVs.** SnapTrade writes to the same `trade_history.csv`,
  `current_positions.csv`, and `account_balances.csv` files Schwab and
  manual upload write to. The convergence point is
  `app.upload.merge_and_push_seeds`.
- **Tenant scoping.** Every SnapTrade-emitted DataFrame stamps
  `account_name` and `user_id` on every row, exactly like Schwab. The
  broker-sync-safety invariants (dedup, monotonic merge, canonical
  uid) all apply automatically because they live in the merge
  function, not in the connector.
- **Data isolation.** SnapTrade userId/userSecret pairs are stored in
  Postgres `snaptrade_connections` (one row per HappyTrader user) and
  per-broker accounts in `snaptrade_accounts` (mirrors the
  one-row-per-account grain of `schwab_connections`).
- **Sync trigger.** Primary path is the `ACCOUNT_HOLDINGS_UPDATED`
  webhook (Step 5) — event-driven, fires when SnapTrade finishes its
  broker poll (so the data it reads is fresh). The `happytrader-snaptrade-sync`
  Render cron (manually managed in the dashboard) is a **fresh daily
  backstop** for days a webhook delivery is missed; it does not force a
  refresh and is not the freshness driver. It runs at **23:00 UTC weekdays**,
  AFTER SnapTrade's daily broker refresh completes (observed ~20:40–22:10 UTC,
  ≈1h later under EST). The original 20:06 UTC schedule fired *before* that
  refresh and always read day-old data — do not move it earlier.

## Limitations

- **History depth varies by broker.** Schwab via SnapTrade can give
  multi-year history; some brokers (e.g. Robinhood) only return
  ~90 days. Don't migrate existing Schwab users away from the native
  connector — they'd lose deep history.
- **Action vocabulary is incremental.** SnapTrade ships a normalized
  activity feed, but every broker has quirks (Vanguard cash sweeps,
  Robinhood crypto, etc). The first sync from a new broker may
  surface activity types we haven't mapped — they're logged as
  warnings and skipped. Add new entries to
  `SNAPTRADE_ACTIVITY_TO_ACTION` in `app/snaptrade_normalize.py` as
  needed.
- **Vendor risk.** A SnapTrade outage takes down all SnapTrade-
  connected users until they recover. Native Schwab is unaffected.
  This hybrid posture is the mitigation.

## Troubleshooting

**`Multi-broker connect is not configured`** — `SNAPTRADE_CLIENT_ID`
or `SNAPTRADE_CONSUMER_KEY` is empty. Re-check `.env` (or the Render
service config), restart the app.

**The SDK is missing** — `snaptrade_enabled()` short-circuits to
False when `import snaptrade_client` fails. `pip install -r
requirements.txt` to install.

**Reconnect banner stuck after re-auth** — `connection_broken_at` is
cleared on the next successful sync. If a SnapTrade callback didn't
clear it (e.g. callback failed silently), trigger a manual sync via
`/snaptrade/sync`.
