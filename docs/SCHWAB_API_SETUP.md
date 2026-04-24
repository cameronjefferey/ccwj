# Schwab API Setup Guide

Connect HappyTrader to your Schwab account via API for **daily auto-sync** of positions and transaction history. No manual CSV uploads needed.

## Prerequisites

- Charles Schwab brokerage account
- Python 3.10+ (for schwab-py)

## Step 1: Create a Schwab Developer Account

1. Go to [Schwab Developer Portal](https://developer.schwab.com)
2. Sign up / log in (use your existing Schwab login or create a separate developer account)
3. Complete developer registration—approval typically takes 1–3 business days

## Step 2: Register Your Application

1. In the developer portal, go to **My Apps** → **Add new app**
2. Fill in:
   - **App Name**: e.g. `HappyTrader` or `My Portfolio Sync`
   - **Callback URL** (Schwab **only allows HTTPS** — plain `http://127.0.0.1` is rejected in the portal):
     - **Production:** `https://your-domain.com/schwab/callback` (e.g. `https://happytrader.me/schwab/callback`)
     - **Local testing:** use an **HTTPS tunnel** to your laptop (see [Local dev with HTTPS](#local-dev-with-https-tunnel) below). Register the tunnel URL, e.g. `https://abc123.ngrok-free.app/schwab/callback`.
3. Select API products:
   - **Accounts and Trading Production** (positions, transactions)
   - **Market Data Production** (quotes, price history)
4. Submit for approval

## Step 3: Get Your Credentials

After approval, open your app in the portal. You’ll get:

- **App Key** (Client ID)
- **App Secret** (Client Secret)

Keep these secret and never commit them to git.

## Step 4: Configure HappyTrader

Add to `.env`:

```bash
# Schwab API (optional - for Connect Schwab feature)
SCHWAB_APP_KEY=your_app_key_here
SCHWAB_APP_SECRET=your_app_secret_here
# Must match a callback URL registered in the Schwab portal (HTTPS only).
SCHWAB_CALLBACK_URL=https://your-domain.com/schwab/callback
# Optional: calendar days of transaction history to request per sync (default 60, max 1825 for UI backfill)
# SCHWAB_SYNC_TRANSACTION_DAYS=120
# Optional: max days per Schwab API request — long backfills are split into many calls (default 60; reduce if the API 400s)
# SCHWAB_TRANSACTION_CHUNK_DAYS=60
```

For **local OAuth**, set `SCHWAB_CALLBACK_URL` to the **same HTTPS URL** you registered (your tunnel URL + `/schwab/callback`), not `http://127.0.0.1`.

### Local dev with HTTPS tunnel

Schwab’s portal requires **HTTPS** for callback URLs, so you cannot register raw `http://127.0.0.1:5000/...`.

1. Run Flask locally: `flask run` (default `127.0.0.1:5000`).
2. Start a tunnel that exposes **HTTPS** to that port, for example:
   - **[ngrok](https://ngrok.com):** `ngrok http 5000` → copy the `https://....ngrok-free.app` URL (or your static ngrok domain).
   - **[Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/):** `cloudflared tunnel --url http://127.0.0.1:5000` → use the printed `https://....trycloudflare.com` URL.
3. In the Schwab developer app, add callback:  
   `https://<your-tunnel-host>/schwab/callback`  
   (path must be exactly `/schwab/callback` — same as this app’s route).
4. In `.env`, set **the same** value:  
   `SCHWAB_CALLBACK_URL=https://<your-tunnel-host>/schwab/callback`
5. Click **Connect Schwab** in the app. The browser goes to Schwab, then Schwab redirects to your **HTTPS** tunnel URL, which forwards to Flask on localhost.

**Note:** Free tunnel URLs change each time unless you pay for a reserved domain. When the host changes, update both the Schwab portal and `.env`, then connect again.

**Alternative:** Skip local OAuth entirely — connect once on **production** (`https://happytrader.me/schwab/callback`), then use **Sync now** on prod, or run `python -m app.schwab_sync_cli` on a machine that has `DATABASE_URL` and the stored token (callback not needed for CLI sync after the token exists).

## Step 5: Connect Your Account

1. In HappyTrader, go to **Settings**
2. Click **Connect Schwab**
3. You’ll be sent to Schwab to log in and authorize the app
4. After authorizing, you’re redirected back and your account is linked
5. Use **Sync now** anytime to pull the latest positions and transactions

## Step 6: Daily Auto-Sync (Optional)

To run sync automatically every day:

### Option A: Cron (Linux/macOS)

```bash
# Example: 6 PM ET after market close (cron uses server local time — set TZ or convert to UTC)
0 18 * * * cd /path/to/ccwj && .venv/bin/python -m app.schwab_sync_cli
```

### Option B: Render Cron Job

Render’s schedule is **UTC only** (see [Render cron jobs](https://render.com/docs/cronjobs)). **1:01 PM Pacific** moves between **20:01 UTC** (during PDT) and **21:01 UTC** (during PST); pick one UTC time and accept the one-hour shift, or run two jobs if you need exact local time.

**Weekdays after close (simple default):** `1 21 * * 1-5` → 21:01 UTC Mon–Fri (≈ 1:01 PM **PST** / 2:01 PM **PDT** — still after the 1:00 PM PT close).

Create a **Cron Job** service in Render with the same **env group** as your web app (at minimum `DATABASE_URL`, `SCHWAB_APP_KEY`, `SCHWAB_APP_SECRET`). Start command:

```bash
python -m app.schwab_sync_cli
```

### Option C: GitHub Actions

Create `.github/workflows/schwab-sync.yml` to run the sync on a schedule.

## Data Flow

```
Schwab API
  → current positions + transactions (lookback window; default 60 days, see SCHWAB_SYNC_TRANSACTION_DAYS)
  → merged into the SAME seeds manual upload uses: trade_history.csv and current_positions.csv (+ schwab_account_balances.csv for cash/equity snapshots)
  → if GITHUB_PAT (+ GITHUB_REPO) is set: commit to GitHub **triggers the same CI as CSV upload** (workflow `Update Daily Position Performance` in `.github/workflows/bigquery_update.yml`): dbt `build` (seeds + models) and the daily price script, so **BigQuery updates without a manual `dbt` run** (typically a few minutes; watch **GitHub → Actions**). Use branch **`master` or `main`** and set `GITHUB_BRANCH` in production to match.
  → always: also writes data/schwab_sync/{account}_*.csv on the server (local/debug; ephemeral on Render)
```

**One pipeline, two sources:** Manual upload and Schwab sync are different front doors into the same `trade_history.csv` + `current_positions.csv` seeds, so everything downstream (stg_history, stg_current, int_*, marts) “just works” without per-source branches. Configure the same `GITHUB_PAT`, `GITHUB_REPO`, and `GITHUB_BRANCH` as for CSV uploads. If GitHub is not configured, sync still runs locally and writes under `data/schwab_sync/` (copy into seeds yourself if needed).

**Account names:** Sync labels the account as the Schwab nickname if the API returns one, otherwise `Schwab ••••<last4>`. Per-account merge semantics mean you should not use the same label for both manual uploads and sync unless you truly want them concatenated — rename one if they describe different things.

## Limits and Notes

- **Option cost basis in seeds:** Schwab’s `averagePrice` on options is usually premium **per underlying share** (not total premium). Sync writes `cost_basis` as API `costBasis` when present, otherwise `averagePrice × |quantity| × instrument multiplier` (defaults to **100** for standard US equity options). Using only `averagePrice × quantity` understates cost and makes return % look absurdly high.
- **Transaction history — first sync vs. routine:** In **Profile → Accounts & login**, the **first** time you use **Sync now**, you can choose a **full backfill (up to 1825 days, ~5 years)** or a **short rolling window only** (faster if you already have history elsewhere). After a successful sync, the UI defaults to the **rolling** mode so daily refreshes do not re-pull years of data; you can still check **one-time long backfill** when you need it. The rolling window size comes from **`SCHWAB_SYNC_TRANSACTION_DAYS`** in `.env` (default **60**, max 1825) and is what the **scheduled cron/CLI** sync uses. The Schwab **transactions** endpoint only accepts a **short date range per HTTP request**; the app automatically pages through history in **chunks** of **`SCHWAB_TRANSACTION_CHUNK_DAYS`** (default **60**). If you see **400 Bad Request** on sync, try lowering `SCHWAB_TRANSACTION_CHUNK_DAYS` to **30** or **7**. For gaps Schwab will not return via API, use CSV upload. New rows are merged with dedupe on the transaction columns (not a blind append of identical trades).
- **Price history**: Stocks/ETFs supported; **options** require capturing daily quotes and storing them yourself (which we do during sync).
- **Rate limits**: Stay under ~120 requests/minute; sync logic batches and throttles where needed.
- **Tokens**: Access tokens expire after ~7 days. Refresh tokens are used automatically when possible; re-authorize if refresh fails.

## Troubleshooting

| Issue | Fix |
|-------|-----|
| **Sync now** / CLI fails with **400** on `/transactions?...` | The requested **date range is too long for one API call** (or Schwab dislikes the bounds). The app should chunk automatically; if it still 400s, set **`SCHWAB_TRANSACTION_CHUNK_DAYS=30`** (or `7`) in `.env` and sync again. |
| Portal says **"URL must be HTTPS"** for `http://127.0.0.1` | Schwab no longer allows `http` callbacks. Use an HTTPS tunnel (ngrok / cloudflared) or connect only on production. |
| "Invalid callback URL" | Ensure `SCHWAB_CALLBACK_URL` in .env matches exactly what you registered in the Schwab portal |
| "App not approved" | Wait for approval or contact traderapi@schwab.com |
| Token expired | Click **Connect Schwab** again to re-authorize |
| No positions returned | Verify the Schwab account has positions and you selected the right account |
| Sync says success but **BigQuery** still old | `GITHUB_PAT` must be able to push; repo **Actions** must be enabled; workflow must run on your branch (use `GITHUB_BRANCH=master` or `main` to match `.github/workflows/bigquery_update.yml`); the workflow needs `DBT_KEYFILE_JSON` and related secrets. Check the latest **Update Daily Position Performance** run for errors. |
