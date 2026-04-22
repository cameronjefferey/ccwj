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
# Optional: days of transactions per sync (default 60)
# SCHWAB_SYNC_TRANSACTION_DAYS=120
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
  → merged into schwab_*.csv seeds (native API columns); dbt unions with manual export seeds
  → if GITHUB_PAT (+ GITHUB_REPO) is set: commit to GitHub **triggers the same CI as CSV upload** (workflow `Update Daily Position Performance` in `.github/workflows/bigquery_update.yml`): dbt `build` (seeds + models) and the daily price script, so **BigQuery updates without a manual `dbt` run** (typically a few minutes; watch **GitHub → Actions**). Use branch **`master` or `main`** and set `GITHUB_BRANCH` in production to match.
  → always: also writes data/schwab_sync/{account}_*.csv on the server (local/debug; ephemeral on Render)
```

**Unified pipeline with manual upload:** Configure the same `GITHUB_PAT`, `GITHUB_REPO`, and `GITHUB_BRANCH` as for CSV uploads. Schwab sync updates `schwab_open_positions.csv`, `schwab_account_balances.csv`, and `schwab_transactions.csv` (not the manual `current_positions.csv` / `trade_history.csv` files). If GitHub is not configured, sync still runs but only writes under `data/schwab_sync/` (copy into seeds yourself if needed).

**Account names:** The linked Schwab account name from the API should match how you want that account labeled in seeds/BigQuery. If you previously used manual upload under a different label, align the name or merge carefully. Avoid using the **same account label** in both manual export seeds (`current_positions.csv`) and Schwab seeds for one brokerage account, or dbt’s union can **double-count** positions.

## Limits and Notes

- **Option cost basis in seeds:** Schwab’s `averagePrice` on options is usually premium **per underlying share** (not total premium). Sync writes `cost_basis` as API `costBasis` when present, otherwise `averagePrice × |quantity| × instrument multiplier` (defaults to **100** for standard US equity options). Using only `averagePrice × quantity` understates cost and makes return % look absurdly high.
- **Transaction history — first sync vs. routine:** In **Profile → Accounts & login**, the **first** time you use **Sync now**, you can choose a **full backfill (up to 1825 days, ~5 years)** or a **short rolling window only** (faster if you already have history elsewhere). After a successful sync, the UI defaults to the **rolling** mode so daily refreshes do not re-pull years of data; you can still check **one-time long backfill** when you need it. The rolling window size comes from **`SCHWAB_SYNC_TRANSACTION_DAYS`** in `.env` (default **60**, max 1825) and is what the **scheduled cron/CLI** sync uses. Schwab may still cap what the API returns—use CSV upload for the deepest history if needed. New rows are merged with dedupe on the transaction columns (not a blind append of identical trades).
- **Price history**: Stocks/ETFs supported; **options** require capturing daily quotes and storing them yourself (which we do during sync).
- **Rate limits**: Stay under ~120 requests/minute; sync logic batches and throttles where needed.
- **Tokens**: Access tokens expire after ~7 days. Refresh tokens are used automatically when possible; re-authorize if refresh fails.

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Portal says **"URL must be HTTPS"** for `http://127.0.0.1` | Schwab no longer allows `http` callbacks. Use an HTTPS tunnel (ngrok / cloudflared) or connect only on production. |
| "Invalid callback URL" | Ensure `SCHWAB_CALLBACK_URL` in .env matches exactly what you registered in the Schwab portal |
| "App not approved" | Wait for approval or contact traderapi@schwab.com |
| Token expired | Click **Connect Schwab** again to re-authorize |
| No positions returned | Verify the Schwab account has positions and you selected the right account |
| Sync says success but **BigQuery** still old | `GITHUB_PAT` must be able to push; repo **Actions** must be enabled; workflow must run on your branch (use `GITHUB_BRANCH=master` or `main` to match `.github/workflows/bigquery_update.yml`); the workflow needs `DBT_KEYFILE_JSON` and related secrets. Check the latest **Update Daily Position Performance** run for errors. |
