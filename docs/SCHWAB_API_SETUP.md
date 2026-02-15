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
   - **Callback URL**: 
     - Local: `http://127.0.0.1:5000/schwab/callback`
     - Production: `https://your-domain.com/schwab/callback`
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
SCHWAB_CALLBACK_URL=http://127.0.0.1:5000/schwab/callback
```

For production, set:

```bash
SCHWAB_CALLBACK_URL=https://your-domain.com/schwab/callback
```

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
# Sync at 6 PM ET after market close
0 18 * * * cd /path/to/ccwj && .venv/bin/python -m app.schwab_sync
```

### Option B: Render Cron Job

If deployed on Render, add a cron job that runs:

```bash
python -m app.schwab_sync
```

### Option C: GitHub Actions

Create `.github/workflows/schwab-sync.yml` to run the sync on a schedule.

## Data Flow

```
Schwab API
  → positions + transactions (last 60 days)
  → mapped to our CSV schema
  → written to data/schwab_sync/{account}_history.csv and _current.csv
  → (optional) merge into dbt seeds or load to BigQuery
  → dbt build → positions_summary
```

Sync output is written to `data/schwab_sync/`. To use it:

1. **Manual**: Copy the CSVs into `dbt/seeds/` (or merge with existing seeds), then run `dbt seed && dbt build`.
2. **Automated**: Extend the sync to push to GitHub (like the upload flow) or write directly to BigQuery.

## Limits and Notes

- **Transaction history**: Schwab API returns only the **last 60 days**. For older history, use CSV upload.
- **Price history**: Stocks/ETFs supported; **options** require capturing daily quotes and storing them yourself (which we do during sync).
- **Rate limits**: Stay under ~120 requests/minute; sync logic batches and throttles where needed.
- **Tokens**: Access tokens expire after ~7 days. Refresh tokens are used automatically when possible; re-authorize if refresh fails.

## Troubleshooting

| Issue | Fix |
|-------|-----|
| "Invalid callback URL" | Ensure `SCHWAB_CALLBACK_URL` in .env matches exactly what you registered in the Schwab portal |
| "App not approved" | Wait for approval or contact traderapi@schwab.com |
| Token expired | Click **Connect Schwab** again to re-authorize |
| No positions returned | Verify the Schwab account has positions and you selected the right account |
