# Temporary demo book

## How to turn it off

Set the GitHub Actions repository variable `DEMO_TEMP_SEED` to `false`, then re-run **Update Daily Position Performance** (`.github/workflows/bigquery_update.yml`).

That is the whole switch. No code edit. The variable is passed into the warehouse job as `DEMO_TEMP_SEED` and becomes the dbt var `demo_temp_seed`. Render does not run dbt, so a Render env var does not change the demo.

After that rebuild, Demo Account is the bot mirror only: zero seed rows and zero seed equity. The CSV files can stay in the repo; nothing selects them while the flag is off.

Where to flip it: GitHub → Settings → Secrets and variables → Actions → Variables → `DEMO_TEMP_SEED`. Unset or `true` adds the seed. `false` removes it on the next rebuild. The nightly run (04:30 UTC) is the backstop if you do not re-run the workflow yourself.

## What this is

While the flag is on, Demo Account (`demo:demo-account`) is **every bot-mirror trade plus** the made-up book in `demo_temp_history.csv` and `demo_temp_current.csv`. The seed does not replace the mirror. Real customers are not in these files.

The account curve is the bot's balance history **plus** the seed book's full account value (cash, including the $100,000 deposit, plus share and option marks). That add is `int_demo_temp_equity_daily`, joined only for `demo:demo-account`.

- Opening deposit of $100,000 on 2026-01-02. That is the only cash transfer. Option credits are trading P&L, not deposits.
- From January through early August the seed sells small 1-lot credit spreads (about $20–$55 of premium each). That raises the win count without parking the gains before the demo's broker connect date.
- **The climb starts 2026-08-10**, two days after the demo connect date (2026-08-08). Share buys, covered calls, the wheel, and the larger credit spreads are all on or after that date, so the equity curve and the cumulative P&L chart both show them.
- **Covered calls** on AAPL (100 shares bought at the 2026-08-10 close) and AMD (100 shares, same day). Short calls expired above that week's high. One call on each name is still open, expiring 2026-10-16.
- **A wheel** on KO in the same window: an 88 put sold 2026-08-10 and assigned 2026-08-14, then a 90 call sold 2026-08-17 and assigned 2026-08-21. The shares were called away at a gain.
- **Credit spreads** on SPY, QQQ, MSFT, NVDA, AMZN, META, GOOGL, and JPM. Call spreads expire the week they are opened. Put spreads expire two weeks later, so the two sides do not share an expiry and do not fuse into an iron condor. A few are closed early for a small loss. Four spreads are still open, expiring 2026-10-16.
- **One long call** on NVDA (opened 2026-08-24, closed 2026-09-04) and **one long put** on SPY that expired worthless on 2026-08-13. The put uses a Thursday expiry so it does not pair with a Friday SPY put spread.

Spreads are two legs, same underlying and expiry, different strikes, opened the same day, so HappyTrader classifies each pair as one spread. The wheel's put assignment and the share buy are the same day. AAPL and AMD short calls are not paired with a long call, so they stay covered calls.

Open AAPL and AMD shares in the current file are marked at the 2026-10-07 close (AAPL 336.67, AMD 645.86). The account curve keeps following the public close after that snapshot.

## How to delete the files

Turning the flag off is enough. Delete the files only when the seed should not be turnable back on.

1. Set `DEMO_TEMP_SEED` to `false` (same as `demo_temp_seed: false`) and let one warehouse build finish.
2. Delete these files:
   - `dbt/seeds/demo_temp_history.csv`
   - `dbt/seeds/demo_temp_current.csv`
   - `dbt/seeds/demo_temp_seed.yml`
   - `dbt/seeds/DEMO_TEMP_SEED.md` (this file)
   - `dbt/models/intermediate/int_demo_temp_equity_daily.sql`
   - `dbt/tests/demo_temp_equity_climbs.sql`
   - `dbt/tests/demo_temp_history_keeps_mirror.sql`
   - `dbt/tests/demo_temp_current_keeps_mirror.sql`
   - `dbt/tests/demo_temp_off_equals_mirror.sql`
   - `tests/test_demo_temp_seed.py`
   - `dbt/macros/demo_temp_seed_on.sql`
3. Remove the `{% if demo_temp_seed_on() %}` branches in
   `dbt/models/staging/demo/stg_demo_{history,current,balances}.sql` and
   `dbt/models/marts/mart_account_equity_daily.sql`, and the
   `DEMO_TEMP_SEED` env line in `.github/workflows/bigquery_update.yml`.
