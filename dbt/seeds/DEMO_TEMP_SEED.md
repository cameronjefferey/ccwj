# Temporary demo book

## How to turn it off

Set the GitHub Actions repository variable `DEMO_TEMP_SEED` to `false`, then re-run **Update Daily Position Performance** (`.github/workflows/bigquery_update.yml`).

That is the whole switch. No code edit. The variable is passed into the warehouse job as `DEMO_TEMP_SEED` and becomes the dbt var `demo_temp_seed`. Render does not run dbt, so a Render env var does not change the demo.

After that rebuild, Demo Account is the bot mirror only: zero seed rows and zero seed equity. The CSV files can stay in the repo; nothing selects them while the flag is off.

Where to flip it: GitHub → Settings → Secrets and variables → Actions → Variables → `DEMO_TEMP_SEED`. Unset or `true` adds the seed. `false` removes it on the next rebuild. The nightly run (04:30 UTC) is the backstop if you do not re-run the workflow yourself.

## What this is

While the flag is on, Demo Account (`demo:demo-account`) is **every bot-mirror trade plus** the made-up book in `demo_temp_history.csv` and `demo_temp_current.csv`. The seed does not replace the mirror. Real customers are not in these files.

The account curve is the bot's balance history **plus** the seed book's full account value (cash, including the $100,000 deposit, plus share and option marks). That add is `int_demo_temp_equity_daily`, joined only for `demo:demo-account`.

- Opening deposit of $100,000 on 2026-01-02, then a rising seed book.
- **Covered calls** on AAPL (200 shares, short calls that expired above the high, one call still open).
- **A wheel** on KO (cash-secured put assigned, covered calls, shares called away at a gain).
- **Call spreads** on MSFT and SPY (credit spreads that expired out of the money, plus one MSFT debit spread closed for a gain).
- **A put spread** on MSFT that is still open and ahead.

Spreads are two legs, same underlying and expiry, different strikes, opened the same day, so HappyTrader classifies each pair as one spread. The wheel's put assignment and the share buy are the same day. AAPL short calls are not paired with a long call, so they stay covered calls.

Open AAPL shares follow the public close after the snapshot price in the current file (333.63 on 2026-10-06).

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
