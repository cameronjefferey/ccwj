# Temporary demo book

The public demo (`demo:demo-account` / Demo Account) is temporarily a
**made-up book** so Overview, Positions, and the account chart show a
healthy account that grows over time. The EarningsFollower trading bot
mirror is still in the repo. It is not what visitors see while this seed
is on.

This is temporary. When that bot's results are worth showing, delete this
seed and the mirror comes back. Real customers are not in these files.

## What visitors see

- Opening deposit of $100,000 on 2026-01-02, then a rising account.
- **Covered calls** on AAPL (200 shares, short calls that expired above
  the high, one call still open).
- **A wheel** on KO (cash-secured put assigned, covered calls, shares
  called away at a gain).
- **Call spreads** on MSFT and SPY (credit spreads that expired
  out of the money, plus one MSFT debit spread closed for a gain).
- **A put spread** on MSFT that is still open and ahead.

Spreads are two legs, same underlying and expiry, different strikes,
opened the same day, so HappyTrader classifies each pair as one spread.
The wheel's put assignment and the share buy are the same day. AAPL
short calls are not paired with a long call, so they stay covered calls.

The daily account curve is `int_demo_temp_equity_daily`: cash from these
fills, shares marked at the public close, short-option premium decaying
toward expiry. It is not the bot's balance snapshot.

## How to delete it

Do this in one change, then let the next warehouse build run.

1. In `dbt/dbt_project.yml`, set `demo_temp_seed: false`.
2. Delete these files:
   - `dbt/seeds/demo_temp_history.csv`
   - `dbt/seeds/demo_temp_current.csv`
   - `dbt/seeds/demo_temp_seed.yml` (BigQuery column types for the two CSVs)
   - `dbt/seeds/DEMO_TEMP_SEED.md` (this file)
   - `dbt/models/intermediate/int_demo_temp_equity_daily.sql`
   - `dbt/tests/demo_temp_equity_climbs.sql`
   - `tests/test_demo_temp_seed.py`
3. The `{% if var('demo_temp_seed') %}` branches in
   `dbt/models/staging/demo/stg_demo_{history,current,balances}.sql`,
   `dbt/models/marts/mart_account_equity_daily.sql`, and
   `dbt/models/intermediate/int_option_marks_daily.sql` already fall
   back to the bot mirror when the var is false. Leave those branches
   in place so the flag is the switch.

After the next `dbt build`, Demo Account is the bot mirror again
(`var('demo_source_tenant_id')`). No real tenant is rewritten: the seed
is stamped `demo:demo-account` only at the demo models.
