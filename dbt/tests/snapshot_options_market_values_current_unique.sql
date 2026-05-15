/*
    snapshot_options_market_values_daily must have AT MOST ONE current
    row (``dbt_valid_to IS NULL``) per ``(account, user_id, trade_symbol)``.

    Why this test exists — production failure on 2026-05-08:
      ``UPDATE/MERGE must match at most one source row for each target row``

    The Schwab "Pull full history" sync wrote the same payload three
    times into ``current_positions.csv`` before the merge logic was
    hardened (see commit 7d90c88). dbt's snapshot then inserted three
    byte-identical rows for every option contract owned by the affected
    user. The CSV was repaired and ``stg_current``'s QUALIFY dedup added,
    but the snapshot table stayed poisoned with duplicate currents — and
    the very next ``dbt snapshot`` run failed at the MERGE step.

    A unique-on-current test would have caught this on the very next
    build (failed test, easy to read) instead of on the run after that
    (cryptic BigQuery MERGE error and a broken nightly job).

    Recovery procedure for any future failure: see the
    ``broker-sync-safety`` skill register entry "snapshot target-side
    triple-write poisoning".
*/

select
    account,
    user_id,
    trade_symbol,
    count(*) as cnt
from {{ ref('snapshot_options_market_values_daily') }}
where dbt_valid_to is null
group by 1, 2, 3
having count(*) > 1
