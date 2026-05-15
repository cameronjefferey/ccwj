/*
    snapshot_account_balances_daily must have AT MOST ONE current row
    (``dbt_valid_to IS NULL``) per ``(account, user_id, row_type)``.

    Companion guard to ``snapshot_options_market_values_current_unique``
    — same failure mode, same root cause: a buggy seed merge can write
    duplicate rows into the seed CSV; dbt's first snapshot run then
    inserts byte-identical duplicates with the same ``dbt_scd_id``;
    every later run fails its MERGE step with
    ``UPDATE/MERGE must match at most one source row for each target row``.

    Catches target-side poisoning at build time so we never have to
    debug it from a cryptic MERGE error in the next nightly job.
    Recovery procedure: see ``broker-sync-safety`` skill register entry
    "snapshot target-side triple-write poisoning".
*/

select
    account,
    user_id,
    row_type,
    count(*) as cnt
from {{ ref('snapshot_account_balances_daily') }}
where dbt_valid_to is null
group by 1, 2, 3
having count(*) > 1
