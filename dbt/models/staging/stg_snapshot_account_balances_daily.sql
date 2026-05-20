{{
    config(
        materialized='view'
    )
}}

/*
    Canonical-uid wrapper over `snapshot_account_balances_daily`.

    Same pattern as `stg_snapshot_options_market_values_daily`. The dbt
    snapshot accumulates historical account-total / cash rows under
    whatever `user_id` was stamped at capture time. After the
    canonical-owner consolidation (`stg_canonical_account_owner`),
    historical rows can point at stale uids; downstream models that
    aggregate by (account, user_id) — `mart_account_equity_daily`,
    `mart_account_snapshots_enriched` — fragment the same account
    across two parallel series, breaking the day-over-day account
    value delta.

    Real example (May 2026 / Cameron Investment):
      - May 7 - May 14 account_total snapshots stamped uid=9
      - May 15 onwards stamped uid=13 (post-canonical fix)
      - `mart_account_equity_daily` keeps uid=9 series ending May 14
        and uid=13 series starting May 15
      - `mart_account_snapshots_enriched.delta_1d` for May 15 looks up
        the prior day under (account=Cameron Investment, user_id=13)
        and finds NOTHING → delta = NULL, calendar shows blank on the
        first day a user reports across the consolidation boundary

    This wrapper rewrites every snapshot row's `user_id` to the
    canonical owner. Multiple captures on the same day across stale
    uids collapse to a single canonical-uid series with the latest
    dbt_valid_from winning (same tie-break the consumers' own
    dedupes use).

    Consumers must read from THIS view, not the raw snapshot.
*/

with raw as (
    select * from {{ ref('snapshot_account_balances_daily') }}
),

normalized as (
    select
        r.* except(user_id),
        coalesce(co.canonical_user_id, r.user_id) as user_id
    from raw r
    left join {{ ref('stg_canonical_account_owner') }} co
        on r.account = co.account
),

-- Multi-uid collision on the SAME (account, row_type, date): after the
-- canonical rewrite, two rows from different stale stamps collapse to
-- one canonical uid. Keep the latest `dbt_valid_from` — matches the
-- dedupe in `mart_account_equity_daily.latest_per_day`.
deduped as (
    select *
    from normalized
    qualify row_number() over (
        partition by account, user_id, row_type, snapshot_date
        order by dbt_valid_from desc
    ) = 1
)

select * from deduped
