{{
    config(
        materialized='view'
    )
}}

/*
    Canonical-uid wrapper over `snapshot_options_market_values_daily`.

    The dbt snapshot accumulates historical rows with whatever
    `user_id` was stamped on the source row at capture time. After
    the canonical-owner consolidation in May 2026 (see
    `stg_canonical_account_owner`), the SAME physical account can
    have a mix of historical rows under stale uids and current rows
    under the canonical uid. Any downstream consumer that joins the
    snapshot to a canonical-uid model (e.g. `int_option_contracts`
    post-consolidation) loses every historical MTM row.

    Real example (May 2026 / Cameron Investment / PLTR
    270115C00120000):
      - Snapshot rows for 5/7 - 5/14 stamped uid=9 (pre-fix)
      - Today's snapshot rows stamped uid=13 (post-fix, canonical)
      - `int_option_contracts` row stamped uid=13 (canonical)
      - `int_option_contract_daily_pnl` joined on uid → empty on
        every historical day → `pnl_today = 0` until today, then
        the full $4,760 unrealized loss appears in one cliff jump.

    This view rewrites every snapshot row's `user_id` to the
    canonical owner for its account, then dedupes across the
    rewritten uid so multi-uid snapshots on the same date collapse
    to one (broker snapshot ran once before the canonical fix, once
    after → two rows on 5/15 — we keep the latest `dbt_valid_from`,
    matching the pre-existing dedupe in `int_option_contract_daily_pnl.snapshots`).

    Consumers should read from THIS view, not the raw snapshot.
    The raw snapshot is preserved (do not modify) so the historical
    audit trail of "what the broker reported, when, stamped how"
    survives.
*/

with raw as (
    select * from {{ ref('snapshot_options_market_values_daily') }}
),

normalized as (
    select
        r.* except(user_id),
        coalesce(co.canonical_user_id, r.user_id) as user_id
    from raw r
    left join {{ ref('stg_canonical_account_owner') }} co
        on r.account = co.account
),

-- Multi-uid collision on the SAME date for the SAME account+contract:
-- after the rewrite, two rows from different stale stamps become
-- byte-near-identical (market_value can differ by intra-day sync
-- runs). Keep the latest `dbt_valid_from` — the same tie-break the
-- pre-existing consumer dedupe (`int_option_contract_daily_pnl.snapshots`
-- with row_number over dbt_valid_from desc) uses, so behavior is
-- preserved for consumers that read this view instead of the raw.
deduped as (
    select *
    from normalized
    qualify row_number() over (
        partition by account, user_id, trade_symbol, snapshot_date
        order by dbt_valid_from desc
    ) = 1
)

select * from deduped
