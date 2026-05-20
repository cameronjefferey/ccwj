{# 
    Invariant: every account in the post-staging warehouse has AT MOST
    ONE non-NULL user_id, AND if any populated user_id exists for an
    account, no NULL-user_id rows should remain for that same account.

    Why: positions_summary, int_strategy_classification, mart_daily_pnl,
    int_dividend_events, etc. all partition by (account, user_id).
    The same physical broker account ending up under multiple tenant
    keys produces:
      (A) NULL + populated split → fully-closed positions read $0 P&L
          while the chart shows the real number (May 2026 / JEPI on
          Schwab ••••0044 — $2,560 chart vs $0 mart gap).
      (B) Stale-uid + canonical-uid split → page surfaces double every
          leg, every closed trade, every strategy row, every breakdown
          (May 2026 / Cameron Investment / PLTR — uid=9 and uid=13
          each contributed a -$4,800 unrealized so the hero rendered
          -$9,878 on a ~-$4,800 position).

    The orphan-tenant backfill in stg_history / stg_current /
    stg_account_balances (combined with stg_canonical_account_owner)
    is supposed to eliminate BOTH cases. This test catches any
    regression that re-introduces either gap (a new staging model
    that forgets the backfill, a malformed seed, a canonical-resolver
    bug, etc.).

    Allowed exception:
      - Accounts with NULL on every row (truly orphaned, no linking
        info available anywhere) — the backfill correctly refuses;
        these are invisible to all real users because tenant
        filtering on `_user_account_list()` excludes them.

    Failure modes flagged by this test:
      * `null_and_populated`: same account has both NULL AND
        non-NULL user_ids (case A regression).
      * `multiple_populated`: same account has TWO OR MORE distinct
        non-NULL user_ids (case B regression — should be impossible
        once the canonical-owner consolidator runs).
#}

with stg_history_check as (
    select
        'stg_history' as source,
        account,
        countif(user_id is null)            as null_rows,
        countif(user_id is not null)        as populated_rows,
        count(distinct user_id)             as distinct_populated_uids
    from {{ ref('stg_history') }}
    group by account
),
stg_current_check as (
    select
        'stg_current' as source,
        account,
        countif(user_id is null)            as null_rows,
        countif(user_id is not null)        as populated_rows,
        count(distinct user_id)             as distinct_populated_uids
    from {{ ref('stg_current') }}
    group by account
),
stg_account_balances_check as (
    select
        'stg_account_balances' as source,
        account,
        countif(user_id is null)            as null_rows,
        countif(user_id is not null)        as populated_rows,
        count(distinct user_id)             as distinct_populated_uids
    from {{ ref('stg_account_balances') }}
    group by account
),

unioned as (
    select * from stg_history_check
    union all select * from stg_current_check
    union all select * from stg_account_balances_check
)

select
    source, account, null_rows, populated_rows, distinct_populated_uids,
    case
        when null_rows > 0 and populated_rows > 0   then 'null_and_populated'
        when distinct_populated_uids > 1            then 'multiple_populated'
    end as failure_kind
from unioned
where (null_rows > 0 and populated_rows > 0)
   or distinct_populated_uids > 1
