{# 
    Invariant: case-A orphan tenancy (NULL + populated user_id mixed
    on the same account) is never allowed.

    Why: positions_summary, int_strategy_classification, mart_daily_pnl,
    int_dividend_events, etc. all partition by (account, user_id).
    The same physical broker account ending up under (account, NULL)
    AND (account, populated) splits in a way that produces:
      (A) NULL + populated split → fully-closed positions read $0 P&L
          while the chart shows the real number (May 2026 / JEPI on
          Schwab ••••0044 — $2,560 chart vs $0 mart gap).

    The case-A backfill in stg_history / stg_current /
    stg_account_balances eliminates this. This test catches any
    regression that re-introduces a NULL+populated split (a new
    staging model that forgets the backfill, a malformed seed, etc.).

    NOT a regression (post May 2026 guard):
      * Same account has TWO OR MORE distinct non-NULL user_ids,
        EACH with recent activity. This is the "concurrent tenants
        sharing an account label" case — two family members linked
        to the same brokerage, or the same physical person with two
        Postgres user records both syncing in parallel. The canonical
        rewriter intentionally DOES NOT collapse these (would silently
        hide one tenant's data from the other — the May 2026 Daily
        Review regression for Cameron Investment / Sara IRA / Sara
        Investment / Schwab ••••9437). Each tenant sees their own
        partition. See `stg_canonical_account_owner.sql`'s
        concurrent-activity guard for details.

      * Stale-uid case B (one uid recent, others outside the 90-day
        window) IS still rewritten by the canonical-owner consolidator
        — exactly one populated uid survives in staging — so it never
        trips this test either.

    Allowed exception:
      - Accounts with NULL on every row (truly orphaned, no linking
        info available anywhere) — the backfill correctly refuses;
        these are invisible to all real users because tenant
        filtering on `_user_account_list()` excludes them.

    Failure mode flagged by this test:
      * `null_and_populated`: same account has both NULL AND
        non-NULL user_ids (case A regression).
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
    'null_and_populated' as failure_kind
from unioned
where null_rows > 0 and populated_rows > 0
