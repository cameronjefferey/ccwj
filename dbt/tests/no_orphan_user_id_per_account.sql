{# 
    Invariant: every account in the post-staging warehouse has at most
    one populated user_id, AND if any populated user_id exists for an
    account, no NULL-user_id rows should remain for that same account.

    Why: positions_summary, int_strategy_classification, mart_daily_pnl,
    and int_dividend_events all partition by (account, user_id). When
    the same physical broker account ends up under both NULL and a real
    user_id (typical orphan-tenant pattern: synced before linking, then
    after), the buy and sell halves land in different partitions and
    Position Detail reads $0 P&L on a fully-closed position.

    The orphan-tenant backfill in stg_history / stg_current /
    stg_account_balances is supposed to eliminate this. This test
    catches any regression that re-introduces the gap (a new staging
    model that forgets to backfill, a malformed seed, etc.).

    Allowed exceptions:
      - Demo seeds (no user_id at all → all NULL → only one user_id =
        NULL → passes the "exactly one user_id" check)
      - Accounts with NULL on every row (truly orphaned, no linking
        info available) — backfill correctly refuses; these are
        invisible to all real users anyway
      - Accounts with multiple distinct populated user_ids (would mean
        a real account-label collision; backfill correctly refuses to
        guess; downstream tenant filtering still works)

    Failure mode: an account has BOTH NULL rows AND populated rows.
#}

with stg_history_check as (
    select
        'stg_history' as source,
        account,
        countif(user_id is null)     as null_rows,
        countif(user_id is not null) as populated_rows
    from {{ ref('stg_history') }}
    group by account
),
stg_current_check as (
    select
        'stg_current' as source,
        account,
        countif(user_id is null)     as null_rows,
        countif(user_id is not null) as populated_rows
    from {{ ref('stg_current') }}
    group by account
),
stg_account_balances_check as (
    select
        'stg_account_balances' as source,
        account,
        countif(user_id is null)     as null_rows,
        countif(user_id is not null) as populated_rows
    from {{ ref('stg_account_balances') }}
    group by account
)

select source, account, null_rows, populated_rows
from (
    select * from stg_history_check
    union all select * from stg_current_check
    union all select * from stg_account_balances_check
)
where null_rows > 0 and populated_rows > 0
