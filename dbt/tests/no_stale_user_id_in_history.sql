{#
    Invariant: every account label that appears in `stg_history` with a
    populated `user_id` must also appear in EITHER `stg_current` OR
    `stg_account_balances` under the SAME `user_id`. If trade history
    has rows under uid=X for an account, but the broker's current
    positions / balance snapshots show that account under uid=Y (and
    only uid=Y), then uid=X is a STALE stamp — likely from:

      - An old user record that got renumbered or merged in the app DB
      - A test import committed under the wrong uid
      - A re-link of the same broker account under a new uid after
        the old app-side record was deleted

    Without reconciliation, marts partition by (account, user_id) and
    the user's trade history sits in one partition while their current
    holdings sit in another — same failure mode as the NULL-vs-populated
    orphan-tenant split (`no_orphan_user_id_per_account`).

    The fix is the canonical-owner cross-reference in
    `stg_history.account_owner` / `stg_history.canonical_account_owner`
    (extended in May 2026 after IYW Emmory rendered with two phantom
    "Leg 1" pills + a -$1,957 phantom Dividend Closed row).

    Allowed exceptions:

      - Accounts in stg_history that have NO row in current/balances
        AT ALL (fully closed accounts, pre-history holdings, true
        orphans). These pass because there's no canonical uid to
        anchor against.

      - Accounts whose current/balances rows are ambiguous (multiple
        distinct uids). The backfill refuses to fire for safety; this
        test mirrors that safety and also passes.

    Failure mode: stg_history has populated rows under uid=X for an
    account where current+balances show only uid=Y (and only one Y).
#}

with hist_uids as (
    select distinct account, user_id
    from {{ ref('stg_history') }}
    where user_id is not null
),

-- Canonical owner: the broker's current view, restricted to accounts
-- where current/balances agrees on exactly ONE uid.
canonical as (
    select account, any_value(user_id) as canonical_uid
    from (
        select account, user_id
        from {{ ref('stg_current') }}
        where user_id is not null
        union distinct
        select account, user_id
        from {{ ref('stg_account_balances') }}
        where user_id is not null
    )
    group by 1
    having count(distinct user_id) = 1
)

select
    h.account,
    h.user_id    as stale_history_uid,
    c.canonical_uid
from hist_uids h
join canonical c using (account)
where h.user_id != c.canonical_uid
