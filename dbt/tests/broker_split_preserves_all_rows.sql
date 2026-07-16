/*
    The per-broker staging split (stg_broker_<slug>_{history,current,balances})
    must be MUTUALLY EXCLUSIVE and EXHAUSTIVE: every real source row lands
    in exactly one broker partition (a named broker, or the ``_other_``
    catch-all). If a future edit to broker_row_filter / known_brokers /
    a per-broker model drops or double-counts rows, the base staging models
    would silently gain or lose data with no other failing test.

    This asserts the row count of the union of the per-broker models equals
    the row count of the underlying raw source for each surface.

    - history: broker models are pure passthrough of the whole seed EXCEPT
      stg_broker_alpaca_history, which intentionally drops Alpaca's duplicate
      activities partial-fill rows when an orders-aggregate covers the same
      order (see stg_broker_alpaca_history.sql + broker-sync-safety 2026-07-16).
      So the expected history count is seed count MINUS those dropped dupes,
      computed below with the identical rule. This still protects routing
      integrity: any accidental drop/double-count from broker_row_filter /
      known_brokers for ANY broker (or an Alpaca drop beyond the exact dedup
      set) still fails the count.
    - current: broker models are pure passthrough of the whole seed, so union
      count == seed count exactly.
    - balances: broker models pull cash/account_total rows from BOTH the
      account_balances seed and the legacy current_positions export, so the
      expected count is the sum of those filtered source counts. Demo rows
      are added by the base model (not the broker models) and are excluded
      here on purpose.

    Returns one row per surface that fails to balance.
*/

with history_split as (
    select count(*) as n from (
        select * from {{ ref('stg_broker_schwab_history') }}
        union all select * from {{ ref('stg_broker_alpaca_history') }}
        union all select * from {{ ref('stg_broker_fidelity_history') }}
        union all select * from {{ ref('stg_broker_interactive_history') }}
        union all select * from {{ ref('stg_broker_other_history') }}
    )
),
-- Alpaca activities partial-fill rows that stg_broker_alpaca_history drops
-- because an orders-aggregate row covers the same
-- (tenant_id, Date, Symbol, Action) equity group. MUST mirror the keep/drop
-- rule in stg_broker_alpaca_history.sql exactly.
alpaca_history_dropped as (
    select count(*) as n from (
        select
            regexp_contains(cast(Description as string), r'(?i) (PARTIAL_)?FILL at ') as is_partial_fill,
            (cast(Action as string) in ('Buy', 'Sell')) as is_equity,
            countif(cast(Action as string) in ('Buy', 'Sell')
                    and not regexp_contains(cast(Description as string), r'(?i) (PARTIAL_)?FILL at '))
                over (partition by cast(tenant_id as string), cast(Date as string),
                                   cast(Symbol as string), cast(Action as string)) as n_aggregate
        from {{ ref('trade_history') }}
        where {{ broker_row_filter('Account', 'alpaca') }}
    )
    where is_equity and is_partial_fill and n_aggregate >= 1
),
history_source as (
    select
        (select count(*) from {{ ref('trade_history') }})
        - (select n from alpaca_history_dropped) as n
),

current_split as (
    select count(*) as n from (
        select * from {{ ref('stg_broker_schwab_current') }}
        union all select * from {{ ref('stg_broker_alpaca_current') }}
        union all select * from {{ ref('stg_broker_fidelity_current') }}
        union all select * from {{ ref('stg_broker_interactive_current') }}
        union all select * from {{ ref('stg_broker_other_current') }}
    )
),
current_source as (
    select count(*) as n from {{ ref('current_positions') }}
),

balances_split as (
    select count(*) as n from (
        select * from {{ ref('stg_broker_schwab_balances') }}
        union all select * from {{ ref('stg_broker_alpaca_balances') }}
        union all select * from {{ ref('stg_broker_fidelity_balances') }}
        union all select * from {{ ref('stg_broker_interactive_balances') }}
        union all select * from {{ ref('stg_broker_other_balances') }}
    )
),
balances_source as (
    select
        (
            select count(*) from {{ ref('account_balances') }}
            where trim(coalesce(cast(account as string), '')) != ''
              and lower(trim(coalesce(cast(row_type as string), ''))) in ('cash', 'account_total')
        )
        + (
            select count(*) from {{ ref('current_positions') }}
            where lower(trim(coalesce(cast(security_type as string), ''))) = 'cash and money market'
        )
        + (
            select count(*) from {{ ref('current_positions') }}
            where lower(trim(coalesce(cast(symbol as string), ''))) in ('account total', 'positions total')
        ) as n
)

select 'history' as surface, hs.n as split_rows, hsrc.n as source_rows
from history_split hs, history_source hsrc
where hs.n != hsrc.n

union all

select 'current' as surface, cs.n as split_rows, csrc.n as source_rows
from current_split cs, current_source csrc
where cs.n != csrc.n

union all

select 'balances' as surface, bs.n as split_rows, bsrc.n as source_rows
from balances_split bs, balances_source bsrc
where bs.n != bsrc.n
