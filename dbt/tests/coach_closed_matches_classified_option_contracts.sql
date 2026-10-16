/*
    CHECK 12. mart_coaching_signals.total_closed is the count of closed
    option contracts the coach narrates. int_strategy_classification
    omits a close that has no opening fill (opened_before_history):
    there is no cost basis, and realized P&L on that contract is 0.
    The coach must use that same population. Counting the close labeled
    it Other Option, so total_closed ran ahead of classified closed
    contracts (reconcile runs 38531276167, 38700075153, and
    38867054831).

    Missing side counts as 0, which is what the nightly audit compares
    on keys present in both. A coach-only key whose total_closed is 0
    is the audit's NOTE, not this failure.
*/

with coach as (
    select
        coalesce(tenant_id, account) as tkey,
        strategy,
        sum(total_closed) as closed_in_coach
    from {{ ref('mart_coaching_signals') }}
    group by 1, 2
),

classified as (
    select
        coalesce(tenant_id, account) as tkey,
        strategy,
        count(*) as closed_in_pos
    from {{ ref('int_strategy_classification') }}
    where trade_group_type = 'option_contract'
      and status = 'Closed'
    group by 1, 2
)

select
    coalesce(c.tkey, p.tkey) as tkey,
    coalesce(c.strategy, p.strategy) as strategy,
    c.closed_in_coach,
    p.closed_in_pos
from coach c
full outer join classified p
    on c.tkey = p.tkey
   and c.strategy = p.strategy
where coalesce(c.closed_in_coach, 0) != coalesce(p.closed_in_pos, 0)
