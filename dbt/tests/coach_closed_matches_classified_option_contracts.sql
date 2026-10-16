/*
    CHECK 12. mart_coaching_signals.total_closed is the count of closed
    option contracts the coach narrates. int_strategy_classification
    omits a close that has no opening fill (opened_before_history):
    there is no cost basis, and realized P&L on that contract is 0.
    The coach must use that same population. Counting the close labeled
    it Other Option, so total_closed ran ahead of classified closed
    contracts (reconcile runs 38531276167, 38700075153, and
    38867054831).

    The nightly audit fails when the counts disagree on a key present
    in both marts. A coach-only key is a NOTE (reconcile.py CHECK 12).
    A missing side counts as 0 here, so a one-sided key with a non-zero
    count still fails, and a coach-only key whose total_closed is 0
    does not. Open contracts can put a strategy on the coach grain
    with total_closed 0. That is not a CHECK 12 failure.
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
