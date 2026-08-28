/*
    cumulative_net_deposits has two modes (see mart_wealth_daily):

      FALLBACK (no cash_transfer on or before first snapshot):
        Day-1 cumulative MUST equal that day's account_value so
        "Exclude deposits & withdrawals" starts at $0. Later explicit
        transfers (trade_date > first snapshot) stack on top.

      ITEMIZED (at least one cash_transfer on or before first snapshot):
        Day-1 cumulative MUST equal Σ cash_transfers through that date
        (CSV Funds Received / MoneyLink, etc.). Do NOT also add
        opening_cash — that lump already contains those deposits.
        Latest-day cumulative is Σ cash_transfers through last_date.

    Fails if either mode's expected cumulative disagrees.
*/

with first_wealth as (
    select
        tenant_id,
        account,
        user_id,
        min(date) as first_date
    from {{ ref('mart_wealth_daily') }}
    where tenant_id is not null
    group by 1, 2, 3
),

first_row as (
    select
        w.tenant_id,
        w.account,
        w.user_id,
        w.date as first_date,
        w.account_value,
        w.cumulative_net_deposits
    from {{ ref('mart_wealth_daily') }} w
    inner join first_wealth f
      on f.tenant_id is not distinct from w.tenant_id
     and f.account = w.account
     and (f.user_id is not distinct from w.user_id)
     and f.first_date = w.date
),

last_row as (
    select * except (rn)
    from (
        select
            w.tenant_id,
            w.account,
            w.user_id,
            w.date as last_date,
            w.cumulative_net_deposits,
            row_number() over (
                partition by w.tenant_id, w.account, w.user_id
                order by w.date desc
            ) as rn
        from {{ ref('mart_wealth_daily') }} w
        where w.tenant_id is not null
    )
    where rn = 1
),

itemized as (
    select
        f.tenant_id,
        f.account,
        f.user_id,
        countif(h.trade_date <= f.first_date) > 0 as has_itemized,
        coalesce(sum(if(h.trade_date <= f.first_date, h.amount, 0)), 0) as through_first,
        coalesce(sum(if(h.trade_date <= l.last_date, h.amount, 0)), 0) as through_last
    from first_row f
    inner join last_row l
      on l.tenant_id is not distinct from f.tenant_id
     and l.account = f.account
     and (l.user_id is not distinct from f.user_id)
    left join {{ ref('stg_history') }} h
      on h.tenant_id is not distinct from f.tenant_id
     and h.account = f.account
     and (h.user_id is not distinct from f.user_id)
     and h.action = 'cash_transfer'
    group by 1, 2, 3
),

post_opening_transfers as (
    select
        f.tenant_id,
        f.account,
        f.user_id,
        coalesce(sum(h.amount), 0) as post_opening
    from first_row f
    left join {{ ref('stg_history') }} h
      on h.tenant_id is not distinct from f.tenant_id
     and h.account = f.account
     and (h.user_id is not distinct from f.user_id)
     and h.action = 'cash_transfer'
     and h.trade_date > f.first_date
    group by 1, 2, 3
)

select
    'first_day' as check_name,
    f.tenant_id,
    f.account,
    f.first_date as as_of,
    case
        when i.has_itemized then i.through_first
        else f.account_value
    end as expected_cum,
    f.cumulative_net_deposits as actual_cum
from first_row f
inner join itemized i
  on i.tenant_id is not distinct from f.tenant_id
 and i.account = f.account
 and (i.user_id is not distinct from f.user_id)
where abs(
    f.cumulative_net_deposits
    - case when i.has_itemized then i.through_first else f.account_value end
) > 0.01

union all

select
    'latest_day' as check_name,
    l.tenant_id,
    l.account,
    l.last_date as as_of,
    case
        when i.has_itemized then i.through_last
        else f.account_value + p.post_opening
    end as expected_cum,
    l.cumulative_net_deposits as actual_cum
from last_row l
inner join first_row f
  on f.tenant_id is not distinct from l.tenant_id
 and f.account = l.account
 and (f.user_id is not distinct from l.user_id)
inner join itemized i
  on i.tenant_id is not distinct from l.tenant_id
 and i.account = l.account
 and (i.user_id is not distinct from l.user_id)
inner join post_opening_transfers p
  on p.tenant_id is not distinct from l.tenant_id
 and p.account = l.account
 and (p.user_id is not distinct from l.user_id)
where abs(
    l.cumulative_net_deposits
    - case
        when i.has_itemized then i.through_last
        else f.account_value + p.post_opening
      end
) > 0.01
